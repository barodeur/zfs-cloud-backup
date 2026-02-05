use anyhow::{Context, Result, bail};
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::serde_types::Object;

const PART_SIZE: usize = 8 * 1024 * 1024; // 8 MB

/// Configuration for S3 connection.
pub struct S3Config {
    pub bucket: String,
    pub endpoint: String,
    pub region: String,
    pub prefix: String,
}

/// Create an S3 bucket handle.
pub fn create_bucket(config: &S3Config) -> Result<Box<Bucket>> {
    let region = Region::Custom {
        region: config.region.clone(),
        endpoint: config.endpoint.clone(),
    };
    let credentials = Credentials::default().context("failed to load S3 credentials")?;

    let bucket = Bucket::new(&config.bucket, region, credentials)
        .context("failed to create S3 bucket handle")?
        .with_path_style();

    Ok(bucket)
}

/// Build the S3 key prefix for a dataset.
pub fn dataset_prefix(prefix: &str, dataset: &str) -> String {
    if prefix.is_empty() {
        dataset.to_string()
    } else {
        format!("{}/{}", prefix.trim_end_matches('/'), dataset)
    }
}

/// List all objects under a prefix.
pub async fn list_objects(bucket: &Bucket, prefix: &str) -> Result<Vec<Object>> {
    let results = bucket
        .list(prefix.to_string(), None)
        .await
        .context("failed to list S3 objects")?;

    let mut objects = Vec::new();
    for result in results {
        objects.extend(result.contents);
    }

    Ok(objects)
}

/// Upload data from a reader using S3 multipart upload.
/// Reads chunks from the reader and uploads them as parts.
pub async fn multipart_upload(
    bucket: &Bucket,
    key: &str,
    mut reader: impl std::io::Read,
) -> Result<()> {
    let init = bucket
        .initiate_multipart_upload(key, "application/octet-stream")
        .await
        .context("failed to initiate multipart upload")?;

    let upload_id = &init.upload_id;
    let mut parts = Vec::new();
    let mut part_number: u32 = 1;

    loop {
        let mut buf = vec![0u8; PART_SIZE];
        let mut filled = 0;

        // Fill the buffer completely (or until EOF)
        while filled < PART_SIZE {
            match reader.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    let _ = bucket.abort_upload(key, upload_id).await;
                    return Err(e).context("failed to read from zfs send stream");
                }
            }
        }

        if filled == 0 {
            break;
        }

        buf.truncate(filled);

        let part = bucket
            .put_multipart_chunk(buf, key, part_number, upload_id, "application/octet-stream")
            .await;

        match part {
            Ok(part) => {
                eprintln!("  uploaded part {} ({:.1} MB)", part_number, filled as f64 / 1_048_576.0);
                parts.push(part);
            }
            Err(e) => {
                let _ = bucket.abort_upload(key, upload_id).await;
                bail!("failed to upload part {}: {}", part_number, e);
            }
        }

        part_number += 1;
    }

    if parts.is_empty() {
        let _ = bucket.abort_upload(key, upload_id).await;
        bail!("zfs send produced no data");
    }

    bucket
        .complete_multipart_upload(key, upload_id, parts)
        .await
        .context("failed to complete multipart upload")?;

    Ok(())
}

/// Delete an S3 object.
pub async fn delete_object(bucket: &Bucket, key: &str) -> Result<()> {
    bucket
        .delete_object(key)
        .await
        .context("failed to delete S3 object")?;

    Ok(())
}
