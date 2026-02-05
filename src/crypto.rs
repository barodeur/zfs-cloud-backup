use age::Decryptor;
use anyhow::{Context, Result};
use std::io::{Read, Write};

/// Encrypt data from a reader using age, returning a reader over the encrypted data.
/// This spawns a background thread that reads from `input`, encrypts, and writes to a pipe.
/// The returned reader is the read end of that pipe.
pub fn encrypt_stream(
    input: impl Read + Send + 'static,
    recipient: &str,
) -> Result<impl Read + Send> {
    let recipient: age::x25519::Recipient = recipient
        .parse()
        .map_err(|e: &str| anyhow::anyhow!("invalid age recipient: {}", e))?;

    let (pipe_reader, pipe_writer) = os_pipe::pipe().context("failed to create pipe")?;

    std::thread::spawn(move || {
        if let Err(e) = encrypt_thread(input, pipe_writer, recipient) {
            eprintln!("encryption thread error: {}", e);
        }
    });

    Ok(pipe_reader)
}

fn encrypt_thread(
    mut input: impl Read,
    output: impl Write,
    recipient: age::x25519::Recipient,
) -> Result<()> {
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
            .context("failed to create encryptor")?;

    let mut writer = encryptor
        .wrap_output(output)
        .context("failed to wrap output for encryption")?;

    std::io::copy(&mut input, &mut writer).context("failed to stream through encryptor")?;

    writer.finish().context("failed to finalize encryption")?;

    Ok(())
}

/// Decrypt data from a reader using an age identity file.
/// Returns a reader over the decrypted plaintext.
pub fn decrypt_reader(input: impl Read + 'static, identity_path: &str) -> Result<Box<dyn Read>> {
    let identity_file =
        std::fs::read_to_string(identity_path).context("failed to read age identity file")?;

    let identity: age::x25519::Identity = identity_file
        .lines()
        .find(|l| l.starts_with("AGE-SECRET-KEY-"))
        .unwrap_or(identity_file.trim())
        .parse()
        .map_err(|e: &str| anyhow::anyhow!("invalid age identity: {}", e))?;

    let decryptor = Decryptor::new(input).context("failed to create age decryptor")?;

    let reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .context("failed to decrypt with provided identity")?;

    Ok(Box::new(reader))
}
