use crate::RegisterCommand;
use bincode::error::{DecodeError, EncodeError};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[derive(Debug)]
pub enum EncodingError {
    IoError(Error),
    BincodeError(EncodeError),
}

#[derive(Debug, derive_more::Display)]
pub enum DecodingError {
    IoError(Error),
    BincodeError(DecodeError),
    InvalidMessageSize,
}

type HmacSha256 = Hmac<Sha256>;

pub fn caclulate_hmac_tag(message: &[u8], key: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&message);
    mac.finalize().into_bytes().into()
}

pub fn verify_hmac_tag(message: &[u8], key: &[u8], tag: &[u8]) -> bool {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&message);
    mac.verify_slice(tag).is_ok()
}

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), DecodingError> {
    
    let mut msg_len_buf = [0u8; 8];

    let msg_len = match data.read_exact(&mut msg_len_buf).await {
        Ok(_) => u64::from_be_bytes(msg_len_buf) as usize,
        Err(e) => { return Err(DecodingError::IoError(e)); },
    };

    if msg_len <= 32 {
        return Err(DecodingError::InvalidMessageSize);
    }

    let mut buf = vec![0u8; msg_len as usize];
    match data.read_exact(&mut buf).await {
        Ok(_) => {},
        Err(e) => { return Err(DecodingError::IoError(e)); },
    }

    let payload = &buf[..msg_len-32];

    let rc = bincode::serde::decode_from_slice(payload, bincode::config::standard().with_big_endian().with_fixed_int_encoding()).map_err(DecodingError::BincodeError)?.0;
    let tag = &buf[msg_len-32..];

    match rc {
        RegisterCommand::Client(_) => Ok((rc, verify_hmac_tag(payload, hmac_client_key, tag))),
        RegisterCommand::System(_) => Ok((rc, verify_hmac_tag(payload, hmac_system_key, tag))),
    }
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), EncodingError> {
    
    let conf = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();
    
    let paylod = bincode::serde::encode_to_vec(&cmd, conf).map_err(EncodingError::BincodeError)?;
    let tag = caclulate_hmac_tag(&paylod, hmac_key);
    let msg_len = (paylod.len() + tag.len()) as u64;

    debug_assert!(tag.len() == 32);

    writer.write_all(&msg_len.to_be_bytes()).await.map_err(EncodingError::IoError)?;
    writer.write_all(&paylod).await.map_err(EncodingError::IoError)?;
    writer.write_all(&tag).await.map_err(EncodingError::IoError)?;
    Ok(())
}
