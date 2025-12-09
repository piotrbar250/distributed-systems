use core::panic;
use std::{io::ErrorKind, path::PathBuf};

use sha2::{Digest, Sha256};
use tokio::{fs::{self, File}, io::{AsyncReadExt, AsyncWriteExt}};

#[async_trait::async_trait]
pub trait StableStorage: Send + Sync {
    /// Stores `value` under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String>;

    /// Retrieves value stored under `key`.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Removes `key` and the value stored under it.
    ///
    /// Detailed requirements are specified in the description of the assignment.
    async fn remove(&mut self, key: &str) -> bool;
}

struct Storage {
    root_dir: PathBuf,
    tmp_dir: PathBuf,
    root_dir_handle: File,
    tmp_dir_handle: File,
}

impl Storage {
    fn compute_checksum(data: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let result = hasher.finalize();
        result.into()
    }

    fn key_to_filename(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        let result = hasher.finalize();
        format!("{:x}", result)
    }
}

#[async_trait::async_trait]
impl StableStorage for Storage {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if key.len() > 255 {
            return None;
        }

        let key = Self::key_to_filename(key);

        let mut file = match File::open(self.root_dir.join(&key)).await {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return None,
            Err(_) => panic!(),
        };

        let mut contents = vec![];
        file.read_to_end(&mut contents).await.unwrap();
        return Some(contents);
    }

    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > 255 || value.len() > 65535 {
            return Err("Invalid parameters".to_string());
        }

        let key = Self::key_to_filename(key);
        let mut tmp_file = File::create(self.tmp_dir.join(&key)).await.unwrap();
        let original_checksum = Self::compute_checksum(value);

        tmp_file.write_all(&original_checksum).await.unwrap();
        tmp_file.write_all(value).await.unwrap();
        tmp_file.sync_data().await.unwrap();
        self.tmp_dir_handle.sync_data().await.unwrap();

        let mut dst_file = File::create(self.root_dir.join(&key)).await.unwrap();
        dst_file.write_all(value).await.unwrap();
        dst_file.sync_data().await.unwrap();
        self.root_dir_handle.sync_data().await.unwrap();

        tokio::fs::remove_file(self.tmp_dir.join(&key)).await.unwrap();
        self.tmp_dir_handle.sync_data().await.unwrap();

        Ok(())
    }

    async fn remove(&mut self, key: &str) -> bool {
        if key.len() > 255 {
            return false;
        }
        let key = Self::key_to_filename(key);

        match tokio::fs::remove_file(self.root_dir.join(&key)).await {
            Ok(_) => {
                self.root_dir_handle.sync_data().await.unwrap();
                true
            }
            Err(e) if e.kind() == ErrorKind::NotFound => false,
            Err(_) => panic!(),
        }
    }
}

pub async fn build_stable_storage(root_storage_dir: PathBuf) -> Box<dyn StableStorage> {
    let root_dir = root_storage_dir;
    let tmp_dir: PathBuf = root_dir.join("tmp");
    tokio::fs::create_dir_all(&tmp_dir).await.unwrap();

    let mut dir = fs::read_dir(&tmp_dir).await.unwrap();
    let root_dir_handle = File::open(&root_dir).await.unwrap();
    let tmp_dir_handle = File::open(&tmp_dir).await.unwrap();

    if let Some(entry) = dir.next_entry().await.unwrap() {
        let key = entry.file_name().to_string_lossy().to_string();
        let mut tmp_file = File::open(tmp_dir.join(&key)).await.unwrap();
        let mut stored_checksum = [0u8; 32];

        match tmp_file.read_exact(&mut stored_checksum).await {
            Ok(_) => {
                let mut value = vec![];
                tmp_file.read_to_end(&mut value).await.unwrap();

                let current_checksum = Storage::compute_checksum(&value);

                if stored_checksum == current_checksum {
                    let mut dst_file = File::create(root_dir.join(&key)).await.unwrap();
                    dst_file.write_all(&value).await.unwrap();
                    dst_file.sync_data().await.unwrap();
                    root_dir_handle.sync_data().await.unwrap();
                }
            }
            Err(e) if e.kind() != ErrorKind::UnexpectedEof => panic!(),
            _ => (),
        }

        tokio::fs::remove_file(tmp_dir.join(&key)).await.unwrap();
        tmp_dir_handle.sync_data().await.unwrap();
    }

    Box::new(Storage {
        root_dir,
        tmp_dir,
        root_dir_handle,
        tmp_dir_handle,
    })
}
