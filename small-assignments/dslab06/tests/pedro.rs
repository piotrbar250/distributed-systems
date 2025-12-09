#[cfg(test)]
mod tests {
    use std::{path::PathBuf};

    use crate::solution::build_stable_storage;
    use ntest::timeout;
    // use sha2::{Sha256, Digest};
    // use tokio::{fs::File, io::AsyncWriteExt};


    // fn compute_checksum(data: &[u8]) -> [u8; 32] {
    //     let mut hasher = Sha256::new();
    //     hasher.update(data);
    //     let result = hasher.finalize();
    //     result.into()
    // }
    
    // fn key_to_filename(key: &str) -> String {
    //     let mut hasher = Sha256::new();
    //     hasher.update(key.as_bytes());
    //     let result = hasher.finalize();
    //     format!("{:x}", result)
    // }

    // async fn helper(key: &str, value: &[u8]) {
    //     let key = key_to_filename(key);

    //     let tmp_dir = PathBuf::from("somedir/tmp");
    //     let tmp_dir_handle = tokio::fs::File::open("somedir/tmp").await.unwrap();
    //     let mut tmp_file = File::create(tmp_dir.join(key)).await.unwrap();
    //     let original_checksum = compute_checksum(value);
    //     tmp_file.write_all(&original_checksum).await.unwrap();
    //     tmp_file.write_all(value).await.unwrap();
    //     tmp_file.sync_data().await.unwrap();
    //     tmp_dir_handle.sync_data().await.unwrap();
    // }

    #[tokio::test]
    #[timeout(500)]
    async fn pedro_test() {

        let mut storage = build_stable_storage(PathBuf::from("somedir")).await;
        // helper("hejo", b"panowie moi kochani siemano tutaj:)").await;
        
        storage.put("ala", b"kot").await.unwrap();

        // let value = String::from_utf8(storage.get("ala").await.unwrap()).unwrap();
        assert_eq!(storage.get("ala").await, Some(Vec::from(b"kot")));

        // let value = String::from_utf8(storage.get("hejo").await.unwrap()).unwrap();
        // assert_eq!(storage.get("hejo").await, Some(Vec::from(b"kot")));
        assert_eq!(storage.get("hejo").await, Some(Vec::from(b"panowie moi kochani siemano tutaj:)")));
    }

}