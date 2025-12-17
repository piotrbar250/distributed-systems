
use tokio::fs::{File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::{SECTOR_SIZE, SectorIdx, SectorVec};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc};

#[async_trait::async_trait]
pub trait SectorsManager: Send + Sync {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec;

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
}

/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    tokio::fs::create_dir_all(&path).await.unwrap();

    let mut index: HashMap<SectorIdx, Entry> = HashMap::new();
    let mut to_delete: Vec<PathBuf> = Vec::new();

    let mut rd = tokio::fs::read_dir(&path).await.unwrap();
    while let Some(entry) = rd.next_entry().await.unwrap() {
        let ft = entry.file_type().await.unwrap();
        if !ft.is_file() {
            continue;
        }

        let file_name_os = entry.file_name();
        let Some(file_name) = file_name_os.to_str() else { continue; };

        if file_name.starts_with("tmp_") {
            to_delete.push(entry.path());
            continue;
        }

        let Some((idx, ts, wr)) = parse_committed_name(file_name) else {
            // unknown file; ignore (or delete if you want stricter cleanup)
            continue;
        };

        let this_path = entry.path();

        match index.get(&idx) {
            None => {
                index.insert(idx, Entry { ts, wr, path: this_path });
            }
            Some(existing) => {
                if is_newer((ts, wr), (existing.ts, existing.wr)) {
                    to_delete.push(existing.path.clone());
                    index.insert(idx, Entry { ts, wr, path: this_path });
                } else {
                    to_delete.push(this_path);
                }
            }
        }
    }

    let std_dir = std::fs::File::open(&path).unwrap();
    let root_dir_handle = tokio::fs::File::from_std(std_dir);

    for p in to_delete {
        _ = tokio::fs::remove_file(&p).await;
    }

    _ = root_dir_handle.sync_all().await;

    Arc::new(MySectorsManager {
        root_dir: path,
        root_dir_handle,
        index: Mutex::new(index),
    })
}

struct Entry {
    ts: u64,
    wr: u8,
    path: PathBuf,
}

struct MySectorsManager {
    root_dir: PathBuf,
    root_dir_handle: File,
    index: Mutex<HashMap<SectorIdx, Entry>>,
}

#[async_trait::async_trait]
impl SectorsManager for MySectorsManager {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let path_opt = {
            let idx_map = self.index.lock().await;
            idx_map.get(&idx).map(|e| e.path.clone())
        };

        let Some(path) = path_opt else {
            return zero_sector();
        };

        let mut f = match File::open(&path).await {
            Ok(f) => f,
            Err(_) => return zero_sector(),
        };

        let mut buf = [0u8; SECTOR_SIZE];
        if f.read_exact(&mut buf).await.is_err() {
            return zero_sector();
        }

        SectorVec(Box::new(serde_big_array::Array(buf)))
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let idx_map = self.index.lock().await;
        match idx_map.get(&idx).map(|e| (e.ts, e.wr)) {
            Some((ts, wr)) => (ts, wr),
            None => (0, 0)
        }
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let (data, ts, wr) = sector;

        let final_path = self.root_dir.join(format!("{}_{}_{}", idx, ts, wr));
        let tmp_path = self.root_dir.join(format!("tmp_{}_{}_{}", idx, ts, wr));

        let old_path = {
            let idx_map = self.index.lock().await;
            idx_map.get(&idx).map(|e| e.path.clone())
        };

        let mut tmp_file = File::create(&tmp_path).await.unwrap();
        tmp_file.write_all(&data.0[..]).await.unwrap();
        tmp_file.sync_data().await.unwrap();
        self.root_dir_handle.sync_all().await.unwrap();

        drop(tmp_file);
        tokio::fs::rename(&tmp_path, &final_path).await.unwrap();

        self.root_dir_handle.sync_all().await.unwrap();

        if let Some(old) = old_path {
            if old != final_path {
                _ = tokio::fs::remove_file(old).await;
                self.root_dir_handle.sync_all().await.unwrap();
            }
        }

        let mut idx_map = self.index.lock().await;
        idx_map.insert(
            idx,
            Entry {
                ts: *ts,
                wr: *wr,
                path: final_path,
            },
        );
    }
}

fn zero_sector() -> SectorVec {
    SectorVec(Box::new(serde_big_array::Array([0u8; SECTOR_SIZE])))
}

fn is_newer((ts1, wr1): (u64, u8), (ts2, wr2): (u64, u8)) -> bool {
    (ts1, wr1) > (ts2, wr2)
}

fn parse_committed_name(name: &str) -> Option<(SectorIdx, u64, u8)> {
    let mut it = name.split('_');
    let idx = it.next()?.parse::<u64>().ok()?;
    let ts  = it.next()?.parse::<u64>().ok()?;
    let wr  = it.next()?.parse::<u8>().ok()?;
    if it.next().is_some() {
        return None;
    }
    Some((idx, ts, wr))
}
