use std::convert::TryInto;

use assignment_2_solution::{
    ClientCommandResponse, Configuration, OperationReturn, PublicConfiguration, RegisterCommand,
    SectorVec, StatusCode, run_register_process, serialize_register_command,
};
use hmac::Hmac;
use rand::Rng;
use serde_big_array::Array;
use sha2::Sha256;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Duration;

pub const HMAC_TAG_SIZE: usize = 32;

pub struct RegisterResponse {
    pub content: ClientCommandResponse,
    pub hmac_tag: [u8; HMAC_TAG_SIZE],
}

pub struct TestProcessesConfig {
    hmac_client_key: Vec<u8>,
    hmac_system_key: Vec<u8>,
    storage_dirs: Vec<TempDir>,
    tcp_locations: Vec<(String, u16)>,
}

impl TestProcessesConfig {
    pub const N_SECTORS: u64 = 65536;

    #[allow(clippy::missing_panics_doc, clippy::must_use_candidate)]
    pub fn new(processes_count: usize, port_range_start: u16) -> Self {
        TestProcessesConfig {
            hmac_client_key: (0..32).map(|_| rand::rng().random_range(0..255)).collect(),
            hmac_system_key: (0..64).map(|_| rand::rng().random_range(0..255)).collect(),
            storage_dirs: (0..processes_count)
                .map(|_| tempfile::tempdir().unwrap())
                .collect(),
            tcp_locations: (0..processes_count)
                .map(|idx| {
                    (
                        "localhost".to_string(),
                        port_range_start + u16::try_from(idx).unwrap(),
                    )
                })
                .collect(),
        }
    }

    fn config(&self, proc_idx: usize) -> Configuration {
        Configuration {
            public: PublicConfiguration {
                storage_dir: self
                    .storage_dirs
                    .get(proc_idx)
                    .unwrap()
                    .path()
                    .to_path_buf(),
                tcp_locations: self.tcp_locations.clone(),
                self_rank: u8::try_from(proc_idx + 1).unwrap(),
                n_sectors: TestProcessesConfig::N_SECTORS,
            },
            hmac_system_key: self.hmac_system_key.clone().try_into().unwrap(),
            hmac_client_key: self.hmac_client_key.clone().try_into().unwrap(),
        }
    }

    pub async fn start(&self) {
        let processes_count = self.storage_dirs.len();
        for idx in 0..processes_count {
            tokio::spawn(run_register_process(self.config(idx)));
        }
        wait_for_tcp_listen().await;
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn send_cmd(&self, register_cmd: &RegisterCommand, stream: &mut TcpStream) {
        let mut data = Vec::new();
        serialize_register_command(register_cmd, &mut data, &self.hmac_client_key)
            .await
            .unwrap();

        stream.write_all(&data).await.unwrap();
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn connect(&self, proc_idx: usize) -> TcpStream {
        let location = self.tcp_locations.get(proc_idx).unwrap();
        TcpStream::connect((location.0.as_str(), location.1))
            .await
            .expect("Could not connect to TCP port")
    }

    #[allow(clippy::missing_panics_doc)]
    pub async fn read_response(&self, stream: &mut TcpStream) -> RegisterResponse {
        // Decode response by hand to avoid leaking solution
        let size = stream.read_u64().await.unwrap();
        let status = match stream.read_u32().await.unwrap() {
            0 => StatusCode::Ok,
            1 => StatusCode::AuthFailure,
            2 => StatusCode::InvalidSectorIndex,
            _ => panic!("Invalide status code"),
        };
        let req_id = stream.read_u64().await.unwrap();
        let op_type = stream.read_u32().await.unwrap();
        let op_return = match op_type {
            0 => {
                let mut buf = [0u8; 4096];
                stream.read_exact(&mut buf).await.unwrap();
                OperationReturn::Read {
                    read_data: SectorVec(Box::new(Array(buf))),
                }
            }
            1 => OperationReturn::Write,
            _ => panic!("Invalid operation type"),
        };

        assert_eq!(
            size,
            match op_return {
                OperationReturn::Write => 16 + HMAC_TAG_SIZE as u64,
                OperationReturn::Read { .. } => 16 + HMAC_TAG_SIZE as u64 + 4096,
            }
        );
        let mut tag = [0x00_u8; HMAC_TAG_SIZE];
        stream.read_exact(&mut tag).await.unwrap();
        RegisterResponse {
            content: ClientCommandResponse {
                status,
                request_identifier: req_id,
                op_return,
            },
            hmac_tag: tag,
        }
    }

    #[allow(clippy::must_use_candidate)]
    pub fn get_hmac_client_key(&self) -> &[u8] {
        &self.hmac_client_key
    }
}

async fn wait_for_tcp_listen() {
    tokio::time::sleep(Duration::from_millis(300)).await;
}

pub type HmacSha256 = Hmac<Sha256>;
