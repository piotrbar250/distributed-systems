use assignment_2_solution::{
    ClientCommandHeader, ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, Configuration, OperationReturn, PublicConfiguration, RegisterCommand, SectorVec, StatusCode, run_register_process, serialize_register_command
};
use assignment_2_test_utils::system::*;
use serde_big_array::Array;
use std::path::{PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct TestProcessesConfigManual {
    hmac_client_key: Vec<u8>,
    hmac_system_key: Vec<u8>,
    storage_dirs: Vec<PathBuf>,
    tcp_locations: Vec<(String, u16)>,
}

impl TestProcessesConfigManual {
    pub const N_SECTORS: u64 = 65536;

    #[allow(clippy::missing_panics_doc, clippy::must_use_candidate)]
    pub fn new(processes_count: usize, port_range_start: u16) -> Self {
        TestProcessesConfigManual {
            hmac_client_key: (0..32).map(|_| 1).collect(),
            hmac_system_key: (0..64).map(|_| 2).collect(),
            storage_dirs: (0..processes_count)
                .map(|idx| PathBuf::from(format!("somedir_{idx}")))
                .collect(),
            tcp_locations: (0..processes_count)
                .map(|idx| {
                    (
                        "127.0.0.1".to_string(),
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
                    .clone(),
                tcp_locations: self.tcp_locations.clone(),
                self_rank: u8::try_from(proc_idx + 1).unwrap(),
                n_sectors: TestProcessesConfigManual::N_SECTORS,
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
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    pub async fn start_single(&self, idx: usize) {
        run_register_process(self.config(idx)).await;
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
