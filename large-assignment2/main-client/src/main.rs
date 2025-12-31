use assignment_2_solution::{
    ClientCommandHeader, ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, Configuration, OperationReturn, PublicConfiguration, RegisterCommand, SectorVec, StatusCode, run_register_process, serialize_register_command
};
use assignment_2_test_utils::system::*;
use hmac::Mac;
use serde_big_array::Array;
use tokio::fs;
use std::path::{PathBuf};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;


pub struct TestProcessesConfig2 {
    hmac_client_key: Vec<u8>,
    hmac_system_key: Vec<u8>,
    storage_dirs: Vec<PathBuf>,
    tcp_locations: Vec<(String, u16)>,
}

impl TestProcessesConfig2 {
    pub const N_SECTORS: u64 = 65536;

    #[allow(clippy::missing_panics_doc, clippy::must_use_candidate)]
    pub fn new(processes_count: usize, port_range_start: u16) -> Self {
        TestProcessesConfig2 {
            // hmac_client_key: (0..32).map(|_| rand::rng().random_range(0..255)).collect(),
            // hmac_system_key: (0..64).map(|_| rand::rng().random_range(0..255)).collect(),            
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
                n_sectors: TestProcessesConfig2::N_SECTORS,
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
        println!("Trying to connect to port: {}", location.1);
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

async fn read_test() {

    let hmac_client_key = [5; 32];
    let hmac_system_key = [1; 64];
    let tcp_port = 30_287;
    let storage_dir = PathBuf::from("somedir");
    let request_identifier = 1778;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir,
        },
        hmac_system_key,
        hmac_client_key,
    };

    let cmd = RegisterCommand::Client(ClientRegisterCommand { 
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Read,
    });

    tokio::spawn(run_register_process(config));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut socket = TcpStream::connect(("127.0.0.1", tcp_port)).await.unwrap();
    
    // let mut data = Vec::new();
    serialize_register_command(&cmd, &mut socket, &hmac_client_key).await.unwrap();
    
    let mut len_buf = [0u8; 8];
    socket.read_exact(&mut len_buf).await.unwrap();
    let msg_len = u64::from_be_bytes(len_buf) as usize;

    println!("client msg len: {}", msg_len);

    let mut frame = vec![0u8; msg_len];
    socket.read_exact(&mut frame).await.unwrap();

    let (payload, tag) = frame.split_at(msg_len - 32);

    let bincode_conf = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    let (resp, _): (ClientCommandResponse, usize) =  bincode::serde::decode_from_slice(&payload, bincode_conf).unwrap();

    match resp.op_return {
        OperationReturn::Read { read_data } => {
            println!("read data{:?}", read_data);
        },
        OperationReturn::Write => {},
    }

}

async fn read_test_utils() {
    let port_range_start = 6000;
    let n_clients = 1;
    let config = TestProcessesConfig2::new(3, port_range_start);
    // config.start().await;

    let mut socket0 = config.connect(2).await;
    println!("connected");
    // let mut socket1 = config.connect(1).await;

    // config
    //     .send_cmd(
    //         &RegisterCommand::Client(ClientRegisterCommand {
    //             header: ClientCommandHeader {
    //                 request_identifier: n_clients,
    //                 sector_idx: 0,
    //             },
    //             content: ClientRegisterCommandContent::Read,
    //         }),
    //         &mut socket1
    //     )
    //     .await;
    // let response = config.read_response(&mut socket1).await;

    // fs::write("log.txt", format!("{:?}", response.content)).await.unwrap();

    // config.send_cmd(
    //     &RegisterCommand::Client(ClientRegisterCommand {
    //         header: ClientCommandHeader {
    //             request_identifier: 1,
    //             sector_idx: 0,
    //         },
    //         content: ClientRegisterCommandContent::Write {
    //             data: SectorVec(Box::new(Array([98; 4096]))),
    //         },
    //     }),
    //     &mut socket0,
    // ).await;

    // let response = config.read_response(&mut socket0).await;

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: n_clients,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut socket0
        )
        .await;
    let response = config.read_response(&mut socket0).await;

    fs::write("log.txt", format!("{:?}", response.content)).await.unwrap();
}


#[tokio::main]
async fn main() {
    // single_process_system_completes_operations().await;
    // read_test().await;
    read_test_utils().await;
    let k: Vec<String> = (0..3).map(|idx| {
        format!("somedir_{idx}").to_string()
    }).collect();

    println!("{:?}", k);

    // let mut arr = [1, 2, 3, 4, 5];

    // let (mut left, right) = arr.split_at_mut(3);

    // left = &mut left[0..2];

    // for el in left.iter_mut() {
    //     *el *= 10;
    // }

    // for el in right.iter_mut() {
    //     *el *= -10;
    // }

    // println!("{:?}", left);
    // println!("{:?}", right);
    // println!("{:?}", arr);

}

async fn send_cmd(register_cmd: &RegisterCommand, stream: &mut TcpStream, hmac_client_key: &[u8]) {
    let mut data = Vec::new();
    serialize_register_command(register_cmd, &mut data, hmac_client_key)
        .await
        .unwrap();

    stream.write_all(&data).await.unwrap();
}

fn hmac_tag_is_ok(key: &[u8], data: &[u8]) -> bool {
    let boundary = data.len() - HMAC_TAG_SIZE;
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(&data[..boundary]);
    mac.verify_slice(&data[boundary..]).is_ok()
}
