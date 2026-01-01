use assignment_2_solution::{
    ClientCommandHeader, ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, Configuration, OperationReturn, PublicConfiguration, RegisterCommand, SectorVec, run_register_process, serialize_register_command
};
use serde_big_array::Array;
use tokio::fs;
use std::path::{PathBuf};
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use shared::TestProcessesConfigManual;

// manual communication
async fn _read_test() {
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
    
    serialize_register_command(&cmd, &mut socket, &hmac_client_key).await.unwrap();
    
    let mut len_buf = [0u8; 8];
    socket.read_exact(&mut len_buf).await.unwrap();
    let msg_len = u64::from_be_bytes(len_buf) as usize;

    let mut frame = vec![0u8; msg_len];
    socket.read_exact(&mut frame).await.unwrap();

    let (payload, _tag) = frame.split_at(msg_len - 32);

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

// communication via test-utils
async fn read_test_utils() {
    let port_range_start = 6000;
    let processes_count = 3;
    let config = TestProcessesConfigManual::new(processes_count, port_range_start);

    let mut socket0 = config.connect(0).await;
    let mut socket1 = config.connect(2).await;

    config.send_cmd(
        &RegisterCommand::Client(ClientRegisterCommand {
            header: ClientCommandHeader {
                request_identifier: 1,
                sector_idx: 0,
            },
            content: ClientRegisterCommandContent::Write {
                data: SectorVec(Box::new(Array([111; 4096]))),
            },
        }),
        &mut socket0,
    ).await;

    _ = config.read_response(&mut socket0).await;

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: 1,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut socket1
        )
        .await;
    let response = config.read_response(&mut socket1).await;

    fs::write("log.txt", format!("{:?}", response.content)).await.unwrap();
}

#[tokio::main]
async fn main() {
    read_test_utils().await;
}