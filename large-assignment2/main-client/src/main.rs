use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, Configuration,
    PublicConfiguration, RegisterCommand, SectorVec, run_register_process,
    serialize_register_command,
};
use assignment_2_test_utils::system::*;
use assignment_2_test_utils::transfer::PacketBuilder;
use hmac::Mac;
use serde_big_array::Array;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

async fn single_process_system_completes_operations() {
    // given
    let hmac_client_key = [5; 32];
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
        hmac_system_key: [1; 64],
        hmac_client_key,
    };

    tokio::spawn(run_register_process(config));

    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut stream = TcpStream::connect(("127.0.0.1", tcp_port))
        .await
        .expect("Could not connect to TCP port");

    let write_cmd = RegisterCommand::Client(ClientRegisterCommand {
        header: ClientCommandHeader {
            request_identifier,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            data: SectorVec(Box::new(Array([97; 4096]))),
        },
    });

    // when
    send_cmd(&write_cmd, &mut stream, &hmac_client_key).await;

    // then
    let mut expected = PacketBuilder::new();
    expected.add_u64(0); // size placeholder
    expected.add_u32(0); // Status ok
    expected.add_u64(request_identifier);
    expected.add_u32(1); // OperationReturn::Write
    expected.add_slice(&[0; HMAC_TAG_SIZE]); // hmac placeholder
    expected.update_size();
    let expected_len = expected.as_slice().len();

    let mut buf = vec![0u8; expected_len];
    stream
        .read_exact(&mut buf)
        .await
        .expect("Less data then expected");

    // asserts for write response
    let cmp_bytes = expected_len - HMAC_TAG_SIZE;
    assert_eq!(buf[..cmp_bytes], expected.as_slice()[..cmp_bytes]);
    assert!(hmac_tag_is_ok(&hmac_client_key, &buf[8..]));
}

#[tokio::main]
async fn main() {
    single_process_system_completes_operations().await;
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
