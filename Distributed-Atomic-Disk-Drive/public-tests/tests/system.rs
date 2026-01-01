use assignment_2_solution::{
    ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, Configuration,
    OperationReturn, PublicConfiguration, RegisterCommand, SectorVec, run_register_process,
    serialize_register_command,
};
use assignment_2_test_utils::system::*;
use assignment_2_test_utils::transfer::PacketBuilder;
use hmac::Mac;
use ntest::timeout;
use serde_big_array::Array;
use std::convert::TryInto;
use std::time::Duration;
use tempfile::tempdir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
#[timeout(4000)]
async fn single_process_system_completes_operations() {
    // given
    let hmac_client_key = [5; 32];
    let tcp_port = 30_287;
    let storage_dir = tempdir().unwrap();
    let request_identifier = 1778;

    let config = Configuration {
        public: PublicConfiguration {
            tcp_locations: vec![("127.0.0.1".to_string(), tcp_port)],
            self_rank: 1,
            n_sectors: 20,
            storage_dir: storage_dir.keep(),
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
            data: SectorVec(Box::new(Array([3; 4096]))),
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

#[tokio::test]
#[serial_test::serial]
#[timeout(30000)]
async fn concurrent_operations_on_the_same_sector() {
    // given
    let port_range_start = 21518;
    let n_clients = 16;
    let config = TestProcessesConfig::new(1, port_range_start);
    config.start().await;
    let mut streams = Vec::new();
    for _ in 0..n_clients {
        streams.push(config.connect(0).await);
    }
    // when
    for (i, stream) in streams.iter_mut().enumerate() {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: i.try_into().unwrap(),
                        sector_idx: 0,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([if i % 2 == 0 { 1 } else { 254 }; 4096]))),
                    },
                }),
                stream,
            )
            .await;
    }

    for stream in &mut streams {
        config.read_response(stream).await;
    }

    config
        .send_cmd(
            &RegisterCommand::Client(ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier: n_clients,
                    sector_idx: 0,
                },
                content: ClientRegisterCommandContent::Read,
            }),
            &mut streams[0],
        )
        .await;
    let response = config.read_response(&mut streams[0]).await;

    match response.content.op_return {
        OperationReturn::Read {
            read_data: SectorVec(sector),
        } => {
            assert!(*sector == Array([1; 4096]) || *sector == Array([254; 4096]));
        }
        _ => panic!("Expected read response"),
    }
}

#[tokio::test]
#[serial_test::serial]
#[timeout(40000)]
async fn large_number_of_operations_execute_successfully() {
    // given
    let port_range_start = 21625;
    let commands_total = 32;
    let config = TestProcessesConfig::new(3, port_range_start);
    config.start().await;
    let mut stream = config.connect(2).await;

    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Write {
                        data: SectorVec(Box::new(Array([cmd_idx as u8; 4096]))),
                    },
                }),
                &mut stream,
            )
            .await;
    }

    for _ in 0..commands_total {
        config.read_response(&mut stream).await;
    }

    // when
    for cmd_idx in 0..commands_total {
        config
            .send_cmd(
                &RegisterCommand::Client(ClientRegisterCommand {
                    header: ClientCommandHeader {
                        request_identifier: cmd_idx + 256,
                        sector_idx: cmd_idx,
                    },
                    content: ClientRegisterCommandContent::Read,
                }),
                &mut stream,
            )
            .await;
    }

    // then
    for _ in 0..commands_total {
        let response = config.read_response(&mut stream).await;
        match response.content.op_return {
            OperationReturn::Read {
                read_data: SectorVec(sector),
            } => {
                assert_eq!(
                    sector,
                    Box::new(Array(
                        [(response.content.request_identifier - 256) as u8; 4096]
                    ))
                )
            }
            _ => panic!("Expected read response"),
        }
    }
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
