mod public_test;
mod gen_tests;
mod solution;

use crate::solution::{DetectorOperation, FailureDetectorModule};
use bincode::config::standard;
use std::collections::{HashMap, HashSet};
use std::net::{Ipv4Addr, SocketAddr};
use tokio::net::UdpSocket;
use tokio::time::{Duration, sleep};
use uuid::Uuid;

use module_system::System;

fn unwrap_alive_info(alive_info: DetectorOperation) -> HashSet<Uuid> {
    match alive_info {
        DetectorOperation::AliveInfo(alive) => alive,
        _ => panic!("invalid type"),
    }
}

#[tokio::main]
async fn main() {
    let mut system = System::new().await;
    let delay = Duration::from_millis(50);
    let addr1 = (
        Uuid::new_v4(),
        SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9121),
    );
    let addr2 = (
        Uuid::new_v4(),
        SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9122),
    );
    let addresses: HashMap<Uuid, SocketAddr> = [addr1, addr2].iter().copied().collect();

    let _detector_1 = FailureDetectorModule::new(&mut system, delay, &addresses, addr1.0).await;
    let _detector_2 = FailureDetectorModule::new(&mut system, delay, &addresses, addr2.0).await;

    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();

    sleep(delay * 3).await;

    let mut buf = [0; 512];

    socket
        .send_to(
            bincode::serde::encode_to_vec(&DetectorOperation::AliveRequest, standard())
                .unwrap()
                .as_slice(),
            &addr1.1,
        )
        .await
        .expect("cannot send?");

    let len = socket.recv(&mut buf).await.unwrap();
    let alive_info = unwrap_alive_info(
        bincode::serde::decode_from_slice(&buf[..len], standard())
            .expect("Invalid format of alive info!")
            .0,
    );
    println!("Alive according to the first process: {alive_info:?}");

    sleep(delay).await;

    socket
        .send_to(
            bincode::serde::encode_to_vec(&DetectorOperation::AliveRequest, standard())
                .unwrap()
                .as_slice(),
            &addr2.1,
        )
        .await
        .expect("cannot send?");

    let len = socket.recv(&mut buf).await.unwrap();
    let alive_info = unwrap_alive_info(
        bincode::serde::decode_from_slice(&buf[..len], standard())
            .expect("Invalid format of alive info!")
            .0,
    );
    println!("Alive according to the second process: {alive_info:?}");

    system.shutdown().await;
}
