#[cfg(test)]
mod tests {
    use crate::solution::{DetectorOperation, Disable, FailureDetectorModule};
    use bincode::config::standard;
    use bincode::serde::{decode_from_slice, encode_to_vec};
    use module_system::System;
    use std::collections::HashMap;
    use std::net::{SocketAddr, UdpSocket as StdUdpSocket};
    use tokio::net::UdpSocket;
    use tokio::time::{sleep, Duration};
    use uuid::Uuid;

    /// Helper: get a free local UDP address (127.0.0.1:random_port).
    fn free_local_addr() -> SocketAddr {
        let sock = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = sock.local_addr().unwrap();
        // drop(sock) happens here, port becomes free
        addr
    }

    /// Helper: wait for `rounds` heartbeat intervals of length `delta`.
    async fn wait_rounds(rounds: u32, delta: Duration) {
        let total_ms = delta.as_millis() as u64 * rounds as u64;
        sleep(Duration::from_millis(total_ms)).await;
    }

    /// Basic check: the module responds to HeartbeatRequest with HeartbeatResponse(self_uuid).
    #[tokio::test]
    async fn heartbeat_request_produces_response() {
        let mut system = System::new().await;
        let delta = Duration::from_millis(50);

        let mut addresses = HashMap::new();
        let id = Uuid::new_v4();
        let addr = free_local_addr();
        addresses.insert(id, addr);

        // Start single detector.
        let _fd = FailureDetectorModule::new(&mut system, delta, &addresses, id).await;

        // Client socket.
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        // Send HeartbeatRequest.
        let request =
            encode_to_vec(DetectorOperation::HeartbeatRequest, standard()).unwrap();
        client.send_to(&request, addr).await.unwrap();

        // Receive response.
        let mut buf = [0u8; 1024];
        let (len, _peer) = client.recv_from(&mut buf).await.unwrap();

        let (operation, _): (DetectorOperation, _) =
            decode_from_slice(&buf[..len], standard()).unwrap();

        match operation {
            DetectorOperation::HeartbeatResponse(uuid) => {
                assert_eq!(uuid, id, "HeartbeatResponse should contain receiver's UUID");
            }
            _ => panic!("Expected HeartbeatResponse"),
        }

        system.shutdown().await;
    }

    /// When all processes are alive and exchanging heartbeats,
    /// AliveRequest to one detector should report the other as alive.
    #[tokio::test]
    async fn alive_request_reports_other_process_when_healthy() {
        let mut system = System::new().await;
        let delta = Duration::from_millis(50);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut addresses = HashMap::new();
        let addr1 = free_local_addr();
        let addr2 = free_local_addr();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);

        let _fd1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let _fd2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

        // Let them run a few intervals and exchange heartbeats.
        wait_rounds(5, delta).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request =
            encode_to_vec(DetectorOperation::AliveRequest, standard()).unwrap();

        // Query process 1's view of the world.
        client.send_to(&request, addr1).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, _peer) = client.recv_from(&mut buf).await.unwrap();
        let (operation, _): (DetectorOperation, _) =
            decode_from_slice(&buf[..len], standard()).unwrap();

        match operation {
            DetectorOperation::AliveInfo(alive_set) => {
                assert!(
                    alive_set.contains(&id2),
                    "Process 1 should consider process 2 alive"
                );
                // (We deliberately do NOT assert about id1; implementation may or may not include self.)
            }
            _ => panic!("Expected AliveInfo"),
        }

        system.shutdown().await;
    }

    /// If one process stops responding (Disable),
    /// another process should eventually stop reporting it as alive.
    #[tokio::test]
    async fn failed_process_is_not_reported_as_alive() {
        let mut system = System::new().await;
        let delta = Duration::from_millis(50);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut addresses = HashMap::new();
        let addr1 = free_local_addr();
        let addr2 = free_local_addr();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);

        let _fd1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let fd2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

        // First let them see each other as alive.
        wait_rounds(5, delta).await;

        // Now "crash" process 2: it will stop replying to UDP messages.
        fd2.send(Disable).await;

        // Wait long enough for at least one full interval with missing heartbeats from id2.
        wait_rounds(5, delta).await;

        // Ask process 1 which processes are alive.
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let request =
            encode_to_vec(DetectorOperation::AliveRequest, standard()).unwrap();

        client.send_to(&request, addr1).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, _peer) = client.recv_from(&mut buf).await.unwrap();
        let (operation, _): (DetectorOperation, _) =
            decode_from_slice(&buf[..len], standard()).unwrap();

        match operation {
            DetectorOperation::AliveInfo(alive_set) => {
                assert!(
                    !alive_set.contains(&id2),
                    "Process 1 should *not* consider process 2 alive after it stops responding"
                );
            }
            _ => panic!("Expected AliveInfo"),
        }

        system.shutdown().await;
    }
}
