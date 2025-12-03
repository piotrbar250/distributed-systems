#[cfg(test)]
mod tests {
    use crate::solution::{DetectorOperation, Disable, Enable, FailureDetectorModule};
    use module_system::System;
    use bincode::config::standard;
    use bincode::serde::{decode_from_slice, encode_to_vec};
    use std::collections::{HashMap, HashSet};
    use std::net::UdpSocket as StdUdpSocket;
    use std::net::SocketAddr;
    use tokio::net::UdpSocket;
    use tokio::time::{sleep, Duration};
    use uuid::Uuid;

    /// Helper: grab a free localhost UDP port for a detector instance.
    fn free_local_addr() -> SocketAddr {
        let sock = StdUdpSocket::bind("127.0.0.1:0").expect("bind ephemeral UDP socket");
        sock.local_addr().expect("local addr")
    }

    /// Helper: send AliveRequest to `addr` and return the set from AliveInfo.
    async fn query_alive(addr: SocketAddr) -> HashSet<Uuid> {
        let socket = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("bind client UDP socket");

        let req = DetectorOperation::AliveRequest;
        let bytes =
            encode_to_vec(&req, standard()).expect("encode AliveRequest with bincode");

        socket
            .send_to(&bytes, addr)
            .await
            .expect("send AliveRequest");

        let mut buf = vec![0u8; 4096];
        let (len, _from) = socket.recv_from(&mut buf).await.expect("recv AliveInfo");

        let (op, _): (DetectorOperation, usize) =
            decode_from_slice(&buf[..len], standard()).expect("decode AliveInfo");

        match op {
            DetectorOperation::AliveInfo(set) => set,
            _ => panic!("unexpected response to AliveRequest"),
        }
    }

    /// Spawns a 2-process system and verifies that each process eventually
    /// considers the other one alive (i.e., appears in AliveInfo).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn processes_eventually_report_each_other_as_alive() {
        let mut system = System::new().await;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = free_local_addr();
        let addr2 = free_local_addr();

        let mut addresses = HashMap::new();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);

        let delta = Duration::from_millis(50);

        // Start both failure detectors
        let _p1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let _p2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

        // Let a few heartbeat intervals pass so they can talk to each other.
        sleep(delta * 10).await;

        let alive_from_1 = query_alive(addr1).await;
        assert!(
            alive_from_1.contains(&id2),
            "process 1 should see process 2 as alive, got {:?}",
            alive_from_1
        );

        let alive_from_2 = query_alive(addr2).await;
        assert!(
            alive_from_2.contains(&id1),
            "process 2 should see process 1 as alive, got {:?}",
            alive_from_2
        );

        system.shutdown().await;
    }

    /// Verifies that when a process stops responding (Disable),
    /// other processes eventually stop reporting it in AliveInfo (suspect it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disabled_process_is_eventually_suspected() {
        let mut system = System::new().await;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = free_local_addr();
        let addr2 = free_local_addr();

        let mut addresses = HashMap::new();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);

        let delta = Duration::from_millis(50);

        let _p1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let p2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

        // Let the system stabilize first.
        sleep(delta * 10).await;

        // Sanity check: initially, p1 should see p2 as alive.
        let alive_before = query_alive(addr1).await;
        assert!(
            alive_before.contains(&id2),
            "precondition failed: p1 should initially see p2 as alive, got {:?}",
            alive_before
        );

        // Now disable p2: it should stop replying to heartbeats.
        p2.send(Disable).await;

        // Wait long enough for at least one full interval where p2 does not respond.
        sleep(delta * 10).await;

        let alive_after = query_alive(addr1).await;
        assert!(
            !alive_after.contains(&id2),
            "p1 should eventually suspect p2 (no longer list it as alive), got {:?}",
            alive_after
        );

        system.shutdown().await;
    }

    /// Verifies that when a previously suspected process comes back (Enable),
    /// other processes eventually consider it alive again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reenabled_process_becomes_alive_again() {
        let mut system = System::new().await;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = free_local_addr();
        let addr2 = free_local_addr();

        let mut addresses = HashMap::new();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);

        let delta = Duration::from_millis(50);

        let _p1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let p2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

        // Let the system stabilize.
        sleep(delta * 10).await;

        // Disable p2 and wait until p1 stops listing it as alive.
        p2.send(Disable).await;
        sleep(delta * 10).await;

        let alive_after_disable = query_alive(addr1).await;
        assert!(
            !alive_after_disable.contains(&id2),
            "p1 should suspect p2 after it is disabled, got {:?}",
            alive_after_disable
        );

        // Bring p2 back.
        p2.send(Enable).await;

        // Allow enough time/intervals for p1 to receive fresh heartbeats from p2.
        sleep(delta * 10).await;

        let alive_after_enable = query_alive(addr1).await;
        assert!(
            alive_after_enable.contains(&id2),
            "p1 should consider p2 alive again after re-enable, got {:?}",
            alive_after_enable
        );

        system.shutdown().await;
    }
}


// #[cfg(test)]
// mod tests {
//     use crate::solution::{DetectorOperation, Disable, Enable, FailureDetectorModule};
//     use module_system::{ModuleRef, System};
//     use bincode::config::standard;
//     use bincode::serde::{decode_from_slice, encode_to_vec};
//     use std::collections::{HashMap, HashSet};
//     use std::net::{Ipv4Addr, UdpSocket as StdUdpSocket};
//     use std::net::SocketAddr;
//     use tokio::net::UdpSocket;
//     use tokio::time::{sleep, Duration};
//     use uuid::Uuid;
    
//     /// Bind to a random free UDP port on localhost and return its address.
//     async fn random_local_addr() -> SocketAddr {
//         let sock = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16))
//             .await
//             .expect("failed to bind temporary UDP socket");
//         let addr = sock.local_addr().unwrap();
//         drop(sock);
//         addr
//     }

//     /// Send an AliveRequest to `target` and wait for an AliveInfo response.
//     async fn send_alive_request(target: SocketAddr) -> HashSet<Uuid> {
//         let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0u16))
//             .await
//             .expect("failed to bind client UDP socket");

//         let payload = bincode::serde::encode_to_vec(
//             DetectorOperation::AliveRequest,
//             standard(),
//         )
//         .expect("bincode encode failed");

//         client
//             .send_to(&payload, target)
//             .await
//             .expect("failed to send AliveRequest");

//         let mut buf = vec![0u8; 2048];

//         loop {
//             let (len, _from) = client
//                 .recv_from(&mut buf)
//                 .await
//                 .expect("failed to receive AliveInfo");
//             let slice = &buf[..len];

//             if let Ok((DetectorOperation::AliveInfo(set), _)) =
//                 bincode::serde::decode_from_slice::<DetectorOperation>(slice, standard())
//             {
//                 return set;
//             }
//             // If it's some other message (shouldn't happen), keep listening.
//         }
//     }

//     /// Helper: create a system with two nodes registered and running.
//     async fn setup_two_nodes(
//         delta: Duration,
//     ) -> (
//         System,
//         ModuleRef<FailureDetectorModule>,
//         ModuleRef<FailureDetectorModule>,
//         Uuid,
//         Uuid,
//         SocketAddr,
//         SocketAddr,
//     ) {
//         let mut system = System::new().await;

//         let id1 = Uuid::new_v4();
//         let id2 = Uuid::new_v4();
//         let addr1 = random_local_addr().await;
//         let addr2 = random_local_addr().await;

//         let mut addresses = HashMap::new();
//         addresses.insert(id1, addr1);
//         addresses.insert(id2, addr2);

//         let n1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
//         let n2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

//         (system, n1, n2, id1, id2, addr1, addr2)
//     }

//     #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
//     async fn nodes_eventually_see_each_other_as_alive() {
//         let delta = Duration::from_millis(50);
//         let (mut system, _n1, _n2, id1, id2, addr1, addr2) =
//             setup_two_nodes(delta).await;

//         // Give the detector some time to exchange heartbeats and finish at least one interval.
//         sleep(delta * 6).await;

//         let alive_from_1 = send_alive_request(addr1).await;
//         let alive_from_2 = send_alive_request(addr2).await;

//         assert!(
//             alive_from_1.contains(&id2),
//             "node1 should eventually report node2 as alive"
//         );
//         assert!(
//             alive_from_2.contains(&id1),
//             "node2 should eventually report node1 as alive"
//         );

//         system.shutdown().await;
//     }

//     #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
//     async fn disabled_node_is_eventually_suspected() {
//         let delta = Duration::from_millis(50);
//         let (mut system, _n1, n2, _id1, id2, addr1, _addr2) =
//             setup_two_nodes(delta).await;

//         // Warm up so nodes have time to see each other alive.
//         sleep(delta * 4).await;

//         // Disable node2; it should stop responding to heartbeats.
//         n2.send(Disable).await;

//         // Wait long enough for node1 to miss several heartbeats from node2.
//         sleep(delta * 10).await;

//         let alive_from_1 = send_alive_request(addr1).await;

//         assert!(
//             !alive_from_1.contains(&id2),
//             "node1 should eventually suspect disabled node2 (not include it in AliveInfo)"
//         );

//         system.shutdown().await;
//     }

//     #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
//     async fn reenabled_node_becomes_alive_again() {
//         let delta = Duration::from_millis(50);
//         let (mut system, _n1, n2, _id1, id2, addr1, _addr2) =
//             setup_two_nodes(delta).await;

//         // Initial warm up.
//         sleep(delta * 4).await;

//         // Disable node2 and wait until it's suspected.
//         n2.send(Disable).await;
//         sleep(delta * 8).await;
//         let alive_after_disable = send_alive_request(addr1).await;
//         assert!(
//             !alive_after_disable.contains(&id2),
//             "sanity: node2 should be suspected after disable"
//         );

//         // Re-enable node2 and give time for heartbeats again.
//         n2.send(Enable).await;
//         sleep(delta * 10).await;

//         let alive_after_enable = send_alive_request(addr1).await;
//         assert!(
//             alive_after_enable.contains(&id2),
//             "node2 should be considered alive again after re-enable"
//         );

//         system.shutdown().await;
//     }
// }
