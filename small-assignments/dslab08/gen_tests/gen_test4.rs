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
    use tokio::time::{sleep, Duration, Instant};
    use uuid::Uuid;

    /// Pick a free local UDP port and return its address.
    fn random_local_addr() -> SocketAddr {
        StdUdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    /// Send a single UDP DetectorOperation and wait for a single response.
    async fn send_udp_and_receive(
        dest: SocketAddr,
        operation: DetectorOperation,
    ) -> DetectorOperation {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let bytes = encode_to_vec(operation, standard()).unwrap();
        socket.send_to(&bytes, dest).await.unwrap();

        let mut buffer = vec![0u8; 2048];
        let (len, _) = socket.recv_from(&mut buffer).await.unwrap();
        buffer.truncate(len);

        let (msg, _): (DetectorOperation, usize) =
            decode_from_slice(&buffer, standard()).unwrap();
        msg
    }

    /// Query a process over UDP for its AliveInfo set.
    async fn query_alive(dest: SocketAddr) -> HashSet<Uuid> {
        match send_udp_and_receive(dest, DetectorOperation::AliveRequest).await {
            DetectorOperation::AliveInfo(set) => set,
            _ => panic!("Unexpected response to AliveRequest"),
        }
    }

    #[tokio::test]
    async fn heartbeat_and_alive_info_work() {
        let mut system = System::new().await;
        let delta = Duration::from_millis(50);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = random_local_addr();
        let addr2 = random_local_addr();

        let mut addresses = HashMap::new();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);

        let _m1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let _m2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;

        // Give the detectors time to start and go through a few intervals.
        sleep(3 * delta).await;

        // Check that a HeartbeatRequest gets a proper HeartbeatResponse with the right id.
        let resp1 = send_udp_and_receive(addr1, DetectorOperation::HeartbeatRequest).await;
        match resp1 {
            DetectorOperation::HeartbeatResponse(id) => assert_eq!(id, id1),
            _ => panic!("Expected HeartbeatResponse from process 1"),
        }

        // AliveInfo should report the other process as alive.
        let alive1 = query_alive(addr1).await;
        let alive2 = query_alive(addr2).await;

        assert!(
            alive1.contains(&id2),
            "process 1 should see process 2 as alive"
        );
        assert!(
            alive2.contains(&id1),
            "process 2 should see process 1 as alive"
        );

        system.shutdown().await;
    }

    #[tokio::test]
    async fn failing_process_is_eventually_suspected() {
        let mut system = System::new().await;
        let delta = Duration::from_millis(80);

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let addr1 = random_local_addr();
        let addr2 = random_local_addr();
        let addr3 = random_local_addr();

        let mut addresses = HashMap::new();
        addresses.insert(id1, addr1);
        addresses.insert(id2, addr2);
        addresses.insert(id3, addr3);

        let _m1 = FailureDetectorModule::new(&mut system, delta, &addresses, id1).await;
        let _m2 = FailureDetectorModule::new(&mut system, delta, &addresses, id2).await;
        let m3 = FailureDetectorModule::new(&mut system, delta, &addresses, id3).await;

        // Let everything stabilize.
        sleep(5 * delta).await;

        // Simulate failure of process 3.
        m3.send(Disable).await;

        // Wait long enough for at least one interval to complete with missing heartbeats.
        sleep(5 * delta).await;

        let alive1 = query_alive(addr1).await;
        let alive2 = query_alive(addr2).await;

        // Processes 1 and 2 should still see each other as alive.
        assert!(alive1.contains(&id2));
        assert!(alive2.contains(&id1));

        // Process 3 should be suspected by both.
        assert!(
            !alive1.contains(&id3),
            "process 1 should suspect process 3 after it stops responding"
        );
        assert!(
            !alive2.contains(&id3),
            "process 2 should suspect process 3 after it stops responding"
        );

        system.shutdown().await;
    }

    /// Helper: measure how long it takes until `monitor` stops reporting `target`
    /// in its AliveInfo set.
    async fn time_until_suspected(
        delta: Duration,
        monitor_addr: SocketAddr,
        target: Uuid,
    ) -> Duration {
        let start = Instant::now();
        loop {
            let alive = query_alive(monitor_addr).await;
            if !alive.contains(&target) {
                return start.elapsed();
            }
            sleep(delta / 4).await;
            if start.elapsed() > delta * 10 {
                panic!("detector did not suspect within expected time");
            }
        }
    }

    #[tokio::test]
    async fn interval_is_increased_after_false_suspicion() {
        let mut system = System::new().await;
        let delta = Duration::from_millis(150);

        let monitor_id = Uuid::new_v4();
        let flapping_id = Uuid::new_v4();

        let monitor_addr = random_local_addr();
        let flapping_addr = random_local_addr();

        let mut addresses = HashMap::new();
        addresses.insert(monitor_id, monitor_addr);
        addresses.insert(flapping_id, flapping_addr);

        let _monitor =
            FailureDetectorModule::new(&mut system, delta, &addresses, monitor_id).await;
        let flapping =
            FailureDetectorModule::new(&mut system, delta, &addresses, flapping_id).await;

        // Stabilize.
        sleep(5 * delta).await;

        // 1st failure: measure how quickly the monitor suspects the flapping process.
        flapping.send(Disable).await;
        let t1 = time_until_suspected(delta, monitor_addr, flapping_id).await;

        // Bring the process back: this should correct the suspicion and cause
        // the detector to increase its timeout by delta.
        flapping.send(Enable).await;
        // Wait enough time for at least one full interval at the *new* timeout.
        sleep(delta * 6).await;

        // 2nd failure: measure suspicion time again; it should be noticeably longer.
        flapping.send(Disable).await;
        let t2 = time_until_suspected(delta, monitor_addr, flapping_id).await;

        assert!(
            t2 > t1 + delta / 2,
            "second suspicion {:?} was not significantly later than first {:?}",
            t2,
            t1
        );

        system.shutdown().await;
    }
}
