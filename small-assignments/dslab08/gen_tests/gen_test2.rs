#[cfg(test)]
mod tests {
    use crate::solution::{DetectorOperation, Disable, Enable, FailureDetectorModule};
    use module_system::System;
    use bincode::config::standard;
    use bincode::serde::{decode_from_slice, encode_to_vec};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU16, Ordering};
    use tokio::net::UdpSocket;
    use tokio::time::{sleep, timeout, Duration, Instant};
    use uuid::Uuid;

    // Global port allocator so that different tests never try to bind the same port.
    static NEXT_PORT: AtomicU16 = AtomicU16::new(41000);

    fn next_port() -> u16 {
        NEXT_PORT.fetch_add(1, Ordering::Relaxed)
    }

    /// Helper: send AliveRequest to `addr` and wait for AliveInfo response.
    async fn query_alive(addr: SocketAddr) -> std::collections::HashSet<Uuid> {
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();

        // Send AliveRequest
        let msg = DetectorOperation::AliveRequest;
        let buf = encode_to_vec(&msg, standard()).unwrap();
        socket.send_to(&buf, addr).await.unwrap();

        // Receive response with timeout
        let mut recv_buf = [0u8; 4096];
        let (len, _) = timeout(
            Duration::from_millis(1_000),
            socket.recv_from(&mut recv_buf),
        )
        .await
        .expect("AliveInfo response timed out")
        .unwrap();

        let (op, _): (DetectorOperation, usize) =
            decode_from_slice(&recv_buf[..len], standard()).unwrap();

        match op {
            DetectorOperation::AliveInfo(set) => set,
            _ => panic!("Expected AliveInfo, got some other DetectorOperation"),
        }
    }

    /// Wait (with a global timeout) until `predicate()` returns true.
    async fn wait_until<F>(mut predicate: F, overall_timeout: Duration)
    where
        F: FnMut() -> tokio::task::JoinHandle<bool>,
    {
        let deadline = Instant::now() + overall_timeout;
        loop {
            if predicate().await.unwrap() {
                break;
            }
            if Instant::now() >= deadline {
                panic!("Condition not satisfied within {:?}", overall_timeout);
            }
            sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn all_processes_report_each_other_alive() {
        let delta = Duration::from_millis(100);
        let mut system = System::new().await;

        // Create two process IDs and unique addresses
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = SocketAddr::from(([127, 0, 0, 1], next_port()));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], next_port()));

        let mut addrs = HashMap::new();
        addrs.insert(id1, addr1);
        addrs.insert(id2, addr2);

        // Start modules (keep refs to avoid unused warnings)
        let _p1 = FailureDetectorModule::new(&mut system, delta, &addrs, id1).await;
        let _p2 = FailureDetectorModule::new(&mut system, delta, &addrs, id2).await;

        // Give them time to stabilize and exchange heartbeats
        wait_until(
            || {
                let addr1 = addr1;
                let addr2 = addr2;
                let id1 = id1;
                let id2 = id2;
                tokio::spawn(async move {
                    let alive_from_1 = query_alive(addr1).await;
                    let alive_from_2 = query_alive(addr2).await;
                    alive_from_1.contains(&id2) && alive_from_2.contains(&id1)
                })
            },
            Duration::from_secs(5),
        )
        .await;

        system.shutdown().await;
    }

    #[tokio::test]
    async fn disabled_process_is_eventually_suspected() {
        let delta = Duration::from_millis(100);
        let mut system = System::new().await;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = SocketAddr::from(([127, 0, 0, 1], next_port()));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], next_port()));

        let mut addrs = HashMap::new();
        addrs.insert(id1, addr1);
        addrs.insert(id2, addr2);

        // Start modules and keep refs to send Disable
        let p1 = FailureDetectorModule::new(&mut system, delta, &addrs, id1).await;
        let p2 = FailureDetectorModule::new(&mut system, delta, &addrs, id2).await;

        // Initial stabilization
        sleep(delta * 5).await;

        // Disable second process (simulate crash)
        p2.send(Disable).await;

        // Wait until process 1 stops reporting process 2 as alive
        wait_until(
            move || {
                let addr1 = addr1;
                let id2 = id2;
                tokio::spawn(async move {
                    let alive_from_1 = query_alive(addr1).await;
                    !alive_from_1.contains(&id2)
                })
            },
            Duration::from_secs(10),
        )
        .await;

        // Optional: ensure p1 itself is still alive and responsive
        let alive_from_1_final = query_alive(addr1).await;
        assert!(
            !alive_from_1_final.contains(&id2),
            "Process 1 should suspect process 2 after it stops responding"
        );

        // We don't actually use p1 here, but keeping it avoids it being dropped prematurely.
        let _ = p1;

        system.shutdown().await;
    }

    #[tokio::test]
    async fn reenabled_process_becomes_alive_again() {
        let delta = Duration::from_millis(100);
        let mut system = System::new().await;

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let addr1 = SocketAddr::from(([127, 0, 0, 1], next_port()));
        let addr2 = SocketAddr::from(([127, 0, 0, 1], next_port()));

        let mut addrs = HashMap::new();
        addrs.insert(id1, addr1);
        addrs.insert(id2, addr2);

        // Start modules
        let p1 = FailureDetectorModule::new(&mut system, delta, &addrs, id1).await;
        let p2 = FailureDetectorModule::new(&mut system, delta, &addrs, id2).await;

        // Initial stabilization
        sleep(delta * 5).await;

        // Disable 2 and wait until 1 suspects it
        p2.send(Disable).await;
        wait_until(
            move || {
                let addr1 = addr1;
                let id2 = id2;
                tokio::spawn(async move {
                    let alive_from_1 = query_alive(addr1).await;
                    !alive_from_1.contains(&id2)
                })
            },
            Duration::from_secs(10),
        )
        .await;

        // Re-enable 2
        p2.send(Enable).await;

        // Wait until 1 considers 2 alive again
        wait_until(
            move || {
                let addr1 = addr1;
                let id2 = id2;
                tokio::spawn(async move {
                    let alive_from_1 = query_alive(addr1).await;
                    alive_from_1.contains(&id2)
                })
            },
            Duration::from_secs(15),
        )
        .await;

        // Also check symmetric view: 2 should see 1 as alive
        wait_until(
            move || {
                let addr2 = addr2;
                let id1 = id1;
                tokio::spawn(async move {
                    let alive_from_2 = query_alive(addr2).await;
                    alive_from_2.contains(&id1)
                })
            },
            Duration::from_secs(15),
        )
        .await;

        // Keep refs alive
        let _ = (p1, p2);

        system.shutdown().await;
    }
}
