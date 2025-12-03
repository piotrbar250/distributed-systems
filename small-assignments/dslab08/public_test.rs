#[cfg(test)]
mod tests {
    use crate::solution::{DetectorOperation, FailureDetectorModule};
    use crate::unwrap_alive_info;
    use bincode::config::standard;
    use module_system::System;
    use ntest::timeout;
    use std::net::SocketAddr;
    use tokio::net::UdpSocket;
    use tokio::time::Duration;
    use uuid::Uuid;

    #[tokio::test]
    #[timeout(300)]
    async fn data_on_wire_should_parse_with_bincode_for_single_node() {
        let mut system = System::new().await;

        let delay = Duration::from_millis(20);
        let (ident, addr): (Uuid, SocketAddr) =
            (Uuid::new_v4(), "127.0.0.1:17844".parse().unwrap());
        let addresses = [(ident, addr)].iter().copied().collect();

        let send_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let sock = UdpSocket::bind(send_addr).await.unwrap();
        let _detector = FailureDetectorModule::new(&mut system, delay, &addresses, ident).await;
        assert_eq!(
            sock.send_to(
                bincode::serde::encode_to_vec(&DetectorOperation::AliveRequest, standard())
                    .unwrap()
                    .as_slice(),
                addr,
            )
            .await
            .unwrap(),
            1
        );

        let mut buf = [0; 256];
        let len = sock.recv(&mut buf).await.unwrap();
        let alive_info = unwrap_alive_info(
            bincode::serde::decode_from_slice(&buf[..len], standard())
                .unwrap()
                .0,
        );

        assert_eq!(alive_info.len(), 1);
        assert_eq!(alive_info.iter().next().unwrap(), &ident);

        system.shutdown().await;
    }
}
