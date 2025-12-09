use std::net::UdpSocket;
use std::io;

fn main() {

    let socket = UdpSocket::bind("127.0.0.1:8080").unwrap();
    println!("UDP server listening on 127.0.0.1:8080");

    let mut buf = [0u8; 30];

    loop {
        let (len, src_addr) = socket.recv_from(&mut buf).unwrap();
        println!(
            "Received {} bytes from {}: {:?}",
            len,
            src_addr,
            String::from_utf8_lossy(&buf[..len])
        );

        socket.send_to(b"gone?", src_addr).unwrap();
    }
}
