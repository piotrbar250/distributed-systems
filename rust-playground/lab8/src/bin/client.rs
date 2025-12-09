use std::net::UdpSocket;

fn main() {
    let socket = UdpSocket::bind("127.0.0.1:0").unwrap();

    socket.connect("127.0.0.1:8080").unwrap();

    let msg = b"Hello via UDP";
    socket.send(msg).unwrap();

    let mut buf= [0u8; 3];
    socket.recv(&mut buf).unwrap();

    println!("buf: {}", String::from_utf8_lossy(&buf));
}