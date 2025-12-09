use uuid::Uuid;
use std::net::{Ipv4Addr, SocketAddr};
fn main() {

   let addr1 = (
        Uuid::new_v4(),
        SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 9121),
    );

    let input: (f32, (i32, i32)) = (
        5.0,
        (10, 22)
    );

    let mut buffer = [0u8; 30];

    let len = bincode::encode_into_slice(input, &mut buffer, bincode::config::standard()).unwrap();

    println!("{:?}", buffer);

    let decoded: (f32, (i32, i32)) = bincode::decode_from_slice(&buffer, bincode::config::standard()).unwrap().0;

    println!("{:?}", decoded);
}