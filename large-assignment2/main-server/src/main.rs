use assignment_2_solution::
    deserialize_register_command
;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};


async fn handle_client(mut socket: TcpStream, _client_addr: SocketAddr) {
    loop {
        println!("paszlo");
        let hmac_client_key = [0u8; 32];
        let hmac_system_key = [0u8; 64];
        let rc = deserialize_register_command(&mut socket, &hmac_system_key, &hmac_client_key).await.unwrap();
        println!("{:?}",rc);
    }
}

async fn run_server(addr: String, port: u16) {
    let bind_addr: String = format!{"{addr}:{port}"};
    println!("{bind_addr}");

    let listener = TcpListener::bind(bind_addr).await.unwrap();

    loop {
        let (socket, client_addr) = listener.accept().await.unwrap();
        tokio::spawn(handle_client(socket, client_addr));
    }
}

#[tokio::main]
async fn main() {
    println!("Hello, world!");
    run_server("127.0.0.1".to_string(), 5001).await;
    // tokio::spawn(run_server("127.0.0.1".to_string(), 5001));
    // tokio::time::sleep(Duration::from_millis(1)).await;


    
}
