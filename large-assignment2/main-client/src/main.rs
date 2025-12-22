use assignment_2_solution::{ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, RegisterCommand, SectorVec, serialize_register_command};
use tokio::net::TcpStream;


#[tokio::main]
async fn main() {
    // RegisterCommand::
    let mut data = SectorVec(Box::new(serde_big_array::Array([0u8; 4096])));
    data.0[0] = 5;

    let rc = RegisterCommand::Client(ClientRegisterCommand { 
        header: ClientCommandHeader {
            request_identifier: 42,
            sector_idx: 12,
        },
        content: ClientRegisterCommandContent::Write {
            // data: SectorVec(Box::new(serde_big_array::Array([0u8; 4096])))
            data,
        },
    });

    let mut socket = TcpStream::connect("127.0.0.1:5001").await.unwrap();

    let hmac_client_key = [5; 32];
    serialize_register_command(&rc, &mut socket, hmac_client_key.as_ref()).await.unwrap();
}
