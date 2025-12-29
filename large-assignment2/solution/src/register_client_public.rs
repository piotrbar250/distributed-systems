use tokio::{net::TcpStream, sync::mpsc::{UnboundedReceiver, UnboundedSender}, time::sleep};

use crate::{EncodingError, RegisterCommand, SystemRegisterCommand, serialize_register_command};
use std::{sync::Arc, time::Duration};

#[async_trait::async_trait]
/// We do not need any public implementation of this trait. It is there for use
/// in `AtomicRegister`. In our opinion it is a safe bet to say some structure of
/// this kind must appear in your solution.
pub trait RegisterClient: core::marker::Send + Sync {
    /// Sends a system message to a single process.
    async fn send(&self, msg: Send);

    /// Broadcasts a system message to all processes in the system, including self.
    async fn broadcast(&self, msg: Broadcast);
}

pub struct Broadcast {
    pub cmd: Arc<SystemRegisterCommand>,
}

pub struct Send {
    pub cmd: Arc<SystemRegisterCommand>,
    /// Identifier of the target process. Those start at 1.
    pub target: u8,
}

pub async fn sender_worker(addr: String, mut internal_rx: UnboundedReceiver<Arc<SystemRegisterCommand>>, hmac_system_key: [u8; 64]) {

    let mut socket: Option<TcpStream> = None;
    let mut pending: Option<Arc<SystemRegisterCommand>> = None;

    let delay: u64 = 50;

    loop {
        if pending.is_none() {
            match internal_rx.recv().await {
                Some(cmd) => pending = Some(cmd),
                None => {
                    eprintln!("sender_worker ERROR: internal_tx dropped");
                    return;
                },
            }
        }

        if socket.is_none() {
            match TcpStream::connect(&addr).await {
                Ok(s) => socket = Some(s),
                Err(_) => {
                    sleep(Duration::from_millis(delay)).await;
                    continue;
                }
            }
        }

        let cmd = RegisterCommand::System((**pending.as_ref().unwrap()).clone());
        
        match serialize_register_command(&cmd, socket.as_mut().unwrap(), &hmac_system_key).await {
            Ok(()) => {
                pending = None;
            },
            Err(EncodingError::IoError(_)) => {
                socket = None;
                sleep(Duration::from_millis(delay)).await;
                continue;
            }
            Err(e) => {
                eprintln!("sender_worker fatal encode: {e:?}");
            },
        }
    }
}

pub struct MyRegisterClient {
    pub self_ident: u8,
    pub processes_count: u8,
    pub local_tx: UnboundedSender<Arc<SystemRegisterCommand>>,
    pub internal_tx: Vec<Option<UnboundedSender<Arc<SystemRegisterCommand>>>>,
}


#[async_trait::async_trait]
impl RegisterClient for MyRegisterClient {
    async fn send(&self, msg: Send) {
        if msg.target == self.self_ident {
            let _ = self.local_tx.send(Arc::clone(&msg.cmd));
            return;
        }

        if let Some(tx) = &self.internal_tx[msg.target as usize] {
            tx.send(msg.cmd).unwrap();
        } else {
            eprintln!("RegisterClient::send: no tx");
        }
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 1..=self.processes_count {
            self.send(Send {
                cmd: Arc::clone(&msg.cmd),
                target,
            }).await;
        }
    }
}
