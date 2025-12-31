use tokio::{net::TcpStream, select, sync::{Mutex, mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}}, time::{interval, sleep}};
use uuid::Uuid;

use crate::{EncodingError, RegisterCommand, SystemRegisterCommand, SystemRegisterCommandContent, serialize_register_command};
use std::{collections::HashMap, sync::Arc, thread::scope, time::Duration};

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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Kind {ReadProc, WriteProc, Value, Ack}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Key {
    pub sector: u64,
    pub op: Uuid,
    pub kind: Kind,
}

pub enum LinkOperation {
    UpsertOnce {
        key: Key,
        cmd: Arc<SystemRegisterCommand>,
    },
    UpsertStubborn {
        key: Key,
        cmd: Arc<SystemRegisterCommand>,
    },
    RemoveStubborn {
        key: Key,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum LinkAction {
    Once,
    StubbornImmediate,
    StubbornAll,
    None,
}

pub async fn sender_worker(addr: String, mut internal_rx: UnboundedReceiver<LinkOperation>, hmac_system_key: [u8; 64]) {

    let mut socket: Option<TcpStream> = None;

    let delay: u64 = 50;

    let mut once: HashMap<Key, Arc<SystemRegisterCommand>> = HashMap::new();
    let mut stubborn: HashMap<Key, Arc<SystemRegisterCommand>> = HashMap::new();

    let mut tick = interval(Duration::from_millis(150));
    
    loop {
        let mut single_send: Option<Arc<SystemRegisterCommand>> = None;
        let mut action: LinkAction = LinkAction::None;

        select! {
            maybe = internal_rx.recv() => {
                match maybe {
                    Some(op) => {
                        match op {
                            LinkOperation::UpsertOnce { key, cmd } => {
                                once.insert(key, cmd);
                                action = LinkAction::Once;
                            },
                            LinkOperation::UpsertStubborn { key, cmd } => {
                                single_send = Some(Arc::clone(&cmd));
                                stubborn.insert(key, cmd);
                                action = LinkAction::StubbornImmediate;
                            },
                            LinkOperation::RemoveStubborn { key } => {
                                stubborn.remove(&key);
                                action = LinkAction::None
                            },
                        }
                    },
                    None => {
                        eprintln!("sender_worker ERROR: internal_tx dropped");
                        return;
                    },
                }
            }

            _ = tick.tick() => {
                if !once.is_empty() || !stubborn.is_empty() {
                    action = LinkAction::StubbornAll;
                }
            }
        }

        if action == LinkAction::None {
            continue;
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

        if action == LinkAction::StubbornImmediate {
            if let Some(cmd) = single_send {
                if !try_send(&mut socket, cmd, &hmac_system_key).await {
                    continue;
                }
            }
        }

        let once_keys: Vec<Key> = once.keys().cloned().collect();
        let mut broken_pipe = false;


        for key in once_keys {
            let Some(sys_cmd) = once.get(&key).cloned() else { 
                continue; 
            };

            if try_send(&mut socket, sys_cmd, &hmac_system_key).await {
                once.remove(&key);
            } else {
                broken_pipe = true;
                break;
            }
        }

        if broken_pipe {
            continue;
        }

        if action == LinkAction::StubbornAll {
            let stubborn_cmds: Vec<Arc<SystemRegisterCommand>> = stubborn.values().cloned().collect();

            for sys_cmd in stubborn_cmds {
                if !try_send(&mut socket, sys_cmd, &hmac_system_key).await {
                    break;
                }
            }
        }
    }
}

async fn try_send(socket: &mut Option<TcpStream>, sys_cmd: Arc<SystemRegisterCommand>, hmac_system_key: &[u8; 64]) -> bool{
    let cmd = RegisterCommand::System((*sys_cmd).clone());

    match serialize_register_command(&cmd, socket.as_mut().unwrap(), hmac_system_key).await {
        Ok(()) => {true},
        Err(EncodingError::IoError(_)) => {
            *socket = None;
            false
        }
        Err(e) => {
            eprintln!("sender_worker fatal encode: {e:?}");
            panic!();
        },
    }
}
pub struct MyRegisterClient {
    pub self_ident: u8,
    pub processes_count: u8,
    pub local_tx: UnboundedSender<Arc<SystemRegisterCommand>>,
    pub internal_tx: Vec<Option<UnboundedSender<LinkOperation>>>,
    pub key_tx: UnboundedSender<Key>,
}

pub async fn build_register_client(
    self_rank: u8,
    n: u8,
    local_tx: UnboundedSender<Arc<SystemRegisterCommand>>,
    internal_tx:Vec<Option<UnboundedSender<LinkOperation>>>,
) -> Arc<dyn RegisterClient> {
    
    let (key_tx, key_rx) = unbounded_channel();
    let register_client: Arc<MyRegisterClient> = Arc::new(MyRegisterClient {
        self_ident: self_rank,
        processes_count: n,
        local_tx,
        internal_tx,
        key_tx,
    });

    tokio::spawn(canceller_task(Arc::clone(&register_client), key_rx));
    register_client
}

fn kind_from_cmd(cmd: &SystemRegisterCommand) -> Kind {
    match cmd.content {
        SystemRegisterCommandContent::ReadProc => Kind::ReadProc,
        SystemRegisterCommandContent::WriteProc { .. } => Kind::WriteProc,
        SystemRegisterCommandContent::Value { .. } => Kind::Value,
        SystemRegisterCommandContent::Ack => Kind::Ack,
    }
}

pub async fn canceller_task(
    register_client: Arc<MyRegisterClient>,
    mut rx: UnboundedReceiver<Key>,
) {

    while let Some(key) = rx.recv().await {
        for target in 1..=register_client.processes_count {
            if target == register_client.self_ident {
                continue;
            }

            let Some(tx) = &register_client.internal_tx[target as usize] else {
                eprintln!("RegisterClient::send: missing target {}", target);
                return;
            };

            let op = LinkOperation::RemoveStubborn { key: key.clone() };
            let _ = tx.send(op);
        } 
    }
}

#[async_trait::async_trait]
impl RegisterClient for MyRegisterClient {
    async fn send(&self, msg: Send) {
        if msg.target == self.self_ident {
            let _ = self.local_tx.send(Arc::clone(&msg.cmd));
            return;
        }

        let Some(tx) = &self.internal_tx[msg.target as usize] else {
            eprintln!("RegisterClient::send: missing target {}", msg.target);
            return;
        };

        let key = Key {
            sector: msg.cmd.header.sector_idx,
            op: msg.cmd.header.msg_ident,
            kind: kind_from_cmd(&msg.cmd),
        };

        let op = match key.kind {
            Kind::ReadProc | Kind::WriteProc => LinkOperation::UpsertStubborn { key, cmd: msg.cmd },
            Kind::Value | Kind::Ack => LinkOperation::UpsertOnce { key, cmd: msg.cmd },
        };

        let _ = tx.send(op);
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
