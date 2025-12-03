use bincode::config::{standard};
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::time::Duration;
use uuid::Uuid;

use module_system::{Handler, ModuleRef, System, TimerHandle};

/// A message, which disables a process. Used for testing.
pub struct Disable;

/// A message, which enables a process. Used for testing.
pub struct Enable;

struct Init;

#[derive(Clone)]
struct Timeout;

pub struct FailureDetectorModule {
    /// This is to simulate a disabled process. Keep those checks in place.
    enabled: bool,
    timeout_handle: Option<TimerHandle>,
    delta: Duration,
    delay: Duration,
    self_ref: ModuleRef<Self>,
    addresses: HashMap<Uuid, SocketAddr>,
    alive: HashSet<Uuid>,
    suspected: HashSet<Uuid>,
    socket: Arc<UdpSocket>,
    ident: Uuid,
}

impl FailureDetectorModule {
    pub async fn new(
        system: &mut System,
        delta: Duration,
        addresses: &HashMap<Uuid, SocketAddr>,
        ident: Uuid,
    ) -> ModuleRef<Self> {
        let addr = addresses.get(&ident).unwrap();
        let socket = Arc::new(UdpSocket::bind(addr).await.unwrap());

        let mut addresses = addresses.clone();
        addresses.remove(&ident);

        let module_ref = system
            .register_module(|self_ref| Self {
                enabled: true,
                timeout_handle: None,
                delta,
                delay: delta,
                self_ref,
                addresses,
                alive: HashSet::new(),
                suspected: HashSet::new(),
                socket: socket.clone(),
                ident,
            })
            .await;

        tokio::spawn(deserialize_and_forward(socket, module_ref.clone()));

        module_ref.send(Init).await;

        module_ref
    }
}

#[async_trait::async_trait]
impl Handler<Init> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Init) {
        for key in self.addresses.keys() {
            self.alive.insert(key.clone());
        }

        self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
    }
}

/// New operation arrived at a socket.
#[async_trait::async_trait]
impl Handler<DetectorOperationUdp> for FailureDetectorModule {
    async fn handle(&mut self, msg: DetectorOperationUdp) {
        if self.enabled {
            let DetectorOperationUdp(operation, reply_addr) = msg;

            match operation {
                DetectorOperation::HeartbeatRequest => {
                    let msg = DetectorOperation::HeartbeatResponse(self.ident);
                    let buf = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
                    self.socket.send_to(&buf, reply_addr).await.unwrap();
                },
                DetectorOperation::HeartbeatResponse(reply_ident) => {
                    self.alive.insert(reply_ident.clone());
                },
                DetectorOperation::AliveRequest => {
                    let mut previous_set = HashSet::new();

                    previous_set.insert(self.ident);

                    for key in self.addresses.keys() {
                        if !self.suspected.contains(key) {
                            previous_set.insert(key.clone());
                        }
                    }

                    let msg = DetectorOperation::AliveInfo(previous_set);
                    let buf = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
                    self.socket.send_to(&buf, reply_addr).await.unwrap();
                },
                DetectorOperation::AliveInfo(_) => {}
            }

        }
    }
}

/// Called periodically to check send broadcast and update alive processes.
#[async_trait::async_trait]
impl Handler<Timeout> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Timeout) {
        if self.enabled {
            for key_sus in self.suspected.iter() {

                if self.alive.contains(key_sus) {
                    self.delay += self.delta;

                    let h = self.timeout_handle.as_ref().unwrap();
                    h.stop().await;
                    
                    self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
                    break;
                }
            }

            for key in self.addresses.keys() {
                if !self.alive.contains(key) && !self.suspected.contains(key) {
                    self.suspected.insert(key.clone());
                }
                else if self.alive.contains(key) && self.suspected.contains(key) {
                    self.suspected.remove(key);
                }

                let msg = DetectorOperation::HeartbeatRequest;
                let buf = bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap();
                self.socket.send_to(&buf, self.addresses.get(key).unwrap()).await.unwrap();
            }
            
            self.alive = HashSet::new();
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Disable) {
        self.enabled = false;
        if let Some(h) = self.timeout_handle.as_ref() {
            h.stop().await;
            self.timeout_handle = None;
        };
    }
}

#[async_trait::async_trait]
impl Handler<Enable> for FailureDetectorModule {
    async fn handle(&mut self, _msg: Enable) {
        if self.enabled {
            return;
        }
        self.enabled = true;
        self.timeout_handle = Some(self.self_ref.request_tick(Timeout, self.delay).await);
    }
}

/// Receives messages over UDP and converts them into our module system's messages
async fn deserialize_and_forward(
    socket: Arc<UdpSocket>,
    module_ref: ModuleRef<FailureDetectorModule>,
) {
    let mut buffer = vec![0];
    while let Ok((len, sender)) = socket.peek_from(&mut buffer).await {
        if len == buffer.len() {
            buffer.resize(2 * buffer.len(), 0);
        } else {
            socket.recv_from(&mut buffer).await.unwrap();
            match bincode::serde::decode_from_slice(&buffer, standard()) {
                Ok((msg, _took)) => module_ref.send(DetectorOperationUdp(msg, sender)).await,
                Err(err) => {
                    debug!("Invalid format of detector operation ({})!", err);
                }
            }
        }
    }
}

/// Received UDP message
struct DetectorOperationUdp(DetectorOperation, SocketAddr);

/// Messages that are sent over UDP
#[derive(Serialize, Deserialize)]
pub enum DetectorOperation {
    /// Request to receive a heartbeat.
    HeartbeatRequest,
    /// Response to heartbeat, contains uuid of the receiver of `HeartbeatRequest`.
    HeartbeatResponse(Uuid),
    /// Request to receive information about working processes.
    AliveRequest,
    /// Vector of processes which are alive according to `AliveRequest` receiver.
    AliveInfo(HashSet<Uuid>),
}
