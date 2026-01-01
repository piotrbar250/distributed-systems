use tokio::sync::{Mutex, Notify};
use tokio::time::interval;
use uuid::Uuid;

use crate::{
    Broadcast, ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, RegisterClient, SectorIdx, SectorVec, SectorsManager, Send, SuccessCb, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent
};
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

#[async_trait::async_trait]
pub trait AtomicRegister: core::marker::Send + Sync {
    /// Handle a client command. After the command is completed, we expect
    /// callback to be called. Note that completion of client command happens after
    /// delivery of multiple system commands to the register, as the algorithm specifies.
    ///
    /// This function corresponds to the handlers of Read and Write events in the
    /// (N,N)-AtomicRegister algorithm.
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>>
                + core::marker::Send
                + Sync,
        >,
    );

    /// Handle a system command.
    ///
    /// This function corresponds to the handlers of `SystemRegisterCommand` messages in the (N,N)-AtomicRegister algorithm.
    async fn system_command(&mut self, cmd: SystemRegisterCommand);
}

/// Idents are numbered starting at 1 (up to the number of processes in the system).
/// Communication with other processes of the system is to be done by `register_client`.
/// And sectors must be stored in the `sectors_manager` instance.
///
/// This function corresponds to the handlers of Init and Recovery events in the
/// (N,N)-AtomicRegister algorithm.
pub async fn build_atomic_register(
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,
) -> Box<dyn AtomicRegister> {

    let (ts, wr) = sectors_manager.read_metadata(sector_idx).await;
    let val = sectors_manager.read_data(sector_idx).await;

    let resend_state = Arc::new(Mutex::new(None));
    let resend_notify = Arc::new(Notify::new());

    tokio::spawn(stubborn_resender(
        Arc::clone(&register_client),
        processes_count,
        Arc::clone(&resend_state),
        Arc::clone(&resend_notify),
    ));

    Box::new(MyAtomicRegister {
        self_ident,
        sector_idx,
        processes_count,
        register_client,
        sectors_manager,
        ts,
        wr,
        val,
        op_id: None,
        mode: Mode::Idle,
        phase: Phase::Query,
        max_value_seen: None,
        value_from: HashSet::new(),
        ack_from: HashSet::new(),
        writeval: None,
        pending_req_id: None,
        pending_callback: None,
        resend_state,
        resend_notify
    })
}

struct ResendState {
    cmd: Arc<SystemRegisterCommand>,
    missing: Vec<bool>,
}

async fn stubborn_resender(
    register_client: Arc<dyn RegisterClient>,
    processes_count: u8,
    state: Arc<Mutex<Option<ResendState>>>,
    notify: Arc<Notify>,
) {
    loop {
        loop {
            if state.lock().await.is_some() {
                break;
            }
            notify.notified().await;
        }

        let mut tick = interval(Duration::from_millis(150));

        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = notify.notified() => {}
            }

            let (cmd, targets): (Arc<SystemRegisterCommand>, Vec<u8>) = {
                let g = state.lock().await;
                let Some(st) = g.as_ref() else {
                    break;
                };

                let mut targets = Vec::new();
                for t in 1..=processes_count {
                    if st.missing.get(t as usize).copied().unwrap_or(false) {
                        targets.push(t);
                    }
                }

                (Arc::clone(&st.cmd), targets)
            };

            for target in targets {
                register_client
                    .send(Send { cmd: Arc::clone(&cmd), target })
                    .await;
            }
        }
    }
}

enum Mode { Idle, Reading, Writing }
enum Phase { Query, WriteBack }

pub struct MyAtomicRegister {
    self_ident: u8,
    sector_idx: SectorIdx, 
    processes_count: u8,
    register_client: Arc<dyn RegisterClient>, 
    sectors_manager: Arc<dyn SectorsManager>,
    ts: u64,
    wr: u8,
    val: SectorVec,
    op_id: Option<Uuid>,
    mode: Mode,
    phase: Phase,
    max_value_seen: Option<(u64, u8, SectorVec)>,
    value_from: HashSet<u8>,
    ack_from: HashSet<u8>,
    writeval: Option<SectorVec>,
    pending_req_id: Option<u64>,
    pending_callback: Option<SuccessCb>,
    resend_state: Arc<Mutex<Option<ResendState>>>,
    resend_notify: Arc<Notify>,
}

impl MyAtomicRegister {
    fn update_max_value(&mut self, new_value: Option<(u64, u8, SectorVec)>) {

        let Some((ts, wr, value)) = new_value else { return };

        let should_replace = match self.max_value_seen.as_ref() {
            None => true,
            Some((mts, mwr, _)) => ts > *mts || (ts == *mts && wr > *mwr),
        };

        if should_replace {
            self.max_value_seen = Some((ts, wr, value));
        }
    }
}

#[async_trait::async_trait]
impl AtomicRegister for MyAtomicRegister {
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: SuccessCb,
    ) {

        let op_id = Uuid::new_v4();
        self.op_id = Some(op_id);
        self.pending_req_id = Some(cmd.header.request_identifier);
        self.pending_callback = Some(success_callback);

        self.phase = Phase::Query;
        self.value_from.clear();
        self.ack_from.clear();

        self.max_value_seen = Some((self.ts, self.wr, self.val.clone()));

        match cmd.content {
            ClientRegisterCommandContent::Read => {
                self.mode = Mode::Reading;
                self.writeval = None;
            },

            ClientRegisterCommandContent::Write { data } => {
                self.mode = Mode::Writing;
                self.writeval = Some(data);
            }
        }

        let msg = Arc::new(SystemRegisterCommand { 
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: op_id,
                sector_idx: self.sector_idx,
            }, 
            content: SystemRegisterCommandContent::ReadProc
        });

        let mut missing = vec![false; (self.processes_count as usize) + 1];
        for r in 1..=self.processes_count {
            missing[r as usize] = true;
        }

        {
            let mut g = self.resend_state.lock().await;
            *g = Some(ResendState {
                cmd: Arc::clone(&msg),
                missing,
            });
        }
        self.resend_notify.notify_one();

        self.register_client.broadcast(Broadcast {
            cmd: msg
        }).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let majority = (self.processes_count as usize / 2) + 1;

        match cmd.content {
            SystemRegisterCommandContent::ReadProc => {
                let reply = SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: self.sector_idx,
                    },
                    content: SystemRegisterCommandContent::Value {
                        timestamp: self.ts,
                        write_rank: self.wr,
                        sector_data: self.val.clone(),
                    },
                };

                self.register_client
                    .send(Send {
                        cmd: Arc::new(reply),
                        target: cmd.header.process_identifier,
                    })
                    .await;
            }

            SystemRegisterCommandContent::Value {
                timestamp,
                write_rank,
                sector_data,
            } => {
                let Some(op_id) = self.op_id else { return; };
                if op_id != cmd.header.msg_ident { return; }
                if !matches!(self.phase, Phase::Query) { return; }
                if matches!(self.mode, Mode::Idle) { return; }

                if self.value_from.contains(&cmd.header.process_identifier) {
                    return;
                }
                self.value_from.insert(cmd.header.process_identifier);
                self.update_max_value(Some((timestamp, write_rank, sector_data)));

                {
                    let mut g = self.resend_state.lock().await;
                    if let Some(st) = g.as_mut() {
                        let src = cmd.header.process_identifier as usize;
                        if src < st.missing.len() {
                            st.missing[src] = false;
                        }
                    }
                }

                if self.value_from.len() >= majority {
                    self.phase = Phase::WriteBack;
                    self.ack_from.clear();

                    match self.mode {
                        Mode::Reading => {
                            let (ts, wr, val) = self.max_value_seen.as_ref().unwrap().clone();

                            let msg = Arc::new(SystemRegisterCommand {
                                header: SystemCommandHeader {
                                    process_identifier: self.self_ident,
                                    msg_ident: op_id,
                                    sector_idx: self.sector_idx,
                                },
                                content: SystemRegisterCommandContent::WriteProc {
                                    timestamp: ts,
                                    write_rank: wr,
                                    data_to_write: val,
                                },
                            });

                            let mut missing = vec![false; (self.processes_count as usize) + 1];
                            for r in 1..=self.processes_count {
                                missing[r as usize] = true;
                            }
                            {
                                let mut g = self.resend_state.lock().await;
                                *g = Some(ResendState {
                                    cmd: Arc::clone(&msg),
                                    missing,
                                });
                            }
                            self.resend_notify.notify_one();

                            self.register_client
                                .broadcast(Broadcast { cmd: msg })
                                .await;
                        }

                        Mode::Writing => {
                            let max_ts = self.max_value_seen.as_ref().unwrap().0;

                            let new_ts = max_ts + 1;
                            let new_wr = self.self_ident;
                            let new_val = self.writeval.as_ref().unwrap().clone();

                            self.ts = new_ts;
                            self.wr = new_wr;
                            self.val = new_val.clone();

                            self.sectors_manager
                                .write(self.sector_idx, &(new_val.clone(), new_ts, new_wr))
                                .await;

                            let msg = Arc::new(SystemRegisterCommand {
                                header: SystemCommandHeader {
                                    process_identifier: self.self_ident,
                                    msg_ident: op_id,
                                    sector_idx: self.sector_idx,
                                },
                                content: SystemRegisterCommandContent::WriteProc {
                                    timestamp: new_ts,
                                    write_rank: new_wr,
                                    data_to_write: new_val,
                                },
                            });

                            let mut missing = vec![false; (self.processes_count as usize) + 1];
                            for r in 1..=self.processes_count {
                                missing[r as usize] = true;
                            }
                            {
                                let mut g = self.resend_state.lock().await;
                                *g = Some(ResendState {
                                    cmd: Arc::clone(&msg),
                                    missing,
                                });
                            }
                            self.resend_notify.notify_one();

                            self.register_client
                                .broadcast(Broadcast { cmd: msg })
                                .await;
                        }

                        Mode::Idle => {}
                    }

                    self.value_from.clear();
                }
            }

            SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write,
            } => {
                if timestamp > self.ts || (timestamp == self.ts && write_rank > self.wr) {
                    self.ts = timestamp;
                    self.wr = write_rank;
                    self.val = data_to_write.clone();
                    self.sectors_manager
                        .write(self.sector_idx, &(data_to_write, timestamp, write_rank))
                        .await;
                }

                let ack = SystemRegisterCommand {
                    header: SystemCommandHeader {
                        process_identifier: self.self_ident,
                        msg_ident: cmd.header.msg_ident,
                        sector_idx: self.sector_idx,
                    },
                    content: SystemRegisterCommandContent::Ack,
                };

                self.register_client
                    .send(Send {
                        cmd: Arc::new(ack),
                        target: cmd.header.process_identifier,
                    })
                    .await;
            }

            SystemRegisterCommandContent::Ack => {
                let Some(op_id) = self.op_id else { return; };
                if op_id != cmd.header.msg_ident { return; }
                if !matches!(self.phase, Phase::WriteBack) { return; }
                if matches!(self.mode, Mode::Idle) { return; }

                if self.ack_from.contains(&cmd.header.process_identifier) {
                    return;
                }
                self.ack_from.insert(cmd.header.process_identifier);

                {
                    let mut g = self.resend_state.lock().await;
                    if let Some(st) = g.as_mut() {
                        let src = cmd.header.process_identifier as usize;
                        if src < st.missing.len() {
                            st.missing[src] = false;
                        }
                    }
                }

                if self.ack_from.len() >= majority {
                    let req_id = self.pending_req_id.take().unwrap();
                    let cb = self.pending_callback.take().unwrap();

                    let response = match self.mode {
                        Mode::Reading => {
                            let val = self.max_value_seen.as_ref().unwrap().2.clone();
                            ClientCommandResponse {
                                status: crate::StatusCode::Ok,
                                request_identifier: req_id,
                                op_return: crate::OperationReturn::Read { read_data: val },
                            }
                        }
                        Mode::Writing => ClientCommandResponse {
                            status: crate::StatusCode::Ok,
                            request_identifier: req_id,
                            op_return: crate::OperationReturn::Write,
                        },
                        Mode::Idle => return,
                    };

                    self.mode = Mode::Idle;
                    self.phase = Phase::Query;
                    self.op_id = None;
                    self.writeval = None;
                    self.max_value_seen = None;
                    self.value_from.clear();
                    self.ack_from.clear();

                    {
                        let mut g = self.resend_state.lock().await;
                        *g = None;
                    }
                    self.resend_notify.notify_one();

                    cb(response).await;
                }
            }
        }
    }

}
