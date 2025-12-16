mod domain;

use std::{collections::{HashMap, VecDeque}, hash::Hash, io::Read, path::PathBuf, pin::Pin, sync::{Arc, atomic}};

pub use crate::domain::*;
use async_channel::unbounded;
pub use atomic_register_public::*;
use base64::display;
use bincode::config::{self, standard};
pub use register_client_public::*;
pub use sectors_manager_public::*;
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream}, select, sync::{Mutex, mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel}, oneshot}};
pub use transfer_public::*;

fn zero_sector() -> SectorVec {
    SectorVec(Box::new(serde_big_array::Array([0u8; SECTOR_SIZE])))
}

type SuccessCb = Box<dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>> + core::marker::Send + Sync>;

async fn write_client_response(
    socket: &mut TcpStream,
    msg: ClientCommandResponse,
    hmac_client_key: &[u8; 32],
) -> Result<(), EncodingError> {
    let conf = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    let payload = bincode::serde::encode_to_vec(msg, conf).map_err(EncodingError::BincodeError)?;
    let tag = caclulate_hmac_tag(&payload, hmac_client_key);
    let msg_len = (payload.len() + tag.len()) as u64;

    socket.write_all(&msg_len.to_be_bytes()).await.map_err(EncodingError::IoError)?;
    socket.write_all(&payload).await.map_err(EncodingError::IoError)?;
    socket.write_all(&tag).await.map_err(EncodingError::IoError)?;

    Ok(())
}

fn make_invalid_response(cmd: &ClientRegisterCommand, status: StatusCode) -> ClientCommandResponse {
    ClientCommandResponse {
        status,
        request_identifier: cmd.header.request_identifier,
        op_return: match cmd.content {
            ClientRegisterCommandContent::Read => OperationReturn::Read { read_data: zero_sector() },
            ClientRegisterCommandContent::Write { .. } => OperationReturn::Write,
        }
    }
}

async fn handle_connection(
    mut socket: TcpStream,
    config: Arc<Configuration>,
    dispatcher: Arc<Dispatcher>,
) {
    let hmac_system_key= &config.hmac_system_key;
    let hmac_client_key= &config.hmac_client_key;

    loop {

        let (cmd, valid) = match deserialize_register_command(&mut socket, hmac_system_key, hmac_client_key).await {
            Ok(x) => x,
            Err(_) => return,
        };

        match cmd {
            RegisterCommand::Client(client_cmd) => {
                if !valid {
                    let resp = make_invalid_response(&client_cmd, StatusCode::AuthFailure);
                    if write_client_response(&mut socket, resp, hmac_client_key).await.is_err() {
                        return;
                    }
                    continue;
                }
                
                if client_cmd.header.sector_idx >= config.public.n_sectors {
                    let resp = make_invalid_response(&client_cmd, StatusCode::InvalidSectorIndex);
                    if write_client_response(&mut socket, resp, hmac_client_key).await.is_err() {
                        return;
                    }
                    continue;
                }

                let (oneshot_tx, oneshot_rx) = oneshot::channel();
                
                dispatcher.handle_client(client_cmd, oneshot_tx).await;

                let resp = match oneshot_rx.await {
                    Ok(resp) => resp,
                    Err(_) => return,
                };

                _ = write_client_response(&mut socket, resp, hmac_client_key).await;

            },
            RegisterCommand::System(system_cmd) => {
                if !valid{
                    return;
                }
                dispatcher.handle_system(system_cmd).await;
            },
        }
    }
}

pub async fn run_register_process(config: Configuration) {
    let n = config.public.tcp_locations.len() as u8;
    let self_rank = config.public.self_rank;
    let self_idx = self_rank as usize - 1;

    let bind_addr = format!("{}:{}", config.public.tcp_locations[self_idx].0, config.public.tcp_locations[self_idx].1);
    let listener = TcpListener::bind(bind_addr).await.unwrap();

    let config = Arc::new(config);

    let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;

    let mut internal_tx: Vec<Option<UnboundedSender<Arc<SystemRegisterCommand>>>> = vec![None; n as usize + 1];
    
    for target_rank in 1..=n {
        if target_rank == self_rank {
            continue;
        }

        let (tx, rx) = unbounded_channel();
        internal_tx[target_rank as usize] = Some(tx);
        let (thost, tport) = &config.public.tcp_locations[target_rank as usize - 1];
        let addr = format!("{}:{}", thost, tport);
        tokio::spawn(sender_worker(addr, rx, config.hmac_system_key.clone()));
    }

    let (local_tx, mut local_rx) = unbounded_channel();

    let register_client: Arc<dyn RegisterClient> = Arc::new(MyRegisterClient {
        self_ident: self_rank,
        processes_count: n,
        local_tx,
        internal_tx,
    });

    let dispatcher = Dispatcher {
        config: Arc::clone(&config),
        sectors_manager: Arc::clone(&sectors_manager),
        register_client: Arc::clone(&register_client),
        router: Mutex::new(HashMap::new())
    };
    let dispatcher = Arc::new(dispatcher);

    let dispatcher_local = Arc::clone(&dispatcher);
    tokio::spawn(async move {
        while let Some(cmd) = local_rx.recv().await {
            dispatcher_local.handle_system((*cmd).clone()).await;
        }
    });

    loop {
        let (socket, _client_addr) = listener.accept().await.unwrap();
        tokio::spawn(handle_connection(
            socket,
            Arc::clone(&config),
            Arc::clone(&dispatcher)
        ));
    }
}

pub mod atomic_register_public {
    use hmac::digest::consts::False;
    use tokio::sync::{Mutex, Notify};
    use tokio::time::interval;
    use uuid::Uuid;

    use crate::{
        Broadcast, ClientCommandResponse, ClientRegisterCommand, ClientRegisterCommandContent, RegisterClient, SectorIdx, SectorVec, SectorsManager, Send, SuccessCb, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent, zero_sector
    };
    use core::time;
    use std::collections::{HashMap, HashSet};
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
}

struct Dispatcher {
    config: Arc<Configuration>,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<dyn RegisterClient>,
    router: Mutex<HashMap<SectorIdx, mpsc::UnboundedSender<SectorMsg>>>,
}

impl Dispatcher {

    async fn get_sector_queue(&self, sector_idx: SectorIdx) -> mpsc::UnboundedSender<SectorMsg>{
        let mut map = self.router.lock().await;
        
        if let Some(tx) = map.get(&sector_idx) {
            return tx.clone();
        }

        let (tx, rx) = unbounded_channel::<SectorMsg>();
        map.insert(sector_idx, tx.clone());

        tokio::spawn(sector_worker(
            sector_idx, 
            rx, 
            Arc::clone(&self.config),
            Arc::clone(&self.sectors_manager), 
            Arc::clone(&self.register_client) 
        ));

        return tx;
    }

    async fn handle_client(&self, client_cmd: ClientRegisterCommand, oneshot_tx: oneshot::Sender<ClientCommandResponse>) {
        let sector_idx = client_cmd.header.sector_idx;

        let sector_tx = self.get_sector_queue(sector_idx).await;
        _ = sector_tx.send(SectorMsg::Client {
            client_cmd,
            oneshot_tx,
        });
    }

    async fn handle_system(&self, system_cmd: SystemRegisterCommand) {
        let sector_idx = system_cmd.header.sector_idx;

        let sector_tx = self.get_sector_queue(sector_idx).await;
        _ = sector_tx.send(SectorMsg::System(system_cmd));
    }
}


enum SectorMsg {
    Client {
        client_cmd: ClientRegisterCommand,
        oneshot_tx: oneshot::Sender<ClientCommandResponse>,
    },
    System(SystemRegisterCommand),
}

struct ClientTask {
    cmd: ClientRegisterCommand,
    respond_to: oneshot::Sender<ClientCommandResponse>,
}
async fn sector_worker(
    sector_idx: SectorIdx,
    mut rx: UnboundedReceiver<SectorMsg>, 
    config: Arc<Configuration>,
    sectors_manager: Arc<dyn SectorsManager>,
    register_client: Arc<dyn RegisterClient>,
) {
    let mut atomic_register = build_atomic_register(
        config.public.self_rank,
        sector_idx,
        Arc::clone(&register_client),
        Arc::clone(&sectors_manager),
        config.public.tcp_locations.len() as u8
    ).await;

    let mut pending = VecDeque::new();
    let mut busy = false;

    let (ready_tx, mut ready_rx) = unbounded_channel();

    async fn try_start_next_client(
        busy: &mut bool,
        pending: &mut VecDeque<ClientTask>,
        ready_tx: UnboundedSender<()>,
        atomic_register: &mut Box<dyn AtomicRegister>,
    ) {
        if *busy {
            return;
        }

        let Some(ClientTask {cmd, respond_to}) = pending.pop_front() else {
            return;
        };
        *busy = true;

        let cb = Box::new(move |resp: ClientCommandResponse| -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>> {
            Box::pin(async move {
                _ = respond_to.send(resp);
                _ = ready_tx.send(());
            })
        });

        atomic_register.client_command(cmd,cb).await;
    }

    loop {
        select! {
            Some(cmd) = rx.recv() => {
                match cmd {
                    SectorMsg::System(system_cmd) => {
                        atomic_register.system_command(system_cmd).await;
                    },

                    SectorMsg::Client { client_cmd, oneshot_tx} => {
                        pending.push_back(ClientTask {
                            cmd: client_cmd,
                            respond_to: oneshot_tx,
                        });
                    }                        
                }
                try_start_next_client(&mut busy, &mut pending, ready_tx.clone(), &mut atomic_register).await;
            }
            Some(()) = ready_rx.recv() => {
                busy = false;
                try_start_next_client(&mut busy, &mut pending, ready_tx.clone(), &mut atomic_register).await;
            }
        }
    }
   
}

pub mod transfer_public {
    use crate::RegisterCommand;
    use bincode::error::{DecodeError, EncodeError};
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    use std::{io::{self, Error}, rc::Rc};
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    #[derive(Debug)]
    pub enum EncodingError {
        IoError(Error),
        BincodeError(EncodeError),
    }

    #[derive(Debug, derive_more::Display)]
    pub enum DecodingError {
        IoError(Error),
        BincodeError(DecodeError),
        InvalidMessageSize,
    }

    type HmacSha256 = Hmac<Sha256>;

    pub fn caclulate_hmac_tag(message: &[u8], key: &[u8]) -> [u8; 32] {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(&message);
        mac.finalize().into_bytes().into()
    }

    pub fn verify_hmac_tag(message: &[u8], key: &[u8], tag: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update(&message);
        mac.verify_slice(tag).is_ok()
    }

    pub async fn deserialize_register_command(
        data: &mut (dyn AsyncRead + Send + Unpin),
        hmac_system_key: &[u8; 64],
        hmac_client_key: &[u8; 32],
    ) -> Result<(RegisterCommand, bool), DecodingError> {
        
        let mut msg_len_buf = [0u8; 8];
   
        let msg_len = match data.read_exact(&mut msg_len_buf).await {
            Ok(_) => u64::from_be_bytes(msg_len_buf) as usize,
            Err(e) => { return Err(DecodingError::IoError(e)); },
        };

        if msg_len <= 32 {
            return Err(DecodingError::InvalidMessageSize);
        }

        let mut buf = vec![0u8; msg_len as usize];
        match data.read_exact(&mut buf).await {
            Ok(_) => {},
            Err(e) => { return Err(DecodingError::IoError(e)); },
        }

        let payload = &buf[..msg_len-32];

        let rc = bincode::serde::decode_from_slice(payload, bincode::config::standard().with_big_endian().with_fixed_int_encoding()).map_err(DecodingError::BincodeError)?.0;
        let tag = &buf[msg_len-32..];

        match rc {
            RegisterCommand::Client(_) => Ok((rc, verify_hmac_tag(payload, hmac_client_key, tag))),
            RegisterCommand::System(_) => Ok((rc, verify_hmac_tag(payload, hmac_system_key, tag))),
        }
    }

    pub async fn serialize_register_command(
        cmd: &RegisterCommand,
        writer: &mut (dyn AsyncWrite + Send + Unpin),
        hmac_key: &[u8],
    ) -> Result<(), EncodingError> {
        
        let conf = bincode::config::standard()
            .with_big_endian()
            .with_fixed_int_encoding();
        
        let paylod = bincode::serde::encode_to_vec(&cmd, conf).map_err(EncodingError::BincodeError)?;
        let tag = caclulate_hmac_tag(&paylod, hmac_key);
        let msg_len = (paylod.len() + tag.len()) as u64;

        debug_assert!(tag.len() == 32);

        writer.write_all(&msg_len.to_be_bytes()).await.map_err(EncodingError::IoError)?;
        writer.write_all(&paylod).await.map_err(EncodingError::IoError)?;
        writer.write_all(&tag).await.map_err(EncodingError::IoError)?;
        Ok(())
    }
}

pub mod register_client_public {
    use tokio::{net::TcpStream, sync::mpsc::{UnboundedReceiver, UnboundedSender}, time::sleep};

    use crate::{EncodingError, RegisterCommand, SystemRegisterCommand, serialize_register_command};
    use std::{alloc::System, arch::aarch64::int8x8_t, sync::Arc, time::Duration};

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
}

pub mod sectors_manager_public {
    use crate::{zero_sector, SectorIdx, SectorVec, SECTOR_SIZE};
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tokio::fs::{self, File};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::{Mutex, RwLock};
    use uuid::Uuid;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
        fs::create_dir_all(&path).await.unwrap();
        let tmp_dir = path.join("tmp");
        fs::create_dir_all(&tmp_dir).await.unwrap();

        // Open directory handles for fsync (works on Unix; assignment targets Linux-like).
        let root_dir_handle = File::open(&path).await.unwrap();
        let tmp_dir_handle = File::open(&tmp_dir).await.unwrap();

        let mgr = MySectorsManager {
            root_dir: path,
            tmp_dir,
            root_dir_handle,
            tmp_dir_handle,
            index: RwLock::new(HashMap::new()),
            stripes: (0..256).map(|_| Mutex::new(())).collect(),
        };

        mgr.recover().await;
        Arc::new(mgr)
    }

    /// Filename format:
    ///   s<idx_hex>_t<ts_hex>_w<wr_hex>.bin
    /// Example:
    ///   s000000000000000c_t0000000000000001_w01.bin
    ///
    /// Metadata lives in filename to keep per-sector overhead tiny.
    struct MySectorsManager {
        root_dir: PathBuf,
        tmp_dir: PathBuf,

        root_dir_handle: File,
        tmp_dir_handle: File,

        /// In-memory index: sector -> (ts, wr, filename)
        /// Memory is linear in number of written sectors.
        index: RwLock<HashMap<SectorIdx, (u64, u8, String)>>,

        /// Fixed striped locks for concurrent ops on different sectors.
        stripes: Vec<Mutex<()>>,
    }

    impl MySectorsManager {
        fn stripe(&self, idx: SectorIdx) -> &Mutex<()> {
            let n = self.stripes.len() as u64;
            &self.stripes[(idx % n) as usize]
        }

        fn make_filename(idx: SectorIdx, ts: u64, wr: u8) -> String {
            format!("s{idx:016x}_t{ts:016x}_w{wr:02x}.bin")
        }

        fn parse_filename(name: &str) -> Option<(SectorIdx, u64, u8)> {
            // Expect "s<idx>_t<ts>_w<wr>.bin"
            let name = name.strip_suffix(".bin")?;
            let mut parts = name.split('_');

            let s_part = parts.next()?;
            let t_part = parts.next()?;
            let w_part = parts.next()?;
            if parts.next().is_some() {
                return None;
            }

            let idx_hex = s_part.strip_prefix('s')?;
            let ts_hex = t_part.strip_prefix('t')?;
            let wr_hex = w_part.strip_prefix('w')?;

            let idx = u64::from_str_radix(idx_hex, 16).ok()?;
            let ts = u64::from_str_radix(ts_hex, 16).ok()?;
            let wr_u8 = u8::from_str_radix(wr_hex, 16).ok()?;
            Some((idx, ts, wr_u8))
        }

        fn is_newer(a: (u64, u8), b: (u64, u8)) -> bool {
            // a > b in lexicographic order (ts, wr)
            a.0 > b.0 || (a.0 == b.0 && a.1 > b.1)
        }

        async fn sync_dir(dir_handle: &File) {
            // fsync directory so renames/unlinks are durable
            let _ = dir_handle.sync_data().await;
        }

        async fn recover(&self) {
            // 1) Clear tmp dir (stale partial writes)
            if let Ok(mut rd) = fs::read_dir(&self.tmp_dir).await {
                while let Ok(Some(ent)) = rd.next_entry().await {
                    let _ = fs::remove_file(ent.path()).await;
                }
            }
            Self::sync_dir(&self.tmp_dir_handle).await;

            // 2) Scan root dir, keep newest per sector, delete stale duplicates
            let mut best: HashMap<SectorIdx, (u64, u8, String)> = HashMap::new();
            let mut all_files: Vec<(SectorIdx, u64, u8, String)> = Vec::new();

            if let Ok(mut rd) = fs::read_dir(&self.root_dir).await {
                while let Ok(Some(ent)) = rd.next_entry().await {
                    let file_name_os = ent.file_name();
                    let file_name = match file_name_os.to_str() {
                        Some(s) => s.to_string(),
                        None => continue,
                    };

                    // Ignore our tmp dir entry and any unknown files.
                    if file_name == "tmp" {
                        continue;
                    }

                    if let Some((idx, ts, wr)) = Self::parse_filename(&file_name) {
                        all_files.push((idx, ts, wr, file_name.clone()));
                        match best.get(&idx) {
                            None => {
                                best.insert(idx, (ts, wr, file_name));
                            }
                            Some((bts, bwr, _)) => {
                                if Self::is_newer((ts, wr), (*bts, *bwr)) {
                                    best.insert(idx, (ts, wr, file_name));
                                }
                            }
                        }
                    }
                }
            }

            // Delete non-best files
            for (idx, ts, wr, fname) in all_files {
                let keep = best
                    .get(&idx)
                    .map(|(bts, bwr, bf)| *bts == ts && *bwr == wr && bf == &fname)
                    .unwrap_or(false);

                if !keep {
                    let _ = fs::remove_file(self.root_dir.join(&fname)).await;
                }
            }
            Self::sync_dir(&self.root_dir_handle).await;

            // Publish recovered index
            let mut idx_guard = self.index.write().await;
            *idx_guard = best;
        }

        async fn read_file_exact_4096(path: &Path) -> Option<SectorVec> {
            let mut f = File::open(path).await.ok()?;
            let mut buf = [0u8; SECTOR_SIZE];
            if f.read_exact(&mut buf).await.is_err() {
                return None;
            }
            Some(SectorVec(Box::new(serde_big_array::Array(buf))))
        }
    }

    #[async_trait::async_trait]
    impl SectorsManager for MySectorsManager {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec {
            let _g = self.stripe(idx).lock().await;

            let entry = { self.index.read().await.get(&idx).cloned() };
            let Some((_ts, _wr, fname)) = entry else {
                return zero_sector();
            };

            let path = self.root_dir.join(fname);
            match Self::read_file_exact_4096(&path).await {
                Some(v) => v,
                None => zero_sector(),
            }
        }

        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
            let _g = self.stripe(idx).lock().await;

            match self.index.read().await.get(&idx) {
                Some((ts, wr, _)) => (*ts, *wr),
                None => (0, 0),
            }
        }

        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
            let _g = self.stripe(idx).lock().await;

            let (data, ts, wr) = sector;

            // 1) Write to a unique temp file
            let tmp_name = format!("tmp-{}-{}.bin", idx, Uuid::new_v4());
            let tmp_path = self.tmp_dir.join(&tmp_name);

            {
                let mut f = File::create(&tmp_path).await.unwrap();
                f.write_all(data.0.as_slice()).await.unwrap();
                f.sync_data().await.unwrap();
            }
            Self::sync_dir(&self.tmp_dir_handle).await;

            // 2) Atomically install via rename to final name
            let final_name = Self::make_filename(idx, *ts, *wr);
            let final_path = self.root_dir.join(&final_name);

            // On Unix, rename over an existing file is atomic replacement.
            fs::rename(&tmp_path, &final_path).await.unwrap();
            Self::sync_dir(&self.root_dir_handle).await;

            // 3) Update in-memory index & remove old file (if any)
            let old = {
                let mut map = self.index.write().await;
                let old = map.get(&idx).cloned();

                match old {
                    None => {
                        map.insert(idx, (*ts, *wr, final_name.clone()));
                        None
                    }
                    Some((old_ts, old_wr, old_fname)) => {
                        // Keep the newest (ts,wr)
                        if MySectorsManager::is_newer((*ts, *wr), (old_ts, old_wr)) {
                            map.insert(idx, (*ts, *wr, final_name.clone()));
                            Some(old_fname)
                        } else {
                            // If caller tries to write older data, keep current index.
                            // Still installed a file; clean it up below.
                            Some(final_name.clone())
                        }
                    }
                }
            };

            if let Some(old_fname) = old {
                // If old_fname equals final_name, do nothing.
                if old_fname != final_name {
                    let _ = fs::remove_file(self.root_dir.join(old_fname)).await;
                } else {
                    // We "installed" a file but decided it's not the newest. Remove it.
                    let _ = fs::remove_file(self.root_dir.join(&final_name)).await;
                }
                Self::sync_dir(&self.root_dir_handle).await;
            }
        }
    }
}