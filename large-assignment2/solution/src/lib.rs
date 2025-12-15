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
    let rank_ind = config.public.self_rank as usize -1;

    let bind_addr = format!("{}:{}", config.public.tcp_locations[rank_ind].0, config.public.tcp_locations[rank_ind].1);
    let listener = TcpListener::bind(bind_addr).await.unwrap();

    let config = Arc::new(config);

    let sectors_manager = build_sectors_manager(config.public.storage_dir.clone()).await;
    let register_client: Arc<dyn RegisterClient> = Arc::new(MyRegisterClient { });

    let dispatcher = Dispatcher {
        config: Arc::clone(&config),
        sectors_manager: Arc::clone(&sectors_manager),
        register_client: Arc::clone(&register_client),
        router: Mutex::new(HashMap::new())
    };
    let dispatcher = Arc::new(dispatcher);

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
    use crate::{
        ClientCommandResponse, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager,
        SystemRegisterCommand,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait AtomicRegister: Send + Sync {
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
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                    + Send
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
        Box::new(MyAtomicRegister {
            self_ident,
            sector_idx,
            register_client,
            sectors_manager,
            processes_count,
        })
    }

    pub struct MyAtomicRegister {
        self_ident: u8,
        sector_idx: SectorIdx, 
        register_client: Arc<dyn RegisterClient>, 
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8,
    }

    #[async_trait::async_trait]
    impl AtomicRegister for MyAtomicRegister {
        async fn client_command(
            &mut self,
            cmd: ClientRegisterCommand,
            success_callback: Box<
                dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + Send>>
                + Send
                + Sync,
            >,
        ) {
            
        }

        async fn system_command(&mut self, cmd: SystemRegisterCommand) {

        }
    }
}

pub mod sectors_manager_public {
    use crate::{SectorIdx, SectorVec, zero_sector};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[async_trait::async_trait]
    pub trait SectorsManager: Send + Sync {
        /// Returns 4096 bytes of sector data by index.
        async fn read_data(&self, idx: SectorIdx) -> SectorVec;

        /// Returns timestamp and write rank of the process which has saved this data.
        /// Timestamps and ranks are relevant for atomic register algorithm, and are described
        /// there.
        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

        /// Writes a new data, along with timestamp and write rank to some sector.
        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
    }

    /// Path parameter points to a directory to which this method has exclusive access.
    pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
       Arc::new(MySectorsManager { })
    }

    struct MySectorsManager {}
    
    #[async_trait::async_trait]
    impl SectorsManager for MySectorsManager {
        async fn read_data(&self, idx: SectorIdx) -> SectorVec {
            return zero_sector();
        }

        async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
            return (0, 0);
        }

        async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {

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
    use crate::SystemRegisterCommand;
    use std::sync::Arc;

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

    pub struct MyRegisterClient {}

    #[async_trait::async_trait]
    impl RegisterClient for MyRegisterClient {
        async fn send(&self, msg: Send) {

        }

        async fn broadcast(&self, msg: Broadcast) {

        }
    }

    pub struct Broadcast {
        pub cmd: Arc<SystemRegisterCommand>,
    }

    pub struct Send {
        pub cmd: Arc<SystemRegisterCommand>,
        /// Identifier of the target process. Those start at 1.
        pub target: u8,
    }
}
