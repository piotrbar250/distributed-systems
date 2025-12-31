mod domain;
pub mod sectors_manager_public;
pub mod register_client_public;
pub mod transfer_public;
pub mod atomic_register_public;

use std::{collections::{HashMap, VecDeque}, pin::Pin, sync::Arc};

pub use crate::domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
use tokio::{io::AsyncWriteExt, net::{TcpListener, TcpStream, tcp::OwnedWriteHalf}, select, sync::{Mutex, mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel}, oneshot}};
pub use transfer_public::*;

fn zero_sector() -> SectorVec {
    SectorVec(Box::new(serde_big_array::Array([0u8; SECTOR_SIZE])))
}

type SuccessCb = Box<dyn FnOnce(ClientCommandResponse) -> Pin<Box<dyn Future<Output = ()> + core::marker::Send>> + core::marker::Send + Sync>;

async fn write_client_response(
    socket: &mut OwnedWriteHalf,
    msg: ClientCommandResponse,
    hmac_client_key: &[u8; 32],
) -> Result<(), EncodingError> {
    let conf = bincode::config::standard()
        .with_big_endian()
        .with_fixed_int_encoding();

    let payload = bincode::serde::encode_to_vec(msg, conf).map_err(EncodingError::BincodeError)?;
    let tag = caclulate_hmac_tag(&payload, hmac_client_key);
    let msg_len = (payload.len() + tag.len()) as u64;

    println!("server msg len: {}", msg_len);

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
    socket: TcpStream,
    config: Arc<Configuration>,
    dispatcher: Arc<Dispatcher>,
) {
    let hmac_system_key= &config.hmac_system_key;
    let hmac_client_key= &config.hmac_client_key;

    let (mut reader_socket, mut writer_socket) = socket.into_split();
    let (response_tx, mut response_rx) = unbounded_channel();
    let hmac_client_key_copy = config.hmac_client_key;

    let writer_handle = tokio::spawn(async move {
        while let Some(msg) = response_rx.recv().await {
            if write_client_response(&mut writer_socket, msg, &hmac_client_key_copy).await.is_err() {
                break;
            }
        }
    });

    loop {
        let response_tx_copy = response_tx.clone();
        let (cmd, valid) = match deserialize_register_command(&mut reader_socket, hmac_system_key, hmac_client_key).await {
            Ok(x) => x,
            Err(_) => break,
        };

        match cmd {
            RegisterCommand::Client(client_cmd) => {
                if !valid {
                    let resp = make_invalid_response(&client_cmd, StatusCode::AuthFailure);
                    if response_tx_copy.send(resp).is_err() {
                        break;
                    }
                    continue;
                }
                
                if client_cmd.header.sector_idx >= config.public.n_sectors {
                    let resp = make_invalid_response(&client_cmd, StatusCode::InvalidSectorIndex);
                    if response_tx_copy.send(resp).is_err() {
                        break;
                    }
                    continue;
                }

                let (oneshot_tx, oneshot_rx) = oneshot::channel();
                
                dispatcher.handle_client(client_cmd, oneshot_tx).await;

                tokio::spawn(async move {
                    if let Ok(resp) = oneshot_rx.await {
                        if !response_tx_copy.is_closed() {
                            _ = response_tx_copy.send(resp);
                        }
                    }
                });
            },
            RegisterCommand::System(system_cmd) => {
                if !valid {
                    break;
                }
                dispatcher.handle_system(system_cmd).await;
            },
        }
    }
    writer_handle.abort();
}

pub async fn run_register_process(config: Configuration) {
    let n = config.public.tcp_locations.len() as u8;
    let self_rank = config.public.self_rank;
    let self_idx = self_rank as usize - 1;

    let bind_addr = format!("{}:{}", config.public.tcp_locations[self_idx].0, config.public.tcp_locations[self_idx].1);
    let listener = TcpListener::bind(&bind_addr).await.unwrap();
    println!("idx: {}, listening on: {}", config.public.self_rank, bind_addr);

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