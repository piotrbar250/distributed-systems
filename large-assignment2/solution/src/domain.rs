use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use uuid::Uuid;

pub const SECTOR_SIZE: usize = 4096;

pub struct Configuration {
    /// Hmac key to verify and sign internal requests.
    pub hmac_system_key: [u8; 64],
    /// Hmac key to verify client requests.
    pub hmac_client_key: [u8; 32],
    /// Part of configuration which is safe to share with external world.
    pub public: PublicConfiguration,
}

#[derive(Debug)]
pub struct PublicConfiguration {
    /// Storage for durable data.
    pub storage_dir: PathBuf,
    /// Host and port, indexed by identifiers, of every process, including itself
    /// (subtract 1 from `self_rank` to obtain index in this array).
    /// You can assume that `tcp_locations.len() < 255`.
    pub tcp_locations: Vec<(String, u16)>,
    /// Identifier of this process. Identifiers start at 1.
    pub self_rank: u8,
    /// The number of sectors. The range of supported sectors is <0, `n_sectors`).
    pub n_sectors: u64,
}

#[derive(Clone, Eq, PartialEq, Debug, Serialize, Deserialize)]
pub struct SectorVec(
    // This is effectively Box<[u8; SECTOR_SIZE]> but due to Serde's limitations we have to use following wrapper
    pub Box<serde_big_array::Array<u8, SECTOR_SIZE>>,
);

pub type SectorIdx = u64;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
#[non_exhaustive]
pub enum RegisterCommand {
    /// Command sent from the end client
    Client(ClientRegisterCommand),
    /// Internal system command
    System(SystemRegisterCommand),
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
/// Repr u8 macro marks this enum as translatable to a single byte. So `Ok` is 0x0,
/// and consecutive values are consecutive numbers.
pub enum StatusCode {
    /// Command completed successfully
    Ok,
    /// Invalid HMAC signature
    AuthFailure,
    /// Sector index is out of range <0, `Configuration.n_sectors`)
    InvalidSectorIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClientRegisterCommand {
    /// Register (sector) and request identifiers
    pub header: ClientCommandHeader,
    /// Contents of Read / Write command
    pub content: ClientRegisterCommandContent,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct SystemRegisterCommand {
    /// Identifier of sender process, register (sector) and message
    pub header: SystemCommandHeader,
    /// Content of the system message
    pub content: SystemRegisterCommandContent,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum SystemRegisterCommandContent {
    /// Request current register state
    ReadProc,
    /// Current register state
    Value {
        /// Timestamp of the last change
        timestamp: u64,
        /// rank of the writer
        write_rank: u8,
        /// Register data
        sector_data: SectorVec,
    },
    /// New register state
    WriteProc {
        /// Timestamp of the change
        timestamp: u64,
        /// rank of the writer
        write_rank: u8,
        /// Data to set
        data_to_write: SectorVec,
    },
    /// Acknowledgement of the processing completion
    Ack,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub enum ClientRegisterCommandContent {
    /// Read command from the client
    Read,
    /// Write command with new data from the client
    Write { data: SectorVec },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub struct ClientCommandHeader {
    /// Identifier of the request
    pub request_identifier: u64,
    /// Identifier of the register (sector)
    pub sector_idx: SectorIdx,
}

#[derive(Debug, Clone, Copy, Eq, Serialize, Deserialize, PartialEq)]
pub struct SystemCommandHeader {
    /// Sender identifier
    pub process_identifier: u8,
    /// Message identifier
    pub msg_ident: Uuid,
    /// Register (sector) identifier
    pub sector_idx: SectorIdx,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientCommandResponse {
    /// Status of the operation
    pub status: StatusCode,
    /// Corresponding request identifier
    pub request_identifier: u64,
    /// Return value of the operation
    pub op_return: OperationReturn,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationReturn {
    /// Response for the `Read` command with sector data
    Read { read_data: SectorVec },
    /// Response for `Write` command
    Write,
}
