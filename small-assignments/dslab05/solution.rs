use hmac::{Hmac, Mac};

use rustls::pki_types::ServerName;
use rustls::pki_types::pem::PemObject;
use rustls::{ClientConnection, RootCertStore, ServerConnection, StreamOwned};
use sha2::Sha256;
use std::io::{Read, Write};

use std::sync::Arc;

type HmacSha256 = Hmac<Sha256>;

pub struct SecureClient<L: Read + Write> {
    stream: StreamOwned<ClientConnection, L>,
    hmac_key: Vec<u8>,
}

pub struct SecureServer<L: Read + Write> {
    stream: StreamOwned<ServerConnection, L>,
    hmac_key: Vec<u8>,
}

impl<L: Read + Write> SecureClient<L> {
    /// Creates a new instance of `SecureClient`.
    ///
    /// `SecureClient` communicates with `SecureServer` via `link`.
    /// The messages include a HMAC tag calculated using `hmac_key`.
    /// A certificate of `SecureServer` is signed by `root_cert`.
    /// We are connecting with `server_hostname`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        root_cert: &str,
        server_hostname: ServerName<'static>,
    ) -> Self {
        
        let mut root_store = RootCertStore::empty();

        let root_certs: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(root_cert.as_bytes())
            .flatten()
            .collect();

        root_store.add_parsable_certificates(root_certs);

        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let connection = ClientConnection::new(Arc::new(client_config), server_hostname).unwrap();

        Self {
            stream: StreamOwned::new(connection, link),
            hmac_key: Vec::from(hmac_key),
        }
    }

    /// Sends the data to the server. The sent message follows the
    /// format specified in the description of the assignment.
    pub fn send_msg(&mut self, data: Vec<u8>) {
        let len = data.len() as u32;
        let len_bytes: [u8; 4] = len.to_be_bytes();

        let mut mac = HmacSha256::new_from_slice(&self.hmac_key).unwrap();

        mac.update(&data);
        let hmac_tag = mac.finalize().into_bytes();

        let mut msg = Vec::new();
        msg.extend_from_slice(&len_bytes);
        msg.extend_from_slice(&data);
        msg.extend_from_slice(&hmac_tag);

        self.stream.write_all(&msg).unwrap();
        self.stream.flush().unwrap(); // This wasn't showed in the examples, however I add it to be extra sure
    }
}

impl<L: Read + Write> SecureServer<L> {
    /// Creates a new instance of `SecureServer`.
    ///
    /// `SecureServer` receives messages from `SecureClients` via `link`.
    /// HMAC tags of the messages are verified against `hmac_key`.
    /// The private key of the `SecureServer`'s certificate is `server_private_key`,
    /// and the full certificate chain is `server_full_chain`.
    pub fn new(
        link: L,
        hmac_key: &[u8],
        server_private_key: &str,
        server_full_chain: &str,
    ) -> Self {
        let certs: Vec<rustls::pki_types::CertificateDer<'_>> = rustls::pki_types::CertificateDer::pem_slice_iter(server_full_chain.as_bytes())
            .flatten()
            .collect();

        let private_key = rustls::pki_types::PrivateKeyDer::from_pem_slice(server_private_key.as_bytes()).unwrap();

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, private_key)
            .unwrap();

        let connection = ServerConnection::new(Arc::new(server_config)).unwrap();

        Self {
            stream: StreamOwned::new(connection, link),
            hmac_key: Vec::from(hmac_key),
        }
    }

    /// Receives the next incoming message and returns the message's content
    /// (i.e., without the message size and without the HMAC tag) if the
    /// message's HMAC tag is correct. Otherwise, returns `SecureServerError`.
    pub fn recv_message(&mut self) -> Result<Vec<u8>, SecureServerError> {
        let mut len_bytes = [0u8; 4];
        self.stream.read_exact(&mut len_bytes).unwrap();
        let len = u32::from_be_bytes(len_bytes) as usize;

        let mut data = vec![0u8; len];
        self.stream.read_exact(&mut data).unwrap();

        let mut hmac_tag = [0u8; 32];
        self.stream.read_exact(&mut hmac_tag).unwrap();

        let mut mac = HmacSha256::new_from_slice(&self.hmac_key).unwrap();
        mac.update(&data);

        match mac.verify_slice(&hmac_tag) {
            Ok(_) => Ok(data),
            Err(_) => Err(SecureServerError::InvalidHmac),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SecureServerError {
    /// The HMAC tag of a message is invalid.
    InvalidHmac,
}
