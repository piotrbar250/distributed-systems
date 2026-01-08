use std::{collections::HashSet, time::Duration};

use module_system::{Handler, ModuleRef, System};

pub use domain::*;
use rand::{Rng, rngs::ThreadRng};
use uuid::Uuid;

mod domain;

pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[non_exhaustive]
pub struct Raft {
    // TODO you can add fields to this struct.
    config: ServerConfig,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
    role: Role,

    current_term: u64,
    voted_for: Uuid,
    received_votes: HashSet<Uuid>,
}

#[derive(Clone)]
struct Tick;

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {

        let mut rng = rand::rng();
        let election_timeout = rng.random_range(config.election_timeout_range.clone());
                
        let raft = system.register_module(|_| Raft {
            config,
            state_machine,
            stable_storage,
            message_sender,
            role: Role::Follower,
        }).await;

        raft.request_tick(Tick, election_timeout).await;
        raft
    }

    pub async fn broadcast() {
        
    }
}

#[async_trait::async_trait]
impl Handler<Tick> for Raft {
    async fn handle(&mut self, msg: Tick) {
        println!("{}: Tick timeout", self.config.self_id);
        match self.role {
            Role::Follower => {
                // starts elections
                // increment current term
                // becomes candidate
                // votes for itself
                // issues broadcast
                
            },
            Role::Candidate => {
                // 
            },
            Role::Leader => {

            },
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        match msg.content {
            RaftMessageContent::AppendEntries(a) => {

            },
            _ => {}
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        todo!()
    }
}

// TODO you can implement handlers of messages of other types for the Raft struct.
