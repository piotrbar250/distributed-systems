use module_system::{Handler, ModuleRef, System, TimerHandle};
use std::collections::HashSet;
use tokio::time::Duration;
use uuid::Uuid;

// As always, do not invalidate the public interfaces and modify only explicitly marked areas.
// You may add new derives and private methods as needed.

/// State of a Raft process.
/// It shall be kept in stable storage, and updated before replying to messages.
#[derive(Default, Clone, Copy, Debug)]
pub(crate) struct ProcessState {
    /// Number of the current term. `0` at boot.
    pub(crate) current_term: u64,
    /// Identifier of a process which has received this process' vote.
    /// `None` if this process has not voted in this term.
    voted_for: Option<Uuid>,
    /// Identifier of a process which is thought to be the leader.
    leader_id: Option<Uuid>,
}

/// Configuration of a Raft process.
#[derive(Copy, Clone, Debug)]
pub(crate) struct ProcessConfig {
    /// UUID of the process.
    pub(crate) self_id: Uuid,
    /// Timeout used by this process.
    /// In contrast to real-world applications, in this assignment,
    /// the timeout shall not be randomized!
    pub(crate) election_timeout: Duration,
    /// Number of processes in the system.
    /// In the assignment, there is a constant number of processes.
    pub(crate) processes_count: usize,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct RaftMessage {
    pub(crate) header: RaftMessageHeader,
    pub(crate) content: RaftMessageContent,
}

#[derive(Clone)]
struct Timeout;

struct Init;

/// Message disabling a process. Used for testing to simulate failures.
pub(crate) struct Disable;

#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) struct RaftMessageHeader {
    /// Term of the process which issues the message.
    pub(crate) term: u64,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum RaftMessageContent {
    Heartbeat {
        /// Id of the process issuing the message, which claims to be the leader.
        leader_id: Uuid,
    },
    HeartbeatResponse,
    RequestVote {
        /// Id of the process issuing the message, which is a candidate.
        candidate_id: Uuid,
    },
    RequestVoteResponse {
        /// Whether the vote was granted or not.
        granted: bool,
        /// Id of the process issuing the message, which grants (or not) the vote.
        source: Uuid,
    },
}

pub(crate) trait StableStorage: Send {
    fn put(&mut self, state: &ProcessState);

    fn get(&self) -> Option<ProcessState>;
}

#[async_trait::async_trait]
pub(crate) trait Sender: Send + Sync {
    async fn send(&self, target: &Uuid, msg: RaftMessage);

    async fn broadcast(&self, msg: RaftMessage);
}

/// Process of Raft.
pub struct Raft {
    state: ProcessState,
    config: ProcessConfig,
    storage: Box<dyn StableStorage>,
    sender: Box<dyn Sender>,
    process_type: ProcessType,
    timer_handle: Option<TimerHandle>,
    enabled: bool,
    self_ref: ModuleRef<Self>,
}

impl Raft {
    pub(crate) async fn new(
        system: &mut System,
        config: ProcessConfig,
        storage: Box<dyn StableStorage>,
        sender: Box<dyn Sender>,
    ) -> ModuleRef<Self> {
        let state = storage.get().unwrap_or_default();

        let self_ref = system
            .register_module(|self_ref| Self {
                state,
                config,
                storage,
                sender,
                process_type: Default::default(),
                timer_handle: None,
                enabled: true,
                self_ref,
            })
            .await;
        self_ref.send(Init).await;
        self_ref
    }

    // -------------
    // Some helper methods are already implemented to make sure they're correct.
    // -------------

    async fn reset_timer(&mut self, interval: Duration) {
        if let Some(handle) = self.timer_handle.take() {
            handle.stop().await;
        }
        self.timer_handle = Some(self.self_ref.request_tick(Timeout, interval).await);
    }

    /// Set the process's term to the higher number.
    fn update_term(&mut self, new_term: u64) {
        assert!(self.state.current_term < new_term);
        self.state.current_term = new_term;
        self.state.voted_for = None;
        self.state.leader_id = None;
        // No reliable state update called here, must be called separately.
    }

    /// Reliably save the state.
    fn update_state(&mut self) {
        self.storage.put(&self.state);
    }

    /// Common message processing.
    fn check_for_higher_term(&mut self, msg: &RaftMessage) {
        if msg.header.term > self.state.current_term {
            self.update_term(msg.header.term);
            self.process_type = ProcessType::Follower;
        }
    }

    /// Broadcast heartbeat.
    async fn broadcast_heartbeat(&mut self) {
        self.sender
            .broadcast(RaftMessage {
                header: RaftMessageHeader {
                    term: self.state.current_term,
                },
                content: RaftMessageContent::Heartbeat {
                    leader_id: self.config.self_id,
                },
            })
            .await;
        RaftMessageContent::RequestVote { candidate_id: () }
    }

    // -------------
    // Example implementation is given for the Heartbeat message
    // -------------

    /// Handle the received heartbeat.
    async fn handle_heartbeat(&mut self, leader_id: Uuid, leader_term: u64) {
        if leader_term >= self.state.current_term {
            self.state.leader_id = Some(leader_id);
            self.update_state();

            // Update the volatile state:
            match &mut self.process_type {
                ProcessType::Follower => {
                    self.reset_timer(self.config.election_timeout).await;
                }
                ProcessType::Candidate { .. } => {
                    self.process_type = ProcessType::Follower;
                    self.reset_timer(self.config.election_timeout).await;
                }
                ProcessType::Leader => {
                    log::debug!("Ignore, heartbeat from self");
                }
            }
        }

        // Response is always sent, so information about the term is disseminated:
        self.sender
            .send(
                &leader_id,
                RaftMessage {
                    header: RaftMessageHeader {
                        term: self.state.current_term,
                    },
                    content: RaftMessageContent::HeartbeatResponse,
                },
            )
            .await;
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, _msg: Init) {
        if self.enabled {
            self.reset_timer(self.config.election_timeout).await;
        }
    }
}

/// Handle timer timeout.
#[async_trait::async_trait]
impl Handler<Timeout> for Raft {
    async fn handle(&mut self, _: Timeout) {
        if self.enabled {
            match &mut self.process_type {
                ProcessType::Follower => {
                    unimplemented!();
                }
                ProcessType::Candidate { .. } => {
                    unimplemented!();
                }
                ProcessType::Leader => {
                    unimplemented!();
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        if self.enabled {
            // Reset the term and become a follower if we're outdated:
            self.check_for_higher_term(&msg);
            // Received term is <= our term

            // TODO message specific processing. Heartbeat is given as an example:
            match (&mut self.process_type, msg.content) {
                (_, RaftMessageContent::Heartbeat { leader_id }) => {
                    self.handle_heartbeat(leader_id, msg.header.term).await;
                },

                (_, RaftMessageContent::RequestVote { candidate_id }) => {
                    // jesli ten term ktory dostales >= current term && self state voted for
                    // glosujesz
                    // 
                },
                (_, RaftMessageContent::RequestVoteResponse { granted, source }) => {
                    // jesli ten term ktory dostales >= current term && self state voted for
                    // glosujesz
                    // 
                },

                _ => {
                    unimplemented!();
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for Raft {
    async fn handle(&mut self, _: Disable) {
        self.enabled = false;
    }
}

/// State of a Raft process with a corresponding (volatile) information.
#[derive(Default)]
enum ProcessType {
    #[default]
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader,
}
