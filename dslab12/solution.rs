use crate::domain::{Action, ClientRef, Edit, EditRequest, Operation, ReliableBroadcastRef};
use module_system::Handler;

impl Operation {
    // Add any methods you need.
}

/// Process of the system.
pub(crate) struct Process<const N: usize> {
    /// Rank of the process.
    rank: usize,
    /// Reference to the broadcast module.
    broadcast: Box<dyn ReliableBroadcastRef<N>>,
    /// Reference to the process's client.
    client: Box<dyn ClientRef>,
    // Add any fields you need.
}

impl<const N: usize> Process<N> {
    pub(crate) fn new(
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> Self {
        Self {
            rank,
            broadcast,
            client,
            // Add any fields you need.
        }
    }

    // Add any methods you need.
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, msg: Operation) {
        todo!("Handle operation issued by other process.");
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, request: EditRequest) {
        todo!("Handle edit request from the client.");
    }
}
