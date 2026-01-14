// Do not submit this file! Any changes will be ignored!
use module_system::{Handler, ModuleRef};

/// Marker trait indicating that a broadcast implementation provides
/// guarantees specified in the assignment description.
pub(crate) trait ReliableBroadcast<const N: usize> {}

#[async_trait::async_trait]
pub(crate) trait ReliableBroadcastRef<const N: usize>: Send + Sync + 'static {
    async fn send(&self, msg: Operation);
}

#[async_trait::async_trait]
impl<T, const N: usize> ReliableBroadcastRef<N> for ModuleRef<T>
where
    T: ReliableBroadcast<N> + Handler<Operation> + Send,
{
    async fn send(&self, msg: Operation) {
        self.send(msg).await;
    }
}

/// Marker trait indicating that a client implementation
/// follows specification from the assignment description.
pub(crate) trait EditorClient {}

#[async_trait::async_trait]
pub(crate) trait ClientRef: Send + Sync + 'static {
    async fn send(&self, msg: Edit);
}

#[async_trait::async_trait]
impl<T> ClientRef for ModuleRef<T>
where
    T: EditorClient + Handler<Edit> + Send,
{
    async fn send(&self, msg: Edit) {
        self.send(msg).await;
    }
}

/// Actions (edits) which can be applied to a text.
#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Debug))]
pub(crate) enum Action {
    /// Insert the character at the position.
    Insert { idx: usize, ch: char },
    /// Delete a character at the position.
    Delete { idx: usize },
    /// A _do nothing_ operation. `Nop` cannot be issued by a client.
    /// `Nop` can only be issued by a process or result from a transformation.
    Nop,
}

impl Action {
    /// Apply the action to the text.
    pub(crate) fn apply_to(&self, text: &mut String) {
        match self {
            Action::Insert { idx, ch } => {
                text.insert(*idx, *ch);
            }
            Action::Delete { idx } => {
                text.remove(*idx);
            }
            Action::Nop => {
                // Do nothing.
            }
        }
    }
}

/// Client's request to edit the text.
#[derive(Clone)]
pub(crate) struct EditRequest {
    /// Total number of operations a client has applied to its text so far.
    pub(crate) num_applied: usize,
    /// Action (edit) to be applied to a text.
    pub(crate) action: Action,
}

/// Response to a client with action (edit) it should apply to its text.
#[derive(Clone)]
pub(crate) struct Edit {
    pub(crate) action: Action,
}

#[derive(Clone)]
pub(crate) struct Operation {
    /// Rank of a process which issued this operation.
    pub(crate) process_rank: usize,
    /// Action (edit) to be applied to a text.
    pub(crate) action: Action,
}
