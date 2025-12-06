use crate::relay_util::{BoxedModuleSender, ModuleProxy};
use module_system::Handler;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

// As always, you should not modify the public types unless explicitly asked!

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub(crate) enum ProductType {
    Electronics,
    Toys,
    Books,
}

#[derive(Clone)]
pub(crate) struct StoreMsg {
    sender: BoxedModuleSender<DistributedStore>,
    content: StoreMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum StoreMsgContent {
    /// Transaction Manager initiates voting for the transaction.
    RequestVote(Transaction),
    /// If every process is ok with transaction, TM issues commit.
    Commit,
    /// System-wide abort.
    Abort,
}

#[derive(Clone)]
pub(crate) struct NodeMsg {
    content: NodeMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum NodeMsgContent {
    /// Process replies to TM whether it can/cannot commit the transaction.
    RequestVoteResponse(TwoPhaseResult),
    /// Process acknowledges to TM committing/aborting the transaction.
    FinalizationAck,
}

pub(crate) struct TransactionMessage {
    /// Request to change price.
    pub(crate) transaction: Transaction,

    /// Called after 2PC completes (i.e., the transaction was decided to be
    /// committed/aborted by `DistributedStore`). This must be called after responses
    /// from all processes acknowledging commit or abort are collected.
    #[allow(clippy::type_complexity, reason = "Single use")]
    pub(crate) completed_callback:
        Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TwoPhaseResult {
    Ok,
    Abort,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Product {
    pub(crate) identifier: Uuid,
    pub(crate) pr_type: ProductType,
    pub(crate) price: u64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) pr_type: ProductType,
    pub(crate) shift: i32,
}

#[derive(Debug)]
pub(crate) struct ProductPriceQuery {
    pub(crate) product_ident: Uuid,
    pub(crate) result_sender: Sender<ProductPrice>,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct ProductPrice(pub(crate) Option<u64>);

/// Message which disables a node. Used for testing.
pub(crate) struct Disable;

/// `DistributedStore`.
/// This structure serves as TM.
pub(crate) struct DistributedStore {
    // Add any fields you need.
}

impl DistributedStore {
    pub(crate) fn new(
        nodes: Vec<BoxedModuleSender<Node>>,
        self_ref: BoxedModuleSender<Self>,
    ) -> Self {
        unimplemented!()
    }
}

/// Node of `DistributedStore`.
/// This structure serves as a process of the distributed system.
// Add any fields you need.
pub(crate) struct Node {
    products: Vec<Product>,
    pending_transaction: Option<Transaction>,
    enabled: bool,
}

impl Node {
    pub(crate) fn new(products: Vec<Product>) -> Self {
        Self {
            products,
            pending_transaction: None,
            enabled: true,
        }
    }
}

#[async_trait::async_trait]
impl Handler<TransactionMessage> for DistributedStore {
    async fn handle(&mut self, msg: TransactionMessage) {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl Handler<NodeMsg> for DistributedStore {
    async fn handle(&mut self, msg: NodeMsg) {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl Handler<StoreMsg> for Node {
    async fn handle(&mut self, msg: StoreMsg) {
        if self.enabled {
            let mut sender = msg.sender;
            unimplemented!()
        }
    }
}

#[async_trait::async_trait]
impl Handler<ProductPriceQuery> for Node {
    async fn handle(&mut self, msg: ProductPriceQuery) {
        if self.enabled {
            unimplemented!()
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for Node {
    async fn handle(&mut self, _msg: Disable) {
        self.enabled = false;
    }
}
