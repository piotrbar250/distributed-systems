use tokio::{select, sync::{mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel}, watch}, task::JoinHandle, time::{self, Duration}};

pub trait Message: Send + 'static {}
impl<T: Send + 'static> Message for T {}

pub trait Module: Send + 'static {}
impl<T: Send + 'static> Module for T {}

/// A trait for modules capable of handling messages of type `M`.
#[async_trait::async_trait]
pub trait Handler<M: Message>: Module {
    /// Handles the message.
    async fn handle(&mut self, msg: M);
}

#[async_trait::async_trait]
trait Handlee<T: Module>: Message {
    async fn get_handled(self: Box<Self>, module: &mut T);
}

#[async_trait::async_trait]
impl<M: Message, T: Handler<M>> Handlee<T> for M {
    async fn get_handled(self: Box<Self>, module: &mut T) {
        module.handle(*self).await;
    }
}

/// A handle returned by `ModuleRef::request_tick()` can be used to stop sending further ticks.
#[non_exhaustive]
pub struct TimerHandle {
    tx_tick_stop: UnboundedSender<()>,
}

impl TimerHandle {
    /// Stops the sending of ticks resulting from the corresponding call to `ModuleRef::request_tick()`.
    /// If the ticks are already stopped, does nothing.
    pub async fn stop(&self) {
        _ = self.tx_tick_stop.send(());
    }
}

#[non_exhaustive]
pub struct System {
    handles: Vec<JoinHandle<()>>,
    tx_shutdown: watch::Sender<bool>,
    rx_shutdown: watch::Receiver<bool>,
    tx_tick_shutdown: watch::Sender<bool>,
    rx_tick_shutdown: watch::Receiver<bool>,
}

impl System {

    async fn module_task_worker<T>(
        mut rx: UnboundedReceiver<Box<dyn Handlee<T>>>,
        mut rx_shutdown: watch::Receiver<bool>,
        mut module: T
    )
    where
        T: Module
    {
        loop {
            select!{
                biased;
                _ = rx_shutdown.changed() => {
                    break;
                }
                result = rx.recv() => {
                    match result {
                        Some(msg) => msg.get_handled(&mut module).await,
                        None => break,
                    }
                }
            }
        }
    }

    /// Registers the module in the system.
    ///
    /// Accepts a closure constructing the module (allowing it to hold a reference to itself).
    /// Returns a `ModuleRef`, which can be used then to send messages to the module.
    pub async fn register_module<T: Module>(
        &mut self,
        module_constructor: impl FnOnce(ModuleRef<T>) -> T,
    ) -> ModuleRef<T> 
    {
        let (tx, rx) = unbounded_channel::<Box<dyn Handlee<T>>>();

        let module_ref = ModuleRef::<T>{
            tx,
            rx_tick_shutdown: self.rx_tick_shutdown.clone(),
        };
        
        let module = module_constructor(module_ref.clone());

        self.handles.push(tokio::spawn(Self::module_task_worker(
            rx, self.rx_shutdown.clone(), module
        )));

        module_ref
    }

    /// Creates and starts a new instance of the system.
    pub async fn new() -> Self {
        let (tx_shutdown, rx_shutdown) = watch::channel(false);
        let (tx_tick_shutdown, rx_tick_shutdown) = watch::channel(false);
        Self {
            handles: Vec::new(),
            tx_shutdown,
            rx_shutdown,
            tx_tick_shutdown,
            rx_tick_shutdown,
        }
    }

    /// Gracefully shuts the system down.
    pub async fn shutdown(&mut self) {
        _ = self.tx_tick_shutdown.send(true);
        _ = self.tx_shutdown.send(true);
        for h in self.handles.iter_mut() {
            _ = h.await;
        }
    }
}

/// A reference to a module used for sending messages.
#[non_exhaustive]
pub struct ModuleRef<T: Module>
where
    Self: Send,
{
    tx: UnboundedSender<Box<dyn Handlee<T>>>,
    rx_tick_shutdown: watch::Receiver<bool>,
}

impl<T: Module> ModuleRef<T> {
    /// Sends the message to the module.
    pub async fn send<M: Message>(&self, msg: M)
    where
        T: Handler<M>,
    {
        _ = self.tx.send(Box::new(msg));
    }

    /// Schedules a message to be sent to the module periodically with the given interval.
    /// The first tick is sent after the interval elapses.
    /// Every call to this function results in sending new ticks and does not cancel
    /// ticks resulting from previous calls.
    pub async fn request_tick<M>(&self, message: M, delay: Duration) -> TimerHandle
    where
        M: Message + Clone,
        T: Handler<M>,
    {
        let (tx_tick_stop, mut rx_tick_stop) = unbounded_channel::<>();

        let tx_tick_stop_handle = tx_tick_stop.clone();

        let module_ref = self.clone();
        let mut rx_tick_shutdown = self.rx_tick_shutdown.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(delay);
            let _tx_tick_stop_local_reference = tx_tick_stop;

            interval.tick().await;

            loop {
                select! {
                    biased;
                    _ = rx_tick_stop.recv() => break,
                    _ = rx_tick_shutdown.changed() => break,
                    _ = interval.tick() => module_ref.send(message.clone()).await,
                }
            }
        });

        TimerHandle {tx_tick_stop: tx_tick_stop_handle}
    }
    
}

impl<T: Module> Clone for ModuleRef<T> {
    /// Creates a new reference to the same module.
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx_tick_shutdown: self.rx_tick_shutdown.clone(),
        }
    }
}
