//! A trait that accepts the same message types as a module of a specific type.
//!
//! Our core trait: `Handler<Message> for Module`, in essence, defines a relationship
//! `MayHandle(Module, Message)`.
//! Here, we define a helper trait `SenderTo<Module>` with a method accepting
//! a `Message` if and only if `MayHandle(Module, Message)`.
//! `SenderTo<Module>` is trivially implemented by `ModuleRef<Module>`.
//!
//! This module does a bit of type juggling to safely work on type-erased messages and modules.
use module_system::{Handler, Message, Module, ModuleRef};
use std::any::Any;
// Do not submit this file! Any changes will be ignored!

/// A trait for sending messages to move a `ModuleRef<T>` method into the traits space.
///
/// It is almost like [`Handler<M>`], but takes immutable reference to self.
#[async_trait::async_trait]
trait Sender<M: Message>: Send + Sync + 'static {
    #[allow(dead_code, reason = "This is more like a marker")]
    async fn send_message(&self, msg: M);
}

/// `Sender<M>` is implemented for `ModuleRef<T>` iff `MayHandle(T, M)`.
///
/// By the task definition, `ModuleRef<T>` has a method `send<M>` iff `MayHandle(T, M)`.
/// In effect, this trait moves a generic parameter from a generic method to the type level on
/// which the Rust compiler allows us to reason about.
#[async_trait::async_trait]
impl<M: Message, H: Handler<M>> Sender<M> for ModuleRef<H> {
    async fn send_message(&self, msg: M) {
        self.send(msg).await;
    }
}

/// `Sendee<T>` accepts any message `M` and any module `T`.
///
/// However...
#[async_trait::async_trait]
pub trait Sendee<T: Module>: Message + Any {
    async fn send(self: Box<Self>, sender: &ModuleRef<T>);
}

/// There exists a blanket implementation of `Sendee<T>` for `M` iff `MayHandle(T, M)`.
///
/// Consider this a sealed trait!
#[async_trait::async_trait]
impl<M: Message, H: Handler<M>> Sendee<H> for M
where
    ModuleRef<H>: Sender<M>,
{
    async fn send(self: Box<Self>, sender: &ModuleRef<H>) {
        sender.send(*self).await;
    }
}

/// Universal proxy trait that accepts all and only messages accepted by `T`.
///
/// This trait may be implemented on any type, and the method is not generic.
#[allow(dead_code)]
#[async_trait::async_trait]
pub trait SenderTo<T: Module>: Send + Sync + 'static {
    // This method is not named `send` to not create ambiguity with ModuleRef's method
    async fn send_message(&self, msg: Box<dyn Sendee<T>>);
    fn cloned_box(&self) -> BoxedModuleSender<T>;
}

/// `SenderTo<Module>` is trivially implemented by `ModuleRef<Module>`.
#[async_trait::async_trait]
impl<T: Module> SenderTo<T> for ModuleRef<T> {
    async fn send_message(&self, msg: Box<dyn Sendee<T>>) {
        msg.send(self).await;
    }
    fn cloned_box(&self) -> BoxedModuleSender<T> {
        Box::new(self.clone())
    }
}

/// A boxed version of [`SenderTo`] which may be passed by value.
///
/// The final interface is essentially the same as a `ModuleRef<T>`.
pub type BoxedModuleSender<T> = Box<dyn SenderTo<T>>;

/// `BoxedModuleSender<T>` is `Clone` just as `ModuleRef`
impl<T: Module> Clone for BoxedModuleSender<T> {
    fn clone(&self) -> Self {
        self.cloned_box()
    }
}

/// Finally, a trait bringing back the `ModuleRef` interface
///
/// It is not expected to be dyn-compatible (use `SenderTo` for that!).
/// This trait is only for implementing a halper method on `Box<dyn SenderTo<T>>`
#[async_trait::async_trait]
pub trait ModuleProxy<T: Module>: Module {
    async fn send(&mut self, msg: impl Sendee<T>);
}
#[async_trait::async_trait]
impl<T: Module> ModuleProxy<T> for BoxedModuleSender<T> {
    /// Send a message with the same interface as `ModuleRef<T>`
    ///
    /// Here, we add a stronger requirement of a `&mut self` reference
    /// to make sure we don't introduce `&Self: Send`, that is `Self: Sync`.
    /// If you need to use a shared reference, use just the `SenderTo` trait!
    async fn send(&mut self, msg: impl Sendee<T>) {
        self.send_message(Box::new(msg)).await;
    }
}
