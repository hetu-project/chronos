//! Producer-oriented transportation abstraction.
//!
//! Transport, i.e., network message sender, can be expressed with sender of
//! `(destination address(es), message)` event, or submit handle of such event
//! if synchronous transportation is desired. Actually an `impl Transport<M>` is
//! a lot like a `EventSender<(Addr, M)>`, in the sense it is required to be
//! `Send + Sync + Clone + 'static` and methods take a `&self`. You can use an
//! `impl Transport<M>` in the same way as using an event sender (or a submit
//! handle).
//!
//! The purpose of a dedicated `Transport<M>` trait is to decouple
//! transportation implementations with transported message type `M`, and enable
//! a session that sends `M` to work with any `impl Transport<M>` consumers.
//! The transportation semantic may be arbitrary, e.g., send `M` as is, convert
//! it into another message type before sending, or even discard it.
//!
//! The alternative approach is to use service session to perform message type
//! based polymorphism (as described in the session overview document). A trait
//! could be better because it allow straightforward, even automatical adapting
//! from a `impl Transport<M>` to `impl Transport<N>`, which fits the common
//! scenario where the session's owner (potentially its super-session) instead
//! of the underlying transport provider wants to customize the transport
//! semantic.
//!
//! The cases that demand such decoupling include multiplexing, where a
//! sub-session should be working with any compatible super-session that uses
//! a on-wire message format that is unknown to the sub-session. In this case,
//! the sub-session can take a `impl Transport<M>` for its message type `M`, and
//! it's super-session's responsibility to convert the sub-session's messages
//! into its own format, i.e., provide `impl Transport<M>` for the sub-session.
//!
//! (Super-session is also responsible for decoding and dispatching sub-session
//! messages, which happens naturally in session logic and does not require an
//! abstraction.)
use borsh::{BorshDeserialize, BorshSerialize};

pub trait Message
where
    Self: Clone + Send + Sync + 'static,
{
}

impl<M> Message for M where M: Clone + Send + Sync + 'static {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub enum Addr {
    Socket(std::net::SocketAddr),
    Untyped(String),
}

pub trait Transport<M>
where
    Self: Clone + Send + Sync + 'static,
{
    fn addr(&self) -> Addr;

    fn send_to(&self, destination: Addr, message: M) -> impl std::future::Future<Output =  crate::Result<()>> + Send
    where
        M: Message;

    fn send_to_all(
        &self,
        destinations: impl Iterator<Item = Addr> + Send,
        message: M,
    ) -> impl std::future::Future<Output = crate::Result<()>> + Send
    where
        M: Message,
    {
        async move {
        for destination in destinations {
            if destination == self.addr() {
                crate::bail!("unexpected loopback message")
            }
            self.send_to(destination, message.clone()).await?
        }
        Ok(())}
    }

    fn wrap<W>(self, w: W) -> WrapTransport<Self, M, W> {
        WrapTransport(self, w, Default::default())
    }
}

pub trait Wrap<M, N> {
    fn wrap(&self, inner: M) -> N;
}

pub struct Into;

impl<M, N> Wrap<M, N> for Into
where
    M: std::convert::Into<N>,
{
    fn wrap(&self, inner: M) -> N {
        inner.into()
    }
}

#[derive(Debug)]
pub struct WrapTransport<T, M, W>(T, W, std::marker::PhantomData<M>);

impl<T, M, W> Clone for WrapTransport<T, M, W>
where
    T: Clone,
    W: Clone,
{
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone(), Default::default())
    }
}

impl<M, N, T, W> Transport<M> for WrapTransport<T, N, W>
where
    T: Transport<N>,
    W: Wrap<M, N> + Clone + Send + Sync + 'static,
    N: Message,
{
    fn addr(&self) -> Addr {
        self.0.addr()
    }

    async fn send_to(&self, destination: Addr, message: M) -> crate::Result<()>
    where
        M: Message,
    {
        self.0.send_to(destination, self.1.wrap(message)).await
    }

    async fn send_to_all(
        &self,
        destinations: impl Iterator<Item = Addr> + Send,
        message: M,
    ) -> crate::Result<()>
    where
        M: Message,
    {
        self.0.send_to_all(destinations, self.1.wrap(message)).await
    }
}
