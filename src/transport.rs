//! A transportation abstraction that supports multiplexing.
//!
//! The implementations of `Transport<M>` e.g. `UdpTransport<N>` may take
//! another type parameter `N`, and implement `Transport<M>` whenever
//! `M: Into<N>`. This would allow sessions to be multiplexed together by
//! sharing the same transportation, thus share the same network channel.
//!
//! For example, a session A may keep session B and session C as its
//! sub-sessions, and define its on-wire message format as:
//!
//! ```rust
//! #[derive(From)]
//! enum AMessage {
//!     B(BMessage),
//!     C(CMessage),
//!     // other variants that produced by A itself
//! }
//! ```
//!
//! Then a `UdpTransport<AMessage>` implements both `Transport<BMessage>` and
//! `Transport<CMessage>` by wrapping them into `AMessage` before transmitting,
//! so session A can shared the transport with the sub-sessions. By working with
//! `impl Transport<BMessage>` and `impl Transport<CMessage>`, sub-sessions are
//! unmodified for the fact that all sessions are sending messages in the form
//! of `AMessage`. Notice that the incoming messages for the sub-sessions will
//! end up in session A's event loop, so session A should relay the messages
//! into sub-sessions event loop manually, act as a middleware if necessary.

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

#[async_trait::async_trait]
pub trait Transport<M>
where
    Self: Clone + Send + Sync + 'static,
{
    fn addr(&self) -> Addr;

    async fn send_to(&self, destination: Addr, message: M) -> crate::Result<()>
    where
        M: Message;

    async fn send_to_all(
        &self,
        destinations: impl Iterator<Item = Addr> + Send,
        message: M,
    ) -> crate::Result<()>
    where
        M: Message,
    {
        for destination in destinations {
            if destination == self.addr() {
                crate::bail!("unexpected loopback message")
            }
            self.send_to(destination, message.clone()).await?
        }
        Ok(())
    }
}
