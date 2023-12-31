//! Common structures and methods for implementing replication protocols. Kind
//! of like `Configuration` and `Replica` and `Client` base classes in
//! SpecPaxos.

use std::{collections::HashMap, net::SocketAddr, time::Duration};

use tokio::time::{timeout, Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    channel::SubmitHandle,
    crypto::{Signer, Verifier},
    task::BackgroundSpawner,
    transport::{Addr, Message},
    Transport,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AddrBook {
    Socket(SocketAddrBook),
    Untyped(UntypedAddrBook),
}

impl AddrBook {
    pub fn replica_addr(&self, id: u8) -> crate::Result<Addr> {
        match self {
            Self::Socket(book) => book.replica_addr(id),
            Self::Untyped(book) => Ok(book.replica_addr(id)),
        }
    }

    pub fn replica_addrs(&self) -> impl Iterator<Item = Addr> {
        // TODO make it more efficient and elegant
        match self {
            Self::Socket(book) => book.replica_addrs().collect::<Vec<_>>(),
            Self::Untyped(book) => book.replica_addrs().collect(),
        }
        .into_iter()
    }

    pub fn client_addr(&self, id: u32) -> crate::Result<Addr> {
        match self {
            Self::Socket(book) => book.client_addr(id),
            Self::Untyped(book) => Ok(book.client_addr(id)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SocketAddrBook {
    replica_addrs: HashMap<u8, SocketAddr>,
    client_addrs: HashMap<u32, SocketAddr>,
}

impl From<replication_control_messages::AddrBook> for SocketAddrBook {
    fn from(value: replication_control_messages::AddrBook) -> Self {
        Self {
            replica_addrs: value.replica_addrs,
            client_addrs: value.client_addrs,
        }
    }
}

impl SocketAddrBook {
    fn replica_addr(&self, id: u8) -> crate::Result<Addr> {
        Ok(Addr::Socket(
            *self
                .replica_addrs
                .get(&id)
                .ok_or(crate::err!("replica {id} address not found"))?,
        ))
    }

    pub fn remove_addr(&mut self, id: u8) -> crate::Result<SocketAddr> {
        self.replica_addrs
            .remove(&id)
            .ok_or(crate::err!("replica {id} address not found"))
    }

    // TODO cache addresses and remove lifetime bound?
    fn replica_addrs(&self) -> impl Iterator<Item = Addr> + '_ {
        self.replica_addrs.values().copied().map(Addr::Socket)
    }

    fn client_addr(&self, id: u32) -> crate::Result<Addr> {
        Ok(Addr::Socket(
            *self
                .client_addrs
                .get(&id)
                .ok_or(crate::err!("client {id} address not found"))?,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UntypedAddrBook {
    pub num_replica: usize,
    pub replica_id: u8,
}

impl UntypedAddrBook {
    fn replica_addr(&self, id: u8) -> Addr {
        Addr::Untyped(format!("replica-{id}"))
    }

    fn replica_addrs(&self) -> impl Iterator<Item = Addr> {
        (0..self.replica_id)
            .chain(self.replica_id + 1..self.num_replica as _)
            .map(|i| Addr::Untyped(format!("replica-{i}")))
    }

    fn client_addr(&self, id: u32) -> Addr {
        Addr::Untyped(format!("client-{id}"))
    }
}

#[derive(Debug)]
pub struct Client {
    pub id: u32,
    pub num_replica: usize,
    pub num_faulty: usize,
    pub addr_book: AddrBook,
    pub retry_interval: Duration,
}

impl Client {
    pub fn primary_replica(&self, view_num: u32) -> u8 {
        (view_num as usize % self.num_replica) as _
    }
}

pub trait Workload {
    fn session(&mut self, invoke_handle: SubmitHandle<Vec<u8>, Vec<u8>>)
        -> impl std::future::Future<Output = crate::Result<()>>;
}

pub async fn close_loop_session(
    mut workload: impl Workload,
    duration: Duration,
    invoke_handle: SubmitHandle<Vec<u8>, Vec<u8>>,
) -> crate::Result<Vec<Duration>> {
    let deadline = Instant::now() + duration;
    let mut latencies = Vec::new();
    while deadline.elapsed() == Duration::ZERO {
        let start = Instant::now();
        timeout(
            Duration::from_millis(10),
            workload.session(invoke_handle.clone()),
        )
        .await??;
        latencies.push(start.elapsed());
    }
    Ok(latencies)
}

#[derive(Debug)]
pub struct Replica {
    pub id: u8,
    pub num_replica: usize,
    pub num_faulty: usize,
    pub app: SubmitHandle<Vec<u8>, Vec<u8>>,
    pub spawner: BackgroundSpawner,
    pub signer: Signer,
    pub verifiers: HashMap<u8, Verifier>,
    pub addr_book: AddrBook,
    pub stop: CancellationToken,
}

impl Replica {
    pub fn primary(&self, view_num: u32) -> u8 {
        (view_num as usize % self.num_replica) as _
    }

    pub fn is_primary(&self, view_num: u32) -> bool {
        self.id == self.primary(view_num)
    }

    pub fn send_to_replica<M>(&self, id: u8, message: M, transport: impl Transport<M>)
    where
        M: Message,
    {
        assert_ne!(id, self.id);
        // a better design may be failing outside of spawning
        // currently doing this because it should make little difference and not bother to modify
        // exist protocol code
        let destination = self.addr_book.replica_addr(id);
        self.spawner
            .spawn(async move { transport.send_to(destination?, message).await });
    }

    pub fn send_to_all_replica<M>(&self, message: M, transport: impl Transport<M>)
    where
        M: Message,
    {
        let destinations = self.addr_book.replica_addrs();
        self.spawner
            .spawn(async move { transport.send_to_all(destinations, message).await });
    }

    pub fn send_to_client<M>(&self, id: u32, message: M, transport: impl Transport<M>)
    where
        M: Message,
    {
        let destination = self.addr_book.client_addr(id);
        self.spawner
            .spawn(async move { transport.send_to(destination?, message).await });
    }
}
