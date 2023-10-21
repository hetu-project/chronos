use std::{
    net::SocketAddr,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use serde::Serialize;

use super::{
    crypto::{DigestHash, Sign},
    Addr,
};

pub type ReplicaIndex = u8;
pub type ClientIndex = u16;

#[derive(Debug, Clone)]
pub struct Config {
    pub num_faulty: usize,
    pub client_addrs: Vec<Addr>,
    pub replica_addrs: Vec<Addr>,
    pub multicast_addr: Option<Addr>,
}

impl Config {
    pub fn new_socket(
        client_addrs: impl IntoIterator<Item = SocketAddr>,
        replica_addrs: impl IntoIterator<Item = SocketAddr>,
        num_faulty: usize,
    ) -> Self {
        let client_addrs = client_addrs.into_iter().map(Addr::Socket).collect();
        let replica_addrs = replica_addrs
            .into_iter()
            .map(Addr::Socket)
            .collect::<Vec<_>>();
        assert!(num_faulty * 3 < replica_addrs.len());
        Self {
            num_faulty,
            client_addrs,
            replica_addrs,
            multicast_addr: None,
            // simplified symmetrical keys setup
            // also reduce client-side overhead a little bit by only need to sign once for broadcast
        }
    }
}

#[derive(Debug)]
pub struct Context<M> {
    pub inner: crate::context::Context<M>,
    pub config: Arc<Config>,
}

impl<M> crate::context::Context<M> {
    pub fn into_replication(self, config: impl Into<Arc<Config>>) -> Context<M> {
        Context {
            inner: self,
            config: config.into(),
        }
    }
}

impl<M> Deref for Context<M> {
    type Target = crate::context::Context<M>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> DerefMut for Context<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<M> Context<M> {
    pub fn num_faulty(&self) -> usize {
        self.config.num_faulty
    }

    pub fn num_replica(&self) -> usize {
        self.config.replica_addrs.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum To {
    Addr(Addr),
    Addrs(Vec<Addr>),

    Replica(ReplicaIndex),
    Client(ClientIndex),
    Clients(Vec<ClientIndex>),
    AllReplica,
    Loopback,
    AllReplicaWithLoopback,
}

impl<M> Context<M> {
    pub fn send<N>(&mut self, to: To, message: N)
    where
        M: Sign<N> + Serialize + Clone,
    {
        use crate::context::To::*;
        match to {
            To::Addr(addr) => self.inner.send(Addr(addr), message),
            To::Addrs(addrs) => self.inner.send(Addrs(addrs), message),
            To::Client(index) => self
                .inner
                .send(Addr(self.config.client_addrs[index as usize]), message),
            To::Clients(indexes) => self.inner.send(
                Addrs(
                    indexes
                        .into_iter()
                        .map(|index| self.config.client_addrs[index as usize])
                        .collect(),
                ),
                message,
            ),
            To::Replica(index) => self
                .inner
                .send(Addr(self.config.replica_addrs[index as usize]), message),
            To::AllReplica => self.inner.send(
                Addrs(
                    self.config
                        .replica_addrs
                        .iter()
                        .copied()
                        .filter(|&addr| addr != self.inner.addr())
                        .collect(),
                ),
                message,
            ),
            To::Loopback => self.inner.send(Loopback, message),
            To::AllReplicaWithLoopback => self.inner.send(
                AddrsWithLoopback(
                    self.config
                        .replica_addrs
                        .iter()
                        .copied()
                        .filter(|&addr| addr != self.inner.addr())
                        .collect(),
                ),
                message,
            ),
        }
    }

    pub fn send_ordered_multicast(&self, message: impl Serialize + DigestHash) {
        self.inner.send_buf(
            self.config.multicast_addr.unwrap(),
            super::ordered_multicast::serialize(&message),
        )
    }
}
