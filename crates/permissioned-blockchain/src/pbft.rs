use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    client::BoxedConsume,
    common::{Block, BlockDigest, Chain, Request, Timer},
    context::{
        crypto::{Sign, Signed, Verify},
        Addr, Receivers,
    },
    App, ClientIndex, Context, ReplicaIndex, To,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Message {
    Request(Signed<Request>),
    Reply(Signed<Reply>),
    PrePrepare(Signed<PrePrepare>),
    Prepare(Signed<Prepare>),
    Commit(Signed<Commit>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reply {
    request_num: u32,
    result: Vec<u8>,
    block_digest: BlockDigest,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PrePrepare {
    view_num: u32,
    block: Block,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Prepare {
    view_num: u32,
    block_digest: BlockDigest,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Commit {
    view_num: u32,
    block_digest: BlockDigest,
    replica_index: ReplicaIndex,
}

#[derive(Debug)]
pub struct Client {
    index: ClientIndex,
    shared: Arc<Mutex<ClientShared>>,
}

#[derive(Debug)]
struct ClientShared {
    context: Context<Message>,
    request_num: u32,
    invoke: Option<ClientInvoke>,
    resend_timer: Timer,
}

#[derive(Debug)]
struct ClientInvoke {
    op: Vec<u8>,
    replies: HashMap<ReplicaIndex, Reply>,
    consume: BoxedConsume,
}

impl Client {
    pub fn new(context: Context<Message>, index: ClientIndex) -> Self {
        Self {
            index,
            shared: Arc::new(Mutex::new(ClientShared {
                context,
                request_num: 0,
                invoke: None,
                resend_timer: Timer::new(Duration::from_millis(100)),
            })),
        }
    }
}

impl crate::Client for Client {
    type Message = Message;

    fn invoke(&self, op: Vec<u8>, consume: impl Into<BoxedConsume>) {
        let shared = &mut *self.shared.lock().unwrap();
        assert!(shared.invoke.is_none());
        shared.request_num += 1;
        shared.invoke = Some(ClientInvoke {
            op: op.clone(),
            replies: Default::default(),
            consume: consume.into(),
        });
        let request = Request {
            client_index: self.index,
            request_num: shared.request_num,
            op,
        };
        // TODO
        shared.context.send(To::Replica(0), request);
        shared.resend_timer.set(&mut shared.context)
    }

    fn handle(&self, message: Self::Message) {
        let Message::Reply(message) = message else {
            unimplemented!()
        };
        let mut shared = self.shared.lock().unwrap();
        if message.request_num != shared.request_num {
            return;
        }
        let Some(invoke) = &mut shared.invoke else {
            return;
        };
        invoke
            .replies
            .insert(message.replica_index, Reply::clone(&message));
        let num_match = invoke
            .replies
            .values()
            .filter(|reply| {
                (reply.block_digest, &reply.result) == (message.block_digest, &message.result)
            })
            .count();
        assert!(num_match <= shared.context.num_faulty() + 1);
        if num_match == shared.context.num_faulty() + 1 {
            {
                let shared = &mut *shared;
                shared.resend_timer.unset(&mut shared.context);
            }
            let invoke = shared.invoke.take().unwrap();
            drop(shared);

            let _op = invoke.op;
            invoke.consume.apply(message.inner.result)
        }
    }
}

#[derive(Debug)]
pub struct Replica {
    context: Context<Message>,
    index: ReplicaIndex,
    view_num: u32,
    requests: Vec<Request>,
    pre_prepares: HashMap<BlockDigest, Signed<PrePrepare>>,
    prepare_certificates: HashMap<BlockDigest, HashMap<ReplicaIndex, Signed<Prepare>>>,
    commit_certificates: HashMap<BlockDigest, HashMap<ReplicaIndex, Signed<Commit>>>,
    chain: Chain,
    app: App,
}

impl Replica {
    pub fn new(context: Context<Message>, index: ReplicaIndex, app: App) -> Self {
        Self {
            context,
            index,
            view_num: 0,
            requests: Default::default(),
            pre_prepares: Default::default(),
            prepare_certificates: Default::default(),
            commit_certificates: Default::default(),
            chain: Default::default(),
            app,
        }
    }
}

impl Receivers for Replica {
    type Message = Message;

    fn handle(&mut self, receiver: Addr, remote: Addr, message: Self::Message) {
        // println!("{message:02x?}");
        assert_eq!(receiver, self.context.addr());
        match message {
            Message::Request(message) => self.handle_request(remote, message),
            Message::PrePrepare(message) => self.handle_pre_prepare(remote, message),
            Message::Prepare(message) => self.handle_prepare(remote, message),
            Message::Commit(message) => self.handle_commit(remote, message),
            _ => unimplemented!(),
        }
    }

    fn on_timer(&mut self, receiver: Addr, _: crate::context::TimerId) {
        assert_eq!(receiver, self.context.addr());
        todo!()
    }

    fn handle_loopback(&mut self, receiver: Addr, message: Self::Message) {
        assert_eq!(receiver, self.context.addr());
        match message {
            Message::PrePrepare(message) => {
                self.pre_prepares.insert(message.block.digest(), message);
            }
            Message::Prepare(message) => self.insert_prepare(message),
            Message::Commit(message) => self.insert_commit(message),
            _ => unimplemented!(),
        }
    }

    fn on_pace(&mut self) {
        if self.index == self.primary_index() && !self.requests.is_empty() {
            self.do_propose()
        }
    }
}

impl Replica {
    fn primary_index(&self) -> ReplicaIndex {
        (self.view_num as usize % self.context.num_replica()) as _
    }

    fn handle_request(&mut self, _remote: Addr, message: Signed<Request>) {
        if self.index != self.primary_index() {
            // TODO
            return;
        }

        // TODO check resend

        self.requests.push(message.inner);
    }

    fn handle_pre_prepare(&mut self, _remote: Addr, message: Signed<PrePrepare>) {
        if message.view_num < self.view_num {
            return;
        }

        if message.view_num > self.view_num {
            // TODO
            return;
        }

        let block_digest = message.block.digest();
        self.pre_prepares.insert(block_digest, message);
        assert_ne!(self.index, self.primary_index());
        let prepare = Prepare {
            view_num: self.view_num,
            block_digest,
            replica_index: self.index,
        };
        self.context.send(To::AllReplicaWithLoopback, prepare)
    }

    fn handle_prepare(&mut self, _remote: Addr, message: Signed<Prepare>) {
        if message.view_num < self.view_num {
            return;
        }

        if message.view_num > self.view_num {
            //
            return;
        }

        self.insert_prepare(message);
    }

    fn handle_commit(&mut self, _remote: Addr, message: Signed<Commit>) {
        if message.view_num < self.view_num {
            return;
        }

        if message.view_num > self.view_num {
            //
            return;
        }

        self.insert_commit(message);
    }

    fn do_propose(&mut self) {
        assert_eq!(self.index, self.primary_index());
        let pre_prepare = PrePrepare {
            view_num: self.view_num,
            block: self.chain.propose(&mut self.requests),
        };
        self.context.send(To::AllReplicaWithLoopback, pre_prepare)
    }

    fn insert_prepare(&mut self, prepare: Signed<Prepare>) {
        let block_digest = prepare.block_digest;
        let prepare_certificate = self.prepare_certificates.entry(block_digest).or_default();
        #[allow(clippy::int_plus_one)]
        {
            assert!(
                prepare_certificate.len() + 1
                    <= self.context.num_replica() - self.context.num_faulty()
            );
        }
        // corner case handling: receive `PrePrepare` after sufficient `Prepare`s
        if prepare_certificate.len() + 1 == self.context.num_replica() - self.context.num_faulty() {
            if prepare.replica_index != self.index {
                return;
            }
        } else {
            prepare_certificate.insert(prepare.replica_index, prepare);
        }
        if self.pre_prepares.contains_key(&block_digest)
            && prepare_certificate.len() + 1
                == self.context.num_replica() - self.context.num_faulty()
        {
            let commit = Commit {
                view_num: self.view_num,
                block_digest,
                replica_index: self.index,
            };
            self.context
                .send(To::AllReplicaWithLoopback, commit.clone())
        }
    }

    fn insert_commit(&mut self, commit: Signed<Commit>) {
        let block_digest = commit.block_digest;
        let commit_certificate = self.commit_certificates.entry(block_digest).or_default();
        assert!(commit_certificate.len() <= self.context.num_replica() - self.context.num_faulty());
        if commit_certificate.len() == self.context.num_replica() - self.context.num_faulty() {
            return;
        }
        commit_certificate.insert(commit.replica_index, commit);
        if commit_certificate.len() >= self.context.num_replica() - self.context.num_faulty() {
            self.do_execute(block_digest);
        }
    }

    fn do_execute(&mut self, block_digest: BlockDigest) {
        let mut block = &self.pre_prepares[&block_digest].block;
        if !self.chain.commit(block) {
            return;
        }
        loop {
            for request in &block.requests {
                let reply = Reply {
                    request_num: request.request_num,
                    result: self.app.execute(&request.op),
                    block_digest,
                    replica_index: self.index,
                };
                self.context.send(To::Client(request.client_index), reply)
            }
            if let Some(block_digest) = self.chain.next_execute() {
                block = &self.pre_prepares[&block_digest].block;
            } else {
                break;
            }
        }
    }
}

impl Sign<Request> for Message {
    fn sign(message: Request, signer: &crate::context::crypto::Signer) -> Self {
        Self::Request(signer.sign_private(message))
    }
}

impl Sign<Reply> for Message {
    fn sign(message: Reply, signer: &crate::context::crypto::Signer) -> Self {
        Self::Reply(signer.sign_private(message))
    }
}

impl Sign<PrePrepare> for Message {
    fn sign(message: PrePrepare, signer: &crate::context::crypto::Signer) -> Self {
        Self::PrePrepare(signer.sign_public(message))
    }
}

impl Sign<Prepare> for Message {
    fn sign(message: Prepare, signer: &crate::context::crypto::Signer) -> Self {
        Self::Prepare(signer.sign_public(message))
    }
}

impl Sign<Commit> for Message {
    fn sign(message: Commit, signer: &crate::context::crypto::Signer) -> Self {
        Self::Commit(signer.sign_public(message))
    }
}

impl Verify<ReplicaIndex> for Message {
    fn verify(
        &self,
        verifier: &crate::context::crypto::Verifier<ReplicaIndex>,
    ) -> Result<(), crate::context::crypto::Invalid> {
        match self {
            Self::Request(message) => verifier.verify(message, None),
            Self::Reply(message) => verifier.verify(message, message.replica_index),
            Self::PrePrepare(message) => verifier.verify(message, 0), // TODO
            Self::Prepare(message) => verifier.verify(message, message.replica_index),
            Self::Commit(message) => verifier.verify(message, message.replica_index),
        }
    }
}
