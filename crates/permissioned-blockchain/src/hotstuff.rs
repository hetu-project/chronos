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
    Generic(Signed<Generic>),
    Vote(Signed<Vote>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reply {
    request_num: u32,
    result: Vec<u8>,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Generic {
    block: Block,
    certified_digest: BlockDigest,
    certificate: Vec<Signed<Vote>>,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Vote {
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
        shared.context.send(To::AllReplica, request);
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
            .filter(|reply| reply.result == message.result)
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

pub struct Replica {
    context: Context<Message>,
    index: ReplicaIndex,

    view_height: u32,
    propose_height: u32,
    digest_certified: BlockDigest, // qc_{high}
    digest_lock: BlockDigest,

    requests: Vec<Request>,
    replies: HashMap<ClientIndex, (u32, Option<Reply>)>,
    generics: HashMap<BlockDigest, Signed<Generic>>,
    votes: HashMap<BlockDigest, HashMap<ReplicaIndex, Signed<Vote>>>,
    reordering_generics: HashMap<BlockDigest, Vec<Signed<Generic>>>,
    chain: Chain,
    app: App,
}

impl Replica {
    pub fn new(context: Context<Message>, index: ReplicaIndex, app: App) -> Self {
        let mut votes = HashMap::new();
        votes.insert(Chain::genesis().digest(), Default::default());
        let mut generics = HashMap::new();
        let mut genesis_block = Chain::genesis();
        genesis_block.parent_digest = genesis_block.digest();
        generics.insert(
            Chain::genesis().digest(),
            Signed {
                inner: Generic {
                    block: genesis_block,
                    certified_digest: Chain::genesis().digest(),
                    certificate: Default::default(),
                    replica_index: u8::MAX,
                },
                signature: crate::context::crypto::Signature::Plain,
            },
        );
        Self {
            context,
            index,
            view_height: 0,
            propose_height: 0,
            digest_certified: Chain::genesis().digest(),
            digest_lock: Chain::genesis().digest(),
            requests: Default::default(),
            replies: Default::default(),
            generics,
            votes,
            reordering_generics: Default::default(),
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
            Message::Generic(message) => self.handle_generic(remote, message),
            Message::Vote(message) => self.handle_vote(remote, message),
            _ => unimplemented!(),
        }
    }

    fn handle_loopback(&mut self, receiver: Addr, message: Self::Message) {
        // println!("{message:02x?}");
        assert_eq!(receiver, self.context.addr());
        match message {
            Message::Generic(message) => self.insert_generic(message),
            Message::Vote(message) => self.handle_vote(receiver, message),
            _ => unimplemented!(),
        }
    }

    fn on_timer(&mut self, receiver: Addr, _: crate::context::TimerId) {
        assert_eq!(receiver, self.context.addr());
        todo!()
    }

    fn on_pace(&mut self) {
        if self.index == self.primary_index()
            && self.replies.values().any(|(_, reply)| reply.is_none())
            && self.generics[&self.digest_certified].block.height >= self.propose_height
        {
            self.do_propose()
        }
    }
}

impl Replica {
    fn primary_index(&self) -> ReplicaIndex {
        0 // TODO rotate
    }

    fn handle_request(&mut self, remote: Addr, message: Signed<Request>) {
        match self.replies.get(&message.client_index) {
            Some((request_num, _)) if request_num > &message.request_num => return,
            Some((request_num, reply)) if request_num == &message.request_num => {
                if let Some(reply) = reply {
                    self.context.send(To::Addr(remote), reply.clone())
                }
                return;
            }
            _ => {}
        }
        self.replies
            .insert(message.client_index, (message.request_num, None));

        if self.index != self.primary_index() {
            return;
        }

        self.requests.push(message.inner)
    }

    fn handle_generic(&mut self, _remote: Addr, message: Signed<Generic>) {
        self.do_reorder_generic(message)
    }

    fn handle_vote(&mut self, _remote: Addr, message: Signed<Vote>) {
        let block_digest = message.block_digest;
        assert!(self.generics.contains_key(&block_digest)); // TODO
        let votes = self.votes.entry(block_digest).or_default();
        if votes.len() == self.context.num_replica() - self.context.num_faulty() {
            return;
        }
        votes.insert(message.replica_index, message);
        if votes.len() == self.context.num_replica() - self.context.num_faulty() {
            self.do_update_certified(&block_digest)
        }
    }

    fn do_propose(&mut self) {
        self.chain.digest_parent = self.digest_certified; // careful
        let block = if !self.requests.is_empty() {
            self.chain.propose(&mut self.requests)
        } else {
            self.chain.propose_empty()
        };
        let generic = Generic {
            replica_index: self.index,
            block,
            certified_digest: self.digest_certified,
            certificate: self.votes[&self.digest_certified]
                .values()
                .cloned()
                .collect(),
        };
        self.propose_height = generic.block.height;
        self.context.send(To::AllReplicaWithLoopback, generic)
    }

    fn do_reorder_generic(&mut self, generic: Signed<Generic>) {
        if !self.generics.contains_key(&generic.block.parent_digest) {
            self.reordering_generics
                .entry(generic.block.parent_digest)
                .or_default()
                .push(generic);
            return;
        }

        if !self.generics.contains_key(&generic.certified_digest) {
            self.reordering_generics
                .entry(generic.certified_digest)
                .or_default()
                .push(generic);
            return;
        }

        let block_digest = generic.block.digest();
        self.insert_generic(generic);
        if let Some(generics) = self.reordering_generics.remove(&block_digest) {
            for generic in generics {
                self.do_reorder_generic(generic)
            }
        }
    }

    fn insert_generic(&mut self, generic: Signed<Generic>) {
        // println!("> insert {:02x?}", generic.inner);
        self.generics
            .insert(generic.block.digest(), generic.clone());

        if generic.block.height > self.view_height
            && (self.extend(&generic.block, &self.digest_lock)
                || self.block_height(&generic.certified_digest)
                    > self.block_height(&self.digest_lock))
        {
            // println!("> vote   {:02x?}", generic.inner);
            self.view_height = generic.block.height;
            let vote = Vote {
                block_digest: generic.block.digest(),
                replica_index: self.index,
            };
            let to = if self.index == self.primary_index() {
                To::Loopback
            } else {
                To::Replica(self.primary_index())
            };
            // println!("! send vote {to:?}");
            self.context.send(to, vote)
        }
        self.do_update(&generic.block.digest())
    }

    fn do_update(&mut self, block_digest: &BlockDigest) {
        let block_digest3 = *block_digest;
        let block_digest2 = self.generics[&block_digest3].certified_digest;
        let block_digest1 = self.generics[&block_digest2].certified_digest;
        let block_digest0 = self.generics[&block_digest1].certified_digest;
        self.do_update_certified(&block_digest2);
        if self.block_height(&block_digest1) > self.block_height(&self.digest_lock) {
            self.digest_lock = block_digest1
        }
        if self.generics[&block_digest2].block.parent_digest == block_digest1
            && self.generics[&block_digest1].block.parent_digest == block_digest0
            && block_digest0 != Chain::genesis().digest()
        {
            // commit block0
            let block = &self.generics[&block_digest0].block;
            let execute = self.chain.commit(block);
            assert!(execute);
            for request in &block.requests {
                let reply = Reply {
                    request_num: request.request_num,
                    result: self.app.execute(&request.op),
                    replica_index: self.index,
                };
                self.replies.insert(
                    request.client_index,
                    (request.request_num, Some(reply.clone())),
                );
                self.context.send(To::Client(request.client_index), reply)
            }
            assert!(self.chain.next_execute().is_none())
        }
    }

    fn do_update_certified(&mut self, digest_certified: &BlockDigest) {
        if self.block_height(digest_certified) > self.block_height(&self.digest_certified) {
            self.digest_certified = *digest_certified
        }
    }

    fn extend(&self, block: &Block, base_digest: &BlockDigest) -> bool {
        if &block.parent_digest == base_digest {
            true
        } else if block.parent_digest == Chain::genesis().digest() {
            false
        } else {
            self.extend(&self.generics[&block.parent_digest].block, base_digest)
        }
    }

    fn block_height(&self, block_digest: &BlockDigest) -> u32 {
        self.generics[block_digest].block.height
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

impl Sign<Generic> for Message {
    fn sign(message: Generic, signer: &crate::context::crypto::Signer) -> Self {
        Self::Generic(signer.sign_public(message))
    }
}

impl Sign<Vote> for Message {
    fn sign(message: Vote, signer: &crate::context::crypto::Signer) -> Self {
        Self::Vote(signer.sign_public_for_batch(message))
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
            Self::Generic(message) => {
                verifier.verify(message, message.replica_index)?;
                if message.certified_digest == Chain::genesis().digest() {
                    return Ok(());
                }
                // TODO check certification size
                verifier.verify_batch(
                    &message.certificate,
                    &message
                        .certificate
                        .iter()
                        .map(|vote| vote.replica_index)
                        .collect::<Vec<_>>(),
                )
            }
            Self::Vote(message) => verifier.verify(message, message.replica_index),
        }
    }
}
