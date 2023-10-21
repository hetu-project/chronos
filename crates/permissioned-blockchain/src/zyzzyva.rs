use std::{
    collections::{HashMap, HashSet},
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
    OrderRequest(Signed<OrderRequest>),
    SpecResponse(Signed<SpecResponse>),
    Commit(Signed<Commit>),
    LocalCommit(Signed<LocalCommit>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderRequest {
    view_num: u32,
    block: Block,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SpecResponse {
    block: Block,
    results: Vec<Vec<u8>>,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Commit {
    client_index: ClientIndex,
    block_digest: BlockDigest,
    responses: Vec<Signed<SpecResponse>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LocalCommit {
    block_digest: BlockDigest,
    replica_index: ReplicaIndex,
}

#[derive(Debug)]
pub struct Client {
    index: ClientIndex,
    byzantine: bool,
    shared: Arc<Mutex<ClientShared>>,
}

#[derive(Debug)]
pub struct ClientShared {
    context: Context<Message>,
    request_num: u32,
    invoke: Option<ClientInvoke>,
    resend_timer: Timer,
}

#[derive(Debug)]
struct ClientInvoke {
    op: Vec<u8>,
    responses: HashMap<ReplicaIndex, Signed<SpecResponse>>,
    commit_digest: Option<BlockDigest>,
    commit_result: Option<Vec<u8>>,
    local_commits: HashSet<ReplicaIndex>,
    consume: BoxedConsume,
}

impl Client {
    pub fn new(context: Context<Message>, index: ClientIndex, byzantine: bool) -> Self {
        Self {
            index,
            byzantine,
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
            responses: Default::default(),
            commit_digest: None,
            commit_result: None,
            local_commits: Default::default(),
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
        let mut shared_guard = self.shared.lock().unwrap();
        let shared = &mut *shared_guard;
        match message {
            Message::SpecResponse(message) => {
                // println!("{message:?}");
                let Some(invoke) = &mut shared.invoke else {
                    return;
                };
                let Some(result) = message
                    .block
                    .requests
                    .iter()
                    .position(|request| {
                        request.client_index == self.index
                            && request.request_num == shared.request_num
                    })
                    .map(|i| &message.results[i])
                else {
                    return;
                };
                invoke
                    .responses
                    .insert(message.replica_index, message.clone());
                let matched_responses = invoke.responses.values().filter(|response| {
                    (response.block.digest(), &response.results)
                        == (message.block.digest(), &message.results)
                });
                let num_match = matched_responses.clone().count();
                if num_match == shared.context.num_replica() {
                    shared.resend_timer.unset(&mut shared.context);
                    let invoke = shared.invoke.take().unwrap();
                    drop(shared_guard);

                    let _op = invoke.op;
                    invoke.consume.apply(result.clone())
                } else if self.byzantine
                    && num_match == shared.context.num_replica() - shared.context.num_faulty()
                {
                    invoke.commit_digest = Some(message.block.digest());
                    invoke.commit_result = Some(result.clone());
                    let commit = Commit {
                        client_index: self.index,
                        block_digest: message.block.digest(),
                        responses: matched_responses.cloned().collect(),
                    };
                    shared.context.send(To::AllReplica, commit)
                }
            }
            Message::LocalCommit(message) => {
                let Some(invoke) = &mut shared.invoke else {
                    return;
                };
                if Some(message.block_digest) != invoke.commit_digest {
                    return;
                }
                invoke.local_commits.insert(message.replica_index);
                if invoke.local_commits.len()
                    == shared.context.num_replica() - shared.context.num_faulty()
                {
                    shared.resend_timer.unset(&mut shared.context);
                    let invoke = shared.invoke.take().unwrap();
                    drop(shared_guard);

                    invoke.consume.apply(invoke.commit_result.unwrap())
                }
            }
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplicaTimerValue {}

#[derive(Debug)]
pub struct Replica {
    context: Context<Message>,
    index: ReplicaIndex,

    view_num: u32,
    requests: Vec<Request>,
    order_requests: HashMap<BlockDigest, Signed<OrderRequest>>,
    commits: HashMap<BlockDigest, Signed<Commit>>,
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
            order_requests: Default::default(),
            commits: Default::default(),
            chain: Default::default(),
            app,
        }
    }
}

impl Receivers for Replica {
    type Message = Message;

    fn handle(&mut self, receiver: Addr, remote: Addr, message: Self::Message) {
        assert_eq!(receiver, self.context.addr());
        // println!("{message:?}");
        match message {
            Message::Request(message) => self.handle_request(remote, message),
            Message::OrderRequest(message) => self.handle_order_request(remote, message),
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
        let Message::OrderRequest(message) = message else {
            unimplemented!()
        };
        // is this ok?
        self.handle_order_request(self.context.addr(), message)
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

    fn handle_request(&mut self, _remote: Addr, request: Signed<Request>) {
        if self.index != self.primary_index() {
            // TODO
            return;
        }

        // TODO check resend

        self.requests.push(request.inner);
    }

    fn handle_order_request(&mut self, _remote: Addr, order_request: Signed<OrderRequest>) {
        if order_request.view_num < self.view_num {
            return;
        }

        if order_request.view_num > self.view_num {
            //
            return;
        }

        let digest = order_request.block.digest();
        self.order_requests.insert(digest, order_request);
        self.do_execute(digest);
    }

    fn handle_commit(&mut self, remote: Addr, commit: Signed<Commit>) {
        if self.commits.contains_key(&commit.block_digest) {
            let local_commit = LocalCommit {
                block_digest: commit.block_digest,
                replica_index: self.index,
            };
            self.context.send(To::Addr(remote), local_commit);
            return;
        }

        let Some(_order_request) = self.order_requests.get(&commit.block_digest) else {
            // TODO
            return;
        };

        let local_commit = LocalCommit {
            block_digest: commit.block_digest,
            replica_index: self.index,
        };
        self.commits.insert(commit.block_digest, commit);
        self.context.send(To::Addr(remote), local_commit)
    }

    fn do_propose(&mut self) {
        assert_eq!(self.index, self.primary_index());
        let order_request = OrderRequest {
            view_num: self.view_num,
            block: self.chain.propose(&mut self.requests),
        };
        self.context.send(To::AllReplicaWithLoopback, order_request)
    }

    fn do_execute(&mut self, block_digest: BlockDigest) {
        let mut block = &self.order_requests[&block_digest].block;
        if !self.chain.commit(block) {
            return;
        }
        while let Some(block_digest) = {
            let results = Vec::from_iter(
                block
                    .requests
                    .iter()
                    .map(|request| self.app.execute(&request.op)),
            );
            let spec_response = SpecResponse {
                block: block.clone(),
                results,
                replica_index: self.index,
            };
            let indexes = block
                .requests
                .iter()
                .map(|request| request.client_index)
                .collect();
            self.context.send(To::Clients(indexes), spec_response);
            self.chain.next_execute()
        } {
            block = &self.order_requests[&block_digest].block
        }
    }
}

impl Sign<Request> for Message {
    fn sign(message: Request, signer: &crate::context::crypto::Signer) -> Self {
        Self::Request(signer.sign_private(message))
    }
}

impl Sign<OrderRequest> for Message {
    fn sign(message: OrderRequest, signer: &crate::context::crypto::Signer) -> Self {
        Self::OrderRequest(signer.sign_public(message))
    }
}

impl Sign<SpecResponse> for Message {
    fn sign(message: SpecResponse, signer: &crate::context::crypto::Signer) -> Self {
        Self::SpecResponse(signer.sign_public(message))
    }
}

impl Sign<Commit> for Message {
    fn sign(message: Commit, signer: &crate::context::crypto::Signer) -> Self {
        Self::Commit(signer.sign_private(message))
    }
}

impl Sign<LocalCommit> for Message {
    fn sign(message: LocalCommit, signer: &crate::context::crypto::Signer) -> Self {
        Self::LocalCommit(signer.sign_private(message))
    }
}

impl Verify<ReplicaIndex> for Message {
    fn verify(
        &self,
        verifier: &crate::context::crypto::Verifier<ReplicaIndex>,
    ) -> Result<(), crate::context::crypto::Invalid> {
        match self {
            Self::Request(message) => verifier.verify(message, None),
            Self::OrderRequest(message) => verifier.verify(message, 0), // TODO
            Self::SpecResponse(message) => verifier.verify(message, message.replica_index),
            Self::Commit(message) => {
                if message.responses[0].block.requests[0].client_index != message.client_index {
                    return Ok(());
                }
                verifier.verify(message, None)?;
                // TODO check responses length
                for response in &message.responses {
                    verifier.verify(response, response.replica_index)?
                }
                Ok(())
            }
            Self::LocalCommit(message) => verifier.verify(message, message.replica_index),
        }
    }
}
