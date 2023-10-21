use std::{collections::HashMap, sync::Mutex, time::Duration};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reply {
    request_num: u32,
    result: Vec<u8>,
}

#[derive(Debug)]
pub struct Client {
    index: ClientIndex,
    shared: Mutex<ClientShared>,
}

#[derive(Debug)]
struct ClientShared {
    context: Context<Message>,
    request_num: u32,
    op: Vec<u8>,
    consume: Option<BoxedConsume>,
    resend_timer: Timer,
}

impl Client {
    pub fn new(context: Context<Message>, index: ClientIndex) -> Self {
        Self {
            index,
            shared: Mutex::new(ClientShared {
                context,
                request_num: 0,
                op: Default::default(),
                consume: None,
                resend_timer: Timer::new(Duration::from_millis(100)),
            }),
        }
    }
}

impl crate::Client for Client {
    type Message = Message;

    fn invoke(&self, op: Vec<u8>, consume: impl Into<BoxedConsume>) {
        let shared = &mut *self.shared.lock().unwrap();
        assert!(shared.consume.is_none());
        shared.request_num += 1;
        shared.op = op.clone();
        shared.consume = Some(consume.into());
        shared.resend_timer.set(&mut shared.context);

        let request = Request {
            client_index: self.index,
            request_num: shared.request_num,
            op,
        };
        shared.context.send(To::Replica(0), request)
    }

    fn handle(&self, message: Self::Message) {
        let Message::Reply(reply) = message else {
            unimplemented!()
        };
        let mut shared = self.shared.lock().unwrap();
        if reply.request_num != shared.request_num {
            return;
        }

        {
            let shared = &mut *shared;
            shared.resend_timer.unset(&mut shared.context);
        }
        shared.op.clear();
        let consume = shared.consume.take().unwrap();
        drop(shared);

        consume.apply(reply.inner.result);
    }
}

#[derive(Debug)]
pub struct Replica {
    context: Context<Message>,
    blocks: HashMap<BlockDigest, Block>,
    chain: Chain,
    requests: Vec<Request>,
    replies: HashMap<ClientIndex, Reply>,
    app: App,
    pub make_blocks: bool,
}

impl Replica {
    pub fn new(context: Context<Message>, app: App) -> Self {
        Self {
            context,
            // probably need to reserve if `make_blocks` is set
            // or the rehashing will cause huge latency spikes
            blocks: HashMap::default(),
            chain: Default::default(),
            requests: Default::default(),
            replies: Default::default(),
            app,
            make_blocks: false,
        }
    }
}

impl Receivers for Replica {
    type Message = Message;

    fn handle(&mut self, receiver: Addr, remote: Addr, message: Self::Message) {
        assert_eq!(receiver, self.context.addr());
        let Message::Request(request) = message else {
            unimplemented!()
        };
        match self.replies.get(&request.client_index) {
            Some(reply) if reply.request_num > request.request_num => return,
            Some(reply) if reply.request_num == request.request_num => {
                self.context.send(To::Addr(remote), reply.clone());
                return;
            }
            _ => {}
        }

        self.requests.push(request.inner);
        if !self.make_blocks {
            let request = self.requests.last().unwrap();
            let reply = Reply {
                request_num: request.request_num,
                result: self.app.execute(&request.op),
            };
            let evicted = self.replies.insert(request.client_index, reply.clone());
            if let Some(evicted) = evicted {
                assert_eq!(evicted.request_num, request.request_num - 1)
            }
            self.context.send(To::Client(request.client_index), reply)
        }
    }

    fn on_timer(&mut self, _: Addr, _: crate::context::TimerId) {
        unreachable!()
    }

    fn on_pace(&mut self) {
        if self.make_blocks && !self.requests.is_empty() {
            let block = self.chain.propose(&mut self.requests);
            assert!(block.digest() != Chain::genesis().digest());
            let evicted = self.blocks.insert(block.digest(), block.clone());
            assert!(evicted.is_none());

            let execute = self.chain.commit(&block);
            assert!(execute);
            for request in &block.requests {
                let reply = Reply {
                    request_num: request.request_num,
                    result: self.app.execute(&request.op),
                };
                let evicted = self.replies.insert(request.client_index, reply.clone());
                if let Some(evicted) = evicted {
                    assert_eq!(evicted.request_num, request.request_num - 1)
                }
                self.context.send(To::Client(request.client_index), reply)
            }
            assert!(self.chain.next_execute().is_none())
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

impl Verify<ReplicaIndex> for Message {
    fn verify(
        &self,
        verifier: &crate::context::crypto::Verifier<ReplicaIndex>,
    ) -> Result<(), crate::context::crypto::Invalid> {
        match self {
            Self::Request(message) => verifier.verify(message, None),
            Self::Reply(message) => verifier.verify(message, 0),
        }
    }
}
