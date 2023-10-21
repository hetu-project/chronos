use std::{
    collections::HashMap,
    ops::RangeInclusive,
    sync::{Arc, Mutex},
    time::Duration,
};

use k256::sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};

use crate::{
    client::BoxedConsume,
    common::{Request, Timer},
    context::{
        crypto::{Hasher, Sign, Signed, Verify},
        ordered_multicast::{
            OrderedMulticast,
            Signature::{K256Unverified, K256},
        },
        Addr, OrderedMulticastReceivers, Receivers,
    },
    App, ClientIndex, Context, ReplicaIndex, To,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Message {
    Request(OrderedMulticast<Request>),
    Reply(Signed<Reply>),
    Confirm(Signed<Confirm>),
    Query(Signed<Query>),
    QueryOk(QueryOk),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Reply {
    request_num: u32,
    result: Vec<u8>,
    epoch_num: u32,
    seq_num: u32,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Confirm {
    digest: [u8; 32],
    op_nums: RangeInclusive<u32>,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Query {
    op_num: u32,
    replica_index: ReplicaIndex,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QueryOk {
    op_num: u32,
    request: OrderedMulticast<Request>,
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
        shared.context.send_ordered_multicast(request);
        shared.resend_timer.set(&mut shared.context)
    }

    fn handle(&self, message: Self::Message) {
        let Message::Reply(message) = message else {
            unimplemented!()
        };
        // println!("{message:?}");
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
        if invoke
            .replies
            .values()
            .filter(|reply| {
                (reply.epoch_num, reply.seq_num, &reply.result)
                    == (message.epoch_num, message.seq_num, &message.result)
            })
            .count()
            >= shared.context.num_replica() - shared.context.num_faulty()
        {
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

    seq_num_offset: Option<u32>,
    reordering_requests: HashMap<u32, OrderedMulticast<Request>>,
    requests: Vec<OrderedMulticast<Request>>,
    ordered_num: u32,
    verified_num: u32,
    replies: HashMap<ClientIndex, Reply>,
    app: App,

    confirm: bool,
    confirmed_num: u32, // global minimum
    local_confirmed_num: u32,
    remote_confirmed_nums: HashMap<ReplicaIndex, u32>,
    // TODO persistent confirm as certificates
    reordering_confirms1: HashMap<u32, Vec<Signed<Confirm>>>,
    reordering_confirms2: HashMap<(ReplicaIndex, u32), Signed<Confirm>>,
}

impl Replica {
    pub fn new(context: Context<Message>, index: ReplicaIndex, app: App, confirm: bool) -> Self {
        let remote_confirmed_nums = if confirm {
            (0..context.num_replica())
                .map(|index| (index as ReplicaIndex, 0))
                .collect()
        } else {
            Default::default()
        };
        Self {
            context,
            index,
            seq_num_offset: None,
            reordering_requests: Default::default(),
            requests: Default::default(),
            ordered_num: 0,
            verified_num: 0,
            replies: Default::default(),
            app,
            confirm,
            confirmed_num: 0,
            local_confirmed_num: 0,
            remote_confirmed_nums,
            reordering_confirms2: Default::default(),
            reordering_confirms1: Default::default(),
        }
    }
}

struct I<'a>(&'a [OrderedMulticast<Request>]);

impl std::ops::Index<u32> for I<'_> {
    type Output = OrderedMulticast<Request>;

    fn index(&self, index: u32) -> &Self::Output {
        &self.0[(index - 1) as usize]
    }
}

impl std::ops::Index<RangeInclusive<u32>> for I<'_> {
    type Output = [OrderedMulticast<Request>];

    fn index(&self, index: RangeInclusive<u32>) -> &Self::Output {
        &self.0[(*index.start() - 1) as usize..=(*index.end() - 1) as usize]
    }
}

impl Receivers for Replica {
    type Message = Message;

    fn handle(&mut self, receiver: Addr, remote: Addr, message: Self::Message) {
        // println!("{message:?}");
        if !matches!(message, Message::Request(_)) {
            assert_eq!(receiver, self.context.addr())
        }
        match message {
            Message::Request(message) => self.handle_request(remote, message),
            Message::Confirm(message) => self.handle_confirm(remote, message),
            Message::Query(message) => self.handle_query(remote, message),
            Message::QueryOk(message) => self.handle_query_ok(remote, message),
            _ => unimplemented!(),
        }
    }

    fn handle_loopback(&mut self, receiver: Addr, message: Self::Message) {
        assert_eq!(receiver, self.context.addr());
        let Message::Confirm(confirm) = message else {
            unreachable!()
        };
        // println!("> confirm #s {:?}", confirm.op_nums);
        let evicted = self
            .remote_confirmed_nums
            .insert(self.index, *confirm.op_nums.end());
        assert_eq!(evicted.unwrap() + 1, *confirm.op_nums.start());
        self.do_update_confirm_num();

        // if self.ordered_num >= self.local_confirmed_num + Self::CONFIRM_THRESHOLD {
        //     self.do_send_confirm()
        // }
    }

    fn on_timer(&mut self, _: Addr, _: crate::context::TimerId) {
        unreachable!()
    }

    fn on_pace(&mut self) {
        if self.confirm {
            self.do_send_confirm()
        }
    }
}

impl OrderedMulticastReceivers for Replica {
    type Message = Request;
}

impl Replica {
    // pub const CONFIRM_THRESHOLD: u32 = 100;
    pub const QUERY_THRESHOLD: usize = 100;

    fn handle_request(&mut self, _remote: Addr, message: OrderedMulticast<Request>) {
        // Jialin's trick to avoid resetting switch for every run
        let op_num = message.seq_num - *self.seq_num_offset.get_or_insert(message.seq_num) + 1;
        // if message.verified() {
        //     println!(">> verified {}", op_num)
        // }
        // eager querying may defeat the slow original message...
        // assert!(op_num >= next_op_num);
        if op_num < self.ordered_num + 1 {
            return;
        }

        if op_num != self.ordered_num + 1 {
            // println!("! miss {}", self.ordered_num + 1);
            self.reordering_requests.insert(op_num, message);
            // reordering should be resolved within millisecond
            assert!(self.reordering_requests.len() < Self::QUERY_THRESHOLD + 1000);
            if self.reordering_requests.len() == Self::QUERY_THRESHOLD {
                self.do_query()
            }
            return;
        }

        let mut verified_num = self.verified_num;
        self.ordered_num += 1;
        if message.verified() {
            verified_num = self.ordered_num
        }
        self.requests.push(message);
        while let Some(request) = self.reordering_requests.remove(&(self.ordered_num + 1)) {
            self.ordered_num += 1;
            if request.verified() {
                verified_num = self.ordered_num
            }
            self.requests.push(request);
        }

        for op_num in self.verified_num + 1..=verified_num {
            if !self.confirm {
                self.do_commit(op_num);
            } else if let Some(confirms) = self.reordering_confirms1.remove(&op_num) {
                for confirm in confirms {
                    self.do_confirm1(confirm)
                }
            }
        }
        // if self.confirm && self.ordered_num >= self.local_confirmed_num + Self::CONFIRM_THRESHOLD {
        //     self.do_send_confirm()
        // }

        self.verified_num = verified_num
    }

    fn handle_confirm(&mut self, _remote: Addr, message: Signed<Confirm>) {
        assert!(self.confirm);
        // println!("> confirm #{} {:?}", message.replica_index, message.op_nums);

        if *message.op_nums.end() > self.ordered_num {
            self.reordering_confirms1
                .entry(*message.op_nums.end())
                .or_default()
                .push(message);
            return;
        }
        self.do_confirm1(message)
    }

    fn handle_query(&mut self, _remote: Addr, message: Signed<Query>) {
        let mut request = if message.op_num <= self.ordered_num {
            I(&self.requests)[message.op_num].clone()
        } else if let Some(request) = self.reordering_requests.get(&message.op_num) {
            request.clone()
        } else {
            println!("! query missing {}", message.op_num);
            return;
        };
        if let &K256Unverified(signature) = &request.signature {
            request.signature = K256(signature)
        }
        // println!("< query replied {}", message.op_num);
        let query_ok = QueryOk {
            op_num: message.op_num,
            request,
        };
        self.context
            .send(To::Replica(message.replica_index), query_ok)
    }

    fn handle_query_ok(&mut self, remote: Addr, message: QueryOk) {
        if message.op_num == self.ordered_num + 1 {
            // println!("> query done {}", message.op_num);
            // let ordered_num = self.ordered_num;
            // let verified_num = self.verified_num;
            self.handle_request(remote, message.request);
            // println!(
            //     "> ordered {ordered_num} -> {} verified {verified_num} -> {}",
            //     self.ordered_num, self.verified_num
            // );
            if self.reordering_requests.len() >= Self::QUERY_THRESHOLD {
                self.do_query()
            }
        }
    }

    fn do_commit(&mut self, op_num: u32) {
        let request = &I(&self.requests)[op_num];
        match self.replies.get(&request.client_index) {
            Some(reply) if reply.request_num > request.request_num => return,
            Some(reply) if reply.request_num == request.request_num => {
                self.context
                    .send(To::Client(request.client_index), reply.clone());
                return;
            }
            _ => {}
        }
        let reply = Reply {
            epoch_num: 0,
            request_num: request.request_num,
            result: self.app.execute(&request.op),
            seq_num: request.seq_num,
            replica_index: self.index,
        };
        self.context.send(To::Client(request.client_index), reply)
    }

    fn do_send_confirm(&mut self) {
        let op_nums = self.local_confirmed_num + 1..=self.ordered_num;
        // println!(
        //     "* op {op_nums:?} remote[local] {}",
        //     self.remote_confirmed_nums[&self.index]
        // );
        if !op_nums.is_empty() && *op_nums.start() == self.remote_confirmed_nums[&self.index] + 1 {
            // println!("confirming {op_nums:?}");
            let mut digest = Sha256::new();
            for request in &I(&self.requests)[op_nums.clone()] {
                Hasher::sha256_update(&request.inner, &mut digest);
            }
            let confirm = Confirm {
                digest: digest.finalize().into(),
                op_nums,
                replica_index: self.index,
            };
            self.context.send(To::AllReplicaWithLoopback, confirm);
            // TODO set up resending confirm
            self.local_confirmed_num = self.ordered_num
        }
    }

    fn do_confirm1(&mut self, message: Signed<Confirm>) {
        let confirmed_num = self.remote_confirmed_nums[&message.replica_index];
        assert!(*message.op_nums.start() > confirmed_num);
        if *message.op_nums.start() != confirmed_num + 1 {
            self.reordering_confirms2
                .insert((message.replica_index, *message.op_nums.start()), message);
            return;
        }
        let index = message.replica_index;
        self.do_confirm2(message);
        while let Some(message) = self
            .reordering_confirms2
            .remove(&(index, self.remote_confirmed_nums[&index] + 1))
        {
            self.do_confirm2(message)
        }
    }

    fn do_confirm2(&mut self, message: Signed<Confirm>) {
        let mut local_digest = Sha256::new();
        for request in &I(&self.requests)[message.op_nums.clone()] {
            Hasher::sha256_update(&request.inner, &mut local_digest)
        }
        assert_eq!(<[_; 32]>::from(local_digest.finalize()), message.digest);
        self.remote_confirmed_nums
            .insert(message.replica_index, *message.op_nums.end());
        self.do_update_confirm_num()
    }

    fn do_update_confirm_num(&mut self) {
        let mut confirmed_nums = Vec::from_iter(self.remote_confirmed_nums.values().copied());
        confirmed_nums.sort_unstable();
        let new_confirmed_num = confirmed_nums[self.context.num_faulty()];
        assert!(new_confirmed_num >= self.confirmed_num);
        // println!("* confirmed {} -> {new_confirmed_num}", self.confirmed_num);
        for op_num in self.confirmed_num + 1..=new_confirmed_num {
            self.do_commit(op_num)
        }
        self.confirmed_num = new_confirmed_num;
    }

    fn do_query(&mut self) {
        let query = Query {
            op_num: self.ordered_num + 1,
            replica_index: self.index,
        };
        // println!("< query sent {}", query.op_num);
        self.context.send(To::AllReplica, query)
    }
}

impl From<OrderedMulticast<Request>> for Message {
    fn from(value: OrderedMulticast<Request>) -> Self {
        Self::Request(value)
    }
}

impl Sign<Reply> for Message {
    fn sign(message: Reply, signer: &crate::context::crypto::Signer) -> Self {
        Message::Reply(signer.sign_private(message))
    }
}

impl Sign<Confirm> for Message {
    fn sign(message: Confirm, signer: &crate::context::crypto::Signer) -> Self {
        Message::Confirm(signer.sign_public(message))
    }
}

impl Sign<Query> for Message {
    fn sign(message: Query, signer: &crate::context::crypto::Signer) -> Self {
        Message::Query(signer.sign_public(message))
    }
}

impl From<QueryOk> for Message {
    fn from(value: QueryOk) -> Self {
        Self::QueryOk(value)
    }
}

impl Verify<ReplicaIndex> for Message {
    fn verify(
        &self,
        verifier: &crate::context::crypto::Verifier<ReplicaIndex>,
    ) -> Result<(), crate::context::crypto::Invalid> {
        match self {
            Self::Request(message) => verifier.verify_ordered_multicast(message),
            Self::Reply(message) => verifier.verify(message, message.replica_index),
            Self::Confirm(message) => verifier.verify(message, message.replica_index),
            Self::Query(message) => verifier.verify(message, message.replica_index),
            Self::QueryOk(message) => verifier.verify_ordered_multicast(&message.request),
        }
    }
}
