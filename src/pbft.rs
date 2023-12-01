use std::{collections::HashMap, iter::repeat, sync::Arc};

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::{
    task::{JoinHandle, JoinSet},
    time::{timeout_at, Instant},
};

use crate::{
    channel::{EventSender, EventSource, PromiseSender, PromiseSource, SubmitSource},
    crypto::{digest, Digest, Message, Packet},
    event_channel, promise_channel,
    transport::Addr,
    Client, Replica, Transport,
};

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Request {
    client_id: u32,
    request_num: u32,
    op: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Reply {
    request_num: u32,
    result: Vec<u8>,
    replica_id: u8,
    view_num: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct PrePrepare {
    view_num: u32,
    op_num: u32,
    digest: Digest,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct Prepare {
    view_num: u32,
    op_num: u32,
    digest: Digest,
    replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct Commit {
    view_num: u32,
    op_num: u32,
    digest: Digest,
    replica_id: u8,
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub enum ToReplica {
    Request(Request),
    PrePrepare(Packet, Vec<Request>),
    Prepare(Packet),
    Commit(Packet),
}

pub async fn client_session(
    client: Arc<Client>,
    mut invoke_source: SubmitSource<Vec<u8>, Vec<u8>>,
    mut source: EventSource<(Addr, Reply)>,
    transport: impl Transport<ToReplica>,
) -> crate::Result<()> {
    let mut request_num = 0;
    let mut primary_replica = 0;

    while let Some((op, result)) = invoke_source.option_next().await {
        request_num += 1;
        let request = Request {
            client_id: client.id,
            request_num,
            op,
        };
        result.resolve(
            request_session(
                &client,
                request,
                &mut primary_replica,
                &mut source,
                &transport,
            )
            .await?,
        )?
    }
    Ok(())
}

async fn request_session(
    client: &Client,
    request: Request,
    primary_replica: &mut u8,
    source: &mut EventSource<(Addr, Reply)>,
    transport: &impl Transport<ToReplica>,
) -> crate::Result<Vec<u8>> {
    let mut replies = HashMap::new();

    transport
        .send_to(
            client.addr_book.replica_addr(*primary_replica)?,
            ToReplica::Request(request.clone()),
        )
        .await?;
    loop {
        let deadline = Instant::now() + client.retry_interval;
        while let Ok(reply) = timeout_at(deadline, source.next()).await {
            let (_remote, reply) = reply?;
            assert!(reply.request_num <= request.request_num);
            if reply.request_num == request.request_num {
                replies.insert(reply.replica_id, reply.clone());
                if replies
                    .values()
                    .filter(|matched_reply| matched_reply.result == reply.result)
                    .count()
                    == client.num_faulty + 1
                {
                    *primary_replica = client.primary_replica(reply.view_num);
                    return Ok(reply.result);
                }
            }
        }
        transport
            .send_to_all(
                client.addr_book.replica_addrs(),
                ToReplica::Request(request.clone()),
            )
            .await?
    }
}

pub async fn replica_session(
    replica: Arc<Replica>,
    mut listen_source: EventSource<(Addr, ToReplica)>,
    transport: impl Transport<ToReplica>,
    reply_transport: impl Transport<Reply>,
) -> crate::Result<()> {
    let mut reply_sessions = JoinSet::new();
    for view_num in 0.. {
        let mut view = ViewState::new(
            replica.clone(),
            view_num,
            &mut listen_source,
            &mut reply_sessions,
            transport.clone(),
            reply_transport.clone(),
        );
        tokio::select! {
            view = view.session() => view?,
            () = replica.stop.cancelled() => break,
        }
        // TODO view change
    }
    while let Some(result) = reply_sessions.join_next().await {
        result??;
    }
    Ok(())
}

type Quorum<T> = HashMap<u8, Message<T>>;

#[derive(Debug)]
struct LogEntry {
    requests: Vec<Request>,
    #[allow(unused)]
    pre_prepare: Message<PrePrepare>,
    #[allow(unused)]
    prepares: Quorum<Prepare>,
    commits: Quorum<Commit>,
}

async fn prepare_session(
    replica: Arc<Replica>,
    pre_prepare: Message<PrePrepare>,
    requests: Vec<Request>,
    mut prepare_source: EventSource<Message<Prepare>>,
    commit_digest: PromiseSender<Digest>,
    transport: impl Transport<ToReplica>,
) -> crate::Result<LogEntry> {
    assert!(pre_prepare.verified);
    let mut prepares = HashMap::new();
    if !replica.is_primary(pre_prepare.view_num) {
        let prepare = Prepare {
            view_num: pre_prepare.view_num,
            op_num: pre_prepare.op_num,
            digest: pre_prepare.digest,
            replica_id: replica.id,
        };
        let prepare = replica.signer.serialize_sign(prepare)?;
        prepares.insert(replica.id, prepare.clone());
        replica.send_to_all_replica(ToReplica::Prepare(prepare.into()), transport);
    }
    while prepares.len() < 2 * replica.num_faulty {
        let mut prepare = prepare_source.next().await?;
        assert_eq!(
            prepare.op_num, pre_prepare.op_num,
            "incorrect dispatching of {prepare:?}"
        );
        // the verification of `Prepare`s (and `PrePrepare`s and `Commit`s below) can have some
        // concurrency by lifting this verification out of the loop. however, "whether or not
        // verify the following message" actually depends on the result of all previous
        // verifications. in another word, an incoming message will be unconditionally verified
        // even if there are already sufficient valid messages to form a quorum collected. This
        // basically means 50% more verification overhead, which is silly
        // nevertheless the protocol will always concurrently work on different op numbers
        if replica.verifiers[&prepare.replica_id]
            .verify(&mut prepare)
            .is_ok()
            && prepare.view_num == pre_prepare.view_num
            && prepare.digest == pre_prepare.digest
        {
            prepares.insert(prepare.replica_id, prepare);
        }
    }
    commit_digest.resolve(pre_prepare.digest)?;
    Ok(LogEntry {
        requests,
        pre_prepare,
        prepares,
        commits: Default::default(),
    })
}

async fn primary_prepare_session(
    replica: Arc<Replica>,
    view_num: u32,
    op_num: u32,
    requests: Vec<Request>,
    prepare_source: EventSource<Message<Prepare>>,
    commit_digest: PromiseSender<Digest>,
    transport: impl Transport<ToReplica>,
) -> crate::Result<LogEntry> {
    assert!(replica.is_primary(view_num));
    let pre_prepare = PrePrepare {
        view_num,
        op_num,
        digest: digest(&requests)?,
    };
    let pre_prepare = replica.signer.serialize_sign(pre_prepare)?;
    replica.send_to_all_replica(
        ToReplica::PrePrepare(pre_prepare.clone().into(), requests.clone()),
        transport.clone(),
    );
    prepare_session(
        replica,
        pre_prepare,
        requests,
        prepare_source,
        commit_digest,
        transport,
    )
    .await
}

async fn backup_prepare_session(
    replica: Arc<Replica>,
    view_num: u32,
    mut pre_prepare_source: EventSource<(Message<PrePrepare>, Vec<Request>)>,
    prepare_source: EventSource<Message<Prepare>>,
    commit_digest: PromiseSender<Digest>,
    transport: impl Transport<ToReplica>,
) -> crate::Result<LogEntry> {
    loop {
        let (mut pre_prepare, requests) = pre_prepare_source.next().await?;
        if replica.verifiers[&replica.primary(pre_prepare.view_num)]
            .verify(&mut pre_prepare)
            .is_ok()
            && pre_prepare.view_num == view_num
            && pre_prepare.digest == digest(&requests)?
        {
            return prepare_session(
                replica,
                pre_prepare,
                requests,
                prepare_source,
                commit_digest,
                transport,
            )
            .await;
        }
    }
}

async fn commit_session(
    replica: Arc<Replica>,
    view_num: u32,
    op_num: u32,
    digest_promise: PromiseSource<Digest>,
    mut source: EventSource<Message<Commit>>,
    transport: impl Transport<ToReplica>,
) -> crate::Result<Quorum<Commit>> {
    let digest = digest_promise.await?;
    let mut commits = HashMap::new();
    let commit = Commit {
        view_num,
        op_num,
        digest,
        replica_id: replica.id,
    };
    let commit = replica.signer.serialize_sign(commit)?;
    commits.insert(replica.id, commit.clone());
    replica.send_to_all_replica(ToReplica::Commit(commit.into()), transport);
    while commits.len() <= 2 * replica.num_faulty {
        let mut commit = source.next().await?;
        assert_eq!(commit.op_num, op_num, "incorrect dispatching of {commit:?}");
        if replica.verifiers[&commit.replica_id]
            .verify(&mut commit)
            .is_ok()
            && commit.view_num == view_num
            && commit.digest == digest
        {
            commits.insert(commit.replica_id, commit);
        }
    }
    Ok(commits)
}

#[derive(Debug)]
struct ViewState<'a, T, U> {
    replica: Arc<Replica>,
    view_num: u32,
    listen_source: &'a mut EventSource<(Addr, ToReplica)>,
    transport: T,
    reply_transport: U,

    client_entires: HashMap<u32, ClientEntry>,
    requests: Vec<Request>,
    propose_op: u32,
    prepare_op: u32,
    commit_op: u32,
    quorum_handles: HashMap<u32, QuorumHandle>,
    log_entries: HashMap<u32, LogEntry>,
    reply_sessions: &'a mut JoinSet<crate::Result<(u32, Reply)>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientEntry {
    Committing(u32),
    Committed(Reply),
}

#[derive(Debug)]
struct QuorumHandle {
    prepare_session: JoinHandle<crate::Result<LogEntry>>,
    commit_session: JoinHandle<crate::Result<Quorum<Commit>>>,
    pre_prepare_event: Option<EventSender<(Message<PrePrepare>, Vec<Request>)>>,
    prepare_event: EventSender<Message<Prepare>>,
    commit_event: EventSender<Message<Commit>>,
}

impl<'a, T, U> ViewState<'a, T, U>
where
    T: Transport<ToReplica>,
    U: Transport<Reply>,
{
    fn new(
        replica: Arc<Replica>,
        view_num: u32,
        listen_source: &'a mut EventSource<(Addr, ToReplica)>,
        reply_sessions: &'a mut JoinSet<crate::Result<(u32, Reply)>>,
        transport: T,
        reply_transport: U,
    ) -> Self {
        Self {
            replica,
            view_num,
            listen_source,
            transport,
            reply_transport,
            client_entires: Default::default(),
            requests: Default::default(),
            propose_op: Default::default(),
            prepare_op: Default::default(),
            commit_op: Default::default(),
            quorum_handles: Default::default(),
            log_entries: Default::default(),
            reply_sessions,
        }
    }

    async fn session(&mut self) -> crate::Result<()> {
        loop {
            assert!(self.commit_op <= self.prepare_op);
            // inefficient and anonying workaround for cannot exclusively borrow multiple values
            // maybe get solved by `get_many_mut` if it get stablized
            let mut prepare_session = None;
            let mut commit_session = None;
            for (&op, handle) in &mut self.quorum_handles {
                if op == self.prepare_op + 1 {
                    prepare_session = Some(&mut handle.prepare_session)
                }
                if op <= self.prepare_op && op == self.commit_op + 1 {
                    commit_session = Some(&mut handle.commit_session)
                }
            }
            async fn join_option<T>(
                handle: Option<&mut JoinHandle<T>>,
            ) -> Option<crate::Result<T>> {
                if let Some(handle) = handle {
                    Some(handle.await.map_err(Into::into))
                } else {
                    None
                }
            }
            enum Select {
                Listen((Addr, ToReplica)),
                PrepareQuorum(LogEntry),
                CommitQuorum(Quorum<Commit>),
                Reply((u32, Reply)),
            }
            match tokio::select! {
                listen = self.listen_source.next() => Select::Listen(listen?),
                Some(entry) = join_option(prepare_session) => Select::PrepareQuorum(entry??),
                Some(commits) = join_option(commit_session) => Select::CommitQuorum(commits??),
                Some(reply) = self.reply_sessions.join_next() => Select::Reply(reply??),
            } {
                Select::Listen((_remote, message)) => match message {
                    ToReplica::Request(request) => self.handle_request(request)?,
                    ToReplica::PrePrepare(pre_prepare, requests) => {
                        self.handle_pre_prepare(pre_prepare.deserialize()?, requests)?
                    }
                    ToReplica::Prepare(prepare) => self.handle_prepare(prepare.deserialize()?)?,
                    ToReplica::Commit(commit) => self.handle_commit(commit.deserialize()?)?,
                },
                Select::PrepareQuorum(entry) => self.on_prepare_quorum(entry),
                Select::CommitQuorum(commits) => self.on_commit_quorum(commits),
                Select::Reply((client_id, reply)) => self.on_reply(client_id, reply),
            }
        }
    }

    fn handle_request(&mut self, request: Request) -> crate::Result<()> {
        match self.client_entires.get(&request.client_id) {
            Some(ClientEntry::Committing(request_num)) if *request_num >= request.request_num => {}
            Some(ClientEntry::Committed(reply)) if reply.request_num > request.request_num => {}
            _ => {
                if !self.replica.is_primary(self.view_num) {
                    todo!("relay Request to primary")
                } else {
                    self.client_entires.insert(
                        request.client_id,
                        ClientEntry::Committing(request.request_num),
                    );
                    self.requests.push(request);
                    if self.propose_op < self.commit_op + 1 {
                        self.close_batch()
                    }
                }
            }
        }
        Ok(())
    }

    fn close_batch(&mut self) {
        assert!(self.replica.is_primary(self.view_num));
        assert!(!self.requests.is_empty());

        self.propose_op += 1;
        let (prepare_event, prepare_source) = event_channel();
        let (commit_event, commit_source) = event_channel();
        let (digest, digest_promise) = promise_channel();
        self.quorum_handles.insert(
            self.propose_op,
            QuorumHandle {
                prepare_session: tokio::spawn(primary_prepare_session(
                    self.replica.clone(),
                    self.view_num,
                    self.propose_op,
                    self.requests
                        .drain(..100.min(self.requests.len()))
                        .collect(),
                    prepare_source,
                    digest,
                    self.transport.clone(),
                )),
                commit_session: tokio::spawn(commit_session(
                    self.replica.clone(),
                    self.view_num,
                    self.propose_op,
                    digest_promise,
                    commit_source,
                    self.transport.clone(),
                )),
                pre_prepare_event: None,
                prepare_event,
                commit_event,
            },
        );
    }

    // silently discard send message errors because the contacted sessions may already finished
    // collecting such kind of messages

    fn handle_pre_prepare(
        &mut self,
        pre_prepare: Message<PrePrepare>,
        requests: Vec<Request>,
    ) -> crate::Result<()> {
        if pre_prepare.op_num > self.prepare_op {
            let _ = self
                .get_or_insert_quorum_state(pre_prepare.op_num)
                .pre_prepare_event
                .as_ref()
                .ok_or(crate::err!("PrePrepare EventSender not exists"))?
                .send((pre_prepare, requests));
        }
        Ok(())
    }

    fn handle_prepare(&mut self, prepare: Message<Prepare>) -> crate::Result<()> {
        if prepare.op_num > self.prepare_op {
            let _ = self
                .get_or_insert_quorum_state(prepare.op_num)
                .prepare_event
                .send(prepare);
        }
        Ok(())
    }

    fn handle_commit(&mut self, commit: Message<Commit>) -> crate::Result<()> {
        if commit.op_num > self.commit_op {
            let _ = self
                .get_or_insert_quorum_state(commit.op_num)
                .commit_event
                .send(commit);
        }
        Ok(())
    }

    fn get_or_insert_quorum_state(&mut self, op_num: u32) -> &mut QuorumHandle {
        self.quorum_handles.entry(op_num).or_insert_with(|| {
            assert!(!self.replica.is_primary(self.view_num));
            let (pre_prepare_event, pre_prepare_source) = event_channel();
            let (prepare_event, prepare_source) = event_channel();
            let (commit_event, commit_source) = event_channel();
            let (digest, digest_promise) = promise_channel();
            // a more "standard" way may be only spawn commit session after prepared. the approach
            // here can handle Commit-Prepare (and even Commit-PrePrepare) reordering without
            // retransmission, by leveraging event channels as implicit buffers
            QuorumHandle {
                prepare_session: tokio::spawn(backup_prepare_session(
                    self.replica.clone(),
                    self.view_num,
                    pre_prepare_source,
                    prepare_source,
                    digest,
                    self.transport.clone(),
                )),
                commit_session: tokio::spawn(commit_session(
                    self.replica.clone(),
                    self.view_num,
                    op_num,
                    digest_promise,
                    commit_source,
                    self.transport.clone(),
                )),
                pre_prepare_event: Some(pre_prepare_event),
                prepare_event,
                commit_event,
            }
        })
    }

    fn on_prepare_quorum(&mut self, entry: LogEntry) {
        self.prepare_op += 1;
        for request in &entry.requests {
            if self.replica.is_primary(self.view_num) {
                assert!(matches!(
                    self.client_entires.get(&request.client_id),
                    Some(ClientEntry::Committing(request_num))
                        if *request_num >= request.request_num
                ))
            } else {
                self.client_entires.insert(
                    request.client_id,
                    ClientEntry::Committing(request.request_num),
                );
            }
        }
        self.log_entries.insert(self.prepare_op, entry);
    }

    fn on_commit_quorum(&mut self, commits: Quorum<Commit>) {
        self.commit_op += 1;
        self.quorum_handles.remove(&self.commit_op).unwrap();
        let entry = self
            .log_entries
            .get_mut(&self.commit_op)
            .expect("committed log entry exists");
        entry.commits = commits;

        let replica_id = self.replica.id;
        let view_num = self.view_num;
        for (request, app) in entry
            .requests
            .iter()
            .cloned()
            .zip(repeat(self.replica.app.clone()))
        {
            self.reply_sessions.spawn(async move {
                let reply = Reply {
                    request_num: request.request_num,
                    result: app.submit(request.op).await?,
                    replica_id,
                    view_num,
                };
                Ok((request.client_id, reply))
            });
        }

        if self.replica.is_primary(self.view_num) && !self.requests.is_empty() {
            self.close_batch()
        }
    }

    fn on_reply(&mut self, client_id: u32, reply: Reply) {
        // println!("{reply:?}");
        match self.client_entires.get(&client_id) {
            Some(ClientEntry::Committing(request_num)) => {
                assert!(*request_num >= reply.request_num);
                if *request_num != reply.request_num {
                    return;
                }
            }
            Some(ClientEntry::Committed(committed_reply)) => {
                assert_ne!(committed_reply.request_num, reply.request_num);
                if committed_reply.request_num > reply.request_num {
                    return;
                }
            }
            _ => {}
        }
        self.client_entires
            .insert(client_id, ClientEntry::Committed(reply.clone()));
        self.replica
            .send_to_client(client_id, reply, self.reply_transport.clone())
    }
}
