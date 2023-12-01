use std::{collections::HashMap, sync::Arc};

use borsh::{BorshDeserialize, BorshSerialize};
use tokio::{
    task::JoinSet,
    time::{timeout_at, Instant},
};

use crate::{
    channel::{EventSource, SubmitSource},
    transport::Addr,
    Client, Replica, Transport,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct Request {
    client_id: u32,
    request_num: u32,
    op: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct Reply {
    request_num: u32,
    result: Vec<u8>,
}

pub async fn client_session(
    client: Arc<Client>,
    mut invoke_source: SubmitSource<Vec<u8>, Vec<u8>>,
    mut source: EventSource<(Addr, Reply)>,
    transport: impl Transport<Request>,
) -> crate::Result<()> {
    let mut request_num = 0;

    while let Some((op, result)) = invoke_source.option_next().await {
        request_num += 1;
        let request = Request {
            client_id: client.id,
            request_num,
            op,
        };
        result.resolve(request_session(&client, request, &mut source, &transport).await?)?
    }
    Ok(())
}

async fn request_session(
    client: &Client,
    request: Request,
    source: &mut EventSource<(Addr, Reply)>,
    transport: &impl Transport<Request>,
) -> crate::Result<Vec<u8>> {
    loop {
        transport
            .send_to(client.addr_book.replica_addr(0)?, request.clone())
            .await?;
        let deadline = Instant::now() + client.retry_interval;
        while let Ok(reply) = timeout_at(deadline, source.next()).await {
            let (_remote, reply) = reply?;
            assert!(reply.request_num <= request.request_num);
            if reply.request_num == request.request_num {
                return Ok(reply.result);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClientEntry {
    Submitted(u32),
    Replied(Reply),
}

pub async fn replica_session(
    replica: Arc<Replica>,
    mut listen_source: EventSource<(Addr, Request)>,
    reply_transport: impl Transport<Reply>,
) -> crate::Result<()> {
    let mut entries = HashMap::new();
    let mut reply_sessions = JoinSet::new();

    loop {
        enum Select {
            Listen((Addr, Request)),
            Reply((u32, Reply)),
        }
        match tokio::select! {
            listen = listen_source.next() => Select::Listen(listen?),
            Some(reply) = reply_sessions.join_next() => Select::Reply(reply??),
            () = replica.stop.cancelled() => break,
        } {
            Select::Listen((_remote, request)) => match entries.get(&request.client_id) {
                Some(ClientEntry::Submitted(request_num))
                    if *request_num >= request.request_num =>
                {
                    continue
                }
                Some(ClientEntry::Replied(reply)) if reply.request_num > request.request_num => {
                    continue
                }
                Some(ClientEntry::Replied(reply)) if reply.request_num == request.request_num => {
                    replica.send_to_client(
                        request.client_id,
                        reply.clone(),
                        reply_transport.clone(),
                    );
                    continue;
                }
                _ => {
                    entries.insert(
                        request.client_id,
                        ClientEntry::Submitted(request.request_num),
                    );
                    let app = replica.app.clone();
                    reply_sessions.spawn(async move {
                        let reply = Reply {
                            request_num: request.request_num,
                            result: app.submit(request.op).await?,
                        };
                        Ok::<_, crate::Error>((request.client_id, reply))
                    });
                }
            },
            Select::Reply((client_id, reply)) => {
                let evicted = entries.insert(client_id, ClientEntry::Replied(reply.clone()));
                assert_eq!(evicted, Some(ClientEntry::Submitted(reply.request_num)));
                replica.send_to_client(client_id, reply, reply_transport.clone());
            }
        }
    }

    while let Some(result) = reply_sessions.join_next().await {
        result??;
    }
    Ok(())
}
