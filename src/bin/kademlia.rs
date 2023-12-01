use std::{
    iter::repeat_with,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};

use hetu::{
    channel::{EventSource, PromiseSender, SubscribeSource},
    crypto::{Signer, Verifier},
    event_channel,
    kademlia::{self, Buckets, Location, Peer, PeerRecord},
    net::UdpSocket,
    promise_channel,
    task::BackgroundMonitor,
    transport::Addr,
};
use kademlia_control_messages as messages;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> hetu::Result<()> {
    std::env::set_var("RUST_BACKTRACE", "1");
    let port = std::env::args()
        .nth(1)
        .as_deref()
        .unwrap_or("10000")
        .parse::<u16>()?;
    let app = AppState::default();
    let shutdown = app.shutdown.clone();
    let app = Router::new()
        .route("/ok", get(|| async {}))
        .route("/run-peer", post(run_peer))
        .route("/find-peer", post(find_peer))
        .route("/find-peer/:id", get(poll_find_peer))
        .with_state(app.into());
    let signal_task = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            let result = tokio::select! {
                result = tokio::signal::ctrl_c() => result,
                result = shutdown.cancelled() => Ok(result),
            };
            shutdown.cancel();
            result
        }
    });
    // we lose graceful shutdown (in a easy way) after upgrading axum to 0.7, so tentatively give up
    // on that
    // also not sure why only type checks with extra async wrapper
    let serve = async { axum::serve(TcpListener::bind(("0.0.0.0", port)).await?, app).await };
    tokio::select! {
        result = serve => result?,
        result = signal_task => result??,
    }
    Ok(())
}

#[derive(Default)]
struct AppState {
    shutdown: CancellationToken,
    session: Mutex<Option<(JoinHandle<()>, SubscribeHandle)>>,
    sources: Mutex<Vec<EventSource<Vec<PeerRecord>>>>,
}

type SubscribeHandle = hetu::channel::SubscribeHandle<(Location, usize), Vec<PeerRecord>>;
type App = State<Arc<AppState>>;

async fn run_peer(State(state): App, Json(payload): Json<messages::Config>) -> impl IntoResponse {
    let (record, promise_record) = promise_channel();
    {
        let mut session = state.session.lock().unwrap();
        assert!(session.is_none());
        let (handle, source) = event_channel();
        *session = Some((
            tokio::spawn(run_peer_interal(
                payload,
                source,
                state.shutdown.clone(),
                record,
            )),
            handle,
        ))
    }
    let record = promise_record.await.unwrap();
    let Addr::Socket(addr) = record.addr else {
        unimplemented!()
    };
    Json((record.id, addr))
}

async fn run_peer_interal(
    config: messages::Config,
    source: SubscribeSource<(Location, usize), Vec<PeerRecord>>,
    shutdown: CancellationToken,
    record: PromiseSender<PeerRecord>,
) {
    if let Err(err) = async {
        let mut rng = StdRng::seed_from_u64(config.seed);
        let mut records = Vec::new();
        let mut signer = None;
        for (i, host) in config.hosts.iter().enumerate() {
            let host_signers = repeat_with(|| {
                let (secret_key, _) = secp256k1::generate_keypair(&mut rng);
                Signer::from(secret_key)
            })
            .take(config.num_host_peer)
            .collect::<Vec<_>>();
            records.extend(
                host_signers
                    .iter()
                    .enumerate()
                    .map(|(i, signer)| {
                        PeerRecord::new(signer, Addr::Socket((*host, 20000 + i as u16).into()))
                    })
                    .collect::<hetu::Result<Vec<_>>>()?,
            );
            if i == config.index.0 {
                signer = Some(host_signers[config.index.1])
            }
        }
        let signer = signer.unwrap();

        let host = config.hosts[config.index.0];
        let mut rng = repeat_with(|| StdRng::from_seed(rng.gen()))
            .nth(config.index.0 * config.num_host_peer + config.index.1)
            .unwrap();
        let monitor = BackgroundMonitor::default();
        let spawner = monitor.spawner();
        let addr = Addr::Socket((host, 20000 + config.index.1 as u16).into());
        let run_record = PeerRecord::new(&signer, addr.clone())?;
        record.resolve(run_record.clone())?;
        let mut buckets = Buckets::new(run_record.clone());
        records.shuffle(&mut rng);
        for record in &records {
            if record.id == run_record.id {
                continue;
            }
            buckets.insert(record.clone())
        }
        let peer = Peer {
            verifier: Verifier::from(&signer),
            signer: signer.into(),
            spawner: spawner.clone(),
        };
        let (message_event, message_source) = event_channel();
        let socket = UdpSocket::bind(addr).await?;
        spawner.spawn(socket.clone().listen_session(message_event));
        spawner.spawn(kademlia::session(
            peer.into(),
            buckets,
            source,
            message_source,
            socket.into_transport::<kademlia::Message>(),
        ));
        drop(spawner);
        monitor.wait().await?;
        Ok::<_, hetu::Error>(())
    }
    .await
    {
        println!("{err:}");
        println!("{}", err.backtrace());
        shutdown.cancel()
    }
}

async fn find_peer(
    State(state): App,
    Json(payload): Json<messages::FindPeer>,
) -> impl IntoResponse {
    let source = state
        .session
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .1
        .subscribe((payload.target, payload.count))
        .unwrap();
    let mut sources = state.sources.lock().unwrap();
    let id = sources.len();
    sources.push(source);
    Json(id)
}

async fn poll_find_peer(State(state): App, Path(id): Path<usize>) -> impl IntoResponse {
    let source = &mut state.sources.lock().unwrap()[id];
    let mut messages = Vec::new();
    let err = loop {
        match source.0.try_recv() {
            Ok(message) => messages.push(
                message
                    .into_iter()
                    .map(|record| {
                        let Addr::Socket(addr) = record.addr else {
                            unimplemented!()
                        };
                        (record.id, addr)
                    })
                    .collect::<Vec<_>>(),
            ),
            Err(err) => break (err),
        }
    };
    Json((
        messages,
        matches!(err, tokio::sync::mpsc::error::TryRecvError::Disconnected),
    ))
}
