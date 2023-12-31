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

use halloween::{
    channel::EventSource,
    crypto::{Signer, Verifier},
    event_channel,
    kademlia::{self, Buckets, Location, Peer, PeerRecord},
    net::UdpSocket,
    task::BackgroundMonitor,
    transport::Addr,
};
use kademlia_control_messages as messages;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> halloween::Result<()> {
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
        .route("/bootstrap-peer", post(bootstrap_peer))
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

type SubscribeHandle = halloween::channel::SubscribeHandle<(Location, usize), Vec<PeerRecord>>;
type App = State<Arc<AppState>>;

async fn run_peer(State(state): App, Json(config): Json<messages::Config>) -> impl IntoResponse {
    async {
        let shutdown = state.shutdown.clone();
        let (monitor, spawner) = BackgroundMonitor::start(move |err| {
            println!("{err}");
            println!("{}", err.backtrace());
            shutdown.cancel()
        });

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
                    .collect::<halloween::Result<Vec<_>>>()?,
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
        let addr = Addr::Socket((host, 20000 + config.index.1 as u16).into());
        let run_record = PeerRecord::new(&signer, addr.clone())?;
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
            signer,
            spawner: spawner.clone(),
        };
        let (message_event, mut message_source) = event_channel();
        let socket = UdpSocket::bind(addr).await?;
        spawner.spawn(socket.clone().listen_session(message_event));
        let (subscribe_handle, subscribe_source) = event_channel();
        spawner.spawn(async move {
            kademlia::session(
                peer.into(),
                &mut buckets,
                subscribe_source,
                &mut message_source,
                socket,
            )
            .await
        });

        let mut session = state.session.lock().unwrap();
        assert!(session.is_none());
        *session = Some((monitor, subscribe_handle));
        Ok::<_, halloween::Error>(Json(messages::Peer::from(run_record)))
    }
    .await
    .unwrap_or_else(|err| {
        eprintln!("{err}");
        eprintln!("{}", err.backtrace());
        state.shutdown.cancel();
        panic!()
    })
}

async fn bootstrap_peer(
    State(state): App,
    Json(config): Json<messages::BootstrapConfig>,
) -> impl IntoResponse {
    async {
        let shutdown = state.shutdown.clone();
        let (monitor, spawner) = BackgroundMonitor::start(move |err| {
            eprintln!("{err}");
            eprintln!("{}", err.backtrace());
            shutdown.cancel()
        });

        let (secret_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
        let signer = Signer::from(secret_key);
        let socket = UdpSocket::bind(Addr::Socket((config.host, 0).into())).await?;

        let (message_event, mut message_source) = event_channel();
        spawner.spawn(
            socket
                .clone()
                .listen_session::<kademlia::Message>(message_event),
        );
        let peer_record = PeerRecord::new(&signer, Addr::Socket(socket.0.local_addr()?))?;
        let peer = Arc::new(Peer {
            verifier: Verifier::from(&signer),
            signer,
            spawner: spawner.clone(),
        });
        let mut buckets = Buckets::new(peer_record.clone());
        buckets.insert(config.seed_peer.try_into()?);
        kademlia::bootstrap_session(
            peer.clone(),
            &mut buckets,
            &mut message_source,
            socket.clone(),
        )
        .await?;
        println!("bootstrap done");
        let (subscribe_handle, subscribe_source) = event_channel();
        spawner.spawn(async move {
            kademlia::session(
                peer,
                &mut buckets,
                subscribe_source,
                &mut message_source,
                socket,
            )
            .await
        });
        drop(spawner);

        let mut session = state.session.lock().unwrap();
        assert!(session.is_none());
        *session = Some((monitor, subscribe_handle));
        Ok::<_, halloween::Error>(Json(messages::Peer::from(peer_record)))
    }
    .await
    .unwrap_or_else(|err| {
        eprintln!("{err}");
        eprintln!("{}", err.backtrace());
        state.shutdown.cancel();
        panic!()
    })
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
