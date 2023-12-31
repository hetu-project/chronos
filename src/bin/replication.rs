use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::State,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use halloween::{
    app::{null_session, NullWorkload},
    crypto::{Signer, Verifier},
    event_channel,
    net::UdpSocket,
    pbft,
    replication::{close_loop_session, AddrBook, SocketAddrBook},
    task::BackgroundMonitor,
    transport::Addr,
    unreplicated, Client, Replica,
};
use replication_control_messages as messages;
use tokio::{
    net::TcpListener,
    task::{JoinHandle, JoinSet},
    time::timeout,
};
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
        .route("/run-client", post(run_client))
        .route("/join-client", post(join_client))
        .route("/run-replica", post(run_replica))
        .route("/reset-replica", post(reset_replica))
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
    client_session: Mutex<Option<JoinHandle<(f32, Duration)>>>,
    replica_session: Mutex<Option<(JoinHandle<()>, CancellationToken)>>,
    shutdown: CancellationToken,
}

type App = State<Arc<AppState>>;

async fn run_client(State(state): App, Json(payload): Json<messages::Client>) {
    *state.client_session.lock().unwrap() = Some(tokio::spawn(run_client_internal(
        payload,
        state.shutdown.clone(),
    )));
}

async fn run_client_internal(
    config: messages::Client,
    shutdown: CancellationToken,
) -> (f32, Duration) {
    match async {
        let mut monitor = BackgroundMonitor::default();
        let spawner = monitor.spawner();
        let stop_listen = CancellationToken::new();

        let addr_book = AddrBook::Socket(config.addr_book.into());
        let mut close_loop_sessions = JoinSet::new();
        let mut client_sessions = Vec::new();
        for id in config.id_range {
            let client = Client {
                id,
                num_replica: config.num_replica,
                num_faulty: config.num_faulty,
                addr_book: addr_book.clone(),
                retry_interval: Duration::from_millis(100),
            };
            let (invoke_event, invoke_source) = event_channel();
            let socket = UdpSocket::bind(addr_book.client_addr(id)?).await?;

            match config.protocol {
                messages::Protocol::Unreplicated => {
                    let (event, source) = event_channel();
                    spawner.spawn(socket.clone().listen_session::<unreplicated::Reply>(event));
                    client_sessions.push(spawner.spawn(unreplicated::client_session(
                        client.into(),
                        invoke_source,
                        source,
                        socket,
                    )));
                }
                messages::Protocol::Pbft => {
                    let (event, source) = event_channel();
                    spawner.spawn(socket.clone().listen_session::<pbft::Reply>(event));
                    client_sessions.push(spawner.spawn(pbft::client_session(
                        client.into(),
                        invoke_source,
                        source,
                        socket,
                    )));
                }
            }

            close_loop_sessions.spawn(close_loop_session(
                NullWorkload,
                Duration::from_secs(10),
                invoke_event,
            ));
        }
        drop(spawner);

        let join_task = tokio::spawn(async move {
            let mut latencies = Vec::new();
            while let Some(result) = close_loop_sessions.join_next().await {
                latencies.extend(result??)
            }
            Ok::<_, halloween::Error>((
                latencies.len() as f32 / 10.,
                latencies.iter().sum::<Duration>() / latencies.len() as _,
            ))
        });
        let result = timeout(
            Duration::from_secs(10) + Duration::from_millis(100),
            monitor.wait_task(join_task),
        )
        .await????;
        for client_session in client_sessions {
            monitor.wait_task(client_session).await??
        }
        stop_listen.cancel();
        timeout(Duration::from_millis(100), monitor.wait()).await??;
        Ok::<_, halloween::Error>(result)
    }
    .await
    {
        Ok(result) => result,
        Err(err) => {
            eprintln!("{err}");
            eprint!("{}", err.backtrace());
            shutdown.cancel();
            panic!() // well
        }
    }
}

async fn join_client(State(state): App) -> impl IntoResponse {
    let session = {
        let mut client_session = state.client_session.lock().unwrap();
        let Some(session) = client_session.as_ref() else {
            return Json(None);
        };
        if !session.is_finished() {
            return Json(None);
        }
        client_session.take().unwrap()
    };
    Json(Some(session.await.unwrap()))
}

async fn run_replica(State(state): App, Json(payload): Json<messages::Replica>) {
    let reset = CancellationToken::new();
    let replica_session = tokio::spawn(run_replica_internal(
        payload,
        reset.clone(),
        state.shutdown.clone(),
    ));
    *state.replica_session.lock().unwrap() = Some((replica_session, reset));
}

async fn run_replica_internal(
    config: messages::Replica,
    reset: CancellationToken,
    shutdown: CancellationToken,
) {
    if let Err(err) = async {
        let mut monitor = BackgroundMonitor::default();
        let spawner = monitor.spawner();

        let (app_event, app_source) = event_channel();
        spawner.spawn(null_session(app_source));

        let mut addr_book = SocketAddrBook::from(config.addr_book);
        let socket = UdpSocket::bind(Addr::Socket(addr_book.remove_addr(config.id)?)).await?;
        let stop = CancellationToken::new();

        let mut verifiers = HashMap::new();
        for i in 0..config.num_replica {
            verifiers.insert(i as _, Verifier::from(Signer::new_hardcoded(i)));
        }
        let replica = Replica {
            id: config.id,
            num_replica: config.num_replica,
            num_faulty: config.num_faulty,
            app: app_event,
            spawner: spawner.clone(),
            signer: Signer::new_hardcoded(config.id as _),
            verifiers,
            addr_book: AddrBook::Socket(addr_book),
            stop: stop.clone(),
        };

        match config.protocol {
            messages::Protocol::Unreplicated => {
                let (listen_event, listen_source) = event_channel();
                spawner.spawn(
                    socket
                        .clone()
                        .listen_session::<unreplicated::Request>(listen_event),
                );
                spawner.spawn(unreplicated::replica_session(
                    replica.into(),
                    listen_source,
                    socket,
                ));
            }
            messages::Protocol::Pbft => {
                let (listen_event, listen_source) = event_channel();
                spawner.spawn(
                    socket
                        .clone()
                        .listen_session::<pbft::ToReplica>(listen_event),
                );
                spawner.spawn(pbft::replica_session(
                    replica.into(),
                    listen_source,
                    socket.clone(),
                    socket,
                ));
            }
        }
        drop(spawner);
        monitor.wait_task(reset.cancelled()).await?;
        stop.cancel();
        timeout(Duration::from_millis(100), monitor.wait()).await??;
        Ok::<_, halloween::Error>(())
    }
    .await
    {
        eprintln!("{err}");
        eprint!("{}", err.backtrace());
        shutdown.cancel()
    }
}

async fn reset_replica(State(state): App) {
    let (session, reset) = state.replica_session.lock().unwrap().take().unwrap();
    reset.cancel();
    session.await.unwrap()
}
