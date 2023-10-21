use std::{
    mem::replace,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router, Server,
};
use control_messages::{BenchmarkStats, Role, Task};
use permissioned_blockchain::{
    app::{ycsb, Workload},
    client::{run_benchmark, RunBenchmarkConfig},
    common::set_affinity,
    context::{
        crypto::{Signer, Verifier},
        ordered_multicast::Variant,
        tokio::Dispatch,
        Addr,
    },
    hotstuff, minbft, neo, pbft, unreplicated, zyzzyva, App, Config,
};
use rand::{rngs::StdRng, SeedableRng};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
enum AppState {
    Idle, // TODO exit on timeout
    Panicked,

    BenchmarkClientRunning,
    BenchmarkClientFinish {
        stats: BenchmarkStats,
    },
    ReplicaRunning {
        cancel: CancellationToken,
        task: JoinHandle<()>,
    },
}

async fn set_task(State(state): State<Arc<Mutex<AppState>>>, Json(task): Json<Task>) {
    assert!(matches!(*state.lock().unwrap(), AppState::Idle));

    let mut replication_config =
        Config::new_socket(task.client_addrs, task.replica_addrs, task.num_faulty);
    replication_config.multicast_addr = Some(Addr::Socket(task.multicast_addr));

    let mut rng = StdRng::seed_from_u64(task.seed);
    match task.role {
        Role::BenchmarkClient(config) => {
            *state.lock().unwrap() = AppState::BenchmarkClientRunning;
            let workload = match task.app {
                control_messages::App::Null => Workload::Null,
                control_messages::App::Ycsb(config) => {
                    Workload::Ycsb(ycsb::Workload::new(config.into(), &mut rng))
                }
            };

            let benchmark_config = RunBenchmarkConfig {
                replication_config,
                offset: config.offset,
                num_group: config.num_group,
                num_client: config.num_client,
                duration: config.duration,
                workload,
            };
            // println!("{benchmark_config:?}");
            let state = state.clone();
            tokio::task::spawn_blocking(move || {
                let latencies = match &*task.mode {
                    "unreplicated" => run_benchmark(benchmark_config, unreplicated::Client::new),
                    "neo-hm" | "neo-pk" | "neo-bn" => {
                        run_benchmark(benchmark_config, neo::Client::new)
                    }
                    "pbft" => run_benchmark(benchmark_config, pbft::Client::new),
                    "zyzzyva" | "zyzzyva-f" => run_benchmark(benchmark_config, |context, index| {
                        zyzzyva::Client::new(context, index, task.mode == "zyzzyva-f")
                    }),
                    "hotstuff" => run_benchmark(benchmark_config, hotstuff::Client::new),
                    "minbft" => run_benchmark(benchmark_config, minbft::Client::new),
                    _ => unimplemented!(),
                };
                *state.lock().unwrap() = AppState::BenchmarkClientFinish {
                    stats: BenchmarkStats {
                        throughput: latencies.len() as f32 / config.duration.as_secs_f32(),
                        average_latency: latencies
                            .iter()
                            .sum::<Duration>()
                            .checked_div(latencies.len() as u32),
                    },
                };
            });
        }
        Role::Replica(replica) => {
            let app = match task.app {
                control_messages::App::Null => App::Null,
                control_messages::App::Ycsb(config) => {
                    App::Ycsb(ycsb::Workload::app(config.into(), &mut rng))
                }
            };

            let cancel = CancellationToken::new();
            let task = tokio::task::spawn_blocking({
                let cancel = cancel.clone();
                move || {
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap();
                    let variant = Arc::new(match &*task.mode {
                        "neo-hm" => Variant::new_half_sip_hash(replica.index),
                        "neo-pk" | "neo-bn" => Variant::new_k256(),
                        _ => Variant::Unreachable,
                    });
                    let mut dispatch = Dispatch::new(runtime.handle().clone(), variant.clone());

                    let handle = dispatch.handle();
                    std::thread::spawn(move || {
                        set_affinity(0);
                        runtime.block_on(async move {
                            cancel.cancelled().await;
                            handle.stop_async().await
                        });
                        runtime.shutdown_background()
                    });

                    set_affinity(1);
                    let addr = replication_config.replica_addrs[replica.index as usize];
                    let signer = Signer::new_standard(
                        permissioned_blockchain::context::crypto::hardcoded_ed25519(replica.index),
                        // permissioned_blockchain::context::crypto::hardcoded_k256(replica.index),
                    );
                    let mut verifier = Verifier::new_standard(variant);
                    for index in 0..replication_config.replica_addrs.len() {
                        verifier.insert_verifying_key(
                            index as _,
                            permissioned_blockchain::context::crypto::hardcoded_ed25519(index as _)
                                // permissioned_blockchain::context::crypto::hardcoded_k256(index as _)
                                .verifying_key(),
                        )
                    }
                    match &*task.mode {
                        "unreplicated" => {
                            assert_eq!(replica.index, 0);
                            let mut replica = unreplicated::Replica::new(
                                dispatch
                                    .register(addr, signer)
                                    .into_replication(replication_config),
                                app,
                            );
                            // replica.make_blocks = true;
                            dispatch.run(&mut replica, verifier)
                        }
                        "neo-hm" | "neo-pk" | "neo-bn" => {
                            let mut replica = neo::Replica::new(
                                dispatch
                                    .register(addr, signer)
                                    .into_replication(replication_config.clone()),
                                replica.index,
                                app,
                                task.mode == "neo-bn",
                            );
                            dispatch.drop_rate = task.drop_rate;
                            dispatch
                                .enable_ordered_multicast(
                                    replication_config.multicast_addr.unwrap(),
                                )
                                .run(&mut replica, verifier)
                        }
                        "pbft" => {
                            let mut replica = pbft::Replica::new(
                                dispatch
                                    .register(addr, signer)
                                    .into_replication(replication_config.clone()),
                                replica.index,
                                app,
                            );
                            dispatch.run(&mut replica, verifier)
                        }
                        "zyzzyva" | "zyzzyva-f" => {
                            let mut replica = zyzzyva::Replica::new(
                                dispatch
                                    .register(addr, signer)
                                    .into_replication(replication_config.clone()),
                                replica.index,
                                app,
                            );
                            dispatch.run(&mut replica, verifier)
                        }
                        "hotstuff" => {
                            let mut replica = hotstuff::Replica::new(
                                dispatch
                                    .register(addr, signer)
                                    .into_replication(replication_config.clone()),
                                replica.index,
                                app,
                            );
                            dispatch.run(&mut replica, verifier)
                        }
                        "minbft" => {
                            let mut replica = minbft::Replica::new(
                                dispatch
                                    .register(addr, signer)
                                    .into_replication(replication_config.clone()),
                                replica.index,
                                app,
                            );
                            dispatch.run(&mut replica, verifier)
                        }
                        _ => unimplemented!(),
                    }
                    // TODO return stats
                }
            });
            *state.lock().unwrap() = AppState::ReplicaRunning { cancel, task };
        }
    }
}

async fn poll_benchmark(State(state): State<Arc<Mutex<AppState>>>) -> Json<Option<BenchmarkStats>> {
    let state = state.lock().unwrap();
    match &*state {
        AppState::BenchmarkClientRunning | AppState::Panicked => Json(None),
        &AppState::BenchmarkClientFinish { stats } => Json(Some(stats)),
        _ => {
            drop(state);
            unimplemented!()
        }
    }
}

async fn poll_panic(State(state): State<Arc<Mutex<AppState>>>) -> Json<bool> {
    Json(matches!(*state.lock().unwrap(), AppState::Panicked))
}

async fn reset(State(state): State<Arc<Mutex<AppState>>>) {
    let state = {
        let mut state = state.lock().unwrap();
        replace(&mut *state, AppState::Idle)
    };
    match state {
        AppState::BenchmarkClientFinish { .. } => {}
        AppState::ReplicaRunning { cancel, task } => {
            cancel.cancel();
            task.await.unwrap()
        }
        _ => unimplemented!(),
    }
}

fn main() {
    let state = Arc::new(Mutex::new(AppState::Idle));
    let hook = std::panic::take_hook();
    std::panic::set_hook({
        let state = state.clone();
        Box::new(move |info| {
            if let Ok(mut state) = state.try_lock() {
                *state = AppState::Panicked
            } else {
                println!("fail to panicking app state")
            }
            hook(info)
        })
    });

    let app = Router::new()
        .route("/panic", get(poll_panic))
        .route("/task", post(set_task))
        .route("/reset", post(reset))
        .route("/benchmark", get(poll_benchmark))
        .with_state(state);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime
        .block_on(async move {
            Server::bind(&"0.0.0.0:9999".parse().unwrap())
                .serve(app.into_make_service())
                .with_graceful_shutdown(async move { tokio::signal::ctrl_c().await.unwrap() })
                .await
        })
        .unwrap();
    runtime.shutdown_background()
}
