use std::{
    collections::HashMap,
    iter::repeat,
    sync::{Arc, Barrier},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use serde::de::DeserializeOwned;
use tokio_util::sync::CancellationToken;

use crate::{
    app::Workload,
    common::set_affinity,
    context::{
        crypto::{Signer, Verifier, Verify},
        ordered_multicast::Variant,
        tokio::{Dispatch, DispatchHandle},
        Addr,
    },
    ClientIndex, ReplicaIndex,
};

pub trait OnResult {
    fn apply(self: Box<Self>, result: Vec<u8>);
}

impl<F: FnOnce(Vec<u8>)> OnResult for F {
    fn apply(self: Box<Self>, result: Vec<u8>) {
        self(result)
    }
}

pub type BoxedConsume = Box<dyn OnResult + Send + Sync>;

impl<T: OnResult + Send + Sync + 'static> From<T> for BoxedConsume {
    fn from(value: T) -> Self {
        Box::new(value)
    }
}

impl std::fmt::Debug for BoxedConsume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("BoxedConsume").field(&"..").finish()
    }
}

pub trait Client {
    type Message;

    fn invoke(&self, op: Vec<u8>, consume: impl Into<BoxedConsume>);

    fn abort(&self) -> Option<BoxedConsume> {
        unimplemented!()
    }

    fn handle(&self, message: Self::Message);

    // on timer
}

impl<T: Client> Client for Arc<T> {
    type Message = T::Message;

    fn invoke(&self, op: Vec<u8>, consume: impl Into<BoxedConsume>) {
        T::invoke(self, op, consume)
    }

    fn abort(&self) -> Option<BoxedConsume> {
        T::abort(self)
    }

    fn handle(&self, message: Self::Message) {
        T::handle(self, message)
    }
}

#[derive(Debug)]
pub struct Benchmark<C> {
    clients: HashMap<Addr, Arc<C>>,
    bootstrap: bool,
    finish_sender: flume::Sender<(Addr, Duration)>,
    finish_receiver: flume::Receiver<(Addr, Duration)>,
    pub latencies: Vec<Duration>,
}

impl<C> Default for Benchmark<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C> Benchmark<C> {
    pub fn new() -> Self {
        let (finish_sender, finish_receiver) = flume::unbounded();
        Self {
            clients: Default::default(),
            bootstrap: true,
            finish_sender,
            finish_receiver,
            latencies: Default::default(),
        }
    }

    pub fn insert_client(&mut self, addr: Addr, client: C) {
        let evicted = self.clients.insert(addr, Arc::new(client));
        assert!(evicted.is_none())
    }

    pub fn close_loop(
        &mut self,
        duration: Duration,
        workload: &Workload,
        runtime: tokio::runtime::Handle,
    ) where
        C: Client + Send + Sync + 'static,
    {
        let invoke = |index, client: Arc<C>| {
            let txn = workload.generate(client.clone(), &mut rand::thread_rng());
            let finish_sender = self.finish_sender.clone();
            async move {
                let start = Instant::now();
                txn.await;
                finish_sender.send((index, start.elapsed())).unwrap()
            }
        };

        if self.bootstrap {
            for (i, (&index, client)) in self.clients.iter().enumerate() {
                // synchronously finish the first invocation, to avoid first-packet reordering
                if i == 0 {
                    runtime.block_on(invoke(index, client.clone()))
                } else {
                    runtime.spawn(invoke(index, client.clone()));
                }
            }
            self.bootstrap = false;
        }
        let deadline = Instant::now() + duration;
        while let Ok((index, latency)) = self.finish_receiver.recv_deadline(deadline) {
            self.latencies.push(latency);
            runtime.spawn(invoke(index, self.clients[&index].clone()));
        }
    }

    pub fn run_dispatch(&self) -> impl FnOnce(&mut crate::context::tokio::Dispatch) + Send
    where
        C: Client + Send + Sync + 'static,
        C::Message: DeserializeOwned + Verify<ReplicaIndex>,
    {
        struct R<C>(HashMap<Addr, Arc<C>>);
        impl<C> crate::context::Receivers for R<C>
        where
            C: Client,
        {
            type Message = C::Message;

            fn handle(&mut self, receiver: Addr, _: Addr, message: Self::Message) {
                self.0[&receiver].handle(message)
            }

            fn on_timer(&mut self, receiver: Addr, _: crate::context::TimerId) {
                panic!("{receiver:?} timeout")
            }
        }

        let mut receivers = R(self.clients.clone());
        move |runtime| runtime.run(&mut receivers, Verifier::<ReplicaIndex>::Nop)
    }
}

#[derive(Debug)]
pub struct RunBenchmarkConfig {
    pub replication_config: crate::Config,
    pub offset: usize,
    pub num_group: usize,
    pub num_client: usize,
    pub duration: Duration,
    pub workload: Workload,
}

// a benchmark runner that is already almost decoupled with replication
// just a few tweak away from lifting into context library
// maybe do so when there's other uses than in this crate

pub fn run_benchmark<C>(
    config: RunBenchmarkConfig,
    new_client: impl Fn(crate::Context<C::Message>, ClientIndex) -> C,
) -> Vec<Duration>
where
    C: Client + Send + Sync + 'static,
    C::Message: DeserializeOwned + Verify<ReplicaIndex>,
{
    struct Group<C> {
        benchmark_thread: JoinHandle<Benchmark<C>>,
        runtime_thread: JoinHandle<()>,
        dispatch_thread: JoinHandle<()>,
        dispatch_handle: DispatchHandle,
    }

    // println!("{config:?}");
    let barrier = Arc::new(Barrier::new(config.num_group));
    let replication_config = Arc::new(config.replication_config);
    let groups = Vec::from_iter(
        repeat((barrier, Arc::new(config.workload)))
            .take(config.num_group)
            .enumerate()
            .map(|(group_index, (barrier, workload))| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let handle = runtime.handle().clone();
                let mut dispatch = Dispatch::new(handle.clone(), Variant::Unreachable);

                let mut benchmark = Benchmark::new();
                for group_offset in 0..config.num_client {
                    let index = config.offset + group_index * config.num_client + group_offset;
                    let addr = replication_config.client_addrs[index];
                    let client = new_client(
                        dispatch
                            .register(addr, Signer::new_standard(None))
                            .into_replication(replication_config.clone()),
                        index as _,
                    );
                    benchmark.insert_client(addr, client);
                }

                let cancel = CancellationToken::new();
                let runtime_thread = std::thread::spawn({
                    set_affinity(group_index * 2);
                    let cancel = cancel.clone();
                    move || runtime.block_on(cancel.cancelled())
                });

                let dispatch_handle = dispatch.handle();
                let run = benchmark.run_dispatch();
                let dispatch_thread = std::thread::spawn(move || {
                    set_affinity(group_index * 2 + 1);
                    run(&mut dispatch);
                    cancel.cancel()
                });

                let benchmark_thread = std::thread::spawn(move || {
                    set_affinity(group_index * 2 + 1);
                    if group_index == 0 {
                        benchmark.close_loop(Duration::from_secs(1), &workload, handle.clone());
                    }
                    barrier.wait();
                    benchmark.close_loop(Duration::from_secs(1), &workload, handle.clone());
                    benchmark.latencies.clear();
                    benchmark.close_loop(config.duration, &workload, handle);
                    benchmark
                });

                Group {
                    benchmark_thread,
                    runtime_thread,
                    dispatch_thread,
                    dispatch_handle,
                }
            }),
    );

    let mut latencies = Vec::new();
    for group in groups {
        let benchmark = group.benchmark_thread.join().unwrap();
        latencies.extend(benchmark.latencies);
        group.dispatch_handle.stop();
        group.dispatch_thread.join().unwrap();
        group.runtime_thread.join().unwrap();
    }
    latencies
}
