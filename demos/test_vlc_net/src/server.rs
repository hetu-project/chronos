use anyhow::anyhow;
use base58::FromBase58;
use base64::{engine::general_purpose, Engine as _};
use bincode::Options;
use gossip::network::{Discover, Event, Network, Sha256Topic};
use libp2p::{
    futures::StreamExt,
    identity::{self, Keypair, PublicKey},
    multiaddr, PeerId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
};
use std::{fs::File, str::FromStr};
use sysinfo::{ProcessesToUpdate, System};
use tokio::{
    sync::{mpsc, watch, OwnedSemaphorePermit, RwLock, Semaphore},
    time::{self, sleep, timeout, Duration},
};
use tracing::{debug, error, info, warn};
use vlc::ordinary_clock::OrdinaryClock;

const _DEFAULT_PAYLOAD: &str = "Hello Hetu";
const NET_TIMEOUT: u64 = 1; // one second
const MB_TO_BYTES: u64 = 1024 * 1024; // 1 MB = 1024 * 1024 bytes

// Metric visualization can utilities prometheus, and grafana toolboxs.
#[derive(Debug, Clone)]
struct OuterMsg {
    data: Vec<u8>,
    topic: Sha256Topic,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct P2PMsg {
    clock: OrdinaryClock,
    data: Vec<u8>,
    pub_key: Option<Vec<u8>>,
    signature: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub topic: String,
    pub private_key: String,
    pub multi_addr: String,
    pub bootnodes: Option<Vec<String>>,
    pub discover: Discover,
    pub max_discover_node: u32,
    pub enable_tx_send: bool,
    pub concurrent_verify: u64,
    pub trigger_us: u64,
    pub max_sys_memory_percent: u64,
    pub min_sys_memory_percent: u64,
    pub max_proc_memory_mb: u64,
    pub disconnect_rate: f64,
    pub time_window_s: u64,
    pub init_clock_keys: u32,
    pub payload_bytes_len: u64,
    pub print_vlc: bool,
    pub tokio_console: bool,
    pub tokio_console_port: u16,
    pub pprof: bool,
}

pub struct Server {
    pub config: Arc<ServerConfig>,
    pub node_id: u64,
    pub peer_id: PeerId,
    pub local_key: Keypair,
    pub state: Arc<RwLock<ServerState>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StateUpdate {
    IncrementSendCount(usize),
    IncrementReceiveCount(usize, VLCUpdate),
    UpdateClock(VLCUpdate),
    Exit(()),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VLCUpdate {
    others: Vec<OrdinaryClock>,
    id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryWatcherConfig {
    is_bootnode: bool,
    is_sender: bool,
    proc_max_mem_mb: u64,
    sys_max_mem_usage: f64,
    sys_min_mem_usage: f64,
    sys_disconnect_mem: f64,
}

impl Server {
    fn generate_data_payload(payload_bytes_len: usize) -> String {
        if payload_bytes_len <= 10 {
            _DEFAULT_PAYLOAD.to_string()
        } else {
            format!("{}{}", _DEFAULT_PAYLOAD, "A".repeat(payload_bytes_len - 10))
        }
    }

    fn mb_to_bytes(mb: u64) -> u64 {
        mb * MB_TO_BYTES
    }

    fn identity_to_u64(identity: &str) -> Option<u64> {
        let bytes = identity.from_base58().ok()?;

        // Hash the bytes using SHA-256
        let hash = Sha256::digest(&bytes);

        // Take the first 8 bytes of the hash and convert to u64
        let mut u64_bytes = [0u8; 8];
        u64_bytes.copy_from_slice(&hash[..8]);

        Some(u64::from_be_bytes(u64_bytes))
    }

    fn sys_memory_usage(system_infos: &mut System) -> f64 {
        system_infos.refresh_memory();

        let total_memory = system_infos.total_memory() as f64;
        if total_memory == 0.0 {
            error!("Failed to get total system memory");
            return 100.0; // Assume worst case
        }
        
        let used_memory = system_infos.used_memory() as f64;
        (used_memory / total_memory) * 100.0
    }

    /// Return memory usage of process in bytes
    fn process_memory_usage(sys: &mut System) -> u64 {
        sys.refresh_processes(ProcessesToUpdate::All, true);

        let pid = sysinfo::get_current_pid().expect("Failed to get current PID");

        if let Some(process) = sys.process(pid) {
            process.memory()
        } else {
            0
        }
    }

    async fn event_trigger_interval(
        self: Arc<Self>,
        tx: mpsc::Sender<OuterMsg>,
        rx_open: watch::Receiver<bool>,
    ) {
        if self.config.bootnodes.is_none() {
            return;
        }

        if self.config.enable_tx_send {
            let target_interval = Duration::from_micros(self.config.trigger_us);
            let min_interval = Duration::from_millis(1000);
            let mut current_interval = target_interval;

            let node_id = self.node_id;
            let pub_key = self.local_key.public().encode_protobuf();
            let topic = Sha256Topic::new(self.config.topic.clone());
            let payload_data =
                Server::generate_data_payload(self.config.payload_bytes_len as usize);

            let mut interval = time::interval(current_interval);
            loop {
                interval.tick().await; // control tick time

                if !*rx_open.borrow() {
                    if current_interval < min_interval {
                        current_interval = current_interval * 4;
                        if current_interval > min_interval {
                            current_interval = min_interval;
                        }
                        interval = time::interval(current_interval);
                        info!("Event trigger task start exponential damping due to high memory usage.");
                    }
                }

                let self_clone = self.clone();
                self_clone
                    .event_trigger_task(node_id, &payload_data, &pub_key, &topic, &tx)
                    .await;

                if *rx_open.borrow() && current_interval > target_interval {
                    current_interval = target_interval;
                    interval = time::interval(current_interval);
                }
            }
        }
    }

    async fn event_trigger_task(
        self: Arc<Self>,
        node_id: u64,
        payload_data: &String,
        pub_key: &Vec<u8>,
        topic: &Sha256Topic,
        tx: &mpsc::Sender<OuterMsg>,
    ) -> bool {
        // Update state and prepare message
        let (event, message, message_len, send_counts) = match self
            .clone()
            .update_prepare_msg(node_id, payload_data, pub_key)
            .await
        {
            Some(value) => value,
            None => return false,
        };

        // send message with vlc
        if send_counts % 100 == 0 {
            debug!("Publish event msg: {:?}", event);
        }
        let value = OuterMsg {
            data: message,
            topic: topic.clone(),
        };
        if timeout(Duration::from_secs(NET_TIMEOUT), tx.send(value))
            .await
            .is_err()
        {
            error!("Network task has been closed, or publish to network timeout. Retrying in 5 second...");
            sleep(Duration::from_secs(5)).await;
            return false;
        }

        // Increment send count
        {
            let mut state = self.state.write().await;
            state.metrics.send_count += 1;
            state.metrics.total_send_bytes += message_len as u64;
        }
        true
    }

    async fn update_prepare_msg(
        self: Arc<Self>,
        node_id: u64,
        payload_data: &String,
        pub_key: &Vec<u8>,
    ) -> Option<(P2PMsg, Vec<u8>, usize, u64)> {
        let (event, message, message_len, send_counts) = {
            // must sync update
            let mut state = self.state.write().await;
            let send_counts = state.metrics.send_count;
            state.clock = state.clock.update([].iter(), node_id);
            state.metrics.clock_update_count = state.metrics.clock_update_count + 1;
            let mut event = P2PMsg {
                clock: state.clock.clone(),
                data: payload_data.clone().into(),
                pub_key: None,
                signature: None,
            };

            // Serialize and sign the event
            let serialized_event = match bincode::options().serialize(&event) {
                Ok(vec) => vec,
                Err(e) => {
                    error!("Failed to serialize event: {}", e);
                    return None;
                }
            };

            let signature = match self.local_key.sign(&serialized_event) {
                Ok(sig) => sig,
                Err(e) => {
                    error!("Failed to sign event: {}", e);
                    return None;
                }
            };

            event.signature = Some(signature);
            event.pub_key = Some(pub_key.clone());
            let message = match bincode::options().serialize(&event) {
                Ok(msg) => msg,
                Err(e) => {
                    error!("Failed to serialize signed event: {}", e);
                    return None;
                }
            };
            let message_len = message.len();
            (event, message, message_len, send_counts)
        };
        Some((event, message, message_len, send_counts))
    }

    async fn event_handler(
        self: Arc<Self>,
        mut rx_net: mpsc::Receiver<Event>,
        tx_async_state: mpsc::Sender<StateUpdate>,
    ) {
        if self.config.bootnodes.is_none() {
            info!("bootnode don't parse events");
            return;
        }
        let semaphore = Arc::new(Semaphore::new(self.config.concurrent_verify as usize));

        // Event loop to handle incoming messages
        loop {
            let event = match rx_net.recv().await {
                Some(ev) => ev,
                None => break, // Receiver has been closed
            };

            match event {
                Event::PeerConnected(peer_id) => {
                    debug!("connected to {:?}", peer_id);
                }
                Event::PeerSubscribed(peer_id, topic_hash) => {
                    debug!("Remote peer {:?} subscribed to {:?}", peer_id, topic_hash);
                }
                Event::Message(msg) => {
                    if self.config.concurrent_verify <= 1 {
                        self.clone()
                            .handle_p2p_msg(msg, tx_async_state.clone(), None)
                            .await;
                    } else {
                        let self_clone = self.clone();
                        let tx_state_clone = tx_async_state.clone();
                        let semaphore_clone = semaphore.clone();
                        if let Ok(permit) = semaphore_clone.acquire_owned().await {
                            tokio::spawn(async move {
                                self_clone
                                    .handle_p2p_msg(msg, tx_state_clone, Some(permit))
                                    .await;
                            });
                        } else {
                            error!("Semaphore has been closed");
                        }
                    }
                }
                _ => {}
            }
        }
    }

    async fn handle_p2p_msg(
        self: Arc<Self>,
        msg: libp2p::gossipsub::Message,
        tx_async_state: mpsc::Sender<StateUpdate>,
        _permit: Option<OwnedSemaphorePermit>,
    ) -> bool {
        // Deserialize and verify
        let msg_len = msg.data.len();
        let received_event: P2PMsg = match bincode::options().deserialize::<P2PMsg>(&msg.data) {
            Ok(event) => event,
            Err(e) => {
                error!("Failed to deserialize received message: {}", e);
                return false;
            }
        };
        debug!("Recv message: {:?}", received_event);
        let received_clock = received_event.clock;
        let received_signature = match received_event.signature {
            Some(sig) => sig,
            None => {
                warn!("Received event without a signature.");
                return false;
            }
        };

        let serialized_event = match bincode::options().serialize(&P2PMsg {
            clock: received_clock.clone(),
            data: received_event.data.clone(),
            pub_key: None,
            signature: None,
        }) {
            Ok(vec) => vec,
            Err(e) => {
                error!("Failed to serialize event for verification: {}", e);
                return false;
            }
        };

        // Verify signature
        if received_event.pub_key.is_some() {
            let pubkey = match PublicKey::try_decode_protobuf(&received_event.pub_key.unwrap()) {
                Ok(pk) => pk,
                Err(e) => {
                    error!("Failed to decode public key: {}", e);
                    return false;
                }
            };
            if pubkey.verify(&serialized_event, &received_signature) {
                {
                    if let Err(err) = tx_async_state
                        .send(StateUpdate::IncrementReceiveCount(
                            msg_len,
                            VLCUpdate {
                                others: vec![received_clock],
                                id: self.node_id,
                            },
                        ))
                        .await
                    {
                        error!("Update state send fail, err: {:?}", err);
                    }
                }
            } else {
                warn!("signature validate failed.")
            }
        } else {
            warn!("lack pubkey to verify.")
        }
        true
    }

    async fn start_tps_monitoring(self: Arc<Self>) {
        if self.config.bootnodes.is_none() {
            return;
        }

        // default tps time window uses 1s
        let time_window = self.config.time_window_s;
        let mut interval = time::interval(Duration::from_secs(time_window));
        let mut prev_metrics = ServerMetric::default();

        loop {
            interval.tick().await;

            let state_read = self.state.read().await;
            let current_metrics = &state_read.metrics;
            let node_id = &self.node_id;

            let send_tps = current_metrics.send_count - prev_metrics.send_count;
            let receive_tps = current_metrics.receive_count - prev_metrics.receive_count;
            let clock_update_tps =
                current_metrics.clock_update_count - prev_metrics.clock_update_count;
            let send_bytes_tps = current_metrics.total_send_bytes - prev_metrics.total_send_bytes;
            let recv_bytes_tps = current_metrics.total_recv_bytes - prev_metrics.total_recv_bytes;

            prev_metrics = current_metrics.clone();

            info!("Id {}, Send TPS: {}", node_id, send_tps / time_window);
            info!("Id {}, Receive TPS: {}", node_id, receive_tps / time_window);
            info!(
                "Id {}, Clock Update TPS: {}",
                node_id,
                clock_update_tps / time_window
            );
            info!(
                "Id {}, Total Send Bytes: {}",
                node_id,
                send_bytes_tps / time_window
            );
            info!(
                "Id {}, Total Receive Bytes: {}",
                node_id,
                recv_bytes_tps / time_window
            );
            if self.config.print_vlc {
                info!("Id {}, Current vlc value: {:?}", node_id, state_read.clock);
            }
        }
    }

    async fn async_state_update(
        state: Arc<RwLock<ServerState>>,
        mut rx: mpsc::Receiver<StateUpdate>,
    ) {
        while let Some(update) = rx.recv().await {
            let mut state = state.write().await;
            match update {
                StateUpdate::IncrementSendCount(message_len) => {
                    state.metrics.send_count += 1;
                    state.metrics.total_send_bytes += message_len as u64;
                }
                StateUpdate::IncrementReceiveCount(msg_len, vlc_update) => {
                    state.metrics.receive_count += 1;
                    state.metrics.total_recv_bytes += msg_len as u64;
                    state.metrics.clock_update_count += 1;
                    state.clock = state.clock.update(vlc_update.others.iter(), vlc_update.id);
                }
                StateUpdate::UpdateClock(vlc_update) => {
                    state.clock = state.clock.update(vlc_update.others.iter(), vlc_update.id);
                    state.metrics.clock_update_count += 1;
                }
                StateUpdate::Exit(_) => break,
            }
        }
    }

    async fn network_task(
        mut network: Network,
        mut rx_publish: mpsc::Receiver<OuterMsg>,
        tx_net: mpsc::Sender<Event>,
        tx_sender_open: watch::Sender<bool>,
        mem: MemoryWatcherConfig,
    ) {
        let mut check_interval = time::interval(Duration::from_secs(2));
        let mut system_infos = System::new();
        loop {
            tokio::select! {
                Some(outer_msg) = rx_publish.recv() => {
                    let _ = network.publish(outer_msg.data, outer_msg.topic).await;
                },
                Some(event) = network.next() => {
                    if timeout(Duration::from_secs(NET_TIMEOUT), tx_net.send(event)).await.is_err(){
                            error!("send to recv handler timeout, event_handler busy and drop event");
                        };
                },
                _ = check_interval.tick() => {
                    info!("Connected node num: {}, addr: {:?}", network.peer_nums(), network.multiaddr);
                    // first check process resources
                    let now_proc_memory_bytes = Server::process_memory_usage(&mut system_infos);
                    if now_proc_memory_bytes > Server::mb_to_bytes(mem.proc_max_mem_mb) {
                        info!("Process memory exceeded maximum usage {:?} MB, current {:?} Bytes, auto disconnect", mem.proc_max_mem_mb, now_proc_memory_bytes);
                        let _ = tx_sender_open.send(false); // silent send
                        network.disconnect_by_traffic();
                        continue;
                    }
                    // second check system resources
                    let sys_memory_usage_percentage = Server::sys_memory_usage(&mut system_infos);
                    if sys_memory_usage_percentage > mem.sys_disconnect_mem {
                        info!("System memory usage exceeded {:?}, current usage {:?}, auto disconnect", mem.sys_disconnect_mem, sys_memory_usage_percentage);
                        network.disconnect_by_traffic();
                    }
                    if ! mem.is_bootnode && mem.is_sender {
                        if sys_memory_usage_percentage > mem.sys_max_mem_usage {
                            info!("System memory usage exceeded {:?}, current usage {:?}, pausing event trigger task...", mem.sys_max_mem_usage, sys_memory_usage_percentage);
                            if let Err(err) = tx_sender_open.send(false) {
                                error!("tx_sender_open send error: {:?}", err);
                            }
                        } else if sys_memory_usage_percentage > mem.sys_min_mem_usage {
                            info!("System memory current usage {:?}", sys_memory_usage_percentage);
                        } else if sys_memory_usage_percentage < mem.sys_min_mem_usage {
                            info!("System memory usage below {:?}, current usage {:?}, proceeding with computation...", mem.sys_min_mem_usage, sys_memory_usage_percentage);
                            network.recover_connecting();
                            if let Err(err) = tx_sender_open.send(true) {
                                error!("tx_sender_open send error: {:?}", err);
                            }
                        }
                    }
                },
            }
        }
    }
}

pub type ServerArc = Arc<Server>;

/// A cache state of a server node.
#[derive(Debug, Clone, Default)]
pub struct ServerState {
    pub clock: OrdinaryClock,
    pub metrics: ServerMetric,
    pub message_ids: VecDeque<String>,
    pub cache_items: BTreeMap<String, String>,
    pub cache_maximum: u64,
}

#[derive(Debug, Clone, Default)]
pub struct ServerMetric {
    pub send_count: u64,
    pub receive_count: u64,
    pub clock_update_count: u64,
    pub total_send_bytes: u64,
    pub total_recv_bytes: u64,
}

impl ServerState {
    /// Create a new server state.
    pub fn new(cache_maximum: u64) -> Self {
        Self {
            clock: OrdinaryClock::default(),
            message_ids: VecDeque::new(),
            cache_items: BTreeMap::new(),
            cache_maximum,
            metrics: ServerMetric::default(),
        }
    }
}
pub fn generate_config(
    host: String,
    port: u16,
    topic: String,
    bootnodes: Option<Vec<String>>,
    discover: String,
    max_discover_node: u32,
    enable_tx_send: bool,
    concurrent_verify: u64,
    trigger_us: u64,
    max_sys_memory_percent: u64,
    max_proc_memory_mb: u64,
    disconnect_rate: f64,
    time_window_s: u64,
    init_clock_keys: u32,
    payload_bytes_len: u64,
    print_vlc: bool,
    tokio_console: bool,
    tokio_console_port: u16,
    pprof: bool,
    output: &str,
) {
    let discover = Discover::from_str(&discover).unwrap();
    let local_key = identity::Keypair::generate_ed25519();
    let local_peer_id = PeerId::from(local_key.public());
    let private_key = general_purpose::STANDARD.encode(local_key.to_protobuf_encoding().unwrap());
    let multi_addr = format!(
        "{}{}{}",
        host,
        port,
        multiaddr::Protocol::P2p(local_peer_id)
    );
    let concurrent_verify = if concurrent_verify == 0 {
        num_cpus::get() as u64
    } else {
        concurrent_verify
    };
    let min_sys_memory_percent = if max_sys_memory_percent > 5 {
        max_sys_memory_percent - 2
    } else {
        max_sys_memory_percent
    };
    let config = ServerConfig {
        host,
        port,
        topic,
        private_key,
        multi_addr,
        bootnodes,
        discover,
        max_discover_node,
        enable_tx_send,
        concurrent_verify,
        trigger_us,
        max_sys_memory_percent,
        min_sys_memory_percent,
        max_proc_memory_mb,
        disconnect_rate,
        time_window_s,
        init_clock_keys,
        payload_bytes_len,
        print_vlc,
        tokio_console,
        tokio_console_port,
        pprof,
    };

    let config_json = serde_json::to_string_pretty(&config).unwrap();
    let mut file = File::create(output).unwrap();
    file.write_all(config_json.as_bytes()).unwrap();
}

pub async fn run(config: Arc<ServerConfig>) -> anyhow::Result<(), anyhow::Error> {
    let local_key = if config.private_key.is_empty() {
        identity::Keypair::generate_ed25519()
    } else {
        let decoded_key = general_purpose::STANDARD
            .decode(config.private_key.clone())
            .map_err(|e| anyhow!("Failed to decode private key: {}", e))?;
        identity::Keypair::from_protobuf_encoding(&decoded_key)
            .map_err(|e| anyhow!("Failed to create keypair from decoded key: {}", e))?
    };
    let local_peer_id = PeerId::from(local_key.public());
    let node_id = Server::identity_to_u64(&local_peer_id.to_base58());
    if node_id.is_none() {
        return Err(anyhow!("peer_id string is not base58 format!"));
    }
    let topic = Sha256Topic::new(config.topic.clone());
    let listen_addr = format!(
        "{}{}{}",
        config.host,
        config.port,
        multiaddr::Protocol::P2p(local_peer_id)
    );
    info!(
        "node_id={:?}, topic={}, listen_addr={}",
        node_id,
        config.topic.clone(),
        listen_addr
    );
    let mut swarm = Network::new(
        local_key.clone(),
        config.bootnodes.clone(),
        listen_addr,
        config.discover.clone(),
        config.max_discover_node,
        config.disconnect_rate,
    )
    .map_err(|err| anyhow!(err.to_string()))?;
    let is_bootnode = config.bootnodes.is_none();
    // not subscribe topic only when bootnode & DHT
    if !is_bootnode || config.discover != Discover::DHT {
        swarm.subscribe(&topic);
    }

    let mem_conf = MemoryWatcherConfig {
        is_bootnode,
        is_sender: config.enable_tx_send,
        proc_max_mem_mb: config.max_proc_memory_mb,
        sys_max_mem_usage: config.max_sys_memory_percent as f64,
        sys_min_mem_usage: config.min_sys_memory_percent as f64,
        sys_disconnect_mem: 95.0,
    };
    let mut state = ServerState::new(500);
    state.clock = OrdinaryClock((0..config.init_clock_keys).map(|i| (i as _, 0)).collect());
    let state_arc = Arc::new(RwLock::new(state));
    let server_arc = Arc::new(Server {
        config,
        node_id: node_id.unwrap(),
        peer_id: local_peer_id,
        local_key,
        state: state_arc.clone(),
    });

    let (tx_publish, rx_publish) = mpsc::channel(1000);
    let (tx_sender_open, rx_sender_open) = watch::channel(true);
    let (tx_net, rx_net) = mpsc::channel(1000);
    let (tx_async_state, rx_async_state) = mpsc::channel(1000);

    // Periodic message sender
    let message_sender = tokio::spawn(
        server_arc
            .clone()
            .event_trigger_interval(tx_publish, rx_sender_open),
    );
    let event_handler = tokio::spawn(server_arc.clone().event_handler(rx_net, tx_async_state));
    let tps_monitoring = tokio::spawn(server_arc.clone().start_tps_monitoring());
    let state_mgr = tokio::spawn(Server::async_state_update(state_arc, rx_async_state));
    let network_task = tokio::spawn(Server::network_task(
        swarm,
        rx_publish,
        tx_net,
        tx_sender_open,
        mem_conf,
    ));

    // Wait for all tasks to complete
    let _ = tokio::join!(
        event_handler,
        message_sender,
        tps_monitoring,
        network_task,
        state_mgr
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    #[test]
    fn test_verify_signature() {
        let local_key = identity::Keypair::generate_ed25519();
        let pub_key_bytes = local_key.public().encode_protobuf();
        let peer_id = PeerId::from(local_key.public());
        let node_id = Server::identity_to_u64(&peer_id.to_base58()).unwrap();
        let clock = OrdinaryClock::default();
        clock.update(vec![].iter(), node_id);
        let event = P2PMsg {
            clock,
            data: "data".into(),
            pub_key: None,
            signature: None,
        };

        // Serialize and sign the event
        let serialized_event = bincode::options().serialize(&event).unwrap();
        let signature = local_key.sign(&serialized_event);
        let pubkey = PublicKey::try_decode_protobuf(&pub_key_bytes).unwrap();
        assert_eq!(true, pubkey.verify(&serialized_event, &signature.unwrap()));
    }

    #[test]
    fn test_stress_ser_unser() {
        let local_key = identity::Keypair::generate_ed25519();
        let pub_key_bytes = local_key.public().encode_protobuf();
        let peer_id = PeerId::from(local_key.public());
        let node_id = Server::identity_to_u64(&peer_id.to_base58()).unwrap();
        let clock = OrdinaryClock::default();
        clock.update(vec![].iter(), node_id);
        let mut event_net = P2PMsg {
            clock: clock.clone(),
            data: _DEFAULT_PAYLOAD.into(),
            pub_key: None,
            signature: None,
        };
        let event_raw = event_net.clone();

        let duration = Duration::from_secs(1);

        // Serialize and sign the event
        let start = Instant::now();
        let mut serialize_count = 0;

        while start.elapsed() < duration {
            let serialized_event = bincode::serialize(&event_net).unwrap();
            let signature = local_key.sign(&serialized_event);
            event_net.signature = Some(signature.unwrap());
            let _ = bincode::serialize(&event_net).unwrap();
            serialize_count += 1;
        }
        event_net.pub_key = Some(pub_key_bytes.clone());
        println!(
            "Serialization and sign performed: {} times in 1 second",
            serialize_count
        );

        // Measure deserialization repetitions in 1 second
        let serialized_raw = bincode::serialize(&event_raw).unwrap();
        let signature = local_key.sign(&serialized_raw).unwrap();
        let serialized_net = bincode::serialize(&event_net).unwrap();
        let start = Instant::now();
        let mut deserialize_count = 0;
        while start.elapsed() < duration {
            let pubkey = PublicKey::try_decode_protobuf(&pub_key_bytes).unwrap();
            let _deserialized: P2PMsg = bincode::deserialize(&serialized_net).unwrap();
            let serialized_event = bincode::serialize(&event_raw).unwrap();
            assert_eq!(true, pubkey.verify(&serialized_event, &signature));
            deserialize_count += 1;
        }
        println!(
            "Deserialization performed: {} times in 1 second",
            deserialize_count
        );
    }

    pub async fn make_test_server(
        listen_addr: &str,
        boot_nodes: Vec<String>,
        topic_str: &str,
    ) -> anyhow::Result<(Server, Network, Sha256Topic)> {
        let local_key = identity::Keypair::generate_ed25519();
        let peer_id = PeerId::from(local_key.public());
        let node_id = Server::identity_to_u64(&peer_id.to_base58()).unwrap();
        let mut config = ServerConfig::default();
        config.bootnodes = Some(boot_nodes);
        let config = Arc::new(config);
        let state = Arc::new(RwLock::new(ServerState::new(500)));
        let server = Server {
            local_key,
            state,
            config,
            node_id,
            peer_id,
        };

        let topic = Sha256Topic::new(topic_str);
        let mut swarm = Network::new(
            server.local_key.clone(),
            server.config.bootnodes.clone(),
            listen_addr.to_string(),
            Discover::Config,
            50,
            1.0,
        )
        .map_err(|err| anyhow!(err.to_string()))?;
        swarm.subscribe(&topic);
        return Ok((server, swarm, topic));
    }

    #[tokio::test]
    async fn test_press_trigger_event() -> anyhow::Result<()> {
        let listen_addr = "/ip4/127.0.0.1/tcp/9601";
        let topic_str = "test";

        let (server, mut swarm, topic) = make_test_server(&listen_addr, vec![], &topic_str)
            .await
            .unwrap();
        let server_arc = Arc::new(server);
        let (tx_publish, mut rx_publish) = mpsc::channel::<OuterMsg>(100);
        let (tx_end, mut rx_end) = mpsc::channel(1);

        let task_net = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(outer_msg) = rx_publish.recv() => {
                        let _ = swarm.publish(outer_msg.data, outer_msg.topic).await;
                    },
                    Some(_) = rx_end.recv() => {
                        break;
                    }
                }
            }
        });

        let node_id = server_arc.node_id;
        let pub_key = server_arc.local_key.public().encode_protobuf();
        let payload_data =
            Server::generate_data_payload(server_arc.config.payload_bytes_len as usize);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(1) {
            server_arc
                .clone()
                .event_trigger_task(node_id, &payload_data, &pub_key, &topic, &tx_publish)
                .await;
        }
        let _ = tx_end.send(()).await;
        print_metric(server_arc.state.clone(), 1).await;
        let _ = tokio::join!(task_net);
        Ok(())
    }

    async fn print_metric(state: Arc<RwLock<ServerState>>, time_windows_s: u64) {
        let state = state.read().await;
        println!(
            "Receive TPS: {}",
            state.metrics.receive_count / time_windows_s
        );
        println!(
            "Clock Update TPS: {}",
            state.metrics.clock_update_count / time_windows_s
        );
        println!(
            "Total Receive Bytes: {}",
            state.metrics.total_recv_bytes / time_windows_s
        );
        println!(
            "Executed {} times in 1 second",
            state.metrics.receive_count / time_windows_s
        );
    }

    #[ignore]
    #[tokio::test]
    async fn test_press_recv_event() -> anyhow::Result<()> {
        let listen_addr = "/ip4/127.0.0.1/tcp/9602";
        let topic_str = "vlc";
        // Need start node1 sender listen on /ip4/127.0.0.1/tcp/9601,
        let boot_nodes = vec!["/ip4/127.0.0.1/tcp/9601".to_string()];
        let time_windows_s = 1;
        let (server, mut swarm, _topic) = make_test_server(&listen_addr, boot_nodes, &topic_str)
            .await
            .unwrap();
        let server_arc = Arc::new(server);
        let (tx_net, mut rx_net) = mpsc::channel::<Event>(100);
        let (tx_state, rx_state) = mpsc::channel(100);
        let (tx_end, mut rx_end) = mpsc::channel(1);

        let task_net = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(event) = swarm.next() => {
                        let _ = tx_net.send(event).await;
                    },
                    Some(_) = rx_end.recv() => {
                        break;
                    }
                }
            }
        });
        let task_state_update = tokio::spawn(Server::async_state_update(
            server_arc.state.clone(),
            rx_state,
        ));

        let semaphore = Arc::new(Semaphore::new(3));
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(time_windows_s) {
            let event = match rx_net.recv().await {
                Some(ev) => ev,
                None => break, // Receiver has been closed
            };

            match event {
                Event::PeerConnected(peer_id) => {
                    println!("connected to {:?}", peer_id);
                }
                Event::Message(msg) => {
                    let semaphore_clone = semaphore.clone();
                    let permit = semaphore_clone.acquire_owned().await.unwrap();
                    server_arc
                        .clone()
                        .handle_p2p_msg(msg, tx_state.clone(), Some(permit))
                        .await;
                }
                _ => {}
            }
        }
        let _ = tx_end.send(()).await;
        let _ = tx_state.send(StateUpdate::Exit(())).await;
        // Wait for tasks to complete
        if let Err(e) = task_net.await {
            eprintln!("Error in task_net: {:?}", e);
        }
        if let Err(e) = task_state_update.await {
            eprintln!("Error in task_state_update: {:?}", e);
        }
        print_metric(server_arc.state.clone(), time_windows_s).await;
        Ok(())
    }
}
