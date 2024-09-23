/// Gossip implements a basic gossip network using libp2p.
/// It currently supports discovery via mdns and bootnodes, dht.
use ahash::HashSetExt;
use color_eyre::eyre::{eyre, Result, WrapErr};
use crypto::core::DigestHash;
use futures::StreamExt;
use libp2p::kad::{BootstrapResult, QueryId};
use libp2p::{
    connection_limits,
    allow_block_list::{self, BlockedPeers},
    core::upgrade::Version,
    gossipsub::{self, Message, MessageId, TopicHash},
    identity::{self, Keypair},
    noise, ping,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    tcp, yamux, Multiaddr, PeerId, Transport,
};
use serde::{Deserialize, Serialize};
use std::{
    pin::Pin,
    str::FromStr,
    task::{Context, Poll},
    time::Duration,
};
use tracing::{debug, info, trace, warn};

pub use libp2p::gossipsub::Sha256Topic;

use crate::discovery::{DiscoveryBehaviour, DiscoveryConfig};
use crate::peer_manager::PeerManager;

pub const LISTEN_ADDR: &str = "/ip4/0.0.0.0/tcp/9600";
// Configures the maximum number of concurrent established connections per peer, regardless of direction (incoming or outgoing).
const MAX_ESTABLISHED_PER_PEER: u32 = 4;

#[derive(NetworkBehaviour)]
pub struct GossipnetBehaviour {
    // Behaviours that manage connections should come first, to get rid of some panics in debug build.
    // See <https://github.com/libp2p/rust-libp2p/issues/4773#issuecomment-2042676966>
    connection_limits: connection_limits::Behaviour,
    blocked_peers: allow_block_list::Behaviour<BlockedPeers>,
    ping: ping::Behaviour,
    gossipsub: gossipsub::Behaviour,
    pub discovery: DiscoveryBehaviour,
}

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub enum Discover {
    Config,
    MDns,
    DHT,
}

impl FromStr for Discover {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "config" => Ok(Discover::Config),
            "mdns" => Ok(Discover::MDns),
            "dht" => Ok(Discover::DHT),
            _ => Err(format!("Invalid value for Discover: {}", s)),
        }
    }
}

impl Default for Discover {
    fn default() -> Self {
        Discover::DHT
    }
}

pub struct NetworkBuilder {
    local_key: Keypair,
    bootnodes: Option<Vec<String>>,
    listen_addr: String,
}

impl NetworkBuilder {
    pub fn new(local_key: Keypair) -> Self {
        Self {
            local_key,
            bootnodes: None,
            listen_addr: LISTEN_ADDR.to_string(),
        }
    }

    pub fn bootnodes(mut self, bootnodes: Vec<String>) -> Self {
        self.bootnodes = Some(bootnodes);
        self
    }

    pub fn listen_addr(mut self, listen_addr: String) -> Self {
        self.listen_addr = listen_addr;
        self
    }

    pub fn build(self) -> Result<Network> {
        Network::new(
            self.local_key,
            self.bootnodes,
            self.listen_addr,
            Discover::Config,
            50,
            1.0,
        )
    }
}

impl Default for NetworkBuilder {
    fn default() -> Self {
        Self::new(identity::Keypair::generate_ed25519())
    }
}

pub struct Network {
    pub local_key: Keypair,
    pub multiaddr: Multiaddr,
    swarm: Swarm<GossipnetBehaviour>,
    peer_manager: PeerManager,
    _discover: Discover,
    terminated: bool,
}

impl Network {
    pub fn new(
        local_key: Keypair,
        bootnodes: Option<Vec<String>>,
        listen_addr: String,
        discover: Discover,
        max_discover_node: u32,
        disconnect_rate: f64,
    ) -> Result<Self> {
        let local_peer_id = PeerId::from(local_key.public());

        let transport = tcp::tokio::Transport::default()
            .upgrade(Version::V1Lazy)
            .authenticate(noise::Config::new(&local_key)?)
            .multiplex(yamux::Config::default())
            .boxed();

        let gossipsub = Network::new_gossip(&local_key)?;
        let swarm_config = libp2p::swarm::Config::with_tokio_executor()
            .with_idle_connection_timeout(Duration::from_secs(300));

        let bootlist: Vec<Multiaddr> = if let Some(addrs) = bootnodes {
            addrs
                .into_iter()
                .filter_map(|addr| match Multiaddr::from_str(&addr) {
                    Ok(remote) => Some(remote),
                    Err(e) => {
                        eprintln!("Failed to parse address: {}, error: {:?}", addr, e);
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        let (enable_mdns, enable_kad) = match discover {
            Discover::MDns => (true, false),
            Discover::DHT => (false, true),
            _ => (false, false),
        };
        let discovery = DiscoveryConfig::new(local_key.public(), "hetu")
            .with_mdns(enable_mdns)
            .with_kademlia(enable_kad)
            .with_user_defined(bootlist)
            .map_err(|e| eyre!("bootnode format error: {}", e))?
            .target_peer_count(max_discover_node as u64)
            .finish()
            .map_err(|e| eyre!("bootstrap error: {}", e))?;

        let connection_limits = connection_limits::Behaviour::new(
            connection_limits::ConnectionLimits::default()
                .with_max_pending_incoming(Some(
                    max_discover_node.saturating_mul(MAX_ESTABLISHED_PER_PEER),
                ))
                .with_max_pending_outgoing(Some(
                    max_discover_node.saturating_mul(MAX_ESTABLISHED_PER_PEER),
                ))
                .with_max_established_incoming(Some(max_discover_node))
                .with_max_established_outgoing(Some(max_discover_node))
                .with_max_established_per_peer(Some(MAX_ESTABLISHED_PER_PEER)),
        );

        let mut swarm = {
            let behaviour = GossipnetBehaviour {
                connection_limits,
                blocked_peers: allow_block_list::Behaviour::default(),
                gossipsub,
                ping: ping::Behaviour::default(),
                discovery,
            };
            Swarm::new(transport, behaviour, local_peer_id, swarm_config)
        };

        swarm.listen_on(listen_addr.parse()?)?;

        // Bootstrap with Kademlia or config dial
        if let Err(e) = swarm.behaviour_mut().discovery.bootstrap() {
            warn!("Failed to bootstrap with Kademlia: {e}");
        }

        let multiaddr = Multiaddr::from_str(&listen_addr)?;
        let peer_manager = PeerManager::new(disconnect_rate);
        Ok(Network {
            local_key,
            multiaddr,
            swarm,
            peer_manager,
            _discover: discover,
            terminated: false,
        })
    }

    fn new_gossip(local_key: &Keypair) -> Result<gossipsub::Behaviour, color_eyre::eyre::Error> {
        let message_id_fn = |message: &gossipsub::Message| {
            use DigestHash as _;
            let hash = message.data.blake2();
            gossipsub::MessageId::from(hash.as_bytes())
        };
        let gossipsub_config = gossipsub::ConfigBuilder::default()
            .heartbeat_interval(Duration::from_secs(1))
            .duplicate_cache_time(Duration::from_secs(10))
            .max_transmit_size(1024 * 1024) // 1 MB
            .history_length(2)
            .history_gossip(1)
            .gossip_lazy(5) // Minimum number of peers to gossip to
            .gossip_factor(0.25) // Fraction of peers to gossip to
            .gossip_retransimission(3) // Number of times to retransmit messages
            .validation_mode(gossipsub::ValidationMode::Strict) // the default is Strict (enforce message signing)
            .message_id_fn(message_id_fn) // content-address messages so that duplicates aren't propagated
            // .do_px() // not implemented by rust-libp2p
            .build()
            .map_err(|e| eyre!("failed to build gossipsub config: {}", e))?;
        let gossipsub = gossipsub::Behaviour::new(
            gossipsub::MessageAuthenticity::Signed(local_key.clone()),
            gossipsub_config,
        )
        .map_err(|e| eyre!("failed to create gossipsub behaviour: {}", e))?;
        Ok(gossipsub)
    }

    pub fn restart_gossip(&mut self) -> Result<(), color_eyre::eyre::Error> {
        self.swarm.behaviour_mut().gossipsub = Network::new_gossip(&self.local_key)?;
        Ok(())
    }

    pub async fn publish(&mut self, message: Vec<u8>, topic: Sha256Topic) -> Result<MessageId> {
        self.swarm
            .behaviour_mut()
            .gossipsub
            .publish(topic, message)
            .wrap_err("failed to publish message")
    }

    pub fn subscribe(&mut self, topic: &Sha256Topic) {
        self.swarm
            .behaviour_mut()
            .gossipsub
            .subscribe(topic)
            .unwrap();
    }

    pub fn unsubscribe(&mut self, topic: &Sha256Topic) {
        self.swarm
            .behaviour_mut()
            .gossipsub
            .unsubscribe(topic)
            .unwrap();
    }

    /// Bootstrap Kademlia network
    pub fn bootstrap(&mut self) -> Result<QueryId, String> {
        self.swarm
            .behaviour_mut()
            .discovery
            .bootstrap()
            .map_err(|e| e.to_string())
    }

    /// Get peer node numbers
    pub fn peer_nums(&mut self) -> usize {
        self.swarm.behaviour_mut().discovery.peers().len()
    }

    /// Get all_mesh_peers
    pub fn all_mesh_peers(&mut self) -> usize {
        self.swarm
            .behaviour_mut()
            .gossipsub
            .all_mesh_peers()
            .count()
    }

    /// disconnect by traffic & left bootnode
    pub fn disconnect_by_traffic(&mut self) {
        let conn_peers = &self.swarm.behaviour().discovery.peers;
        let mut peers = self.peer_manager.traffic.top_traffic_peers();
        self.peer_manager.traffic.clear_peer_traffic();
        peers.retain(|id| conn_peers.contains(id));
        let custom_seed_peers = self.swarm.behaviour().discovery.seed_peers().clone();

        for peer_id in &peers {
            let is_seed_peer = custom_seed_peers
                .iter()
                .any(|(seed_peer_id, _)| seed_peer_id == peer_id);

            if !is_seed_peer {
                let addr = self
                    .swarm
                    .behaviour()
                    .discovery
                    .peer_info(peer_id)
                    .map_or(ahash::HashSet::new(), |info| info.addresses.clone());
                if ! addr.is_empty() {
                    info!("Disconnecting & add blocked list due to high traffic, Multiaddr: {:?}", addr);
                    self.swarm.behaviour_mut().discovery.remove_peer(peer_id);
                    self.peer_manager.peer_ban_list.insert(*peer_id, None);
                    self.swarm.behaviour_mut().blocked_peers.block_peer(*peer_id);
                    // let _ = self.swarm.disconnect_peer_id(*peer_id);
                }
            }
        }
    }

    pub fn recover_connecting(&mut self) {
        for (peer_id, _expire) in &self.peer_manager.peer_ban_list {
            info!("Recover PeerId: {:?} and delete from blocked list", peer_id);
            self.swarm.behaviour_mut().blocked_peers.unblock_peer(*peer_id);
        }
        self.peer_manager.peer_ban_list.clear();
    }
}

#[derive(Debug)]
pub enum Event {
    NewListenAddr(Multiaddr),
    Message(Message),
    MdnsPeersConnected(Vec<PeerId>),
    MdnsPeersDisconnected(Vec<PeerId>),
    PeerConnected(PeerId),
    PeerSubscribed(PeerId, TopicHash),
    Bootstrap(BootstrapResult),
    NewExternalAddrCandidate(Multiaddr),
    ExternalAddrExpired(Multiaddr),
}

impl futures::Stream for Network {
    type Item = Event;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while let Poll::Ready(maybe_event) = self.swarm.poll_next_unpin(cx) {
            let Some(event) = maybe_event else {
                self.terminated = true;
                return Poll::Ready(None);
            };

            match event {
                SwarmEvent::Behaviour(GossipnetBehaviourEvent::Gossipsub(
                    gossipsub::Event::Message {
                        propagation_source: peer_id,
                        message_id: id,
                        message,
                    },
                )) => {
                    debug!(
                        "Got message: '{:?}' with id: {id} from peer: {peer_id}",
                        message.data,
                    );
                    self.peer_manager.traffic.record_traffic(peer_id, message.data.len() as u64);
                    return Poll::Ready(Some(Event::Message(message)));
                }
                SwarmEvent::NewListenAddr { address, .. } => {
                    debug!("Local node is listening on {address}");
                    return Poll::Ready(Some(Event::NewListenAddr(address)));
                }
                SwarmEvent::Behaviour(GossipnetBehaviourEvent::Gossipsub(
                    gossipsub::Event::Subscribed { peer_id, topic },
                )) => {
                    debug!(
                        "Peer {peer_id} subscribed to topic: {topic:?}",
                        peer_id = peer_id,
                        topic = topic,
                    );
                    return Poll::Ready(Some(Event::PeerSubscribed(peer_id, topic)));
                }
                SwarmEvent::ConnectionEstablished {
                    peer_id,
                    endpoint,
                    num_established,
                    concurrent_dial_errors: _,
                    established_in: _,
                    connection_id: _,
                } => {
                    debug!(
                        "Connection with {peer_id} & {endpoint:?} established (total: {num_established})",
                        peer_id = peer_id,
                        num_established = num_established,
                    );
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(&peer_id);
                    return Poll::Ready(Some(Event::PeerConnected(peer_id)));
                }
                _ => {
                    debug!("unhandled swarm event: {:?}", event);
                    trace!("Event type: {:?}", std::any::type_name_of_val(&event));
                }
            }
        }

        Poll::Pending
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use futures::{channel::oneshot, join};
    use tokio::select;

    const TEST_TOPIC: &str = "test";

    #[tokio::test]
    async fn test_gossip_two_nodes_by_dm() {
        let (bootnode_tx, bootnode_rx) = oneshot::channel();
        let (alice_tx, mut alice_rx) = oneshot::channel();

        let msg_a = b"hello world".to_vec();
        let recv_msg_a = msg_a.clone();
        let msg_b = b"i am responding".to_vec();
        let recv_msg_b = msg_b.clone();

        let alice_handle = tokio::task::spawn(async move {
            let topic = Sha256Topic::new(TEST_TOPIC);
            let local_key = identity::Keypair::generate_ed25519();

            let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", 9000);
            let mut alice =
                Network::new(local_key, None, listen_addr, Discover::Config, 50, 1.0).unwrap();
            alice.subscribe(&topic);

            let Some(event) = alice.next().await else {
                panic!("expected stream event");
            };

            match event {
                Event::NewListenAddr(addr) => {
                    println!("Alice listening on {:?}", addr);
                    bootnode_tx.send(addr.clone()).unwrap();
                }
                _ => panic!("unexpected event"),
            };

            loop {
                let Some(event) = alice.next().await else {
                    break;
                };

                match event {
                    Event::PeerConnected(peer_id) => {
                        println!("Alice connected to {:?}", peer_id);
                    }
                    Event::PeerSubscribed(peer_id, topic_hash) => {
                        println!("Remote peer {:?} subscribed to {:?}", peer_id, topic_hash);
                        alice.publish(msg_a.clone(), topic.clone()).await.unwrap();
                    }
                    Event::Message(msg) => {
                        println!("Alice got message: {:?}", msg);
                        assert_eq!(msg.data, recv_msg_b);
                        alice_tx.send(()).unwrap();
                        return;
                    }
                    _ => {}
                }
            }
        });

        let bob_handle = tokio::task::spawn(async move {
            let topic = Sha256Topic::new(TEST_TOPIC);

            let bootnode = bootnode_rx.await.unwrap();
            let local_key = identity::Keypair::generate_ed25519();
            let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", 9001);
            let mut bob = Network::new(
                local_key,
                Some(vec![bootnode.to_string()]),
                listen_addr,
                Discover::Config,
                50,
                1.0,
            )
            .unwrap();
            bob.subscribe(&topic);

            loop {
                select! {
                    event = bob.next() => {
                        let Some(event) = event else {
                            continue;
                        };

                        match event {
                            Event::PeerConnected(peer_id) => {
                                println!("Bob connected to {:?}", peer_id);
                            }
                            Event::Message(msg) => {
                                println!("Bob got message: {:?}", msg);
                                assert_eq!(msg.data, recv_msg_a);
                                bob.publish(msg_b.clone(), topic.clone()).await.unwrap();
                            }
                            _ => {}
                        }
                    }
                    _ = &mut alice_rx => {
                        return;
                    }
                }
            }
        });

        let (res_a, res_b) = join!(alice_handle, bob_handle);
        res_a.unwrap();
        res_b.unwrap();
    }
}
