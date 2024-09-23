use crate::network::GossipnetBehaviour;
use libp2p::{PeerId, Swarm};
use std::{collections::HashMap, time::Instant};
use tokio::time::{interval, Duration};

pub struct PeerManager {
    pub traffic: TrafficMonitor,
    pub peer_ban_list: HashMap<PeerId, Option<Instant>>,
}

impl PeerManager {
    pub fn new(fraction_to_disconnect: f64) -> Self {
        Self {
            traffic: TrafficMonitor::new(fraction_to_disconnect),
            peer_ban_list: HashMap::new(),
        }
    }
}

pub struct TrafficMonitor {
    peer_traffic: HashMap<PeerId, u64>,
    fraction_to_disconnect: f64,
}

impl TrafficMonitor {
    pub fn new(fraction_to_disconnect: f64) -> Self {
        Self {
            peer_traffic: HashMap::new(),
            fraction_to_disconnect,
        }
    }

    // Monitor peers traffic and control whether is need to disconnect.
    async fn _monitor(&mut self, swarm: &mut Swarm<GossipnetBehaviour>) {
        let mut interval = interval(Duration::from_secs(60));

        loop {
            interval.tick().await;

            if self.peer_traffic.is_empty() {
                continue;
            }

            let mut traffic_list: Vec<_> = self.peer_traffic.iter().collect();
            traffic_list.sort_by(|a, b| b.1.cmp(a.1));

            let num_to_disconnect =
                (traffic_list.len() as f64 * self.fraction_to_disconnect) as usize;

            for (peer_id, _) in traffic_list.iter().take(num_to_disconnect) {
                println!("Disconnecting PeerId: {:?} due to high traffic", peer_id);
                Swarm::disconnect_peer_id(swarm, **peer_id).ok();
            }

            self.peer_traffic.clear();
        }
    }

    /// Return slice of ordered peers from the peer manager. Ordering
    /// is based on traffic size of the peer.
    pub fn top_traffic_peers(&mut self) -> Vec<PeerId> {
        let mut result = Vec::new();

        if !self.peer_traffic.is_empty() {
            let mut traffic_list: Vec<_> = self.peer_traffic.iter().collect();
            traffic_list.sort_by(|a, b| b.1.cmp(a.1));

            let num_to_disconnect =
                (traffic_list.len() as f64 * self.fraction_to_disconnect) as usize;

            let top_peers: Vec<PeerId> = traffic_list
                .iter()
                .take(num_to_disconnect)
                .map(|(&peer_id, _)| peer_id)
                .collect();
            result = top_peers;
        }

        result
    }

    pub fn record_traffic(&mut self, peer_id: PeerId, bytes: u64) {
        *self.peer_traffic.entry(peer_id).or_insert(0) += bytes;
    }

    pub fn clear_peer_traffic(&mut self) {
        self.peer_traffic.clear();
    }
}
