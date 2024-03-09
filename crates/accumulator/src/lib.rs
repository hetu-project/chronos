//! A simple accumulator application.
//!
//! Each accumulator node maintains a set of strings. Upon receiving a string
//! from a client, the node adds the string to its state, and broadcast the
//! new state to other nodes in the network. All nodes eventually converge to
//! the same state, by merging received states into their own states.

use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::BTreeSet;
use std::io::BufRead;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use vlc::Clock;

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    FromClient(ClientMessage),
    FromServer(ServerMessage),
    Terminate,
}

/// Network configuration. Contains a list of server addresses.
#[derive(Debug, Clone)]
pub struct Configuration {
    server_addrs: Vec<SocketAddr>,
}

impl Configuration {
    /// Create configuration from a file.
    pub fn from_file(path: &str) -> Self {
        let mut config = Configuration {
            server_addrs: Vec::new(),
        };
        let file = std::fs::File::open(path).unwrap();
        let reader = std::io::BufReader::new(file);
        for line in reader.lines() {
            let addr = line.unwrap().parse().unwrap();
            config.server_addrs.push(addr);
        }
        config
    }
}

/// Client message type for the accumulator application. Each message contains
/// a string.
#[derive(Serialize, Deserialize, Debug)]
struct ClientMessage {
    item: String,
}

/// The current node state, which is a set of strings.
#[derive(Serialize, Deserialize, Debug)]
struct ServerMessage {
    state: ServerState,
}

/// A client node for the accumulator application.
pub struct Client {
    socket: UdpSocket,
    config: Configuration,
}

impl Client {
    /// Create a new client
    pub async fn new(config: &Configuration) -> Self {
        let s = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        Self {
            socket: s,
            config: config.clone(),
        }
    }

    /// Disseminate a string to the accumulator network.
    pub async fn disseminate(&mut self, item: &str) {
        let msg = Message::FromClient(ClientMessage {
            item: String::from(item),
        });
        self.socket
            .send_to(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                self.config.server_addrs[0],
            )
            .await
            .unwrap();
    }

    /// Terminate a running accumulator server.
    pub async fn terminate(&mut self, index: usize) {
        let msg = Message::Terminate;
        self.socket
            .send_to(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                self.config.server_addrs[index],
            )
            .await
            .unwrap();
    }
}

/// State of a server node.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServerState {
    clock: Clock,
    id: u128,
    items: BTreeSet<String>,
}

impl ServerState {
    /// Create a new server state.
    fn new(id: u128) -> Self {
        Self {
            clock: Clock::new(),
            id,
            items: BTreeSet::new(),
        }
    }

    /// Add items into the state. Returns true if resulting in a new state.
    fn add(&mut self, items: BTreeSet<String>) -> bool {
        if items.is_subset(&self.items) {
            false
        } else {
            self.items.extend(items);
            self.clock.inc(self.id);
            true
        }
    }

    /// Merge another ServerState into the current state. Returns true if
    /// resulting in a new state (different from current and received
    /// state).
    fn merge(&mut self, other: &Self) -> bool {
        match self.clock.partial_cmp(&other.clock) {
            Some(cmp::Ordering::Equal) => false,
            Some(cmp::Ordering::Greater) => false,
            Some(cmp::Ordering::Less) => {
                self.clock = other.clock.clone();
                self.items = other.items.clone();
                false
            }
            None => {
                self.clock.merge(&vec![&other.clock]);
                self.add(other.items.clone())
            }
        }
    }
}

/// An accumulator server node. Each node maintains a UDP socket, and a set of
/// strings as its internal state.
pub struct Server {
    config: Configuration,
    index: usize,
    socket: UdpSocket,
    state: ServerState,
    running: bool,
}

impl Server {
    /// Create a new server
    pub async fn new(config: &Configuration, index: usize) -> Self {
        let s = UdpSocket::bind(config.server_addrs[index]).await.unwrap();
        Self {
            config: config.clone(),
            index,
            socket: s,
            state: ServerState::new(index.try_into().unwrap()),
            running: false,
        }
    }

    /// Handle a message
    async fn handle_msg(&mut self, msg: Message) {
        match msg {
            Message::FromClient(msg) => {
                if self.state.add(BTreeSet::from_iter(vec![msg.item])) {
                    self.broadcast_state().await;
                }
            }
            Message::FromServer(msg) => {
                if self.state.merge(&msg.state) {
                    self.broadcast_state().await;
                }
            }
            Message::Terminate => {
                self.running = false;
            }
        }
    }

    /// Broadcast current state to all other nodes in the network.
    async fn broadcast_state(&mut self) {
        let msg = Message::FromServer(ServerMessage {
            state: self.state.clone(),
        });
        for i in 0..self.config.server_addrs.len() {
            if self.index != i {
                self.socket
                    .send_to(
                        serde_json::to_string(&msg).unwrap().as_bytes(),
                        self.config.server_addrs[i],
                    )
                    .await
                    .unwrap();
            }
        }
    }

    /// Main event loop.
    pub async fn run(&mut self) {
        self.running = true;
        while self.running {
            let mut buf = [0; 1500];
            let (n, _) = self.socket.recv_from(&mut buf).await.unwrap();
            let msg: Message = serde_json::from_str(&String::from_utf8_lossy(&buf[..n])).unwrap();
            self.handle_msg(msg).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;
    use std::time;
    use tokio::task::JoinHandle;

    use super::*;

    async fn start_servers(n_server: usize) -> (Configuration, Vec<JoinHandle<BTreeSet<String>>>) {
        let mut config = Configuration {
            server_addrs: Vec::new(),
        };
        let port = 8000 + rand::thread_rng().gen_range(0..100) * 10;
        for i in 0..n_server {
            let addr = format!("127.0.0.1:{}", port + i).parse().unwrap();
            config.server_addrs.push(addr);
        }
        let mut handles = Vec::new();
        for i in 0..n_server {
            let c = config.clone();
            handles.push(tokio::spawn(async move {
                let mut server = Server::new(&c, i).await;
                server.run().await;
                server.state.items
            }));
        }
        (config, handles)
    }

    async fn collect_states(handles: Vec<JoinHandle<BTreeSet<String>>>) -> Vec<BTreeSet<String>> {
        let mut states = Vec::new();
        for handle in handles {
            states.push(handle.await.unwrap());
        }
        states
    }

    async fn terminate(config: &Configuration) {
        let mut client = Client::new(config).await;
        for i in 0..config.server_addrs.len() {
            client.terminate(i).await;
        }
    }

    #[tokio::test]
    async fn single_server() {
        // Start server
        let (config, handles) = start_servers(1).await;
        // Run client
        let mut client = Client::new(&config).await;
        client.disseminate("hello").await;
        // End test
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        terminate(&config).await;
        let states = collect_states(handles).await;
        assert!(states[0].contains("hello"));
    }

    #[tokio::test]
    async fn multi_servers() {
        // Start servers
        let (config, handles) = start_servers(3).await;
        // Run client
        let mut client = Client::new(&config).await;
        client.disseminate("hello").await;
        client.disseminate("world").await;
        // End test
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        terminate(&config).await;
        let states = collect_states(handles).await;
        assert!(states.iter().all(|s| s.contains("hello")));
        assert!(states.iter().all(|s| s.contains("world")));
        assert!(states.iter().all(|s| s.len() == 2));
    }
}
