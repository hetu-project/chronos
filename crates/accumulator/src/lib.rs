//! A simple accumulator application.
//!
//! Each accumulator node maintains a set of strings. Upon receiving a string
//! from a client, the node adds the string to its state, and broadcast the
//! new state to other nodes in the network. All nodes eventually converge to
//! the same state, by merging received states into their own states.

use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::net::{SocketAddr, UdpSocket};
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
    pub fn new(config: &Configuration) -> Self {
        let s = UdpSocket::bind("0.0.0.0:0").unwrap();
        Self {
            socket: s,
            config: config.clone(),
        }
    }

    /// Disseminate a string to the accumulator network.
    pub fn disseminate(&mut self, item: &str) {
        let msg = Message::FromClient(ClientMessage {
            item: String::from(item),
        });
        self.socket
            .send_to(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                self.config.server_addrs[0],
            )
            .unwrap();
    }

    /// Terminate a running accumulator server.
    pub fn terminate(&mut self, index: usize) {
        let msg = Message::Terminate;
        self.socket
            .send_to(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                self.config.server_addrs[index],
            )
            .unwrap();
    }
}

/// State of a server node.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServerState {
    clock: Clock,
    items: BTreeSet<String>,
}

impl ServerState {
    /// Create a new server state.
    fn new() -> Self {
        Self {
            clock: Clock::new(),
            items: BTreeSet::new(),
        }
    }

    fn hash(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.items.hash(&mut hasher);
        hasher.finish()
    }

    /// Add items into the state. Returns true if resulting in a new state.
    fn add(&mut self, items: BTreeSet<String>) -> bool {
        if items.is_subset(&self.items) {
            false
        } else {
            self.items.extend(items);
            self.clock.inc(self.hash() as usize)
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
                // TODO: the two states have no causality, but result in the
                // same items.
                self.clock.merge(&other.clock);
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
    pub fn new(config: &Configuration, index: usize) -> Self {
        let s = UdpSocket::bind(config.server_addrs[index]).unwrap();
        Self::from_socket(config, index, s)
    }

    /// Create a new server from existing socket
    fn from_socket(config: &Configuration, index: usize, socket: UdpSocket) -> Self {
        Self {
            config: config.clone(),
            index,
            socket,
            state: ServerState::new(),
            running: false,
        }
    }

    /// Handle a message
    fn handle_msg(&mut self, msg: Message) {
        match msg {
            Message::FromClient(msg) => {
                if self.state.add(BTreeSet::from_iter(vec![msg.item])) {
                    self.broadcast_state();
                }
            }
            Message::FromServer(msg) => {
                if self.state.merge(&msg.state) {
                    self.broadcast_state();
                }
            }
            Message::Terminate => {
                self.running = false;
            }
        }
    }

    /// Broadcast current state to all other nodes in the network.
    fn broadcast_state(&mut self) {
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
                    .unwrap();
            }
        }
    }

    /// Main event loop.
    pub fn run(&mut self) {
        self.running = true;
        while self.running {
            let mut buf = [0; 1500];
            let (n, _) = self.socket.recv_from(&mut buf).unwrap();
            let msg: Message = serde_json::from_str(&String::from_utf8_lossy(&buf[..n])).unwrap();
            self.handle_msg(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::thread::JoinHandle;
    use std::time;

    use super::*;

    fn start_servers(n_server: usize) -> (Configuration, Vec<JoinHandle<BTreeSet<String>>>) {
        let mut config = Configuration {
            server_addrs: Vec::new(),
        };
        let mut sockets = Vec::new();
        for _ in 0..n_server {
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            config.server_addrs.push(s.local_addr().unwrap());
            sockets.push(s);
        }
        let mut handles = Vec::new();
        for i in 0..n_server {
            let c = config.clone();
            let s = sockets[i].try_clone().unwrap();
            handles.push(std::thread::spawn(move || {
                let mut server = Server::from_socket(&c, i, s);
                server.run();
                server.state.items
            }));
        }
        (config, handles)
    }

    fn collect_states(handles: Vec<JoinHandle<BTreeSet<String>>>) -> Vec<BTreeSet<String>> {
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    }

    fn terminate(config: &Configuration) {
        let mut client = Client::new(config);
        for i in 0..config.server_addrs.len() {
            client.terminate(i);
        }
    }

    #[test]
    fn single_server() {
        // Start server
        let (config, handles) = start_servers(1);
        // Run client
        let mut client = Client::new(&config);
        client.disseminate("hello");
        // End test
        thread::sleep(time::Duration::from_millis(100));
        terminate(&config);
        let states = collect_states(handles);
        assert!(states[0].contains("hello"));
    }

    #[test]
    fn multi_servers() {
        // Start servers
        let (config, handles) = start_servers(3);
        // Run client
        let mut client = Client::new(&config);
        client.disseminate("hello");
        client.disseminate("world");
        // End test
        thread::sleep(time::Duration::from_millis(100));
        terminate(&config);
        let states = collect_states(handles);
        assert!(states.iter().all(|s| s.contains("hello")));
        assert!(states.iter().all(|s| s.contains("world")));
        assert!(states.iter().all(|s| s.len() == 2));
    }
}
