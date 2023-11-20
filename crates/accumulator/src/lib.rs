//! A simple accumulator application.
//!
//! Each accumulator node maintains a set of strings. Upon receiving a string
//! from a client, the node adds the string to its state, and broadcast the
//! new state to other nodes in the network. All nodes eventually converge to
//! the same state, by merging received states into their own states.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::net::{SocketAddr, UdpSocket};

#[derive(Serialize, Deserialize, Debug)]
enum Message {
    FromClient(ClientMessage),
    FromServer(ServerMessage),
    Terminate,
}

/// Network configuration. Contains a list of server addresses.
#[derive(Debug, Clone)]
struct Configuration {
    server_addrs: Vec<SocketAddr>,
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
    state: HashSet<String>,
}

/// A client node for the accumulator application.
struct Client {
    socket: UdpSocket,
    config: Configuration,
}

impl Client {
    /// Create a new client
    fn new(config: &Configuration) -> Self {
        let s = UdpSocket::bind("0.0.0.0:0").unwrap();
        Self {
            socket: s,
            config: config.clone(),
        }
    }

    /// Disseminate a string to the accumulator network.
    fn disseminate(&mut self, item: &str) {
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
    fn terminate(&mut self, index: usize) {
        let msg = Message::Terminate;
        self.socket
            .send_to(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                self.config.server_addrs[index],
            )
            .unwrap();
    }
}

/// An accumulator server node. Each node maintains a UDP socket, and a set of
/// strings as its internal state.
struct Server {
    config: Configuration,
    addr: SocketAddr,
    socket: UdpSocket,
    state: HashSet<String>,
    running: bool,
}

impl Server {
    /// Create a new server
    fn new(config: &Configuration, addr: &SocketAddr) -> Self {
        let s = UdpSocket::bind(addr).unwrap();
        Self::from_socket(config, addr, s)
    }

    /// Create a new server from existing socket
    fn from_socket(config: &Configuration, addr: &SocketAddr, socket: UdpSocket) -> Self {
        Self {
            config: config.clone(),
            addr: addr.clone(),
            socket: socket,
            state: HashSet::new(),
            running: false,
        }
    }

    /// Handle a message
    fn handle_msg(&mut self, msg: Message) {
        match msg {
            Message::FromClient(msg) => {
                let s = HashSet::from_iter(vec![msg.item]);
                self.merge(s);
            }
            Message::FromServer(msg) => {
                self.merge(msg.state);
            }
            Message::Terminate => {
                self.running = false;
            }
        }
    }

    /// Merge a state into the current state. If the state changes, broadcast
    /// the new state.
    fn merge(&mut self, state: HashSet<String>) {
        let old_size = self.state.len();
        self.state.extend(state);
        if self.state.len() > old_size {
            self.broadcast(Message::FromServer(ServerMessage {
                state: self.state.clone(),
            }));
        }
    }

    /// Broadcast message to all other nodes in the network.
    fn broadcast(&mut self, msg: Message) {
        for addr in &self.config.server_addrs {
            if self.addr.ne(addr) {
                self.socket
                    .send_to(serde_json::to_string(&msg).unwrap().as_bytes(), addr)
                    .unwrap();
            }
        }
    }

    /// Main event loop.
    fn run(&mut self) {
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

    fn start_servers(n_server: usize) -> (Configuration, Vec<JoinHandle<HashSet<String>>>) {
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
                let mut server = Server::from_socket(&c, &c.server_addrs[i], s);
                server.run();
                server.state
            }));
        }
        (config, handles)
    }

    fn collect_states(handles: Vec<JoinHandle<HashSet<String>>>) -> Vec<HashSet<String>> {
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
        // End test
        thread::sleep(time::Duration::from_millis(100));
        terminate(&config);
        let states = collect_states(handles);
        assert!(states.iter().all(|s| s.contains("hello")));
    }
}
