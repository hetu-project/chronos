//! A causally consistent data store inspired by COPS.
//!
//! The data store maintains a set of key-value pairs. It provides
//! causal consistency to clients.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::net::SocketAddr;
use tokio::net::UdpSocket;
use vlc::Clock;

/// Message types.
#[derive(Serialize, Deserialize, Debug)]
enum Message {
    Request(Request),
    Reply(Reply),
    Sync(Sync),
    Terminate,
}

#[derive(Serialize, Deserialize, Debug)]
enum Request {
    Read(Read),
    Write(Write),
}

#[derive(Serialize, Deserialize, Debug)]
struct Read {
    key: String,
    clock: Clock,
}

#[derive(Serialize, Deserialize, Debug)]
struct Write {
    key: String,
    value: String,
    clock: Clock,
}

#[derive(Serialize, Deserialize, Debug)]
enum Reply {
    ReadReply(ReadReply),
    WriteReply(WriteReply),
}

#[derive(Serialize, Deserialize, Debug)]
struct ReadReply {
    value: Option<String>,
    clock: Option<Clock>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WriteReply {
    clock: Option<Clock>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Sync {
    state: ServerState,
}

/// System configuration. Contains a list of server addresses.
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

/// A client node for COPS.
pub struct Client {
    socket: UdpSocket,
    config: Configuration,
    clock: Clock,
}

impl Client {
    /// Create a new client
    pub async fn new(config: &Configuration) -> Self {
        let s = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        Self {
            socket: s,
            config: config.clone(),
            clock: Clock::new(),
        }
    }

    /// Read a key from COPS. If index is not None, read from the specified server.
    /// Otherwise, read from a random server.
    pub async fn read(&mut self, key: &str, index: Option<usize>) -> (bool, Option<String>) {
        let request = Request::Read(Read {
            key: key.to_string(),
            clock: self.clock.clone(),
        });
        let index = match index {
            Some(index) => index,
            None => rand::random::<usize>() % self.config.server_addrs.len(),
        };
        self.invoke(request, index).await
    }

    /// Write a key-value pair to COPS. If index is not None, write to the specified
    /// server; otherwise, writes always go to the home server of the key.
    /// This removes write conflicts on the same key.
    ///
    /// Todo: allow same key conflict.
    pub async fn write(&mut self, key: &str, value: &str, index: Option<usize>) {
        let request = Request::Write(Write {
            key: key.to_string(),
            value: value.to_string(),
            clock: self.clock.clone(),
        });
        let index = match index {
            Some(index) => index,
            None => self.key_to_server(key),
        };
        self.invoke(request, index).await;
    }

    /// Determine the home server of a key.
    fn key_to_server(&self, key: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.config.server_addrs.len()
    }

    /// Invoke a request to COPS and wait for a reply. For read request, the
    /// read result is returned. Returns false if the server currently does not
    /// have all the causal dependencies.
    async fn invoke(&mut self, request: Request, index: usize) -> (bool, Option<String>) {
        let msg = serde_json::to_string(&Message::Request(request)).unwrap();
        self.socket
            .send_to(msg.as_bytes(), self.config.server_addrs[index])
            .await
            .unwrap();
        // Todo: add retransmission timeout
        // Todo: add request id
        loop {
            let mut buf = [0; 1500];
            let (len, _) = self.socket.recv_from(&mut buf).await.unwrap();
            let msg =
                serde_json::from_str::<Message>(&String::from_utf8_lossy(&buf[..len])).unwrap();
            match msg {
                Message::Reply(reply) => match reply {
                    Reply::ReadReply(reply) => match reply.clock {
                        Some(clock) => {
                            self.clock.merge(&clock);
                            return (true, reply.value);
                        }
                        None => {
                            return (false, None);
                        }
                    },
                    Reply::WriteReply(reply) => match reply.clock {
                        Some(clock) => {
                            self.clock.merge(&clock);
                            return (true, None);
                        }
                        None => {
                            return (false, None);
                        }
                    },
                },
                _ => {}
            }
        }
    }

    /// Terminate a running COPS server.
    pub async fn terminate(&mut self, index: usize) {
        let msg = serde_json::to_string(&Message::Terminate).unwrap();
        self.socket
            .send_to(msg.as_bytes(), self.config.server_addrs[index])
            .await
            .unwrap();
    }
}

/// State of a COPS storage node.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ServerState {
    store: HashMap<String, (String, u32)>,
    clock: Clock,
}

impl ServerState {
    /// Create a new server state.
    fn new() -> Self {
        Self {
            store: HashMap::new(),
            clock: Clock::new(),
        }
    }

    /// Retrieve a value from the store.
    fn read(&mut self, key: &str) -> Option<String> {
        match self.store.get_mut(key) {
            Some((value, _)) => Some(value.clone()),
            None => None,
        }
    }

    /// Update a value in the store.
    fn write(&mut self, key: &str, value: &str) {
        let ver = match self.store.get_mut(key) {
            Some((_, ver)) => *ver + 1,
            None => 1,
        };
        self.store.insert(key.to_string(), (value.to_string(), ver));
        // Increment clock. Instead of using the hash of the entire state, we use
        // the hash of the key-value pair.
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        let kv = format!("{}:{}", key, value);
        kv.hash(&mut hasher);
        self.clock.inc(hasher.finish() as usize);
    }

    /// Merge another ServerState into the current state.
    ///
    /// Todo: with the home server approach, there won't be any single key conflict.
    /// Simply merge the two maps is sufficient (higher version wins for each key).
    /// Need to have a more general approach when we allow single key conflict.
    fn merge(&mut self, other: &Self) {
        for (key, (value, ver)) in &other.store {
            if let Some((_, my_ver)) = self.store.get(key) {
                if ver <= my_ver {
                    continue;
                }
            }
            self.store.insert(key.to_string(), (value.clone(), *ver));
        }
        self.clock.merge(&other.clock);
    }
}

/// A storage node for COPS.
pub struct Server {
    config: Configuration,
    index: usize,
    socket: UdpSocket,
    state: ServerState,
    running: bool,
}

impl Server {
    /// Create a new storage node.
    pub async fn new(config: &Configuration, index: usize) -> Self {
        let s = UdpSocket::bind(config.server_addrs[index]).await.unwrap();
        Self {
            config: config.clone(),
            index,
            socket: s,
            state: ServerState::new(),
            running: false,
        }
    }

    /// Handle a message.
    async fn handle_message(&mut self, msg: Message, src: SocketAddr) {
        match msg {
            Message::Request(request) => {
                self.handle_request(request, src).await;
            }
            Message::Sync(sync) => {
                self.handle_sync(sync).await;
            }
            Message::Terminate => {
                self.running = false;
            }
            _ => {}
        }
    }

    /// Check if local clock is not up-to-date.
    fn is_behind(&self, clock: &Clock) -> bool {
        match self.state.clock.partial_cmp(clock) {
            Some(Ordering::Less) => true,
            None => true,
            _ => false,
        }
    }

    /// Handle a client request.
    async fn handle_request(&mut self, request: Request, src: SocketAddr) {
        // If local clock is behind the request dependency, ask client to retry.
        // State should eventually be synchronized.
        //
        // Todo: active synchronization when local clock is behind.
        let reply = match request {
            Request::Read(request) => {
                if self.is_behind(&request.clock) {
                    Reply::ReadReply(ReadReply {
                        value: None,
                        clock: None,
                    })
                } else {
                    Reply::ReadReply(ReadReply {
                        value: self.state.read(&request.key),
                        clock: Some(self.state.clock.clone()),
                    })
                }
            }
            Request::Write(request) => {
                if self.is_behind(&request.clock) {
                    Reply::WriteReply(WriteReply { clock: None })
                } else {
                    self.state.write(&request.key, &request.value);
                    // Synchronize state with other servers.
                    for i in 0..self.config.server_addrs.len() {
                        if i != self.index {
                            let sync = Sync {
                                state: self.state.clone(),
                            };
                            let msg = serde_json::to_string(&Message::Sync(sync)).unwrap();
                            self.socket
                                .send_to(msg.as_bytes(), self.config.server_addrs[i])
                                .await
                                .unwrap();
                        }
                    }
                    Reply::WriteReply(WriteReply {
                        clock: Some(self.state.clock.clone()),
                    })
                }
            }
        };

        let msg = serde_json::to_string(&Message::Reply(reply)).unwrap();
        self.socket.send_to(msg.as_bytes(), src).await.unwrap();
    }

    /// Handle a sync message.
    async fn handle_sync(&mut self, sync: Sync) {
        self.state.merge(&sync.state);
    }

    /// Start the storage node.
    pub async fn run(&mut self) {
        self.running = true;
        while self.running {
            let mut buf = [0; 1500];
            let (len, src) = self.socket.recv_from(&mut buf).await.unwrap();
            let msg =
                serde_json::from_str::<Message>(&String::from_utf8_lossy(&buf[..len])).unwrap();
            self.handle_message(msg, src).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinHandle;

    async fn start_servers(n_server: usize) -> (Configuration, Vec<JoinHandle<ServerState>>) {
        let mut config = Configuration {
            server_addrs: Vec::new(),
        };
        let port = 8000 + (rand::random::<usize>() % 100) * 10;
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
                server.state
            }));
        }
        (config, handles)
    }

    async fn collect_states(handles: Vec<JoinHandle<ServerState>>) -> Vec<ServerState> {
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
    async fn from_same_server() {
        // Start server
        let (config, handles) = start_servers(1).await;
        // Run client
        let mut client = Client::new(&config).await;
        client.write("k1", "v1", Some(0)).await;
        client.write("k2", "v2", Some(0)).await;
        assert_eq!(
            client.read("k1", Some(0)).await,
            (true, Some("v1".to_string()))
        );
        assert_eq!(
            client.read("k2", Some(0)).await,
            (true, Some("v2".to_string()))
        );
        assert_eq!(client.read("k3", Some(0)).await, (true, None));
        terminate(&config).await;
        let mut states = collect_states(handles).await;
        assert_eq!(states[0].read("k1"), Some("v1".to_string()));
        assert_eq!(states[0].read("k2"), Some("v2".to_string()));
        assert_eq!(states[0].read("k3"), None);
    }

    #[tokio::test]
    async fn sync() {
        // Start server
        let (config, handles) = start_servers(3).await;
        // Run client
        let mut client = Client::new(&config).await;
        client.write("k1", "v1", Some(0)).await;
        client.write("k2", "v2", Some(1)).await;
        assert_eq!(
            client.read("k1", Some(2)).await,
            (true, Some("v1".to_string()))
        );
        assert_eq!(
            client.read("k2", Some(2)).await,
            (true, Some("v2".to_string()))
        );
        client.write("k1", "v3", Some(2)).await;
        terminate(&config).await;
        let mut states = collect_states(handles).await;
        assert_eq!(states[0].read("k1"), Some("v3".to_string()));
        assert_eq!(states[1].read("k1"), Some("v3".to_string()));
        assert_eq!(states[2].read("k1"), Some("v3".to_string()));
        assert_eq!(states[0].read("k2"), Some("v2".to_string()));
        assert_eq!(states[1].read("k2"), Some("v2".to_string()));
        assert_eq!(states[2].read("k2"), Some("v2".to_string()));
    }
}
