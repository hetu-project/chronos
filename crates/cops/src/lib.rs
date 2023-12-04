//! A causally consistent data store inspired by COPS.
//!
//! The data store maintains a set of key-value pairs. It provides
//! causal consistency to clients.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::BufRead;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

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
enum Reply {
    ReadReply(ReadReply),
    WriteReply(WriteReply),
}

#[derive(Serialize, Deserialize, Debug)]
struct Read {
    key: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Write {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct ReadReply {
    value: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
struct WriteReply {
    success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
struct Sync {
    key: String,
    value: String,
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

    /// Read a key from COPS.
    pub async fn read(&mut self, key: &str, index: Option<usize>) -> Option<String> {
        let request = Request::Read(Read {
            key: key.to_string(),
        });
        let index = match index {
            Some(index) => index,
            None => rand::random::<usize>() % self.config.server_addrs.len(),
        };
        self.invoke(request, index).await
    }

    /// Write a key-value pair to COPS.
    pub async fn write(&mut self, key: &str, value: &str, index: Option<usize>) {
        let request = Request::Write(Write {
            key: key.to_string(),
            value: value.to_string(),
        });
        let index = match index {
            Some(index) => index,
            None => rand::random::<usize>() % self.config.server_addrs.len(),
        };
        self.invoke(request, index).await;
    }

    /// Invoke a request to COPS and wait for a reply. For read request, the
    /// read result is returned.
    async fn invoke(&mut self, request: Request, index: usize) -> Option<String> {
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
                    Reply::ReadReply(reply) => return reply.value,
                    Reply::WriteReply(_) => {
                        return None;
                    }
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
#[derive(Debug, Clone)]
struct ServerState {
    store: HashMap<String, String>,
}

impl ServerState {
    /// Create a new server state.
    fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    /// Retrieve a value from the store.
    fn read(&mut self, key: &str) -> Option<String> {
        self.store.get(key).cloned()
    }

    /// Update a value in the store.
    fn write(&mut self, key: &str, value: &str) {
        self.store.insert(key.to_string(), value.to_string());
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

    /// Handle a client request.
    async fn handle_request(&mut self, request: Request, src: SocketAddr) {
        let reply = match request {
            Request::Read(request) => Reply::ReadReply(ReadReply {
                value: self.state.read(&request.key),
            }),
            Request::Write(request) => {
                self.state.write(&request.key, &request.value);
                // Synchronize state with other servers.
                for i in 0..self.config.server_addrs.len() {
                    if i != self.index {
                        let sync = Sync {
                            key: request.key.clone(),
                            value: request.value.clone(),
                        };
                        let msg = serde_json::to_string(&Message::Sync(sync)).unwrap();
                        self.socket
                            .send_to(msg.as_bytes(), self.config.server_addrs[i])
                            .await
                            .unwrap();
                    }
                }
                Reply::WriteReply(WriteReply { success: true })
            }
        };
        let msg = serde_json::to_string(&Message::Reply(reply)).unwrap();
        self.socket.send_to(msg.as_bytes(), src).await.unwrap();
    }

    /// Handle a sync message.
    async fn handle_sync(&mut self, sync: Sync) {
        self.state.write(&sync.key, &sync.value);
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
        let (config, handles) = start_servers(3).await;
        // Run client
        let mut client = Client::new(&config).await;
        client.write("k1", "v1", Some(0)).await;
        client.write("k2", "v2", Some(0)).await;
        assert_eq!(client.read("k1", Some(0)).await, Some("v1".to_string()));
        assert_eq!(client.read("k2", Some(0)).await, Some("v2".to_string()));
        assert_eq!(client.read("k3", Some(0)).await, None);
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
        assert_eq!(client.read("k1", Some(2)).await, Some("v1".to_string()));
        assert_eq!(client.read("k2", Some(2)).await, Some("v2".to_string()));
        client.write("k1", "v3", Some(2)).await;
        terminate(&config).await;
        let mut states = collect_states(handles).await;
        assert_eq!(states[0].read("k1"), Some("v3".to_string()));
        assert_eq!(states[1].read("k1"), Some("v3".to_string()));
        assert_eq!(states[2].read("k1"), Some("v3".to_string()));
    }
}
