//! A vlc-dag application.
//!
//! The vlc-dag is base on the simple accumulator application. 
//! Please reference on crates/accumulator. But add new features as follow:
//! * defined the vertex and edge of the event & clock propagation dag.
//! * support the LMDB to persistence storage for now，
//! * maybe time-series db,like influxDB, or related postgre more suitable. (TD)
//! * Support increment state sync with peers using p2p communication protocol.
//! * so we can use the vertex and edge from db to reconstruct clock propagation dag.

pub mod db_client;

use clap::builder::Str;
use db_client::lldb_client::VLCLLDb;
use serde::{Deserialize, Serialize};
use std::time::UNIX_EPOCH;
use std::{cmp, time::SystemTime};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::BufRead;
use std::sync::{Arc, RwLock};
use std::net::SocketAddr;
use tokio::net::UdpSocket;

use std::time;
use tokio::{task::JoinHandle};
use vlc::Clock;
use lmdb_rs as lmdb;

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
#[derive(Serialize, Deserialize, Debug, Clone)]
enum ServerMessage {
    EventTrigger(EventTrigger),
    DiffReq(DiffReq),
    DiffRsp(DiffRsp),
    ActiveSync(ActiveSync)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct EventTrigger {
    clock_info: ClockInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DiffReq {
    to : u128,
    from_clock: ClockInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct DiffRsp {
    to : u128,
    diffs: Vec<String>,
    from: ClockInfo,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ActiveSync {
    to : u128,
    diffs: Vec<String>,
    latest: ClockInfo,
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
    pub async fn disseminate(&mut self, item: &str, id: usize) {
        let msg = Message::FromClient(ClientMessage {
            item: String::from(item),
        });
        self.socket
            .send_to(
                serde_json::to_string(&msg).unwrap().as_bytes(),
                self.config.server_addrs[id],
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
    clock_info: ClockInfo,
    id: u128,
    items: Vec<String>, // BtreeSet or HashSet can't keep event inserted order.
    clock_to_eventid: HashMap<String, String>, // clock_id to event_id
    message_id_set: BTreeSet<String>,
}

impl ServerState {
    /// Create a new server state.
    fn new(id: u128) -> Self {

        Self {
            clock_info: ClockInfo {
                clock: Clock::new(),
                id,
                message_id: String::new(),
                count: 0,
                create_at: get_time_ms(),
            },
            id,
            items: Vec::new(),
            clock_to_eventid: HashMap::new(),
            message_id_set: BTreeSet::new(),
        }
    }

    /// Add items into the state. Returns true if resulting in a new state.
    fn add(&mut self, items: Vec<String>) -> bool {
        if items.is_empty() {
            return false;
        } 

        for item in items.iter() {
            // filter replicate message id
            if !self.message_id_set.contains(item) {
                self.items.push(item.clone());
                self.message_id_set.insert(item.clone());
            }
        }

        self.clock_info.clock.inc(self.id);
        self.clock_info.count += 1;
        if let Some(last) = items.last() {
            self.clock_info.message_id = last.to_string();
        }

        true
    }

    fn handle_event_trigger(&mut self, msg: EventTrigger) -> Option<ServerMessage> {
        match self.clock_info.clock.partial_cmp(&msg.clock_info.clock) {
            Some(cmp::Ordering::Equal) => None,
            Some(cmp::Ordering::Greater) => None,
            Some(cmp::Ordering::Less) => {
                let self_clock_info = ClockInfo::new(self.clock_info.clock.clone(), self.id, self.clock_info.message_id.clone(), 0);
                let req = ServerMessage::DiffReq(DiffReq { from_clock: self_clock_info, to:  msg.clock_info.id });
                Some(req)
            }
            None => {
                let base = self.clock_info.clock.base_common(&msg.clock_info.clock);
                if base.is_genesis() {
                    let req = ServerMessage::ActiveSync(ActiveSync { diffs: self.items.clone(), latest: self.clock_info.clone(), to: msg.clock_info.id });
                    Some(req)
                } else {
                    if let Some(start_msg_id) = self.clock_to_eventid.get(&base.index_key()) {
                        if let Some(diff_msg_ids) = get_suffix(&self.items, start_msg_id.to_string()) {
                            // req diff and send self diff
                            let req = ServerMessage::ActiveSync(ActiveSync { diffs: diff_msg_ids, latest: self.clock_info.clone(), to: msg.clock_info.id });
                            Some(req)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
            }
        }
    }

    fn handle_diff_req(&mut self, msg: DiffReq, db: Arc<RwLock<VLCLLDb>>) -> Option<ServerMessage> {
        // println!("Key-To-messageid: {:?}", self.clock_to_eventid);
        if msg.from_clock.count == 0 {
            let req = ServerMessage::DiffRsp(DiffRsp { diffs: self.items.clone(), from:self.clock_info.clone(), to: msg.from_clock.id });
            return Some(req);
        }
        let empty_str = String::new();
        let start_msg_id =self.clock_to_eventid.get(&msg.from_clock.clock.index_key()).unwrap_or(&empty_str);
        // get begin from start_msg_id to end of state set
        if let Some(diff_msg_ids) = get_suffix(&self.items, start_msg_id.to_string()) {
            let req = ServerMessage::DiffRsp(DiffRsp { diffs: diff_msg_ids,from:self.clock_info.clone(), to: msg.from_clock.id });
            Some(req)
        } else {
            None
        }
    }

    fn handle_diff_rsp(&mut self, msg: DiffRsp, db: Arc<RwLock<VLCLLDb>>) -> bool {
        match self.clock_info.clock.partial_cmp(&msg.from.clock) {
            Some(cmp::Ordering::Equal) => {},
            Some(cmp::Ordering::Greater) => {},
            Some(cmp::Ordering::Less) => {
                self.items.extend(msg.diffs.clone());  // message id represent message 
                self.clock_info = msg.from.clone();
                self.clock_info.clock.inc(self.id);
                self.clock_to_eventid.insert(msg.from.clock.index_key(), msg.from.message_id);
                self.message_id_set.extend(msg.diffs);
                return true;
            }
            None => {
                self.clock_info.clock.merge(&vec![&msg.from.clock]);
                let add = self.add(msg.diffs);
                if !add {
                    return true;
                }
            }
        }
        false
    }

    fn handle_active_sync(&mut self, msg: ActiveSync, db: Arc<RwLock<VLCLLDb>>) -> (Option<ServerMessage>, bool){
        match self.clock_info.clock.partial_cmp(&msg.latest.clock) {
            Some(cmp::Ordering::Equal) => return (None, false),
            Some(cmp::Ordering::Greater) => return (None, false),
            Some(cmp::Ordering::Less) => {
                self.items.extend(msg.diffs.clone());  // message id represent message 
                self.clock_info = msg.latest.clone();
                self.clock_info.clock.inc(self.id);
                self.message_id_set.extend(msg.diffs);
                (None, true)
            }
            None => {
                // let from_clock = self.clock_info.clone();
                // let pre_items = self.items.clone();
                // Todo: Calculate diff 
                // Todo: simulator
                let init_clock = self.clock_info.clock.clone();
                self.clock_info.clock.merge(&vec![&msg.latest.clock]);
                let add = self.add(msg.diffs);
                if !add {
                    self.clock_info.clock = init_clock;
                    return (None, false);
                }
                (None, true)

                /* 
                Follow code may led to recursive merge
                let base = from_clock.clock.base_common(&msg.latest.clock);
                if base.is_genesis() {
                    let req = ServerMessage::ActiveSync(ActiveSync { diffs: pre_items, latest: self.clock_info.clone(), to: msg.latest.id });
                    Some(req)
                } else {
                    if let Some(start_msg_id) = self.clock_to_eventid.get(&base.index_key()) {
                        if let Some(diff_msg_ids) = get_suffix(&self.items, start_msg_id.to_string()) {
                            // req diff and send self diff
                            let req = ServerMessage::ActiveSync(ActiveSync { diffs: diff_msg_ids, latest: self.clock_info.clone(), to: msg.latest.id });
                            Some(req)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                */
            }
        }

    }
    
}

/// Clock info sinker to db.
/// id is server node id, count is the event count in this server.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct ClockInfo {
    pub clock: Clock,
    pub id: u128,  
    pub message_id: String,
    pub count: u128,
    pub create_at: u128,
}

impl ClockInfo {
    fn new(clock: Clock, id: u128, message_id: String, count: u128) -> Self {
        let create_at = get_time_ms();
        Self { clock, id, message_id, count, create_at }
    }
}

/// MergeLog sinker to db.
/// id is server node id, count is the event count in this server.
#[derive(Serialize, Deserialize, Debug, Clone)]
struct MergeLog {
    from_id: u128,
    to_id: u128,
    start_count: u128,
    end_count: u128,
    s_clock_hash: Clock,    // todo: needs hash when use related-db
    e_clock_hash: Clock,
    merge_at: u128,
}

/// An accumulator server node. Each node maintains a UDP socket, and a set of
/// strings as its internal state.
pub struct Server {
    config: Configuration,
    index: usize,
    socket: UdpSocket,
    state: ServerState,
    running: bool,
    db: Arc<RwLock<VLCLLDb>>,
}

impl Server {
    /// Create a new server
    pub async fn new(config: &Configuration, index: usize, db: Arc<RwLock<VLCLLDb>>) -> Self {
        let s = UdpSocket::bind(config.server_addrs[index]).await.unwrap();
        Self {
            config: config.clone(),
            index,
            socket: s,
            state: ServerState::new(index.try_into().unwrap()),
            running: false,
            db,
        }
    }

    /// Handle a message
    async fn handle_msg(&mut self, msg: Message) {
        match msg {
            Message::FromClient(msg) => {
                if self.state.add(Vec::from_iter(vec![msg.item.clone()])) {
                    self.sinker_clock(msg.item.clone()).await;
                    let msg = ServerMessage::EventTrigger(EventTrigger {
                        clock_info: self.state.clock_info.clone(),
                    });
                    self.broadcast_state(msg).await;
                }
            }
            Message::FromServer(msg) => {
                println!("FromServer: {:?}, cur_clock: {:?}", msg, self.state.clock_info.clock);
                match msg {
                    ServerMessage::EventTrigger(msg) => {
                        if let Some(msg) = self.state.handle_event_trigger(msg) {
                            self.broadcast_state(msg).await;
                        }
                    }
                    ServerMessage::DiffReq(msg) => {
                        if let Some(msg) = self.state.handle_diff_req(msg, self.db.clone()) {
                            self.broadcast_state(msg).await;
                        }
                    }
                    ServerMessage::DiffRsp(msg) => {
                        // not need to broadcast
                        let merged = self.state.handle_diff_rsp(msg.clone(), self.db.clone());
                        if merged {
                            self.sinker_merge_log(&msg.from).await;
                        }
                    }
                    ServerMessage::ActiveSync(msg) => {
                        let (ret_msg, merged) = self.state.handle_active_sync(msg.clone(), self.db.clone());
                        if let Some(send_msg) = ret_msg {
                            self.broadcast_state(send_msg).await;
                        }
                        if merged {
                            self.sinker_merge_log(&msg.latest).await;
                        }
                    }
                    _ => { println!("[broadcast_state]: not support ServerMessage ")}
                }
            }
            Message::Terminate => {
                self.running = false;
            }
        }
    }

    /// sinker clock info to db
    async fn sinker_clock(&mut self, message_id: String) {
        let mut clock_infos = self.state.clock_info.clone();
        let id = self.state.id;
        let count = clock_infos.clock.get(self.state.id);
        let key = format!("{}-{}-vertex", id, count);
        let time = get_time_ms();
        let clock_info = ClockInfo {
            clock: clock_infos.clock,
            id,
            message_id: message_id.clone(),
            count,
            create_at: time,
        };
        self.db.write().unwrap().add_clock_infos(key, clock_info.clone());
        self.state.clock_to_eventid.insert(clock_info.clock.index_key(), message_id.clone());
    }

    /// sinker merge action to db
    async fn sinker_merge_log(&mut self, fclock_info: &ClockInfo) {
        let mut clock_infos = self.state.clock_info.clone();
        let to_id = self.state.id;
        let tcount = clock_infos.clock.get(self.state.id);
        let from_id = fclock_info.id;
        let mut fclock = fclock_info.clone();
        let fcount = fclock.clock.get(fclock_info.id);
        let key =  format!("{}-{}-{}-{}-edge", from_id, fcount, to_id, tcount);
        let time = get_time_ms();
        let merge_log = MergeLog {
            from_id,
            to_id,
            start_count: fcount,
            end_count: tcount,
            s_clock_hash: fclock.clock,
            e_clock_hash: clock_infos.clock,
            merge_at: time,
        };
        self.db.write().unwrap().add_merge_log(key, merge_log.clone());
    }

    /// direct send to someone node
    async fn direct_send(&mut self, fmsg: ServerMessage) {
        let mut index = 0;
        let server_msg = Message::FromServer(fmsg.clone());
        match fmsg.clone() {
            ServerMessage::DiffReq(msg) => {
                index = msg.to;
            }
            ServerMessage::DiffRsp(msg) => {
                index = msg.to;
            }
            ServerMessage::ActiveSync(msg) => {
                index = msg.to;
            }
            _ => { return }
        }

        let msg_index: usize = index.try_into().unwrap();
        if self.index != msg_index {
            self.socket
                .send_to(
                    serde_json::to_string(&server_msg).unwrap().as_bytes(),
                    self.config.server_addrs[msg_index],
                )
                .await
                .unwrap();
        }
    }

    /// Broadcast current state to all other nodes in the network.
    async fn broadcast_state(&mut self, msg: ServerMessage) {
        let server_msg = Message::FromServer(msg.clone());
        match msg {
            ServerMessage::EventTrigger(_) => {
                for i in 0..self.config.server_addrs.len() {
                    if self.index != i {
                        self.socket
                            .send_to(
                                serde_json::to_string(&server_msg).unwrap().as_bytes(),
                                self.config.server_addrs[i],
                            )
                            .await
                            .unwrap();
                    }
                }
            }
            _ => {
                self.direct_send(msg).await;
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

fn get_time_ms() -> u128 {
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    time
}

fn get_suffix<T: PartialEq + Clone>(vec: &[T], target: T) -> Option<Vec<T>> {
    let iter = vec.iter();
    let suffix: Vec<T> = iter
        .skip_while(|x| **x != target)
        .skip(1) // Skip the target value itself
        .cloned()
        .collect();

    if suffix.is_empty() {
        None
    } else {
        Some(suffix)
    }
}

#[cfg(test)]
mod tests {
    use rand::Rng;

    use super::*;

    async fn start_servers(n_server: usize) -> (Configuration, Vec<JoinHandle<Vec<String>>>) {
        let mut config = Configuration {
            server_addrs: Vec::new(),
        };
        let port = 8000 + rand::thread_rng().gen_range(0..100) * 10;
        for i in 0..n_server {
            let addr = format!("127.0.0.1:{}", port + i).parse().unwrap();
            config.server_addrs.push(addr);
        }
        let db = Arc::new(RwLock::new(VLCLLDb::new("./db", None)));
        let mut handles = Vec::new();
        for i in 0..n_server {
            let c = config.clone();
            let db_clone = db.clone(); 
            handles.push(tokio::spawn(async move {
                let mut server = Server::new(&c, i, db_clone).await;
                server.run().await;
                server.state.items
            }));
        }
        (config, handles)
    }

    async fn collect_states(handles: Vec<JoinHandle<Vec<String>>>) -> Vec<Vec<String>> {
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
        client.disseminate("hello", 0).await;
        // End test
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        terminate(&config).await;
        let states = collect_states(handles).await;
        assert!(states[0][0] == "hello");
    }

    #[tokio::test]
    async fn multi_servers() {
        // Start servers
        let (config, handles) = start_servers(10).await;
        // Run client
        let mut client = Client::new(&config).await;
        client.disseminate("hello", 0).await;
        client.disseminate("world", 0).await;
        // End test
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        terminate(&config).await;
        let states = collect_states(handles).await;
        assert!(states.iter().all(|s| s[0] == "hello"));
        assert!(states.iter().all(|s| s[1] == "world"));
        assert!(states.iter().all(|s| s.len() == 2));
    }

    #[tokio::test]
    async fn multi_servers_multi_clients() {
        let server_nums = 10;
        // Start servers
        let (config, handles) = start_servers(server_nums).await;
        // Run client
        let mut client = Client::new(&config).await;
        for i in 0..server_nums {
            let str1 = format!("{}-hello", i);
            client.disseminate(&str1, i).await;
            
            let str2 = format!("{}-world", i);
            client.disseminate(&str2, i).await;
        }
        // End test
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        terminate(&config).await;
        let states = collect_states(handles).await;
        for value in states.clone() {
            println!("{:?}", value);
        }
        assert!(states[0][0] == "0-hello");
        assert!(states[1][0] == "1-hello");
        assert!(states[8][0] == "8-hello");
        assert!(states.iter().all(|s| s.len() == 2 * server_nums));
    }

    #[tokio::test]
    async fn multi_servers_some_clients() {
        let server_nums = 50;
        let some = 2;
        // Start servers
        let (config, handles) = start_servers(server_nums).await;
        // Run client
        let mut client = Client::new(&config).await;
        for i in 0..server_nums {
            let str1 = format!("{}-hello", i);
            client.disseminate(&str1, i).await;
            
            let str2 = format!("{}-world", i);
            client.disseminate(&str2, i).await;

            if i == some {
                break;
            }
        }
        // End test
        tokio::time::sleep(time::Duration::from_millis(100)).await;
        terminate(&config).await;
        let states = collect_states(handles).await;
        for value in states.clone() {
            println!("{:?}", value);
        }
        assert!(states[0][0] == "0-hello");
        assert!(states[1][0] == "1-hello");
        assert!(states[2][0] == "2-hello");
        assert!(states.iter().all(|s| s.len() == 2 * (some+1)));
    }

    #[test]
    fn test_suffix() {
        let vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
        let target = 5;

        if let Some(suffix) = get_suffix(&vec, target) {
            assert!(suffix == vec![6, 7, 8, 9]);
            println!("Suffix after {}: {:?}", target, suffix);
        } else {
            println!("Target {} not found in the vector", target);
        }
    }
}
