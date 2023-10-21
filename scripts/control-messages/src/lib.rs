use std::{net::SocketAddr, time::Duration};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub mode: String,
    pub app: App,
    pub client_addrs: Vec<SocketAddr>,
    pub replica_addrs: Vec<SocketAddr>,
    pub multicast_addr: SocketAddr,
    pub num_faulty: usize,
    pub drop_rate: f64,
    pub seed: u64,
    pub role: Role,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum App {
    Null,
    Ycsb(YcsbConfig),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct YcsbConfig {
    pub num_key: usize,
    pub num_value: usize,
    pub key_len: usize,
    pub value_len: usize,
    pub read_portion: u32,
    pub update_portion: u32,
    pub rmw_portion: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Role {
    BenchmarkClient(BenchmarkClient),
    Replica(Replica),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BenchmarkClient {
    pub num_group: usize,
    pub num_client: usize, // per group
    pub offset: usize,
    pub duration: Duration,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Replica {
    //
    pub index: u8,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BenchmarkStats {
    pub throughput: f32,
    pub average_latency: Option<Duration>,
}
