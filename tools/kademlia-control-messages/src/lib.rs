use std::net::IpAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]

pub struct Config {
    pub seed: u64,
    pub num_host_peer: usize,
    pub hosts: Vec<IpAddr>,
    pub index: (usize, usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FindPeer {
    pub target: [u8; 32],
    pub count: usize,
}
