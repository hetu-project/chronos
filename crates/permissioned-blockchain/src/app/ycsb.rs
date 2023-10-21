use std::{collections::BTreeMap, future::Future, iter::repeat_with, pin::Pin};

use bincode::Options;
use rand::{distributions::Alphanumeric, seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::Client;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Read(String),
    Scan(String, usize),
    Update(String, String),
    Insert(String, String),
    Delete(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Result {
    ReadOk(String),
    ScanOk(Vec<String>),
    UpdateOk,
    InsertOk,
    DeleteOk,
    NotFound,
    // batched?
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct App(BTreeMap<String, String>);
impl App {
    pub fn execute(&mut self, op: &[u8]) -> Vec<u8> {
        let Self(table) = self;
        let result = match bincode::options()
            .allow_trailing_bytes()
            .deserialize(op)
            .unwrap()
        {
            Op::Read(key) => {
                if let Some(value) = table.get(&key).cloned() {
                    Result::ReadOk(value)
                } else {
                    Result::NotFound
                }
            }
            Op::Scan(key, count) => {
                let values = table
                    .range(key..)
                    .map(|(_, value)| value.clone())
                    .take(count)
                    .collect();
                Result::ScanOk(values)
            }
            Op::Update(key, value) => {
                if let Some(value_mut) = table.get_mut(&key) {
                    *value_mut = value;
                    Result::UpdateOk
                } else {
                    Result::NotFound
                }
            }
            Op::Insert(key, value) => {
                table.insert(key, value); // check for override?
                Result::InsertOk
            }
            Op::Delete(key) => {
                if table.remove(&key).is_some() {
                    Result::DeleteOk
                } else {
                    Result::NotFound
                }
            }
        };
        assert_ne!(result, Result::NotFound);
        bincode::options().serialize(&result).unwrap()
    }
}

#[derive(Debug)]
pub struct Workload {
    keys: Vec<String>,
    values: Vec<String>,
    read_portion: u32,
    update_portion: u32,
    // rmw_portion: u32,
}

impl Workload {
    fn iter_strings(rng: &mut impl Rng, len: usize) -> impl Iterator<Item = String> + '_ {
        repeat_with(move || {
            rng.sample_iter(Alphanumeric)
                .take(len)
                .map(char::from)
                .collect()
        })
    }

    pub fn app(config: WorkloadConfig, rng: &mut impl Rng) -> App {
        let keys = Vec::from_iter(Self::iter_strings(rng, config.key_len).take(config.num_key));
        let entries = keys
            .into_iter()
            .zip(Self::iter_strings(rng, config.value_len))
            .collect();
        App(entries)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkloadConfig {
    pub num_key: usize,
    pub num_value: usize,
    pub key_len: usize,
    pub value_len: usize,
    pub read_portion: u32,
    pub update_portion: u32,
    pub rmw_portion: u32,
}

impl From<control_messages::YcsbConfig> for WorkloadConfig {
    fn from(value: control_messages::YcsbConfig) -> Self {
        let control_messages::YcsbConfig {
            num_key,
            num_value,
            key_len,
            value_len,
            read_portion,
            update_portion,
            rmw_portion,
        } = value;
        Self {
            num_key,
            num_value,
            key_len,
            value_len,
            read_portion,
            update_portion,
            rmw_portion,
        }
    }
}

impl Workload {
    pub fn new(config: WorkloadConfig, rng: &mut impl Rng) -> Self {
        let keys = Self::iter_strings(rng, config.key_len)
            .take(config.num_key)
            .collect();
        let values = Self::iter_strings(rng, config.value_len)
            .take(config.num_value)
            .collect();
        assert_eq!(
            config.read_portion + config.update_portion + config.rmw_portion,
            100
        );
        Self {
            keys,
            values,
            read_portion: config.read_portion,
            update_portion: config.update_portion,
            // rmw_portion,
        }
    }

    pub fn generate(
        &self,
        client: impl Client + Send + Sync + 'static,
        rng: &mut impl Rng,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        async fn invoke(client: &(impl Client + Send + Sync), op: Vec<u8>) {
            let finish = CancellationToken::new();
            client.invoke(op, {
                let finish = finish.clone();
                move |_| finish.cancel()
            });
            finish.cancelled().await
        }
        let serialize = |op| bincode::options().serialize(&op).unwrap();

        let txn_type = rng.gen_range(0..100);
        if txn_type < self.read_portion {
            // TODO zipf distribution
            let op = serialize(Op::Read(self.keys.choose(rng).unwrap().clone()));
            Box::pin(async move { invoke(&client, op).await })
        } else if txn_type < self.read_portion + self.update_portion {
            let op = serialize(Op::Update(
                self.keys.choose(rng).unwrap().clone(),
                self.values.choose(rng).unwrap().clone(),
            ));
            Box::pin(async move { invoke(&client, op).await })
        } else {
            let key = self.keys.choose(rng).unwrap();
            let value = self.values.choose(rng).unwrap();
            let op1 = serialize(Op::Read(key.clone()));
            let op2 = serialize(Op::Update(key.clone(), value.clone()));
            Box::pin(async move {
                invoke(&client, op1).await;
                invoke(&client, op2).await
            })
        }
    }
}
