use std::{future::Future, pin::Pin};

use rand::Rng;
use tokio_util::sync::CancellationToken;

use crate::Client;

pub mod ycsb;

#[derive(Debug, Clone)]
pub enum App {
    Null,
    Ycsb(ycsb::App),
}

impl App {
    pub fn execute(&mut self, op: &[u8]) -> Vec<u8> {
        match self {
            Self::Null => Default::default(),
            Self::Ycsb(app) => app.execute(op),
        }
    }
}

#[derive(Debug)]
pub enum Workload {
    Null,
    Ycsb(ycsb::Workload),
}

impl Workload {
    pub fn generate(
        &self,
        client: impl Client + Send + Sync + 'static,
        rng: &mut impl Rng,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + Sync>> {
        match self {
            Self::Null => Box::pin(async move {
                let finish = CancellationToken::new();
                client.invoke(Default::default(), {
                    let finish = finish.clone();
                    move |_| finish.cancel()
                });
                finish.cancelled().await
            }),
            Self::Ycsb(workload) => workload.generate(client, rng),
        }
    }
}
