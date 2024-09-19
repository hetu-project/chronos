pub mod nitro_clock;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Clocked<M, C> {
    pub clock: C,
    pub inner: M,
}

pub trait Verify<S>: Send + Sync + 'static {
    fn verify_clock(&self, num_faulty: usize, state: &S) -> anyhow::Result<()>;
}