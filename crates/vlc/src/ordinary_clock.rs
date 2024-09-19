//! This clock use the BTreeMap as its core data structure.

use std::{cmp::Ordering, collections::BTreeMap};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use bincode::Options;

pub trait Clock: PartialOrd + Clone + Send + Sync + 'static {
    fn reduce(&self) -> LamportClock;
}

pub type LamportClock = u64;

impl Clock for LamportClock {
    fn reduce(&self) -> LamportClock {
        *self
    }
}

/// clock key_id 
pub type KeyId = u64;

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, Default, derive_more::Deref, Serialize, Deserialize,
)]
pub struct OrdinaryClock(pub BTreeMap<KeyId, u64>);

impl AsRef<OrdinaryClock> for OrdinaryClock {
    fn as_ref(&self) -> &OrdinaryClock {
        self
    }
}

impl OrdinaryClock {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_genesis(&self) -> bool {
        self.0.values().all(|n| *n == 0)
    }

    fn merge(&self, other: &Self) -> Self {
        let merged = self
            .0
            .keys()
            .chain(other.0.keys())
            .map(|id| {
                let n = match (self.0.get(id), other.0.get(id)) {
                    (Some(n), Some(other_n)) => (*n).max(*other_n),
                    (Some(n), None) | (None, Some(n)) => *n,
                    (None, None) => unreachable!(),
                };
                (*id, n)
            })
            .collect();
        Self(merged)
    }

    pub fn update<'a>(&'a self, others: impl Iterator<Item = &'a Self>, id: u64) -> Self {
        let mut updated = others.fold(self.clone(), |version, dep| version.merge(dep));
        *updated.0.entry(id).or_default() += 1;
        updated
    }

    pub fn base<'a>(others: impl Iterator<Item = &'a Self>) -> Self {
        let mut combined = BTreeMap::new();

        for clock in others {
            for (&key, &value) in &clock.0 {
                combined
                    .entry(key)
                    .and_modify(|e: &mut u64| *e = (*e).min(value))
                    .or_insert(value);
            }
        }

        OrdinaryClock(combined)
    }

    pub fn calculate_sha256(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        let data = bincode::options().serialize(&self.0).expect("Failed to serialize data");
        // Update the hasher with the JSON string
        hasher.update(data);

        // Calculate the hash & return bytes
        hasher.finalize().into()
    }
}

impl PartialOrd for OrdinaryClock {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        fn ge(clock: &OrdinaryClock, other_clock: &OrdinaryClock) -> bool {
            for (other_id, other_n) in &other_clock.0 {
                if *other_n == 0 {
                    continue;
                }
                let Some(n) = clock.0.get(other_id) else {
                    return false;
                };
                if n < other_n {
                    return false;
                }
            }
            true
        }
        match (ge(self, other), ge(other, self)) {
            (true, true) => Some(Ordering::Equal),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => None,
        }
    }
}

impl OrdinaryClock {
    pub fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering {
        match (self.0.get(&id), other.0.get(&id)) {
            // disabling this check after the definition of genesis clock has been extended
            // haven't revealed any bug with this assertion before, hopefully disabling it will not
            // hide any bug in the future as well
            (None, Some(_)) => Ordering::Less,
            (Some(_), None) => Ordering::Greater,
            // this can happen on the startup insertion
            (None, None) => Ordering::Equal,
            (Some(n), Some(m)) => n.cmp(m),
        }
    }
}

impl Clock for OrdinaryClock {
    fn reduce(&self) -> LamportClock {
        self.0.values().copied().sum()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::{atomic::{AtomicUsize, Ordering}, Arc}, time::{Duration, Instant}};
    use rand::rngs::OsRng;
    use futures::future::join_all;
    use tokio::runtime::Builder;
    use crypto::{core::DigestHash, recovery::{recover_public_key, sign_message_recover_pk}};


    #[test]
    fn default_is_genesis() -> anyhow::Result<()> {
        anyhow::ensure!(OrdinaryClock::default().is_genesis());
        Ok(())
    }

    #[test]
    fn test_clock_base_func() -> anyhow::Result<()> {
        let mut clock1 = BTreeMap::new();
        clock1.insert(1, 10);
        clock1.insert(2, 0);
        clock1.insert(3, 5);
    
        let mut clock2 = BTreeMap::new();
        clock2.insert(1, 0);
        clock2.insert(2, 20);
        clock2.insert(3, 2);
    
        let mut clock3 = BTreeMap::new();
        clock3.insert(1, 7);
        clock3.insert(2, 15);
        clock3.insert(4, 8);
    
        let oc1 = OrdinaryClock(clock1);
        let oc2 = OrdinaryClock(clock2);
        let oc3 = OrdinaryClock(clock3);
    
        let clocks = vec![&oc1, &oc2, &oc3];
        let base_clock = OrdinaryClock::base(clocks.into_iter());
        println!("{:?}", base_clock); // Should print: OrdinaryClock({1: 0, 2: 0, 3: 2, 4: 8})
        assert_eq!(base_clock, OrdinaryClock(BTreeMap::from([(1, 0), (2, 0), (3, 2), (4, 8)])));
        Ok(())
    }

    #[test]
    fn clock_sha256() -> anyhow::Result<()> {
        let mut clock = OrdinaryClock((0..4).map(|i| (i as _, 0)).collect());
        clock = clock.update(vec![OrdinaryClock::default()].iter(), 0);
        println!("{:?}, {:?}", clock, clock.calculate_sha256());
        
        // Tips: when clock is hashmap, this serialize and sha256 can't reproduce, every time is different.
        Ok(())
    }

    #[test]
    #[ignore]
    fn hash_big_clock_sha256() -> anyhow::Result<()> {
        let clock = OrdinaryClock((0..1<<27).map(|i| (i as _, 0)).collect());
        let start_time = Instant::now();
        let clock_hash = clock.sha256().to_fixed_bytes();
        println!("{:?}, {:?}", clock_hash, start_time.elapsed());
        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn stress_raw_update() -> anyhow::Result<()> {
        for size in (0..=12).step_by(2).map(|n| 1 << n) {
            let num_merged = 0;
            let clock = OrdinaryClock((0..size).map(|i| (i as _, 0)).collect());

            let mut count = 0;
            let start_time = Instant::now();
            let close_loops_session = async {
                let mut current_clock = clock.clone();
                loop {
                    if start_time.elapsed() >= Duration::from_secs(10) {
                        break;
                    }

                    let updated_clock = current_clock.update(vec![clock.clone(); num_merged].iter(), 0);
                    count += 1;
                    current_clock = updated_clock;
                }
                anyhow::Ok(())
            };

            close_loops_session.await?;
            println!(
                "key {size},merged {num_merged}, tps {}",
                count as f32 / 10.
            );
        }
        Ok(())
    }
    
    #[tokio::test]
    #[ignore]
    async fn stress_raw_update_concurrency() -> anyhow::Result<()> {
        let core = num_cpus::get();
        let rt = Arc::new(Builder::new_multi_thread()
            .worker_threads(core)
            .build()
            .unwrap());

        for size in (0..=12).step_by(2).map(|n| 1 << n) {
            let count = Arc::new(AtomicUsize::new(0));
            let mut tasks = Vec::new();
            let mut shifts: Vec<i32> = Vec::with_capacity(core);
            for _ in 0..core {
                shifts.push(size);
            }
            for size in shifts {
                let num_merged = 0;
                let clock = OrdinaryClock((0..size).map(|i| (i as _, 0)).collect());
        
                let count_clone = Arc::clone(&count);
                let start_time = Instant::now();
                let close_loops_session = async move {
                    // different clocks in different threads
                    let mut current_clock = clock.clone();
                    loop {
                        if start_time.elapsed() >= Duration::from_secs(10) {
                            break;
                        }
        
                        let updated_clock = current_clock.update(vec![clock.clone(); num_merged].iter(), 0);
                        count_clone.fetch_add(1, Ordering::Relaxed);
                        current_clock = updated_clock;
                    }
                    current_clock
                };
                tasks.push(rt.spawn(close_loops_session));
            }
            let results = join_all(tasks).await;
            for result in results {
                let clock = result?;
                println!("key: {}, clock: {:?}", size, clock.0.get(&0));
            }
    
            println!(
                "key {}, merged 0, tps {}",
                size,
                count.load(Ordering::Relaxed) as f32 / 10.
            );
        }

        // Shutdown Runtime
        Arc::try_unwrap(rt).unwrap().shutdown_background();

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn stress_verify_update() -> anyhow::Result<()> {
        use DigestHash as _;

        let secp = secp256k1::Secp256k1::new();
        let (secret_key, public_key) = secp.generate_keypair(&mut OsRng);
        
        for size in (0..=12).step_by(2).map(|n| 1 << n) {
            let num_merged = 0;
            let clock = OrdinaryClock((0..size).map(|i| (i as _, 0)).collect());
            let clock_hash = clock.sha256().to_fixed_bytes();
            let mut count = 0;
            
            // sign once
            let signature_recover = sign_message_recover_pk(&secp, &secret_key, &clock.sha256().to_fixed_bytes()).unwrap();

            let start_time = Instant::now();
            let close_loops_session = async {
                let mut current_clock = clock.clone();
                loop {
                    if start_time.elapsed() >= Duration::from_secs(10) {
                        break;
                    }
                    
                    // verify
                    let recover_pubkey = recover_public_key(&secp, &signature_recover, &clock_hash).unwrap();
                    assert_eq!(recover_pubkey, public_key);

                    // update
                    let updated_clock = current_clock.update(vec![clock.clone(); num_merged].iter(), 0);
                    count += 1;
                    current_clock = updated_clock;
                }
                anyhow::Ok(())
            };

            close_loops_session.await?;
            println!(
                "key {size},merged {num_merged}, tps {}",
                count as f32 / 10.
            );
        }
        Ok(())
    }
    
    #[tokio::test]
    #[ignore]
    async fn stress_signature_verify_update() -> anyhow::Result<()> {
        use DigestHash as _;

        let secp = secp256k1::Secp256k1::new();
        let (secret_key, public_key) = secp.generate_keypair(&mut OsRng);
        
        for size in (0..=12).step_by(2).map(|n| 1 << n) {
            let num_merged = 0;
            let clock = OrdinaryClock((0..size).map(|i| (i as _, 0)).collect());

            let mut count = 0;
            let mut signatures = None;
            let start_time = Instant::now();
            let close_loops_session = async {
                let mut current_clock = clock.clone();
                loop {
                    if start_time.elapsed() >= Duration::from_secs(10) {
                        break;
                    }
                    
                    // verify
                    if !signatures.is_none() {
                        let clock_hash = current_clock.sha256().to_fixed_bytes();
                        let recover_pubkey = recover_public_key(&secp, &signatures.unwrap(), &clock_hash).unwrap();
                        assert_eq!(recover_pubkey, public_key);
                    }

                    // update
                    let updated_clock = current_clock.update(vec![clock.clone(); num_merged].iter(), 0);
                    count += 1;
                    current_clock = updated_clock;
                    
                    // sign
                    let signature_recover = sign_message_recover_pk(&secp, &secret_key, &current_clock.sha256().to_fixed_bytes());
                    signatures = Some(signature_recover.unwrap());
                }
                anyhow::Ok(())
            };

            close_loops_session.await?;
            println!(
                "key {size},merged {num_merged}, tps {}",
                count as f32 / 10.
            );
        }
        Ok(())
    }
    
}