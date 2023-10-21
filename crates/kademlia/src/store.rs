use std::time::{Duration, SystemTime};

use neat::context::{
    crypto::{Signed, VerifyingKey},
    Addr,
};
use primitive_types::U256;
use serde::{Deserialize, Serialize};

use crate::PeerId;

pub fn distance(peer_id: &PeerId, target: &PeerId) -> U256 {
    U256::from_big_endian(peer_id) ^ U256::from_big_endian(target)
}

#[derive(Debug, Clone)]
pub struct Store {
    peer_id: PeerId,
    buckets: Vec<KBucket>,
    // sibling list?
    pub bucket_size: usize,
    expire_duration: Duration,
}

#[derive(Debug, Clone, Default)]
struct KBucket {
    id: U256,
    id_mask: U256,
    records: Vec<Signed<PeerRecord>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PeerRecord {
    pub id: PeerId,
    pub verifying_key: VerifyingKey,
    pub addr: Addr,
    pub last_active: SystemTime,
}

impl Store {
    pub fn new(peer_id: PeerId, bucket_size: usize, expire_duration: Duration) -> Self {
        let mut buckets = Vec::new();
        let invert_id = !U256::from_big_endian(&peer_id);
        let mut invert_mask = !U256::zero();
        for _ in 0..256 {
            invert_mask >>= 1;
            buckets.push(KBucket {
                id: invert_id & !invert_mask,
                id_mask: !invert_mask,
                records: Default::default(),
            })
        }
        buckets.reverse();
        Self {
            peer_id,
            buckets,
            bucket_size,
            expire_duration,
        }
    }

    pub fn insert(&mut self, record: Signed<PeerRecord>) {
        let bucket_index = distance(&record.id, &self.peer_id).bits();
        let bucket = &mut self.buckets[bucket_index];
        assert!(Self::fit_in(&record.id, bucket));
        for bucket_record in &mut bucket.records {
            if bucket_record.id == record.id {
                if record.last_active > bucket_record.last_active {
                    *bucket_record = record
                }
                return;
            }
        }
        if bucket.records.len() == self.bucket_size {
            // if Self::fit_in(&self.peer_id, bucket) {
            //     // split
            // } else {
            assert!(!Self::fit_in(&self.peer_id, bucket));

            // evict
            let (index, least_recent) = bucket
                .records
                .iter()
                .enumerate()
                .min_by_key(|(_, record)| record.last_active)
                .unwrap();
            if least_recent
                .last_active
                .elapsed()
                .ok()
                .filter(|&duration| duration >= self.expire_duration)
                .is_some()
            {
                bucket.records.remove(index);
            } else {
                return;
            }
            // }
        }
        assert!(bucket.records.len() < self.bucket_size);
        bucket.records.push(record)
    }

    fn fit_in(peer_id: &PeerId, bucket: &KBucket) -> bool {
        (U256::from_big_endian(peer_id) & bucket.id_mask) == bucket.id
    }

    pub fn closest(&self, target: &PeerId, num_peer: usize) -> Vec<Signed<PeerRecord>> {
        let bucket_index = distance(target, &self.peer_id).bits();
        assert!(Self::fit_in(target, &self.buckets[bucket_index]));
        let mut candidates = Vec::new();
        for bucket in self.buckets[..=bucket_index]
            .iter()
            .rev()
            .chain(&self.buckets[bucket_index + 1..])
        {
            let mut records = bucket.records.clone();
            records.sort_by_key(|record| distance(&record.id, target));
            candidates.extend(records);
            if candidates.len() >= num_peer {
                break;
            }
        }
        candidates.truncate(num_peer);
        candidates
    }
}
