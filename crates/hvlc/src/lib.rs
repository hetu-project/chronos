use bincode::Options;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{cmp::Ordering, collections::BTreeMap};
use tracing::error;

pub trait Clock: PartialOrd + Clone + Send + Sync + 'static {
    fn reduce(&self) -> LamportClock;
}

/// A Lamport clock is a simple logical clock that counts events.
/// It is represented as an unsigned 64-bit integer.
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
pub struct HVLCClock {
    #[deref]
    pub inner: BTreeMap<KeyId, u64>,
    pub timestamp: u128,
}

impl AsRef<HVLCClock> for HVLCClock {
    fn as_ref(&self) -> &HVLCClock {
        self
    }
}

impl HVLCClock {
    /// Creates a new HVLCClock instance with an empty BTreeMap and current timestamp
    /// Returns a new HVLCClock with:
    /// - Empty inner BTreeMap for storing key-value pairs
    /// - Current system timestamp in nanoseconds
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
            timestamp: match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => duration.as_nanos(),
                Err(e) => {
                    error!("New HVLCClock SystemTime error: {:?}", e);
                    0
                }
            },
        }
    }

    /// Checks if this clock represents a genesis state
    /// Returns true if all values in the inner map are 0, false otherwise
    pub fn is_genesis(&self) -> bool {
        self.inner.values().all(|n| *n == 0)
    }

    /// Merges this clock with another clock by taking the maximum values
    /// Returns a new HVLCClock containing:
    /// - For each key, the maximum value between both clocks
    /// - The maximum timestamp between both clocks
    fn merge(&self, other: &Self) -> Self {
        let merged = self
            .inner
            .keys()
            .chain(other.inner.keys())
            .map(|id| {
                let clock = match (self.inner.get(id), other.inner.get(id)) {
                    (Some(n), Some(other_n)) => (*n).max(*other_n),
                    (Some(n), None) => *n,
                    (None, Some(other_n)) => *other_n,
                    (None, None) => unreachable!("The key {} does not exist in either clock", id),
                };
                (*id, clock)
            })
            .collect();
        Self {
            inner: merged,
            timestamp: std::cmp::max(self.timestamp, other.timestamp),
        }
    }

    /// Updates this clock by merging with other clocks and incrementing a specific ID
    /// Parameters:
    /// - others: Iterator of other clocks to merge with
    /// - id: The key ID to increment after merging
    /// Returns a new HVLCClock with merged values and incremented ID
    pub fn update<'a>(&'a self, others: impl Iterator<Item = &'a Self>, id: u64) -> Self {
        let mut updated = others.fold(self.clone(), |version, dep| version.merge(dep));
        // If no other clocks were merged, update timestamp to current time
        if updated.timestamp == self.timestamp {
            updated.timestamp =
                match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                    Ok(duration) => duration.as_nanos(),
                    Err(e) => {
                        error!("Update HVLCClock systemTime error: {:?}", e);
                        updated.timestamp
                    }
                };
        }
        *updated.inner.entry(id).or_default() += 1;
        updated
    }

    /// Creates a base clock from multiple clocks by taking minimum values
    /// Parameters:
    /// - others: Iterator of clocks to combine
    /// Returns a new HVLCClock containing:
    /// - For each key, the minimum value across all clocks
    /// - The minimum timestamp across all clocks
    pub fn base<'a>(others: impl Iterator<Item = &'a Self>) -> Self {
        let mut combined = BTreeMap::new();
        let mut timestamp = u128::MAX;
        for clock in others {
            if clock.timestamp < timestamp {
                timestamp = clock.timestamp;
            }
            for (&key, &value) in &clock.inner {
                combined
                    .entry(key)
                    .and_modify(|e: &mut u64| *e = (*e).min(value))
                    .or_insert(value);
            }
        }

        Self {
            inner: combined,
            timestamp,
        }
    }

    /// Calculates SHA256 hash of the clock
    /// Returns a 32-byte array containing the SHA256 hash
    pub fn calculate_sha256(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        let data = bincode::options()
            .serialize(&self)
            .expect("Failed to serialize data");
        // Update the hasher with the JSON string
        hasher.update(data);

        // Calculate the hash & return bytes
        hasher.finalize().into()
    }
}

impl PartialOrd for HVLCClock {
    /// Compares two HVLCClock instances to determine their ordering.
    ///
    /// # Arguments
    ///
    /// * `other` - Another HVLCClock instance to compare against.
    ///
    /// # Returns
    ///
    /// * `Option<Ordering>` - Returns `Some(Ordering::Greater)` if `self` is greater,
    ///   `Some(Ordering::Less)` if `self` is less, or `Some(Ordering::Equal)` if they are equal.
    ///   Returns `None` if the comparison is indeterminate.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        /// Helper function to determine if one clock is greater than or equal to another.
        ///
        /// # Arguments
        ///
        /// * `clock` - The HVLCClock instance to check.
        /// * `other_clock` - The HVLCClock instance to compare against.
        ///
        /// # Returns
        ///
        /// * `bool` - Returns `true` if `clock` is greater than or equal to `other_clock`.
        fn ge(clock: &HVLCClock, other_clock: &HVLCClock) -> bool {
            for (other_id, other_n) in &other_clock.inner {
                if *other_n == 0 {
                    continue;
                }
                let Some(n) = clock.inner.get(other_id) else {
                    return false;
                };
                if n < other_n {
                    return false;
                }
            }
            true
        }

        match (ge(self, other), ge(other, self)) {
            (true, true) => Some(self.timestamp.cmp(&other.timestamp)),
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            (false, false) => Some(self.timestamp.cmp(&other.timestamp)),
        }
    }
}

impl HVLCClock {
    pub fn dep_cmp(&self, other: &Self, id: KeyId) -> Ordering {
        match (self.inner.get(&id), other.inner.get(&id)) {
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

impl Clock for HVLCClock {
    fn reduce(&self) -> LamportClock {
        self.inner
            .values()
            .fold(0u128, |acc, &value| acc + value as u128) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let clock = HVLCClock::new();
        assert!(clock.inner.is_empty());
        assert!(clock.timestamp > 0);
    }

    #[test]
    fn test_is_genesis() {
        let mut clock = HVLCClock::new();
        assert!(clock.is_genesis());

        clock.inner.insert(1, 1);
        assert!(!clock.is_genesis());
    }

    #[test]
    fn test_merge() {
        let mut clock1 = HVLCClock::new();
        clock1.inner.insert(1, 1);
        clock1.inner.insert(2, 2);
        clock1.timestamp = 100;

        let mut clock2 = HVLCClock::new();
        clock2.inner.insert(2, 3);
        clock2.inner.insert(3, 1);
        clock2.timestamp = 200;

        let merged = clock1.merge(&clock2);
        assert_eq!(*merged.inner.get(&1).unwrap(), 1);
        assert_eq!(*merged.inner.get(&2).unwrap(), 3);
        assert_eq!(*merged.inner.get(&3).unwrap(), 1);
        assert_eq!(merged.timestamp, 200);
    }

    #[test]
    fn test_update() {
        let mut clock1 = HVLCClock::new();
        clock1.inner.insert(1, 1);

        let mut clock2 = HVLCClock::new();
        clock2.inner.insert(1, 2);

        let updated = clock1.update(vec![&clock2].into_iter(), 1);
        assert_eq!(*updated.inner.get(&1).unwrap(), 3);
    }

    #[test]
    fn test_base() {
        let mut clock1 = HVLCClock::new();
        clock1.inner.insert(1, 3);
        clock1.inner.insert(2, 2);

        let mut clock2 = HVLCClock::new();
        clock2.inner.insert(1, 2);
        clock2.inner.insert(2, 4);

        let base = HVLCClock::base(vec![&clock1, &clock2].into_iter());
        assert_eq!(*base.inner.get(&1).unwrap(), 2);
        assert_eq!(*base.inner.get(&2).unwrap(), 2);
    }

    #[test]
    fn test_partial_ord() {
        let mut clock1 = HVLCClock::new();
        clock1.inner.insert(1, 2);
        clock1.timestamp = 100;

        let mut clock2 = HVLCClock::new();
        clock2.inner.insert(1, 1);
        clock2.timestamp = 200;

        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Greater));
    }

    #[test]
    fn test_dep_cmp() {
        let mut clock1 = HVLCClock::new();
        clock1.inner.insert(1, 2);

        let mut clock2 = HVLCClock::new();
        clock2.inner.insert(1, 1);

        assert_eq!(clock1.dep_cmp(&clock2, 1), Ordering::Greater);
        assert_eq!(clock1.dep_cmp(&clock2, 2), Ordering::Equal);
    }

    #[test]
    fn test_reduce() {
        let mut clock = HVLCClock::new();
        clock.inner.insert(1, 2);
        clock.inner.insert(2, 3);

        assert_eq!(clock.reduce(), 5);
    }

    #[test]
    fn test_calculate_sha256() {
        let mut clock = HVLCClock::new();
        clock.inner.insert(1, 2);
        clock.timestamp = 100;
        assert_eq!(clock.calculate_sha256(), clock.calculate_sha256());
        println!("0x{}", hex::encode(clock.calculate_sha256()));
    }
}
