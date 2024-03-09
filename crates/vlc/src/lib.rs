//! Verifiable logical clock.
//!
//! This crate implements a verifiable logical clock construct. The clock
//! can be used in a peer-to-peer network to order events. Any node in the
//! network can verify the correctness of the clock.

use serde::{Deserialize, Serialize};
use std::cmp;
use std::collections::HashMap;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Clock {
    values: HashMap<u128, u128>,
}

impl PartialOrd for Clock {
    fn partial_cmp(&self, other: &Clock) -> Option<cmp::Ordering> {
        let mut less = false;
        let mut greater = false;

        for (id, value) in &self.values {
            let other_value = other.values.get(id);
            if other_value.is_none() || value > other_value.unwrap() {
                greater = true;
            } else if value < other_value.unwrap() {
                less = true;
            }
        }

        for (id, _) in &other.values {
            if self.values.get(id).is_none() {
                less = true;
            }
        }

        if less && greater {
            None
        } else if less {
            Some(cmp::Ordering::Less)
        } else if greater {
            Some(cmp::Ordering::Greater)
        } else {
            Some(cmp::Ordering::Equal)
        }
    }
}

impl Clock {
    /// Create a new clock.
    pub fn new() -> Self {
        Self {
            values: HashMap::new(),
        }
    }

    /// Increment the clock
    pub fn inc(&mut self, id: u128) {
        let value = self.values.entry(id).or_insert(0);
        *value += 1;
    }

    /// Reset the clock.
    pub fn clear(&mut self) {
        self.values.clear();
    }

    /// Merge the clock with other clocks.
    pub fn merge(&mut self, others: &Vec<&Clock>) {
        for &clock in others {
            for (id, value) in &clock.values {
                let v = self.values.entry(*id).or_insert(0);
                *v = std::cmp::max(*v, *value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_inc() {
        let mut c = Clock::new();
        c.inc(0);
        c.inc(0);
        assert_eq!(c.values.get(&0), Some(&2));
    }

    #[test]
    fn clock_cmp() {
        let mut c1 = Clock::new();
        c1.inc(0);
        let c2 = c1.clone();
        let mut c3 = Clock::new();
        c3.inc(1);

        assert_eq!(c1, c2);
        assert_eq!(c1.partial_cmp(&c3), None);
        assert_eq!(c2.partial_cmp(&c3), None);

        c1.inc(0);
        assert_eq!(c2.partial_cmp(&c1), Some(cmp::Ordering::Less));
        assert_eq!(c3.partial_cmp(&c1), None);
    }

    #[test]
    fn clock_merge() {
        let mut c1 = Clock::new();
        c1.inc(0);
        let mut c2 = Clock::new();
        c2.inc(1);
        let mut c3 = Clock::new();
        c3.inc(2);

        assert_eq!(c1.partial_cmp(&c2), None);
        assert_eq!(c1.partial_cmp(&c3), None);
        assert_eq!(c2.partial_cmp(&c3), None);

        c1.merge(&vec![&c2, &c3]);
        assert_eq!(c2.partial_cmp(&c1), Some(cmp::Ordering::Less));
        assert_eq!(c1.partial_cmp(&c2), Some(cmp::Ordering::Greater));
        assert_eq!(c3.partial_cmp(&c1), Some(cmp::Ordering::Less));
        assert_eq!(c1.partial_cmp(&c3), Some(cmp::Ordering::Greater));
    }
}
