//! Verifiable logical clock.
//!
//! This crate implements a verifiable logical clock construct. The clock
//! can be used in a peer-to-peer network to order events. Any node in the
//! network can verify the correctness of the clock.

use serde::{Deserialize, Serialize};
use std::cmp;

const N_SLOTS: usize = 32; // Currently, serde only supports array serialization up to 32 elements. TODO: Fix this after serde addresses the issue.
const MAX_DEPTH: usize = 128;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Clock {
    value: [u128; N_SLOTS],
    depth: usize,
}

impl PartialOrd for Clock {
    fn partial_cmp(&self, other: &Clock) -> Option<cmp::Ordering> {
        let mut ord = cmp::Ordering::Equal;
        for i in 0..N_SLOTS {
            if self.value[i] < other.value[i] {
                if ord == cmp::Ordering::Equal {
                    ord = cmp::Ordering::Less;
                } else if ord == cmp::Ordering::Greater {
                    return None;
                }
            } else if self.value[i] > other.value[i] {
                if ord == cmp::Ordering::Equal {
                    ord = cmp::Ordering::Greater;
                } else if ord == cmp::Ordering::Less {
                    return None;
                }
            }
        }
        Some(ord)
    }
}

impl Clock {
    /// Create a new clock.
    pub fn new() -> Self {
        Self {
            value: [0; N_SLOTS],
            depth: MAX_DEPTH,
        }
    }

    /// Increment the clock at the given index. Returns false if the clock
    /// is saturated.
    pub fn inc(&mut self, index: usize) -> bool {
        if self.depth > 0 {
            self.depth -= 1;
            self.value[index] |= 1 << self.depth;
            true
        } else {
            false
        }
    }

    /// Reset the clock.
    pub fn clear(&mut self) {
        self.depth = MAX_DEPTH;
        for i in 0..N_SLOTS {
            self.value[i] = 0;
        }
    }

    /// Merge the clock with another clock.
    pub fn merge(&mut self, other: &Clock) {
        self.depth = cmp::min(self.depth, other.depth);
        for i in 0..N_SLOTS {
            self.value[i] = self.value[i] | other.value[i];
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_inc() {
        let mut c = Clock::new();
        assert!(c.inc(0));
        assert!(c.inc(5));
        assert_eq!(c.value[0], 1 << (MAX_DEPTH - 1));
        assert_eq!(c.value[5], 1 << (MAX_DEPTH - 2));
    }

    #[test]
    fn clock_cmp() {
        let mut c1 = Clock::new();
        let mut c2 = Clock::new();

        assert!(c1.inc(0));
        assert!(c2.inc(0));
        assert_eq!(c1, c2);

        assert!(c1.inc(1));
        assert!(c1 > c2);

        assert!(c2.inc(2));
        assert_eq!(c1.partial_cmp(&c2), None);
    }

    #[test]
    fn clock_merge() {
        let mut c1 = Clock::new();
        let mut c2 = Clock::new();

        assert!(c1.inc(0));
        assert!(c1.inc(1));
        assert!(c2.inc(2));
        assert!(c2.inc(3));
        assert_eq!(c1.partial_cmp(&c2), None);

        let mut c3 = c1.clone();
        c3.merge(&c2);
        assert!(c3 > c1);
        assert!(c3 > c2);
    }
}
