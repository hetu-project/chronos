//! Verifiable logical clock.
//!
//! This crates implements a verifiable logical clock construct. The clock
//! can be used in a peer-to-peer network to order events. Any node in the
//! network can verify the correctness of the clock.

use std::cmp;

const N_SLOTS: usize = 128;
const MAX_DEPTH: usize = 128;

#[derive(PartialEq, Debug)]
pub enum ClockCompare {
    Before,
    After,
    Equal,
    Concurrent,
}

#[derive(Clone)]
pub struct Clock {
    value: [u128; N_SLOTS],
    depth: usize,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            value: [0; N_SLOTS],
            depth: MAX_DEPTH,
        }
    }

    pub fn inc(&mut self, index: usize) -> bool {
        if self.depth > 0 {
            self.depth -= 1;
            self.value[index] |= 1 << self.depth;
            true
        } else {
            false
        }
    }

    pub fn clear(&mut self) {
        self.depth = MAX_DEPTH;
        for i in 0..N_SLOTS {
            self.value[i] = 0;
        }
    }

    pub fn compare(&self, other: &Clock) -> ClockCompare {
        let mut comp = ClockCompare::Equal;
        for i in 0..N_SLOTS {
            if self.value[i] < other.value[i] {
                if comp == ClockCompare::Equal {
                    comp = ClockCompare::Before;
                } else if comp == ClockCompare::After {
                    return ClockCompare::Concurrent;
                }
            } else if self.value[i] > other.value[i] {
                if comp == ClockCompare::Equal {
                    comp = ClockCompare::After;
                } else if comp == ClockCompare::Before {
                    return ClockCompare::Concurrent;
                }
            }
        }
        return comp;
    }

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
        assert_eq!(c1.compare(&c2), ClockCompare::Equal);

        assert!(c1.inc(1));
        assert_eq!(c1.compare(&c2), ClockCompare::After);
        assert_eq!(c2.compare(&c1), ClockCompare::Before);

        assert!(c2.inc(2));
        assert_eq!(c1.compare(&c2), ClockCompare::Concurrent);
        assert_eq!(c2.compare(&c1), ClockCompare::Concurrent);
    }

    #[test]
    fn clock_merge() {
        let mut c1 = Clock::new();
        let mut c2 = Clock::new();

        assert!(c1.inc(0));
        assert!(c1.inc(1));
        assert!(c2.inc(2));
        assert!(c2.inc(3));
        assert_eq!(c1.compare(&c2), ClockCompare::Concurrent);

        let mut c3 = c1.clone();
        c3.merge(&c2);
        assert_eq!(c3.compare(&c1), ClockCompare::After);
        assert_eq!(c3.compare(&c2), ClockCompare::After);
    }
}
