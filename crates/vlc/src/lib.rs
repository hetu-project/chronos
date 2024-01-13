//! Verifiable logical clock.
//!
//! This crate implements a verifiable logical clock construct. The clock
//! can be used in a peer-to-peer network to order events. Any node in the
//! network can verify the correctness of the clock.

use serde::{Deserialize, Serialize};
use std::cmp;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct Clock {
    id: u128,
    value: u128,
    ancestors: Vec<Clock>,
}

impl PartialOrd for Clock {
    fn partial_cmp(&self, other: &Clock) -> Option<cmp::Ordering> {
        if self.id == other.id {
            return self.value.partial_cmp(&other.value);
        } else {
            // If current clock is <= to any of the other clock's ancestors,
            // the clock is ordered before the other clock.
            for anc in &other.ancestors {
                match self.partial_cmp(anc) {
                    Some(cmp::Ordering::Less) | Some(cmp::Ordering::Equal) => {
                        return Some(cmp::Ordering::Less)
                    }
                    _ => (),
                }
            }
            // Do the same check with the reverse direction
            for anc in &self.ancestors {
                match other.partial_cmp(anc) {
                    Some(cmp::Ordering::Less) | Some(cmp::Ordering::Equal) => {
                        return Some(cmp::Ordering::Greater)
                    }
                    _ => (),
                }
            }
        }
        None
    }
}

impl Clock {
    /// Create a new clock.
    pub fn new(id: u128) -> Self {
        Self {
            id,
            value: 0,
            ancestors: Vec::new(),
        }
    }

    /// Create a new clock that extends other clocks.
    pub fn create(id: u128, ancestors: &Vec<Clock>) -> Self {
        Self {
            id,
            value: 0,
            ancestors: ancestors.clone(),
        }
    }

    /// Increment the clock
    pub fn inc(&mut self) {
        // If clock value overflows, panic
        assert_ne!(self.value.checked_add(1), None);
        self.value += 1;
    }

    /// Reset the clock.
    pub fn clear(&mut self) {
        self.value = 0;
        self.ancestors.clear();
    }

    /// Merge the clock with other clocks.
    pub fn merge(&mut self, others: &Vec<&Clock>) {
        self.inc();
        for &clock in others {
            // TODO: duplicate detection
            self.ancestors.push(clock.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_inc() {
        let mut c = Clock::new(1);
        c.inc();
        c.inc();
        assert_eq!(c.value, 2);
    }

    #[test]
    fn clock_cmp() {
        let mut c1 = Clock::new(1);
        let c2 = c1.clone();
        let c3 = Clock::new(2);

        assert_eq!(c1, c2);
        assert_eq!(c1.partial_cmp(&c3), None);
        assert_eq!(c2.partial_cmp(&c3), None);

        c1.inc();
        assert_eq!(c2.partial_cmp(&c1), Some(cmp::Ordering::Less));
        assert_eq!(c3.partial_cmp(&c1), None);
    }

    #[test]
    fn clock_merge() {
        let mut c1 = Clock::new(1);
        let mut c2 = Clock::new(2);
        let mut c3 = Clock::new(3);

        c1.inc();
        c2.inc();
        c3.inc();

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
