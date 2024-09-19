//! This module contains implementations of a
//! [verifiable random function](https://en.wikipedia.org/wiki/Verifiable_random_function)
//! (currently only ECVRF). VRFs can be used in the consensus protocol for leader election.

pub mod ecvrf;
pub mod traits;
pub mod test_utils;
pub mod sample;

#[cfg(test)]
mod unit_tests;


pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
