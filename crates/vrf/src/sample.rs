use num_bigint::BigUint;
use num_traits::{One, Num};

#[derive(Debug, Clone, Copy)]
pub struct Sampler {
    precision: usize
}

impl Sampler {
    pub fn new(precision: usize) -> Self {
        Sampler {
            precision
        }
    }

    pub fn hex_to_biguint(&self, hex_str: &str) -> BigUint {
        BigUint::from_str_radix(hex_str, 16).expect("Invalid hex string")
    }
    
    pub fn calculate_threshold(&self, probability: f64) -> BigUint {
        // percision is based at bit
        let max_output = BigUint::one() << self.precision;
        let threshold = max_output * BigUint::from((probability * 100.0) as u64) / BigUint::from(100u64);
        threshold
    }
    
    pub fn meets_threshold(&self, output: &BigUint, threshold: &BigUint) -> bool {
        output < threshold
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meets() {
        let sampler = Sampler::new(512);
        let vrf_output_hex = "a64c292ec45f6b252828aff9a02a0fe88d2fcc7f5fc61bb328f03f4c6c0657a9d26efb23b87647ff54f71cd51a6fa4c4e31661d8f72b41ff00ac4d2eec2ea7b3";
        let vrf_output = sampler.hex_to_biguint(vrf_output_hex);

        let target_probability = 0.1;
        let threshold = sampler.calculate_threshold(target_probability);
        let meets = sampler.meets_threshold(&vrf_output, &threshold);
        if  meets {
            println!("Node is selected.");
        } else {
            println!("Node is not selected.");
        }
        
        assert_eq!(meets, false);
    }
}