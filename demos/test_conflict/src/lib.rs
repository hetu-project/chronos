use std::collections::HashMap;

#[derive(Debug)]
struct VectorClock {
    clock: HashMap<String, i64>,
}

impl VectorClock {
    fn new() -> Self {
        VectorClock {
            clock: HashMap::new(),
        }
    }

    fn get_version(&self, node_id: &str) -> &i64 {
        self.clock.get(node_id).unwrap_or(&0)
    }

    fn update_version(&mut self, node_id: &str, version: i64) {
        self.clock.insert(node_id.parse().unwrap(), version);
    }

    fn has_conflict(&self, other: &VectorClock) -> bool {
        let mut all_greater = true;
        let mut all_smaller = true;

        for (node, version) in &self.clock {
            if let Some(other_version) = other.clock.get(node) {
                if *version < *other_version {
                    all_greater = false;
                } else if *version > *other_version {
                    all_smaller = false;
                }
            }
        }

        !(all_greater || all_smaller)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict() {
        let mut clock1 = VectorClock::new();
        clock1.update_version("A".to_string().as_str(), 1);
        clock1.update_version("B".to_string().as_str(), 2);

        let mut clock2 = VectorClock::new();
        clock2.update_version("A".to_string().as_str(), 2);
        clock2.update_version("B".to_string().as_str(), 1);

        assert!(clock1.has_conflict(&clock2));
    }
}
