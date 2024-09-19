use std::collections::HashMap;

#[derive(Debug)]
struct _VectorClock {
    clock: HashMap<String, i64>,
}

impl _VectorClock {
    fn _new() -> Self {
        _VectorClock {
            clock: HashMap::new(),
        }
    }

    fn _get_version(&self, node_id: &str) -> &i64 {
        self.clock.get(node_id).unwrap_or(&0)
    }

    fn _update_version(&mut self, node_id: &str, version: i64) {
        match node_id.parse() {
            Ok(node) => {
                self.clock.insert(node, version);
            }
            Err(e) => {
                eprintln!("Invalid node ID: {}", e);
            }
        }
    }

    fn _has_conflict(&self, other: &_VectorClock,) -> bool {
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
        let mut clock1 = _VectorClock::_new();
        clock1._update_version("A", 1);
        clock1._update_version("B", 2);

        let mut clock2 = _VectorClock::_new();
        clock2._update_version("A", 2);
        clock2._update_version("B", 1);

        assert!(clock1._has_conflict(&clock2));
    }
}

