use std::{fmt::Debug, hash::Hash};
use serde::{Deserialize, Serialize};

/// Payload is the vec<u8> but derive many traits, like hash, debug, clone, etc.
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, derive_more::Deref, Serialize, Deserialize,
)]
pub struct Payload(pub Vec<u8>);

impl Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "Payload(\"{s}\")")
        } else {
            write!(
                f,
                "Payload({}{})",
                self.0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .take(32)
                    .collect::<Vec<_>>()
                    .concat(),
                if self.0.len() > 32 {
                    format!(".. <len {}>", self.0.len())
                } else {
                    String::new()
                }
            )
        }
    }
}