# Hetu

This is the developing prototype of Hetu, a general-propose message-passing framework which consider causality as first class citizen.

Project layout:
* `src/` the shared core definitions, including the abstract of protocol context and a Tokio-based implementation, cryptography-related types, etc.
* `docs/` high-level design documents
* `crates/permissioned-blockchain` collection of various permissioned blockchain protocols implementation
* `crates/kademlia` distributed hash table implementation
* `scripts/` scripts for testing and benchmarking
