# Core Functional Crates

The crates folder of Chronos includes core functional code crates and utility libraries, etc.

## [vlc](./vlc/)

- This verifiable logical clock crate implements a verifiable logical clock construct. 
- The clock can be used in a peer-to-peer network to order events. 
- Any node in the network can verify the correctness of the clock.

## [hlc](./hlc/)

- This hybrid logical clock crate implements a hybrid logical clock (HLC) construct designed to combine physical and logical timestamps.
- The clock can be used in distributed systems to efficiently order events while minimizing the reliance on synchronized physical clocks.
- Each timestamp consists of a wall-clock time and a logical component, allowing for easy comparison and conflict resolution.
- This crate is an implementation of the [Hybrid Logical Clock](http://www.cse.buffalo.edu/tech-reports/2014-04.pdf).

## [hvlc](./hvlc/)

- This Hybrid Vector Logical Clock (HVLC) crate implements a hybrid vector clock structure that combines physical timestamps with vector clock properties.
- HVLC uses a BTreeMap to store logical clock values for multiple nodes while maintaining a physical timestamp, enabling efficient tracking of causality and concurrent events in distributed systems.
- Each clock instance contains:
  - A mapping table (inner) that records logical clock values for each node ID
  - A physical timestamp used to provide total ordering when logical clock comparison is insufficient
- The implementation provides core functionalities like event ordering, clock merging, and base calculation, suitable for scenarios requiring distributed causality tracking.
- Compared to regular vector clocks, HVLC offers better total ordering support through physical timestamps while maintaining the causal consistency properties of vector clocks. 
- It can be used to as the [CRDTs](https://crdt.tech/)(Conflict-free Replicated Data Type) algorithm in distributed scenarios for providing total ordering.

## [accumulator](./accumulator/)

- A simple accumulator application.
- Each accumulator node maintains a set of strings. Upon receiving a string from a client, the node adds the string to its state, and broadcast the new state to other nodes in the network. 
- All nodes eventually converge to the same state, by merging received states into their own states.

## [cops](./cops/)

- A causally consistent data store inspired by [COPS](https://www.cs.cmu.edu/~dga/papers/cops-sosp2011.pdf).
- The data store maintains a set of key-value pairs. 
- It provides causal consistency to clients.

## [enclaves](./enclaves/)

- This module provides some common utilities of TEE (Trusted Execution Environment) Enclaves. 
- For examples: AWS nitro enclave, Mircosoft Azure, Intel SGX, etc.

## [gossip](./gossip/)

- This module provides the Gossip network toolkit for customizing a specified parameter. 
- It implements a basic gossip network using libp2p. It currently supports discovery via mdns and bootnodes.

## [crypto](./crypto/)

- Some common crypto utilities, signatures, verify, and hash functions for elliptic curve.

## [vrf](./vrf/)

- This module contains implementations of a [verifiable random function](https://en.wikipedia.org/wiki/Verifiable_random_function), currently only ECVRF. 
- VRFs can be used in the consensus protocol for leader election.