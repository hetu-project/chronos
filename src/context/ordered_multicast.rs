use std::{hash::Hash, ops::Deref, sync::Arc};

use bincode::Options;
use k256::{
    ecdsa::{SigningKey, VerifyingKey},
    schnorr::signature::{DigestSigner, DigestVerifier},
    sha2::{Digest, Sha256},
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{
    crypto::{DigestHash, Hasher, Invalid, Verifier, Verify},
    replication::ReplicaIndex,
    Addr, Receivers,
};

pub fn serialize(message: &(impl Serialize + DigestHash)) -> Vec<u8> {
    let digest = Hasher::sha256(message).finalize();
    [
        &[0; 20],
        &digest[..8], // read by HalfSipHash
        &[0; 40],
        &*digest, // read by K256
        &bincode::options().serialize(message).unwrap(),
    ]
    .concat()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderedMulticast<M> {
    pub seq_num: u32,
    pub signature: Signature,
    pub linked: [u8; 32],
    pub inner: M,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signature {
    HalfSipHash([[u8; 4]; 4]),
    K256Linked,
    K256(k256::ecdsa::Signature),
    K256Unverified(k256::ecdsa::Signature),
}

impl<M> Deref for OrderedMulticast<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Hash for Signature {
    fn hash<H>(&self, hasher: &mut H)
    where
        H: std::hash::Hasher,
    {
        match self {
            Self::HalfSipHash([code0, code1, code2, code3]) => {
                hasher.write(&code0[..]);
                hasher.write(&code1[..]);
                hasher.write(&code2[..]);
                hasher.write(&code3[..])
            }
            Self::K256Linked => {} // TODO
            Self::K256(signature) => hasher.write(&signature.to_bytes()),
            Self::K256Unverified(signature) => hasher.write(&signature.to_bytes()),
        }
    }
}

impl<M> OrderedMulticast<M> {
    pub fn verified(&self) -> bool {
        use Signature::*;
        matches!(self.signature, HalfSipHash(_) | K256(_))
    }

    pub fn state(&self) -> Sha256
    where
        M: DigestHash,
    {
        use Signature::*;
        assert!(matches!(
            self.signature,
            K256(_) | K256Unverified(_) | K256Linked
        ));
        state_internal(
            self.linked,
            Hasher::sha256(&self.inner).finalize().into(),
            self.seq_num,
        )
    }
}

fn state_internal(linked: [u8; 32], digest: [u8; 32], seq_num: u32) -> Sha256 {
    let mut state = [0; 52];
    state[0..32].copy_from_slice(&linked);
    for (a, b) in state[16..48].iter_mut().zip(digest) {
        *a ^= b
    }
    state[48..52].copy_from_slice(&seq_num.to_be_bytes());
    Sha256::new().chain_update(state)
}

#[derive(Debug, Clone)]
pub enum Variant {
    Unreachable,
    HalfSipHash(HalfSipHash),
    K256(K256),
}

#[derive(Debug, Clone)]
pub struct HalfSipHash {
    index: ReplicaIndex,
    //
}

#[derive(Debug, Clone)]
pub struct K256 {
    verifying_key: VerifyingKey,
}

const SIGNING_KEY: &[u8] = include_bytes!("ordered_multicast_signing_key");

impl Variant {
    pub fn new_half_sip_hash(index: ReplicaIndex) -> Self {
        Self::HalfSipHash(HalfSipHash { index })
    }

    pub fn new_k256() -> Self {
        Self::K256(K256 {
            verifying_key: *SigningKey::from_slice(SIGNING_KEY).unwrap().verifying_key(),
        })
    }

    pub fn deserialize<M>(&self, buf: impl AsRef<[u8]>) -> OrderedMulticast<M>
    where
        M: DeserializeOwned,
    {
        let buf = buf.as_ref();
        // for (i, byte) in buf.iter().enumerate() {
        //     print!("{byte:02x}");
        //     if (i + 1) % 32 == 0 {
        //         println!()
        //     } else {
        //         print!(" ")
        //     }
        // }
        // println!();
        let mut seq_num = [0; 4];
        seq_num.copy_from_slice(&buf[0..4]);
        let signature = match self {
            Self::Unreachable => unreachable!(),
            Self::HalfSipHash(_) => {
                let mut codes = [[0; 4]; 4];
                codes[0].copy_from_slice(&buf[4..8]);
                codes[1].copy_from_slice(&buf[8..12]);
                codes[2].copy_from_slice(&buf[12..16]);
                codes[3].copy_from_slice(&buf[16..20]);
                Signature::HalfSipHash(codes)
            }
            Self::K256(_) if buf[4..68].iter().all(|&b| b == 0) => Signature::K256Linked,
            Self::K256(_) => {
                let mut signature = [0; 64];
                signature.copy_from_slice(&buf[4..68]);
                signature.reverse();
                // println!("{:02x?}", signature);
                Signature::K256(k256::ecdsa::Signature::from_bytes(&signature.into()).unwrap())
            }
        };
        let mut linked = [0; 32];
        if matches!(self, Self::K256(_)) {
            linked.copy_from_slice(&buf[68..100]);
        }
        OrderedMulticast {
            seq_num: u32::from_be_bytes(seq_num),
            signature,
            linked,
            inner: bincode::options()
                .allow_trailing_bytes()
                .deserialize(&buf[100..])
                .unwrap(),
        }
    }

    pub fn verify<M>(&self, message: &OrderedMulticast<M>) -> Result<(), Invalid>
    where
        M: DigestHash,
    {
        let digest = <[_; 32]>::from(Hasher::sha256(&**message).finalize());
        match (self, message.signature) {
            (Self::Unreachable, _) => unreachable!(),
            (Self::HalfSipHash(variant), Signature::HalfSipHash(codes)) => {
                // TODO tentatively mock the HalfSipHash for SipHash
                use std::hash::BuildHasher;
                if std::collections::hash_map::RandomState::new().hash_one(digest) == 0 {
                    return Err(Invalid::Private);
                }
                if codes[variant.index as usize % 4] == [0; 4] {
                    return Err(Invalid::Private);
                }
                Ok(())
            }
            (Self::K256(_), Signature::K256Linked)
            | (Self::K256(_), Signature::K256Unverified(_)) => Ok(()),
            (Self::K256(k256), Signature::K256(signature)) => k256
                .verifying_key
                .verify_digest(message.state(), &signature)
                .map_err(|_| Invalid::Public),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug)]
pub enum Delegate<M> {
    Nop(ReplicaIndex),
    K256(Option<(Addr, OrderedMulticast<M>)>),
}

impl Variant {
    pub fn delegate<M>(&self) -> Delegate<M> {
        match self {
            Self::Unreachable => Delegate::Nop(ReplicaIndex::MAX),
            Self::HalfSipHash(variant) => Delegate::Nop(variant.index),
            Self::K256(_) => Delegate::K256(Default::default()),
        }
    }
}

impl<M> Delegate<M> {
    pub fn on_receive<N, I>(
        &mut self,
        remote: Addr,
        message: OrderedMulticast<M>,
        receivers: &mut impl Receivers<Message = N>,
        verifier: &Verifier<I>,
        into: impl Fn(OrderedMulticast<M>) -> N,
    ) where
        N: Verify<I>,
    {
        match self {
            &mut Self::Nop(index) => {
                if let Signature::HalfSipHash(codes) = &message.signature {
                    let code = codes[index as usize % 4];
                    if code[0] == 0xcc && code[1] == 0xcc && code[2] == 0xcc && code[3] != index {
                        return;
                    }
                }
                let message = into(message);
                message.verify(verifier).unwrap();
                receivers.handle(Addr::Multicast, remote, message)
            }
            Self::K256(saved) => {
                let (remote, message) = if !message.verified() {
                    (remote, message)
                } else if let Some((saved_remote, saved_message)) = saved.replace((remote, message))
                {
                    let OrderedMulticast {
                        seq_num,
                        signature: Signature::K256(signature),
                        linked,
                        inner,
                    } = saved_message
                    else {
                        unreachable!()
                    };
                    let saved_message = OrderedMulticast {
                        seq_num,
                        signature: Signature::K256Unverified(signature),
                        linked,
                        inner,
                    };
                    (saved_remote, saved_message)
                } else {
                    return;
                };
                let message = into(message);
                message.verify(verifier).unwrap();
                receivers.handle(Addr::Multicast, remote, message)
            }
        }
    }

    pub fn on_pace<N, I>(
        &mut self,
        receivers: &mut impl Receivers<Message = N>,
        verifier: &Verifier<I>,
        into: impl Fn(OrderedMulticast<M>) -> N,
    ) where
        N: Verify<I>,
    {
        if let Self::K256(saved) = self {
            if let Some((remote, message)) = saved.take() {
                let message = into(message);
                message.verify(verifier).unwrap();
                receivers.handle(Addr::Multicast, remote, message)
            } else {
                // println!("! no signed ordered multicast buffer")
            }
        }
    }
}

#[derive(Debug)]
pub struct Sequencer {
    seq_num: u32,
    crypto: SequencerCrypto,
}

#[derive(Debug, Clone)]
enum SequencerCrypto {
    HalfSipHash {
        num_replica: usize,
    },
    K256 {
        state: Sha256,
        signing_key: Arc<SigningKey>,
    },
}

impl Sequencer {
    pub fn new_half_sip_hash(num_replica: usize) -> Self {
        Self {
            seq_num: 0,
            crypto: SequencerCrypto::HalfSipHash { num_replica },
        }
    }

    pub fn new_k256() -> Self {
        Self {
            seq_num: 0,
            crypto: SequencerCrypto::K256 {
                state: Default::default(),
                signing_key: Arc::new(SigningKey::from_slice(SIGNING_KEY).unwrap()),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SequencerProcess {
    buf: Vec<u8>,
    seq_num: u32,
    crypto: SequencerProcessCrypto,
}

#[derive(Debug, Clone)]
enum SequencerProcessCrypto {
    HalfSipHash {
        num_replica: usize,
    },
    K256 {
        linked: [u8; 32],
        state: Sha256,
        signing_key: Arc<SigningKey>,
    },
}

impl Sequencer {
    pub fn process(&mut self, buf: Vec<u8>) -> SequencerProcess {
        self.seq_num += 1;
        let crypto = match &mut self.crypto {
            &mut SequencerCrypto::HalfSipHash { num_replica } => {
                SequencerProcessCrypto::HalfSipHash { num_replica }
            }
            SequencerCrypto::K256 { state, signing_key } => {
                let mut digest = [0; 32];
                digest.copy_from_slice(&buf[68..100]);
                let linked = std::mem::take(state).finalize().into();
                *state = state_internal(linked, digest, self.seq_num);
                SequencerProcessCrypto::K256 {
                    linked,
                    state: state.clone(),
                    signing_key: signing_key.clone(),
                }
            }
        };
        SequencerProcess {
            buf,
            seq_num: self.seq_num,
            crypto,
        }
    }
}

impl SequencerProcess {
    pub fn apply(mut self, send: impl Fn(&[u8])) {
        self.buf[0..4].copy_from_slice(&self.seq_num.to_be_bytes());
        match self.crypto {
            SequencerProcessCrypto::HalfSipHash { num_replica } => {
                let mut offset = 0;
                while offset < num_replica as u8 {
                    self.buf[4..8].copy_from_slice(&[0xcc, 0xcc, 0xcc, offset]);
                    self.buf[8..12].copy_from_slice(&[0xcc, 0xcc, 0xcc, offset + 1]);
                    self.buf[12..16].copy_from_slice(&[0xcc, 0xcc, 0xcc, offset + 2]);
                    self.buf[16..20].copy_from_slice(&[0xcc, 0xcc, 0xcc, offset + 3]);
                    send(&self.buf);
                    offset += 4
                }
            }
            SequencerProcessCrypto::K256 {
                linked,
                state,
                signing_key,
            } => {
                self.buf[68..100].copy_from_slice(&linked);
                let signature: k256::ecdsa::Signature = signing_key.sign_digest(state);
                let mut signature = signature.to_bytes();
                signature.reverse();
                self.buf[4..68].copy_from_slice(&signature);
                send(&self.buf)
            }
        }
    }
}
