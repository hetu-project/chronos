//! Solution for digest and digital signature.
//!
//! This codebase chooses borsh as digest solution: a typed message's digest is
//! computed by first serializing it into borsh's byte representation, then
//! SHA256 on the bytes. This solution workarounds the need of implementing
//! digest hashing on every message type, for which Rust community is currently
//! lack of good auto-deriving solution.
//!
//! (The closest I can get is to derive `std::hash::Hash`, but that is believed
//! to be a bad practice for security usage.)

use std::ops::Deref;

use borsh::{BorshDeserialize, BorshSerialize};

pub type Digest = [u8; 32];

pub fn digest(data: &impl BorshSerialize) -> crate::Result<Digest> {
    Ok(
        *secp256k1::Message::from_hashed_data::<secp256k1::hashes::sha256::Hash>(&borsh::to_vec(
            data,
        )?)
        .as_ref(),
    )
}

/// On-wire format of signed messages. Happen to be type-erasured.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Packet {
    signature: Signature,
    inner: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct Signature([u8; 64]);

/// In-memory representation of `M`, equiped with a signature that may or may
/// not be verified.
///
/// If `inner` is mutated, the `inner_bytes` and `signature` may get out of sync
/// with it, so keep `Message<M>` read only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message<M> {
    pub signature: Signature,
    pub inner: M,
    pub verified: bool,
    inner_bytes: Vec<u8>,
}

impl<M> Deref for Message<M> {
    type Target = M;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<M> From<Message<M>> for Packet {
    fn from(value: Message<M>) -> Self {
        Self {
            inner: value.inner_bytes,
            signature: value.signature,
        }
    }
}

impl Packet {
    pub fn deserialize<M>(self) -> crate::Result<Message<M>>
    where
        M: BorshDeserialize,
    {
        Ok(Message {
            signature: self.signature,
            inner: borsh::from_slice(&self.inner)?,
            inner_bytes: self.inner,
            verified: false,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Signer(secp256k1::SecretKey);

impl From<secp256k1::SecretKey> for Signer {
    fn from(value: secp256k1::SecretKey) -> Self {
        Self(value)
    }
}

impl Signer {
    pub fn new_hardcoded(index: usize) -> Self {
        let mut key = [0xcc; 32];
        let index = index.to_be_bytes();
        key[..index.len()].copy_from_slice(&index);
        Self(secp256k1::SecretKey::from_slice(&key).unwrap())
    }

    pub fn serialize_sign<M>(&self, message: M) -> crate::Result<Message<M>>
    where
        M: BorshSerialize,
    {
        let serialized_inner = borsh::to_vec(&message)?;
        let hash = secp256k1::Message::from_hashed_data::<secp256k1::hashes::sha256::Hash>(
            &serialized_inner,
        );
        thread_local! {
            static SECP: secp256k1::Secp256k1<secp256k1::SignOnly> = secp256k1::Secp256k1::signing_only();
        }
        let signature = Signature(
            SECP.with(|secp| secp.sign_ecdsa(&hash, &self.0))
                .serialize_compact(),
        );
        Ok(Message {
            signature,
            inner: message,
            verified: true,
            inner_bytes: serialized_inner,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Verifier(secp256k1::PublicKey);

impl From<secp256k1::PublicKey> for Verifier {
    fn from(value: secp256k1::PublicKey) -> Self {
        Self(value)
    }
}

impl From<&Signer> for Verifier {
    fn from(Signer(key): &Signer) -> Self {
        thread_local! {
            static SECP: secp256k1::Secp256k1<secp256k1::SignOnly> =
                secp256k1::Secp256k1::signing_only();
        }
        Self(SECP.with(|secp| key.public_key(secp)))
    }
}

impl From<Signer> for Verifier {
    fn from(value: Signer) -> Self {
        Self::from(&value)
    }
}

impl BorshSerialize for Verifier {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.0.serialize(), writer)
    }
}

impl BorshDeserialize for Verifier {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> borsh::io::Result<Self> {
        Ok(Self(
            secp256k1::PublicKey::from_slice(&<[u8; 33] as BorshDeserialize>::deserialize_reader(
                reader,
            )?)
            .map_err(|err| borsh::io::Error::new(borsh::io::ErrorKind::InvalidInput, err))?,
        ))
    }
}

impl Verifier {
    pub fn verify<M>(&self, message: &mut Message<M>) -> crate::Result<()> {
        if message.verified {
            return Ok(());
        }
        let hash = secp256k1::Message::from_hashed_data::<secp256k1::hashes::sha256::Hash>(
            &message.inner_bytes,
        );
        thread_local! {
            static SECP: secp256k1::Secp256k1<secp256k1::VerifyOnly> =
                secp256k1::Secp256k1::verification_only();
        }
        let Signature(signature) = &message.signature;
        SECP.with(|secp| {
            secp.verify_ecdsa(
                &hash,
                &secp256k1::ecdsa::Signature::from_compact(signature)?,
                &self.0,
            )
        })?;
        message.verified = true;
        Ok(())
    }
}
