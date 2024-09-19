use anyhow::Ok;
use hex::FromHex;
use rand::rngs::OsRng;
use secp256k1::ecdsa::{RecoverableSignature, RecoveryId};
use sha3::{Digest, Keccak256};

pub fn public_key_to_address(public_key_hex: &str) -> anyhow::Result<String, anyhow::Error>  {
    let public_key_bytes = hex::decode(public_key_hex)?;

    let mut hasher = Keccak256::new();
    hasher.update(&public_key_bytes[1..]);
    let binding = hasher.finalize();
    let hash_bytes = binding.as_slice();

    let address_bytes = &hash_bytes[hash_bytes.len() - 20..];

    let mut address = "0x".to_owned();
    address.push_str(&hex::encode(address_bytes));

    Ok(address)
}

pub fn gen_secp256k1_keypair() -> (String, String) {
    let secp = secp256k1::Secp256k1::new();
    let (secret_key, public_key) = secp.generate_keypair(&mut OsRng);
    let secret_key_hex = hex::encode(secret_key.secret_bytes());
    let public_key_hex = hex::encode(public_key.serialize());
    (secret_key_hex, public_key_hex)
}

pub fn sign_message_recover_pk(
    secp: &secp256k1::Secp256k1<secp256k1::All>,
    secret_key: &secp256k1::SecretKey,
    message: &[u8],
) -> anyhow::Result<RecoverableSignature, anyhow::Error> {
    let message = secp256k1::Message::from_digest_slice(message)?;
    Ok(secp.sign_ecdsa_recoverable(&message, &secret_key))
}

pub fn recover_public_key(
    secp: &secp256k1::Secp256k1<secp256k1::All>,
    signature: &RecoverableSignature,
    message: &[u8],
) -> anyhow::Result<secp256k1::PublicKey, anyhow::Error> {
    let message = secp256k1::Message::from_digest_slice(message)?;
    let pub_key = secp.recover_ecdsa(&message, &signature)?;
    Ok(pub_key)
}

pub fn verify_secp256k1_recovery_pk(
    signature_hex: &str,
    message_hex: &str,
) -> anyhow::Result<(), anyhow::Error> {
    let signature_bytes = Vec::from_hex(signature_hex)?;
    let message_bytes = Vec::from_hex(message_hex)?;

    let secp = secp256k1::Secp256k1::new();

    let recovery_id = RecoveryId::from_i32(i32::from(signature_bytes[64]))?;
    let signatures_no_id = &signature_bytes[0..64];

    let recoverable_signature = RecoverableSignature::from_compact(signatures_no_id, recovery_id)?;
    let message = secp256k1::Message::from_digest_slice(&message_bytes)?;
    let public_key = secp.recover_ecdsa(&message, &recoverable_signature)?;

    let signature = secp256k1::ecdsa::Signature::from_compact(signatures_no_id)?;
    secp.verify_ecdsa(&message, &signature, &public_key)?;
    Ok(())
}

pub fn verify_secp256k1_recovery_pk_bytes(
    signature_bytes: Vec<u8>,
    message_bytes: [u8; 32],
) -> anyhow::Result<secp256k1::PublicKey, anyhow::Error> {

    let secp = secp256k1::Secp256k1::new();

    let recovery_id = RecoveryId::from_i32(i32::from(signature_bytes[64]))?;
    let signatures_no_id = &signature_bytes[0..64];

    let recoverable_signature = RecoverableSignature::from_compact(signatures_no_id, recovery_id)?;
    let message = secp256k1::Message::from_digest_slice(&message_bytes)?;
    let pub_key = secp.recover_ecdsa(&message, &recoverable_signature)?;
    Ok(pub_key)
}

#[cfg(test)]
mod tests {
    use crate::core::DigestHash;
    use rand::rngs::OsRng;

    use super::*;

    #[test]
    fn sign_recover_verify() {
        use DigestHash as _;

        let secp = secp256k1::Secp256k1::new();
        let (secret_key, public_key) = secp.generate_keypair(&mut OsRng);
        let secret_key_hex = hex::encode(secret_key.secret_bytes());
        let public_key_hex = hex::encode(public_key.serialize());
        println!("pri :{} \npub : {}", secret_key_hex, public_key_hex);

        let message = "Hello, Ethereum!".to_owned();
        let msg = message.sha256().to_fixed_bytes();
        let signature_recover = sign_message_recover_pk(&secp, &secret_key, &msg).unwrap();
        let serialized_signature = signature_recover.serialize_compact();
        println!("sig struct: {:?}", serialized_signature);

        let recovery_id_byte = serialized_signature.0.to_i32() as u8;
        let mut serialized_with_recovery_id = serialized_signature.1.to_vec();
        serialized_with_recovery_id.push(recovery_id_byte);
        let sig_hex = hex::encode(serialized_with_recovery_id);
        let msg_hex = hex::encode(msg);
        println!(
            "Signature with recovery ID in hex: {}, len = {},\n msg_hex: {}",
            sig_hex,
            sig_hex.len(),
            msg_hex
        );
        
        let ret = verify_secp256k1_recovery_pk(&sig_hex, &msg_hex);
        let recover_pubkey = recover_public_key(&secp, &signature_recover, &msg).unwrap();
        assert!(ret.is_ok());
        assert_eq!(recover_pubkey, public_key);
    }
}
