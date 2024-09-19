//! A simple utxo structure for testing case.

use std::io;
use sha2::Digest;

/// SimpleUTXO input
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxInput {
    pub tx_id: Vec<u8>,
    pub index: u32,
    pub signature: Vec<u8>,
}

impl TxInput {
    pub fn new(tx_id: Vec<u8>, index: u32, signature: Vec<u8>) -> Self {
        TxInput {
            tx_id,
            index,
            signature,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxOutput {
    pub value: u64,
    pub pubkey: Vec<u8>,
}

impl TxOutput {
    pub fn new(value: u64, pubkey: Vec<u8>) -> Self {
        TxOutput { value, pubkey }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleUTXO {
    pub inputs: Vec<TxInput>,
    pub outputs: Vec<TxOutput>,
}

impl SimpleUTXO {
    /// new empty SimpleUTXO
    pub fn new() -> Self {
        SimpleUTXO {
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    /// new SimpleUTXO with inputs and outputs
    pub fn new_with_inputs_outputs(
        inputs: Vec<TxInput>,
        outputs: Vec<TxOutput>,
    ) -> Self {
        SimpleUTXO { inputs, outputs }
    }

    /// calculate ID
    pub fn tx_id(&self) -> Vec<u8> {
        let mut encoder = sha2::Sha256::new();
        encoder.update(&self.encode());
        encoder.finalize().to_vec()
    }

    /// serialize
    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = vec![];
        for input in &self.inputs {
            encoded.extend(input.tx_id.clone());
            encoded.extend(input.index.to_be_bytes().to_vec());
            encoded.extend(input.signature.clone());
        }
        for output in &self.outputs {
            encoded.extend(output.value.to_be_bytes().to_vec());
            encoded.extend(output.pubkey.clone());
        }
        encoded
    }

    /// deserialize
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut inputs = vec![];
        let mut outputs = vec![];
        let mut pos = 0;

        let input_count = bytes[pos] as usize;
        pos += 1;
        for _ in 0..input_count {
            let tx_id_len = bytes[pos] as usize;
            pos += 1;
            let tx_id = bytes[pos..(pos + tx_id_len)].to_vec();
            pos += tx_id_len;

            let index = u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
            pos += 4;

            let signature_len = bytes[pos] as usize;
            pos += 1;
            let signature = bytes[pos..(pos + signature_len)].to_vec();
            pos += signature_len;

            inputs.push(TxInput {
                tx_id,
                index,
                signature,
            });
        }

        let output_count = bytes[pos] as usize;
        pos += 1;
        for _ in 0..output_count {
            let value = u64::from_be_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7],
            ]);
            pos += 8;

            let pubkey_len = bytes[pos] as usize;
            pos += 1;
            let pubkey = bytes[pos..(pos + pubkey_len)].to_vec();
            pos += pubkey_len;

            outputs.push(TxOutput {
                value,
                pubkey,
            });
        }

        Ok(SimpleUTXO {
            inputs,
            outputs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_empty_transaction() {
        let bytes = [0u8, 0u8];
        let tx = SimpleUTXO::decode(&bytes).unwrap();
        assert_eq!(tx.inputs.len(), 0);
        assert_eq!(tx.outputs.len(), 0);
    }
}