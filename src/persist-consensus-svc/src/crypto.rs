// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! KMS-based envelope encryption for S3 data at rest.
//!
//! Uses AWS KMS to generate Data Encryption Keys (DEKs), then encrypts data
//! locally with AES-256-GCM. Each encrypted object is self-contained: it
//! includes the KMS-wrapped DEK so only the master key ARN is needed to decrypt.
//!
//! Encrypted object byte layout:
//! ```text
//! [version: 1 byte (0x01)] || [wrapped_dek_len: 2 bytes LE] || [wrapped_dek: N bytes]
//! || [nonce: 12 bytes] || [ciphertext + GCM tag: variable]
//! ```

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::DataKeySpec;
use aws_sdk_kms::Client as KmsClient;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use aws_lc_rs::aead::{
    Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN,
};

const ENVELOPE_VERSION: u8 = 0x01;
const WRAPPED_DEK_LEN_SIZE: usize = 2;
const GCM_TAG_LEN: usize = 16;

struct DataEncryptionKey {
    plaintext: [u8; 32],
    wrapped: Vec<u8>,
}

/// Manages KMS-derived data encryption keys with background rotation.
pub struct EnvelopeEncryption {
    kms_client: KmsClient,
    kms_key_id: String,
    current_dek: Arc<RwLock<DataEncryptionKey>>,
}

impl EnvelopeEncryption {
    /// Initialize: call KMS GenerateDataKey, cache result.
    pub async fn new(kms_client: KmsClient, kms_key_id: String) -> Result<Self, anyhow::Error> {
        let dek = Self::generate_dek(&kms_client, &kms_key_id).await?;
        info!(kms_key_id = %kms_key_id, "envelope encryption initialized");
        Ok(EnvelopeEncryption {
            kms_client,
            kms_key_id,
            current_dek: Arc::new(RwLock::new(dek)),
        })
    }

    async fn generate_dek(
        kms_client: &KmsClient,
        kms_key_id: &str,
    ) -> Result<DataEncryptionKey, anyhow::Error> {
        let resp = kms_client
            .generate_data_key()
            .key_id(kms_key_id)
            .key_spec(DataKeySpec::Aes256)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("KMS GenerateDataKey failed: {}", e))?;

        let plaintext_blob = resp
            .plaintext()
            .ok_or_else(|| anyhow::anyhow!("KMS GenerateDataKey returned no plaintext"))?;
        let wrapped_blob = resp
            .ciphertext_blob()
            .ok_or_else(|| anyhow::anyhow!("KMS GenerateDataKey returned no ciphertext_blob"))?;

        let plaintext_bytes = plaintext_blob.as_ref();
        if plaintext_bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "KMS returned key of length {}, expected 32",
                plaintext_bytes.len()
            ));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(plaintext_bytes);

        Ok(DataEncryptionKey {
            plaintext: key,
            wrapped: wrapped_blob.as_ref().to_vec(),
        })
    }

    /// Spawn background rotation task (every `interval`).
    pub fn start_rotation(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip first immediate tick
            loop {
                ticker.tick().await;
                match Self::generate_dek(&this.kms_client, &this.kms_key_id).await {
                    Ok(new_dek) => {
                        *this.current_dek.write().await = new_dek;
                        info!("DEK rotated successfully");
                    }
                    Err(e) => {
                        warn!("DEK rotation failed, keeping current key: {}", e);
                    }
                }
            }
        })
    }

    /// Encrypt plaintext. Reads cached DEK (RwLock read — uncontended).
    /// Returns: version || wrapped_dek_len || wrapped_dek || nonce || ciphertext || tag
    pub async fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let dek = self.current_dek.read().await;
        encrypt_with_dek(&dek.plaintext, &dek.wrapped, plaintext)
    }

    /// Decrypt ciphertext. Parses wrapped DEK from header, uses cached key if
    /// it matches, otherwise calls KMS Decrypt for the wrapped DEK.
    pub async fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let (wrapped_dek, nonce_and_ciphertext) = parse_envelope(encrypted)?;

        // Fast path: check if wrapped DEK matches our cached key.
        let dek = self.current_dek.read().await;
        let plaintext_key = if dek.wrapped == wrapped_dek {
            dek.plaintext
        } else {
            drop(dek);
            debug!("wrapped DEK mismatch, calling KMS Decrypt for old DEK");
            self.decrypt_dek(wrapped_dek).await?
        };

        decrypt_with_key(&plaintext_key, nonce_and_ciphertext)
    }

    async fn decrypt_dek(&self, wrapped_dek: &[u8]) -> Result<[u8; 32], anyhow::Error> {
        let resp = self
            .kms_client
            .decrypt()
            .key_id(&self.kms_key_id)
            .ciphertext_blob(Blob::new(wrapped_dek))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("KMS Decrypt failed: {}", e))?;

        let plaintext_blob = resp
            .plaintext()
            .ok_or_else(|| anyhow::anyhow!("KMS Decrypt returned no plaintext"))?;
        let bytes = plaintext_blob.as_ref();
        if bytes.len() != 32 {
            return Err(anyhow::anyhow!(
                "KMS Decrypt returned key of length {}, expected 32",
                bytes.len()
            ));
        }
        let mut key = [0u8; 32];
        key.copy_from_slice(bytes);
        Ok(key)
    }
}

/// Encrypt using a raw AES-256-GCM key. Builds the envelope format.
fn encrypt_with_dek(
    key: &[u8; 32],
    wrapped_dek: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, anyhow::Error> {
    let unbound = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|_| anyhow::anyhow!("failed to create AES-256-GCM key"))?;
    let aead_key = LessSafeKey::new(unbound);

    let mut nonce_bytes = [0u8; NONCE_LEN];
    aws_lc_rs::rand::fill(&mut nonce_bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate random nonce"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);

    // in_out buffer: plaintext + space for GCM tag
    let mut in_out = Vec::with_capacity(plaintext.len() + GCM_TAG_LEN);
    in_out.extend_from_slice(plaintext);

    aead_key
        .seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("AES-256-GCM seal failed"))?;

    let wrapped_len = wrapped_dek.len() as u16;
    let header_size = 1 + WRAPPED_DEK_LEN_SIZE + wrapped_dek.len() + NONCE_LEN;
    let mut output = Vec::with_capacity(header_size + in_out.len());
    output.push(ENVELOPE_VERSION);
    output.extend_from_slice(&wrapped_len.to_le_bytes());
    output.extend_from_slice(wrapped_dek);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&in_out);

    Ok(output)
}

/// Parse the envelope header, returning (wrapped_dek, nonce_and_ciphertext_with_tag).
fn parse_envelope(data: &[u8]) -> Result<(&[u8], &[u8]), anyhow::Error> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("encrypted data is empty"));
    }
    if data[0] != ENVELOPE_VERSION {
        return Err(anyhow::anyhow!(
            "unsupported envelope version: 0x{:02x}",
            data[0]
        ));
    }
    let min_header = 1 + WRAPPED_DEK_LEN_SIZE;
    if data.len() < min_header {
        return Err(anyhow::anyhow!("encrypted data too short for header"));
    }
    let wrapped_len =
        u16::from_le_bytes([data[1], data[2]]) as usize;
    let wrapped_end = min_header + wrapped_len;
    let payload_start = wrapped_end + NONCE_LEN;
    if data.len() < payload_start + GCM_TAG_LEN {
        return Err(anyhow::anyhow!("encrypted data too short for envelope"));
    }
    Ok((&data[min_header..wrapped_end], &data[wrapped_end..]))
}

/// Decrypt using a raw AES-256-GCM key. Input is nonce || ciphertext || tag.
fn decrypt_with_key(key: &[u8; 32], nonce_and_ciphertext: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    if nonce_and_ciphertext.len() < NONCE_LEN + GCM_TAG_LEN {
        return Err(anyhow::anyhow!("ciphertext too short"));
    }
    let (nonce_bytes, ciphertext_with_tag) = nonce_and_ciphertext.split_at(NONCE_LEN);

    let unbound = UnboundKey::new(&AES_256_GCM, key)
        .map_err(|_| anyhow::anyhow!("failed to create AES-256-GCM key"))?;
    let aead_key = LessSafeKey::new(unbound);

    let mut nonce_arr = [0u8; NONCE_LEN];
    nonce_arr.copy_from_slice(nonce_bytes);
    let nonce = Nonce::assume_unique_for_key(nonce_arr);

    let mut buf = ciphertext_with_tag.to_vec();
    let plaintext = aead_key
        .open_in_place(nonce, Aad::empty(), &mut buf)
        .map_err(|_| anyhow::anyhow!("AES-256-GCM authentication failed: data may be tampered"))?;

    Ok(plaintext.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        [0x42u8; 32]
    }

    fn test_wrapped_dek() -> Vec<u8> {
        vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE]
    }

    #[test]
    fn roundtrip() {
        let key = test_key();
        let wrapped = test_wrapped_dek();
        let plaintext = b"hello, envelope encryption!";

        let encrypted = encrypt_with_dek(&key, &wrapped, plaintext).unwrap();
        assert_ne!(&encrypted[..], plaintext);

        let (parsed_wrapped, nonce_ct) = parse_envelope(&encrypted).unwrap();
        assert_eq!(parsed_wrapped, &wrapped[..]);

        let decrypted = decrypt_with_key(&key, nonce_ct).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn roundtrip_empty_plaintext() {
        let key = test_key();
        let wrapped = test_wrapped_dek();

        let encrypted = encrypt_with_dek(&key, &wrapped, b"").unwrap();
        let (_, nonce_ct) = parse_envelope(&encrypted).unwrap();
        let decrypted = decrypt_with_key(&key, nonce_ct).unwrap();
        assert!(decrypted.is_empty());
    }

    #[test]
    fn tamper_detection() {
        let key = test_key();
        let wrapped = test_wrapped_dek();

        let mut encrypted = encrypt_with_dek(&key, &wrapped, b"secret data").unwrap();
        // Flip a byte in the ciphertext portion (after header + nonce).
        let flip_pos = encrypted.len() - 5;
        encrypted[flip_pos] ^= 0xFF;

        let (_, nonce_ct) = parse_envelope(&encrypted).unwrap();
        let result = decrypt_with_key(&key, nonce_ct);
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("authentication failed"),
            "should detect tampered data"
        );
    }

    #[test]
    fn wrong_key_fails() {
        let key = test_key();
        let wrong_key = [0x99u8; 32];
        let wrapped = test_wrapped_dek();

        let encrypted = encrypt_with_dek(&key, &wrapped, b"secret").unwrap();
        let (_, nonce_ct) = parse_envelope(&encrypted).unwrap();
        let result = decrypt_with_key(&wrong_key, nonce_ct);
        assert!(result.is_err());
    }

    #[test]
    fn version_byte_validation() {
        let mut data = vec![0x02]; // wrong version
        data.extend_from_slice(&[6, 0]); // wrapped_dek_len
        data.extend_from_slice(&[0u8; 6]); // wrapped_dek
        data.extend_from_slice(&[0u8; 12]); // nonce
        data.extend_from_slice(&[0u8; 16]); // minimal ciphertext (just tag)

        let result = parse_envelope(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unsupported envelope version"));
    }

    #[test]
    fn envelope_format_parsing() {
        let key = test_key();
        let wrapped = vec![1, 2, 3, 4, 5];

        let encrypted = encrypt_with_dek(&key, &wrapped, b"test").unwrap();

        // Version byte
        assert_eq!(encrypted[0], ENVELOPE_VERSION);
        // Wrapped DEK length (LE u16)
        let len = u16::from_le_bytes([encrypted[1], encrypted[2]]) as usize;
        assert_eq!(len, wrapped.len());
        // Wrapped DEK content
        assert_eq!(&encrypted[3..3 + len], &wrapped[..]);
    }

    #[test]
    fn truncated_data_rejected() {
        // Empty
        assert!(parse_envelope(&[]).is_err());
        // Just version
        assert!(parse_envelope(&[ENVELOPE_VERSION]).is_err());
        // Version + len but no wrapped DEK / nonce / ciphertext
        assert!(parse_envelope(&[ENVELOPE_VERSION, 4, 0]).is_err());
    }
}
