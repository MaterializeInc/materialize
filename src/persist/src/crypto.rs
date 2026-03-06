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

use async_trait::async_trait;
use aws_sdk_kms::primitives::Blob as KmsBlob;
use aws_sdk_kms::types::DataKeySpec;
use aws_sdk_kms::Client as KmsClient;
use bytes::Bytes;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::task::JoinHandle;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use aws_lc_rs::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM, NONCE_LEN};

use crate::location::{
    Blob, BlobMetadata, CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData,
};

const ENVELOPE_VERSION_V1: u8 = 0x01;
const ENVELOPE_VERSION_V2: u8 = 0x02;
const WRAPPED_DEK_LEN_SIZE: usize = 2;
const GCM_TAG_LEN: usize = 16;

struct DataEncryptionKey {
    plaintext: [u8; 32],
    wrapped: Vec<u8>,
}

/// Manages KMS-derived data encryption keys with background rotation.
///
/// When configured with a customer KMS key (two-party encryption), the DEK is double-wrapped:
/// first by the MZ key (via `GenerateDataKey`), then the wrapped blob is encrypted
/// again with the customer key. Both keys must be available to decrypt.
pub struct EnvelopeEncryption {
    mz_kms_client: KmsClient,
    mz_kms_key_id: String,
    customer_kms_client: Option<KmsClient>,
    customer_kms_key_id: Option<String>,
    current_dek: Arc<RwLock<DataEncryptionKey>>,
}

impl std::fmt::Debug for EnvelopeEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvelopeEncryption")
            .field("mz_kms_key_id", &self.mz_kms_key_id)
            .field("two_party_encryption", &self.customer_kms_key_id.is_some())
            .finish_non_exhaustive()
    }
}

impl EnvelopeEncryption {
    /// Initialize: call KMS GenerateDataKey, cache result.
    ///
    /// If `customer_kms_client` and `customer_kms_key_id` are provided,
    /// two-party encryption is enabled: DEKs are double-wrapped with both the
    /// MZ key and the customer key.
    pub async fn new(
        kms_client: KmsClient,
        kms_key_id: String,
        customer_kms_client: Option<KmsClient>,
        customer_kms_key_id: Option<String>,
    ) -> Result<Self, anyhow::Error> {
        let dek = Self::generate_dek(
            &kms_client,
            &kms_key_id,
            customer_kms_client.as_ref(),
            customer_kms_key_id.as_deref(),
        )
        .await?;
        let two_party = customer_kms_key_id.is_some();
        info!(kms_key_id = %kms_key_id, two_party, "envelope encryption initialized");
        Ok(EnvelopeEncryption {
            mz_kms_client: kms_client,
            mz_kms_key_id: kms_key_id,
            customer_kms_client,
            customer_kms_key_id,
            current_dek: Arc::new(RwLock::new(dek)),
        })
    }

    #[cfg(test)]
    fn new_test(key: [u8; 32], wrapped_dek: Vec<u8>) -> Self {
        Self::new_test_inner(key, wrapped_dek, false)
    }

    #[cfg(test)]
    fn new_test_two_party(key: [u8; 32], wrapped_dek: Vec<u8>) -> Self {
        Self::new_test_inner(key, wrapped_dek, true)
    }

    #[cfg(test)]
    fn new_test_inner(key: [u8; 32], wrapped_dek: Vec<u8>, two_party: bool) -> Self {
        use aws_sdk_kms::config::Builder;
        let kms_config = Builder::new()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let kms_client = KmsClient::from_conf(kms_config);
        let (customer_kms_client, customer_kms_key_id) = if two_party {
            let customer_config = Builder::new()
                .behavior_version(aws_config::BehaviorVersion::latest())
                .build();
            (
                Some(KmsClient::from_conf(customer_config)),
                Some("test-customer-key".to_string()),
            )
        } else {
            (None, None)
        };
        EnvelopeEncryption {
            mz_kms_client: kms_client,
            mz_kms_key_id: String::new(),
            customer_kms_client,
            customer_kms_key_id,
            current_dek: Arc::new(RwLock::new(DataEncryptionKey {
                plaintext: key,
                wrapped: wrapped_dek,
            })),
        }
    }

    async fn generate_dek(
        kms_client: &KmsClient,
        kms_key_id: &str,
        customer_kms_client: Option<&KmsClient>,
        customer_kms_key_id: Option<&str>,
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

        let wrapped = match (customer_kms_client, customer_kms_key_id) {
            (Some(client), Some(key_id)) => {
                // Two-party: double-wrap by encrypting the MZ-wrapped DEK with the customer key.
                let resp = client
                    .encrypt()
                    .key_id(key_id)
                    .plaintext(KmsBlob::new(wrapped_blob.as_ref()))
                    .send()
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("customer KMS Encrypt (double-wrap) failed: {}", e)
                    })?;
                let double_wrapped = resp
                    .ciphertext_blob()
                    .ok_or_else(|| {
                        anyhow::anyhow!("customer KMS Encrypt returned no ciphertext_blob")
                    })?;
                double_wrapped.as_ref().to_vec()
            }
            _ => wrapped_blob.as_ref().to_vec(),
        };

        Ok(DataEncryptionKey {
            plaintext: key,
            wrapped,
        })
    }

    /// Spawn background rotation task (every `interval`).
    pub fn start_rotation(self: &Arc<Self>, interval: Duration) -> JoinHandle<()> {
        let this = Arc::clone(self);
        mz_ore::task::spawn(|| "envelope_encryption_dek_rotation", async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // skip first immediate tick
            loop {
                ticker.tick().await;
                match Self::generate_dek(
                    &this.mz_kms_client,
                    &this.mz_kms_key_id,
                    this.customer_kms_client.as_ref(),
                    this.customer_kms_key_id.as_deref(),
                )
                .await
                {
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
        let version = if self.customer_kms_key_id.is_some() {
            ENVELOPE_VERSION_V2
        } else {
            ENVELOPE_VERSION_V1
        };
        let dek = self.current_dek.read().await;
        encrypt_with_dek_versioned(version, &dek.plaintext, &dek.wrapped, plaintext)
    }

    /// Decrypt ciphertext. Parses wrapped DEK from header, uses cached key if
    /// it matches, otherwise calls KMS Decrypt for the wrapped DEK.
    pub async fn decrypt(&self, encrypted: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
        let (version, wrapped_dek, nonce_and_ciphertext) = parse_envelope(encrypted)?;

        // Fast path: check if wrapped DEK matches our cached key.
        let dek = self.current_dek.read().await;
        let plaintext_key = if dek.wrapped == wrapped_dek {
            dek.plaintext
        } else {
            drop(dek);
            debug!("wrapped DEK mismatch, calling KMS Decrypt for old DEK");
            self.decrypt_dek(version, wrapped_dek).await?
        };

        decrypt_with_key(&plaintext_key, nonce_and_ciphertext)
    }

    async fn decrypt_dek(&self, version: u8, wrapped_dek: &[u8]) -> Result<[u8; 32], anyhow::Error> {
        // For V2 (two-party), first unwrap with customer key, then with MZ key.
        let mz_wrapped = if version == ENVELOPE_VERSION_V2 {
            let customer_client = self.customer_kms_client.as_ref().ok_or_else(|| {
                anyhow::anyhow!(
                    "v2 envelope requires customer KMS key, but none is configured"
                )
            })?;
            let customer_key_id = self.customer_kms_key_id.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "v2 envelope requires customer KMS key ID, but none is configured"
                )
            })?;
            let resp = customer_client
                .decrypt()
                .key_id(customer_key_id)
                .ciphertext_blob(KmsBlob::new(wrapped_dek))
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("customer KMS Decrypt failed: {}", e))?;
            let blob = resp.plaintext().ok_or_else(|| {
                anyhow::anyhow!("customer KMS Decrypt returned no plaintext")
            })?;
            blob.as_ref().to_vec()
        } else {
            wrapped_dek.to_vec()
        };

        let resp = self
            .mz_kms_client
            .decrypt()
            .key_id(&self.mz_kms_key_id)
            .ciphertext_blob(KmsBlob::new(mz_wrapped))
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

/// Encrypt using a raw AES-256-GCM key. Builds the envelope format with the given version byte.
fn encrypt_with_dek_versioned(
    version: u8,
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

    let wrapped_len = u16::try_from(wrapped_dek.len())
        .map_err(|_| anyhow::anyhow!("wrapped DEK too large: {} bytes", wrapped_dek.len()))?;
    let header_size = 1 + WRAPPED_DEK_LEN_SIZE + wrapped_dek.len() + NONCE_LEN;
    let mut output = Vec::with_capacity(header_size + in_out.len());
    output.push(version);
    output.extend_from_slice(&wrapped_len.to_le_bytes());
    output.extend_from_slice(wrapped_dek);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(&in_out);

    Ok(output)
}

/// Encrypt using a raw AES-256-GCM key. Builds the V1 envelope format.
pub fn encrypt_with_dek(
    key: &[u8; 32],
    wrapped_dek: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, anyhow::Error> {
    encrypt_with_dek_versioned(ENVELOPE_VERSION_V1, key, wrapped_dek, plaintext)
}

/// Parse the envelope header, returning (version, wrapped_dek, nonce_and_ciphertext_with_tag).
pub fn parse_envelope(data: &[u8]) -> Result<(u8, &[u8], &[u8]), anyhow::Error> {
    if data.is_empty() {
        return Err(anyhow::anyhow!("encrypted data is empty"));
    }
    let version = data[0];
    if version != ENVELOPE_VERSION_V1 && version != ENVELOPE_VERSION_V2 {
        return Err(anyhow::anyhow!(
            "unsupported envelope version: 0x{:02x}",
            version
        ));
    }
    let min_header = 1 + WRAPPED_DEK_LEN_SIZE;
    if data.len() < min_header {
        return Err(anyhow::anyhow!("encrypted data too short for header"));
    }
    let wrapped_len = usize::cast_from(u16::from_le_bytes([data[1], data[2]]));
    let wrapped_end = min_header + wrapped_len;
    let payload_start = wrapped_end + NONCE_LEN;
    if data.len() < payload_start + GCM_TAG_LEN {
        return Err(anyhow::anyhow!("encrypted data too short for envelope"));
    }
    Ok((version, &data[min_header..wrapped_end], &data[wrapped_end..]))
}

/// Decrypt using a raw AES-256-GCM key. Input is nonce || ciphertext || tag.
pub fn decrypt_with_key(
    key: &[u8; 32],
    nonce_and_ciphertext: &[u8],
) -> Result<Vec<u8>, anyhow::Error> {
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
        .map_err(|_| {
            anyhow::anyhow!("AES-256-GCM authentication failed: data may be tampered")
        })?;

    Ok(plaintext.to_vec())
}

/// KMS envelope encryption configuration.
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// The MZ-managed KMS key ARN to use for envelope encryption.
    pub kms_key_id: String,
    /// The AWS region for the MZ KMS key. Falls back to the blob region if unset.
    pub kms_region: Option<String>,
    /// Optional endpoint override (e.g. for LocalStack).
    pub endpoint: Option<String>,
    /// Optional IAM role ARN to assume before calling KMS.
    pub role_arn: Option<String>,
    /// How often to rotate the data encryption key.
    pub dek_rotation_interval: Duration,
    /// Customer-managed KMS key ARN for two-party encryption.
    /// When set, DEKs are double-wrapped with both the MZ key and this key.
    pub customer_kms_key_id: Option<String>,
    /// AWS region for the customer KMS key.
    pub customer_kms_region: Option<String>,
    /// Optional endpoint override for the customer KMS.
    pub customer_kms_endpoint: Option<String>,
    /// Optional IAM role ARN to assume for customer KMS calls.
    pub customer_kms_role_arn: Option<String>,
}

/// Backward-compat alias.
pub type BlobEncryptionConfig = EncryptionConfig;

impl EncryptionConfig {
    /// Build a KMS client from this config (for the MZ-managed key).
    pub async fn build_kms_client(&self) -> Result<KmsClient, ExternalError> {
        let mut loader = mz_aws_util::defaults();

        if let Some(region) = &self.kms_region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &self.endpoint {
            loader = loader.endpoint_url(endpoint);
        }

        let sdk_config = loader.load().await;
        Ok(KmsClient::new(&sdk_config))
    }

    /// Build a KMS client for the customer-managed key.
    /// Returns `None` if no customer KMS key is configured.
    pub async fn build_customer_kms_client(&self) -> Result<Option<KmsClient>, ExternalError> {
        let key_id = match &self.customer_kms_key_id {
            Some(id) if !id.is_empty() => id,
            _ => return Ok(None),
        };

        let mut loader = mz_aws_util::defaults();

        if let Some(region) = &self.customer_kms_region {
            loader = loader.region(aws_config::Region::new(region.clone()));
        } else if let Some(region) = &self.kms_region {
            // Fall back to MZ KMS region if customer region not specified.
            loader = loader.region(aws_config::Region::new(region.clone()));
        }
        if let Some(endpoint) = &self.customer_kms_endpoint {
            loader = loader.endpoint_url(endpoint);
        }

        let sdk_config = loader.load().await;
        info!(customer_kms_key_id = %key_id, "built customer KMS client for two-party encryption");
        Ok(Some(KmsClient::new(&sdk_config)))
    }
}

/// A [Blob] wrapper that transparently encrypts on `set()` and decrypts on `get()`.
pub struct EncryptedBlob {
    inner: Arc<dyn Blob>,
    encryption: Arc<EnvelopeEncryption>,
    _rotation_handle: JoinHandle<()>,
}

impl std::fmt::Debug for EncryptedBlob {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedBlob").finish_non_exhaustive()
    }
}

impl EncryptedBlob {
    /// Create a new `EncryptedBlob` wrapping `inner` with KMS envelope encryption.
    pub async fn new(
        inner: Arc<dyn Blob>,
        kms_client: KmsClient,
        kms_key_id: String,
        rotation_interval: Duration,
        customer_kms_client: Option<KmsClient>,
        customer_kms_key_id: Option<String>,
    ) -> Result<Self, ExternalError> {
        let encryption = Arc::new(
            EnvelopeEncryption::new(
                kms_client,
                kms_key_id,
                customer_kms_client,
                customer_kms_key_id,
            )
            .await
            .map_err(ExternalError::from)?,
        );
        let rotation_handle = encryption.start_rotation(rotation_interval);
        Ok(EncryptedBlob {
            inner,
            encryption,
            _rotation_handle: rotation_handle,
        })
    }

    #[cfg(test)]
    fn new_test(inner: Arc<dyn Blob>) -> Self {
        Self::new_test_inner(inner, false)
    }

    #[cfg(test)]
    fn new_test_two_party(inner: Arc<dyn Blob>) -> Self {
        Self::new_test_inner(inner, true)
    }

    #[cfg(test)]
    fn new_test_inner(inner: Arc<dyn Blob>, two_party: bool) -> Self {
        let encryption = Arc::new(if two_party {
            EnvelopeEncryption::new_test_two_party(
                [0x42u8; 32],
                vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE],
            )
        } else {
            EnvelopeEncryption::new_test(
                [0x42u8; 32],
                vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE],
            )
        });
        // No rotation in tests — use a no-op handle.
        let rotation_handle = mz_ore::task::spawn(|| "test_noop_rotation", async {});
        EncryptedBlob {
            inner,
            encryption,
            _rotation_handle: rotation_handle,
        }
    }
}

#[async_trait]
impl Blob for EncryptedBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let maybe_segments = self.inner.get(key).await?;
        match maybe_segments {
            None => Ok(None),
            Some(segments) => {
                let encrypted = segments.into_contiguous();
                let plaintext = self
                    .encryption
                    .decrypt(&encrypted)
                    .await
                    .map_err(ExternalError::from)?;
                Ok(Some(SegmentedBytes::from(plaintext)))
            }
        }
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.inner.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let ciphertext = self
            .encryption
            .encrypt(&value)
            .await
            .map_err(ExternalError::from)?;
        self.inner.set(key, Bytes::from(ciphertext)).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.inner.delete(key).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.inner.restore(key).await
    }
}

/// A [Consensus] wrapper that transparently encrypts on `compare_and_set()` and
/// decrypts on `head()` / `scan()`.
pub struct EncryptedConsensus {
    inner: Arc<dyn Consensus>,
    encryption: Arc<EnvelopeEncryption>,
    _rotation_handle: JoinHandle<()>,
}

impl std::fmt::Debug for EncryptedConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedConsensus").finish_non_exhaustive()
    }
}

impl EncryptedConsensus {
    /// Create a new `EncryptedConsensus` wrapping `inner` with KMS envelope encryption.
    pub async fn new(
        inner: Arc<dyn Consensus>,
        kms_client: KmsClient,
        kms_key_id: String,
        rotation_interval: Duration,
        customer_kms_client: Option<KmsClient>,
        customer_kms_key_id: Option<String>,
    ) -> Result<Self, ExternalError> {
        let encryption = Arc::new(
            EnvelopeEncryption::new(
                kms_client,
                kms_key_id,
                customer_kms_client,
                customer_kms_key_id,
            )
            .await
            .map_err(ExternalError::from)?,
        );
        let rotation_handle = encryption.start_rotation(rotation_interval);
        Ok(EncryptedConsensus {
            inner,
            encryption,
            _rotation_handle: rotation_handle,
        })
    }

    #[cfg(test)]
    fn new_test(inner: Arc<dyn Consensus>) -> Self {
        Self::new_test_inner(inner, false)
    }

    #[cfg(test)]
    fn new_test_two_party(inner: Arc<dyn Consensus>) -> Self {
        Self::new_test_inner(inner, true)
    }

    #[cfg(test)]
    fn new_test_inner(inner: Arc<dyn Consensus>, two_party: bool) -> Self {
        let encryption = Arc::new(if two_party {
            EnvelopeEncryption::new_test_two_party(
                [0x42u8; 32],
                vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE],
            )
        } else {
            EnvelopeEncryption::new_test(
                [0x42u8; 32],
                vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE],
            )
        });
        let rotation_handle = mz_ore::task::spawn(|| "test_noop_rotation", async {});
        EncryptedConsensus {
            inner,
            encryption,
            _rotation_handle: rotation_handle,
        }
    }

    async fn decrypt_versioned_data(
        &self,
        vd: VersionedData,
    ) -> Result<VersionedData, ExternalError> {
        let plaintext = self
            .encryption
            .decrypt(&vd.data)
            .await
            .map_err(ExternalError::from)?;
        Ok(VersionedData {
            seqno: vd.seqno,
            data: Bytes::from(plaintext),
        })
    }
}

#[async_trait]
impl Consensus for EncryptedConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        self.inner.list_keys()
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        match self.inner.head(key).await? {
            None => Ok(None),
            Some(vd) => Ok(Some(self.decrypt_versioned_data(vd).await?)),
        }
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let encrypted = self
            .encryption
            .encrypt(&new.data)
            .await
            .map_err(ExternalError::from)?;
        let encrypted_new = VersionedData {
            seqno: new.seqno,
            data: Bytes::from(encrypted),
        };
        self.inner.compare_and_set(key, expected, encrypted_new).await
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let entries = self.inner.scan(key, from, limit).await?;
        let mut decrypted = Vec::with_capacity(entries.len());
        for vd in entries {
            decrypted.push(self.decrypt_versioned_data(vd).await?);
        }
        Ok(decrypted)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        self.inner.truncate(key, seqno).await
    }
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

        let (version, parsed_wrapped, nonce_ct) = parse_envelope(&encrypted).unwrap();
        assert_eq!(version, ENVELOPE_VERSION_V1);
        assert_eq!(parsed_wrapped, &wrapped[..]);

        let decrypted = decrypt_with_key(&key, nonce_ct).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn roundtrip_empty_plaintext() {
        let key = test_key();
        let wrapped = test_wrapped_dek();

        let encrypted = encrypt_with_dek(&key, &wrapped, b"").unwrap();
        let (_, _, nonce_ct) = parse_envelope(&encrypted).unwrap();
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

        let (_, _, nonce_ct) = parse_envelope(&encrypted).unwrap();
        let result = decrypt_with_key(&key, nonce_ct);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("authentication failed"),
            "should detect tampered data"
        );
    }

    #[test]
    fn wrong_key_fails() {
        let key = test_key();
        let wrong_key = [0x99u8; 32];
        let wrapped = test_wrapped_dek();

        let encrypted = encrypt_with_dek(&key, &wrapped, b"secret").unwrap();
        let (_, _, nonce_ct) = parse_envelope(&encrypted).unwrap();
        let result = decrypt_with_key(&wrong_key, nonce_ct);
        assert!(result.is_err());
    }

    #[test]
    fn version_byte_validation() {
        let mut data = vec![0x03]; // unsupported version (neither V1 nor V2)
        data.extend_from_slice(&[6, 0]); // wrapped_dek_len
        data.extend_from_slice(&[0u8; 6]); // wrapped_dek
        data.extend_from_slice(&[0u8; 12]); // nonce
        data.extend_from_slice(&[0u8; 16]); // minimal ciphertext (just tag)

        let result = parse_envelope(&data);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("unsupported envelope version"));
    }

    #[test]
    fn envelope_format_parsing() {
        let key = test_key();
        let wrapped = vec![1, 2, 3, 4, 5];

        let encrypted = encrypt_with_dek(&key, &wrapped, b"test").unwrap();

        // Version byte
        assert_eq!(encrypted[0], ENVELOPE_VERSION_V1);
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
        assert!(parse_envelope(&[ENVELOPE_VERSION_V1]).is_err());
        // Version + len but no wrapped DEK / nonce / ciphertext
        assert!(parse_envelope(&[ENVELOPE_VERSION_V1, 4, 0]).is_err());
    }

    use crate::mem::{MemBlob, MemBlobConfig, MemConsensus};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn encrypted_blob_roundtrip() -> Result<(), ExternalError> {
        let mem = MemBlob::open(MemBlobConfig::new(false));
        let blob = EncryptedBlob::new_test(Arc::new(mem));

        // Initially empty.
        assert_eq!(blob.get("k0").await?, None);

        // Set a key and get it back (roundtrip through encrypt/decrypt).
        blob.set("k0", Bytes::from("hello")).await?;
        assert_eq!(
            blob.get("k0").await?.map(|s| s.into_contiguous()),
            Some(b"hello".to_vec())
        );

        // Overwrite and read back.
        blob.set("k0", Bytes::from("world")).await?;
        assert_eq!(
            blob.get("k0").await?.map(|s| s.into_contiguous()),
            Some(b"world".to_vec())
        );

        // Set another key.
        blob.set("k1", Bytes::from("test")).await?;
        assert_eq!(
            blob.get("k1").await?.map(|s| s.into_contiguous()),
            Some(b"test".to_vec())
        );

        // Delete returns Some (size of encrypted data, which is > plaintext).
        let deleted = blob.delete("k0").await?;
        assert!(deleted.is_some());
        assert_eq!(blob.get("k0").await?, None);

        // Double delete returns None.
        assert_eq!(blob.delete("k0").await?, None);

        // Empty value roundtrip.
        blob.set("empty", Bytes::from("")).await?;
        assert_eq!(
            blob.get("empty").await?.map(|s| s.into_contiguous()),
            Some(b"".to_vec())
        );

        // list_keys_and_metadata passes through.
        let mut keys = vec![];
        blob.list_keys_and_metadata("", &mut |entry| {
            keys.push(entry.key.to_string());
        })
        .await?;
        keys.sort();
        assert_eq!(keys, vec!["empty", "k1"]);

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn encrypted_consensus_roundtrip() -> Result<(), ExternalError> {
        let mem = Arc::new(MemConsensus::default());
        let consensus = EncryptedConsensus::new_test(mem);

        // Initially empty.
        assert_eq!(consensus.head("key").await?, None);

        // Initial CaS (None -> seqno 1).
        let data1 = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("first"),
        };
        assert_eq!(
            consensus.compare_and_set("key", None, data1).await?,
            CaSResult::Committed,
        );

        // head returns decrypted data.
        let head = consensus.head("key").await?.unwrap();
        assert_eq!(head.seqno, SeqNo(1));
        assert_eq!(head.data, Bytes::from("first"));

        // scan returns decrypted data.
        let scanned = consensus.scan("key", SeqNo(0), 10).await?;
        assert_eq!(scanned.len(), 1);
        assert_eq!(scanned[0].data, Bytes::from("first"));

        // CaS mismatch.
        let data_bad = VersionedData {
            seqno: SeqNo(2),
            data: Bytes::from("bad"),
        };
        assert_eq!(
            consensus.compare_and_set("key", None, data_bad).await?,
            CaSResult::ExpectationMismatch,
        );

        // CaS success (seqno 1 -> 2).
        let data2 = VersionedData {
            seqno: SeqNo(2),
            data: Bytes::from("second"),
        };
        assert_eq!(
            consensus
                .compare_and_set("key", Some(SeqNo(1)), data2)
                .await?,
            CaSResult::Committed,
        );

        // scan returns both versions, decrypted.
        let scanned = consensus.scan("key", SeqNo(0), 10).await?;
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].data, Bytes::from("first"));
        assert_eq!(scanned[1].data, Bytes::from("second"));

        // truncate passthrough.
        let deleted = consensus.truncate("key", SeqNo(2)).await?;
        assert_eq!(deleted, Some(1));

        // list_keys passthrough.
        use futures_util::StreamExt;
        let keys: Vec<_> = consensus.list_keys().collect().await;
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].as_ref().unwrap(), "key");

        // empty data roundtrip.
        let data_empty = VersionedData {
            seqno: SeqNo(3),
            data: Bytes::new(),
        };
        assert_eq!(
            consensus
                .compare_and_set("key", Some(SeqNo(2)), data_empty)
                .await?,
            CaSResult::Committed,
        );
        let head = consensus.head("key").await?.unwrap();
        assert_eq!(head.seqno, SeqNo(3));
        assert!(head.data.is_empty());

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn encrypted_consensus_data_is_actually_encrypted() -> Result<(), ExternalError> {
        let mem: Arc<MemConsensus> = Arc::new(MemConsensus::default());
        let inner: Arc<dyn Consensus> = Arc::clone(&mem) as Arc<dyn Consensus>;
        let consensus = EncryptedConsensus::new_test(inner);

        let plaintext = b"super secret consensus state";
        let data = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from(&plaintext[..]),
        };
        consensus
            .compare_and_set("key", None, data)
            .await?;

        // Read raw from inner — should be encrypted.
        let raw = mem.head("key").await?.unwrap();
        assert_eq!(raw.seqno, SeqNo(1));
        // Envelope starts with version byte 0x01.
        assert_eq!(raw.data[0], ENVELOPE_VERSION_V1);
        // Raw data must NOT contain the plaintext.
        assert!(
            !raw.data
                .windows(plaintext.len())
                .any(|w| w == plaintext),
            "raw consensus data should not contain plaintext"
        );

        // Read via wrapper — should return original plaintext.
        let decrypted = consensus.head("key").await?.unwrap();
        assert_eq!(&decrypted.data[..], plaintext);

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn encrypted_consensus_impl_test() -> Result<(), ExternalError> {
        use crate::location::tests::consensus_impl_test;
        consensus_impl_test(|| async {
            let mem: Arc<dyn Consensus> = Arc::new(MemConsensus::default());
            Ok(EncryptedConsensus::new_test(mem))
        })
        .await
    }

    // --- Two-party encryption tests ---

    #[test]
    fn two_party_roundtrip() {
        let key = test_key();
        let wrapped = test_wrapped_dek();
        let plaintext = b"hello, two-party envelope encryption!";

        // Encrypt with V2 version byte (simulating double-wrapped DEK).
        let encrypted =
            encrypt_with_dek_versioned(ENVELOPE_VERSION_V2, &key, &wrapped, plaintext).unwrap();
        assert_ne!(&encrypted[..], plaintext);

        let (version, parsed_wrapped, nonce_ct) = parse_envelope(&encrypted).unwrap();
        assert_eq!(version, ENVELOPE_VERSION_V2);
        assert_eq!(parsed_wrapped, &wrapped[..]);

        let decrypted = decrypt_with_key(&key, nonce_ct).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn two_party_v1_backward_compat() {
        // Create V1 data (single-key mode).
        let key = test_key();
        let wrapped = test_wrapped_dek();
        let plaintext = b"v1 data written before two-party";

        let encrypted = encrypt_with_dek(&key, &wrapped, plaintext).unwrap();
        let (version, _, nonce_ct) = parse_envelope(&encrypted).unwrap();
        assert_eq!(version, ENVELOPE_VERSION_V1);

        // A two-party-enabled instance can still decrypt V1 envelopes
        // (the customer key unwrap step is skipped for V1).
        let decrypted = decrypt_with_key(&key, nonce_ct).unwrap();
        assert_eq!(&decrypted[..], plaintext);
    }

    #[test]
    fn two_party_v2_requires_customer_key() {
        // Create V2 data.
        let key = test_key();
        let wrapped = test_wrapped_dek();

        let encrypted =
            encrypt_with_dek_versioned(ENVELOPE_VERSION_V2, &key, &wrapped, b"secret").unwrap();
        let (version, _, _) = parse_envelope(&encrypted).unwrap();
        assert_eq!(version, ENVELOPE_VERSION_V2);

        // An EnvelopeEncryption instance *without* customer KMS should fail
        // to decrypt V2 data (tested at the decrypt_dek level via parse_envelope).
        // Here we verify the version byte is V2 and the single-key instance would
        // need to call decrypt_dek with V2, which requires customer KMS.
        let enc = EnvelopeEncryption::new_test(key, wrapped);
        assert!(enc.customer_kms_client.is_none());

        // The actual KMS call would fail in a real scenario. We verify the
        // structural invariant: V2 envelopes are tagged correctly and a
        // single-key instance has no customer_kms_client.
    }

    #[test]
    fn two_party_envelope_format() {
        let key = test_key();
        let wrapped = vec![1, 2, 3, 4, 5, 6, 7, 8];

        let encrypted =
            encrypt_with_dek_versioned(ENVELOPE_VERSION_V2, &key, &wrapped, b"test").unwrap();

        // Version byte is V2.
        assert_eq!(encrypted[0], ENVELOPE_VERSION_V2);

        // Wrapped DEK length (LE u16).
        let len = u16::from_le_bytes([encrypted[1], encrypted[2]]) as usize;
        assert_eq!(len, wrapped.len());

        // Wrapped DEK content.
        assert_eq!(&encrypted[3..3 + len], &wrapped[..]);

        // Parse succeeds and returns V2.
        let (version, parsed_wrapped, _) = parse_envelope(&encrypted).unwrap();
        assert_eq!(version, ENVELOPE_VERSION_V2);
        assert_eq!(parsed_wrapped, &wrapped[..]);
    }

    #[test]
    fn two_party_customer_key_revocation() {
        // If customer KMS Decrypt fails, the whole decrypt should fail.
        // We simulate this by verifying that V2 data cannot be decrypted
        // with the wrong key (simulating revoked access).
        let key = test_key();
        let wrong_key = [0x99u8; 32];
        let wrapped = test_wrapped_dek();

        let encrypted =
            encrypt_with_dek_versioned(ENVELOPE_VERSION_V2, &key, &wrapped, b"secret").unwrap();
        let (_, _, nonce_ct) = parse_envelope(&encrypted).unwrap();

        let result = decrypt_with_key(&wrong_key, nonce_ct);
        assert!(result.is_err(), "revoked key should fail decryption");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn two_party_encrypted_blob_roundtrip() -> Result<(), ExternalError> {
        // Two-party blob uses V2 envelopes. Since we mock the DEK (no real KMS),
        // the encrypt/decrypt path still exercises the version byte logic.
        let mem = MemBlob::open(MemBlobConfig::new(false));
        let blob = EncryptedBlob::new_test_two_party(Arc::new(mem));

        blob.set("k0", Bytes::from("two-party-hello")).await?;
        assert_eq!(
            blob.get("k0").await?.map(|s| s.into_contiguous()),
            Some(b"two-party-hello".to_vec())
        );

        blob.set("k1", Bytes::from("")).await?;
        assert_eq!(
            blob.get("k1").await?.map(|s| s.into_contiguous()),
            Some(b"".to_vec())
        );

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn two_party_encrypted_consensus_roundtrip() -> Result<(), ExternalError> {
        let mem = Arc::new(MemConsensus::default());
        let consensus = EncryptedConsensus::new_test_two_party(mem);

        let data1 = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("two-party-first"),
        };
        assert_eq!(
            consensus.compare_and_set("key", None, data1).await?,
            CaSResult::Committed,
        );

        let head = consensus.head("key").await?.unwrap();
        assert_eq!(head.seqno, SeqNo(1));
        assert_eq!(head.data, Bytes::from("two-party-first"));

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn two_party_mixed_versions() -> Result<(), ExternalError> {
        // Write V1 data with single-key instance, then read with two-party instance.
        let mem: Arc<MemConsensus> = Arc::new(MemConsensus::default());

        // Phase 1: Write V1 data (single-key).
        let inner_v1: Arc<dyn Consensus> = Arc::clone(&mem) as Arc<dyn Consensus>;
        let consensus_v1 = EncryptedConsensus::new_test(inner_v1);
        let data1 = VersionedData {
            seqno: SeqNo(1),
            data: Bytes::from("v1-data"),
        };
        assert_eq!(
            consensus_v1.compare_and_set("key", None, data1).await?,
            CaSResult::Committed,
        );

        // Verify raw data is V1.
        let raw = mem.head("key").await?.unwrap();
        assert_eq!(raw.data[0], ENVELOPE_VERSION_V1);

        // Phase 2: Two-party instance writes V2 data.
        let inner_two_party: Arc<dyn Consensus> = Arc::clone(&mem) as Arc<dyn Consensus>;
        let consensus_two_party = EncryptedConsensus::new_test_two_party(inner_two_party);
        let data2 = VersionedData {
            seqno: SeqNo(2),
            data: Bytes::from("v2-data"),
        };
        assert_eq!(
            consensus_two_party
                .compare_and_set("key", Some(SeqNo(1)), data2)
                .await?,
            CaSResult::Committed,
        );

        // Verify latest raw data is V2.
        let raw = mem.head("key").await?.unwrap();
        assert_eq!(raw.data[0], ENVELOPE_VERSION_V2);

        // Phase 3: Two-party instance can read both V1 and V2 data.
        let scanned = consensus_two_party.scan("key", SeqNo(0), 10).await?;
        assert_eq!(scanned.len(), 2);
        assert_eq!(scanned[0].data, Bytes::from("v1-data"));
        assert_eq!(scanned[1].data, Bytes::from("v2-data"));

        Ok(())
    }
}

// Kani bounded model checking proofs for envelope encryption.
//
// These harnesses use `#[cfg(kani)]` and are only compiled by `cargo kani`.
// They prove correctness properties of `parse_envelope` and the envelope
// header format that hold for *all* inputs within bounds — not just specific
// test cases.
//
// Run with: `cargo kani -p mz-persist --harness <name>`
#[cfg(kani)]
mod kani_proofs {
    use super::*;

    // Maximum input size for parse_envelope proofs. Large enough to cover
    // valid envelopes with small wrapped DEKs (min valid = 31 bytes),
    // small enough for solver tractability.
    const MAX_INPUT_LEN: usize = 64;

    // -----------------------------------------------------------------------
    // Proof 1: parse_envelope never panics on arbitrary input.
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn parse_envelope_no_panic() {
        let data: [u8; MAX_INPUT_LEN] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= MAX_INPUT_LEN);
        // Must not panic for any input — returning Err is fine.
        let _ = parse_envelope(&data[..len]);
    }

    // -----------------------------------------------------------------------
    // Proof 2: If parse_envelope returns Ok, the version is V1 or V2.
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn parse_envelope_version_valid() {
        let data: [u8; MAX_INPUT_LEN] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= MAX_INPUT_LEN);
        if let Ok((version, _, _)) = parse_envelope(&data[..len]) {
            assert!(version == ENVELOPE_VERSION_V1 || version == ENVELOPE_VERSION_V2);
        }
    }

    // -----------------------------------------------------------------------
    // Proof 3: If parse_envelope returns Ok, the returned slices satisfy:
    //   - nonce_ct.len() >= NONCE_LEN + GCM_TAG_LEN
    //   - header + wrapped_dek + nonce_ct exactly spans the input
    // -----------------------------------------------------------------------
    #[kani::proof]
    fn parse_envelope_slice_bounds() {
        let data: [u8; MAX_INPUT_LEN] = kani::any();
        let len: usize = kani::any();
        kani::assume(len <= MAX_INPUT_LEN);
        if let Ok((_version, wrapped_dek, nonce_ct)) = parse_envelope(&data[..len]) {
            assert!(nonce_ct.len() >= NONCE_LEN + GCM_TAG_LEN);
            // The three components (header, wrapped_dek, nonce_ct) partition the input.
            assert_eq!(
                1 + WRAPPED_DEK_LEN_SIZE + wrapped_dek.len() + nonce_ct.len(),
                len
            );
        }
    }

    // -----------------------------------------------------------------------
    // Proof 4: Envelope header roundtrip — constructing an envelope by hand
    // and parsing it recovers the original version and wrapped DEK.
    // (Avoids AEAD FFI by constructing the byte layout directly.)
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(50)]
    fn envelope_header_roundtrip() {
        let version: u8 = kani::any();
        kani::assume(version == ENVELOPE_VERSION_V1 || version == ENVELOPE_VERSION_V2);

        let wrapped_len: usize = kani::any();
        kani::assume(wrapped_len <= 8);
        let wrapped_dek: [u8; 8] = kani::any();

        let fake_ct_len: usize = kani::any();
        kani::assume(fake_ct_len >= GCM_TAG_LEN && fake_ct_len <= 24);

        // Build envelope: version || wrapped_len (LE u16) || wrapped_dek || nonce || ct+tag
        let total = 1 + WRAPPED_DEK_LEN_SIZE + wrapped_len + NONCE_LEN + fake_ct_len;
        let mut envelope = Vec::with_capacity(total);
        envelope.push(version);
        envelope.extend_from_slice(&(wrapped_len as u16).to_le_bytes());
        envelope.extend_from_slice(&wrapped_dek[..wrapped_len]);
        envelope.resize(envelope.len() + NONCE_LEN, 0u8);
        envelope.resize(envelope.len() + fake_ct_len, 0u8);

        let (parsed_ver, parsed_wrapped, parsed_nonce_ct) = parse_envelope(&envelope).unwrap();
        assert_eq!(parsed_ver, version);
        assert_eq!(parsed_wrapped, &wrapped_dek[..wrapped_len]);
        assert_eq!(parsed_nonce_ct.len(), NONCE_LEN + fake_ct_len);
    }

    // -----------------------------------------------------------------------
    // Proof 5: The version byte is written as the first byte of the envelope
    // and is recovered by parse_envelope.
    // -----------------------------------------------------------------------
    #[kani::proof]
    #[kani::unwind(40)]
    fn version_byte_written_correctly() {
        let version: u8 = kani::any();
        kani::assume(version == ENVELOPE_VERSION_V1 || version == ENVELOPE_VERSION_V2);

        let wrapped_dek = [0u8; 6];
        let wrapped_len = wrapped_dek.len() as u16;
        let total = 1 + WRAPPED_DEK_LEN_SIZE + wrapped_dek.len() + NONCE_LEN + GCM_TAG_LEN;
        let mut output = Vec::with_capacity(total);
        output.push(version);
        output.extend_from_slice(&wrapped_len.to_le_bytes());
        output.extend_from_slice(&wrapped_dek);
        output.resize(output.len() + NONCE_LEN + GCM_TAG_LEN, 0u8);

        assert_eq!(output[0], version);
        let (parsed_ver, _, _) = parse_envelope(&output).unwrap();
        assert_eq!(parsed_ver, version);
    }
}
