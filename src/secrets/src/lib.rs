// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions for secure management of user secrets.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use mz_repr::CatalogItemId;

pub mod cache;

/// Validates the name of an internal secret.
///
/// Internal secret names must be valid across all secrets backends (Kubernetes secret names,
/// file names, AWS Secrets Manager names), so the accepted alphabet is the conservative
/// intersection: lowercase alphanumerics and `-`, starting and ending with an alphanumeric,
/// at most 128 characters.
pub fn validate_internal_secret_name(name: &str) -> Result<(), anyhow::Error> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-');
    if valid {
        Ok(())
    } else {
        Err(anyhow::anyhow!("invalid internal secret name: {name:?}"))
    }
}

/// Securely manages user secrets.
///
/// In addition to user secrets, which are keyed by the [`CatalogItemId`] of the corresponding
/// `CREATE SECRET` item, a controller manages internal secrets, which are keyed by name and hold
/// system-generated credentials (e.g. transport keys) rather than catalog state. The two
/// namespaces are disjoint: internal secrets never collide with user secrets and are not
/// returned by [`list`](SecretsController::list).
#[async_trait]
pub trait SecretsController: Debug + Send + Sync {
    /// Creates or updates the specified secret with the specified binary
    /// contents.
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error>;

    /// Creates or updates the specified internal secret with the specified binary contents.
    ///
    /// The name must satisfy [`validate_internal_secret_name`].
    async fn ensure_internal(&self, name: &str, contents: &[u8]) -> Result<(), anyhow::Error>;

    /// Deletes the specified secret.
    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error>;

    /// Deletes the specified internal secret.
    ///
    /// Deleting an internal secret that does not exist is not an error.
    async fn delete_internal(&self, name: &str) -> Result<(), anyhow::Error>;

    /// Lists known user secrets. Internal secrets are not included.
    /// Unrecognized secret objects do not produce an error and are ignored.
    async fn list(&self) -> Result<Vec<CatalogItemId>, anyhow::Error>;

    /// Returns a reader for the secrets managed by this controller.
    fn reader(&self) -> Arc<dyn SecretsReader>;
}

#[derive(Debug)]
pub struct CachingPolicy {
    /// Whether or not caching is enabled.
    pub enabled: bool,
    /// "time to live" of records within the cache.
    pub ttl: Duration,
}

/// Securely reads secrets that are managed by a [`SecretsController`].
///
/// Does not provide access to create, update, or delete the secrets within.
#[async_trait]
pub trait SecretsReader: Debug + Send + Sync {
    /// Returns the binary contents of the specified secret.
    async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error>;

    /// Returns the binary contents of the specified internal secret.
    async fn read_internal(&self, name: &str) -> Result<Vec<u8>, anyhow::Error>;

    /// Returns the string contents of the specified secret.
    ///
    /// Returns an error if the secret's contents cannot be decoded as UTF-8.
    async fn read_string(&self, id: CatalogItemId) -> Result<String, anyhow::Error> {
        let contents = self.read(id).await?;
        String::from_utf8(contents).context("converting secret value to string")
    }
}

#[derive(Debug)]
pub struct InMemorySecretsController {
    data: Arc<Mutex<BTreeMap<CatalogItemId, Vec<u8>>>>,
    internal_data: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl InMemorySecretsController {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
            internal_data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl SecretsController for InMemorySecretsController {
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error> {
        self.data.lock().unwrap().insert(id, contents.to_vec());
        Ok(())
    }

    async fn ensure_internal(&self, name: &str, contents: &[u8]) -> Result<(), anyhow::Error> {
        validate_internal_secret_name(name)?;
        self.internal_data
            .lock()
            .unwrap()
            .insert(name.to_string(), contents.to_vec());
        Ok(())
    }

    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error> {
        self.data.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn delete_internal(&self, name: &str) -> Result<(), anyhow::Error> {
        self.internal_data.lock().unwrap().remove(name);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<CatalogItemId>, anyhow::Error> {
        Ok(self.data.lock().unwrap().keys().cloned().collect())
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(InMemorySecretsController {
            data: Arc::clone(&self.data),
            internal_data: Arc::clone(&self.internal_data),
        })
    }
}

#[async_trait]
impl SecretsReader for InMemorySecretsController {
    async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error> {
        let contents = self.data.lock().unwrap().get(&id).cloned();
        contents.ok_or_else(|| anyhow::anyhow!("secret does not exist"))
    }

    async fn read_internal(&self, name: &str) -> Result<Vec<u8>, anyhow::Error> {
        let contents = self.internal_data.lock().unwrap().get(name).cloned();
        contents.ok_or_else(|| anyhow::anyhow!("secret does not exist"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn test_in_memory_internal_secrets_roundtrip() {
        let controller = InMemorySecretsController::new();

        controller
            .ensure_internal("ctp-ca", b"key material")
            .await
            .unwrap();
        let reader = controller.reader();
        assert_eq!(
            reader.read_internal("ctp-ca").await.unwrap(),
            b"key material"
        );

        // Internal secrets do not appear in the user secret listing, and do not collide with
        // user secrets of a numeric name.
        let id = CatalogItemId::User(1);
        controller.ensure(id, b"user").await.unwrap();
        assert_eq!(controller.list().await.unwrap(), vec![id]);
        assert_eq!(reader.read(id).await.unwrap(), b"user");

        controller.delete_internal("ctp-ca").await.unwrap();
        assert!(reader.read_internal("ctp-ca").await.is_err());
        // Deleting a nonexistent internal secret is not an error.
        controller.delete_internal("ctp-ca").await.unwrap();
    }

    #[mz_ore::test]
    fn test_validate_internal_secret_name() {
        for valid in ["a", "ctp-ca", "cluster-u1-replica-u3", "0-a-9"] {
            validate_internal_secret_name(valid).unwrap();
        }
        let too_long = "a".repeat(129);
        for invalid in ["", "-a", "a-", "A", "a_b", "a.b", "a/b", "ä", &too_long] {
            assert!(
                validate_internal_secret_name(invalid).is_err(),
                "name {invalid:?} must be rejected"
            );
        }
    }
}
