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

/// Securely manages user secrets.
#[async_trait]
pub trait SecretsController: Debug + Send + Sync {
    /// Creates or updates the specified secret with the specified binary
    /// contents.
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error>;

    /// Deletes the specified secret.
    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error>;

    /// Lists known secrets. Unrecognized secret objects do not produce an error
    /// and are ignored.
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
}

impl InMemorySecretsController {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

#[async_trait]
impl SecretsController for InMemorySecretsController {
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error> {
        self.data.lock().unwrap().insert(id, contents.to_vec());
        Ok(())
    }

    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error> {
        self.data.lock().unwrap().remove(&id);
        Ok(())
    }

    async fn list(&self) -> Result<Vec<CatalogItemId>, anyhow::Error> {
        Ok(self.data.lock().unwrap().keys().cloned().collect())
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(InMemorySecretsController {
            data: Arc::clone(&self.data),
        })
    }
}

#[async_trait]
impl SecretsReader for InMemorySecretsController {
    async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error> {
        let contents = self.data.lock().unwrap().get(&id).cloned();
        contents.ok_or_else(|| anyhow::anyhow!("secret does not exist"))
    }
}
