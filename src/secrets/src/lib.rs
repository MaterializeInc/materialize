// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions for secure management of user secrets.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use anyhow::Context;
use async_trait::async_trait;

use mz_repr::GlobalId;

/// Securely manages user secrets.
#[async_trait]
pub trait SecretsController: Debug + Send + Sync {
    /// Creates or updates the specified secret with the specified binary
    /// contents.
    async fn ensure(&self, id: GlobalId, contents: &[u8]) -> Result<(), anyhow::Error>;

    /// Deletes the specified secret.
    async fn delete(&self, id: GlobalId) -> Result<(), anyhow::Error>;

    /// Returns a reader for the secrets managed by this controller.
    fn reader(&self) -> Arc<dyn SecretsReader>;
}

/// Securely reads secrets that are managed by a [`SecretsController`].
///
/// Does not provide access to create, update, or delete the secrets within.
#[async_trait]
pub trait SecretsReader: Debug + Send + Sync {
    /// Returns the binary contents of the specified secret.
    async fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error>;

    /// Returns the string contents of the specified secret.
    ///
    /// Returns an error if the secret's contents cannot be decoded as UTF-8.
    async fn read_string(&self, id: GlobalId) -> Result<String, anyhow::Error> {
        let contents = self.read(id).await?;
        String::from_utf8(contents).context("converting secret value to string")
    }
}

#[derive(Debug)]
pub struct InMemorySecretsController {
    data: Arc<Mutex<HashMap<GlobalId, Vec<u8>>>>,
}

impl InMemorySecretsController {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl SecretsController for InMemorySecretsController {
    async fn ensure(&self, id: GlobalId, contents: &[u8]) -> Result<(), anyhow::Error> {
        self.data.lock().unwrap().insert(id, contents.to_vec());
        Ok(())
    }

    async fn delete(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        self.data.lock().unwrap().remove(&id);
        Ok(())
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(InMemorySecretsController {
            data: Arc::clone(&self.data),
        })
    }
}

#[async_trait]
impl SecretsReader for InMemorySecretsController {
    async fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error> {
        let contents = self.data.lock().unwrap().get(&id).cloned();
        contents.ok_or_else(|| anyhow::anyhow!("secret does not exist"))
    }
}
