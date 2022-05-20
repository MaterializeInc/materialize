// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::path::PathBuf;

use async_trait::async_trait;

use mz_repr::GlobalId;

/// Securely stores secrets.
#[async_trait]
pub trait SecretsController: Send + Sync {
    /// Applies the specified secret operations in bulk.
    ///
    /// Implementations must apply the operations atomically. If the method
    /// returns `Ok(())`, then all operations have been applied successfully;
    /// if the method returns `Err(())`, then none of the operations have been
    /// applied.
    ///
    /// Implementations are permitted to reject combinations of operations which
    /// they cannot apply atomically.
    async fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), anyhow::Error>;
}

/// An operation on a [`SecretsController`].
pub enum SecretOp {
    /// Create or update the contents of a secret.
    Ensure {
        /// The ID of the secret to create or update.
        id: GlobalId,
        /// The binary contents of the secret.
        contents: Vec<u8>,
    },
    /// Delete a secret.
    Delete {
        /// The id of the secret to delete.
        id: GlobalId,
    },
}

/// Configures a [`SecretsReader`].
pub struct SecretsReaderConfig {
    /// The directory at which secrets are mounted.
    pub mount_path: PathBuf,
}

/// Securely reads secrets that are managed by a [`SecretsController`].
///
/// Does not provide access to create, update, or delete the secrets within.
pub struct SecretsReader {
    config: SecretsReaderConfig,
}

impl SecretsReader {
    pub fn new(config: SecretsReaderConfig) -> Self {
        Self { config }
    }

    /// Returns the contents of a secret identified by GlobalId
    pub fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error> {
        let file_path = self.config.mount_path.join(id.to_string());
        Ok(fs::read(file_path)?)
    }

    /// Returns the path of the secret consisting of a configured base path
    /// and the GlobalId
    pub fn canonical_path(&self, id: GlobalId) -> Result<PathBuf, anyhow::Error> {
        let path = self.config.mount_path.join(id.to_string());
        Ok(path)
    }
}
