// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
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
#[derive(Debug)]
pub struct SecretsReaderConfig {
    /// The directory at which secrets are mounted.
    pub mount_path: PathBuf,
}

/// Securely reads secrets that are managed by a [`SecretsController`].
///
/// Does not provide access to create, update, or delete the secrets within.
#[derive(Debug, Clone)]
pub struct SecretsReader {
    config: Arc<SecretsReaderConfig>,
}

impl SecretsReader {
    pub fn new(config: SecretsReaderConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }

    /// Returns the contents of a secret identified by GlobalId
    ///
    /// This `read` will return a complete version of the secret. It will not
    /// return, e.g., one block from v1 and another from v2.
    ///
    /// - On Linux / OSX filesystems, `File::open` will hold a handle open so
    ///   that even if the file is deleted, we still continue to read from it.
    ///   This means a SecretOp::Delete followed by a SecretOp::Ensure can never
    ///   "swap out" data mid-`read_to_end`.
    /// - We do not allow editing secrets which _would_ expose us to this issue.
    ///
    /// (N.B. Were we ever to run with Windows / NTFS, this would also work
    /// properly _and_ mid-read edits would be disallowed)
    pub fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error> {
        let file_path = self.config.mount_path.join(id.to_string());

        // Use a `File` handle directly to make clear that we are upholding our documented guarantee
        // of opening and reading from the file only once.
        let mut file = File::open(file_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    pub fn read_string(&self, id: GlobalId) -> anyhow::Result<String> {
        String::from_utf8(self.read(id)?).context("converting secret value to string")
    }

    /// Returns the path of the secret consisting of a configured base path
    /// and the GlobalId
    pub fn canonical_path(&self, id: GlobalId) -> Result<PathBuf, anyhow::Error> {
        let path = self.config.mount_path.join(id.to_string());
        Ok(path)
    }
}
