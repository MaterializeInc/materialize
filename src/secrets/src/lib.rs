// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use async_trait::async_trait;
use mz_repr::GlobalId;
use std::path::PathBuf;

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

pub trait SecretsReader: Send + Sync {
    fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error>;
    fn canonical_path(&self, id: GlobalId) -> Result<PathBuf, anyhow::Error>;
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
