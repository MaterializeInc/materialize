// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Error;
use mz_expr::GlobalId;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

/// Securely stores secrets.
pub trait SecretsController {
    /// Applies the specified secret operations in bulk.
    ///
    /// Implementations must apply the operations atomically. If the method
    /// returns `Ok(())`, then all operations have been applied successfully;
    /// if the method returns `Err(())`, then none of the operations have been
    /// applied.
    ///
    /// Implementations are permitted to reject combinations of operations which
    /// they cannot apply atomically.
    fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), anyhow::Error>;

    /// Returns the IDs of all known secrets.
    fn list(&self) -> Result<Vec<GlobalId>, anyhow::Error>;
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

pub struct FilesystemSecretsController {
    secrets_storage_path: PathBuf,
}

impl FilesystemSecretsController {
    pub fn new(secrets_storage_path: PathBuf) -> Self {
        Self {
            secrets_storage_path,
        }
    }
}

impl SecretsController for FilesystemSecretsController {
    fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        assert_eq!(ops.len(), 1);
        for op in ops.iter() {
            match op {
                SecretOp::Ensure { id, contents } => {
                    // create will override an existing file
                    let mut file = File::create(self.secrets_storage_path.join(format!("{}", id)))?;
                    file.write_all(contents)?;
                }
                SecretOp::Delete { id } => {
                    fs::remove_file(self.secrets_storage_path.join(format!("{}", id)))?;
                }
            }
        }

        return Ok(());
    }

    fn list(&self) -> Result<Vec<GlobalId>, Error> {
        todo!()
    }
}
