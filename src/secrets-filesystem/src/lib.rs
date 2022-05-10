// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use anyhow::{bail, Error};
use async_trait::async_trait;
use mz_secrets::{SecretOp, SecretsController};
use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

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

#[async_trait]
impl SecretsController for FilesystemSecretsController {
    async fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        // The filesystem controller can not execute multiple FS operations in a single atomic txn.
        // Deletes are a special case. They are being executed as a post-commit event and once
        // the coordinator reaches that code, the secrets can no longer be reached by any reader.
        // This behavior is atomic in nature and the FS operations do not have to be.
        // On the other hand, Ensure has to be atomic, since a concurrent reader should not
        // see torn FS transactions.
        // Hence we enforce the limit of exactly 1 ensure (update/create) operation per txn

        for op in ops.iter() {
            match op {
                SecretOp::Ensure { id, contents } => {
                    if ops.len() > 1 {
                        bail!("secrets controller does not support creating multiple secrets atomically")
                    }
                    let file_path = self.secrets_storage_path.join(format!("{}", id));
                    let mut file = OpenOptions::new()
                        .mode(0o600)
                        .create(true)
                        .write(true)
                        .truncate(true)
                        .open(file_path)?;
                    file.write_all(contents)?;
                    file.sync_all()?;
                }
                SecretOp::Delete { id } => {
                    fs::remove_file(self.secrets_storage_path.join(format!("{}", id)))?;
                }
            }
        }

        return Ok(());
    }
}
