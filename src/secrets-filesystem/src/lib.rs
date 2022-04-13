// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use anyhow::Error;
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

impl SecretsController for FilesystemSecretsController {
    fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        assert_eq!(ops.len(), 1);
        for op in ops.iter() {
            match op {
                SecretOp::Ensure { id, contents } => {
                    let file_path = self.secrets_storage_path.join(format!("{}", id));
                    let mut file = OpenOptions::new()
                        .mode(0o600)
                        .create_new(true)
                        .write(true)
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
