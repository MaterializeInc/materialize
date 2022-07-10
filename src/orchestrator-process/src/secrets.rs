// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of user secrets via the local file system.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

use mz_repr::GlobalId;
use mz_secrets::{SecretsController, SecretsReader};

use crate::ProcessOrchestrator;

#[async_trait]
impl SecretsController for ProcessOrchestrator {
    async fn ensure(&self, id: GlobalId, contents: &[u8]) -> Result<(), anyhow::Error> {
        let file_path = self.secrets_dir.join(id.to_string());
        let mut file = OpenOptions::new()
            .mode(0o600)
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)
            .await?;
        file.write_all(contents).await?;
        file.sync_all().await?;
        Ok(())
    }

    async fn delete(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        fs::remove_file(self.secrets_dir.join(id.to_string())).await?;
        Ok(())
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(ProcessSecretsReader {
            secrets_dir: self.secrets_dir.clone(),
        })
    }
}

/// A secrets reader associated with a [`ProcessOrchestrator`].
#[derive(Debug)]
pub struct ProcessSecretsReader {
    secrets_dir: PathBuf,
}

impl ProcessSecretsReader {
    /// Constructs a new [`ProcessSecretsReader`] that reads secrets out of the
    /// specified directory.
    pub fn new(secrets_dir: PathBuf) -> ProcessSecretsReader {
        ProcessSecretsReader { secrets_dir }
    }
}

#[async_trait]
impl SecretsReader for ProcessSecretsReader {
    async fn read(&self, id: GlobalId) -> Result<Vec<u8>, anyhow::Error> {
        let contents = fs::read(self.secrets_dir.join(id.to_string())).await?;
        Ok(contents)
    }
}
