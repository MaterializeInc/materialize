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

use anyhow::Context;
use async_trait::async_trait;
use mz_repr::CatalogItemId;
use mz_secrets::{SecretsController, SecretsReader, validate_internal_secret_name};
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::ProcessOrchestrator;

/// The file name for an internal secret.
///
/// The `internal-` prefix keeps internal secrets disjoint from user secrets, whose file names
/// are numeric catalog item IDs.
fn internal_secret_file_name(name: &str) -> String {
    format!("internal-{name}")
}

/// Writes a secret file with owner-only permissions.
async fn write_secret_file(
    file_path: std::path::PathBuf,
    contents: &[u8],
    description: &str,
) -> Result<(), anyhow::Error> {
    let mut file = OpenOptions::new()
        .mode(0o600)
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .await
        .with_context(|| format!("writing secret {description}"))?;
    file.write_all(contents)
        .await
        .with_context(|| format!("writing secret {description}"))?;
    file.sync_all()
        .await
        .with_context(|| format!("writing secret {description}"))?;
    Ok(())
}

#[async_trait]
impl SecretsController for ProcessOrchestrator {
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error> {
        let file_path = self.secrets_dir.join(id.to_string());
        write_secret_file(file_path, contents, &id.to_string()).await
    }

    async fn ensure_internal(&self, name: &str, contents: &[u8]) -> Result<(), anyhow::Error> {
        validate_internal_secret_name(name)?;
        let file_path = self.secrets_dir.join(internal_secret_file_name(name));
        write_secret_file(file_path, contents, name).await
    }

    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error> {
        fs::remove_file(self.secrets_dir.join(id.to_string()))
            .await
            .with_context(|| format!("deleting secret {id}"))?;
        Ok(())
    }

    async fn delete_internal(&self, name: &str) -> Result<(), anyhow::Error> {
        match fs::remove_file(self.secrets_dir.join(internal_secret_file_name(name))).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("deleting secret {name}")),
        }
    }

    async fn list(&self) -> Result<Vec<CatalogItemId>, anyhow::Error> {
        let mut ids = Vec::new();
        let mut entries = fs::read_dir(&self.secrets_dir)
            .await
            .context("listing secrets")?;
        while let Some(dir) = entries.next_entry().await? {
            // Ignore file names that are not valid user secret IDs, such as internal secrets.
            if let Ok(id) = dir.file_name().to_string_lossy().parse::<CatalogItemId>() {
                ids.push(id);
            }
        }
        Ok(ids)
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
    async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error> {
        let contents = fs::read(self.secrets_dir.join(id.to_string()))
            .await
            .with_context(|| format!("reading secret {id}"))?;
        Ok(contents)
    }

    async fn read_internal(&self, name: &str) -> Result<Vec<u8>, anyhow::Error> {
        let contents = fs::read(self.secrets_dir.join(internal_secret_file_name(name)))
            .await
            .with_context(|| format!("reading secret {name}"))?;
        Ok(contents)
    }
}
