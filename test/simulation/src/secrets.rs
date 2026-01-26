use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use mz_orchestrator_process::secrets::ProcessSecretsReader;
use mz_repr::CatalogItemId;
use mz_secrets::{SecretsController, SecretsReader};
use tempfile::TempDir;

#[derive(Debug)]
pub(crate) struct FileSecretsController {
    secrets_dir: TempDir,
}

impl FileSecretsController {
    pub fn new() -> Self {
        Self {
            secrets_dir: tempfile::tempdir().unwrap(),
        }
    }
    
    pub fn path(&self) -> PathBuf {
        self.secrets_dir.path().into()
    }
}

#[async_trait]
impl SecretsController for FileSecretsController {
    async fn ensure(&self, id: CatalogItemId, contents: &[u8]) -> Result<(), anyhow::Error> {
        let file_path = self.secrets_dir.path().join(id.to_string());
        std::fs::write(file_path, contents)?;
        Ok(())
    }

    async fn delete(&self, id: CatalogItemId) -> Result<(), anyhow::Error> {
        let file_path = self.secrets_dir.path().join(id.to_string());
        std::fs::remove_file(file_path)?;
        Ok(())
    }

    async fn list(&self) -> Result<Vec<CatalogItemId>, anyhow::Error> {
        let mut ids = Vec::new();
        let mut entries = std::fs::read_dir(&self.secrets_dir)?;
        while let Some(entry) = entries.next() {
            let entry = entry?;
            let id: CatalogItemId = entry.file_name().to_string_lossy().parse()?;
            ids.push(id);
        }
        Ok(ids)
    }

    fn reader(&self) -> Arc<dyn SecretsReader> {
        Arc::new(ProcessSecretsReader::new(self.secrets_dir.path().into()))
    }
}
