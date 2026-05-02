---
source: src/orchestrator-process/src/secrets.rs
revision: 235d12a7d9
---

# mz-orchestrator-process::secrets

Implements `SecretsController` and `SecretsReader` for the process orchestrator using the local filesystem.
`ProcessOrchestrator` writes secrets as mode-0600 files named by `CatalogItemId` under `secrets_dir`, and deletes or lists them with standard tokio filesystem operations.
`ProcessSecretsReader` reads secret contents from the same directory and can be constructed independently for use in spawned child processes.
