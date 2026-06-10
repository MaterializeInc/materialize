---
source: src/orchestrator-kubernetes/src/secrets.rs
revision: e757b4d11b
---

# mz-orchestrator-kubernetes::secrets

Implements `SecretsController` for `KubernetesOrchestrator`, storing user-managed secrets as Kubernetes `Secret` objects named `<prefix>user-managed-<id>`.
Provides `KubernetesSecretsReader`, a standalone reader that retrieves secret contents from Kubernetes without requiring a full orchestrator instance.
Deletions are fire-and-forget (404 responses are silently ignored), with garbage collection delegated to a future task.
