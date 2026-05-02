---
source: src/service/src/secrets.rs
revision: 82d92a7fad
---

# mz-service::secrets

Provides `SecretsReaderCliArgs`, a `clap`-derived struct for selecting and constructing a `SecretsReader` from three backends: local file, Kubernetes, or AWS Secrets Manager.
`load` builds the concrete `Arc<dyn SecretsReader>` and `to_flags` serializes the args back to CLI flag strings for pass-through to child services.
