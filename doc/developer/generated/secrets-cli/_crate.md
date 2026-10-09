---
source: src/secrets-cli/src/lib.rs
revision: c0f89a5887
---

# mz-secrets-cli

CLI argument types for configuring the secrets reader backend used by Materialize services.
Provides `SecretsReaderCliArgs` (a `clap::Parser` struct with `--secrets-reader`, `--secrets-reader-local-file-dir`, `--secrets-reader-kubernetes-context`, `--secrets-reader-aws-prefix`, and `--secrets-reader-name-prefix` flags) and `SecretsControllerKind` (an enum with `LocalFile`, `Kubernetes`, and `AwsSecretsManager` variants).
`SecretsReaderCliArgs::load` constructs the appropriate `Arc<dyn SecretsReader>` from the parsed arguments: `LocalFile` uses `ProcessSecretsReader`, `Kubernetes` uses `KubernetesSecretsReader`, and `AwsSecretsManager` uses `AwsSecretsClient`.
`SecretsReaderCliArgs::to_flags` serializes the args back into a `Vec<String>` of CLI flags for forwarding to child processes.
