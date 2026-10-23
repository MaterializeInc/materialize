---
source: src/secrets-cli/src/lib.rs
revision: 344aa3cdd0
---

# mz-secrets-cli

CLI argument types for configuring the secrets reader backend used by Materialize services.
Provides `SecretsReaderCliArgs` (a `clap::Parser` struct with `--secrets-reader`, `--secrets-reader-local-file-dir`, `--secrets-reader-kubernetes-context`, `--secrets-reader-aws-prefix`, and `--secrets-reader-name-prefix` flags) and `SecretsControllerKind` (an enum with `LocalFile`, `Kubernetes`, and `AwsSecretsManager` variants).
`SecretsReaderCliArgs::to_flags` serializes the args back into a `Vec<String>` of CLI flags for forwarding to child processes.
Loading the secrets reader from these args is handled by `mz_secrets_loader::load`, not by a method on `SecretsReaderCliArgs`.
