// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use clap::ArgEnum;

use mz_orchestrator_kubernetes::secrets::KubernetesSecretsReader;
use mz_orchestrator_process::secrets::ProcessSecretsReader;
use mz_secrets::SecretsReader;

#[derive(clap::Parser)]
pub struct SecretsReaderCliArgs {
    /// The secrets reader implementation to use.
    #[structopt(long, arg_enum)]
    secrets_reader: SecretsReaderKind,
    /// When using the process secrets reader, the directory on the filesystem
    /// where secrets are stored.
    #[structopt(long, required_if_eq("secrets-reader", "process"))]
    secrets_reader_process_dir: Option<PathBuf>,
    /// When using the Kubernetes secrets reader, the Kubernetes context to
    /// load.
    #[structopt(long, required_if_eq("secrets-reader", "kubernetes"))]
    secrets_reader_kubernetes_context: Option<String>,
}

#[derive(ArgEnum, Debug, Clone)]
enum SecretsReaderKind {
    Process,
    Kubernetes,
}

impl SecretsReaderCliArgs {
    /// Loads the secrets reader specified by the command-line arguments.
    pub async fn load(self) -> Result<Arc<dyn SecretsReader>, anyhow::Error> {
        match self.secrets_reader {
            SecretsReaderKind::Process => {
                let dir = self.secrets_reader_process_dir.expect("clap enforced");
                Ok(Arc::new(ProcessSecretsReader::new(dir)))
            }
            SecretsReaderKind::Kubernetes => {
                let context = self
                    .secrets_reader_kubernetes_context
                    .expect("clap enforced");
                Ok(Arc::new(KubernetesSecretsReader::new(context).await?))
            }
        }
    }
}
