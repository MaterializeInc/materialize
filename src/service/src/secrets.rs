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

use clap::ValueEnum;
use mz_aws_secrets_controller::AwsSecretsClient;
use mz_orchestrator_kubernetes::secrets::KubernetesSecretsReader;
use mz_orchestrator_process::secrets::ProcessSecretsReader;
use mz_secrets::SecretsReader;

#[derive(clap::Parser, Clone, Debug)]
pub struct SecretsReaderCliArgs {
    /// The secrets reader implementation to use.
    #[structopt(long, value_enum, env = "SECRETS_READER")]
    pub secrets_reader: SecretsControllerKind,
    /// When using the process secrets reader, the directory on the filesystem
    /// where secrets are stored.
    #[structopt(
        long,
        required_if_eq("secrets_reader", "local-file"),
        env = "SECRETS_READER_LOCAL_FILE_DIR"
    )]
    pub secrets_reader_local_file_dir: Option<PathBuf>,
    /// When using the Kubernetes secrets reader, the Kubernetes context to
    /// load.
    #[structopt(
        long,
        required_if_eq("secrets_reader", "kubernetes"),
        env = "SECRETS_READER_KUBERNETES_CONTEXT"
    )]
    pub secrets_reader_kubernetes_context: Option<String>,
    /// When using the AWS secrets reader, we need both of the following.
    #[structopt(
        long,
        required_if_eq("secrets_reader", "aws-secrets-manager"),
        env = "SECRETS_READER_AWS_PREFIX"
    )]
    pub secrets_reader_aws_prefix: Option<String>,
    /// When using the Kubernetes secrets reader, the prefix to use for secret
    /// names.
    #[structopt(long, env = "SECRETS_READER_NAME_PREFIX")]
    pub secrets_reader_name_prefix: Option<String>,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum SecretsControllerKind {
    LocalFile,
    Kubernetes,
    AwsSecretsManager,
}

impl SecretsReaderCliArgs {
    /// Loads the secrets reader specified by the command-line arguments.
    pub async fn load(self) -> Result<Arc<dyn SecretsReader>, anyhow::Error> {
        match self.secrets_reader {
            SecretsControllerKind::LocalFile => {
                let dir = self.secrets_reader_local_file_dir.expect("clap enforced");
                Ok(Arc::new(ProcessSecretsReader::new(dir)))
            }
            SecretsControllerKind::Kubernetes => {
                let context = self
                    .secrets_reader_kubernetes_context
                    .expect("clap enforced");
                Ok(Arc::new(
                    KubernetesSecretsReader::new(context, self.secrets_reader_name_prefix).await?,
                ))
            }
            SecretsControllerKind::AwsSecretsManager => {
                let prefix = self.secrets_reader_aws_prefix.expect("clap enforced");
                Ok(Arc::new(AwsSecretsClient::new(&prefix).await))
            }
        }
    }

    /// Turn this struct back into arguments. Useful for passing through to other services.
    ///
    /// Expects the correct arguments to be filled in, based on the `clap` requirements.
    pub fn to_flags(&self) -> Vec<String> {
        match self.secrets_reader {
            SecretsControllerKind::LocalFile => {
                vec![
                    "--secrets-reader=local-file".to_string(),
                    format!(
                        "--secrets-reader-local-file-dir={}",
                        self.secrets_reader_local_file_dir
                            .as_ref()
                            .expect("initialized correctly")
                            .display()
                    ),
                ]
            }
            SecretsControllerKind::Kubernetes => {
                vec![
                    "--secrets-reader=kubernetes".to_string(),
                    format!(
                        "--secrets-reader-kubernetes-context={}",
                        self.secrets_reader_kubernetes_context
                            .as_ref()
                            .expect("initialized correctly")
                    ),
                ]
            }
            SecretsControllerKind::AwsSecretsManager => {
                vec![
                    "--secrets-reader=aws-secrets-manager".to_string(),
                    format!(
                        "--secrets-reader-aws-prefix={}",
                        self.secrets_reader_aws_prefix
                            .as_ref()
                            .expect("initialized correctly")
                    ),
                ]
            }
        }
    }
}
