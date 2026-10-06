// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_aws_secrets_controller::AwsSecretsClient;
use mz_orchestrator_kubernetes::secrets::KubernetesSecretsReader;
use mz_orchestrator_process::secrets::ProcessSecretsReader;
use mz_secrets::SecretsReader;
use mz_secrets_cli::{SecretsControllerKind, SecretsReaderCliArgs};

/// Loads the secrets reader specified by the command-line arguments.
pub async fn load(args: SecretsReaderCliArgs) -> Result<Arc<dyn SecretsReader>, anyhow::Error> {
    match args.secrets_reader {
        SecretsControllerKind::LocalFile => {
            let dir = args.secrets_reader_local_file_dir.expect("clap enforced");
            Ok(Arc::new(ProcessSecretsReader::new(dir)))
        }
        SecretsControllerKind::Kubernetes => {
            let context = args
                .secrets_reader_kubernetes_context
                .expect("clap enforced");
            Ok(Arc::new(
                KubernetesSecretsReader::new(context, args.secrets_reader_name_prefix).await?,
            ))
        }
        SecretsControllerKind::AwsSecretsManager => {
            let prefix = args.secrets_reader_aws_prefix.expect("clap enforced");
            Ok(Arc::new(AwsSecretsClient::new(&prefix).await))
        }
    }
}
