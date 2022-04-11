// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Error};
use kube::config::KubeConfigOptions;
use kube::{Client, Config};
use mz_secrets::{SecretOp, SecretsController};

pub struct KubernetesSecretsController {
    kube_client: Client,
}

impl KubernetesSecretsController {
    pub async fn new(context: String) -> Result<KubernetesSecretsController, anyhow::Error> {
        let kubeconfig_options = KubeConfigOptions {
            context: Some(context),
            ..Default::default()
        };

        let kubeconfig = match Config::from_kubeconfig(&kubeconfig_options).await {
            Ok(config) => config,
            Err(kubeconfig_err) => match Config::from_cluster_env() {
                Ok(config) => config,
                Err(in_cluster_err) => {
                    // bail!("failed to infer config: in-cluster: ({in_cluster_err}), kubeconfig: ({kubeconfig_err})");
                    todo!()
                }
            },
        };
        let client = Client::try_from(kubeconfig)?;

        Ok(KubernetesSecretsController {
            kube_client: client,
        })
    }
}

impl SecretsController for KubernetesSecretsController {
    fn apply(&mut self, ops: Vec<SecretOp>) -> Result<(), Error> {
        return Ok(());
    }
}
