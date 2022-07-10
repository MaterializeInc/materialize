// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use kube::config::KubeConfigOptions;
use kube::{Client, Config};

/// Constructs a new Kubernetes client.
///
/// The `context` specifies the Kubernetes context to load. If loading from the
/// context fails, the in-cluster configuration is attempted.
///
/// Returns the constructed client and the default namespace loaded from the
/// configuration.
pub async fn create_client(context: String) -> Result<(Client, String), anyhow::Error> {
    let kubeconfig_options = KubeConfigOptions {
        context: Some(context),
        ..Default::default()
    };
    let kubeconfig = match Config::from_kubeconfig(&kubeconfig_options).await {
        Ok(config) => config,
        Err(kubeconfig_err) => match Config::from_cluster_env() {
            Ok(config) => config,
            Err(in_cluster_err) => {
                bail!("failed to infer config: in-cluster: ({in_cluster_err}), kubeconfig: ({kubeconfig_err})");
            }
        },
    };
    let namespace = kubeconfig.default_namespace.clone();
    let client = Client::try_from(kubeconfig)?;
    Ok((client, namespace))
}
