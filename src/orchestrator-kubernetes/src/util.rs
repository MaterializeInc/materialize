// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::bail;
use k8s_openapi::api::core::v1::{PodDNSConfig, PodDNSConfigOption};
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
    let mut kubeconfig = match Config::from_kubeconfig(&kubeconfig_options).await {
        Ok(config) => config,
        Err(kubeconfig_err) => match Config::incluster_env() {
            Ok(config) => config,
            Err(in_cluster_err) => {
                bail!(
                    "failed to infer config: in-cluster: ({in_cluster_err}), kubeconfig: ({kubeconfig_err})"
                );
            }
        },
    };

    kubeconfig.connect_timeout = Some(Duration::from_secs(10));
    // The read timeout bounds how long a hung Kubernetes call can block an orchestrator worker,
    // which handles one command at a time.
    //
    // NOTE: The read timeout must exceed the connection pool's 90 s idle expiry. hyper-timeout
    // starts the read timer while a pooled connection sits idle and does not reset it when a
    // request is written. A request on a connection that idled for close to `read_timeout`
    // therefore times out before the response arrives and fails with
    // `client error (SendRequest)`.
    //
    // TODO(CPU-306): Return to 60 s once kube-client enables hyper-timeout's
    // `reset_reader_on_write`.
    kubeconfig.read_timeout = Some(Duration::from_secs(120));
    kubeconfig.write_timeout = Some(Duration::from_secs(60));

    let namespace = kubeconfig.default_namespace.clone();
    let client = Client::try_from(kubeconfig)?;
    Ok((client, namespace))
}

/// Returns the DNS configuration for `environmentd` and `clusterd` pods.
pub fn pod_dns_config() -> PodDNSConfig {
    PodDNSConfig {
        // Kubernetes sets `ndots:5`, with which the resolver tries a name with fewer than five
        // dots under every search domain before it tries the name itself. The blob store and the
        // metadata store are configured by fully qualified names with fewer dots than that, such
        // as `service.namespace.svc.cluster.local`, so each lookup starts with one failed query
        // pair per search domain. With `ndots:2` such names are queried as given.
        //
        // NOTE: A name with two to four dots that only resolves under a search domain, such as
        // `pod.service.namespace`, is now queried as given first and under the search domains
        // after that. `service` and `service.namespace` are unaffected.
        options: Some(vec![PodDNSConfigOption {
            name: Some("ndots".into()),
            value: Some("2".into()),
        }]),
        ..Default::default()
    }
}
