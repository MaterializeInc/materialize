// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Debug tool for self managed environments.

use std::fmt::Debug;
use std::process;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use clap::Parser;
use futures::future::join_all;
use k8s_openapi::api::admissionregistration::v1::{
    MutatingWebhookConfiguration, ValidatingWebhookConfiguration,
};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Node, PersistentVolume, PersistentVolumeClaim, Pod, Secret, Service,
    ServiceAccount,
};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::api::rbac::v1::{Role, RoleBinding};
use k8s_openapi::api::storage::v1::StorageClass;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::config::KubeConfigOptions;
use kube::{Client, Config};
use mz_build_info::{build_info, BuildInfo};
use mz_cloud_resources::crd::gen::cert_manager::certificates::Certificate;
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use tracing::error;
use tracing_subscriber::EnvFilter;

use crate::system_catalog_dumper::create_postgres_connection_string;

mod k8s_resource_dumper;
mod system_catalog_dumper;
mod utils;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(Parser, Debug, Clone)]
#[clap(name = "self-managed-debug", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    // === Kubernetes options. ===
    #[clap(long, env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    #[clap(long= "k8s-namespace", required = true, action = clap::ArgAction::Append)]
    k8s_namespaces: Vec<String>,
    #[clap(long , action = clap::ArgAction::SetTrue)]
    k8s_dump_secret_values: bool,
    // === Port forwarding options. ===
    /// If true, the tool will not attempt to port-forward the SQL port.
    #[clap(long , action = clap::ArgAction::SetTrue)]
    skip_port_forward: bool,
    /// The kubernetes service and port with the SQL connection we want to port-forward to
    /// By default, we will attempt to find both by looking for an environmentd service with a port named "internal sql"
    #[clap(long, requires = "sql_target_port")]
    sql_target_service: Option<String>,
    #[clap(long, requires = "sql_target_service")]
    sql_target_port: Option<i32>,
    /// The port that will be forwarded to the target port.
    /// By default, this will be the same as the target port.
    #[clap(long)]
    sql_local_port: Option<i32>,
    /// The address string to bind the local port to. e.g. "0.0.0.0"
    /// By default, this will be "localhost".
    #[clap(long)]
    sql_local_address: Option<String>,
}

#[derive(Clone)]
pub struct Context {
    start_time: DateTime<Utc>,
    args: Args,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("mz_self_managed_debug=info"))
        .without_time()
        .with_target(false)
        .init();

    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("SELF_MANAGED_DEBUG_"),
        enable_version_flag: true,
    });

    let start_time = Utc::now();

    let context = Context { start_time, args };

    if let Err(err) = run(context).await {
        error!(
            "self-managed-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

async fn run(context: Context) -> Result<(), anyhow::Error> {
    let mut handles = Vec::new();

    for namespace in context.args.k8s_namespaces.clone() {
        handles.extend([
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Pod>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Service>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Deployment>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<StatefulSet>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<ReplicaSet>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<NetworkPolicy>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Event>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Materialize>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Role>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<RoleBinding>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<ConfigMap>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Secret>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<PersistentVolumeClaim>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<ServiceAccount>(
                context.clone(),
                Some(namespace.clone()),
            ),
            k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Certificate>(
                context.clone(),
                Some(namespace.clone()),
            ),
        ]);
    }

    handles.extend([
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<Node>(context.clone(), None),
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<DaemonSet>(
            context.clone(),
            None,
        ),
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<StorageClass>(
            context.clone(),
            None,
        ),
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<PersistentVolume>(
            context.clone(),
            None,
        ),
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<MutatingWebhookConfiguration>(
            context.clone(),
            None,
        ),
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<ValidatingWebhookConfiguration>(
            context.clone(),
            None,
        ),
        k8s_resource_dumper::spawn_dump_kubectl_describe_process::<CustomResourceDefinition>(
            context.clone(),
            None,
        ),
    ]);

    let client = match create_k8s_client(context.args.k8s_context.clone()).await {
        Ok(client) => client,
        Err(e) => {
            error!("Failed to create k8s client: {}", e);
            return Err(e);
        }
    };

    for namespace in context.args.k8s_namespaces.clone() {
        k8s_resource_dumper::dump_namespaced_resources(&context, &client, namespace).await;
    }
    k8s_resource_dumper::dump_cluster_resources(&context, &client).await;

    let _port_forward_handle;
    let mut host_port: Option<i32> = None;
    let mut target_port: Option<i32> = None;
    if !context.args.skip_port_forward {
        // Find the namespace, service name, local port, and target port.
        let port_forwarding_info =
            system_catalog_dumper::get_sql_port_forwarding_info(&client, &context.args).await?;
        host_port = Some(port_forwarding_info.local_port);
        target_port = Some(port_forwarding_info.target_port);

        _port_forward_handle = system_catalog_dumper::spawn_sql_port_forwarding_process(
            &port_forwarding_info,
            &context.args,
        );
        // There may be a delay between when the port forwarding process starts and when it's ready to use.
        // We wait a few seconds to ensure that port forwarding is ready.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    let catalog_dumper = match system_catalog_dumper::SystemCatalogDumper::new(
        &context,
        &create_postgres_connection_string(
            context.args.sql_local_address.as_deref(),
            host_port,
            target_port,
        ),
    )
    .await
    {
        Ok(dumper) => Some(dumper),
        Err(e) => {
            error!("Failed to dump system catalog: {}", e);
            None
        }
    };

    if let Some(dumper) = catalog_dumper {
        dumper.dump_all_relations().await;
    }

    join_all(handles).await;
    // TODO(debug_tool1): Compress files to ZIP
    Ok(())
}

/// Creates a k8s client given a context. If no context is provided, the default context is used.
async fn create_k8s_client(k8s_context: Option<String>) -> Result<Client, anyhow::Error> {
    let kubeconfig_options = KubeConfigOptions {
        context: k8s_context,
        ..Default::default()
    };

    let kubeconfig = Config::from_kubeconfig(&kubeconfig_options).await?;

    let client = Client::try_from(kubeconfig)?;

    Ok(client)
}
