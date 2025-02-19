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
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;

mod k8s_resource_dumper;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(Parser, Debug, Clone)]
#[clap(name = "self-managed-debug", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    // === Kubernetes options. ===
    #[clap(long = "k8s-context", env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    #[clap(long = "k8s-namespace", required = true, action = clap::ArgAction::Append)]
    k8s_namespaces: Vec<String>,
    #[clap(long = "k8s-dump-secret-values", action = clap::ArgAction::SetTrue)]
    k8s_dump_secret_values: bool,
}

#[derive(Clone)]
pub struct Context {
    start_time: DateTime<Utc>,
    args: Args,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("SELF_MANAGED_DEBUG_"),
        enable_version_flag: true,
    });

    let start_time = Utc::now();

    let context = Context { start_time, args };

    if let Err(err) = run(context).await {
        eprintln!(
            "self-managed-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

async fn run(context: Context) -> Result<(), anyhow::Error> {
    let mut describe_handles = Vec::new();

    for namespace in context.args.k8s_namespaces.clone() {
        describe_handles.extend([
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
        ]);
    }

    describe_handles.extend([
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

    match create_k8s_client(context.args.k8s_context.clone()).await {
        Ok(client) => {
            for namespace in context.args.k8s_namespaces.clone() {
                k8s_resource_dumper::dump_namespaced_resources(&context, &client, namespace).await;
            }
            k8s_resource_dumper::dump_cluster_resources(&context, &client).await;
        }
        Err(e) => {
            eprintln!("Failed to create k8s client: {}", e);
        }
    };

    join_all(describe_handles).await;

    // TODO: Compress files to ZIP
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
