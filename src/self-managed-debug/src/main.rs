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
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::process;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use clap::Parser;
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::api::{Api, ListParams, LogParams};
use kube::config::KubeConfigOptions;
use kube::{Client, Config};
use mz_build_info::{build_info, BuildInfo};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;

pub const BUILD_INFO: BuildInfo = build_info!();
pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(Parser, Debug)]
#[clap(name = "self-managed-debug", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    // === Kubernetes options. ===
    #[clap(long = "k8s-context", env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    #[clap(long = "k8s-namespace", required = true, action = clap::ArgAction::Append)]
    k8s_namespaces: Vec<String>,
}

pub struct Context {
    start_time: DateTime<Utc>,
    args: Args,
}

pub enum K8sResourceType {
    Pod,
    Service,
    Deployment,
    StatefulSet,
    ReplicaSet,
    NetworkPolicy,
    Log,
    Event,
}

impl std::fmt::Display for K8sResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            K8sResourceType::Pod => "pods",
            K8sResourceType::Service => "services",
            K8sResourceType::Deployment => "deployments",
            K8sResourceType::StatefulSet => "statefulsets",
            K8sResourceType::ReplicaSet => "replicasets",
            K8sResourceType::NetworkPolicy => "networkpolicies",
            K8sResourceType::Log => "logs",
            K8sResourceType::Event => "events",
        };
        write!(f, "{}", s)
    }
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
    match create_k8s_client(context.args.k8s_context.clone()).await {
        Ok(client) => {
            for namespace in context.args.k8s_namespaces.clone() {
                let _ = match dump_k8s_pod_logs(&context, client.clone(), &namespace).await {
                    Ok(_) => Some(()),
                    Err(e) => {
                        eprintln!(
                            "Failed to write k8s pod logs for namespace {}: {}",
                            namespace, e
                        );
                        if let Some(_) = e.downcast_ref::<kube::Error>() {
                            eprintln!("Ensure that {} exists.", namespace);
                        }
                        None
                    }
                };

                let _ = match dump_k8s_events(&context, client.clone(), &namespace).await {
                    Ok(_) => Some(()),
                    Err(e) => {
                        eprintln!(
                            "Failed to write k8s events for namespace {}: {}",
                            namespace, e
                        );
                        None
                    }
                };
            }
        }
        Err(e) => {
            eprintln!("Failed to create k8s client: {}", e);
        }
    };
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

/// Write k8s pod logs to a file per pod.
async fn dump_k8s_pod_logs(
    context: &Context,
    client: Client,
    namespace: &String,
) -> Result<(), anyhow::Error> {
    let file_path = format_resource_path(context.start_time, namespace, &K8sResourceType::Log);
    create_dir_all(&file_path)?;

    let pods: Api<Pod> = Api::<Pod>::namespaced(client.clone(), namespace);

    let pod_list = pods.list(&ListParams::default()).await?;

    for pod in &pod_list.items {
        let pod_name = pod.metadata.name.clone().unwrap_or_default();
        let previous_logs_file_name = format!("{}/{}.previous.log", file_path, pod_name);
        let current_logs_file_name = format!("{}/{}.current.log", file_path, pod_name);

        if let Err(e) = async {
            let previous_logs = pods
                .logs(
                    &pod_name,
                    &LogParams {
                        previous: true,
                        timestamps: true,
                        ..Default::default()
                    },
                )
                .await?;

            if previous_logs.is_empty() {
                println!("No previous logs found for pod {}", pod_name);
                return Ok(());
            }

            let mut file = File::create(&previous_logs_file_name)?;

            file.write_all(previous_logs.as_bytes())?;

            println!("Exported {}", &previous_logs_file_name);
            Ok::<(), anyhow::Error>(())
        }
        .await
        {
            eprintln!("Failed to export {}: {}", &previous_logs_file_name, e);
        }

        if let Err(e) = async {
            let logs = pods
                .logs(
                    &pod_name,
                    &LogParams {
                        timestamps: true,
                        ..Default::default()
                    },
                )
                .await?;

            if logs.is_empty() {
                println!("No logs found for pod {}", pod_name);
                return Ok(());
            }
            let mut file = File::create(&current_logs_file_name)?;

            file.write_all(logs.as_bytes())?;

            println!("Exported {}", &current_logs_file_name);
            Ok::<(), anyhow::Error>(())
        }
        .await
        {
            eprintln!("Failed to export {}: {}", &current_logs_file_name, e);
        }
    }
    Ok(())
}

/// Write k8s events to a yaml file.
async fn dump_k8s_events(
    context: &Context,
    client: Client,
    namespace: &String,
) -> Result<(), anyhow::Error> {
    let events: Api<Event> = Api::<Event>::namespaced(client.clone(), namespace);

    let event_list = events.list(&ListParams::default()).await?;

    if event_list.items.is_empty() {
        println!("No events found for namespace {}", namespace);
        return Ok(());
    }

    let file_path = format_resource_path(context.start_time, namespace, &K8sResourceType::Event);
    let file_name = format!("{}/events.yaml", file_path);
    create_dir_all(&file_path)?;
    let mut file = File::options().append(true).create(true).open(&file_name)?;

    for event in event_list {
        serde_yaml::to_writer(&mut file, &event)?;
    }

    println!("Exported {}", &file_name);

    Ok(())
}

fn format_resource_path(
    date_time: DateTime<Utc>,
    namespace: &str,
    resource_type: &K8sResourceType,
) -> String {
    format!(
        "mz-debug/{}/{}/{}",
        date_time.format("%Y-%m-%dT%H:%MZ"),
        resource_type,
        namespace
    )
}
