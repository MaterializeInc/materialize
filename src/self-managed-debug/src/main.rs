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
use std::path::PathBuf;
use std::process;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use clap::Parser;
use futures::future::join_all;
use k8s_openapi::api::core::v1::{Event, Pod};
use kube::api::{Api, ListParams, LogParams};
use kube::config::KubeConfigOptions;
use kube::{Client, Config};
use mz_build_info::{build_info, BuildInfo};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;

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
}

#[derive(Clone)]
pub struct Context {
    start_time: DateTime<Utc>,
    args: Args,
}

#[derive(Clone)]
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
    let mut describe_handles = Vec::new();

    for resource_type in [
        K8sResourceType::Pod,
        K8sResourceType::Service,
        K8sResourceType::Deployment,
        K8sResourceType::StatefulSet,
        K8sResourceType::ReplicaSet,
        K8sResourceType::NetworkPolicy,
    ] {
        for namespace in context.args.k8s_namespaces.clone() {
            // Clone the values needed inside the task
            let context = context.clone();
            let resource_type = resource_type.clone();
            let namespace = namespace.clone();

            // Spawn a new task for each describe operation
            let handle = mz_ore::task::spawn(|| "dump-kubectl-describe", async move {
                if let Err(e) = dump_kubectl_describe(&context, &namespace, &resource_type).await {
                    eprintln!(
                        "Failed to dump kubectl describe for {}: {}",
                        resource_type, e
                    );
                }
            });
            describe_handles.push(handle);
        }
    }
    match create_k8s_client(context.args.k8s_context.clone()).await {
        Ok(client) => {
            for namespace in context.args.k8s_namespaces.clone() {
                if let Err(e) = dump_k8s_pod_logs(&context, client.clone(), &namespace).await {
                    eprintln!(
                        "Failed to write k8s pod logs for namespace {}: {}",
                        namespace, e
                    );
                }

                if let Err(e) = dump_k8s_events(&context, client.clone(), &namespace).await {
                    eprintln!(
                        "Failed to write k8s events for namespace {}: {}",
                        namespace, e
                    );
                }
            }
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
        async fn export_pod_logs(
            pods: &Api<Pod>,
            pod_name: &str,
            file_path: &PathBuf,
            is_previous: bool,
        ) -> Result<(), anyhow::Error> {
            let suffix = if is_previous { "previous" } else { "current" };
            let file_name = file_path.join(format!("{}.{}.log", pod_name, suffix));

            let logs = pods
                .logs(
                    pod_name,
                    &LogParams {
                        previous: is_previous,
                        timestamps: true,
                        ..Default::default()
                    },
                )
                .await?;

            if logs.is_empty() {
                println!("No {} logs found for pod {}", suffix, pod_name);
                return Ok(());
            }

            let mut file = File::create(&file_name)?;
            file.write_all(logs.as_bytes())?;
            println!("Exported {}", file_name.display());

            Ok(())
        }

        if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, true).await {
            let print_error = || {
                eprintln!(
                    "Failed to export previous logs for pod {}: {}",
                    &pod_name, e
                );
            };

            if let Some(kube::Error::Api(e)) = e.downcast_ref::<kube::Error>() {
                if e.code == 400 {
                    eprintln!("No previous logs available for pod {}", pod_name);
                } else {
                    print_error();
                }
            } else {
                print_error();
            }
        }

        if let Err(e) = export_pod_logs(&pods, &pod_name, &file_path, false).await {
            eprintln!("Failed to export current logs for pod {}: {}", &pod_name, e);
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
    let file_name = file_path.join("events.yaml");
    create_dir_all(&file_path)?;
    let mut file = File::create(&file_name)?;

    for event in event_list {
        serde_yaml::to_writer(&mut file, &event)?;
    }

    println!("Exported {}", file_name.display());

    Ok(())
}

/// Runs `kubectl describe` for a given resource and writes the output to a file.
async fn dump_kubectl_describe(
    context: &Context,
    namespace: &String,
    resource_type: &K8sResourceType,
) -> Result<(), anyhow::Error> {
    let resource_type_str = resource_type.to_string();
    let mut args = vec!["describe", &resource_type_str, "-n", namespace];

    if let Some(k8s_context) = &context.args.k8s_context {
        args.extend(["--context", k8s_context]);
    }

    let output = tokio::process::Command::new("kubectl")
        .args(args)
        .stderr(std::process::Stdio::null()) // Silence stderr
        .output()
        .await?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "{}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    if output.stdout.is_empty() {
        println!("No {} found in namespace {}", resource_type, namespace);
        return Ok(());
    }

    let file_path = format_resource_path(context.start_time, namespace, resource_type);
    let file_name = file_path.join("describe.txt");
    create_dir_all(&file_path)?;
    let mut file = File::create(&file_name)?;
    file.write_all(&output.stdout)?;

    println!("Exported {}", file_name.display());

    Ok(())
}

fn format_resource_path(
    date_time: DateTime<Utc>,
    namespace: &str,
    resource_type: &K8sResourceType,
) -> PathBuf {
    PathBuf::from(format!(
        "mz-debug/{}/{}/{}",
        date_time.format("%Y-%m-%dT%H:%MZ"),
        resource_type,
        namespace
    ))
}
