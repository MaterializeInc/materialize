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
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{Event, Node, Pod, Service};
use k8s_openapi::api::networking::v1::NetworkPolicy;
use k8s_openapi::ListableResource;
use kube::api::{Api, ListParams, LogParams};
use kube::config::KubeConfigOptions;
use kube::{Client, Config, Resource};
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
    Event,
    Pod,
    Service,
    Deployment,
    StatefulSet,
    ReplicaSet,
    NetworkPolicy,
    Node,
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
            K8sResourceType::Event => "events",
            K8sResourceType::Node => "nodes",
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

    let mut spawn_kubectl_describe_process =
        |resource_type: &K8sResourceType, namespace: Option<String>| {
            let context = context.clone();
            let resource_type = resource_type.clone();
            let namespace = namespace.clone();

            // Spawn a new task for each describe operation
            let handle = mz_ore::task::spawn(|| "dump-kubectl-describe", async move {
                if let Err(e) =
                    dump_kubectl_describe(&context, &resource_type, namespace.as_ref()).await
                {
                    eprintln!(
                        "Failed to dump kubectl describe for {}: {}",
                        resource_type, e
                    );
                }
            });
            describe_handles.push(handle);
        };

    for resource_type in [
        K8sResourceType::Pod,
        K8sResourceType::Service,
        K8sResourceType::Deployment,
        K8sResourceType::StatefulSet,
        K8sResourceType::ReplicaSet,
        K8sResourceType::NetworkPolicy,
        K8sResourceType::Event,
    ] {
        for namespace in context.args.k8s_namespaces.clone() {
            spawn_kubectl_describe_process(&resource_type, Some(namespace));
        }
    }

    spawn_kubectl_describe_process(&K8sResourceType::Node, None);

    match create_k8s_client(context.args.k8s_context.clone()).await {
        Ok(client) => {
            for namespace in context.args.k8s_namespaces.clone() {
                if let Err(e) = dump_k8s_pod_logs(&context, client.clone(), &namespace).await {
                    eprintln!(
                        "Failed to write k8s pod logs for namespace {}: {}",
                        namespace, e
                    );
                }

                let events_api = Api::<Event>::namespaced(client.clone(), &namespace);
                if let Err(e) = dump_k8s_resources(
                    &context,
                    events_api,
                    &K8sResourceType::Event,
                    Some(&namespace),
                )
                .await
                {
                    eprintln!(
                        "Failed to write k8s events for namespace {}: {}",
                        namespace, e
                    );
                }
                let pods_api = Api::<Pod>::namespaced(client.clone(), &namespace);
                if let Err(e) =
                    dump_k8s_resources(&context, pods_api, &K8sResourceType::Pod, Some(&namespace))
                        .await
                {
                    eprintln!(
                        "Failed to write k8s pods for namespace {}: {}",
                        namespace, e
                    );
                }
                let services_api = Api::<Service>::namespaced(client.clone(), &namespace);
                if let Err(e) = dump_k8s_resources(
                    &context,
                    services_api,
                    &K8sResourceType::Service,
                    Some(&namespace),
                )
                .await
                {
                    eprintln!(
                        "Failed to write k8s services for namespace {}: {}",
                        namespace, e
                    );
                }
                let deployments_api = Api::<Deployment>::namespaced(client.clone(), &namespace);
                if let Err(e) = dump_k8s_resources(
                    &context,
                    deployments_api,
                    &K8sResourceType::Deployment,
                    Some(&namespace),
                )
                .await
                {
                    eprintln!(
                        "Failed to write k8s deployments for namespace {}: {}",
                        namespace, e
                    );
                }
                let statefulsets_api = Api::<StatefulSet>::namespaced(client.clone(), &namespace);
                if let Err(e) = dump_k8s_resources(
                    &context,
                    statefulsets_api,
                    &K8sResourceType::StatefulSet,
                    Some(&namespace),
                )
                .await
                {
                    eprintln!(
                        "Failed to write k8s statefulsets for namespace {}: {}",
                        namespace, e
                    );
                }
                let replicasets_api = Api::<ReplicaSet>::namespaced(client.clone(), &namespace);
                if let Err(e) = dump_k8s_resources(
                    &context,
                    replicasets_api,
                    &K8sResourceType::ReplicaSet,
                    Some(&namespace),
                )
                .await
                {
                    eprintln!(
                        "Failed to write k8s replicasets for namespace {}: {}",
                        namespace, e
                    );
                }
                let networkpolicies_api =
                    Api::<NetworkPolicy>::namespaced(client.clone(), &namespace);
                if let Err(e) = dump_k8s_resources(
                    &context,
                    networkpolicies_api,
                    &K8sResourceType::NetworkPolicy,
                    Some(&namespace),
                )
                .await
                {
                    eprintln!(
                        "Failed to write k8s networkpolicies for namespace {}: {}",
                        namespace, e
                    );
                }
            }
            let nodes_api = Api::<Node>::all(client.clone());
            if let Err(e) =
                dump_k8s_resources(&context, nodes_api, &K8sResourceType::Node, None).await
            {
                eprintln!("Failed to write k8s nodes: {}", e);
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
    let file_path = format_resource_path(context.start_time, "logs", Some(namespace));
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
                eprintln!("No {} logs found for pod {}", suffix, pod_name);
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

async fn dump_k8s_resources<T>(
    context: &Context,
    api: Api<T>,
    resource_type: &K8sResourceType,
    namespace: Option<&String>,
) -> Result<(), anyhow::Error>
where
    T: ListableResource,
    T: Resource<DynamicType = ()>,
    T: Clone,
    T: std::fmt::Debug,
    T: serde::Serialize,
    T: serde::de::DeserializeOwned,
{
    let object_list = api.list(&ListParams::default()).await?;

    if object_list.items.is_empty() {
        let mut err_msg = format!("No {} found", resource_type);
        if let Some(namespace) = namespace {
            err_msg = format!("{} for namespace {}", err_msg, namespace);
        }
        println!("{}", err_msg);
        return Ok(());
    }
    let file_path = format_resource_path(context.start_time, &resource_type.to_string(), namespace);
    create_dir_all(&file_path)?;

    // for object in &object_list.items {
    //     let file_name = file_path.join(format!("{}.yaml", resource_type));
    //     let mut file = File::create(&file_name)?;
    //     serde_yaml::to_writer(&mut file, &object)?;
    //     println!("Exported {}", file_name.display());
    // }
    create_dir_all(&file_path)?;
    for item in &object_list.items {
        let file_name = file_path.join(format!(
            "{}.yaml",
            &item.meta().name.clone().unwrap_or_default()
        ));
        let mut file = File::create(&file_name)?;
        serde_yaml::to_writer(&mut file, &item)?;
        println!("Exporting {}", file_name.display());
    }

    Ok(())
}

/// Runs `kubectl describe` for a given resource and writes the output to a file.
async fn dump_kubectl_describe(
    context: &Context,
    resource_type: &K8sResourceType,
    namespace: Option<&String>,
) -> Result<(), anyhow::Error> {
    let resource_type_str = resource_type.to_string();
    let mut args = vec!["describe", &resource_type_str];
    if let Some(namespace) = namespace {
        args.extend(["-n", namespace]);
    }

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
        let mut err_msg = format!("Describe: No {} found", resource_type);
        if let Some(namespace) = namespace {
            err_msg = format!("{} for namespace {}", err_msg, namespace);
        }
        eprintln!("{}", err_msg);
        return Ok(());
    }

    let file_path = format_resource_path(context.start_time, &resource_type.to_string(), namespace);
    let file_name = file_path.join("describe.txt");
    create_dir_all(&file_path)?;
    let mut file = File::create(&file_name)?;
    file.write_all(&output.stdout)?;

    println!("Exported {}", file_name.display());

    Ok(())
}

fn format_resource_path(
    date_time: DateTime<Utc>,
    resource_type: &str,
    namespace: Option<&String>,
) -> PathBuf {
    let mut path = PathBuf::from(format!(
        "mz-debug/{}/{}",
        date_time.format("%Y-%m-%dT%H:%MZ"),
        resource_type,
    ));
    if let Some(namespace) = namespace {
        path = path.join(namespace);
    }
    path
}
