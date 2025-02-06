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
use std::fs::File;
use std::io::Write;
use std::process;
use std::sync::LazyLock;

use clap::Parser;
use k8s_openapi::api::core::v1::Pod;
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
    #[clap(long, env = "KUBERNETES_CONTEXT")]
    kubernetes_context: Option<String>,
    #[clap(long, value_delimiter = ',', num_args = 1..)]
    kubernetes_namespaces: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("SELF_MANAGED_DEBUG_"),
        enable_version_flag: true,
    });

    if let Err(err) = run(args).await {
        eprintln!(
            "self-managed-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let client = create_k8s_client(args.kubernetes_context.clone()).await?;
    // TODO: Make namespaces mandatory
    // TODO: Print a warning if namespace doesn't exist
    for namespace in args.kubernetes_namespaces {
        let _ = match dump_k8s_pod_logs(client.clone(), &namespace).await {
            Ok(file_names) => Some(file_names),
            Err(e) => {
                eprintln!(
                    "Failed to write k8s pod logs for namespace {}: {}",
                    namespace, e
                );
                None
            }
        };
    }

    // TODO: Compress files to ZIP
    Ok(())
}

/// Creates a k8s client given a context. If no context is provided, the default context is used.
async fn create_k8s_client(context: Option<String>) -> Result<Client, anyhow::Error> {
    let kubeconfig_options = KubeConfigOptions {
        context,
        ..Default::default()
    };

    let kubeconfig = Config::from_kubeconfig(&kubeconfig_options).await?;

    let client = Client::try_from(kubeconfig)?;

    Ok(client)
}

/// Write k8s pod logs to a file per pod as mz-pod-logs.<namespace>.<pod-name>.log.
/// Returns a list of file names on success.
async fn dump_k8s_pod_logs(
    client: Client,
    namespace: &String,
) -> Result<Vec<String>, anyhow::Error> {
    let mut file_names = Vec::new();

    let pods: Api<Pod> = Api::<Pod>::namespaced(client.clone(), namespace);

    let pod_list = pods.list(&ListParams::default()).await?;

    for pod in &pod_list.items {
        if let Err(e) = async {
            let pod_name = pod.metadata.name.clone().unwrap_or_default();
            let file_name = format!("mz-pod-logs.{}.{}.log", namespace, pod_name);
            let mut file = File::create(&file_name)?;

            let logs = match pods
                .logs(
                    &pod_name,
                    &LogParams {
                        previous: true,
                        timestamps: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(logs) => logs,
                Err(_) => {
                    // If we get a bad request error, try without the previous flag.
                    pods.logs(
                        &pod_name,
                        &LogParams {
                            timestamps: true,
                            ..Default::default()
                        },
                    )
                    .await?
                }
            };

            for line in logs.lines() {
                writeln!(file, "{}", line)?;
            }

            println!("Finished exporting logs for {}", pod_name);
            file_names.push(file_name);
            Ok::<(), anyhow::Error>(())
        }
        .await
        {
            let pod_name = pod.metadata.name.clone().unwrap_or_default();
            eprintln!("Failed to process pod {}: {}", pod_name, e);
        }
    }
    Ok(file_names)
}
