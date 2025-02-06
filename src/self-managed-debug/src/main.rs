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

use chrono::{Duration, Utc};
use clap::Parser;
use k8s_openapi::api::apps::v1::{Deployment, ReplicaSet, StatefulSet};
use k8s_openapi::api::core::v1::{ContainerStatus, Pod, Service};
use k8s_openapi::api::networking::v1::{NetworkPolicy, NetworkPolicyPeer, NetworkPolicyPort};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{Api, ListParams, LogParams, ObjectMeta};
use kube::config::KubeConfigOptions;
use kube::{Client, Config};
use mz_build_info::{build_info, BuildInfo};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use tabled::{Style, Table, Tabled};

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

        let _ = match dump_k8s_get_all(client.clone(), &namespace).await {
            Ok(file_name) => Some(file_name),
            Err(e) => {
                eprintln!(
                    "Failed to write k8s get all for namespace {}: {}",
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

/**
 * Write k8s information similar to `kubectl get all` to `mz-k8s-get-all.<namespace>.log`.
 * Gets output of pods, services, replicasets, deployments, network policies, and statefulsets similar to k9s.
 * Returns the file name on success.
 */
async fn dump_k8s_get_all(client: Client, namespace: &String) -> Result<String, anyhow::Error> {
    #[derive(Tabled)]
    struct PodInfo {
        #[tabled(rename = "NAME")]
        name: String,
        #[tabled(rename = "READY")]
        ready: String,
        #[tabled(rename = "RESTARTS")]
        restarts: i32,
        #[tabled(rename = "STATUS")]
        status: String,
        #[tabled(rename = "IP")]
        ip: String,
        #[tabled(rename = "NODE")]
        node: String,
        #[tabled(rename = "AGE")]
        age: String,
    }

    #[derive(Tabled)]
    struct ServiceInfo {
        #[tabled(rename = "NAME")]
        name: String,
        #[tabled(rename = "TYPE")]
        type_: String,
        #[tabled(rename = "CLUSTER-IP")]
        cluster_ip: String,
        #[tabled(rename = "EXTERNAL-IP")]
        external_ip: String,
        #[tabled(rename = "PORT(S)")]
        ports: String,
        #[tabled(rename = "AGE")]
        age: String,
    }

    #[derive(Tabled)]
    struct DeploymentInfo {
        #[tabled(rename = "NAME")]
        name: String,
        #[tabled(rename = "READY")]
        ready: String,
        #[tabled(rename = "AGE")]
        age: String,
    }

    #[derive(Tabled)]
    struct ReplicaSetInfo {
        #[tabled(rename = "NAME")]
        name: String,
        #[tabled(rename = "READY")]
        ready: String,
        #[tabled(rename = "AGE")]
        age: String,
    }

    #[derive(Tabled)]
    struct StatefulSetInfo {
        #[tabled(rename = "NAME")]
        name: String,
        #[tabled(rename = "READY")]
        ready: String,
        #[tabled(rename = "SERVICE")]
        service: String,
        #[tabled(rename = "AGE")]
        age: String,
    }

    #[derive(Tabled)]
    struct NetworkPolicyInfo {
        #[tabled(rename = "NAME")]
        name: String,
        #[tabled(rename = "ING-PORTS")]
        ing_ports: String,
        #[tabled(rename = "ING-BLOCK")]
        ing_block: String,
        #[tabled(rename = "EGR-PORTS")]
        egress_ports: String,
        #[tabled(rename = "EGR-BLOCK")]
        egress_block: String,
        #[tabled(rename = "AGE")]
        age: String,
    }

    let file_name = format!("mz-k8s-get-all.{}.log", namespace);
    let mut file = File::create(&file_name)?;

    let pods = match Api::<Pod>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
    {
        Ok(list) => list.items,
        Err(e) => {
            eprintln!("Failed to get pods for namespace {}: {}", namespace, e);
            Vec::new()
        }
    };
    let services = match Api::<Service>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
    {
        Ok(list) => list.items,
        Err(e) => {
            eprintln!("Failed to get services for namespace {}: {}", namespace, e);
            Vec::new()
        }
    };

    let deployments = match Api::<Deployment>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
    {
        Ok(list) => list.items,
        Err(e) => {
            eprintln!(
                "Failed to get deployments for namespace {}: {}",
                namespace, e
            );
            Vec::new()
        }
    };

    let replicasets = match Api::<ReplicaSet>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
    {
        Ok(list) => list.items,
        Err(e) => {
            eprintln!(
                "Failed to get replicasets for namespace {}: {}",
                namespace, e
            );
            Vec::new()
        }
    };

    let statefulsets = match Api::<StatefulSet>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
    {
        Ok(list) => list.items,
        Err(e) => {
            eprintln!(
                "Failed to get statefulsets for namespace {}: {}",
                namespace, e
            );
            Vec::new()
        }
    };

    let network_policies = match Api::<NetworkPolicy>::namespaced(client.clone(), namespace)
        .list(&ListParams::default())
        .await
    {
        Ok(list) => list.items,
        Err(e) => {
            eprintln!(
                "Failed to get network policies for namespace {}: {}",
                namespace, e
            );
            Vec::new()
        }
    };

    enum FormatReadyRatioInput<'a> {
        Statuses(&'a Option<Vec<ContainerStatus>>),
        ReadyCount((i32, i32)), // (ready_count, total_count)
    }

    let format_ready_ratio = |input: FormatReadyRatioInput| match input {
        FormatReadyRatioInput::Statuses(container_statuses) => match container_statuses {
            Some(statuses) => format!(
                "{}/{}",
                statuses.iter().filter(|c| c.ready).count(),
                statuses.len()
            ),
            None => "N/A".to_string(),
        },
        FormatReadyRatioInput::ReadyCount((ready, total)) => format!("{}/{}", ready, total),
    };

    let format_restart_count =
        |container_statuses: &Option<Vec<ContainerStatus>>| match container_statuses {
            Some(statuses) => statuses.iter().map(|c| c.restart_count).sum(),
            None => 0,
        };

    let format_age = |metadata: &ObjectMeta| {
        let start_time = metadata.creation_timestamp.clone();
        if let Some(start_time) = start_time {
            let duration: Duration = Utc::now() - start_time.0;
            format_duration(duration)
        } else {
            "N/A".to_string()
        }
    };

    // Pods
    let pod_info: Vec<PodInfo> = pods
        .into_iter()
        .filter_map(|pod| {
            pod.metadata.name.clone().map(|name| {
                let status = pod.status.unwrap_or_default();
                PodInfo {
                    name,
                    ready: format_ready_ratio(FormatReadyRatioInput::Statuses(
                        &status.container_statuses,
                    )),
                    status: status.phase.unwrap_or_default(),
                    restarts: format_restart_count(&status.container_statuses),
                    age: format_age(&pod.metadata),
                    ip: status.pod_ip.unwrap_or_default(),
                    node: pod.spec.unwrap_or_default().node_name.unwrap_or_default(),
                }
            })
        })
        .collect();

    if !pod_info.is_empty() {
        writeln!(
            file,
            "Pods:\n{}\n",
            Table::new(pod_info).with(Style::psql())
        )?;
    }

    // Services
    let service_info: Vec<ServiceInfo> = services
        .into_iter()
        .filter_map(|svc| {
            svc.metadata.name.clone().map(|name| {
                let spec = svc.spec.unwrap_or_default();
                ServiceInfo {
                    name,
                    type_: spec.type_.unwrap_or_default(),
                    cluster_ip: spec.cluster_ip.unwrap_or_default(),
                    external_ip: spec.external_ips.unwrap_or_default().join(","),
                    ports: spec
                        .ports
                        .map(|p| {
                            p.iter()
                                .map(|port| {
                                    let mut s = String::new();
                                    if let Some(name) = &port.name {
                                        s.push_str(&format!("{}:", name));
                                    }
                                    s.push_str(&port.port.to_string());
                                    if let Some(protocol) = &port.protocol {
                                        if protocol != "TCP" {
                                            s.push_str(&format!("/{}", protocol));
                                        }
                                    }
                                    s
                                })
                                .collect::<Vec<_>>()
                                .join(",")
                        })
                        .unwrap_or_default(),
                    age: format_age(&svc.metadata),
                }
            })
        })
        .collect();
    if !service_info.is_empty() {
        writeln!(
            file,
            "Services:\n{}\n",
            Table::new(service_info).with(Style::psql())
        )?;
    }

    // // Deployments
    let deployment_info: Vec<DeploymentInfo> = deployments
        .into_iter()
        .filter_map(|deploy| {
            deploy.metadata.name.clone().map(|name| {
                let status = deploy.status.unwrap_or_default();

                DeploymentInfo {
                    name,
                    ready: format_ready_ratio(FormatReadyRatioInput::ReadyCount((
                        status.ready_replicas.unwrap_or(0),
                        status.available_replicas.unwrap_or(0),
                    ))),
                    age: format_age(&deploy.metadata),
                }
            })
        })
        .collect();
    if !deployment_info.is_empty() {
        writeln!(
            file,
            "Deployments:\n{}\n",
            Table::new(deployment_info).with(Style::psql())
        )?;
    }

    // // ReplicaSets
    let replicaset_info: Vec<ReplicaSetInfo> = replicasets
        .into_iter()
        .filter_map(|rs| {
            rs.metadata.name.clone().map(|name| {
                let status = rs.status.unwrap_or_default();
                ReplicaSetInfo {
                    name,
                    ready: format_ready_ratio(FormatReadyRatioInput::ReadyCount((
                        status.ready_replicas.unwrap_or(0),
                        status.available_replicas.unwrap_or(0),
                    ))),
                    age: format_age(&rs.metadata),
                }
            })
        })
        .collect();
    if !replicaset_info.is_empty() {
        writeln!(
            file,
            "ReplicaSets:\n{}\n",
            Table::new(replicaset_info).with(Style::psql())
        )?;
    }

    // // StatefulSets
    let statefulset_info: Vec<StatefulSetInfo> = statefulsets
        .into_iter()
        .filter_map(|sts| {
            sts.metadata.name.clone().map(|name| {
                let status = sts.status.unwrap_or_default();
                StatefulSetInfo {
                    name,
                    ready: format_ready_ratio(FormatReadyRatioInput::ReadyCount((
                        status.ready_replicas.unwrap_or(0),
                        status.available_replicas.unwrap_or(0),
                    ))),
                    service: sts.spec.unwrap_or_default().service_name,
                    age: format_age(&sts.metadata),
                }
            })
        })
        .collect();
    if !statefulset_info.is_empty() {
        writeln!(
            file,
            "StatefulSets:\n{}\n",
            Table::new(statefulset_info).with(Style::psql())
        )?;
    }

    let format_network_policy_ports = |ports: &Vec<NetworkPolicyPort>| {
        ports
            .iter()
            .filter_map(|port_info| {
                port_info.port.as_ref().map(|port| {
                    let mut s = String::new();
                    if let Some(protocol) = &port_info.protocol {
                        s.push_str(&format!("{}:", protocol));
                    }

                    let port_str = match port {
                        IntOrString::Int(i) => i.to_string(),
                        IntOrString::String(s) => s.clone(),
                    };

                    s.push_str(&port_str);
                    s
                })
            })
            .collect::<Vec<_>>()
            .join(",")
    };

    fn format_network_policy_peer(peers: &[NetworkPolicyPeer]) -> String {
        peers
            .iter()
            .filter_map(|peer| {
                peer.ip_block.as_ref().map(|ip_block| {
                    if let Some(except) = &ip_block.except {
                        format!("{}[{}]", ip_block.cidr, except.join(","))
                    } else {
                        ip_block.cidr.clone()
                    }
                })
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    // // NetworkPolicies
    let netpol_info: Vec<NetworkPolicyInfo> = network_policies
        .into_iter()
        .filter_map(|netpol| {
            netpol.metadata.name.clone().map(|name| {
                let spec = netpol.spec.unwrap_or_default();

                let ing_ports = spec.ingress.as_ref().map(|ingress| {
                    ingress
                        .iter()
                        .filter_map(|rule| rule.ports.as_ref().map(format_network_policy_ports))
                        .collect::<Vec<_>>()
                        .join(",")
                });

                let ing_block = spec.ingress.as_ref().map(|ingress| {
                    ingress
                        .iter()
                        .filter_map(|rule| {
                            rule.from
                                .as_ref()
                                .map(|from| format_network_policy_peer(from))
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                });

                let egress_ports = spec.egress.as_ref().map(|egress| {
                    egress
                        .iter()
                        .filter_map(|rule| rule.ports.as_ref().map(format_network_policy_ports))
                        .collect::<Vec<_>>()
                        .join(",")
                });

                let egress_block = spec.egress.as_ref().map(|egress| {
                    egress
                        .iter()
                        .filter_map(|rule| {
                            rule.to.as_ref().map(|to| format_network_policy_peer(to))
                        })
                        .collect::<Vec<_>>()
                        .join(",")
                });

                NetworkPolicyInfo {
                    name,
                    ing_ports: ing_ports.unwrap_or_default(),
                    ing_block: ing_block.unwrap_or_default(),
                    egress_ports: egress_ports.unwrap_or_default(),
                    egress_block: egress_block.unwrap_or_default(),
                    age: format_age(&netpol.metadata),
                }
            })
        })
        .collect();
    if !netpol_info.is_empty() {
        writeln!(
            file,
            "NetworkPolicies:\n{}\n",
            Table::new(netpol_info).with(Style::psql())
        )?;
    }

    println!("Finished exporting k8s get all for {}", namespace);
    Ok(file_name)
}

fn format_duration(duration: Duration) -> String {
    if duration <= Duration::zero() {
        return "0s".to_string();
    }
    let seconds = duration.num_seconds();

    const MINUTE_IN_SECONDS: i64 = 60;
    const HOUR_IN_SECONDS: i64 = MINUTE_IN_SECONDS * 60;
    const DAY_IN_SECONDS: i64 = HOUR_IN_SECONDS * 24;

    let units = [
        (DAY_IN_SECONDS, "d"),
        (HOUR_IN_SECONDS, "h"),
        (MINUTE_IN_SECONDS, "m"),
        (1, "s"),
    ];

    let result = {
        for window in units.windows(2) {
            let (current_unit, current_suffix) = window[0];
            let (next_unit, next_suffix) = window[1];

            if seconds >= current_unit {
                let current_unit_value = seconds / current_unit;
                let next_unit_value = (seconds % current_unit) / next_unit;
                if next_unit_value > 0 {
                    return format!(
                        "{}{}{}{}",
                        current_unit_value, current_suffix, next_unit_value, next_suffix
                    );
                }
                return format!("{}{}", current_unit_value, current_suffix);
            }
        }

        format!("{}s", seconds)
    };

    result.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[mz_ore::test]
    fn test_format_duration() {
        // Test negative duration
        assert_eq!(format_duration(Duration::try_seconds(-1).unwrap()), "0s");

        // Test zero duration
        assert_eq!(format_duration(Duration::try_seconds(0).unwrap()), "0s");

        // Test seconds only
        assert_eq!(format_duration(Duration::try_seconds(45).unwrap()), "45s");

        // Test minutes only
        assert_eq!(format_duration(Duration::try_minutes(5).unwrap()), "5m");

        // Test minutes and seconds
        assert_eq!(
            format_duration(Duration::try_minutes(5).unwrap() + Duration::try_seconds(30).unwrap()),
            "5m30s"
        );

        // Test hours only
        assert_eq!(format_duration(Duration::try_hours(2).unwrap()), "2h");

        // Test hours and minutes
        assert_eq!(
            format_duration(Duration::try_hours(2).unwrap() + Duration::try_minutes(30).unwrap()),
            "2h30m"
        );

        // Test days only
        assert_eq!(format_duration(Duration::try_days(3).unwrap()), "3d");

        // Test days and hours
        assert_eq!(
            format_duration(Duration::try_days(3).unwrap() + Duration::try_hours(12).unwrap()),
            "3d12h"
        );
    }
}
