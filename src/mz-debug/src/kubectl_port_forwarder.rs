// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port forwards k8s service via Kubectl

use anyhow::{Context, Result};
use k8s_openapi::api::core::v1::{Service, ServicePort};
use kube::api::ListParams;
use kube::{Api, Client};
use tokio::io::AsyncBufReadExt;

use tracing::info;

#[derive(Debug)]
pub struct KubectlPortForwarder {
    pub namespace: String,
    pub service_name: String,
    pub target_port: i32,
    pub context: Option<String>,
}

pub struct PortForwardConnection {
    // tokio process that's killed on drop
    pub _port_forward_process: tokio::process::Child,
    // We need to keep the lines otherwise the process will be killed when new lines
    // are added to the stdout.
    pub _lines: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    // The local address and port that the port forward is established on
    pub local_address: String,
    pub local_port: i32,
}

impl KubectlPortForwarder {
    /// Spawns a port forwarding process that resolves when
    /// the port forward is established.
    pub async fn spawn_port_forward(&self) -> Result<PortForwardConnection, anyhow::Error> {
        let port_arg_str = format!(":{}", &self.target_port);
        let service_name_arg_str = format!("services/{}", &self.service_name);
        let mut args = vec![
            "port-forward",
            &service_name_arg_str,
            &port_arg_str,
            "-n",
            &self.namespace,
        ];

        if let Some(k8s_context) = &self.context {
            args.extend(["--context", k8s_context]);
        }

        let child = tokio::process::Command::new("kubectl")
            .args(args)
            // Silence stderr
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn();

        if let Ok(mut child) = child {
            if let Some(stdout) = child.stdout.take() {
                let stdout_reader = tokio::io::BufReader::new(stdout);
                let mut lines = stdout_reader.lines();
                let mut local_address = None;
                let mut local_port = None;
                let local_address_and_port_regex =
                    regex::Regex::new(r"Forwarding from ([^:]+):(\d+)")?;

                // Wait until we know port forwarding is established
                let timeout = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    // kubectl-port-forward output looks like:
                    // ```
                    // Forwarding from 127.0.0.1:6875 -> 6875
                    // Forwarding from [::1]:6875 -> 6875
                    // ```
                    // We want to extract the local address and port from the first line.
                    while let Ok(Some(line)) = lines.next_line().await {
                        if let Some(captures) = local_address_and_port_regex.captures(&line) {
                            local_address = Some(captures[1].to_string());
                            local_port = captures[2].parse::<i32>().ok();
                            break;
                        }
                    }
                })
                .await;

                if timeout.is_err() {
                    return Err(anyhow::anyhow!("Port forwarding timed out after 5 seconds"));
                }

                if let (Some(local_address), Some(local_port)) = (local_address, local_port) {
                    info!(
                        "Port forwarding established for {} from ports {}:{} -> {}",
                        &self.service_name, local_address, local_port, &self.target_port
                    );
                    return Ok(PortForwardConnection {
                        _lines: lines,
                        _port_forward_process: child,
                        local_address,
                        local_port,
                    });
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to extract local address and port from kubectl-port-forward output"
                    ));
                }
            }
        }
        Err(anyhow::anyhow!("Failed to spawn port forwarding process"))
    }
}

#[derive(Debug)]
pub struct ServiceInfo {
    pub service_name: String,
    pub service_ports: Vec<ServicePort>,
    pub namespace: String,
}

/// Returns ServiceInfo for balancerd
pub async fn find_environmentd_service(
    client: &Client,
    k8s_namespaces: &Vec<String>,
) -> Result<ServiceInfo> {
    for namespace in k8s_namespaces {
        let services: Api<Service> = Api::namespaced(client.clone(), namespace);
        let services = services
            .list(&ListParams::default().labels("materialize.cloud/mz-resource-id"))
            .await
            .with_context(|| format!("Failed to list services in namespace {}", namespace))?;

        // Find the first sql service that contains balancerd
        let maybe_service =
            services
                .iter()
                .find_map(|service| match (&service.metadata.name, &service.spec) {
                    (Some(service_name), Some(spec)) => {
                        if !service_name.to_lowercase().contains("environmentd") {
                            return None;
                        }

                        if let Some(ports) = &spec.ports {
                            Some(ServiceInfo {
                                service_name: service_name.clone(),
                                service_ports: ports.clone(),
                                namespace: namespace.clone(),
                            })
                        } else {
                            None
                        }
                    }
                    _ => None,
                });

        if let Some(service) = maybe_service {
            return Ok(service);
        }
    }

    Err(anyhow::anyhow!("Could not find environmentd service"))
}

/// Returns Vec<(service_name, ports)> for cluster services
pub async fn find_cluster_services(
    client: &Client,
    k8s_namespaces: &Vec<String>,
) -> Result<Vec<ServiceInfo>> {
    for namespace in k8s_namespaces {
        let services: Api<Service> = Api::namespaced(client.clone(), namespace);
        let services = services
            .list(&ListParams::default())
            .await
            .with_context(|| format!("Failed to list services in namespace {}", namespace))?;
        let cluster_services: Vec<ServiceInfo> = services
            .iter()
            .filter_map(|service| {
                let name = service.metadata.name.clone()?;
                let spec = service.spec.clone()?;
                let selector = spec.selector?;
                let ports = spec.ports?;

                // Check if this is a cluster service
                if selector.get("environmentd.materialize.cloud/namespace")? != "cluster" {
                    return None;
                }

                Some(ServiceInfo {
                    service_name: name,
                    service_ports: ports,
                    namespace: namespace.clone(),
                })
            })
            .collect();

        if !cluster_services.is_empty() {
            return Ok(cluster_services);
        }
    }

    Err(anyhow::anyhow!("Could not find cluster services"))
}

/// Creates a port forwarder for the external pg wire port of environmentd.
pub async fn create_pg_wire_port_forwarder(
    client: &Client,
    k8s_context: &Option<String>,
    k8s_namespaces: &Vec<String>,
) -> Result<KubectlPortForwarder> {
    let service_info = find_environmentd_service(client, k8s_namespaces)
        .await
        .with_context(|| "Cannot find ports for environmentd service")?;

    let maybe_external_sql_port = service_info.service_ports.iter().find_map(|port_info| {
        if let Some(port_name) = &port_info.name {
            let port_name = port_name.to_lowercase();
            if port_name == "sql" {
                return Some(port_info);
            }
        }
        None
    });

    if let Some(external_sql_port) = maybe_external_sql_port {
        Ok(KubectlPortForwarder {
            context: k8s_context.clone(),
            namespace: service_info.namespace,
            service_name: service_info.service_name,
            target_port: external_sql_port.port,
        })
    } else {
        Err(anyhow::anyhow!(
            "No SQL port forwarding info found. Set --mz-connection-url to a Materialize instance."
        ))
    }
}
