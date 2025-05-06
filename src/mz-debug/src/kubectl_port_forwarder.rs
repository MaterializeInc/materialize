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

use anyhow::Result;
use k8s_openapi::api::core::v1::Service;
use kube::api::ListParams;
use kube::{Api, Client};
use tokio::io::AsyncBufReadExt;

use tracing::info;

use crate::SelfManagedDebugMode;
#[derive(Debug)]
pub struct KubectlPortForwarder {
    pub namespace: String,
    pub service_name: String,
    pub target_port: i32,
    pub local_address: String,
    pub local_port: i32,
    pub context: Option<String>,
}

pub struct PortForwardConnection {
    // tokio process that's killed on drop
    pub _port_forward_process: tokio::process::Child,
}

impl KubectlPortForwarder {
    /// Spawns a port forwarding process that resolves when
    /// the port forward is established.
    pub async fn spawn_port_forward(&self) -> Result<PortForwardConnection, anyhow::Error> {
        let port_arg_str = format!("{}:{}", &self.local_port, &self.target_port);
        let service_name_arg_str = format!("services/{}", &self.service_name);
        let mut args = vec![
            "port-forward",
            &service_name_arg_str,
            &port_arg_str,
            "-n",
            &self.namespace,
            "--address",
            &self.local_address,
        ];

        if let Some(k8s_context) = &self.context {
            args.extend(["--context", k8s_context]);
        }

        let child = tokio::process::Command::new("kubectl")
            .args(args)
            // Silence stdout
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn();

        if let Ok(mut child) = child {
            if let Some(stderr) = child.stderr.take() {
                let stderr_reader = tokio::io::BufReader::new(stderr);
                // Wait until we know port forwarding is established
                let timeout = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let mut lines = stderr_reader.lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        if line.contains("Forwarding from") {
                            break;
                        }
                    }
                })
                .await;

                if timeout.is_err() {
                    return Err(anyhow::anyhow!("Port forwarding timed out after 5 seconds"));
                }

                info!(
                    "Port forwarding established for {} from ports {}:{} -> {}",
                    &self.service_name, &self.local_address, &self.local_port, &self.target_port
                );

                return Ok(PortForwardConnection {
                    _port_forward_process: child,
                });
            }
        }
        Err(anyhow::anyhow!("Failed to spawn port forwarding process"))
    }
}

/// Creates a port forwarder for the external pg wire port of balancerd.
pub async fn create_pg_wire_port_forwarder(
    client: &Client,
    args: &SelfManagedDebugMode,
) -> Result<KubectlPortForwarder, anyhow::Error> {
    for namespace in &args.k8s_namespaces {
        let services: Api<Service> = Api::namespaced(client.clone(), namespace);
        let services = services
            .list(&ListParams::default().labels("materialize.cloud/mz-resource-id"))
            .await?;
        // Finds the sql service that contains a port with name "sql"
        let maybe_port_info = services
            .iter()
            .filter_map(|service| {
                let spec = service.spec.as_ref()?;
                let service_name = service.metadata.name.as_ref()?;
                if !service_name.to_lowercase().contains("balancerd") {
                    return None;
                }
                Some((spec, service_name))
            })
            .flat_map(|(spec, service_name)| {
                spec.ports
                    .iter()
                    .flatten()
                    .map(move |port| (port, service_name))
            })
            .find_map(|(port_info, service_name)| {
                if let Some(port_name) = &port_info.name {
                    // We want to find the external SQL port and not the internal one
                    if port_name.to_lowercase().contains("pgwire") {
                        return Some(KubectlPortForwarder {
                            context: args.k8s_context.clone(),
                            namespace: namespace.clone(),
                            service_name: service_name.to_owned(),
                            target_port: port_info.port,
                            local_address: args.port_forward_local_address.clone(),
                            local_port: args.port_forward_local_port,
                        });
                    }
                }

                None
            });

        if let Some(port_info) = maybe_port_info {
            return Ok(port_info);
        }
    }

    Err(anyhow::anyhow!(
        "No SQL port forwarding info found. Set --auto-port-forward to false and point --mz-connection-url to a Materialize instance."
    ))
}

pub fn create_mz_connection_url(
    local_address: String,
    local_port: i32,
    connection_url_override: Option<String>,
) -> String {
    if let Some(connection_url_override) = connection_url_override {
        return connection_url_override;
    }
    format!("postgres://{}:{}?sslmode=prefer", local_address, local_port)
}
