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

use std::time::Duration;

use mz_ore::retry::{self, RetryResult};
use tracing::{error, info};

use crate::SelfManagedDebugMode;
#[derive(Debug, Clone)]
pub struct KubectlPortForwarder {
    pub namespace: String,
    pub service_name: String,
    pub target_port: i32,
    pub local_address: String,
    pub local_port: i32,
    pub context: Option<String>,
}

impl KubectlPortForwarder {
    /// Port forwards a given k8s service via Kubectl.
    /// The process will retry if the port-forwarding fails and
    /// will terminate once the port forwarding reaches the max number of retries.
    /// We retry since kubectl port-forward is flaky.
    pub async fn port_forward(&self) {
        if let Err(err) = retry::Retry::default()
            .max_duration(Duration::from_secs(60))
            .retry_async(|retry_state| {
                let k8s_context = self.context.clone();
                let namespace = self.namespace.clone();
                let service_name = self.service_name.clone();
                let local_address = self.local_address.clone();
                let local_port = self.local_port;
                let target_port = self.target_port;

                info!(
                    "Spawning port forwarding process for {} from ports {}:{} -> {}",
                    service_name, local_address, local_port, target_port
                );

                async move {
                    let port_arg_str = format!("{}:{}", &local_port, &target_port);
                    let service_name_arg_str = format!("services/{}", &service_name);
                    let mut args = vec![
                        "port-forward",
                        &service_name_arg_str,
                        &port_arg_str,
                        "-n",
                        &namespace,
                        "--address",
                        &local_address,
                    ];

                    if let Some(k8s_context) = &k8s_context {
                        args.extend(["--context", k8s_context]);
                    }

                    match tokio::process::Command::new("kubectl")
                        .args(args)
                        // Silence stdout/stderr
                        .stdout(std::process::Stdio::null())
                        .stderr(std::process::Stdio::null())
                        .kill_on_drop(true)
                        .output()
                        .await
                    {
                        Ok(output) => {
                            if !output.status.success() {
                                let retry_err_msg = format!(
                                    "Failed to port-forward{}: {}",
                                    retry_state.next_backoff.map_or_else(
                                        || "".to_string(),
                                        |d| format!(", retrying in {:?}", d)
                                    ),
                                    String::from_utf8_lossy(&output.stderr)
                                );
                                error!("{}", retry_err_msg);

                                return RetryResult::RetryableErr(anyhow::anyhow!(retry_err_msg));
                            }
                        }
                        Err(err) => {
                            return RetryResult::RetryableErr(anyhow::anyhow!(
                                "Failed to port-forward: {}",
                                err
                            ));
                        }
                    }
                    // The kubectl subprocess's future will only resolve on error, thus the
                    // code here is unreachable. We return RetryResult::Ok to satisfy
                    // the type checker.
                    RetryResult::Ok(())
                }
            })
            .await
        {
            error!("{}", err);
        }
    }
}

pub async fn create_kubectl_port_forwarder(
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
