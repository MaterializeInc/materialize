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

//! Dumps catalog information to files.

use std::time::Duration;

use k8s_openapi::api::core::v1::Service;
use kube::{Api, Client};
use mz_ore::retry::{self, RetryResult};
use mz_ore::task::{self, JoinHandle};
use tracing::{error, info};

use crate::Args;

#[derive(Debug, Clone)]
pub struct SqlPortForwardingInfo {
    pub namespace: String,
    pub service_name: String,
    pub target_port: i32,
    pub local_port: i32,
}

pub async fn get_sql_port_forwarding_info(
    client: &Client,
    args: &Args,
) -> Result<SqlPortForwardingInfo, anyhow::Error> {
    for namespace in &args.k8s_namespaces {
        let services: Api<Service> = Api::namespaced(client.clone(), namespace);

        // If override for target service and port is provided, verify that the
        // service exists and has the port.
        let maybe_service_and_port_override = match (&args.sql_target_service, args.sql_target_port)
        {
            (Some(service), Some(port)) => Some((service, port)),
            _ => None,
        };

        if let Some((service_override, port_override)) = maybe_service_and_port_override {
            let service = services.get(service_override).await?;
            if let Some(spec) = service.spec {
                let contains_port = spec
                    .ports
                    .unwrap_or_default()
                    .iter()
                    .any(|port_info| port_info.port == port_override);

                if contains_port {
                    return Ok(SqlPortForwardingInfo {
                        namespace: namespace.clone(),
                        service_name: service_override.clone(),
                        target_port: port_override,
                        local_port: args.sql_local_port.unwrap_or(port_override),
                    });
                }
            }

            return Err(anyhow::anyhow!(
                "Service {} with port {} not found",
                service_override,
                port_override
            ));
        }
        let services = services.list(&Default::default()).await?;
        // Check if any service contains a port with name "internal-sql"
        let maybe_port_info = services
            .iter()
            .filter_map(|service| {
                let spec = service.spec.as_ref()?;
                let service_name = service.metadata.name.as_ref()?;
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
                    if port_name.to_lowercase().contains("internal")
                        && port_name.to_lowercase().contains("sql")
                    {
                        return Some(SqlPortForwardingInfo {
                            namespace: namespace.clone(),
                            service_name: service_name.to_owned(),
                            target_port: port_info.port,
                            local_port: args.sql_local_port.unwrap_or(port_info.port),
                        });
                    }
                }

                None
            });

        if let Some(port_info) = maybe_port_info {
            return Ok(port_info);
        }
    }

    Err(anyhow::anyhow!("No SQL port forwarding info found"))
}

/// Spawns a port forwarding process for the given k8s service.
/// The process will retry if the port-forwarding fails and
/// will terminate once the port forwarding reaches the max number of retries.
/// We retry since kubectl port-forward is flaky.
pub fn spawn_sql_port_forwarding_process(
    port_forwarding_info: &SqlPortForwardingInfo,
    args: &Args,
) -> JoinHandle<()> {
    let port_forwarding_info = port_forwarding_info.clone();

    let k8s_context = args.k8s_context.clone();
    let local_address = args.sql_local_address.clone();

    task::spawn(|| "port-forwarding", async move {
        if let Err(err) = retry::Retry::default()
            .max_duration(Duration::from_secs(60))
            .retry_async(|retry_state| {
                let k8s_context = k8s_context.clone();
                let namespace = port_forwarding_info.namespace.clone();
                let service_name = port_forwarding_info.service_name.clone();
                let local_address = local_address.clone();
                let local_port = port_forwarding_info.local_port;
                let target_port = port_forwarding_info.target_port;
                let local_address_or_default =
                    local_address.clone().unwrap_or("localhost".to_string());

                info!(
                    "Spawning port forwarding process for {} from ports {}:{} -> {}",
                    service_name, local_address_or_default, local_port, target_port
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
                    ];

                    if let Some(k8s_context) = &k8s_context {
                        args.extend(["--context", k8s_context]);
                    }

                    if let Some(local_address) = &local_address {
                        args.extend(["--address", local_address]);
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
    })
}
