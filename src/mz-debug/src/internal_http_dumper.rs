// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Dumps internal http debug information to files.
use anyhow::{Context as AnyhowContext, Result};
use futures::StreamExt;
use k8s_openapi::api::core::v1::ServicePort;
use reqwest::header::{HeaderMap, HeaderValue};
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, create_dir_all};
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use url::Url;

use crate::kubectl_port_forwarder::{
    KubectlPortForwarder, find_cluster_services, find_environmentd_service,
};
use crate::{AuthMode, Context, EmulatorContext, PasswordAuthCredentials, SelfManagedContext};

static HEAP_PROFILES_DIR: &str = "profiles";
static PROM_METRICS_DIR: &str = "prom_metrics";
static PROM_METRICS_ENDPOINT: &str = "metrics";
static ENVD_HEAP_PROFILE_ENDPOINT: &str = "prof/heap";
static CLUSTERD_HEAP_PROFILE_ENDPOINT: &str = "heap";
/// The default port for the external HTTP endpoint.
static DEFAULT_EXTERNAL_HTTP_PORT: i32 = 6877;
/// The default port for the internal HTTP endpoint.
static DEFAULT_INTERNAL_HTTP_PORT: i32 = 6878;

/// The label for the internal HTTP port.
const INTERNAL_HTTP_PORT_LABEL: &str = "internal-http";
/// The label for the external HTTP port.
// TODO: Even when not using TLS, the external HTTP port is labeled as "https". Fix when fixed in orchestratord.
const EXTERNAL_HTTP_PORT_LABEL: &str = "https";

enum ServiceType {
    Clusterd,
    Environmentd,
}

fn get_profile_endpoint(service_type: &ServiceType) -> &'static str {
    match service_type {
        ServiceType::Clusterd => CLUSTERD_HEAP_PROFILE_ENDPOINT,
        ServiceType::Environmentd => ENVD_HEAP_PROFILE_ENDPOINT,
    }
}

#[derive(Debug, Clone)]
struct HttpPortLabels {
    heap_profile_port_label: &'static str,
    prom_metrics_port_label: &'static str,
}

fn get_port_labels(auth_mode: &AuthMode, service_type: &ServiceType) -> HttpPortLabels {
    match (auth_mode, service_type) {
        (AuthMode::None, ServiceType::Clusterd)
        | (AuthMode::None, ServiceType::Environmentd)
        // Even if in the password listener config, the heap profile port is specified as external, clusterd will
        // still use the internal port.
        // TODO: Once fixed in orchestratord, we can use the external port for the heap profile label.
        | (AuthMode::Password(_), ServiceType::Clusterd) => HttpPortLabels {
            heap_profile_port_label: INTERNAL_HTTP_PORT_LABEL,
            prom_metrics_port_label: INTERNAL_HTTP_PORT_LABEL,
        },
        (AuthMode::Password(_), ServiceType::Environmentd) => HttpPortLabels {
            heap_profile_port_label: EXTERNAL_HTTP_PORT_LABEL,
            prom_metrics_port_label: INTERNAL_HTTP_PORT_LABEL,
        },
    }
}

struct HttpDefaultPorts {
    heap_profile_port: i32,
    prom_metrics_port: i32,
}

fn get_default_port(auth_mode: &AuthMode) -> HttpDefaultPorts {
    match auth_mode {
        AuthMode::None => HttpDefaultPorts {
            heap_profile_port: DEFAULT_INTERNAL_HTTP_PORT,
            prom_metrics_port: DEFAULT_INTERNAL_HTTP_PORT,
        },
        AuthMode::Password(_) => HttpDefaultPorts {
            heap_profile_port: DEFAULT_EXTERNAL_HTTP_PORT,
            prom_metrics_port: DEFAULT_INTERNAL_HTTP_PORT,
        },
    }
}

/// A struct that handles downloading and saving profile data from HTTP endpoints.
pub struct HttpDumpClient<'n> {
    context: &'n Context,
    auth_mode: &'n AuthMode,
    http_client: &'n reqwest::Client,
}

/// A struct that handles downloading and exporting data from our internal HTTP endpoints.
impl<'n> HttpDumpClient<'n> {
    pub fn new(
        context: &'n Context,
        auth_mode: &'n AuthMode,
        http_client: &'n reqwest::Client,
    ) -> Self {
        Self {
            context,
            auth_mode,
            http_client,
        }
    }

    async fn dump_request_to_file(
        &self,
        relative_url: &str,
        headers: HeaderMap,
        output_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let create_request = |url: &Url| {
            let mut request_builder = self
                .http_client
                .get(url.to_string())
                .headers(headers.clone());

            if let AuthMode::Password(PasswordAuthCredentials { username, password }) =
                self.auth_mode
            {
                request_builder = request_builder.basic_auth(&username, Some(&password));
            }

            request_builder
        };
        // Try HTTPS first, then fall back to HTTP if that fails
        let mut url = Url::parse(&format!("https://{}", relative_url))
            .with_context(|| format!("Failed to parse URL: https://{}", relative_url))?;

        let request = create_request(&url);

        let mut response = request.send().await;

        if response.is_err() {
            // Fall back to HTTP if HTTPS fails
            let _ = url.set_scheme("http");

            response = create_request(&url).send().await;
        }

        let response = response.with_context(|| format!("Failed to send request to {}", url))?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to get response from {}: {}",
                url,
                response.status()
            ));
        }

        let mut file = File::create(output_path)
            .await
            .with_context(|| format!("Failed to create file: {}", output_path.display()))?;

        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.with_context(|| "Failed to read chunk from response")?;
            file.write_all(&chunk)
                .await
                .with_context(|| "Failed to write chunk to file")?;
        }

        file.flush()
            .await
            .with_context(|| "Failed to flush file contents")?;

        Ok(())
    }

    /// Downloads and saves heap profile data
    pub async fn dump_heap_profile(&self, relative_url: &str, service_name: &str) -> Result<()> {
        let output_dir = self.context.base_path.join(HEAP_PROFILES_DIR);
        create_dir_all(&output_dir).await.with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;
        let output_path = output_dir.join(format!("{}.memprof.pprof.gz", service_name));

        self.dump_request_to_file(
            relative_url,
            {
                let mut headers = HeaderMap::new();
                headers.insert(
                    "Accept",
                    HeaderValue::from_static("application/octet-stream"),
                );
                headers
            },
            &output_path,
        )
        .await
        .with_context(|| format!("Failed to dump heap profile to {}", output_path.display()))?;

        Ok(())
    }

    pub async fn dump_prometheus_metrics(
        &self,
        relative_url: &str,
        service_name: &str,
    ) -> Result<()> {
        let output_dir = self.context.base_path.join(PROM_METRICS_DIR);
        create_dir_all(&output_dir).await.with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;

        let output_path = output_dir.join(format!("{}.metrics.txt", service_name));
        self.dump_request_to_file(
            relative_url,
            {
                let mut headers = HeaderMap::new();
                headers.insert("Accept", HeaderValue::from_static("text/plain"));
                headers
            },
            &output_path,
        )
        .await?;

        Ok(())
    }
}

// TODO (debug_tool3): Scrape cluster profiles through a proxy when (database-issues#7049) is implemented
pub async fn dump_emulator_http_resources(
    context: &Context,
    emulator_context: &EmulatorContext,
) -> Result<()> {
    let http_client = reqwest::Client::new();
    let dump_task = HttpDumpClient::new(
        context,
        &emulator_context.http_connection_auth_mode,
        &http_client,
    );

    if context.dump_heap_profiles {
        let resource_name = "environmentd".to_string();

        // We assume the emulator is exposed on the local network and uses port 6878.
        if let Err(e) = dump_task
            .dump_heap_profile(
                &format!(
                    "{}:{}/{}",
                    emulator_context.container_ip,
                    get_default_port(&emulator_context.http_connection_auth_mode).heap_profile_port,
                    ENVD_HEAP_PROFILE_ENDPOINT
                ),
                &resource_name,
            )
            .await
        {
            warn!("Failed to dump heap profile: {:#}", e);
        }
    }

    if context.dump_prometheus_metrics {
        let resource_name = "environmentd".to_string();

        if let Err(e) = dump_task
            .dump_prometheus_metrics(
                &format!(
                    "{}:{}/{}",
                    emulator_context.container_ip,
                    get_default_port(&emulator_context.http_connection_auth_mode).prom_metrics_port,
                    PROM_METRICS_ENDPOINT
                ),
                &resource_name,
            )
            .await
        {
            warn!("Failed to dump prometheus metrics: {:#}", e);
        }
    }

    Ok(())
}

pub async fn dump_self_managed_http_resources(
    context: &Context,
    self_managed_context: &SelfManagedContext,
) -> Result<()> {
    let http_client = reqwest::Client::new();
    let dump_task = HttpDumpClient::new(
        context,
        &self_managed_context.http_connection_auth_mode,
        &http_client,
    );

    let cluster_services = find_cluster_services(
        &self_managed_context.k8s_client,
        &self_managed_context.k8s_namespaces,
    )
    .await
    .with_context(|| "Failed to find cluster services")?;

    let environmentd_service = find_environmentd_service(
        &self_managed_context.k8s_client,
        &self_managed_context.k8s_namespaces,
    )
    .await
    .with_context(|| "Failed to find environmentd service")?;

    let services = cluster_services
        .iter()
        .map(|service| (service, ServiceType::Clusterd))
        .chain(std::iter::once((
            &environmentd_service,
            ServiceType::Environmentd,
        )));

    // Scrape each service
    for (service_info, service_type) in services {
        let profiling_endpoint = get_profile_endpoint(&service_type);
        let heap_profile_port_label = get_port_labels(
            &self_managed_context.http_connection_auth_mode,
            &service_type,
        )
        .heap_profile_port_label;

        let prom_metrics_port_label = get_port_labels(
            &self_managed_context.http_connection_auth_mode,
            &service_type,
        )
        .prom_metrics_port_label;

        let (heap_profile_http_connection, prom_metrics_http_connection) = match (
            heap_profile_port_label,
            prom_metrics_port_label,
        ) {
            (INTERNAL_HTTP_PORT_LABEL, INTERNAL_HTTP_PORT_LABEL)
            | (EXTERNAL_HTTP_PORT_LABEL, EXTERNAL_HTTP_PORT_LABEL) => {
                let maybe_http_port = service_info.service_ports.iter().find_map(|port_info| {
                    find_http_port_by_label(port_info, heap_profile_port_label)
                });

                if let Some(internal_http_port) = maybe_http_port {
                    let port_forwarder = KubectlPortForwarder {
                        context: self_managed_context.k8s_context.clone(),
                        namespace: service_info.namespace.clone(),
                        service_name: service_info.service_name.clone(),
                        target_port: internal_http_port.port,
                    };

                    let heap_profile_http_connection =
                        Arc::new(port_forwarder.spawn_port_forward().await.with_context(|| {
                            format!(
                                "Failed to spawn port forwarder for service {}",
                                service_info.service_name
                            )
                        })?);

                    let prom_metrics_http_connection = Arc::clone(&heap_profile_http_connection);
                    (heap_profile_http_connection, prom_metrics_http_connection)
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to find HTTP ports for service {}",
                        service_info.service_name
                    ));
                }
            }
            (INTERNAL_HTTP_PORT_LABEL, EXTERNAL_HTTP_PORT_LABEL)
            | (EXTERNAL_HTTP_PORT_LABEL, INTERNAL_HTTP_PORT_LABEL) => {
                let maybe_heap_profile_port =
                    service_info.service_ports.iter().find_map(|port_info| {
                        find_http_port_by_label(port_info, heap_profile_port_label)
                    });

                let maybe_prom_metrics_port =
                    service_info.service_ports.iter().find_map(|port_info| {
                        find_http_port_by_label(port_info, prom_metrics_port_label)
                    });

                if let (Some(heap_profile_port), Some(prom_metrics_port)) =
                    (maybe_heap_profile_port, maybe_prom_metrics_port)
                {
                    let heap_profile_port_forwarder = KubectlPortForwarder {
                        context: self_managed_context.k8s_context.clone(),
                        namespace: service_info.namespace.clone(),
                        service_name: service_info.service_name.clone(),
                        target_port: heap_profile_port.port,
                    };

                    let prom_metrics_port_forwarder = KubectlPortForwarder {
                        context: self_managed_context.k8s_context.clone(),
                        namespace: service_info.namespace.clone(),
                        service_name: service_info.service_name.clone(),
                        target_port: prom_metrics_port.port,
                    };

                    let heap_profile_http_connection = Arc::new(
                        heap_profile_port_forwarder
                            .spawn_port_forward()
                            .await
                            .with_context(|| {
                                format!(
                                    "Failed to spawn port forwarder for service {}",
                                    service_info.service_name
                                )
                            })?,
                    );

                    let prom_metrics_http_connection = Arc::new(
                        prom_metrics_port_forwarder
                            .spawn_port_forward()
                            .await
                            .with_context(|| {
                                format!(
                                    "Failed to spawn port forwarder for service {}",
                                    service_info.service_name
                                )
                            })?,
                    );

                    (heap_profile_http_connection, prom_metrics_http_connection)
                } else {
                    return Err(anyhow::anyhow!(
                        "Failed to find HTTP port for service {}, heap_profile_port_label={}, prom_metrics_port_label={}",
                        service_info.service_name,
                        heap_profile_port_label,
                        prom_metrics_port_label
                    ));
                }
            }
            (_, _) => {
                return Err(anyhow::anyhow!(
                    "Invalid port label combination: heap_profile_port_label={}, prom_metrics_port_label={}",
                    heap_profile_port_label,
                    prom_metrics_port_label
                ));
            }
        };

        if context.dump_heap_profiles {
            let profiling_endpoint = format!(
                "{}:{}/{}",
                heap_profile_http_connection.local_address,
                heap_profile_http_connection.local_port,
                profiling_endpoint
            );

            info!(
                "Dumping heap profile for service {}",
                service_info.service_name
            );
            dump_task
                .dump_heap_profile(&profiling_endpoint, &service_info.service_name)
                .await
                .with_context(|| {
                    format!(
                        "Failed to dump heap profile for service {}",
                        service_info.service_name
                    )
                })?;
        }

        if context.dump_prometheus_metrics {
            let prom_metrics_endpoint = format!(
                "{}:{}/{}",
                prom_metrics_http_connection.local_address,
                prom_metrics_http_connection.local_port,
                PROM_METRICS_ENDPOINT
            );
            info!(
                "Dumping prometheus metrics for service {}",
                service_info.service_name
            );
            dump_task
                .dump_prometheus_metrics(&prom_metrics_endpoint, &service_info.service_name)
                .await
                .with_context(|| {
                    format!(
                        "Failed to dump prometheus metrics for service {}",
                        service_info.service_name
                    )
                })?;
        }
    }

    Ok(())
}

fn find_http_port_by_label<'a>(
    port_info: &'a ServicePort,
    target_port_label: &'static str,
) -> Option<&'a ServicePort> {
    if let Some(port_name) = &port_info.name {
        let port_name = port_name.to_lowercase();
        if port_name == target_port_label {
            return Some(port_info);
        }
    }
    None
}
