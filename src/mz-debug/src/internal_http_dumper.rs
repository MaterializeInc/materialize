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
use tokio::fs::{File, create_dir_all};
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use url::Url;

use crate::kubectl_port_forwarder::{
    KubectlPortForwarder, find_cluster_services, find_environmentd_service,
};
use crate::{Context, EmulatorContext, SelfManagedContext};

static HEAP_PROFILES_DIR: &str = "profiles";
static PROM_METRICS_DIR: &str = "prom_metrics";
static PROM_METRICS_ENDPOINT: &str = "metrics";
static ENVD_HEAP_PROFILE_ENDPOINT: &str = "prof/heap";
static CLUSTERD_HEAP_PROFILE_ENDPOINT: &str = "heap";
static INTERNAL_HOST_ADDRESS: &str = "127.0.0.1";
static INTERNAL_HTTP_PORT: i32 = 6878;

/// A struct that handles downloading and saving profile data from HTTP endpoints.
pub struct InternalHttpDumpClient<'n> {
    context: &'n Context,
    http_client: &'n reqwest::Client,
}

/// A struct that handles downloading and exporting data from our internal HTTP endpoints.
impl<'n> InternalHttpDumpClient<'n> {
    pub fn new(context: &'n Context, http_client: &'n reqwest::Client) -> Self {
        Self {
            context,
            http_client,
        }
    }

    async fn dump_request_to_file(
        &self,
        relative_url: &str,
        headers: HeaderMap,
        output_path: &Path,
    ) -> Result<(), anyhow::Error> {
        // Try HTTPS first, then fall back to HTTP if that fails
        let mut url = Url::parse(&format!("https://{}", relative_url))
            .with_context(|| format!("Failed to parse URL: https://{}", relative_url))?;

        let mut response = self
            .http_client
            .get(url.to_string())
            .headers(headers.clone())
            .send()
            .await;

        if response.is_err() {
            // Fall back to HTTP if HTTPS fails
            let _ = url.set_scheme("http");

            response = self
                .http_client
                .get(url.to_string())
                .headers(headers)
                .send()
                .await;
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

// TODO (debug_tool3): Scrape cluster profiles through a proxy when (database-issues#8942) is implemented
pub async fn dump_emulator_http_resources(
    context: &Context,
    emulator_context: &EmulatorContext,
) -> Result<()> {
    let http_client = reqwest::Client::new();
    let dump_task = InternalHttpDumpClient::new(context, &http_client);

    if context.dump_heap_profiles {
        let resource_name = "environmentd".to_string();

        // We assume the emulator is exposed on the local network and uses port 6878.
        if let Err(e) = dump_task
            .dump_heap_profile(
                &format!(
                    "{}:{}/{}",
                    emulator_context.container_ip, INTERNAL_HTTP_PORT, ENVD_HEAP_PROFILE_ENDPOINT
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

        let dump_task = InternalHttpDumpClient::new(context, &http_client);

        if let Err(e) = dump_task
            .dump_prometheus_metrics(
                &format!(
                    "{}:{}/{}",
                    emulator_context.container_ip, INTERNAL_HTTP_PORT, PROM_METRICS_ENDPOINT
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
    // TODO (debug_tool3): Allow user to override temporary local address for http port forwarding
    let prom_metrics_endpoint = format!(
        "{}:{}/{}",
        INTERNAL_HOST_ADDRESS, INTERNAL_HTTP_PORT, PROM_METRICS_ENDPOINT
    );

    let clusterd_heap_profile_endpoint = format!(
        "{}:{}/{}",
        INTERNAL_HOST_ADDRESS, INTERNAL_HTTP_PORT, CLUSTERD_HEAP_PROFILE_ENDPOINT
    );

    let envd_heap_profile_endpoint = format!(
        "{}:{}/{}",
        INTERNAL_HOST_ADDRESS, INTERNAL_HTTP_PORT, ENVD_HEAP_PROFILE_ENDPOINT
    );

    let services = cluster_services
        .iter()
        .map(|service| (service, &clusterd_heap_profile_endpoint))
        .chain(std::iter::once((
            &environmentd_service,
            &envd_heap_profile_endpoint,
        )));

    // Scrape each service
    for (service_info, profiling_endpoint) in services {
        let maybe_internal_http_port = service_info
            .service_ports
            .iter()
            .find_map(find_internal_http_port);

        if let Some(internal_http_port) = maybe_internal_http_port {
            let port_forwarder = KubectlPortForwarder {
                context: self_managed_context.k8s_context.clone(),
                namespace: service_info.namespace.clone(),
                service_name: service_info.service_name.clone(),
                target_port: internal_http_port.port,
                local_address: INTERNAL_HOST_ADDRESS.to_string(),
                local_port: INTERNAL_HTTP_PORT,
            };

            let _internal_http_connection =
                port_forwarder.spawn_port_forward().await.with_context(|| {
                    format!(
                        "Failed to spawn port forwarder for service {}",
                        service_info.service_name
                    )
                })?;

            let dump_task = InternalHttpDumpClient::new(context, &http_client);

            if context.dump_heap_profiles {
                info!(
                    "Dumping heap profile for service {}",
                    service_info.service_name
                );
                dump_task
                    .dump_heap_profile(profiling_endpoint, &service_info.service_name)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to dump heap profile for service {}",
                            service_info.service_name
                        )
                    })?;
            }

            if context.dump_prometheus_metrics {
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
    }

    Ok(())
}

fn find_internal_http_port(port_info: &ServicePort) -> Option<&ServicePort> {
    if let Some(port_name) = &port_info.name {
        let port_name = port_name.to_lowercase();
        if port_name == "internal-http" {
            return Some(port_info);
        }
    }
    None
}
