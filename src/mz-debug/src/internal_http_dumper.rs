use anyhow::{Context as AnyhowContext, Result};
use futures::StreamExt;
use k8s_openapi::api::core::v1::ServicePort;
use reqwest::header::{HeaderMap, HeaderValue};
use std::path::Path;
use tokio::fs::{File, create_dir_all};
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

use crate::kubectl_port_forwarder::{
    KubectlPortForwarder, find_cluster_services, find_environmentd_service,
};
use crate::{Context, SelfManagedContext};

static PROM_METRICS_ENDPOINT: &str = "metrics";
static ENVD_HEAP_PROFILE_ENDPOINT: &str = "prof/heap";
static CLUSTERD_HEAP_PROFILE_ENDPOINT: &str = "heap";

/// A struct that handles downloading and saving profile data from HTTP endpoints
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
        let mut url = format!("https://{}", relative_url);
        let mut response = self
            .http_client
            .get(&url)
            .headers(headers.clone())
            .send()
            .await;

        if response.is_err() {
            // Fall back to HTTP if HTTPS fails
            url = format!("http://{}", relative_url);
            response = self.http_client.get(&url).headers(headers).send().await;
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
        let output_dir = self.context.base_path.join("profiles");
        create_dir_all(&output_dir).await.with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;
        let output_path = output_dir.join(format!("{}.memprof.pprof", service_name));

        self.dump_request_to_file(
            &relative_url,
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
        let output_dir = self.context.base_path.join("prom_metrics");
        create_dir_all(&output_dir).await.with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;

        let output_path = output_dir.join(format!("{}.metrics.txt", service_name));
        self.dump_request_to_file(
            &relative_url,
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
pub async fn dump_emulator_http_resources(context: &Context) -> Result<()> {
    let http_client = reqwest::Client::new();
    let dump_task = InternalHttpDumpClient::new(context, &http_client);

    if context.dump_heap_profiles {
        let resource_name = "environmentd".to_string();

        // We assume the emulator is exposed on the local network and uses port 6878.
        // TODO (debug_tool1): Figure out the correct IP address from the docker container
        if let Err(e) = dump_task
            .dump_heap_profile(
                &format!("127.0.0.1:6878/{}", ENVD_HEAP_PROFILE_ENDPOINT),
                &resource_name,
            )
            .await
        {
            warn!("Failed to dump heap profile: {}", e);
        }
    }

    if context.dump_prometheus_metrics {
        let resource_name = "environmentd".to_string();

        let dump_task = InternalHttpDumpClient::new(context, &http_client);

        if let Err(e) = dump_task
            .dump_prometheus_metrics(
                &format!("127.0.0.1:6878/{}", PROM_METRICS_ENDPOINT),
                &resource_name,
            )
            .await
        {
            warn!("Failed to dump prometheus metrics: {}", e);
        }
    }

    Ok(())
}

pub async fn dump_self_managed_http_resources(
    context: &Context,
    self_managed_context: &SelfManagedContext,
) -> Result<()> {
    let http_client = reqwest::Client::new();
    // TODO (debug_tool3): Allow user to override temporary local address for http port forwarding
    let local_address = "127.0.0.1";
    let local_port = 6878;

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

    let prom_metrics_endpoint = format!(
        "{}:{}/{}",
        &local_address, local_port, PROM_METRICS_ENDPOINT
    );

    let clusterd_heap_profile_endpoint = format!(
        "{}:{}/{}",
        &local_address, local_port, CLUSTERD_HEAP_PROFILE_ENDPOINT
    );

    let envd_heap_profile_endpoint = format!(
        "{}:{}/{}",
        &local_address, local_port, ENVD_HEAP_PROFILE_ENDPOINT
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
                local_address: local_address.to_string(),
                local_port,
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
