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
use futures::future::join_all;
use k8s_openapi::api::core::v1::ServicePort;
use reqwest::StatusCode;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{File, create_dir_all};
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};
use url::Url;

use crate::kubectl_port_forwarder::{
    KubectlPortForwarder, ServiceInfo, find_cluster_services, find_environmentd_service,
};
use crate::{AuthMode, Context, EmulatorContext, PasswordAuthCredentials, SelfManagedContext};

static PROFILES_DIR: &str = "profiles";
static PROM_METRICS_DIR: &str = "prom_metrics";
static PROM_METRICS_ENDPOINT: &str = "metrics";
static ENVD_HEAP_PROFILE_ENDPOINT: &str = "prof/heap";
static CLUSTERD_HEAP_PROFILE_ENDPOINT: &str = "heap";
static ENVD_CPU_PROFILE_ENDPOINT: &str = "prof/cpu";
static CLUSTERD_CPU_PROFILE_ENDPOINT: &str = "cpu";
/// The endpoint for reading a service's profiling mode. Used to probe CPU
/// profiling support and to verify memory profiling is active again after a
/// CPU profile capture.
static ENVD_PROF_MODE_ENDPOINT: &str = "prof/mode";
static CLUSTERD_PROF_MODE_ENDPOINT: &str = "mode";
/// The default port for the external HTTP endpoint.
static DEFAULT_EXTERNAL_HTTP_PORT: i32 = 6877;
/// The default port for the internal HTTP endpoint.
static DEFAULT_INTERNAL_HTTP_PORT: i32 = 6878;

/// The sampling frequency, in Hz, used when capturing CPU profiles. We avoid
/// round numbers like 100 to reduce the chance of sampling in lockstep with
/// periodic activity.
static CPU_PROFILE_HZ: u32 = 99;
/// Whether to merge per-thread stacks when capturing CPU profiles. We keep
/// threads separate by default so that per-thread behavior is visible.
static CPU_PROFILE_MERGE_THREADS: bool = false;
/// The timeout for requests to a service's profiling mode endpoint.
static PROF_MODE_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Extra time on top of the requested capture duration before the CPU profile
/// request times out, covering profile conversion and transfer.
static CPU_PROFILE_REQUEST_GRACE: Duration = Duration::from_secs(60);

/// The label for the internal HTTP port.
const INTERNAL_HTTP_PORT_LABEL: &str = "internal-http";
/// The label for the external HTTP port.
// Even when not using TLS, the external HTTP port is labeled as "https".
const EXTERNAL_HTTP_PORT_LABEL: &str = "https";

#[derive(Debug, Clone, Copy)]
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

fn get_cpu_profile_endpoint(service_type: &ServiceType) -> &'static str {
    match service_type {
        ServiceType::Clusterd => CLUSTERD_CPU_PROFILE_ENDPOINT,
        ServiceType::Environmentd => ENVD_CPU_PROFILE_ENDPOINT,
    }
}

fn get_prof_mode_endpoint(service_type: &ServiceType) -> &'static str {
    match service_type {
        ServiceType::Clusterd => CLUSTERD_PROF_MODE_ENDPOINT,
        ServiceType::Environmentd => ENVD_PROF_MODE_ENDPOINT,
    }
}

/// The body of a `POST` request to a service's CPU profile endpoint.
#[derive(Serialize)]
struct CpuProfileRequest {
    /// How long, in seconds, to sample.
    seconds: u64,
    /// The sampling frequency in Hz.
    hz: u32,
    /// Whether to merge per-thread stacks.
    merge_threads: bool,
}

/// The profiling mode reported by a service's profiling mode endpoint.
#[derive(Deserialize)]
struct ProfModeResponse {
    cpu_active: bool,
    memory_available: bool,
    memory_active: bool,
}

/// Returns the HTTP status code of the innermost `reqwest` error in `err`'s
/// chain, if any. Transport failures such as timeouts and connection errors
/// carry no status.
fn http_error_status(err: &anyhow::Error) -> Option<StatusCode> {
    err.chain()
        .find_map(|cause| cause.downcast_ref::<reqwest::Error>())
        .and_then(reqwest::Error::status)
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

    /// Sends a request to `relative_url`, trying HTTPS first and falling back
    /// to HTTP when the connection cannot be established. `build_request`
    /// constructs the request for a given URL and basic auth is layered on top
    /// when configured. Returns the response only if the server returned a
    /// success status. The underlying `reqwest::Error` is preserved in the
    /// error chain so that callers can inspect the HTTP status via
    /// [`http_error_status`].
    async fn send_with_fallback(
        &self,
        relative_url: &str,
        build_request: impl Fn(&Url) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, anyhow::Error> {
        let build_with_auth = |url: &Url| {
            let request_builder = build_request(url);
            if let AuthMode::Password(PasswordAuthCredentials { username, password }) =
                self.auth_mode
            {
                request_builder.basic_auth(username, Some(password))
            } else {
                request_builder
            }
        };
        // Try HTTPS first, then fall back to HTTP if that fails.
        let mut url = Url::parse(&format!("https://{}", relative_url))
            .with_context(|| format!("Failed to parse URL: https://{}", relative_url))?;

        let mut response = build_with_auth(&url).send().await;

        // Only fall back to HTTP when the connection could not be established,
        // which is how an HTTP-only server answers an HTTPS attempt. Errors
        // from a server we actually reached (including timeouts) must not
        // trigger a second attempt, since requests may have side effects such
        // as starting a CPU profile capture.
        if response.as_ref().is_err_and(reqwest::Error::is_connect) {
            let _ = url.set_scheme("http");
            response = build_with_auth(&url).send().await;
        }

        let response = response.with_context(|| format!("Failed to send request to {}", url))?;

        let status = response.status();
        if !status.is_success() {
            // Prefer `error_for_status` since it preserves the status code in
            // the error chain for callers to inspect.
            return match response.error_for_status() {
                Err(err) => {
                    Err(err).with_context(|| format!("Failed to get response from {}", url))
                }
                Ok(_) => Err(anyhow::anyhow!(
                    "Failed to get response from {}: {}",
                    url,
                    status
                )),
            };
        }

        Ok(response)
    }

    /// Streams the body of `response` to `output_path`.
    async fn stream_response_to_file(
        response: reqwest::Response,
        output_path: &Path,
    ) -> Result<(), anyhow::Error> {
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

    async fn dump_request_to_file(
        &self,
        relative_url: &str,
        headers: HeaderMap,
        output_path: &Path,
    ) -> Result<(), anyhow::Error> {
        let response = self
            .send_with_fallback(relative_url, |url| {
                self.http_client
                    .get(url.to_string())
                    .headers(headers.clone())
            })
            .await?;

        Self::stream_response_to_file(response, output_path).await
    }

    /// Downloads and saves heap profile data
    pub async fn dump_heap_profile(&self, relative_url: &str, service_name: &str) -> Result<()> {
        let output_dir = self.context.base_path.join(PROFILES_DIR);
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

    /// Captures a CPU profile from `relative_url` and saves it to disk.
    ///
    /// The server temporarily disables memory (heap) profiling while it
    /// captures a CPU profile. See [`dump_cpu_profile_and_verify_memory`]
    /// for how memory profiling is restored afterwards.
    pub async fn dump_cpu_profile(
        &self,
        relative_url: &str,
        service_name: &str,
        duration_secs: u64,
    ) -> Result<()> {
        let output_dir = self.context.base_path.join(PROFILES_DIR);
        create_dir_all(&output_dir).await.with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;
        let output_path = output_dir.join(format!("{}.cpuprof.pprof.gz", service_name));

        let request = CpuProfileRequest {
            seconds: duration_secs,
            hz: CPU_PROFILE_HZ,
            merge_threads: CPU_PROFILE_MERGE_THREADS,
        };
        // The server samples for `duration_secs`, so give the request that
        // long plus a fixed grace period before timing out.
        let request_timeout =
            Duration::from_secs(duration_secs).saturating_add(CPU_PROFILE_REQUEST_GRACE);

        let response = self
            .send_with_fallback(relative_url, |url| {
                self.http_client
                    .post(url.to_string())
                    .timeout(request_timeout)
                    .header("Accept", "application/octet-stream")
                    .json(&request)
            })
            .await
            .with_context(|| format!("Failed to capture CPU profile from {}", relative_url))?;

        Self::stream_response_to_file(response, &output_path)
            .await
            .with_context(|| format!("Failed to dump CPU profile to {}", output_path.display()))?;

        Ok(())
    }

    /// Reads the profiling mode of the target service.
    async fn get_profiling_mode(&self, relative_url: &str) -> Result<ProfModeResponse> {
        let response = self
            .send_with_fallback(relative_url, |url| {
                self.http_client
                    .get(url.to_string())
                    .timeout(PROF_MODE_REQUEST_TIMEOUT)
            })
            .await?;
        let mode = response
            .json::<ProfModeResponse>()
            .await
            .with_context(|| format!("Failed to parse profiling mode from {}", relative_url))?;
        Ok(mode)
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

/// Captures a CPU profile for a single service and verifies that memory
/// profiling is active again afterwards.
///
/// The server temporarily disables memory (heap) profiling while it captures
/// a CPU profile and restores the prior state itself when the capture ends,
/// including when the capture fails or is cancelled. mz-debug therefore only
/// observes the profiling mode and never modifies it. If memory profiling was
/// active before the capture but is not active afterwards, that is reported
/// for the operator to fix.
///
/// Errors are logged rather than propagated so that one failing service does
/// not abort the others.
async fn dump_cpu_profile_and_verify_memory(
    dump_client: &HttpDumpClient<'_>,
    cpu_endpoint: &str,
    mode_endpoint: &str,
    service_name: &str,
    duration_secs: u64,
) {
    // Reading the mode also serves as a version probe. Servers that predate
    // CPU profiling support have neither the profiling mode endpoint nor the
    // CPU profile endpoint, so skip them without noise.
    let prior_mode = match dump_client.get_profiling_mode(mode_endpoint).await {
        Ok(mode) => Some(mode),
        Err(e) if http_error_status(&e) == Some(StatusCode::NOT_FOUND) => {
            info!(
                "Service {} does not support CPU profiling. Skipping the CPU profile.",
                service_name
            );
            return;
        }
        Err(e) => {
            warn!(
                "Failed to read the profiling mode of service {}: {:#}",
                service_name, e
            );
            None
        }
    };

    info!("Dumping CPU profile for service {}", service_name);
    let capture_result = dump_client
        .dump_cpu_profile(cpu_endpoint, service_name, duration_secs)
        .await;
    let capture_status = capture_result.as_ref().err().and_then(http_error_status);
    if let Err(e) = &capture_result {
        warn!(
            "Failed to dump CPU profile for service {}: {:#}",
            service_name, e
        );
    }
    // If the capture endpoint does not exist, the capture never started and
    // memory profiling was never disturbed.
    if let Some(StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED) = capture_status {
        return;
    }

    // Skip the verification when memory profiling was off or unavailable
    // before the capture, there is nothing to restore then. An unknown prior
    // mode still verifies, a spurious warning is better than silently leaving
    // heap profiling off.
    let memory_was_active = prior_mode
        .as_ref()
        .is_none_or(|mode| mode.memory_available && mode.memory_active);
    if !memory_was_active {
        return;
    }

    match dump_client.get_profiling_mode(mode_endpoint).await {
        Ok(mode) if mode.memory_active => {}
        Ok(mode) if mode.cpu_active => {
            // Another capture is still running, possibly one this tool timed
            // out on. The server restores memory profiling when it finishes.
            info!(
                "A CPU profile capture is still running on service {}. The service restores memory profiling when it finishes.",
                service_name
            );
        }
        Ok(_) => {
            warn!(
                "Memory profiling is not active on service {} after the CPU profile. Re-enable it by POSTing {{\"memory_active\": true}} to the service's profiling mode endpoint or via its /prof web UI.",
                service_name
            );
        }
        Err(e) => {
            warn!(
                "Failed to verify the profiling mode of service {} after the CPU profile: {:#}",
                service_name, e
            );
        }
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
                    get_profile_endpoint(&ServiceType::Environmentd)
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

    // Capture the CPU profile after memory profiling, since the capture
    // temporarily disables memory profiling on the service.
    if context.dump_cpu_profiles {
        let resource_name = "environmentd".to_string();
        let port = get_default_port(&emulator_context.http_connection_auth_mode).heap_profile_port;
        let cpu_endpoint = format!(
            "{}:{}/{}",
            emulator_context.container_ip,
            port,
            get_cpu_profile_endpoint(&ServiceType::Environmentd)
        );
        let mode_endpoint = format!(
            "{}:{}/{}",
            emulator_context.container_ip,
            port,
            get_prof_mode_endpoint(&ServiceType::Environmentd)
        );

        info!(
            "Capturing CPU profile for {} seconds. Memory profiling is temporarily disabled during the capture and restored afterwards.",
            context.cpu_profile_duration_secs
        );
        dump_cpu_profile_and_verify_memory(
            &dump_task,
            &cpu_endpoint,
            &mode_endpoint,
            &resource_name,
            context.cpu_profile_duration_secs,
        )
        .await;
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
        &self_managed_context.k8s_namespace,
        &self_managed_context.mz_instance_name,
    )
    .await
    .with_context(|| "Failed to find cluster services")?;

    let environmentd_service = find_environmentd_service(
        &self_managed_context.k8s_client,
        &self_managed_context.k8s_namespace,
        &self_managed_context.mz_instance_name,
    )
    .await
    .with_context(|| "Failed to find environmentd service")?;

    let services: Vec<(&ServiceInfo, ServiceType)> = cluster_services
        .iter()
        .map(|service| (service, ServiceType::Clusterd))
        .chain(std::iter::once((
            &environmentd_service,
            ServiceType::Environmentd,
        )))
        .collect();

    // Scrape each service for heap profiles and prometheus metrics.
    for &(service_info, service_type) in &services {
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

        let (heap_profile_http_connection, prom_metrics_http_connection) = {
            let maybe_heap_profile_port = service_info
                .service_ports
                .iter()
                .find_map(|port_info| find_http_port_by_label(port_info, heap_profile_port_label));
            let maybe_prom_metrics_port = service_info
                .service_ports
                .iter()
                .find_map(|port_info| find_http_port_by_label(port_info, prom_metrics_port_label));
            if let (Some(heap_profile_port), Some(prom_metrics_port)) =
                (maybe_heap_profile_port, maybe_prom_metrics_port)
            {
                let heap_profile_port_forwarder = KubectlPortForwarder {
                    context: self_managed_context.k8s_context.clone(),
                    namespace: service_info.namespace.clone(),
                    service_name: service_info.service_name.clone(),
                    target_port: heap_profile_port.port,
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
                let prom_metrics_http_connection = if heap_profile_port == prom_metrics_port {
                    Arc::clone(&heap_profile_http_connection)
                } else {
                    let prom_metrics_port_forwarder = KubectlPortForwarder {
                        context: self_managed_context.k8s_context.clone(),
                        namespace: service_info.namespace.clone(),
                        service_name: service_info.service_name.clone(),
                        target_port: prom_metrics_port.port,
                    };
                    Arc::new(
                        prom_metrics_port_forwarder
                            .spawn_port_forward()
                            .await
                            .with_context(|| {
                                format!(
                                    "Failed to spawn port forwarder for service {}",
                                    service_info.service_name
                                )
                            })?,
                    )
                };

                (heap_profile_http_connection, prom_metrics_http_connection)
            } else {
                return Err(anyhow::anyhow!(
                    "Failed to find HTTP port for service {}, heap_profile_port_label={}, prom_metrics_port_label={}",
                    service_info.service_name,
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
            if let Err(e) = dump_task
                .dump_heap_profile(&profiling_endpoint, &service_info.service_name)
                .await
            {
                warn!(
                    "Failed to dump heap profile for service {}: {:#}",
                    service_info.service_name, e
                );
            }
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
            if let Err(e) = dump_task
                .dump_prometheus_metrics(&prom_metrics_endpoint, &service_info.service_name)
                .await
            {
                warn!(
                    "Failed to dump prometheus metrics for service {}: {:#}",
                    service_info.service_name, e
                );
            }
        }
    }

    // Capture CPU profiles after memory profiling, since each capture
    // temporarily disables memory profiling on its service. The captures run
    // in parallel, and a failure on one service does not abort the others.
    if context.dump_cpu_profiles {
        info!(
            "Capturing CPU profiles for {} seconds. Memory profiling is temporarily disabled on each service during its capture and restored afterwards.",
            context.cpu_profile_duration_secs
        );

        let cpu_profile_futures = services.iter().map(|&(service_info, service_type)| {
            // The CPU and mode endpoints are served on the same port as the heap
            // profile endpoint.
            let port_label = get_port_labels(
                &self_managed_context.http_connection_auth_mode,
                &service_type,
            )
            .heap_profile_port_label;
            let k8s_context = self_managed_context.k8s_context.clone();
            let dump_task = &dump_task;
            let duration_secs = context.cpu_profile_duration_secs;

            async move {
                let Some(port) = service_info
                    .service_ports
                    .iter()
                    .find_map(|port_info| find_http_port_by_label(port_info, port_label))
                else {
                    warn!(
                        "Failed to find HTTP port `{}` for CPU profiling of service {}",
                        port_label, service_info.service_name
                    );
                    return;
                };

                let port_forwarder = KubectlPortForwarder {
                    context: k8s_context,
                    namespace: service_info.namespace.clone(),
                    service_name: service_info.service_name.clone(),
                    target_port: port.port,
                };
                let connection = match port_forwarder.spawn_port_forward().await {
                    Ok(connection) => connection,
                    Err(e) => {
                        warn!(
                            "Failed to spawn port forwarder for CPU profiling of service {}: {:#}",
                            service_info.service_name, e
                        );
                        return;
                    }
                };

                let cpu_endpoint = format!(
                    "{}:{}/{}",
                    connection.local_address,
                    connection.local_port,
                    get_cpu_profile_endpoint(&service_type)
                );
                let mode_endpoint = format!(
                    "{}:{}/{}",
                    connection.local_address,
                    connection.local_port,
                    get_prof_mode_endpoint(&service_type)
                );

                dump_cpu_profile_and_verify_memory(
                    dump_task,
                    &cpu_endpoint,
                    &mode_endpoint,
                    &service_info.service_name,
                    duration_secs,
                )
                .await;
            }
        });

        join_all(cpu_profile_futures).await;
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
