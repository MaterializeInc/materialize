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

//! The `self-managed` mode: download snapshots from the in-cluster collector.
//!
//! Collection itself happens in the collector the operator runs for each
//! instance (see [`crate::collector`]). This side finds the instance's
//! `MaterializeDebug` resource, port-forwards to the collector's service, asks
//! for a fresh snapshot, waits for it, and saves the zip.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::Parser;
use futures::StreamExt;
use kube::{Api, Client};
use mz_cloud_resources::crd::materialize_debug::v1alpha1::MaterializeDebug;
use tokio::io::AsyncWriteExt;
use tracing::{info, warn};

use crate::Args;
use crate::collector::http::{SnapshotList, SnapshotRequested};
use crate::collector::snapshot::SnapshotRequest;
use crate::kubectl_port_forwarder::KubectlPortForwarder;

/// The port the collector's service exposes its HTTP API on.
const COLLECTOR_HTTP_PORT: i32 = 8080;
/// How often to ask the collector whether the requested snapshot is done.
const SNAPSHOT_POLL_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Parser, Debug, Clone)]
pub struct SelfManagedDebugModeArgs {
    /// The k8s namespace that the Materialize instance is running in.
    #[clap(long)]
    k8s_namespace: String,
    /// The name of the Materialize instance to target.
    #[clap(long)]
    mz_instance_name: String,
    /// The kubernetes context to use.
    #[clap(long, env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    /// The name of the MaterializeDebug resource to download from, when it
    /// differs from the instance name. The operator names the resource it
    /// creates for an instance after the instance.
    #[clap(long)]
    debug_name: Option<String>,
    /// If true, the fresh snapshot includes Kubernetes resources and pod logs.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    dump_k8s: bool,
    /// Download every snapshot the collector has retained, not only the
    /// latest, to see how the instance got to its current state.
    #[clap(long)]
    all_snapshots: bool,
    /// Do not ask the collector for a fresh snapshot; download what it has
    /// already collected. Useful when the instance is too unhealthy to
    /// snapshot and only the history matters.
    #[clap(long)]
    no_fresh_snapshot: bool,
    /// How long, in seconds, to wait for the fresh snapshot before falling
    /// back to the latest one the collector has.
    #[clap(long, default_value = "600")]
    snapshot_timeout_seconds: u64,
    /// No longer accepted. The namespaces a collector covers are configured
    /// on its MaterializeDebug resource.
    #[clap(long = "additional-k8s-namespace", action = clap::ArgAction::Append, hide = true)]
    additional_k8s_namespaces: Vec<String>,
}

pub async fn run(args: &Args, self_managed: &SelfManagedDebugModeArgs) -> Result<()> {
    if !self_managed.additional_k8s_namespaces.is_empty() {
        bail!(
            "--additional-k8s-namespace is no longer accepted: the namespaces a collector covers \
             are set in the MaterializeDebug resource's spec.additionalNamespaces"
        );
    }
    if args.mz_username.is_some() || args.mz_password.is_some() || args.mz_connection_url.is_some()
    {
        warn!(
            "--mz-username, --mz-password and --mz-connection-url only apply to the emulator; the \
             collector connects to the instance itself. Ignoring them."
        );
    }

    let client = create_k8s_client(self_managed.k8s_context.clone())
        .await
        .context("Failed to create Kubernetes client")?;
    let debug_name = self_managed
        .debug_name
        .clone()
        .unwrap_or_else(|| self_managed.mz_instance_name.clone());
    let debug = fetch_materialize_debug(&client, &self_managed.k8s_namespace, &debug_name).await?;

    let Some(status) = &debug.status else {
        bail!(
            "MaterializeDebug {}/{} has not been reconciled by the operator yet; is the operator running?",
            self_managed.k8s_namespace,
            debug_name
        );
    };
    let ready = status
        .conditions
        .iter()
        .find(|condition| condition.type_ == "Ready");
    match ready {
        Some(condition) if condition.status == "True" => {}
        Some(condition) => warn!(
            "The debug collector is not ready ({}: {}). Snapshots it has already taken can still be downloaded.",
            condition.reason, condition.message
        ),
        None => warn!("The debug collector has not reported readiness yet."),
    }

    let port_forward = KubectlPortForwarder {
        namespace: self_managed.k8s_namespace.clone(),
        service_name: debug.service_name(),
        target_port: COLLECTOR_HTTP_PORT,
        context: self_managed.k8s_context.clone(),
    }
    .spawn_port_forward()
    .await
    .context("Failed to port-forward to the debug collector service")?;
    let collector = CollectorClient {
        base_url: format!(
            "http://{}:{}",
            port_forward.local_address, port_forward.local_port
        ),
        http: reqwest::Client::new(),
    };

    let mut fresh_id = None;
    if self_managed.no_fresh_snapshot {
        if categories_were_narrowed(args, self_managed) {
            warn!(
                "The --dump-* flags select what a fresh snapshot collects and have no effect with --no-fresh-snapshot."
            );
        }
    } else {
        let request = SnapshotRequest {
            k8s: Some(self_managed.dump_k8s),
            system_catalog: Some(args.dump_system_catalog),
            heap_profiles: Some(args.dump_heap_profiles),
            prometheus_metrics: Some(args.dump_prometheus_metrics),
            cpu_profiles: Some(args.dump_cpu_profiles.unwrap_or(true)),
            cpu_profile_duration_seconds: Some(args.cpu_profile_duration_seconds),
        };
        let id = collector.request_snapshot(&request).await?;
        info!(
            "Requested snapshot {}, waiting for the collector to take it",
            id
        );
        let timeout = Duration::from_secs(self_managed.snapshot_timeout_seconds);
        match collector.wait_for_snapshot(&id, timeout).await? {
            true => fresh_id = Some(id),
            false => warn!(
                "Snapshot {} did not complete within {}s. Downloading the latest snapshot the collector has instead.",
                id,
                timeout.as_secs()
            ),
        }
    }

    let list = collector.list().await?;
    if let Some(error) = &list.last_error {
        warn!(
            "The collector's most recent snapshot attempt failed: {}",
            error
        );
    }
    let selected: Vec<&str> = if self_managed.all_snapshots {
        list.snapshots.iter().map(|meta| meta.id.as_str()).collect()
    } else {
        match fresh_id
            .as_deref()
            .or_else(|| list.snapshots.last().map(|meta| meta.id.as_str()))
        {
            Some(id) => vec![id],
            None => Vec::new(),
        }
    };
    if selected.is_empty() {
        bail!(
            "The debug collector has no snapshots yet. It takes one at startup and then every snapshot interval; check its pod if it has been running for a while."
        );
    }

    for id in selected {
        let path = PathBuf::from(format!(
            "mz_debug_{}_{}.zip",
            self_managed.mz_instance_name, id
        ));
        collector.download(id, &path).await?;
        info!("Saved snapshot {} to {}", id, path.display());
    }
    Ok(())
}

/// Whether any category flag departs from its default, in which case a user
/// passing `--no-fresh-snapshot` probably expected it to do something.
fn categories_were_narrowed(args: &Args, self_managed: &SelfManagedDebugModeArgs) -> bool {
    !self_managed.dump_k8s
        || !args.dump_system_catalog
        || !args.dump_heap_profiles
        || !args.dump_prometheus_metrics
        || args.dump_cpu_profiles.is_some()
}

/// Creates a k8s client given a context. If no context is provided, the default context is used.
async fn create_k8s_client(k8s_context: Option<String>) -> Result<Client> {
    let kubeconfig_options = kube::config::KubeConfigOptions {
        context: k8s_context,
        ..Default::default()
    };
    let kubeconfig = kube::Config::from_kubeconfig(&kubeconfig_options).await?;
    Ok(Client::try_from(kubeconfig)?)
}

/// Fetches the MaterializeDebug, telling apart the two ways it can be missing:
/// an operator too old to know the kind at all, and no resource for this
/// instance.
async fn fetch_materialize_debug(
    client: &Client,
    namespace: &str,
    name: &str,
) -> Result<MaterializeDebug> {
    let api = Api::<MaterializeDebug>::namespaced(client.clone(), namespace);
    match api.get(name).await {
        Ok(debug) => Ok(debug),
        Err(kube::Error::Api(response)) if response.code == 404 => {
            // Without the CRD the API server answers with a generic "could not
            // find the requested resource"; with it, a NotFound naming the
            // object.
            if response
                .message
                .contains("could not find the requested resource")
            {
                bail!(
                    "The cluster has no MaterializeDebug kind: the Materialize operator predates in-cluster debug collection. \
                     Upgrade the operator, or use an mz-debug release matching the operator's version."
                );
            }
            bail!(
                "No MaterializeDebug named {name} in namespace {namespace}. Enable the debug collector for the operator's instances with \
                 `debugCollector.enabled: true` in the operator's Helm values, or create a MaterializeDebug resource for the instance \
                 and pass its name with --debug-name if it differs from the instance name."
            );
        }
        Err(e) => {
            Err(e).with_context(|| format!("Failed to get MaterializeDebug {namespace}/{name}"))
        }
    }
}

struct CollectorClient {
    base_url: String,
    http: reqwest::Client,
}

impl CollectorClient {
    async fn request_snapshot(&self, request: &SnapshotRequest) -> Result<String> {
        let response = self
            .http
            .post(format!("{}/api/snapshots", self.base_url))
            .json(request)
            .send()
            .await
            .context("Failed to request a snapshot from the collector")?
            .error_for_status()
            .context("The collector rejected the snapshot request")?;
        let requested: SnapshotRequested = response
            .json()
            .await
            .context("Failed to parse the collector's response")?;
        Ok(requested.id)
    }

    async fn list(&self) -> Result<SnapshotList> {
        self.http
            .get(format!("{}/api/snapshots", self.base_url))
            .send()
            .await
            .context("Failed to list the collector's snapshots")?
            .error_for_status()
            .context("The collector failed to list its snapshots")?
            .json()
            .await
            .context("Failed to parse the collector's snapshot list")
    }

    /// Polls until snapshot `id` is complete. Returns `false` on timeout.
    async fn wait_for_snapshot(&self, id: &str, timeout: Duration) -> Result<bool> {
        let deadline = Instant::now() + timeout;
        loop {
            let list = self.list().await?;
            if list.snapshots.iter().any(|meta| meta.id == id) {
                return Ok(true);
            }
            let still_expected = list
                .in_progress
                .as_ref()
                .is_some_and(|status| status.id == id)
                || list.pending.as_ref().is_some_and(|status| status.id == id);
            if !still_expected {
                // Neither done nor queued: the attempt failed and the
                // collector has moved on.
                match list.last_error {
                    Some(error) => bail!("Snapshot {id} failed: {error}"),
                    None => bail!("Snapshot {id} was dropped by the collector without completing"),
                }
            }
            if Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(SNAPSHOT_POLL_INTERVAL).await;
        }
    }

    async fn download(&self, id: &str, path: &PathBuf) -> Result<()> {
        let response = self
            .http
            .get(format!("{}/api/snapshots/{}", self.base_url, id))
            .send()
            .await
            .with_context(|| format!("Failed to download snapshot {id}"))?
            .error_for_status()
            .with_context(|| format!("The collector failed to serve snapshot {id}"))?;
        let mut file = tokio::fs::File::create(path)
            .await
            .with_context(|| format!("Failed to create {}", path.display()))?;
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.with_context(|| format!("Failed while downloading snapshot {id}"))?;
            file.write_all(&chunk).await?;
        }
        file.flush().await?;
        Ok(())
    }
}
