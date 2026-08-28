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

//! The in-cluster flight recorder.
//!
//! One task owns snapshot execution, so snapshots never overlap: two
//! concurrent CPU profile captures of the same process would corrupt each
//! other's results. Periodic snapshots are triggered by a ticker, on-demand
//! ones arrive through the HTTP API and are run ahead of the next periodic
//! one, since somebody is waiting for them.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use kube::Client;
use serde::Serialize;
use tokio::sync::Notify;
use tokio::time::MissedTickBehavior;
use tracing::{error, info};

use crate::collector::snapshot::{SnapshotCategories, SnapshotRequest, SnapshotRunner};
use crate::collector::store::{SnapshotKind, SnapshotStore};
use crate::{Args, AuthMode, PasswordAuthCredentials};

pub mod http;
pub mod snapshot;
pub mod store;
pub mod targets;

/// Slack added to the snapshot interval when bounding a periodic snapshot's
/// pod logs, so consecutive snapshots overlap rather than leaving gaps when
/// a snapshot starts late.
const PERIODIC_LOG_WINDOW_SLACK: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum AuthModeArg {
    /// The instance runs without authentication; the internal listeners are
    /// used.
    None,
    /// The instance authenticates with passwords; the external listeners are
    /// used as `mz_system`, whose password is read from `MZ_PASSWORD`.
    Password,
}

/// Runs inside the cluster and continuously snapshots one Materialize
/// instance.
///
/// The global `--dump-*` flags select the categories of the periodic
/// snapshots. On-demand snapshots take their categories from the request
/// instead.
#[derive(Parser, Debug, Clone)]
pub struct CollectorArgs {
    /// The namespace the Materialize instance runs in.
    #[clap(long)]
    k8s_namespace: String,
    /// The name of the Materialize instance to collect from.
    #[clap(long)]
    mz_instance_name: String,
    /// Additional namespaces whose resources and logs are included.
    #[clap(long = "additional-k8s-namespace", action = clap::ArgAction::Append)]
    additional_k8s_namespaces: Vec<String>,
    /// Whether periodic snapshots include Kubernetes resources and pod logs.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    dump_k8s: bool,
    /// The address the HTTP API listens on.
    #[clap(long, default_value = "0.0.0.0:8080")]
    listen_addr: SocketAddr,
    /// The directory snapshots are stored in.
    #[clap(long, default_value = "/var/lib/mz-debug")]
    snapshot_dir: PathBuf,
    /// How often a periodic snapshot is taken.
    #[clap(long, default_value = "30m", value_parser = humantime::parse_duration)]
    snapshot_interval: Duration,
    /// How many snapshots to keep.
    #[clap(long, default_value = "12")]
    retained_snapshots: usize,
    /// The total size of retained snapshots, in bytes, above which the
    /// oldest are evicted.
    #[clap(long, default_value_t = 2 * 1024 * 1024 * 1024)]
    buffer_size_limit_bytes: u64,
    /// How the Materialize instance authenticates.
    #[clap(long, value_enum, default_value = "none")]
    auth_mode: AuthModeArg,
}

/// A snapshot that is running or waiting to run.
#[derive(Debug, Clone, Serialize)]
pub struct SnapshotStatus {
    pub id: String,
    pub kind: SnapshotKind,
    /// When the snapshot started, or for a pending one, when it was requested.
    pub since: DateTime<Utc>,
    pub categories: SnapshotCategories,
}

#[derive(Default)]
struct LoopState {
    in_progress: Option<SnapshotStatus>,
    /// At most one on-demand snapshot waits; further requests are merged
    /// into it.
    pending: Option<SnapshotStatus>,
    last_error: Option<String>,
}

/// What the HTTP API shares with the snapshot loop.
pub struct CollectorHandle {
    pub store: SnapshotStore,
    state: Mutex<LoopState>,
    /// Woken when a pending request is queued.
    wake: Notify,
    on_demand_defaults: SnapshotCategories,
}

impl CollectorHandle {
    /// Queues an on-demand snapshot and returns the id it will complete
    /// under.
    ///
    /// A request that a queued snapshot can absorb is merged into it, and a
    /// request fully covered by an on-demand snapshot that is running right
    /// now is answered with that snapshot, so a burst of identical requests
    /// costs one snapshot.
    pub fn request_snapshot(&self, request: &SnapshotRequest) -> String {
        let categories = self.on_demand_defaults.apply(request);
        let mut state = self.state.lock().expect("collector state poisoned");
        if let Some(pending) = &mut state.pending {
            pending.categories = pending.categories.union(categories);
            return pending.id.clone();
        }
        if let Some(in_progress) = &state.in_progress {
            if in_progress.kind == SnapshotKind::OnDemand
                && in_progress.categories.covers(&categories)
            {
                return in_progress.id.clone();
            }
        }
        let now = Utc::now();
        let id = self.store.new_id(now, SnapshotKind::OnDemand);
        state.pending = Some(SnapshotStatus {
            id: id.clone(),
            kind: SnapshotKind::OnDemand,
            since: now,
            categories,
        });
        self.wake.notify_one();
        id
    }

    fn status(
        &self,
    ) -> (
        Option<SnapshotStatus>,
        Option<SnapshotStatus>,
        Option<String>,
    ) {
        let state = self.state.lock().expect("collector state poisoned");
        (
            state.in_progress.clone(),
            state.pending.clone(),
            state.last_error.clone(),
        )
    }

    fn take_pending(&self) -> Option<SnapshotStatus> {
        self.state
            .lock()
            .expect("collector state poisoned")
            .pending
            .take()
    }

    fn set_in_progress(&self, status: Option<SnapshotStatus>) {
        self.state
            .lock()
            .expect("collector state poisoned")
            .in_progress = status;
    }

    fn set_last_error(&self, error: Option<String>) {
        self.state
            .lock()
            .expect("collector state poisoned")
            .last_error = error;
    }
}

pub async fn run(args: &Args, collector_args: &CollectorArgs) -> Result<()> {
    let auth_mode = match collector_args.auth_mode {
        AuthModeArg::None => AuthMode::None,
        AuthModeArg::Password => match (&args.mz_username, &args.mz_password) {
            (Some(username), Some(password)) => AuthMode::Password(PasswordAuthCredentials {
                username: username.clone(),
                password: password.clone(),
            }),
            _ => anyhow::bail!(
                "--auth-mode=password requires MZ_USERNAME and MZ_PASSWORD (or --mz-username and --mz-password)"
            ),
        },
    };

    let k8s_client = Client::try_default()
        .await
        .context("Failed to create Kubernetes client")?;

    let store = SnapshotStore::open(
        collector_args.snapshot_dir.clone(),
        collector_args.retained_snapshots,
        collector_args.buffer_size_limit_bytes,
    )
    .context("Failed to open the snapshot store")?;

    let periodic_categories = SnapshotCategories {
        k8s: collector_args.dump_k8s,
        system_catalog: args.dump_system_catalog,
        heap_profiles: args.dump_heap_profiles,
        prometheus_metrics: args.dump_prometheus_metrics,
        // CPU profiles are opt-in for periodic snapshots: each capture
        // disables memory profiling on the target for its duration and adds
        // sampling load, which is not worth paying every interval unasked.
        cpu_profiles: args.dump_cpu_profiles.unwrap_or(false),
        cpu_profile_duration_seconds: args.cpu_profile_duration_seconds,
    };

    let handle = Arc::new(CollectorHandle {
        store,
        state: Mutex::new(LoopState::default()),
        wake: Notify::new(),
        on_demand_defaults: SnapshotCategories::on_demand_defaults(
            args.cpu_profile_duration_seconds,
        ),
    });

    let runner = SnapshotRunner {
        k8s_client,
        k8s_namespace: collector_args.k8s_namespace.clone(),
        mz_instance_name: collector_args.mz_instance_name.clone(),
        additional_namespaces: collector_args.additional_k8s_namespaces.clone(),
        auth_mode,
        // environmentd's internal certificate covers only its service names
        // and is issued by the customer's own issuer, which this pod has no
        // trust root for, so certificates are not verified. The data fetched
        // is diagnostics from inside the same network policy boundary.
        http_client: reqwest::Client::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .context("Failed to build HTTP client")?,
    };

    let listener = tokio::net::TcpListener::bind(collector_args.listen_addr)
        .await
        .with_context(|| format!("Failed to bind {}", collector_args.listen_addr))?;
    info!("Serving the snapshot API on {}", collector_args.listen_addr);
    let server = {
        let handle = Arc::clone(&handle);
        mz_ore::task::spawn(|| "snapshot api", async move {
            axum::serve(listener, http::router(handle)).await
        })
    };

    let snapshot_loop = snapshot_loop(
        Arc::clone(&handle),
        runner,
        collector_args.snapshot_interval,
        periodic_categories,
    );

    tokio::select! {
        result = server => {
            match result {
                Ok(()) => anyhow::bail!("snapshot API server exited"),
                Err(e) => Err(anyhow::Error::new(e).context("snapshot API server failed")),
            }
        }
        () = snapshot_loop => anyhow::bail!("snapshot loop exited"),
    }
}

async fn snapshot_loop(
    handle: Arc<CollectorHandle>,
    runner: SnapshotRunner,
    interval: Duration,
    periodic_categories: SnapshotCategories,
) {
    let mut ticker = tokio::time::interval(interval);
    // A snapshot that runs past the next tick must not be followed by a
    // burst of catch-up snapshots.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let periodic_logs_since = interval + PERIODIC_LOG_WINDOW_SLACK;

    loop {
        let periodic_due = tokio::select! {
            _ = ticker.tick() => true,
            () = handle.wake.notified() => false,
        };

        // Whoever requested an on-demand snapshot is waiting on it, so it
        // goes before a periodic snapshot that fell due at the same time.
        if let Some(pending) = handle.take_pending() {
            take_snapshot(&handle, &runner, pending, None).await;
        }
        if periodic_due {
            let now = Utc::now();
            let status = SnapshotStatus {
                id: handle.store.new_id(now, SnapshotKind::Periodic),
                kind: SnapshotKind::Periodic,
                since: now,
                categories: periodic_categories,
            };
            take_snapshot(&handle, &runner, status, Some(periodic_logs_since)).await;
        }
    }
}

async fn take_snapshot(
    handle: &CollectorHandle,
    runner: &SnapshotRunner,
    status: SnapshotStatus,
    logs_since: Option<Duration>,
) {
    let started_at = Utc::now();
    let status = SnapshotStatus {
        since: started_at,
        ..status
    };
    info!("Taking {:?} snapshot {}", status.kind, status.id);
    handle.set_in_progress(Some(status.clone()));

    let workdir = handle.store.workdir(&status.id);
    let result = runner
        .run(
            &status.id,
            status.kind,
            started_at,
            workdir,
            status.categories,
            logs_since,
        )
        .await
        .and_then(|()| {
            handle
                .store
                .commit(&status.id, status.kind, started_at, status.categories)
        });
    match result {
        Ok(meta) => {
            info!(
                "Completed snapshot {} in {}s",
                meta.id,
                (meta.completed_at - meta.started_at).num_seconds()
            );
            handle.set_last_error(None);
        }
        Err(e) => {
            error!("Snapshot {} failed: {:#}", status.id, e);
            handle.store.abandon(&status.id);
            handle.set_last_error(Some(format!("{}: {:#}", status.id, e)));
        }
    }
    handle.set_in_progress(None);
}
