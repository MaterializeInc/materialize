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

//! Taking one snapshot: the same collection a CLI run does, into a work
//! directory, against targets reached directly from inside the cluster.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use kube::Client;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::collector::store::SnapshotKind;
use crate::collector::targets::{self, Targets};
use crate::internal_http_dumper::dump_in_cluster_http_resources;
use crate::k8s_dumper::K8sDumper;
use crate::system_catalog_dumper::SystemCatalogDumper;
use crate::{AuthMode, ContainerDumper, DumpConfig, create_mz_connection_url};

/// The SQL user the collector connects as. On the internal listener it is
/// accepted without a password; with password authentication its password
/// comes from the instance's backend secret.
const SQL_USER: &str = "mz_system";

/// Which parts of a snapshot to collect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCategories {
    pub k8s: bool,
    pub system_catalog: bool,
    pub heap_profiles: bool,
    pub prometheus_metrics: bool,
    pub cpu_profiles: bool,
    pub cpu_profile_duration_seconds: u64,
}

impl SnapshotCategories {
    /// The defaults for an on-demand snapshot: everything, including CPU
    /// profiles, matching what a CLI run collects by default.
    pub fn on_demand_defaults(cpu_profile_duration_seconds: u64) -> Self {
        Self {
            k8s: true,
            system_catalog: true,
            heap_profiles: true,
            prometheus_metrics: true,
            cpu_profiles: true,
            cpu_profile_duration_seconds,
        }
    }

    /// Applies the overrides of an on-demand request.
    pub fn apply(self, request: &SnapshotRequest) -> Self {
        Self {
            k8s: request.k8s.unwrap_or(self.k8s),
            system_catalog: request.system_catalog.unwrap_or(self.system_catalog),
            heap_profiles: request.heap_profiles.unwrap_or(self.heap_profiles),
            prometheus_metrics: request
                .prometheus_metrics
                .unwrap_or(self.prometheus_metrics),
            cpu_profiles: request.cpu_profiles.unwrap_or(self.cpu_profiles),
            cpu_profile_duration_seconds: request
                .cpu_profile_duration_seconds
                .unwrap_or(self.cpu_profile_duration_seconds),
        }
    }

    /// The categories that satisfy both `self` and `other`, used to coalesce
    /// requests that are waiting for the same snapshot.
    pub fn union(self, other: Self) -> Self {
        Self {
            k8s: self.k8s || other.k8s,
            system_catalog: self.system_catalog || other.system_catalog,
            heap_profiles: self.heap_profiles || other.heap_profiles,
            prometheus_metrics: self.prometheus_metrics || other.prometheus_metrics,
            cpu_profiles: self.cpu_profiles || other.cpu_profiles,
            cpu_profile_duration_seconds: self
                .cpu_profile_duration_seconds
                .max(other.cpu_profile_duration_seconds),
        }
    }

    /// Whether a snapshot with these categories contains everything a request
    /// for `other` asked for.
    pub fn covers(&self, other: &Self) -> bool {
        (self.k8s || !other.k8s)
            && (self.system_catalog || !other.system_catalog)
            && (self.heap_profiles || !other.heap_profiles)
            && (self.prometheus_metrics || !other.prometheus_metrics)
            && (self.cpu_profiles || !other.cpu_profiles)
            && (!other.cpu_profiles
                || self.cpu_profile_duration_seconds >= other.cpu_profile_duration_seconds)
    }

    fn any_http(&self) -> bool {
        self.heap_profiles || self.prometheus_metrics || self.cpu_profiles
    }
}

/// The body of `POST /api/snapshots`. Unset fields take the on-demand
/// defaults.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotRequest {
    pub k8s: Option<bool>,
    pub system_catalog: Option<bool>,
    pub heap_profiles: Option<bool>,
    pub prometheus_metrics: Option<bool>,
    pub cpu_profiles: Option<bool>,
    pub cpu_profile_duration_seconds: Option<u64>,
}

/// A record of the snapshot written into its root directory, so an extracted
/// zip carries its own provenance.
#[derive(Debug, Serialize)]
struct SnapshotManifest<'a> {
    id: &'a str,
    kind: SnapshotKind,
    started_at: DateTime<Utc>,
    mz_instance_name: &'a str,
    k8s_namespace: &'a str,
    categories: SnapshotCategories,
    /// The window of pod logs included, or `null` for the full logs.
    logs_since_seconds: Option<u64>,
}

/// Everything constant across snapshots of one instance.
pub struct SnapshotRunner {
    pub k8s_client: Client,
    pub k8s_namespace: String,
    pub mz_instance_name: String,
    pub additional_namespaces: Vec<String>,
    pub auth_mode: AuthMode,
    pub http_client: reqwest::Client,
}

impl SnapshotRunner {
    /// Collects `categories` into `workdir`. Failures of individual parts are
    /// logged and skipped, so the snapshot always holds whatever could be
    /// collected; only failing to create the directory is an error.
    pub async fn run(
        &self,
        id: &str,
        kind: SnapshotKind,
        started_at: DateTime<Utc>,
        workdir: PathBuf,
        categories: SnapshotCategories,
        logs_since: Option<Duration>,
    ) -> Result<()> {
        tokio::fs::create_dir_all(&workdir)
            .await
            .with_context(|| format!("Failed to create {}", workdir.display()))?;
        let config = DumpConfig {
            base_path: workdir.clone(),
            dump_system_catalog: categories.system_catalog,
            dump_heap_profiles: categories.heap_profiles,
            dump_prometheus_metrics: categories.prometheus_metrics,
            dump_cpu_profiles: categories.cpu_profiles,
            cpu_profile_duration_secs: categories.cpu_profile_duration_seconds,
        };

        let manifest = SnapshotManifest {
            id,
            kind,
            started_at,
            mz_instance_name: &self.mz_instance_name,
            k8s_namespace: &self.k8s_namespace,
            categories,
            logs_since_seconds: logs_since.map(|d| d.as_secs()),
        };
        if let Err(e) = tokio::fs::write(
            workdir.join("snapshot.json"),
            serde_json::to_vec_pretty(&manifest)?,
        )
        .await
        {
            warn!("Failed to write snapshot manifest: {}", e);
        }

        if categories.k8s {
            let mut dumper = K8sDumper::new(
                &config,
                self.k8s_client.clone(),
                self.k8s_namespace.clone(),
                Some(self.additional_namespaces.clone()),
            );
            if let Some(logs_since) = logs_since {
                dumper = dumper.with_logs_since(logs_since);
            }
            dumper.dump_container_resources().await;
        }

        if !categories.any_http() && !categories.system_catalog {
            return Ok(());
        }

        let targets = match targets::discover(
            &self.k8s_client,
            &self.k8s_namespace,
            &self.mz_instance_name,
        )
        .await
        {
            Ok(targets) => targets,
            Err(e) => {
                warn!(
                    "Failed to discover the instance's services, skipping profiles, metrics and the system catalog: {:#}",
                    e
                );
                return Ok(());
            }
        };

        if categories.any_http() {
            if let Err(e) = dump_in_cluster_http_resources(
                &config,
                &targets.http_targets,
                &self.auth_mode,
                &self.http_client,
            )
            .await
            {
                warn!("Failed to dump http resources: {:#}", e);
            }
        }

        if categories.system_catalog {
            self.dump_system_catalog(&config, &targets).await;
        }

        Ok(())
    }

    async fn dump_system_catalog(&self, config: &DumpConfig, targets: &Targets) {
        // With password authentication the internal SQL listener does not
        // exist, so the external one is used with mz_system's password. Without
        // it the internal listener accepts mz_system with no password.
        let (port_label, credentials) = match &self.auth_mode {
            AuthMode::Password(credentials) => ("sql", Some(credentials.clone())),
            AuthMode::None => (
                "internal-sql",
                Some(crate::PasswordAuthCredentials {
                    username: SQL_USER.to_owned(),
                    password: String::new(),
                }),
            ),
        };
        let Some(port) = targets
            .environmentd
            .service_ports
            .iter()
            .find(|port| port.name.as_deref() == Some(port_label))
        else {
            warn!(
                "environmentd service {} has no `{}` port, skipping the system catalog",
                targets.environmentd.service_name, port_label
            );
            return;
        };
        let url = create_mz_connection_url(targets.environmentd.fqdn(), port.port, credentials);

        info!("Dumping the system catalog");
        match SystemCatalogDumper::new(&url, config.base_path.clone()).await {
            Ok(dumper) => dumper.dump_all_relations().await,
            Err(e) => warn!("Failed to connect to the system catalog: {:#}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn request_overrides_apply_and_coalesce() {
        let defaults = SnapshotCategories::on_demand_defaults(10);
        assert!(defaults.cpu_profiles);

        let no_cpu = defaults.apply(&SnapshotRequest {
            cpu_profiles: Some(false),
            k8s: Some(false),
            ..Default::default()
        });
        assert!(!no_cpu.cpu_profiles && !no_cpu.k8s && no_cpu.system_catalog);

        let long_cpu = defaults.apply(&SnapshotRequest {
            cpu_profile_duration_seconds: Some(30),
            ..Default::default()
        });
        assert!(defaults.covers(&no_cpu));
        assert!(!no_cpu.covers(&defaults), "missing cpu profiles");
        assert!(!defaults.covers(&long_cpu), "shorter cpu profile");
        assert!(long_cpu.covers(&defaults));

        let merged = no_cpu.union(long_cpu);
        assert!(merged.k8s && merged.cpu_profiles);
        assert_eq!(merged.cpu_profile_duration_seconds, 30);
    }
}
