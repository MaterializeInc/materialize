// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reconciler for Materialize resources.
//!
//! Each reconcile takes one step of a state machine: read `status.phase`,
//! derive a single `Step` with the pure `plan_step`, execute it. A step
//! performs at most one action and at most one status write. The write is the
//! step's last effect, so a step that fails partway is planned again from the
//! same status, and every action has to be idempotent.
//!
//! A pass over a resource applies the global resources, rolls out a new
//! generation if the spec calls for one, then applies the balancer and the
//! console. `status.rollout` records what a rollout was started for, so a
//! spec change is detected at every step of the rollout, and a completed
//! rollout records what was promoted.
//!
//! Conditions are for display. Planning never reads them back, apart from
//! `current_status`.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::Context as _;
use http::HeaderValue;
use k8s_controller::{
    Outcome, TraceMetadata,
    events::{Event, EventType},
};
use k8s_openapi::{
    api::core::v1::{Affinity, Probe, ResourceRequirements, Secret, Toleration},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
    jiff::{SignedDuration, Timestamp},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ListParams, PostParams},
    runtime::controller::Action,
};
use sha2::{Digest, Sha256};
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::{
    Error,
    controller::materialize::generation::V161,
    k8s::{apply_resource, delete_resource},
    matching_image_from_environmentd_image_ref,
    metrics::Metrics,
    parse_image_tag,
    tls::{DefaultCertificateSpecs, issuer_ref_defined, resolved_dns_names},
};
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::{
    ManagedResource,
    balancer::v1alpha1::{Balancer, BalancerSpec},
    console::v1alpha1::{BalancerdRef, Console, ConsoleSpec, HttpConnectionScheme},
    materialize::v1alpha1::{Materialize, MaterializeStatus},
    materialize::{MaterializePhase, MaterializeRolloutStrategy, RolloutStatus},
};
use mz_license_keys::validate;
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::{cast::CastFrom, cli::KeyValueArg, instrument};

pub mod generation;
pub mod global;
pub mod teleport;

/// The name identifying this controller in its reconciliation metrics and in
/// the reporter of the events it publishes.
pub const CONTROLLER_NAME: &str = "materialize";

#[derive(Clone)]
pub struct Config {
    pub cloud_provider: CloudProvider,
    pub region: String,
    pub create_balancers: bool,
    pub create_console: bool,
    pub helm_chart_version: Option<String>,
    pub secrets_controller: String,
    pub collect_pod_metrics: bool,
    pub enable_prometheus_scrape_annotations: bool,

    pub segment_api_key: Option<String>,
    pub segment_client_side: bool,

    pub console_image_tag_default: String,
    pub console_image_tag_map: Vec<KeyValueArg<String, String>>,

    pub aws_account_id: Option<String>,
    pub environmentd_iam_role_arn: Option<String>,
    pub environmentd_connection_role_arn: Option<String>,
    pub aws_secrets_controller_tags: Vec<String>,
    pub environmentd_availability_zones: Option<Vec<String>>,

    pub ephemeral_volume_class: Option<String>,
    pub scheduler_name: Option<String>,
    pub enable_security_context: bool,
    /// Whether to run environmentd and clusterd pods in a Kubernetes user
    /// namespace (`hostUsers: false`).
    pub enable_user_namespaces: bool,
    pub enable_internal_statement_logging: bool,
    pub statement_logging_max_sample_rate: Option<f64>,
    pub statement_logging_target_data_rate: Option<usize>,

    pub orchestratord_pod_selector_labels: Vec<KeyValueArg<String, String>>,
    pub environmentd_node_selector: Vec<KeyValueArg<String, String>>,
    pub environmentd_affinity: Option<Affinity>,
    pub environmentd_tolerations: Option<Vec<Toleration>>,
    pub environmentd_default_resources: Option<ResourceRequirements>,
    /// Overlaid onto the built-in environmentd readiness probe. See
    /// `generation::environmentd_probe` for the overlay semantics.
    pub environmentd_default_readiness_probe: Option<Probe>,
    /// Overlaid onto the built-in environmentd liveness probe. See
    /// `generation::environmentd_probe` for the overlay semantics.
    pub environmentd_default_liveness_probe: Option<Probe>,
    /// Overlaid onto the built-in environmentd probe to form the startup
    /// probe. See `generation::environmentd_probe` for the overlay semantics.
    pub environmentd_default_startup_probe: Option<Probe>,
    /// The name of a `PriorityClass` to assign to environmentd pods, if any.
    pub environmentd_priority_class_name: Option<String>,
    pub clusterd_node_selector: Vec<KeyValueArg<String, String>>,
    pub clusterd_affinity: Option<Affinity>,
    pub clusterd_tolerations: Option<Vec<Toleration>>,
    /// The name of a `PriorityClass` to assign to clusterd pods, if any.
    pub clusterd_priority_class_name: Option<String>,
    pub image_pull_policy: KubernetesImagePullPolicy,
    pub network_policies_internal_enabled: bool,
    pub network_policies_ingress_enabled: bool,
    pub network_policies_ingress_cidrs: Vec<String>,
    pub network_policies_egress_enabled: bool,
    pub network_policies_egress_cidrs: Vec<String>,

    pub environmentd_cluster_replica_sizes: Option<String>,
    pub bootstrap_default_cluster_replica_size: Option<String>,
    pub bootstrap_builtin_system_cluster_replica_size: Option<String>,
    pub bootstrap_builtin_probe_cluster_replica_size: Option<String>,
    pub bootstrap_builtin_support_cluster_replica_size: Option<String>,
    pub bootstrap_builtin_catalog_server_cluster_replica_size: Option<String>,
    pub bootstrap_builtin_analytics_cluster_replica_size: Option<String>,
    pub bootstrap_builtin_system_cluster_replication_factor: Option<u32>,
    pub bootstrap_builtin_probe_cluster_replication_factor: Option<u32>,
    pub bootstrap_builtin_support_cluster_replication_factor: Option<u32>,
    pub bootstrap_builtin_analytics_cluster_replication_factor: Option<u32>,

    pub environmentd_allowed_origins: Vec<HeaderValue>,
    pub internal_console_proxy_url: String,

    pub environmentd_sql_port: u16,
    pub environmentd_http_port: u16,
    pub environmentd_internal_sql_port: u16,
    pub environmentd_internal_http_port: u16,
    pub environmentd_internal_persist_pubsub_port: u16,

    pub default_certificate_specs: DefaultCertificateSpecs,

    pub disable_license_key_checks: bool,
    pub disable_database_network_policies: bool,

    pub tracing: TracingCliArgs,
    pub orchestratord_namespace: String,

    pub teleport: Option<teleport::TeleportConfig>,
}

/// How long an idle resource goes without a full pass. Shorter than
/// `k8s_controller`'s 40 to 60 minute periodic requeue, so every periodic
/// reconcile of an idle resource starts one.
const RESYNC_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// The requeue after a step that advanced the state machine. The step's
/// status write already triggers a reconcile through the watch, so this only
/// covers a missed watch event.
const STEP_REQUEUE: Duration = Duration::from_secs(1);

/// The single action a reconcile takes.
#[derive(Debug, PartialEq)]
enum Step {
    /// Apply the global resources, then move to `next_phase`.
    ApplyGlobalResources {
        next_phase: MaterializePhase,
    },
    /// No rollout to run. Record whether the active generation is up to date.
    Settle {
        up_to_date: bool,
    },
    /// The spec asks for an upgrade outside the upgrade window. Report it and
    /// wait for the spec to change, or for a rollout in flight to time out.
    RefuseUpgrade {
        timeout_in: Option<Duration>,
    },
    /// Record the rollout in the status. This is its own step, ahead of
    /// applying anything, so that a candidate generation never exists without
    /// a status that says to tear it down if the spec is reverted.
    BeginRollout,
    TearDownActiveGeneration,
    ApplyCandidate,
    AwaitCandidate,
    /// The candidate waits for `forcePromote`. Nothing to do until the spec
    /// changes or the rollout times out.
    AwaitPromotion {
        timeout_in: Option<Duration>,
    },
    CancelRollout(Cancel),
    Promote,
    TearDownPreviousGeneration,
    SyncBalancer,
    SyncConsole,
}

#[derive(Debug, PartialEq)]
enum Cancel {
    /// The spec no longer asks for the rollout.
    Reverted,
    TimedOut,
}

/// What one reconcile observed, gathered up front so that [`plan_step`] stays
/// pure.
struct Observed {
    now: Timestamp,
    /// Whether the spec, labels or annotations differ from the ones this
    /// process last applied the global resources for.
    inputs_changed: bool,
    /// Whether this process last started a pass at least [`RESYNC_INTERVAL`]
    /// ago.
    resync_due: bool,
    /// The hash of the generation resources the spec produces for the active
    /// generation.
    active_hash: String,
    /// The hash of the generation resources the spec produces for the
    /// generation after the active one.
    candidate_hash: String,
}

fn plan_step(mz: &Materialize, status: &MaterializeStatus, observed: &Observed) -> Step {
    let phase = status.phase.unwrap_or_default();
    let rollout = status.rollout.as_ref();

    // No step leaves a rollout behind when it goes idle. Cancel one that a
    // status claims anyway, so that it cannot idle with a candidate generation
    // running.
    if phase == MaterializePhase::Idle && rollout.is_some() {
        return Step::CancelRollout(Cancel::Reverted);
    }

    // The global resources come first in every phase, because the generation
    // resources, the balancer and the console all depend on them.
    let stale = observed.inputs_changed || (phase == MaterializePhase::Idle && observed.resync_due);
    if stale || phase == MaterializePhase::Idle {
        let next_phase = match rollout {
            Some(_) => phase,
            None if stale => MaterializePhase::PlanningRollout,
            // Reapplied on every reconcile of an idle resource, which is how
            // a change to one of the watched global resources gets reverted.
            None => MaterializePhase::Idle,
        };
        return Step::ApplyGlobalResources { next_phase };
    }

    let has_changes = status.resources_hash != observed.active_hash;
    let rollout_wanted = has_changes && mz.rollout_requested();
    match (phase, rollout) {
        // Past the point of no return. The candidate may already be the
        // leader, so the rollout completes whatever the spec says now.
        (MaterializePhase::Promoting, Some(_)) => Step::Promote,
        (MaterializePhase::TearingDownPreviousGeneration, Some(_)) => {
            Step::TearDownPreviousGeneration
        }
        (
            MaterializePhase::TearingDownActiveGeneration
            | MaterializePhase::ApplyingCandidate
            | MaterializePhase::AwaitingCandidate
            | MaterializePhase::ReadyToPromote,
            Some(rollout),
        ) => {
            if !rollout_wanted {
                return Step::CancelRollout(Cancel::Reverted);
            }
            let timeout_in = rollout_timeout_in(mz, rollout, observed.now);
            if timeout_in.is_some_and(|timeout_in| timeout_in.is_zero()) {
                return Step::CancelRollout(Cancel::TimedOut);
            }
            if !mz.within_upgrade_window() {
                return Step::RefuseUpgrade { timeout_in };
            }
            if rollout.request != mz.requested_reconciliation_id()
                || rollout.resources_hash != observed.candidate_hash
            {
                // The spec changed under the rollout. Start over, updating
                // the candidate generation in place.
                return Step::BeginRollout;
            }
            match phase {
                // Leaving the strategy mid-teardown must not delete an active
                // generation that the new strategy keeps serving from.
                MaterializePhase::TearingDownActiveGeneration
                    if mz.spec.rollout_strategy
                        == MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime =>
                {
                    Step::TearDownActiveGeneration
                }
                MaterializePhase::TearingDownActiveGeneration
                | MaterializePhase::ApplyingCandidate => Step::ApplyCandidate,
                MaterializePhase::ReadyToPromote
                    if mz.spec.rollout_strategy == MaterializeRolloutStrategy::ManuallyPromote
                        && !mz.should_force_promote() =>
                {
                    Step::AwaitPromotion { timeout_in }
                }
                _ => Step::AwaitCandidate,
            }
        }
        (MaterializePhase::SyncingBalancer, _) => Step::SyncBalancer,
        (MaterializePhase::SyncingConsole, _) => Step::SyncConsole,
        _ if !rollout_wanted => Step::Settle {
            up_to_date: !has_changes,
        },
        _ if !mz.within_upgrade_window() => Step::RefuseUpgrade { timeout_in: None },
        _ => Step::BeginRollout,
    }
}

/// How long until `rollout` exceeds the rollout request timeout. Zero if it
/// already has, `None` if it never will.
fn rollout_timeout_in(
    mz: &Materialize,
    rollout: &RolloutStatus,
    now: Timestamp,
) -> Option<Duration> {
    // A force-promoting rollout is never cancelled. Under the
    // `ImmediatelyPromoteCausingDowntime` strategy the active generation is
    // already torn down, leaving nothing to fall back to.
    if mz.should_force_promote() {
        return None;
    }
    let timeout = SignedDuration::try_from(mz.rollout_request_timeout()).ok()?;
    let remaining = timeout.checked_sub(now.duration_since(rollout.started_at.0))?;
    Some(Duration::try_from(remaining).unwrap_or(Duration::ZERO))
}

/// `status`, with `phase` and `rollout` derived from the `UpToDate` condition
/// if the status was written before `phase` existed.
fn current_status(mz: &Materialize, mut status: MaterializeStatus) -> MaterializeStatus {
    if status.phase.is_some() {
        return status;
    }
    let condition = status
        .conditions
        .iter()
        .find(|condition| condition.type_ == "UpToDate");
    // Such a status held the candidate's hash in `resources_hash` once the
    // candidate was ready, and no hash while it was being applied. An empty
    // hash here makes `plan_step` begin that rollout again.
    let (phase, resources_hash) = match condition.map(|condition| condition.reason.as_str()) {
        Some("Applying") => (MaterializePhase::ApplyingCandidate, String::new()),
        Some("ReadyToPromote") => (
            MaterializePhase::ReadyToPromote,
            status.resources_hash.clone(),
        ),
        Some("Promoting") => (MaterializePhase::Promoting, status.resources_hash.clone()),
        _ => {
            status.phase = Some(MaterializePhase::Idle);
            return status;
        }
    };
    status.rollout = Some(RolloutStatus {
        generation: status.active_generation + 1,
        request: mz.requested_reconciliation_id(),
        environmentd_image_ref: mz.spec.environmentd_image_ref.clone(),
        resources_hash,
        started_at: condition
            .expect("matched on its reason")
            .last_transition_time
            .clone(),
    });
    status.phase = Some(phase);
    status
}

/// A hash of everything in `mz` that a pass reads.
fn inputs_hash(mz: &Materialize) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        serde_json::to_string(&(&mz.spec, &mz.metadata.labels, &mz.metadata.annotations))
            .expect("spec and metadata serialize to JSON"),
    );
    hex::encode(hasher.finalize())
}

fn up_to_date_condition(
    mz: &Materialize,
    status: &str,
    reason: &str,
    message: String,
) -> Vec<Condition> {
    vec![Condition {
        type_: "UpToDate".into(),
        status: status.into(),
        last_transition_time: Time(mz.up_to_date_transition_time(status, Timestamp::now())),
        message,
        observed_generation: mz.meta().generation,
        reason: reason.into(),
    }]
}

/// The conditions for a candidate generation that is being applied or awaited
/// without error. Replaces a `FailedDeploy` left by an earlier attempt, which
/// would otherwise stay until the candidate is ready.
fn retried_conditions(
    mz: &Materialize,
    status: &MaterializeStatus,
    candidate_generation: u64,
) -> Vec<Condition> {
    let failed = status
        .conditions
        .iter()
        .any(|condition| condition.reason == "FailedDeploy");
    if failed {
        up_to_date_condition(
            mz,
            "Unknown",
            "Applying",
            format!("Applying changes for generation {candidate_generation}"),
        )
    } else {
        status.conditions.clone()
    }
}

/// The inputs a pass was last started for, and when.
struct Pass {
    inputs_hash: String,
    started: Instant,
}

pub struct Context {
    config: Config,
    metrics: Arc<Metrics>,
    needs_update: Arc<Mutex<BTreeSet<String>>>,
    /// The last pass this process started for each resource, by namespace and
    /// name. Empty after a restart, so every resource then gets a full pass
    /// under the new configuration.
    passes: Mutex<BTreeMap<String, Pass>>,
}

impl Context {
    pub fn new(config: Config, metrics: Arc<Metrics>) -> Self {
        if config.cloud_provider == CloudProvider::Aws {
            assert!(
                config.aws_account_id.is_some(),
                "--aws-account-id is required when using --cloud-provider=aws"
            );
        }

        Self {
            config,
            metrics,
            needs_update: Default::default(),
            passes: Default::default(),
        }
    }

    fn set_needs_update(&self, mz: &Materialize, needs_update: bool) {
        let mut needs_update_set = self.needs_update.lock().unwrap();
        if needs_update {
            needs_update_set.insert(mz.name_unchecked());
        } else {
            needs_update_set.remove(&mz.name_unchecked());
        }
        self.metrics
            .environmentd_needs_update
            .set(u64::cast_from(needs_update_set.len()));
    }

    /// Writes `status`, if it differs from the current status in anything but
    /// its timestamps, and publishes the `UpToDate` condition as an event if
    /// the write changed it.
    async fn update_status(
        &self,
        metadata: &TraceMetadata,
        mz_api: &Api<Materialize>,
        mz: &Materialize,
        status: MaterializeStatus,
    ) -> Result<(), kube::Error> {
        if mz
            .status
            .as_ref()
            .is_some_and(|mz_status| !mz_status.needs_update(&status))
        {
            return Ok(());
        }

        let unchanged = |current: &Condition, new: &Condition| {
            let mut current = current.clone();
            current.last_transition_time = new.last_transition_time.clone();
            current == *new
        };
        let current = mz
            .status
            .as_ref()
            .and_then(|mz_status| mz_status.conditions.first());
        let condition = status
            .conditions
            .first()
            .filter(|new| !current.is_some_and(|current| unchanged(current, new)))
            .cloned();

        let mut new_mz = mz.clone();
        new_mz.status = Some(status);
        mz_api
            .replace_status(&mz.name_unchecked(), &PostParams::default(), &new_mz)
            .await?;

        if let Some(condition) = condition {
            metadata
                .publish_event(Event {
                    type_: transition_event_type(&condition),
                    reason: condition.reason,
                    action: "Reconcile".into(),
                    note: Some(condition.message),
                    related: None,
                })
                .await;
        }

        Ok(())
    }

    /// Deletes `generation`'s resources, releasing the read holds its
    /// environmentd was keeping.
    async fn teardown_generation(
        &self,
        metadata: &TraceMetadata,
        client: &Client,
        mz: &Materialize,
        resources: &generation::Resources,
        generation: u64,
    ) -> Result<(), Error> {
        let step = metadata.step("teardown_generation");
        resources
            .teardown_generation(client, mz, generation)
            .await?;
        step.finish(Outcome::Completed);
        Ok(())
    }

    /// Reports `error`, from applying or checking on the candidate
    /// generation, in the `UpToDate` condition. The phase is kept, so the
    /// next reconcile retries.
    async fn fail_deploy(
        &self,
        metadata: &TraceMetadata,
        mz_api: &Api<Materialize>,
        mz: &Materialize,
        status: MaterializeStatus,
        generation: u64,
        error: Error,
    ) -> Error {
        let failed = MaterializeStatus {
            conditions: up_to_date_condition(
                mz,
                "False",
                "FailedDeploy",
                format!("Failed to apply changes for generation {generation}: {error}"),
            ),
            ..status
        };
        match self.update_status(metadata, mz_api, mz, failed).await {
            Ok(()) => error,
            Err(e) => e.into(),
        }
    }

    async fn sync_balancer(
        &self,
        metadata: &TraceMetadata,
        client: &Client,
        mz: &Materialize,
        resource_id: &str,
    ) -> Result<Option<Action>, Error> {
        let balancer_api: Api<Balancer> = Api::namespaced(client.clone(), &mz.namespace());
        let step = metadata.step("balancer");
        if !self.config.create_balancers {
            delete_resource(&balancer_api, &mz.name_unchecked()).await?;
            step.finish(Outcome::Skipped);
            return Ok(None);
        }
        let balancer = Balancer {
            metadata: mz.managed_resource_meta(mz.name_unchecked()),
            spec: BalancerSpec {
                balancerd_image_ref: matching_image_from_environmentd_image_ref(
                    mz.active_environmentd_image_ref(),
                    "balancerd",
                    None,
                ),
                resource_requirements: mz.spec.balancerd_resource_requirements.clone(),
                configmap_name: mz.spec.balancerd_configmap_name.clone(),
                replicas: Some(mz.balancerd_replicas()),
                externally_scaled: mz.spec.balancerd_externally_scaled,
                external_certificate_spec: mz.spec.balancerd_external_certificate_spec.clone(),
                internal_certificate_spec: mz.spec.internal_certificate_spec.clone(),
                pod_annotations: mz.spec.pod_annotations.clone(),
                pod_labels: mz.spec.pod_labels.clone(),
                static_routing: Some(
                    mz_cloud_resources::crd::balancer::v1alpha1::StaticRoutingConfig {
                        environmentd_namespace: mz.namespace(),
                        environmentd_service_name: mz.environmentd_service_name(),
                    },
                ),
                frontegg_routing: None,
                resource_id: Some(resource_id.to_owned()),
            },
            status: None,
        };
        let balancer = apply_resource(&balancer_api, &balancer).await?;
        let result = wait_for_balancer(&balancer);
        step.finish_with(&result);
        result
    }

    async fn sync_console(
        &self,
        metadata: &TraceMetadata,
        client: &Client,
        mz: &Materialize,
        resource_id: &str,
    ) -> Result<(), Error> {
        let console_api: Api<Console> = Api::namespaced(client.clone(), &mz.namespace());
        let step = metadata.step("console");
        if !self.config.create_console {
            delete_resource(&console_api, &mz.name_unchecked()).await?;
            step.finish(Outcome::Skipped);
            return Ok(());
        }
        let active_environmentd_image_ref = mz.active_environmentd_image_ref();
        let environmentd_image_tag =
            parse_image_tag(active_environmentd_image_ref).unwrap_or("latest");
        let console_image_tag = self
            .config
            .console_image_tag_map
            .iter()
            .find(|kv| kv.key == environmentd_image_tag)
            .map(|kv| kv.value.clone())
            .unwrap_or_else(|| self.config.console_image_tag_default.clone());
        let console = Console {
            metadata: mz.managed_resource_meta(mz.name_unchecked()),
            spec: ConsoleSpec {
                console_image_ref: matching_image_from_environmentd_image_ref(
                    active_environmentd_image_ref,
                    "console",
                    Some(&console_image_tag),
                ),
                resource_requirements: mz.spec.console_resource_requirements.clone(),
                replicas: Some(mz.console_replicas()),
                external_certificate_spec: mz.spec.console_external_certificate_spec.clone(),
                pod_annotations: mz.spec.pod_annotations.clone(),
                pod_labels: mz.spec.pod_labels.clone(),
                balancerd: BalancerdRef {
                    service_name: mz.balancerd_service_name(),
                    namespace: mz.namespace(),
                    scheme: if issuer_ref_defined(
                        &self.config.default_certificate_specs.balancerd_external,
                        &mz.spec.balancerd_external_certificate_spec,
                    ) {
                        HttpConnectionScheme::Https
                    } else {
                        HttpConnectionScheme::Http
                    },
                    dns_names: resolved_dns_names(
                        &self.config.default_certificate_specs.balancerd_external,
                        &mz.spec.balancerd_external_certificate_spec,
                    ),
                },
                authenticator_kind: mz.spec.authenticator_kind,
                resource_id: Some(resource_id.to_owned()),
            },
            status: None,
        };
        apply_resource(&console_api, &console).await?;
        step.finish(Outcome::Completed);
        Ok(())
    }

    async fn check_environment_id_conflicts(
        &self,
        client: &Client,
        mz: &Materialize,
    ) -> Result<(), Error> {
        if mz.spec.environment_id.is_nil() {
            // this is always a bug - we delay doing this check until the
            // resource should have an environment id set, either from the
            // license key, or explicitly given, or randomly defaulted.
            return Err(Error::Anyhow(anyhow::anyhow!(
                "trying to reconcile a materialize resource with no environment id - this is a bug!"
            )));
        }

        let mz_api: Api<Materialize> = Api::all(client.clone());
        let all_mz = mz_api.list(&ListParams::default()).await?;
        for existing_mz in &all_mz.items {
            if existing_mz.spec.environment_id == mz.spec.environment_id
                && existing_mz.metadata.uid != mz.metadata.uid
            {
                return Err(Error::Anyhow(anyhow::anyhow!(
                    "Materialize resources {}/{} and {}/{} have the environmentId field set to the same value. This field must be unique across environments.",
                    mz.namespace(),
                    mz.name_unchecked(),
                    existing_mz.namespace(),
                    existing_mz.name_unchecked(),
                )));
            }
        }

        Ok(())
    }

    /// Executes `step`. The status write, if the step has one, is its last
    /// effect.
    async fn execute(
        &self,
        metadata: &TraceMetadata,
        client: &Client,
        mz: &Materialize,
        status: MaterializeStatus,
        observed: Observed,
        step: Step,
    ) -> Result<Option<Action>, Error> {
        let mz_api: Api<Materialize> = Api::namespaced(client.clone(), &mz.namespace());
        let namespace = mz.namespace();
        let active_generation = status.active_generation;
        let candidate_generation = active_generation + 1;
        let candidate = generation::Resources::new(&self.config, mz, candidate_generation);
        let advanced = Ok(Some(Action::requeue(STEP_REQUEUE)));

        match step {
            Step::ApplyGlobalResources { next_phase } => {
                let step = metadata.step("global_resources");
                global::Resources::new(&self.config, mz)?
                    .apply(client, &namespace)
                    .await?;
                step.finish(Outcome::Completed);
                let next = MaterializeStatus {
                    phase: Some(next_phase),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                // Recorded after the status write. A pass recorded for a
                // write that failed would hide the changed inputs from the
                // retry until the next resync.
                if observed.inputs_changed || observed.resync_due {
                    self.passes.lock().unwrap().insert(
                        pass_key(mz),
                        Pass {
                            inputs_hash: inputs_hash(mz),
                            started: Instant::now(),
                        },
                    );
                }
                if next_phase == MaterializePhase::Idle {
                    debug!("no changes");
                    return Ok(None);
                }
                advanced
            }
            Step::Settle { up_to_date } => {
                let mut next = MaterializeStatus {
                    phase: Some(MaterializePhase::SyncingBalancer),
                    ..status
                };
                if up_to_date {
                    // A rollout requested with nothing to roll out completes
                    // as a no-op.
                    if mz.conditions_need_update() || mz.rollout_requested() {
                        next.conditions = up_to_date_condition(
                            mz,
                            "True",
                            "Applied",
                            format!("No changes found from generation {active_generation}"),
                        );
                    }
                    next.last_completed_rollout_request = mz.requested_reconciliation_id();
                    next.last_completed_rollout_hash = None;
                } else if mz.conditions_need_update() {
                    next.conditions = up_to_date_condition(
                        mz,
                        "False",
                        "WaitingForApproval",
                        format!(
                            "Changes detected, waiting for approval for generation {candidate_generation}"
                        ),
                    );
                }
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::RefuseUpgrade { timeout_in } => {
                let next = MaterializeStatus {
                    conditions: up_to_date_condition(
                        mz,
                        "False",
                        "FailedDeploy",
                        format!(
                            "Refusing to upgrade from {} to {}. \
                             More than one major version from \
                             last successful rollout. If coming \
                             from Self Managed 25.2, upgrade to \
                             materialize/environmentd:v0.147.20 \
                             first.",
                            status
                                .last_completed_rollout_environmentd_image_ref
                                .as_deref()
                                .expect("should be set if upgrade window check fails"),
                            mz.spec.environmentd_image_ref,
                        ),
                    ),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                Ok(timeout_in.map(Action::requeue))
            }
            Step::BeginRollout => {
                let tear_down_active = mz.spec.rollout_strategy
                    == MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime;
                let next = MaterializeStatus {
                    phase: Some(if tear_down_active {
                        MaterializePhase::TearingDownActiveGeneration
                    } else {
                        MaterializePhase::ApplyingCandidate
                    }),
                    // Cleared before the teardown starts. A teardown whose
                    // status write fails has already deleted the active
                    // generation, and a spec reverted at that point would
                    // otherwise match the stale hash and settle as up to
                    // date with nothing running.
                    resources_hash: if tear_down_active {
                        String::new()
                    } else {
                        status.resources_hash.clone()
                    },
                    rollout: Some(RolloutStatus {
                        generation: candidate_generation,
                        request: mz.requested_reconciliation_id(),
                        environmentd_image_ref: mz.spec.environmentd_image_ref.clone(),
                        resources_hash: observed.candidate_hash,
                        started_at: Time(observed.now),
                    }),
                    conditions: up_to_date_condition(
                        mz,
                        "Unknown",
                        "Applying",
                        format!("Applying changes for generation {candidate_generation}"),
                    ),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::TearDownActiveGeneration => {
                // The only reason someone would choose this strategy is if
                // they didn't have space for the two generations of pods.
                self.teardown_generation(metadata, client, mz, &candidate, active_generation)
                    .await?;
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::ApplyingCandidate),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::ApplyCandidate => {
                trace!("applying environment resources");
                let step = metadata.step("generation_resources");
                let applied = candidate.apply(client, &namespace).await;
                if let Err(e) = applied {
                    step.finish(Outcome::Failed);
                    return Err(self
                        .fail_deploy(metadata, &mz_api, mz, status, candidate_generation, e)
                        .await);
                }
                step.finish(Outcome::Completed);
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::AwaitingCandidate),
                    conditions: retried_conditions(mz, &status, candidate_generation),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::AwaitCandidate => {
                let step = metadata.step("generation_ready");
                let ready = candidate
                    .await_ready(client, mz.should_force_promote(), &namespace)
                    .await;
                step.finish_with(&ready);
                match ready {
                    Ok(Some(action)) => {
                        trace!("new environment is not yet ready");
                        let next = MaterializeStatus {
                            conditions: retried_conditions(mz, &status, candidate_generation),
                            ..status
                        };
                        self.update_status(metadata, &mz_api, mz, next).await?;
                        return Ok(Some(action));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Err(self
                            .fail_deploy(metadata, &mz_api, mz, status, candidate_generation, e)
                            .await);
                    }
                }
                let next = if mz.spec.rollout_strategy
                    == MaterializeRolloutStrategy::ManuallyPromote
                    && !mz.should_force_promote()
                {
                    MaterializeStatus {
                        phase: Some(MaterializePhase::ReadyToPromote),
                        conditions: up_to_date_condition(
                            mz,
                            "Unknown",
                            "ReadyToPromote",
                            format!("Ready to promote generation {candidate_generation}"),
                        ),
                        ..status
                    }
                } else {
                    // Once this status is written, the rollout must promote
                    // without taking other actions.
                    MaterializeStatus {
                        phase: Some(MaterializePhase::Promoting),
                        conditions: up_to_date_condition(
                            mz,
                            "Unknown",
                            "Promoting",
                            format!("Attempting to promote generation {candidate_generation}"),
                        ),
                        ..status
                    }
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::AwaitPromotion { timeout_in } => {
                trace!(
                    "Ready to promote, but not promoting because the instance is configured with ManuallyPromote rollout strategy."
                );
                Ok(timeout_in.map(Action::requeue))
            }
            Step::CancelRollout(cancel) => {
                // Tear down the un-promoted generation to release its read
                // holds. Promoting it after a long delay can cause
                // incident-inducing load, which is why a rollout that times
                // out is cancelled.
                self.teardown_generation(metadata, client, mz, &candidate, candidate_generation)
                    .await?;
                let conditions = match cancel {
                    Cancel::TimedOut => {
                        warn!(
                            "rollout to generation {candidate_generation} exceeded timeout, cancelling"
                        );
                        up_to_date_condition(
                            mz,
                            "False",
                            "RolloutTimeout",
                            format!(
                                "Cancelled rollout to generation \
                                 {candidate_generation} after it \
                                 exceeded the rollout timeout of {}",
                                humantime::format_duration(mz.rollout_request_timeout()),
                            ),
                        )
                    }
                    Cancel::Reverted if status.resources_hash == observed.active_hash => {
                        up_to_date_condition(
                            mz,
                            "True",
                            "Applied",
                            format!("No changes found from generation {active_generation}"),
                        )
                    }
                    Cancel::Reverted => up_to_date_condition(
                        mz,
                        "False",
                        "WaitingForApproval",
                        format!(
                            "Changes detected, waiting for approval for generation {candidate_generation}"
                        ),
                    ),
                };
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::SyncingBalancer),
                    rollout: None,
                    // Mark the request as completed so that it is not
                    // retried. The user must request a new rollout to try
                    // again.
                    last_completed_rollout_request: mz.requested_reconciliation_id(),
                    last_completed_rollout_hash: None,
                    conditions,
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::Promote => {
                let step = metadata.step("promote");
                let promoted = candidate.promote_services(client, &namespace).await;
                step.finish_with(&promoted);
                if let Some(action) = promoted? {
                    return Ok(Some(action));
                }
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::TearingDownPreviousGeneration),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::TearDownPreviousGeneration => {
                self.teardown_generation(metadata, client, mz, &candidate, active_generation)
                    .await?;
                let rollout = status
                    .rollout
                    .clone()
                    .expect("planned from a rollout in flight");
                // Recorded from the rollout and not from the spec, which may
                // have changed since the candidate was applied. Planning
                // again picks such a change up. Going straight on to the
                // balancer would leave it unnoticed until the next resync,
                // because the pass already recorded the changed inputs.
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::PlanningRollout),
                    rollout: None,
                    active_generation: rollout.generation,
                    last_completed_rollout_request: rollout.request,
                    last_completed_rollout_hash: None,
                    last_completed_rollout_environmentd_image_ref: Some(
                        rollout.environmentd_image_ref,
                    ),
                    resources_hash: rollout.resources_hash,
                    conditions: up_to_date_condition(
                        mz,
                        "True",
                        "Applied",
                        format!(
                            "Successfully applied changes for generation {}",
                            rollout.generation
                        ),
                    ),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::SyncBalancer => {
                // The balancer relies on the environmentd service existing,
                // which a completed rollout guarantees.
                if let Some(action) = self
                    .sync_balancer(metadata, client, mz, &status.resource_id)
                    .await?
                {
                    return Ok(Some(action));
                }
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::SyncingConsole),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
            Step::SyncConsole => {
                // The console relies on the balancer service existing, which
                // `SyncBalancer` waited for.
                self.sync_console(metadata, client, mz, &status.resource_id)
                    .await?;
                let next = MaterializeStatus {
                    phase: Some(MaterializePhase::Idle),
                    ..status
                };
                self.update_status(metadata, &mz_api, mz, next).await?;
                advanced
            }
        }
    }
}

fn pass_key(mz: &Materialize) -> String {
    format!("{}/{}", mz.namespace(), mz.name_unchecked())
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = Materialize;
    type Error = Error;

    const FINALIZER_NAME: Option<&'static str> =
        Some("orchestratord.materialize.cloud/materialize");

    #[instrument(fields(organization_name=mz.name_unchecked()))]
    async fn apply(
        &self,
        client: Client,
        mz: &Self::Resource,
        metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        let metadata = &*metadata;
        let mz_api: Api<Materialize> = Api::namespaced(client.clone(), &mz.namespace());
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), &mz.namespace());

        let Some(status) = mz.status.clone() else {
            let step = metadata.step("initialize_status");
            let status = MaterializeStatus {
                phase: Some(MaterializePhase::Idle),
                ..mz.status()
            };
            self.update_status(metadata, &mz_api, mz, status).await?;
            step.finish(Outcome::Completed);
            return Ok(Some(Action::requeue(STEP_REQUEUE)));
        };

        let step = metadata.step("resolve_environment_id");
        let backend_secret = secret_api.get(&mz.spec.backend_secret_name).await?;
        let license_key_environment_id: Option<Uuid> = if let Some(license_key) = backend_secret
            .data
            .as_ref()
            .and_then(|data| data.get("license_key"))
        {
            let license_key = validate(
                str::from_utf8(&license_key.0)
                    .context("invalid utf8")?
                    .trim(),
            )?;
            let environment_id = license_key
                .environment_id
                .parse()
                .context("invalid environment id in license key")?;
            Some(environment_id)
        } else {
            if mz.meets_minimum_version(&V161) {
                return Err(Error::Anyhow(anyhow::anyhow!(
                    "license_key is required when running in kubernetes",
                )));
            } else {
                None
            }
        };

        if mz.spec.request_rollout.is_nil() || mz.spec.environment_id.is_nil() {
            let mut mz = mz.clone();
            if mz.spec.request_rollout.is_nil() {
                mz.spec.request_rollout = Uuid::new_v4();
            }
            if mz.spec.environment_id.is_nil() {
                if let Some(environment_id) = license_key_environment_id {
                    if environment_id.is_nil() {
                        // this makes it easier to use a license key in
                        // development with no environment id set
                        mz.spec.environment_id = Uuid::new_v4();
                    } else {
                        mz.spec.environment_id = environment_id;
                    }
                } else {
                    if mz.meets_minimum_version(&V161) {
                        return Err(Error::Anyhow(anyhow::anyhow!(
                            "environmentId is not set in materialize resource {}/{} but no license key was given",
                            mz.namespace(),
                            mz.name_unchecked()
                        )));
                    } else {
                        mz.spec.environment_id = Uuid::new_v4();
                    }
                }
            }
            mz_api
                .replace(&mz.name_unchecked(), &PostParams::default(), &mz)
                .await?;
            step.finish(Outcome::Completed);
            return Ok(Some(Action::requeue(STEP_REQUEUE)));
        }

        if let Some(environment_id) = license_key_environment_id {
            // we still allow a nil environment id in the license key to be
            // accepted for any provided environment id, to support cloud
            if !environment_id.is_nil() && mz.spec.environment_id != environment_id {
                return Err(Error::Anyhow(anyhow::anyhow!(
                    "environment_id is set in materialize resource {}/{} but does not match the environment_id set in the associated license key {}",
                    mz.namespace(),
                    mz.name_unchecked(),
                    environment_id,
                )));
            }
        }

        for (field, probe) in [
            (
                "environmentdReadinessProbe",
                &mz.spec.environmentd_readiness_probe,
            ),
            (
                "environmentdLivenessProbe",
                &mz.spec.environmentd_liveness_probe,
            ),
            (
                "environmentdStartupProbe",
                &mz.spec.environmentd_startup_probe,
            ),
        ] {
            if let Some(probe) = probe {
                generation::validate_probe_handlers(probe).map_err(|e| {
                    Error::Anyhow(anyhow::anyhow!(
                        "invalid {field} in materialize resource {}/{}: {e}",
                        mz.namespace(),
                        mz.name_unchecked(),
                    ))
                })?;
            }
        }

        self.check_environment_id_conflicts(&client, mz).await?;
        step.finish(Outcome::Completed);

        let status = current_status(mz, status);
        let active = generation::Resources::new(&self.config, mz, status.active_generation);
        let candidate = generation::Resources::new(&self.config, mz, status.active_generation + 1);
        let inputs_hash = inputs_hash(mz);
        let (inputs_changed, resync_due) = match self.passes.lock().unwrap().get(&pass_key(mz)) {
            Some(pass) => (
                pass.inputs_hash != inputs_hash,
                pass.started.elapsed() >= RESYNC_INTERVAL,
            ),
            None => (true, true),
        };
        let observed = Observed {
            now: Timestamp::now(),
            inputs_changed,
            resync_due,
            active_hash: active.generate_hash(),
            candidate_hash: candidate.generate_hash(),
        };
        self.set_needs_update(mz, status.resources_hash != observed.active_hash);

        let step = plan_step(mz, &status, &observed);
        debug!(phase = ?status.phase, ?step, "reconciling");
        self.execute(metadata, &client, mz, status, observed, step)
            .await
    }

    #[instrument(fields(organization_name=mz.name_unchecked()))]
    async fn cleanup(
        &self,
        _client: Client,
        mz: &Self::Resource,
        _metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        self.set_needs_update(mz, false);
        self.passes.lock().unwrap().remove(&pass_key(mz));

        Ok(None)
    }
}

/// The type of the event reporting that the `UpToDate` condition became
/// `condition`: a warning if the environment is not up to date for any reason
/// other than waiting for a rollout to be approved, which is the operator
/// following its configuration.
fn transition_event_type(condition: &Condition) -> EventType {
    match condition.reason.as_str() {
        "WaitingForApproval" => EventType::Normal,
        _ if condition.status == "False" => EventType::Warning,
        _ => EventType::Normal,
    }
}

fn wait_for_balancer(balancer: &Balancer) -> Result<Option<Action>, Error> {
    if let Some(conditions) = balancer
        .status
        .as_ref()
        .map(|status| status.conditions.as_slice())
    {
        if conditions
            .iter()
            .any(|condition| condition.type_ == "Ready" && condition.status == "True")
        {
            return Ok(None);
        }
    }

    Ok(Some(Action::requeue(Duration::from_secs(1))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn condition(status: &str, reason: &str) -> Condition {
        Condition {
            type_: "UpToDate".into(),
            status: status.into(),
            reason: reason.into(),
            message: String::new(),
            last_transition_time: Time(Timestamp::now()),
            observed_generation: None,
        }
    }

    const ACTIVE_HASH: &str = "active";
    const CANDIDATE_HASH: &str = "candidate";

    /// A resource whose spec matches its active generation, in `phase`.
    fn mz_in(phase: MaterializePhase) -> Materialize {
        let request = Uuid::from_u128(1);
        let mut mz = Materialize::new("mz", Default::default());
        mz.spec.request_rollout = request;
        mz.status = Some(MaterializeStatus {
            active_generation: 1,
            last_completed_rollout_request: request,
            resources_hash: ACTIVE_HASH.into(),
            phase: Some(phase),
            ..Default::default()
        });
        mz
    }

    /// `mz_in(phase)`, with a spec change rolling out under a new request.
    fn mz_rolling_out(phase: MaterializePhase) -> Materialize {
        let mut mz = mz_in(phase);
        mz.spec.request_rollout = Uuid::from_u128(2);
        let status = mz.status.as_mut().unwrap();
        status.resources_hash = "previous".into();
        status.rollout = Some(RolloutStatus {
            generation: 2,
            request: mz.spec.request_rollout,
            environmentd_image_ref: String::new(),
            resources_hash: CANDIDATE_HASH.into(),
            started_at: Time(Timestamp::now()),
        });
        mz
    }

    fn observed() -> Observed {
        Observed {
            now: Timestamp::now(),
            inputs_changed: false,
            resync_due: false,
            active_hash: ACTIVE_HASH.into(),
            candidate_hash: CANDIDATE_HASH.into(),
        }
    }

    fn plan(mz: &Materialize, observed: &Observed) -> Step {
        plan_step(mz, mz.status.as_ref().unwrap(), observed)
    }

    #[mz_ore::test]
    fn idle_starts_a_pass_only_when_stale() {
        let mz = mz_in(MaterializePhase::Idle);
        assert_eq!(
            plan(&mz, &observed()),
            Step::ApplyGlobalResources {
                next_phase: MaterializePhase::Idle
            },
        );
        for stale in [
            Observed {
                inputs_changed: true,
                ..observed()
            },
            Observed {
                resync_due: true,
                ..observed()
            },
        ] {
            assert_eq!(
                plan(&mz, &stale),
                Step::ApplyGlobalResources {
                    next_phase: MaterializePhase::PlanningRollout
                },
            );
        }
    }

    #[mz_ore::test]
    fn changed_inputs_restart_the_pass_but_keep_a_rollout() {
        let changed = Observed {
            inputs_changed: true,
            ..observed()
        };
        assert_eq!(
            plan(&mz_in(MaterializePhase::SyncingConsole), &changed),
            Step::ApplyGlobalResources {
                next_phase: MaterializePhase::PlanningRollout
            },
        );
        assert_eq!(
            plan(&mz_rolling_out(MaterializePhase::Promoting), &changed),
            Step::ApplyGlobalResources {
                next_phase: MaterializePhase::Promoting
            },
        );
        // Only an idle resource resyncs.
        let resync = Observed {
            resync_due: true,
            ..observed()
        };
        assert_eq!(
            plan(&mz_in(MaterializePhase::SyncingConsole), &resync),
            Step::SyncConsole,
        );
    }

    #[mz_ore::test]
    fn idle_with_a_rollout_cancels_it() {
        let mz = mz_rolling_out(MaterializePhase::Idle);
        assert_eq!(
            plan(&mz, &observed()),
            Step::CancelRollout(Cancel::Reverted),
        );
    }

    #[mz_ore::test]
    fn refused_upgrade_still_times_out() {
        let mut mz = mz_rolling_out(MaterializePhase::AwaitingCandidate);
        mz.spec.environmentd_image_ref = "materialize/environmentd:v28.0.0".into();
        mz.status
            .as_mut()
            .unwrap()
            .last_completed_rollout_environmentd_image_ref =
            Some("materialize/environmentd:v26.0.0".into());
        assert!(matches!(
            plan(&mz, &observed()),
            Step::RefuseUpgrade {
                timeout_in: Some(_)
            },
        ));

        let mut mz = mz_in(MaterializePhase::PlanningRollout);
        mz.spec.request_rollout = Uuid::from_u128(2);
        mz.spec.environmentd_image_ref = "materialize/environmentd:v28.0.0".into();
        let status = mz.status.as_mut().unwrap();
        status.resources_hash = "previous".into();
        status.last_completed_rollout_environmentd_image_ref =
            Some("materialize/environmentd:v26.0.0".into());
        assert_eq!(
            plan(&mz, &observed()),
            Step::RefuseUpgrade { timeout_in: None },
        );
    }

    #[mz_ore::test]
    fn planning_a_rollout() {
        let mut mz = mz_in(MaterializePhase::PlanningRollout);
        assert_eq!(plan(&mz, &observed()), Step::Settle { up_to_date: true });

        // Changes without a new request wait for approval.
        mz.status.as_mut().unwrap().resources_hash = "previous".into();
        assert_eq!(plan(&mz, &observed()), Step::Settle { up_to_date: false });

        mz.spec.request_rollout = Uuid::from_u128(2);
        assert_eq!(plan(&mz, &observed()), Step::BeginRollout);

        // A request with nothing to roll out completes as a no-op.
        mz.status.as_mut().unwrap().resources_hash = ACTIVE_HASH.into();
        assert_eq!(plan(&mz, &observed()), Step::Settle { up_to_date: true });
    }

    #[mz_ore::test]
    fn rollout_phases_advance() {
        for (phase, step) in [
            (
                MaterializePhase::TearingDownActiveGeneration,
                Step::ApplyCandidate,
            ),
            (MaterializePhase::ApplyingCandidate, Step::ApplyCandidate),
            (MaterializePhase::AwaitingCandidate, Step::AwaitCandidate),
            (MaterializePhase::Promoting, Step::Promote),
            (
                MaterializePhase::TearingDownPreviousGeneration,
                Step::TearDownPreviousGeneration,
            ),
        ] {
            assert_eq!(plan(&mz_rolling_out(phase), &observed()), step, "{phase:?}");
        }
    }

    #[mz_ore::test]
    fn active_generation_is_torn_down_only_under_its_strategy() {
        let mut mz = mz_rolling_out(MaterializePhase::TearingDownActiveGeneration);
        mz.spec.rollout_strategy = MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime;
        assert_eq!(plan(&mz, &observed()), Step::TearDownActiveGeneration);

        mz.spec.rollout_strategy = MaterializeRolloutStrategy::WaitUntilReady;
        assert_eq!(plan(&mz, &observed()), Step::ApplyCandidate);
    }

    #[mz_ore::test]
    fn ready_to_promote_waits_for_force_promote() {
        let mut mz = mz_rolling_out(MaterializePhase::ReadyToPromote);
        mz.spec.rollout_strategy = MaterializeRolloutStrategy::ManuallyPromote;
        assert!(matches!(
            plan(&mz, &observed()),
            Step::AwaitPromotion {
                timeout_in: Some(_)
            },
        ));

        // Setting `forcePromote` does not change the candidate's resources.
        mz.set_force_promote();
        assert_eq!(plan(&mz, &observed()), Step::AwaitCandidate);

        // Neither does leaving the `ManuallyPromote` strategy.
        let mut mz = mz_rolling_out(MaterializePhase::ReadyToPromote);
        mz.spec.rollout_strategy = MaterializeRolloutStrategy::WaitUntilReady;
        assert_eq!(plan(&mz, &observed()), Step::AwaitCandidate);
    }

    #[mz_ore::test]
    fn retried_conditions_replace_only_a_failed_deploy() {
        let mut mz = mz_rolling_out(MaterializePhase::AwaitingCandidate);
        for (reason, expected) in [
            ("FailedDeploy", "Applying"),
            ("Applying", "Applying"),
            ("ReadyToPromote", "ReadyToPromote"),
        ] {
            mz.status.as_mut().unwrap().conditions = vec![condition("Unknown", reason)];
            let conditions = retried_conditions(&mz, mz.status.as_ref().unwrap(), 2);
            assert_eq!(conditions[0].reason, expected, "{reason}");
        }
    }

    #[mz_ore::test]
    fn reverting_the_spec_cancels_an_unpromoted_rollout() {
        for phase in [
            MaterializePhase::TearingDownActiveGeneration,
            MaterializePhase::ApplyingCandidate,
            MaterializePhase::AwaitingCandidate,
            MaterializePhase::ReadyToPromote,
        ] {
            // The request is reverted.
            let mut mz = mz_rolling_out(phase);
            mz.spec.request_rollout = Uuid::from_u128(1);
            assert_eq!(
                plan(&mz, &observed()),
                Step::CancelRollout(Cancel::Reverted),
                "{phase:?}",
            );

            // The spec again matches the active generation.
            let mut mz = mz_rolling_out(phase);
            mz.status.as_mut().unwrap().resources_hash = ACTIVE_HASH.into();
            assert_eq!(
                plan(&mz, &observed()),
                Step::CancelRollout(Cancel::Reverted),
                "{phase:?}",
            );
        }
    }

    #[mz_ore::test]
    fn spec_change_restarts_an_unpromoted_rollout() {
        let changed = Observed {
            candidate_hash: "changed".into(),
            ..observed()
        };
        for phase in [
            MaterializePhase::ApplyingCandidate,
            MaterializePhase::AwaitingCandidate,
            MaterializePhase::ReadyToPromote,
        ] {
            assert_eq!(
                plan(&mz_rolling_out(phase), &changed),
                Step::BeginRollout,
                "{phase:?}",
            );
            let mut mz = mz_rolling_out(phase);
            mz.spec.request_rollout = Uuid::from_u128(3);
            assert_eq!(plan(&mz, &observed()), Step::BeginRollout, "{phase:?}");
        }
    }

    #[mz_ore::test]
    fn promotion_completes_whatever_the_spec_says() {
        let changed = Observed {
            candidate_hash: "changed".into(),
            ..observed()
        };
        for (phase, step) in [
            (MaterializePhase::Promoting, Step::Promote),
            (
                MaterializePhase::TearingDownPreviousGeneration,
                Step::TearDownPreviousGeneration,
            ),
        ] {
            let mut mz = mz_rolling_out(phase);
            // Reverted, changed, and long past the timeout.
            mz.spec.request_rollout = Uuid::from_u128(1);
            mz.status
                .as_mut()
                .unwrap()
                .rollout
                .as_mut()
                .unwrap()
                .started_at = Time(Timestamp::UNIX_EPOCH);
            assert_eq!(plan(&mz, &changed), step, "{phase:?}");
        }
    }

    #[mz_ore::test]
    fn rollout_times_out_unless_force_promoting() {
        let mut mz = mz_rolling_out(MaterializePhase::AwaitingCandidate);
        mz.status
            .as_mut()
            .unwrap()
            .rollout
            .as_mut()
            .unwrap()
            .started_at = Time(Timestamp::UNIX_EPOCH);
        assert_eq!(
            plan(&mz, &observed()),
            Step::CancelRollout(Cancel::TimedOut),
        );

        mz.set_force_promote();
        assert_eq!(plan(&mz, &observed()), Step::AwaitCandidate);
    }

    #[mz_ore::test]
    fn current_status_derives_phase_from_condition() {
        for (reason, phase, rollout_hash) in [
            ("Applied", MaterializePhase::Idle, None),
            ("WaitingForApproval", MaterializePhase::Idle, None),
            ("Applying", MaterializePhase::ApplyingCandidate, Some("")),
            (
                "ReadyToPromote",
                MaterializePhase::ReadyToPromote,
                Some("stored"),
            ),
            ("Promoting", MaterializePhase::Promoting, Some("stored")),
        ] {
            let mz = mz_in(MaterializePhase::Idle);
            let status = MaterializeStatus {
                phase: None,
                resources_hash: "stored".into(),
                conditions: vec![condition("Unknown", reason)],
                ..mz.status.clone().unwrap()
            };
            let status = current_status(&mz, status);
            assert_eq!(status.phase, Some(phase), "{reason}");
            let rollout = status.rollout.as_ref();
            assert_eq!(
                rollout.map(|rollout| rollout.resources_hash.as_str()),
                rollout_hash,
                "{reason}",
            );
            assert_eq!(
                rollout.map(|rollout| rollout.generation),
                rollout_hash.map(|_| 2),
                "{reason}",
            );
        }
    }

    #[mz_ore::test]
    fn test_transition_event_type() {
        for (status, reason, expected) in [
            ("True", "Applied", EventType::Normal),
            ("Unknown", "Applying", EventType::Normal),
            ("Unknown", "ReadyToPromote", EventType::Normal),
            ("Unknown", "Promoting", EventType::Normal),
            ("False", "WaitingForApproval", EventType::Normal),
            ("False", "FailedDeploy", EventType::Warning),
            ("False", "RolloutTimeout", EventType::Warning),
            ("False", "SomeFutureFailure", EventType::Warning),
            ("Unknown", "SomeFuturePhase", EventType::Normal),
        ] {
            assert_eq!(
                transition_event_type(&condition(status, reason)),
                expected,
                "status={status} reason={reason}",
            );
        }
    }
}
