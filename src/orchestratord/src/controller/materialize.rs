// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Context as _;
use http::HeaderValue;
use k8s_controller::TraceMetadata;
use k8s_openapi::{
    api::core::v1::{Affinity, ResourceRequirements, Secret, Toleration},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
    jiff::{SignedDuration, Timestamp},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ListParams, PostParams},
    runtime::controller::Action,
    runtime::events::{Event, EventType},
};
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::{
    Error,
    controller::materialize::generation::V161,
    k8s::{apply_resource, delete_resource},
    matching_image_from_environmentd_image_ref,
    metrics::Metrics,
    parse_image_tag,
    reconcile::{self, Outcome},
    tls::{DefaultCertificateSpecs, issuer_ref_defined, resolved_dns_names},
};
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::{
    ManagedResource,
    balancer::v1alpha1::{Balancer, BalancerSpec},
    console::v1alpha1::{BalancerdRef, Console, ConsoleSpec, HttpConnectionScheme},
    materialize::MaterializeRolloutStrategy,
    materialize::v1alpha1::{Materialize, MaterializeStatus},
};
use mz_license_keys::validate;
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::{cast::CastFrom, cli::KeyValueArg, instrument};

pub mod generation;
pub mod global;

/// The `controller` label that this controller's metrics and Kubernetes events
/// carry. Shared by `Observed` and by every step this controller records, which
/// must agree for a pass and its steps to line up.
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
    pub enable_internal_statement_logging: bool,
    pub statement_logging_max_sample_rate: Option<f64>,
    pub statement_logging_target_data_rate: Option<usize>,

    pub orchestratord_pod_selector_labels: Vec<KeyValueArg<String, String>>,
    pub environmentd_node_selector: Vec<KeyValueArg<String, String>>,
    pub environmentd_affinity: Option<Affinity>,
    pub environmentd_tolerations: Option<Vec<Toleration>>,
    pub environmentd_default_resources: Option<ResourceRequirements>,
    pub clusterd_node_selector: Vec<KeyValueArg<String, String>>,
    pub clusterd_affinity: Option<Affinity>,
    pub clusterd_tolerations: Option<Vec<Toleration>>,
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

    pub tracing: TracingCliArgs,
    pub orchestratord_namespace: String,
}

pub struct Context {
    config: Config,
    metrics: Arc<Metrics>,
    publisher: Arc<reconcile::Publisher>,
    needs_update: Arc<Mutex<BTreeSet<String>>>,
}

impl Context {
    pub fn new(
        config: Config,
        metrics: Arc<Metrics>,
        publisher: Arc<reconcile::Publisher>,
    ) -> Self {
        if config.cloud_provider == CloudProvider::Aws {
            assert!(
                config.aws_account_id.is_some(),
                "--aws-account-id is required when using --cloud-provider=aws"
            );
        }

        Self {
            config,
            metrics,
            publisher,
            needs_update: Default::default(),
        }
    }

    /// Starts recording the step named `step` of this controller's
    /// reconciliation.
    fn step(&self, step: &'static str) -> reconcile::Step<'_> {
        self.metrics.reconcile.step(CONTROLLER_NAME, step)
    }

    /// Deletes `generation`'s resources, releasing the read holds its
    /// environmentd was keeping.
    async fn teardown_generation(
        &self,
        client: &Client,
        mz: &Materialize,
        resources: &generation::Resources,
        generation: u64,
    ) -> Result<(), Error> {
        let step = self.step("teardown_generation");
        resources
            .teardown_generation(client, mz, generation)
            .await?;
        step.finish(Outcome::Applied);
        Ok(())
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

    /// Reports a lifecycle transition as a Kubernetes event on the resource.
    ///
    /// The condition's own `reason` and `message` become the event's, so the
    /// vocabulary an operator sees in `kubectl describe` is the one the status
    /// already reports: `Applying`, `ReadyToPromote`, `Promoting`, `Applied`,
    /// `WaitingForApproval`, `RolloutTimeout`, `FailedDeploy`. Keep those
    /// stable, since they are what a dashboard groups on.
    ///
    /// A `FailedDeploy` reports here and again from the generic reconciliation
    /// wrapper, which files the error itself. The two carry different halves of
    /// the picture, the phase and the cause, and the reasons tell them apart.
    ///
    /// These do not aggregate the way repeated failures do, so each transition
    /// is its own event carrying its own message. Two things make that so: a
    /// transition only reports when the status actually moved, and a pass that
    /// ends cleanly forgets what the resource published. A transition reported
    /// from a pass that then fails is the exception, and there aggregating is
    /// right, since the retry is reporting the same phase again.
    async fn publish_transition(&self, mz: &Materialize, condition: &Condition) {
        self.publisher
            .publish(
                mz,
                Event {
                    type_: transition_event_type(condition),
                    reason: condition.reason.clone(),
                    action: "Reconcile".into(),
                    note: Some(condition.message.clone()),
                    secondary: None,
                },
            )
            .await;
    }

    async fn update_status(
        &self,
        mz_api: &Api<Materialize>,
        mz: &Materialize,
        status: MaterializeStatus,
        needs_update: bool,
    ) -> Result<Materialize, kube::Error> {
        self.set_needs_update(mz, needs_update);

        let mut new_mz = mz.clone();
        if !mz
            .status
            .as_ref()
            .map_or(true, |mz_status| mz_status.needs_update(&status))
        {
            return Ok(new_mz);
        }

        // Reaching here is the definition of a transition: the guard above
        // returns early unless the status moved, comparing everything but the
        // timestamp. That is what keeps this to one event per transition in a
        // reconciler that re-runs on every watch event.
        //
        // The exception is the pass that gives a new resource its first status,
        // which carries no conditions yet and so reports no phase.
        let condition = status.conditions.first().cloned();
        new_mz.status = Some(status);
        let new_mz = mz_api
            .replace_status(&mz.name_unchecked(), &PostParams::default(), &new_mz)
            .await?;

        // Only once the status is durable, so that no event claims a transition
        // that failed to persist.
        if let Some(condition) = condition {
            self.publish_transition(mz, &condition).await;
        }

        Ok(new_mz)
    }

    async fn promote(
        &self,
        client: &Client,
        mz: &Materialize,
        resources: generation::Resources,
        active_generation: u64,
        desired_generation: u64,
        resources_hash: String,
    ) -> Result<Option<Action>, Error> {
        let step = self.step("promote");
        if let Some(action) = resources.promote_services(client, &mz.namespace()).await? {
            step.finish(Outcome::Waiting);
            return Ok(Some(action));
        }
        step.finish(Outcome::Applied);

        self.teardown_generation(client, mz, &resources, active_generation)
            .await?;
        let mz_api: Api<Materialize> = Api::namespaced(client.clone(), &mz.namespace());
        self.update_status(
            &mz_api,
            mz,
            MaterializeStatus {
                active_generation: desired_generation,
                last_completed_rollout_request: mz.requested_reconciliation_id(),
                last_completed_rollout_environmentd_image_ref: Some(
                    mz.spec.environmentd_image_ref.clone(),
                ),
                resource_id: mz.status().resource_id,
                resources_hash,
                last_completed_rollout_hash: None,
                conditions: vec![Condition {
                    type_: "UpToDate".into(),
                    status: "True".into(),
                    last_transition_time: Time(Timestamp::now()),
                    message: format!(
                        "Successfully applied changes for generation {desired_generation}"
                    ),
                    observed_generation: mz.meta().generation,
                    reason: "Applied".into(),
                }],
            },
            false,
        )
        .await?;
        Ok(None)
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
        _metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        let mz_api: Api<Materialize> = Api::namespaced(client.clone(), &mz.namespace());
        let balancer_api: Api<Balancer> = Api::namespaced(client.clone(), &mz.namespace());
        let console_api: Api<Console> = Api::namespaced(client.clone(), &mz.namespace());
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), &mz.namespace());

        let status = mz.status();
        if mz.status.is_none() {
            let step = self.step("initialize_status");
            self.update_status(&mz_api, mz, status, true).await?;
            step.finish(Outcome::Applied);
            // Updating the status should trigger a reconciliation
            // which will include a status this time.
            return Ok(None);
        }

        // Everything from reading the license key through the uniqueness check
        // is one step: it is all about settling on the environment id, and it
        // is where a misconfigured license key stops a new environment before
        // any of its resources exist.
        let step = self.step("resolve_environment_id");
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
            step.finish(Outcome::Applied);
            // Updating the spec should also trigger a reconciliation.
            // We can't do that as part of the above check because you can't
            // update both the spec and the status in a single api call.
            return Ok(None);
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

        self.check_environment_id_conflicts(&client, mz).await?;
        step.finish(Outcome::Applied);

        let step = self.step("global_resources");
        global::Resources::new(&self.config, mz)?
            .apply(&client, &mz.namespace())
            .await?;
        step.finish(Outcome::Applied);

        // we compare the hash against the environment resources generated
        // for the current active generation, since that's what we expect to
        // have been applied earlier, but we don't want to use these
        // environment resources because when we apply them, we want to apply
        // them with data that uses the new generation
        let active_resources =
            generation::Resources::new(&self.config, mz, status.active_generation);
        let has_current_changes = status.resources_hash != active_resources.generate_hash();
        let active_generation = status.active_generation;
        let next_generation = active_generation + 1;
        let desired_generation = if has_current_changes {
            next_generation
        } else {
            active_generation
        };

        // here we regenerate the environment resources using the
        // same inputs except with an updated generation
        let resources = generation::Resources::new(&self.config, mz, desired_generation);
        let resources_hash = resources.generate_hash();

        let mut result = match (
            mz.is_promoting(),
            has_current_changes,
            mz.rollout_requested(),
        ) {
            // If we're in status promoting, we MUST promote now.
            // We don't know if we successfully promoted or not yet.
            (true, _, _) => {
                self.promote(
                    &client,
                    mz,
                    resources,
                    active_generation,
                    desired_generation,
                    resources_hash,
                )
                .await
            }
            // There are changes pending, and we want to apply them.
            (false, true, true) => {
                // If a rollout has been in progress for longer than the
                // configured timeout, cancel it. While a rollout is in
                // progress the new generation runs un-promoted and holds back
                // compaction via read holds; promoting it after a long delay
                // can cause incident-inducing load, so we abort instead and
                // let the user retry by requesting a fresh rollout.
                //
                // We never cancel a force-promoting rollout (including the
                // `ImmediatelyPromoteCausingDowntime` strategy), because by
                // then the previously-active generation may already be torn
                // down, leaving nothing to fall back to.
                if !mz.should_force_promote() {
                    if let Some(started) = mz.rollout_in_progress_since() {
                        let timeout = mz.rollout_request_timeout();
                        let elapsed = Timestamp::now().duration_since(started);
                        let timed_out = SignedDuration::try_from(timeout)
                            .is_ok_and(|timeout| elapsed >= timeout);
                        if timed_out {
                            warn!(
                                "rollout to generation {desired_generation} exceeded timeout, cancelling"
                            );
                            // Tear down the un-promoted generation to release
                            // its read holds.
                            self.teardown_generation(&client, mz, &resources, next_generation)
                                .await?;
                            self.update_status(
                                &mz_api,
                                mz,
                                MaterializeStatus {
                                    active_generation,
                                    // Mark this rollout request as completed so
                                    // that we don't immediately retry it; the
                                    // user must request a new rollout to try
                                    // again.
                                    last_completed_rollout_request: mz
                                        .requested_reconciliation_id(),
                                    last_completed_rollout_environmentd_image_ref: status
                                        .last_completed_rollout_environmentd_image_ref
                                        .clone(),
                                    resource_id: status.resource_id.clone(),
                                    resources_hash: status.resources_hash.clone(),
                                    last_completed_rollout_hash: None,
                                    conditions: vec![Condition {
                                        type_: "UpToDate".into(),
                                        status: "False".into(),
                                        last_transition_time: Time(Timestamp::now()),
                                        message: format!(
                                            "Cancelled rollout to generation \
                                             {desired_generation} after it \
                                             exceeded the rollout timeout of {}",
                                            humantime::format_duration(timeout),
                                        ),
                                        observed_generation: mz.meta().generation,
                                        reason: "RolloutTimeout".into(),
                                    }],
                                },
                                active_generation != desired_generation,
                            )
                            .await?;
                            return Ok(None);
                        }
                    }
                }

                if !mz.within_upgrade_window() {
                    let last_completed_rollout_environmentd_image_ref =
                        status.last_completed_rollout_environmentd_image_ref;

                    self.update_status(
                        &mz_api,
                        mz,
                        MaterializeStatus {
                            active_generation,
                            last_completed_rollout_request: status.last_completed_rollout_request,
                            last_completed_rollout_environmentd_image_ref:
                                last_completed_rollout_environmentd_image_ref.clone(),
                            resource_id: status.resource_id,
                            resources_hash: status.resources_hash,
                            last_completed_rollout_hash: None,
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "False".into(),
                                last_transition_time: Time(Timestamp::now()),
                                message: format!(
                                    "Refusing to upgrade from {} to {}. \
                                     More than one major version from \
                                     last successful rollout. If coming \
                                     from Self Managed 25.2, upgrade to \
                                     materialize/environmentd:v0.147.20 \
                                     first.",
                                    last_completed_rollout_environmentd_image_ref
                                        .expect("should be set if upgrade window check fails"),
                                    mz.spec.environmentd_image_ref,
                                ),
                                observed_generation: mz.meta().generation,
                                reason: "FailedDeploy".into(),
                            }],
                        },
                        active_generation != desired_generation,
                    )
                    .await?;
                    return Ok(None);
                }

                // we remove the environment resources hash annotation here
                // because if we fail halfway through applying the resources,
                // things will be in an inconsistent state, and we don't want
                // to allow the possibility of the user making a second
                // change which reverts to the original state and then
                // skipping retrying this apply, since that would leave
                // things in a permanently inconsistent state.
                // note that environment.spec will be empty here after
                // replace_status, but this is fine because we already
                // extracted all of the information we want from the spec
                // earlier.
                let mz = if mz.is_ready_to_promote(&resources_hash) {
                    mz
                } else {
                    &self
                        .update_status(
                            &mz_api,
                            mz,
                            MaterializeStatus {
                                active_generation,
                                // don't update the reconciliation id yet,
                                // because the rollout hasn't yet completed. if
                                // we fail later on, we want to ensure that the
                                // rollout gets retried.
                                last_completed_rollout_request: status
                                    .last_completed_rollout_request,
                                last_completed_rollout_environmentd_image_ref: status
                                    .last_completed_rollout_environmentd_image_ref,
                                resource_id: status.resource_id.clone(),
                                resources_hash: String::new(),
                                last_completed_rollout_hash: None,
                                conditions: vec![Condition {
                                    type_: "UpToDate".into(),
                                    status: "Unknown".into(),
                                    last_transition_time: Time(Timestamp::now()),
                                    message: format!(
                                        "Applying changes for generation {desired_generation}"
                                    ),
                                    observed_generation: mz.meta().generation,
                                    reason: "Applying".into(),
                                }],
                            },
                            active_generation != desired_generation,
                        )
                        .await?
                };
                let status = mz.status();

                if mz.spec.rollout_strategy
                    == MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime
                {
                    // The only reason someone would choose this strategy is if they didn't have
                    // space for the two generations of pods.
                    // Lets make room for the new ones by deleting the old generation.
                    self.teardown_generation(&client, mz, &resources, active_generation)
                        .await?;
                }

                trace!("applying environment resources");
                let step = self.step("generation_resources");
                let applied = resources
                    .apply(&client, mz.should_force_promote(), &mz.namespace())
                    .await;
                step.finish_with(&applied);
                match applied {
                    Ok(Some(action)) => {
                        trace!("new environment is not yet ready");
                        Ok(Some(action))
                    }
                    Ok(None) => {
                        if mz.spec.rollout_strategy == MaterializeRolloutStrategy::ManuallyPromote
                            && !mz.should_force_promote()
                        {
                            trace!(
                                "Ready to promote, but not promoting because the instance is configured with ManuallyPromote rollout strategy."
                            );
                            self.update_status(
                                &mz_api,
                                mz,
                                MaterializeStatus {
                                    active_generation,
                                    last_completed_rollout_request: status
                                        .last_completed_rollout_request,
                                    last_completed_rollout_environmentd_image_ref: status
                                        .last_completed_rollout_environmentd_image_ref,
                                    resource_id: status.resource_id,
                                    resources_hash,
                                    last_completed_rollout_hash: None,
                                    conditions: vec![Condition {
                                        type_: "UpToDate".into(),
                                        status: "Unknown".into(),
                                        // Carry the `Applying` phase's
                                        // timestamp forward (both phases are
                                        // `Unknown`) so the rollout timeout
                                        // spans Applying + ReadyToPromote
                                        // rather than resetting here.
                                        last_transition_time: Time(mz.up_to_date_transition_time(
                                            "Unknown",
                                            Timestamp::now(),
                                        )),
                                        message: format!(
                                            "Ready to promote generation {desired_generation}"
                                        ),
                                        observed_generation: mz.meta().generation,
                                        reason: "ReadyToPromote".into(),
                                    }],
                                },
                                active_generation != desired_generation,
                            )
                            .await?;
                            return Ok(None);
                        }
                        // do this last, so that we keep traffic pointing at
                        // the previous environmentd until the new one is
                        // fully ready

                        // Update the status before calling promote, so that we know
                        // we've crossed the point of no return.
                        // Once we see this status, we must promote without taking other actions.
                        self.update_status(
                            &mz_api,
                            mz,
                            MaterializeStatus {
                                active_generation,
                                // don't update the reconciliation id yet,
                                // because the rollout hasn't yet completed. if
                                // we fail later on, we want to ensure that the
                                // rollout gets retried.
                                last_completed_rollout_request: status
                                    .last_completed_rollout_request,
                                last_completed_rollout_environmentd_image_ref: status
                                    .last_completed_rollout_environmentd_image_ref,
                                resource_id: status.resource_id,
                                resources_hash: resources_hash.clone(),
                                last_completed_rollout_hash: None,
                                conditions: vec![Condition {
                                    type_: "UpToDate".into(),
                                    status: "Unknown".into(),
                                    last_transition_time: Time(Timestamp::now()),
                                    message: format!(
                                        "Attempting to promote generation {desired_generation}"
                                    ),
                                    observed_generation: mz.meta().generation,
                                    reason: "Promoting".into(),
                                }],
                            },
                            active_generation != desired_generation,
                        )
                        .await?;
                        self.promote(
                            &client,
                            mz,
                            resources,
                            active_generation,
                            desired_generation,
                            resources_hash,
                        )
                        .await
                    }
                    Err(e) => {
                        self.update_status(
                            &mz_api,
                            mz,
                            MaterializeStatus {
                                active_generation,
                                // also don't update the reconciliation id
                                // here, because there was an error during
                                // the rollout and we want to ensure it gets
                                // retried.
                                last_completed_rollout_request: status
                                    .last_completed_rollout_request,
                                last_completed_rollout_environmentd_image_ref: status
                                    .last_completed_rollout_environmentd_image_ref,
                                resource_id: status.resource_id,
                                resources_hash: status.resources_hash,
                                last_completed_rollout_hash: None,
                                conditions: vec![Condition {
                                    type_: "UpToDate".into(),
                                    status: "False".into(),
                                    last_transition_time: Time(Timestamp::now()),
                                    message: format!(
                                        "Failed to apply changes for \
                                         generation {desired_generation}: {e}"
                                    ),
                                    observed_generation: mz.meta().generation,
                                    reason: "FailedDeploy".into(),
                                }],
                            },
                            active_generation != desired_generation,
                        )
                        .await?;
                        Err(e)
                    }
                }
            }
            // There are changes pending, but we don't want to apply them yet.
            (false, true, false) => {
                let mut needs_update = mz.conditions_need_update();
                if mz.update_in_progress() {
                    self.teardown_generation(&client, mz, &resources, next_generation)
                        .await?;
                    needs_update = true;
                }
                if needs_update {
                    self.update_status(
                        &mz_api,
                        mz,
                        MaterializeStatus {
                            active_generation,
                            last_completed_rollout_request: mz.requested_reconciliation_id(),
                            last_completed_rollout_environmentd_image_ref: status
                                .last_completed_rollout_environmentd_image_ref,
                            resource_id: status.resource_id.clone(),
                            resources_hash: status.resources_hash,
                            last_completed_rollout_hash: None,
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "False".into(),
                                last_transition_time: Time(Timestamp::now()),
                                message: format!(
                                    "Changes detected, waiting for approval for generation {desired_generation}"
                                ),
                                observed_generation: mz.meta().generation,
                                reason: "WaitingForApproval".into(),
                            }],
                        },
                        active_generation != desired_generation,
                    )
                    .await?;
                }
                debug!("changes detected, waiting for approval");
                Ok(None)
            }
            // No changes pending, but we might need to clean up a partially applied rollout.
            (false, false, _) => {
                // this can happen if we update the environment, but then revert
                // that update before the update was deployed. in this case, we
                // don't want the environment to still show up as
                // WaitingForApproval.
                let mut needs_update = mz.conditions_need_update() || mz.rollout_requested();
                if mz.update_in_progress() {
                    self.teardown_generation(&client, mz, &resources, next_generation)
                        .await?;
                    needs_update = true;
                }
                if needs_update {
                    self.update_status(
                        &mz_api,
                        mz,
                        MaterializeStatus {
                            active_generation,
                            last_completed_rollout_request: mz.requested_reconciliation_id(),
                            last_completed_rollout_environmentd_image_ref: status
                                .last_completed_rollout_environmentd_image_ref,
                            resource_id: status.resource_id.clone(),
                            resources_hash: status.resources_hash,
                            last_completed_rollout_hash: None,
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "True".into(),
                                last_transition_time: Time(Timestamp::now()),
                                message: format!(
                                    "No changes found from generation {active_generation}"
                                ),
                                observed_generation: mz.meta().generation,
                                reason: "Applied".into(),
                            }],
                        },
                        active_generation != desired_generation,
                    )
                    .await?;
                }
                debug!("no changes");
                Ok(None)
            }
        }?;

        if let Some(action) = result {
            return Ok(Some(action));
        }

        // balancers rely on the environmentd service existing, which is
        // enforced by the environmentd rollout process being able to call
        // into the promotion endpoint

        let step = self.step("balancer");
        if self.config.create_balancers {
            let balancer = Balancer {
                metadata: mz.managed_resource_meta(mz.name_unchecked()),
                spec: BalancerSpec {
                    balancerd_image_ref: matching_image_from_environmentd_image_ref(
                        mz.active_environmentd_image_ref(),
                        "balancerd",
                        None,
                    ),
                    resource_requirements: mz.spec.balancerd_resource_requirements.clone(),
                    replicas: Some(mz.balancerd_replicas()),
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
                    resource_id: Some(status.resource_id.clone()),
                },
                status: None,
            };
            let balancer = apply_resource(&balancer_api, &balancer).await?;
            result = wait_for_balancer(&balancer)?;
            step.finish(match result {
                // The balancer is not ready yet, so we will be back.
                Some(_) => Outcome::Waiting,
                None => Outcome::Applied,
            });
        } else {
            delete_resource(&balancer_api, &mz.name_unchecked()).await?;
            step.finish(Outcome::Skipped);
        }

        if let Some(action) = result {
            return Ok(Some(action));
        }

        // and the console relies on the balancer service existing, which is
        // enforced by wait_for_balancer

        let step = self.step("console");
        if self.config.create_console {
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
                    resource_id: Some(status.resource_id),
                },
                status: None,
            };
            apply_resource(&console_api, &console).await?;
            step.finish(Outcome::Applied);
        } else {
            delete_resource(&console_api, &mz.name_unchecked()).await?;
            step.finish(Outcome::Skipped);
        }

        Ok(result)
    }

    #[instrument(fields(organization_name=mz.name_unchecked()))]
    async fn cleanup(
        &self,
        _client: Client,
        mz: &Self::Resource,
        _metadata: &mut TraceMetadata,
    ) -> Result<Option<Action>, Self::Error> {
        self.set_needs_update(mz, false);

        Ok(None)
    }
}

/// The severity to report a lifecycle transition at.
///
/// `UpToDate=False` says the environment is not up to date, which is worth
/// flagging for every reason that reports it but one. Waiting for approval is
/// the operator doing exactly what it was configured to do, and a rollout
/// nobody has requested yet is not a problem; reporting it as a warning would
/// teach people to ignore warnings on Materialize resources. `Unknown` is a
/// rollout under way, which is also not a problem.
///
/// A reason this does not recognize falls back to the condition's status, so a
/// failure reason added later reports as a warning without having to be named
/// here.
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

    #[mz_ore::test]
    fn test_transition_event_type() {
        // Every condition this controller writes, and how loudly it should be
        // reported. Keep this in step with the `Condition`s built above.
        for (status, reason, expected) in [
            ("True", "Applied", EventType::Normal),
            ("Unknown", "Applying", EventType::Normal),
            ("Unknown", "ReadyToPromote", EventType::Normal),
            ("Unknown", "Promoting", EventType::Normal),
            // Not up to date, but by configuration rather than by failure.
            ("False", "WaitingForApproval", EventType::Normal),
            ("False", "FailedDeploy", EventType::Warning),
            ("False", "RolloutTimeout", EventType::Warning),
            // An unrecognized reason is judged by its status.
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
