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
    fmt::Display,
    future::ready,
    str::FromStr,
    sync::{Arc, Mutex},
};

use anyhow::Context as _;
use futures::StreamExt;
use http::HeaderValue;
use k8s_openapi::{
    api::core::v1::{Affinity, ResourceRequirements, Secret, Toleration},
    apimachinery::pkg::apis::meta::v1::{Condition, Time},
};
use kube::{
    Api, Client, Resource, ResourceExt,
    api::PostParams,
    runtime::{controller::Action, reflector, watcher},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tracing::{debug, trace, warn};
use uuid::Uuid;

use crate::{controller::materialize::environmentd::V161, metrics::Metrics};
use mz_cloud_provider::CloudProvider;
use mz_cloud_resources::crd::materialize::v1alpha1::{
    Materialize, MaterializeCertSpec, MaterializeRolloutStrategy, MaterializeStatus,
};
use mz_license_keys::validate;
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::{cast::CastFrom, cli::KeyValueArg, instrument};

pub mod balancer;
pub mod console;
pub mod environmentd;
pub mod tls;

#[derive(clap::Parser)]
pub struct MaterializeControllerArgs {
    #[clap(long)]
    cloud_provider: CloudProvider,
    #[clap(long)]
    region: String,
    #[clap(long)]
    create_balancers: bool,
    #[clap(long)]
    create_console: bool,
    #[clap(long)]
    helm_chart_version: Option<String>,
    #[clap(long, default_value = "kubernetes")]
    secrets_controller: String,
    #[clap(long)]
    collect_pod_metrics: bool,
    #[clap(long)]
    enable_prometheus_scrape_annotations: bool,
    #[clap(long)]
    disable_authentication: bool,

    #[clap(long)]
    segment_api_key: Option<String>,
    #[clap(long)]
    segment_client_side: bool,

    #[clap(long)]
    console_image_tag_default: String,
    #[clap(long)]
    console_image_tag_map: Vec<KeyValueArg<String, String>>,

    #[clap(flatten)]
    aws_info: AwsInfo,

    #[clap(long)]
    ephemeral_volume_class: Option<String>,
    #[clap(long)]
    scheduler_name: Option<String>,
    #[clap(long)]
    enable_security_context: bool,
    #[clap(long)]
    enable_internal_statement_logging: bool,
    #[clap(long, default_value = "false")]
    disable_statement_logging: bool,

    #[clap(long)]
    orchestratord_pod_selector_labels: Vec<KeyValueArg<String, String>>,
    #[clap(long)]
    environmentd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    environmentd_affinity: Option<Affinity>,
    #[clap(long = "environmentd-toleration", value_parser = parse_tolerations)]
    environmentd_tolerations: Option<Vec<Toleration>>,
    #[clap(long, value_parser = parse_resources)]
    environmentd_default_resources: Option<ResourceRequirements>,
    #[clap(long)]
    clusterd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    clusterd_affinity: Option<Affinity>,
    #[clap(long = "clusterd-toleration", value_parser = parse_tolerations)]
    clusterd_tolerations: Option<Vec<Toleration>>,
    #[clap(long)]
    balancerd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    balancerd_affinity: Option<Affinity>,
    #[clap(long = "balancerd-toleration", value_parser = parse_tolerations)]
    balancerd_tolerations: Option<Vec<Toleration>>,
    #[clap(long, value_parser = parse_resources)]
    balancerd_default_resources: Option<ResourceRequirements>,
    #[clap(long)]
    console_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, value_parser = parse_affinity)]
    console_affinity: Option<Affinity>,
    #[clap(long = "console-toleration", value_parser = parse_tolerations)]
    console_tolerations: Option<Vec<Toleration>>,
    #[clap(long, value_parser = parse_resources)]
    console_default_resources: Option<ResourceRequirements>,
    #[clap(long, default_value = "always", value_enum)]
    image_pull_policy: KubernetesImagePullPolicy,
    #[clap(flatten)]
    network_policies: NetworkPolicyConfig,

    #[clap(long)]
    environmentd_cluster_replica_sizes: Option<String>,
    #[clap(long)]
    bootstrap_default_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_system_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_probe_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_support_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_catalog_server_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_analytics_cluster_replica_size: Option<String>,
    #[clap(long)]
    bootstrap_builtin_system_cluster_replication_factor: Option<u32>,
    #[clap(long)]
    bootstrap_builtin_probe_cluster_replication_factor: Option<u32>,
    #[clap(long)]
    bootstrap_builtin_support_cluster_replication_factor: Option<u32>,
    #[clap(long)]
    bootstrap_builtin_analytics_cluster_replication_factor: Option<u32>,

    #[clap(
        long,
        default_values = &["http://local.dev.materialize.com:3000", "http://local.mtrlz.com:3000", "http://localhost:3000", "https://staging.console.materialize.com"],
    )]
    environmentd_allowed_origins: Vec<HeaderValue>,
    #[clap(long, default_value = "https://console.materialize.com")]
    internal_console_proxy_url: String,

    #[clap(long, default_value = "6875")]
    environmentd_sql_port: u16,
    #[clap(long, default_value = "6876")]
    environmentd_http_port: u16,
    #[clap(long, default_value = "6877")]
    environmentd_internal_sql_port: u16,
    #[clap(long, default_value = "6878")]
    environmentd_internal_http_port: u16,
    #[clap(long, default_value = "6879")]
    environmentd_internal_persist_pubsub_port: u16,

    #[clap(long, default_value = "6875")]
    balancerd_sql_port: u16,
    #[clap(long, default_value = "6876")]
    balancerd_http_port: u16,
    #[clap(long, default_value = "8080")]
    balancerd_internal_http_port: u16,

    #[clap(long, default_value = "8080")]
    console_http_port: u16,

    #[clap(long, default_value = "{}")]
    default_certificate_specs: DefaultCertificateSpecs,

    #[clap(long, hide = true)]
    disable_license_key_checks: bool,
}

fn parse_affinity(s: &str) -> anyhow::Result<Affinity> {
    Ok(serde_json::from_str(s)?)
}

fn parse_tolerations(s: &str) -> anyhow::Result<Toleration> {
    Ok(serde_json::from_str(s)?)
}

fn parse_resources(s: &str) -> anyhow::Result<ResourceRequirements> {
    Ok(serde_json::from_str(s)?)
}

#[derive(Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DefaultCertificateSpecs {
    balancerd_external: Option<MaterializeCertSpec>,
    console_external: Option<MaterializeCertSpec>,
    internal: Option<MaterializeCertSpec>,
}

impl FromStr for DefaultCertificateSpecs {
    type Err = serde_json::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
    }
}

#[derive(clap::Parser)]
pub struct AwsInfo {
    #[clap(long)]
    aws_account_id: Option<String>,
    #[clap(long)]
    environmentd_iam_role_arn: Option<String>,
    #[clap(long)]
    environmentd_connection_role_arn: Option<String>,
    #[clap(long)]
    aws_secrets_controller_tags: Vec<String>,
    #[clap(long)]
    environmentd_availability_zones: Option<Vec<String>>,
}

#[derive(clap::Parser)]
pub struct NetworkPolicyConfig {
    #[clap(long = "network-policies-internal-enabled")]
    internal_enabled: bool,

    #[clap(long = "network-policies-ingress-enabled")]
    ingress_enabled: bool,

    #[clap(long = "network-policies-ingress-cidrs")]
    ingress_cidrs: Vec<String>,

    #[clap(long = "network-policies-egress-enabled")]
    egress_enabled: bool,

    #[clap(long = "network-policies-egress-cidrs")]
    egress_cidrs: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    Anyhow(#[from] anyhow::Error),
    Kube(#[from] kube::Error),
    Reqwest(#[from] reqwest::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anyhow(e) => write!(f, "{e}"),
            Self::Kube(e) => write!(f, "{e}"),
            Self::Reqwest(e) => write!(f, "{e}"),
        }
    }
}

pub struct Context {
    config: MaterializeControllerArgs,
    tracing: TracingCliArgs,
    orchestratord_namespace: String,
    metrics: Arc<Metrics>,
    materializes: reflector::Store<Materialize>,
    needs_update: Arc<Mutex<BTreeSet<String>>>,
}

impl Context {
    pub async fn new(
        config: MaterializeControllerArgs,
        tracing: TracingCliArgs,
        orchestratord_namespace: String,
        metrics: Arc<Metrics>,
        client: kube::Client,
    ) -> Self {
        if config.cloud_provider == CloudProvider::Aws {
            assert!(
                config.aws_info.aws_account_id.is_some(),
                "--aws-account-id is required when using --cloud-provider=aws"
            );
        }

        Self {
            config,
            tracing,
            orchestratord_namespace,
            metrics,
            materializes: Self::make_reflector(client.clone()).await,
            needs_update: Default::default(),
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

        new_mz.status = Some(status);
        mz_api
            .replace_status(
                &mz.name_unchecked(),
                &PostParams::default(),
                serde_json::to_vec(&new_mz).unwrap(),
            )
            .await
    }

    async fn promote(
        &self,
        client: &Client,
        mz: &Materialize,
        resources: environmentd::Resources,
        active_generation: u64,
        desired_generation: u64,
        resources_hash: String,
    ) -> Result<Option<Action>, Error> {
        if let Some(action) = resources.promote_services(client, &mz.namespace()).await? {
            return Ok(Some(action));
        }
        resources
            .teardown_generation(client, mz, active_generation)
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
                conditions: vec![Condition {
                    type_: "UpToDate".into(),
                    status: "True".into(),
                    last_transition_time: Time(chrono::offset::Utc::now()),
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

    fn check_environment_id_conflicts(&self, mz: &Materialize) -> Result<(), Error> {
        if mz.spec.environment_id.is_nil() {
            // this is always a bug - we delay doing this check until the
            // resource should have an environment id set, either from the
            // license key, or explicitly given, or randomly defaulted.
            return Err(Error::Anyhow(anyhow::anyhow!(
                "trying to reconcile a materialize resource with no environment id - this is a bug!"
            )));
        }

        for existing_mz in self.materializes.state() {
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

    async fn make_reflector<K>(client: Client) -> reflector::Store<K>
    where
        K: kube::Resource<DynamicType = ()>
            + Clone
            + Send
            + Sync
            + DeserializeOwned
            + Serialize
            + std::fmt::Debug
            + 'static,
    {
        let api = kube::Api::all(client);
        let (store, writer) = reflector::store();
        let reflector =
            reflector::reflector(writer, watcher(api, watcher::Config::default().timeout(29)));
        mz_ore::task::spawn(
            || format!("{} reflector", K::kind(&Default::default())),
            async {
                reflector
                    .for_each(|res| {
                        if let Err(e) = res {
                            warn!("error in {} reflector: {}", K::kind(&Default::default()), e);
                        }
                        ready(())
                    })
                    .await
            },
        );
        // the only way this can return an error is if we drop the writer,
        // which we do not ever do, so unwrap is fine
        store.wait_until_ready().await.unwrap();
        store
    }
}

#[async_trait::async_trait]
impl k8s_controller::Context for Context {
    type Resource = Materialize;
    type Error = Error;

    const FINALIZER_NAME: &'static str = "orchestratord.materialize.cloud/materialize";

    #[instrument(fields(organization_name=mz.name_unchecked()))]
    async fn apply(
        &self,
        client: Client,
        mz: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        let mz_api: Api<Materialize> = Api::namespaced(client.clone(), &mz.namespace());
        let secret_api: Api<Secret> = Api::namespaced(client.clone(), &mz.namespace());

        let status = mz.status();
        if mz.status.is_none() {
            self.update_status(&mz_api, mz, status, true).await?;
            // Updating the status should trigger a reconciliation
            // which will include a status this time.
            return Ok(None);
        }

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

        self.check_environment_id_conflicts(mz)?;

        // we compare the hash against the environment resources generated
        // for the current active generation, since that's what we expect to
        // have been applied earlier, but we don't want to use these
        // environment resources because when we apply them, we want to apply
        // them with data that uses the new generation
        let active_resources = environmentd::Resources::new(
            &self.config,
            &self.tracing,
            &self.orchestratord_namespace,
            mz,
            status.active_generation,
        );
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
        let resources = environmentd::Resources::new(
            &self.config,
            &self.tracing,
            &self.orchestratord_namespace,
            mz,
            desired_generation,
        );
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
                let mz = self
                    .update_status(
                        &mz_api,
                        mz,
                        MaterializeStatus {
                            active_generation,
                            // don't update the reconciliation id yet,
                            // because the rollout hasn't yet completed. if
                            // we fail later on, we want to ensure that the
                            // rollout gets retried.
                            last_completed_rollout_request: status.last_completed_rollout_request,
                            last_completed_rollout_environmentd_image_ref: status
                                .last_completed_rollout_environmentd_image_ref,
                            resource_id: status.resource_id,
                            resources_hash: String::new(),
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "Unknown".into(),
                                last_transition_time: Time(chrono::offset::Utc::now()),
                                message: format!(
                                    "Applying changes for generation {desired_generation}"
                                ),
                                observed_generation: mz.meta().generation,
                                reason: "Applying".into(),
                            }],
                        },
                        active_generation != desired_generation,
                    )
                    .await?;
                let mz = &mz;
                let status = mz.status();

                if !mz.within_upgrade_window() {
                    let last_completed_rollout_environmentd_image_ref =
                        status.last_completed_rollout_environmentd_image_ref;

                    self.update_status(
                        &mz_api,
                        mz,
                        MaterializeStatus {
                            active_generation,
                            last_completed_rollout_request: status.last_completed_rollout_request,
                            last_completed_rollout_environmentd_image_ref: last_completed_rollout_environmentd_image_ref.clone(),
                            resource_id: status.resource_id,
                            resources_hash: status.resources_hash,
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "False".into(),
                                last_transition_time: Time(chrono::offset::Utc::now()),
                                message: format!(
                        "Refusing to upgrade from {} to {}. More than one major version from last successful rollout.",
                        last_completed_rollout_environmentd_image_ref.expect("should be set if upgrade window check fails"),
                        &mz.spec.environmentd_image_ref,
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

                if mz.spec.rollout_strategy
                    == MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime
                {
                    // The only reason someone would choose this strategy is if they didn't have
                    // space for the two generations of pods.
                    // Lets make room for the new ones by deleting the old generation.
                    resources
                        .teardown_generation(&client, mz, active_generation)
                        .await?;
                }

                trace!("applying environment resources");
                match resources
                    .apply(&client, mz.should_force_promote(), &mz.namespace())
                    .await
                {
                    Ok(Some(action)) => {
                        trace!("new environment is not yet ready");
                        Ok(Some(action))
                    }
                    Ok(None) => {
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
                                conditions: vec![Condition {
                                    type_: "UpToDate".into(),
                                    status: "Unknown".into(),
                                    last_transition_time: Time(chrono::offset::Utc::now()),
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
                                last_completed_rollout_request: status.last_completed_rollout_request,
                                last_completed_rollout_environmentd_image_ref: status
                                    .last_completed_rollout_environmentd_image_ref,
                                resource_id: status.resource_id,
                                resources_hash: status.resources_hash,
                                conditions: vec![Condition {
                                    type_: "UpToDate".into(),
                                    status: "False".into(),
                                    last_transition_time: Time(chrono::offset::Utc::now()),
                                    message: format!(
                                        "Failed to apply changes for generation {desired_generation}: {e}"
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
                    resources
                        .teardown_generation(&client, mz, next_generation)
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
                            resource_id: status.resource_id,
                            resources_hash: status.resources_hash,
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "False".into(),
                                last_transition_time: Time(chrono::offset::Utc::now()),
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
                    resources
                        .teardown_generation(&client, mz, next_generation)
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
                            resource_id: status.resource_id,
                            resources_hash: status.resources_hash,
                            conditions: vec![Condition {
                                type_: "UpToDate".into(),
                                status: "True".into(),
                                last_transition_time: Time(chrono::offset::Utc::now()),
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

        let balancer = balancer::Resources::new(&self.config, mz);
        if self.config.create_balancers {
            result = balancer.apply(&client, &mz.namespace()).await?;
        } else {
            result = balancer.cleanup(&client, &mz.namespace()).await?;
        }

        if let Some(action) = result {
            return Ok(Some(action));
        }

        // and the console relies on the balancer service existing, which is
        // enforced by balancer::Resources::apply having a check for its pods
        // being up, and not returning successfully until they are

        let Some((_, environmentd_image_tag)) = mz.spec.environmentd_image_ref.rsplit_once(':')
        else {
            return Err(Error::Anyhow(anyhow::anyhow!(
                "failed to parse environmentd image ref: {}",
                mz.spec.environmentd_image_ref
            )));
        };
        let console_image_tag = self
            .config
            .console_image_tag_map
            .iter()
            .find(|kv| kv.key == environmentd_image_tag)
            .map(|kv| kv.value.clone())
            .unwrap_or_else(|| self.config.console_image_tag_default.clone());
        let console = console::Resources::new(
            &self.config,
            mz,
            &matching_image_from_environmentd_image_ref(
                &mz.spec.environmentd_image_ref,
                "console",
                Some(&console_image_tag),
            ),
        );
        if self.config.create_console {
            console.apply(&client, &mz.namespace()).await?;
        } else {
            console.cleanup(&client, &mz.namespace()).await?;
        }

        Ok(result)
    }

    #[instrument(fields(organization_name=mz.name_unchecked()))]
    async fn cleanup(
        &self,
        _client: Client,
        mz: &Self::Resource,
    ) -> Result<Option<Action>, Self::Error> {
        self.set_needs_update(mz, false);

        Ok(None)
    }
}

fn matching_image_from_environmentd_image_ref(
    environmentd_image_ref: &str,
    image_name: &str,
    image_tag: Option<&str>,
) -> String {
    let namespace = environmentd_image_ref
        .rsplit_once('/')
        .unwrap_or(("materialize", ""))
        .0;
    let tag = image_tag.unwrap_or_else(|| {
        environmentd_image_ref
            .rsplit_once(':')
            .unwrap_or(("", "unstable"))
            .1
    });
    format!("{namespace}/{image_name}:{tag}")
}
