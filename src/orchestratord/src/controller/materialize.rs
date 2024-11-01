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
    sync::{Arc, Mutex},
};

use http::HeaderValue;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use kube::{api::PostParams, runtime::controller::Action, Api, Client, Resource, ResourceExt};
use serde_json::json;
use tracing::{debug, trace};

use crate::metrics::Metrics;
use mz_cloud_resources::crd::materialize::v1alpha1::{Materialize, MaterializeStatus};
use mz_orchestrator_kubernetes::KubernetesImagePullPolicy;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::{cast::CastFrom, cli::KeyValueArg, instrument};

mod resources;

#[derive(clap::Parser)]
pub struct Args {
    #[clap(long)]
    cloud_provider: CloudProvider,
    #[clap(long)]
    region: String,
    #[clap(long)]
    local_development: bool,
    #[clap(long)]
    create_balancers: bool,
    #[clap(long)]
    enable_tls: bool,

    #[clap(flatten)]
    aws_info: AwsInfo,

    #[clap(long)]
    scheduler_name: Option<String>,
    #[clap(long)]
    enable_security_context: bool,

    #[clap(long)]
    environmentd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long)]
    clusterd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long)]
    balancerd_node_selector: Vec<KeyValueArg<String, String>>,
    #[clap(long, default_value = "always", arg_enum)]
    image_pull_policy: KubernetesImagePullPolicy,
    #[clap(flatten)]
    network_policies: NetworkPolicyConfig,

    #[clap(long, default_value_t = default_cluster_replica_sizes())]
    environmentd_cluster_replica_sizes: String,
    #[clap(long, default_value = "25cc")]
    bootstrap_default_cluster_replica_size: String,
    #[clap(long, default_value = "25cc")]
    bootstrap_builtin_system_cluster_replica_size: String,
    #[clap(long, default_value = "mz_probe")]
    bootstrap_builtin_probe_cluster_replica_size: String,
    #[clap(long, default_value = "25cc")]
    bootstrap_builtin_support_cluster_replica_size: String,
    #[clap(long, default_value = "50cc")]
    bootstrap_builtin_catalog_server_cluster_replica_size: String,
    #[clap(long, default_value = "25cc")]
    bootstrap_builtin_analytics_cluster_replica_size: String,

    #[clap(
        long,
        default_values = &["http://local.dev.materialize.com:3000", "http://local.mtrlz.com:3000", "http://localhost:3000", "https://staging.console.materialize.com"],
    )]
    environmentd_allowed_origins: Vec<HeaderValue>,
    #[clap(long, default_value = "https://console.materialize.com")]
    internal_console_proxy_url: String,

    #[clap(long, default_value = "6875")]
    environmentd_sql_port: i32,
    #[clap(long, default_value = "6876")]
    environmentd_http_port: i32,
    #[clap(long, default_value = "6877")]
    environmentd_internal_sql_port: i32,
    #[clap(long, default_value = "6878")]
    environmentd_internal_http_port: i32,
    #[clap(long)]
    environmentd_internal_http_host_override: Option<String>,
    #[clap(long, default_value = "6879")]
    environmentd_internal_persist_pubsub_port: i32,
    #[clap(long, default_value = "6880")]
    environmentd_balancer_sql_port: i32,
    #[clap(long, default_value = "6881")]
    environmentd_balancer_http_port: i32,

    #[clap(long, default_value = "6875")]
    balancerd_sql_port: i32,
    #[clap(long, default_value = "6876")]
    balancerd_http_port: i32,
    #[clap(long, default_value = "8080")]
    balancerd_internal_http_port: i32,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum CloudProvider {
    Aws,
    Local,
}

impl std::str::FromStr for CloudProvider {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_lowercase().as_ref() {
            "aws" => Ok(Self::Aws),
            "local" => Ok(Self::Local),
            _ => Err("invalid cloud provider".to_string()),
        }
    }
}

impl std::fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Aws => "aws",
                Self::Local => "local",
            }
        )
    }
}

#[derive(clap::Parser)]
pub struct AwsInfo {
    #[clap(long)]
    aws_account_id: Option<String>,
    #[clap(long)]
    environmentd_iam_role_arn: Option<String>,
    #[clap(long)]
    aws_secrets_controller_tags: Vec<String>,
    #[clap(long)]
    environmentd_availability_zones: Option<Vec<String>>,
}

#[derive(clap::Parser)]
pub struct NetworkPolicyConfig {
    #[clap(long = "network-policies-internal-enabled", default_value = "false")]
    internal_enabled: bool,

    #[clap(long = "network-policies-ingress-enabled", default_value = "false")]
    ingress_enabled: bool,

    #[clap(long = "network-policies-ingress-cidrs")]
    ingress_cidrs: Vec<String>,

    #[clap(long = "network-policies-egress-enabled", default_value = "false")]
    egress_enabled: bool,

    #[clap(long = "network-policies-egress-cidrs")]
    egress_cidrs: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    Anyhow(#[from] anyhow::Error),
    Kube(#[from] kube::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Anyhow(e) => write!(f, "{e}"),
            Self::Kube(e) => write!(f, "{e}"),
        }
    }
}

fn default_cluster_replica_sizes() -> String {
    json!({
        "25cc": {"workers": 1, "scale": 1, "credits_per_hour": "0.25"},
        "50cc": {"workers": 1, "scale": 1, "credits_per_hour": "0.5"},
        "mz_probe": {"workers": 1, "scale": 1, "credits_per_hour": "0.00"},
    })
    .to_string()
}

pub struct Context {
    config: Args,
    tracing: TracingCliArgs,
    orchestratord_namespace: String,
    metrics: Arc<Metrics>,
    needs_update: Arc<Mutex<BTreeSet<String>>>,
}

impl Context {
    pub fn new(
        config: Args,
        tracing: TracingCliArgs,
        orchestratord_namespace: String,
        metrics: Arc<Metrics>,
    ) -> Self {
        if config.cloud_provider == CloudProvider::Aws {
            assert!(
                config.aws_info.aws_account_id.is_some(),
                "--aws-account-id is required when using --cloud-provider=aws"
            );
            assert!(
                config.aws_info.environmentd_iam_role_arn.is_some(),
                "--environmentd-iam-role-arn is required when using --cloud-provider=aws"
            );
        }

        assert!(!config.enable_tls, "--enable-tls is not yet implemented");

        Self {
            config,
            tracing,
            orchestratord_namespace,
            metrics,
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
            .needs_update
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

        let status = mz.status();

        // we compare the hash against the environment resources generated
        // for the current active generation, since that's what we expect to
        // have been applied earlier, but we don't want to use these
        // environment resources because when we apply them, we want to apply
        // them with data that uses the new generation
        let active_resources = resources::Resources::new(
            &self.config,
            &self.tracing,
            &self.orchestratord_namespace,
            mz,
            status.active_generation,
        );
        let has_current_changes = status.resources_hash != active_resources.generate_hash();
        let active_generation = status.active_generation;
        let next_generation = active_generation + 1;
        let increment_generation = has_current_changes && !mz.in_place_rollout();
        let desired_generation = if increment_generation {
            next_generation
        } else {
            active_generation
        };

        // here we regenerate the environment resources using the
        // same inputs except with an updated generation
        let resources = resources::Resources::new(
            &self.config,
            &self.tracing,
            &self.orchestratord_namespace,
            mz,
            desired_generation,
        );
        let resources_hash = resources.generate_hash();

        let result = if has_current_changes {
            if mz.rollout_requested() {
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

                trace!("applying environment resources");
                match resources
                    .apply(&client, &self.config, increment_generation, &mz.namespace())
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
                        resources.promote_services(&client, &mz.namespace()).await?;
                        if increment_generation {
                            resources
                                .teardown_generation(&client, mz, active_generation)
                                .await?;
                        }
                        self.update_status(
                            &mz_api,
                            mz,
                            MaterializeStatus {
                                active_generation: desired_generation,
                                last_completed_rollout_request: mz.requested_reconciliation_id(),
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
                    Err(e) => {
                        resources
                            .teardown_generation(&client, mz, next_generation)
                            .await?;
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
            } else {
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
        } else {
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
        };

        result.map_err(Error::Anyhow)
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
