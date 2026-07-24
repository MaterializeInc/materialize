// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::{
    api::core::v1::{EnvVar, ResourceRequirements},
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{Condition, Time},
    },
    jiff::Timestamp,
};
use kube::{CustomResource, Resource, ResourceExt};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::crd::{ManagedResource, MaterializeCertSpec, new_resource_id};
use mz_server_core::listeners::AuthenticatorKind;

pub const LAST_KNOWN_ACTIVE_GENERATION_ANNOTATION: &str =
    "materialize.cloud/last-known-active-generation";
pub const FORCE_ROLLOUT_ANNOTATION: &str = "materialize.cloud/force-rollout";

#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
pub enum MaterializeRolloutStrategy {
    /// Create a new generation of pods, leaving the old generation around until the
    /// new ones are ready to take over.
    /// This minimizes downtime, and is what almost everyone should use.
    #[default]
    WaitUntilReady,

    /// Create a new generation of pods, leaving the old generation as the serving generation
    /// until the user manually promotes the new generation.
    ///
    /// When using `ManuallyPromote`, the new generation can be promoted at any
    /// time, even if it has dataflows that are not fully caught up, by setting
    /// `forcePromote` to the current rollout identifier: in `v1`, the value of
    /// `status.requestedRolloutHash`; in `v1alpha1`, the `requestRollout` value
    /// in the spec.
    ///
    /// To minimize downtime, promotion should occur when the new generation
    /// has caught up to the prior generation. To determine if the new
    /// generation has caught up, consult the `UpToDate` condition in the
    /// status of the Materialize Resource. If the condition's reason is
    /// `ReadyToPromote` the new generation is ready to promote.
    ///
    /// {{<warning>}}
    /// Do not leave new generations unpromoted indefinitely.
    ///
    /// The new generation keeps open read holds which prevent compaction. Once promoted or
    /// cancelled, those read holds are released. If left unpromoted for an extended time, this
    /// data can build up, and can cause extreme deletion load on the metadata backend database
    /// when finally promoted or cancelled.
    ///
    /// To guard against this, a rollout that remains in progress longer
    /// than `rolloutRequestTimeout` (default 24h) is automatically
    /// cancelled.
    /// {{</warning>}}
    ManuallyPromote,

    /// {{<warning>}}
    /// THIS WILL CAUSE YOUR MATERIALIZE INSTANCE TO BE UNAVAILABLE FOR SOME TIME!!!
    ///
    /// This strategy should ONLY be used by customers with physical hardware who do not have
    /// enough hardware for the `WaitUntilReady` strategy. If you think you want this, please
    /// consult with Materialize engineering to discuss your situation.
    /// {{</warning>}}
    ///
    /// Tear down the old generation of pods and promote the new generation of pods immediately,
    /// without waiting for the new generation of pods to be ready.
    ImmediatelyPromoteCausingDowntime,
}

/// Default for [`RolloutRequestTimeout`]. A new generation that sits
/// un-promoted holds back compaction via read holds, and promoting it
/// after a long delay can cause incident-inducing load; 24h is a
/// conservative upper bound on how long any rollout should take.
pub const DEFAULT_ROLLOUT_REQUEST_TIMEOUT: &str = "24h";

/// The maximum time [`v1alpha1::MaterializeSpec::rollout_request_timeout`] allows a
/// rollout to remain in progress.
///
/// A transparent wrapper around the duration string whose [`Default`] is
/// [`DEFAULT_ROLLOUT_REQUEST_TIMEOUT`]. Routing the default through `Default`
/// keeps a single source of truth: the derived `Default` for
/// [`v1alpha1::MaterializeSpec`], serde's `#[serde(default)]` (applied when the field
/// is omitted on deserialize), and the schema default surfaced in the
/// generated CRD (so the API server fills it in and `kubectl explain` shows
/// it) all resolve to the same value.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(transparent)]
pub struct RolloutRequestTimeout(pub String);

impl Default for RolloutRequestTimeout {
    fn default() -> Self {
        RolloutRequestTimeout(DEFAULT_ROLLOUT_REQUEST_TIMEOUT.to_owned())
    }
}

pub mod v1alpha1 {
    use super::*;

    #[derive(
        CustomResource,
        Clone,
        Debug,
        Default,
        PartialEq,
        Deserialize,
        Serialize,
        JsonSchema
    )]
    #[serde(rename_all = "camelCase")]
    #[kube(
        namespaced,
        group = "materialize.cloud",
        version = "v1alpha1",
        kind = "Materialize",
        singular = "materialize",
        plural = "materializes",
        shortname = "mzs",
        status = "MaterializeStatus",
        printcolumn = r#"{"name": "ImageRefRunning", "type": "string", "description": "Reference to the Docker image that is currently in use.", "jsonPath": ".status.lastCompletedRolloutEnvironmentdImageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "ImageRefToDeploy", "type": "string", "description": "Reference to the Docker image which will be deployed on the next rollout.", "jsonPath": ".spec.environmentdImageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "UpToDate", "type": "string", "description": "Whether the spec has been applied", "jsonPath": ".status.conditions[?(@.type==\"UpToDate\")].status", "priority": 1}"#
    )]
    pub struct MaterializeSpec {
        /// The environmentd image to run.
        pub environmentd_image_ref: String,
        /// Extra args to pass to the environmentd binary.
        pub environmentd_extra_args: Option<Vec<String>>,
        /// Extra environment variables to pass to the environmentd binary.
        pub environmentd_extra_env: Option<Vec<EnvVar>>,
        /// {{<warning>}}
        /// Deprecated.
        ///
        /// Use `service_account_annotations` to set "eks.amazonaws.com/role-arn" instead.
        /// {{</warning>}}
        ///
        /// If running in AWS, override the IAM role to use to give
        /// environmentd access to the persist S3 bucket.
        #[kube(deprecated)]
        pub environmentd_iam_role_arn: Option<String>,
        /// If running in AWS, override the IAM role to use to support
        /// the CREATE CONNECTION feature.
        pub environmentd_connection_role_arn: Option<String>,
        /// Resource requirements for the environmentd pod.
        pub environmentd_resource_requirements: Option<ResourceRequirements>,
        /// Amount of disk to allocate, if a storage class is provided.
        pub environmentd_scratch_volume_storage_requirement: Option<Quantity>,
        /// Resource requirements for the balancerd pod.
        pub balancerd_resource_requirements: Option<ResourceRequirements>,
        /// Resource requirements for the console pod.
        pub console_resource_requirements: Option<ResourceRequirements>,
        /// Number of balancerd pods to create.
        pub balancerd_replicas: Option<i32>,
        /// Number of console pods to create.
        pub console_replicas: Option<i32>,

        /// Name of the kubernetes service account to use.
        /// If not set, we will create one with the same name as this Materialize object.
        pub service_account_name: Option<String>,
        /// Annotations to apply to the service account.
        ///
        /// Annotations on service accounts are commonly used by cloud providers for IAM.
        /// AWS uses "eks.amazonaws.com/role-arn".
        /// Azure uses "azure.workload.identity/client-id", but
        /// additionally requires "azure.workload.identity/use": "true" on the pods.
        pub service_account_annotations: Option<BTreeMap<String, String>>,
        /// Labels to apply to the service account.
        pub service_account_labels: Option<BTreeMap<String, String>>,
        /// Annotations to apply to the pods.
        pub pod_annotations: Option<BTreeMap<String, String>>,
        /// Labels to apply to the pods.
        pub pod_labels: Option<BTreeMap<String, String>>,

        /// When changes are made to the environmentd resources (either via
        /// modifying fields in the spec here or by deploying a new
        /// orchestratord version which changes how resources are generated),
        /// existing environmentd processes won't be automatically restarted.
        /// In order to trigger a restart, the request_rollout field should be
        /// set to a new (random) value. Once the rollout completes, the value
        /// of `status.lastCompletedRolloutRequest` will be set to this value
        /// to indicate completion.
        ///
        /// Defaults to a random value in order to ensure that the first
        /// generation rollout is automatically triggered.
        #[serde(default)]
        pub request_rollout: Uuid,
        /// If `forcePromote` is set to the same value as `requestRollout`, the
        /// current rollout will skip waiting for clusters in the new
        /// generation to rehydrate before promoting the new environmentd to
        /// leader.
        #[serde(default)]
        pub force_promote: String,
        /// This value will be written to an annotation in the generated
        /// environmentd statefulset, in order to force the controller to
        /// detect the generated resources as changed even if no other changes
        /// happened. This can be used to force a rollout to a new generation
        /// even without making any meaningful changes, by setting it to the
        /// same value as `requestRollout`.
        #[serde(default)]
        pub force_rollout: Uuid,
        /// {{<warning>}}
        /// Deprecated and ignored. Use `rolloutStrategy` instead.
        /// {{</warning>}}
        #[kube(deprecated)]
        #[serde(default)]
        pub in_place_rollout: bool,
        /// Rollout strategy to use when upgrading this Materialize instance.
        #[serde(default)]
        pub rollout_strategy: MaterializeRolloutStrategy,
        /// The maximum amount of time a rollout may remain in progress before
        /// it is automatically cancelled.
        ///
        /// While a rollout is in progress, the new generation of `environmentd`
        /// runs in a read-only, un-promoted state and holds back compaction via
        /// read holds. Leaving it in this state for too long can cause
        /// incident-inducing load when it is eventually promoted, so the
        /// operator cancels the rollout once this timeout is exceeded: the new
        /// generation is torn down and the previously-active generation
        /// continues serving. A new rollout can then be triggered by setting
        /// `requestRollout` to a new value.
        ///
        /// This does not apply to the `ImmediatelyPromoteCausingDowntime`
        /// rollout strategy or to force-promoted rollouts, since by the time
        /// those are in progress the old generation may already be gone.
        ///
        /// The value is parsed as a human-readable duration, e.g. `24h`,
        /// `90m`, or `1h 30m`. Defaults to [`DEFAULT_ROLLOUT_REQUEST_TIMEOUT`]
        /// when omitted (the API server fills it in); an unparseable value also
        /// falls back to that default.
        #[serde(default)]
        pub rollout_request_timeout: RolloutRequestTimeout,
        /// The name of a secret containing `metadata_backend_url` and `persist_backend_url`.
        /// It may also contain `external_login_password_mz_system`, which will be used as
        /// the password for the `mz_system` user if `authenticatorKind` is `Password`,
        /// `Sasl`, or `Oidc`.
        pub backend_secret_name: String,
        /// How to authenticate with Materialize.
        #[serde(default)]
        pub authenticator_kind: AuthenticatorKind,
        /// Whether to enable role based access control. Defaults to false.
        #[serde(default)]
        pub enable_rbac: bool,

        /// The value used by environmentd (via the --environment-id flag) to
        /// uniquely identify this instance. Must be globally unique, and
        /// is required if a license key is not provided.
        /// NOTE: This value MUST NOT be changed in an existing instance,
        /// since it affects things like the way data is stored in the persist
        /// backend.
        #[serde(default)]
        pub environment_id: Uuid,

        /// The name of a ConfigMap containing system parameters in JSON format.
        /// The ConfigMap must contain a `system-params.json` key whose value
        /// is a valid JSON object containing valid system parameters.
        ///
        /// Run `SHOW ALL` in SQL to see a subset of configurable system parameters.
        ///
        /// Example ConfigMap:
        /// ```yaml
        /// data:
        ///   system-params.json: |
        ///     {
        ///       "max_connections": 1000
        ///     }
        /// ```
        pub system_parameter_configmap_name: Option<String>,

        /// The configuration for generating an x509 certificate using cert-manager for balancerd
        /// to present to incoming connections.
        /// The `dnsNames` and `issuerRef` fields are required.
        pub balancerd_external_certificate_spec: Option<MaterializeCertSpec>,
        /// The configuration for generating an x509 certificate using cert-manager for the console
        /// to present to incoming connections.
        /// The `dnsNames` and `issuerRef` fields are required.
        /// Not yet implemented.
        pub console_external_certificate_spec: Option<MaterializeCertSpec>,
        /// The cert-manager Issuer or ClusterIssuer to use for database internal communication.
        /// The `issuerRef` field is required.
        /// This currently is only used for environmentd, but will eventually support clusterd.
        /// Not yet implemented.
        pub internal_certificate_spec: Option<MaterializeCertSpec>,
    }

    impl Materialize {
        pub fn backend_secret_name(&self) -> String {
            self.spec.backend_secret_name.clone()
        }

        pub fn namespace(&self) -> String {
            self.meta().namespace.clone().unwrap()
        }

        pub fn create_service_account(&self) -> bool {
            self.spec.service_account_name.is_none()
        }

        pub fn service_account_name(&self) -> String {
            self.spec
                .service_account_name
                .clone()
                .unwrap_or_else(|| self.name_unchecked())
        }

        pub fn role_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn role_binding_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn environmentd_statefulset_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("environmentd-{generation}"))
        }

        pub fn environmentd_app_name(&self) -> String {
            "environmentd".to_owned()
        }

        pub fn environmentd_service_name(&self) -> String {
            self.name_prefixed("environmentd")
        }

        pub fn environmentd_service_internal_fqdn(&self) -> String {
            format!(
                "{}.{}.svc.cluster.local",
                self.environmentd_service_name(),
                self.meta().namespace.as_ref().unwrap()
            )
        }

        pub fn environmentd_generation_service_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("environmentd-{generation}"))
        }

        pub fn balancerd_app_name(&self) -> String {
            "balancerd".to_owned()
        }

        pub fn environmentd_certificate_name(&self) -> String {
            self.name_prefixed("environmentd-external")
        }

        pub fn environmentd_certificate_secret_name(&self) -> String {
            self.name_prefixed("environmentd-tls")
        }

        pub fn balancerd_deployment_name(&self) -> String {
            self.name_prefixed("balancerd")
        }

        pub fn balancerd_service_name(&self) -> String {
            self.name_prefixed("balancerd")
        }

        pub fn console_app_name(&self) -> String {
            "console".to_owned()
        }

        pub fn balancerd_external_certificate_name(&self) -> String {
            self.name_prefixed("balancerd-external")
        }

        pub fn balancerd_external_certificate_secret_name(&self) -> String {
            self.name_prefixed("balancerd-external-tls")
        }

        pub fn balancerd_replicas(&self) -> i32 {
            self.spec.balancerd_replicas.unwrap_or(2)
        }

        pub fn console_replicas(&self) -> i32 {
            self.spec.console_replicas.unwrap_or(2)
        }

        pub fn console_configmap_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn console_deployment_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn console_service_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn console_external_certificate_name(&self) -> String {
            self.name_prefixed("console-external")
        }

        pub fn console_external_certificate_secret_name(&self) -> String {
            self.name_prefixed("console-external-tls")
        }

        pub fn persist_pubsub_service_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("persist-pubsub-{generation}"))
        }

        pub fn listeners_configmap_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("listeners-{generation}"))
        }

        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mz{}-{}", self.resource_id(), suffix)
        }

        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        pub fn system_parameter_configmap_name(&self) -> Option<String> {
            self.spec.system_parameter_configmap_name.clone()
        }

        pub fn environmentd_scratch_volume_storage_requirement(&self) -> Quantity {
            self.spec
                .environmentd_scratch_volume_storage_requirement
                .clone()
                .unwrap_or_else(|| {
                    self.spec
                        .environmentd_resource_requirements
                        .as_ref()
                        .and_then(|requirements| {
                            requirements
                                .requests
                                .as_ref()
                                .or(requirements.limits.as_ref())
                        })
                        // TODO: in cloud, we've been defaulting to twice the
                        // memory limit, but k8s-openapi doesn't seem to
                        // provide any way to parse Quantity values, so there
                        // isn't an easy way to do arithmetic on it
                        .and_then(|requirements| requirements.get("memory").cloned())
                        // TODO: is there a better default to use here?
                        .unwrap_or_else(|| Quantity("4096Mi".to_string()))
                })
        }

        pub fn environment_id(&self, cloud_provider: &str, region: &str) -> String {
            format!(
                "{}-{}-{}-0",
                cloud_provider, region, self.spec.environment_id,
            )
        }

        pub fn requested_reconciliation_id(&self) -> Uuid {
            self.spec.request_rollout
        }

        /// The value used to force the generated per-generation resources to
        /// be detected as changed even when nothing else in the spec changed,
        /// so that a requested rollout actually creates a new generation of
        /// pods rather than completing as a no-op.
        ///
        /// Combines `spec.forceRollout` with the
        /// [`FORCE_ROLLOUT_ANNOTATION`] annotation; changing either forces a
        /// new generation. The annotation exists so that automation (e.g. the
        /// GCP node upgrade watcher in orchestratord) can force a rollout
        /// without touching spec fields that may be managed by tools like
        /// Terraform.
        pub fn force_rollout_value(&self) -> String {
            match self
                .meta()
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(FORCE_ROLLOUT_ANNOTATION))
            {
                Some(annotation) => format!("{}/{}", self.spec.force_rollout, annotation),
                None => self.spec.force_rollout.to_string(),
            }
        }

        pub fn rollout_requested(&self) -> bool {
            self.requested_reconciliation_id()
                != self
                    .status
                    .as_ref()
                    .map_or_else(Uuid::nil, |status| status.last_completed_rollout_request)
        }

        /// The maximum amount of time a rollout may remain in progress before
        /// it is automatically cancelled. Parsed from
        /// [`MaterializeSpec::rollout_request_timeout`], falling back to
        /// [`DEFAULT_ROLLOUT_REQUEST_TIMEOUT`] when unset or unparseable.
        pub fn rollout_request_timeout(&self) -> Duration {
            let timeout = &self.spec.rollout_request_timeout.0;
            humantime::parse_duration(timeout)
                .or_else(|e| {
                    tracing::warn!(
                        rollout_request_timeout = %timeout,
                        "failed to parse rolloutRequestTimeout, using default: {e}",
                    );
                    humantime::parse_duration(DEFAULT_ROLLOUT_REQUEST_TIMEOUT)
                })
                .expect("DEFAULT_ROLLOUT_REQUEST_TIMEOUT must be a valid duration")
        }

        /// If a timeout-eligible rollout is currently in progress, returns the
        /// time at which it entered the in-progress (`Unknown`) state. Used to
        /// enforce the rollout timeout.
        ///
        /// The `Applying` and `ReadyToPromote` phases are both reported as a
        /// single in-progress window: [`Self::up_to_date_transition_time`]
        /// carries the timestamp forward across them (they share the `Unknown`
        /// status), so the timeout spans the whole pre-promotion rollout rather
        /// than resetting at each phase.
        ///
        /// The `Promoting` phase is deliberately excluded even though it is
        /// also `Unknown`: once a rollout has reached promotion it must never
        /// be cancelled by the timeout, since the previously-active generation
        /// may already be torn down, leaving nothing to fall back to. (The
        /// controller also never reaches the timeout check while promoting,
        /// because `is_promoting` takes priority; this is belt-and-suspenders.)
        pub fn rollout_in_progress_since(&self) -> Option<Timestamp> {
            self.status
                .as_ref()?
                .conditions
                .iter()
                .find_map(|condition| {
                    if condition.type_ == "UpToDate"
                        && condition.status == "Unknown"
                        && condition.reason != "Promoting"
                    {
                        Some(condition.last_transition_time.0)
                    } else {
                        None
                    }
                })
        }

        /// The `last_transition_time` to record for a new `UpToDate` condition
        /// with `new_status`, following the Kubernetes convention that
        /// `last_transition_time` marks when the condition's *status* last
        /// changed — not its reason or message. While the status is unchanged
        /// the existing timestamp is carried forward; it only resets to `now`
        /// when the status actually changes (or there is no prior condition).
        ///
        /// This is what lets a rollout that moves through several same-status
        /// phases (`Applying` -> `ReadyToPromote`, both `Unknown`) be measured
        /// from when it first entered that status, so the rollout timeout
        /// covers the phases together instead of restarting at each one.
        pub fn up_to_date_transition_time(&self, new_status: &str, now: Timestamp) -> Timestamp {
            self.status
                .as_ref()
                .and_then(|status| {
                    status
                        .conditions
                        .iter()
                        .find(|condition| condition.type_ == "UpToDate")
                })
                .filter(|condition| condition.status == new_status)
                .map_or(now, |condition| condition.last_transition_time.0)
        }

        /// Returns the environmentd image ref of the currently-active
        /// generation: the image of the last completed rollout, falling back
        /// to the spec image when no rollout has completed yet. Downstream
        /// resources (balancerd, console) should track this rather than
        /// [`MaterializeSpec::environmentd_image_ref`] so they stay aligned
        /// with the running environmentd when the spec is mid-rollout or has
        /// been partially reverted (DEP-42).
        pub fn active_environmentd_image_ref(&self) -> &str {
            self.status
                .as_ref()
                .and_then(|s| s.last_completed_rollout_environmentd_image_ref.as_deref())
                .unwrap_or(&self.spec.environmentd_image_ref)
        }

        pub fn set_force_promote(&mut self) {
            self.spec.force_promote = self.spec.request_rollout.hyphenated().to_string();
        }

        pub fn should_force_promote(&self) -> bool {
            self.spec.force_promote == self.spec.request_rollout.hyphenated().to_string()
                || self.spec.force_promote
                    == super::v1::Materialize::from(self.clone()).generate_rollout_hash()
                || self.spec.rollout_strategy
                    == MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime
        }

        pub fn conditions_need_update(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return true;
            };
            if status.conditions.is_empty() {
                return true;
            }
            for condition in &status.conditions {
                if condition.observed_generation != self.meta().generation {
                    return true;
                }
            }
            false
        }

        pub fn is_ready_to_promote(&self, resources_hash: &str) -> bool {
            let Some(status) = self.status.as_ref() else {
                return false;
            };
            if status.conditions.is_empty() {
                return false;
            }
            status
                .conditions
                .iter()
                .any(|condition| condition.reason == "ReadyToPromote")
                && &status.resources_hash == resources_hash
        }

        pub fn is_promoting(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return false;
            };
            if status.conditions.is_empty() {
                return false;
            }
            status
                .conditions
                .iter()
                .any(|condition| condition.reason == "Promoting")
        }

        pub fn update_in_progress(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return false;
            };
            if status.conditions.is_empty() {
                return false;
            }
            for condition in &status.conditions {
                if condition.type_ == "UpToDate" && condition.status == "Unknown" {
                    return true;
                }
            }
            false
        }

        /// Checks that the given version is greater than or equal
        /// to the existing version, if the existing version
        /// can be parsed.
        pub fn meets_minimum_version(&self, minimum: &Version) -> bool {
            let version = parse_image_ref(&self.spec.environmentd_image_ref);
            match version {
                // Use cmp_precedence() to ignore build metadata per SemVer 2.0.0 spec
                Some(version) => version.cmp_precedence(minimum).is_ge(),
                // In the rare case that we see an image reference
                // that we can't parse, we assume that it satisfies all
                // version checks. Usually these are custom images that have
                // been by a developer on a branch forked from a recent copy
                // of main, and so this works out reasonably well in practice.
                None => {
                    tracing::warn!(
                        image_ref = %self.spec.environmentd_image_ref,
                        "failed to parse image ref",
                    );
                    true
                }
            }
        }

        /// This check isn't strictly required since environmentd will still be able to determine
        /// if the upgrade is allowed or not. However, doing this check allows us to provide
        /// the error as soon as possible and in a more user friendly way.
        pub fn is_valid_upgrade_version(active_version: &Version, next_version: &Version) -> bool {
            // Don't allow rolling back
            // Note: semver comparison handles RC versions correctly:
            // v26.0.0-rc.1 < v26.0.0-rc.2 < v26.0.0
            // Use cmp_precedence() to ignore build metadata
            if next_version.cmp_precedence(active_version) == std::cmp::Ordering::Less {
                return false;
            }

            if active_version.major == 0 {
                if next_version.major != active_version.major {
                    if next_version.major == 26 {
                        // We require customers to upgrade from 0.147.20 (Self Managed 25.2) or v0.164.X (Cloud)
                        // before upgrading to 26.0.0
                        return (active_version.minor == 147 && active_version.patch >= 20)
                            || active_version.minor >= 164;
                    } else {
                        return false;
                    }
                }
                // Self managed 25.1 to 25.2
                if next_version.minor == 147 && active_version.minor == 130 {
                    return true;
                }
                // only allow upgrading a single minor version at a time
                return next_version.minor <= active_version.minor + 1;
            } else if active_version.major >= 26 {
                // For versions 26.X.X and onwards, we deny upgrades past 1 major version of the active version
                return next_version.major <= active_version.major + 1;
            }

            true
        }

        /// Checks if the current environmentd image ref is within the upgrade window of the last
        /// successful rollout.
        pub fn within_upgrade_window(&self) -> bool {
            let active_environmentd_version = self
                .status
                .as_ref()
                .and_then(|status| {
                    status
                        .last_completed_rollout_environmentd_image_ref
                        .as_ref()
                })
                .and_then(|image_ref| parse_image_ref(image_ref));

            if let (Some(next_environmentd_version), Some(active_environmentd_version)) = (
                parse_image_ref(&self.spec.environmentd_image_ref),
                active_environmentd_version,
            ) {
                Self::is_valid_upgrade_version(
                    &active_environmentd_version,
                    &next_environmentd_version,
                )
            } else {
                // If we fail to parse either version,
                // we still allow the upgrade since environmentd will still error if the upgrade is not allowed.
                true
            }
        }

        pub fn status(&self) -> MaterializeStatus {
            self.status.clone().unwrap_or_else(|| {
                let mut status = MaterializeStatus::default();

                status.resource_id = new_resource_id();

                // If we're creating the initial status on an un-soft-deleted
                // Environment we need to ensure that the last active generation
                // is restored, otherwise the env will crash loop indefinitely
                // as its catalog would have durably recorded a greater generation
                if let Some(last_active_generation) = self
                    .annotations()
                    .get(LAST_KNOWN_ACTIVE_GENERATION_ANNOTATION)
                {
                    status.active_generation = last_active_generation
                        .parse()
                        .expect("valid int generation");
                }

                // Initialize the last completed rollout environmentd image ref to
                // the current image ref if not already set.
                status.last_completed_rollout_environmentd_image_ref =
                    Some(self.spec.environmentd_image_ref.clone());

                status
            })
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeStatus {
        /// Resource identifier used as a name prefix to avoid pod name collisions.
        pub resource_id: String,
        /// The generation of Materialize pods actively capable of servicing requests.
        pub active_generation: u64,
        /// The UUID of the last successfully completed rollout.
        pub last_completed_rollout_request: Uuid,
        /// The image ref of the environmentd image that was last successfully rolled out.
        /// Used to deny upgrades past 1 major version from the last successful rollout.
        /// When None, we upgrade anyways.
        pub last_completed_rollout_environmentd_image_ref: Option<String>,
        /// A hash calculated from the spec of resources to be created based on this Materialize
        /// spec. This is used for detecting when the existing resources are up to date.
        /// If you want to trigger a rollout without making other changes that would cause this
        /// hash to change, you must set forceRollout to the same UUID as requestRollout.
        pub resources_hash: String,
        /// The last completed rollout hash from v1.
        /// This exists on this older version only for round-trip conversion support.
        pub last_completed_rollout_hash: Option<String>,
        pub conditions: Vec<Condition>,
    }

    impl MaterializeStatus {
        pub fn needs_update(&self, other: &Self) -> bool {
            let now = Timestamp::now();
            let mut a = self.clone();
            for condition in &mut a.conditions {
                condition.last_transition_time = Time(now);
            }
            let mut b = other.clone();
            for condition in &mut b.conditions {
                condition.last_transition_time = Time(now);
            }
            a != b
        }
    }

    impl ManagedResource for Materialize {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/organization-name".to_owned(),
                    self.name_unchecked(),
                ),
                (
                    "materialize.cloud/organization-namespace".to_owned(),
                    self.namespace(),
                ),
                (
                    "materialize.cloud/mz-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
            ])
        }

        fn app_name(&self) -> Option<&str> {
            Some("environmentd")
        }
    }

    impl From<v1::Materialize> for Materialize {
        fn from(value: v1::Materialize) -> Self {
            let rollout_hash = value.generate_rollout_hash();
            // Derive a deterministic UUID from the rollout hash so that the
            // same v1 spec always produces the same requestRollout,
            // making re-applies of an unchanged spec idempotent.
            let request_rollout = Uuid::new_v5(&Uuid::NAMESPACE_OID, rollout_hash.as_bytes());
            Materialize {
                metadata: value.metadata,
                spec: MaterializeSpec {
                    environmentd_image_ref: value.spec.environmentd_image_ref,
                    environmentd_extra_args: value.spec.environmentd_extra_args,
                    environmentd_extra_env: value.spec.environmentd_extra_env,
                    environmentd_iam_role_arn: None,
                    environmentd_connection_role_arn: value.spec.environmentd_connection_role_arn,
                    environmentd_resource_requirements: value
                        .spec
                        .environmentd_resource_requirements,
                    environmentd_scratch_volume_storage_requirement: value
                        .spec
                        .environmentd_scratch_volume_storage_requirement,
                    balancerd_resource_requirements: value.spec.balancerd_resource_requirements,
                    console_resource_requirements: value.spec.console_resource_requirements,
                    balancerd_replicas: value.spec.balancerd_replicas,
                    console_replicas: value.spec.console_replicas,
                    service_account_name: value.spec.service_account_name,
                    service_account_annotations: value.spec.service_account_annotations,
                    service_account_labels: value.spec.service_account_labels,
                    pod_annotations: value.spec.pod_annotations,
                    pod_labels: value.spec.pod_labels,
                    force_promote: value.spec.force_promote.unwrap_or_default(),
                    force_rollout: value.spec.force_rollout,
                    rollout_strategy: value.spec.rollout_strategy,
                    rollout_request_timeout: value.spec.rollout_request_timeout,
                    backend_secret_name: value.spec.backend_secret_name,
                    authenticator_kind: value.spec.authenticator_kind,
                    enable_rbac: value.spec.enable_rbac,
                    environment_id: value.spec.environment_id,
                    system_parameter_configmap_name: value.spec.system_parameter_configmap_name,
                    balancerd_external_certificate_spec: value
                        .spec
                        .balancerd_external_certificate_spec,
                    console_external_certificate_spec: value.spec.console_external_certificate_spec,
                    internal_certificate_spec: value.spec.internal_certificate_spec,
                    request_rollout,
                    in_place_rollout: false,
                },
                status: value.status.map(|status| MaterializeStatus {
                    resource_id: status.resource_id,
                    active_generation: status.active_generation,
                    last_completed_rollout_environmentd_image_ref: status
                        .last_completed_rollout_environmentd_image_ref,
                    conditions: status.conditions,
                    // Derive the same deterministic UUID from the last
                    // completed hash so that request_rollout == this value
                    // when the spec hasn't changed (no rollout needed).
                    last_completed_rollout_request: status
                        .last_completed_rollout_hash
                        .as_ref()
                        .map(|hash| Uuid::new_v5(&Uuid::NAMESPACE_OID, hash.as_bytes()))
                        .unwrap_or(Uuid::nil()),
                    last_completed_rollout_hash: status.last_completed_rollout_hash,
                    resources_hash: "".to_owned(),
                }),
            }
        }
    }

    /// Partial mirror of [`MaterializeSpec`] for field-wise conversion of
    /// objects that may be incomplete. See [`super::convert_v1_to_v1alpha1`].
    ///
    /// Each field is a [`PartialField`], distinguishing absent from explicit
    /// null from a value, and unknown fields pass through via `extra`, so
    /// serializing reproduces exactly what was deserialized. Adding a field
    /// to [`MaterializeSpec`] breaks the exhaustive destructures in the
    /// `From` impls below until its conversion is decided.
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PartialMaterializeSpec {
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_image_ref: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_extra_args: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_extra_env: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_iam_role_arn: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_connection_role_arn: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_resource_requirements: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_scratch_volume_storage_requirement: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub balancerd_resource_requirements: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub console_resource_requirements: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub balancerd_replicas: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub console_replicas: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub service_account_name: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub service_account_annotations: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub service_account_labels: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub pod_annotations: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub pod_labels: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub request_rollout: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub force_promote: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub force_rollout: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub in_place_rollout: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub rollout_strategy: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub rollout_request_timeout: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub backend_secret_name: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub authenticator_kind: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub enable_rbac: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environment_id: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub system_parameter_configmap_name: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub balancerd_external_certificate_spec: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub console_external_certificate_spec: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub internal_certificate_spec: PartialField,
        #[serde(flatten)]
        pub extra: serde_json::Map<String, serde_json::Value>,
    }

    /// Partial mirror of [`MaterializeStatus`], see [`PartialMaterializeSpec`].
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PartialMaterializeStatus {
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub resource_id: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub active_generation: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub last_completed_rollout_request: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub last_completed_rollout_environmentd_image_ref: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub resources_hash: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub last_completed_rollout_hash: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub conditions: PartialField,
        #[serde(flatten)]
        pub extra: serde_json::Map<String, serde_json::Value>,
    }

    /// Partial mirror of [`Materialize`], see [`PartialMaterializeSpec`].
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PartialMaterialize {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub api_version: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub kind: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub metadata: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub spec: Option<PartialMaterializeSpec>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub status: Option<PartialMaterializeStatus>,
        #[serde(flatten)]
        pub extra: serde_json::Map<String, serde_json::Value>,
    }

    impl From<MaterializeSpec> for PartialMaterializeSpec {
        fn from(spec: MaterializeSpec) -> Self {
            let MaterializeSpec {
                environmentd_image_ref,
                environmentd_extra_args,
                environmentd_extra_env,
                environmentd_iam_role_arn,
                environmentd_connection_role_arn,
                environmentd_resource_requirements,
                environmentd_scratch_volume_storage_requirement,
                balancerd_resource_requirements,
                console_resource_requirements,
                balancerd_replicas,
                console_replicas,
                service_account_name,
                service_account_annotations,
                service_account_labels,
                pod_annotations,
                pod_labels,
                request_rollout,
                force_promote,
                force_rollout,
                in_place_rollout,
                rollout_strategy,
                rollout_request_timeout,
                backend_secret_name,
                authenticator_kind,
                enable_rbac,
                environment_id,
                system_parameter_configmap_name,
                balancerd_external_certificate_spec,
                console_external_certificate_spec,
                internal_certificate_spec,
            } = spec;
            Self {
                environmentd_image_ref: present(environmentd_image_ref),
                environmentd_extra_args: present_opt(environmentd_extra_args),
                environmentd_extra_env: present_opt(environmentd_extra_env),
                environmentd_iam_role_arn: present_opt(environmentd_iam_role_arn),
                environmentd_connection_role_arn: present_opt(environmentd_connection_role_arn),
                environmentd_resource_requirements: present_opt(environmentd_resource_requirements),
                environmentd_scratch_volume_storage_requirement: present_opt(
                    environmentd_scratch_volume_storage_requirement,
                ),
                balancerd_resource_requirements: present_opt(balancerd_resource_requirements),
                console_resource_requirements: present_opt(console_resource_requirements),
                balancerd_replicas: present_opt(balancerd_replicas),
                console_replicas: present_opt(console_replicas),
                service_account_name: present_opt(service_account_name),
                service_account_annotations: present_opt(service_account_annotations),
                service_account_labels: present_opt(service_account_labels),
                pod_annotations: present_opt(pod_annotations),
                pod_labels: present_opt(pod_labels),
                request_rollout: present(request_rollout),
                force_promote: present(force_promote),
                force_rollout: present(force_rollout),
                in_place_rollout: present(in_place_rollout),
                rollout_strategy: present(rollout_strategy),
                rollout_request_timeout: present(rollout_request_timeout),
                backend_secret_name: present(backend_secret_name),
                authenticator_kind: present(authenticator_kind),
                enable_rbac: present(enable_rbac),
                environment_id: present(environment_id),
                system_parameter_configmap_name: present_opt(system_parameter_configmap_name),
                balancerd_external_certificate_spec: present_opt(
                    balancerd_external_certificate_spec,
                ),
                console_external_certificate_spec: present_opt(console_external_certificate_spec),
                internal_certificate_spec: present_opt(internal_certificate_spec),
                extra: serde_json::Map::new(),
            }
        }
    }

    impl From<MaterializeStatus> for PartialMaterializeStatus {
        fn from(status: MaterializeStatus) -> Self {
            let MaterializeStatus {
                resource_id,
                active_generation,
                last_completed_rollout_request,
                last_completed_rollout_environmentd_image_ref,
                resources_hash,
                last_completed_rollout_hash,
                conditions,
            } = status;
            Self {
                resource_id: present(resource_id),
                active_generation: present(active_generation),
                last_completed_rollout_request: present(last_completed_rollout_request),
                last_completed_rollout_environmentd_image_ref: present_opt(
                    last_completed_rollout_environmentd_image_ref,
                ),
                resources_hash: present(resources_hash),
                last_completed_rollout_hash: present_opt(last_completed_rollout_hash),
                conditions: present(conditions),
                extra: serde_json::Map::new(),
            }
        }
    }

    impl From<Materialize> for PartialMaterialize {
        fn from(mz: Materialize) -> Self {
            let Materialize {
                metadata,
                spec,
                status,
            } = mz;
            Self {
                api_version: Some("materialize.cloud/v1alpha1".to_owned()),
                kind: Some("Materialize".into()),
                metadata: Some(
                    serde_json::to_value(metadata).expect("ObjectMeta serializes to JSON"),
                ),
                spec: Some(spec.into()),
                status: status.map(Into::into),
                extra: serde_json::Map::new(),
            }
        }
    }

    impl From<super::v1::PartialMaterializeSpec> for PartialMaterializeSpec {
        fn from(spec: super::v1::PartialMaterializeSpec) -> Self {
            let super::v1::PartialMaterializeSpec {
                environmentd_image_ref,
                environmentd_extra_args,
                environmentd_extra_env,
                environmentd_connection_role_arn,
                environmentd_resource_requirements,
                environmentd_scratch_volume_storage_requirement,
                balancerd_resource_requirements,
                console_resource_requirements,
                balancerd_replicas,
                console_replicas,
                service_account_name,
                service_account_annotations,
                service_account_labels,
                pod_annotations,
                pod_labels,
                force_promote,
                force_rollout,
                rollout_strategy,
                rollout_request_timeout,
                backend_secret_name,
                authenticator_kind,
                enable_rbac,
                environment_id,
                system_parameter_configmap_name,
                balancerd_external_certificate_spec,
                console_external_certificate_spec,
                internal_certificate_spec,
                extra,
            } = spec;
            Self {
                environmentd_image_ref,
                environmentd_extra_args,
                environmentd_extra_env,
                environmentd_iam_role_arn: None,
                environmentd_connection_role_arn,
                environmentd_resource_requirements,
                environmentd_scratch_volume_storage_requirement,
                balancerd_resource_requirements,
                console_resource_requirements,
                balancerd_replicas,
                console_replicas,
                service_account_name,
                service_account_annotations,
                service_account_labels,
                pod_annotations,
                pod_labels,
                // Derived from the complete spec. Spliced in by
                // `convert_v1_to_v1alpha1` when the input is complete, left
                // absent for partial objects so a field manager cannot gain
                // ownership of a field it never set.
                request_rollout: None,
                force_promote,
                force_rollout,
                in_place_rollout: None,
                rollout_strategy,
                rollout_request_timeout,
                backend_secret_name,
                authenticator_kind,
                enable_rbac,
                environment_id,
                system_parameter_configmap_name,
                balancerd_external_certificate_spec,
                console_external_certificate_spec,
                internal_certificate_spec,
                extra,
            }
        }
    }

    impl From<super::v1::PartialMaterializeStatus> for PartialMaterializeStatus {
        fn from(status: super::v1::PartialMaterializeStatus) -> Self {
            let super::v1::PartialMaterializeStatus {
                resource_id,
                active_generation,
                last_completed_rollout_environmentd_image_ref,
                last_completed_rollout_hash,
                requested_rollout_hash: _,
                conditions,
                extra,
            } = status;
            Self {
                resource_id,
                active_generation,
                // Derived from the complete object, spliced in by
                // `convert_v1_to_v1alpha1` when the input is complete, left
                // absent for partial objects so a field manager cannot gain
                // ownership of fields it never set.
                last_completed_rollout_request: None,
                resources_hash: None,
                last_completed_rollout_environmentd_image_ref,
                last_completed_rollout_hash,
                conditions,
                extra,
            }
        }
    }

    impl From<super::v1::PartialMaterialize> for PartialMaterialize {
        fn from(mz: super::v1::PartialMaterialize) -> Self {
            let super::v1::PartialMaterialize {
                api_version: _,
                kind,
                metadata,
                spec,
                status,
                extra,
            } = mz;
            Self {
                api_version: Some("materialize.cloud/v1alpha1".to_owned()),
                kind,
                metadata,
                spec: spec.map(Into::into),
                status: status.map(Into::into),
                extra,
            }
        }
    }
}

pub mod v1 {
    use super::*;

    #[derive(
        CustomResource,
        Clone,
        Debug,
        Default,
        PartialEq,
        Deserialize,
        Serialize,
        JsonSchema
    )]
    #[serde(rename_all = "camelCase")]
    #[kube(
        namespaced,
        group = "materialize.cloud",
        version = "v1",
        kind = "Materialize",
        singular = "materialize",
        plural = "materializes",
        shortname = "mzs",
        status = "MaterializeStatus",
        printcolumn = r#"{"name": "ImageRefRunning", "type": "string", "description": "Reference to the Docker image that is currently in use.", "jsonPath": ".status.lastCompletedRolloutEnvironmentdImageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "ImageRefToDeploy", "type": "string", "description": "Reference to the Docker image which will be deployed on the next rollout.", "jsonPath": ".spec.environmentdImageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "UpToDate", "type": "string", "description": "Whether the spec has been applied", "jsonPath": ".status.conditions[?(@.type==\"UpToDate\")].status", "priority": 1}"#
    )]
    pub struct MaterializeSpec {
        /// The environmentd image to run.
        pub environmentd_image_ref: String,
        /// Extra args to pass to the environmentd binary.
        pub environmentd_extra_args: Option<Vec<String>>,
        /// Extra environment variables to pass to the environmentd binary.
        pub environmentd_extra_env: Option<Vec<EnvVar>>,
        /// If running in AWS, override the IAM role to use to support
        /// the CREATE CONNECTION feature.
        pub environmentd_connection_role_arn: Option<String>,
        /// Resource requirements for the environmentd pod.
        pub environmentd_resource_requirements: Option<ResourceRequirements>,
        /// Amount of disk to allocate, if a storage class is provided.
        pub environmentd_scratch_volume_storage_requirement: Option<Quantity>,
        /// Resource requirements for the balancerd pod.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub balancerd_resource_requirements: Option<ResourceRequirements>,
        /// Resource requirements for the console pod.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub console_resource_requirements: Option<ResourceRequirements>,
        /// Number of balancerd pods to create.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub balancerd_replicas: Option<i32>,
        /// Number of console pods to create.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub console_replicas: Option<i32>,

        /// Name of the kubernetes service account to use.
        /// If not set, we will create one with the same name as this Materialize object.
        pub service_account_name: Option<String>,
        /// Annotations to apply to the service account.
        ///
        /// Annotations on service accounts are commonly used by cloud providers for IAM.
        /// AWS uses "eks.amazonaws.com/role-arn".
        /// Azure uses "azure.workload.identity/client-id", but
        /// additionally requires "azure.workload.identity/use": "true" on the pods.
        pub service_account_annotations: Option<BTreeMap<String, String>>,
        /// Labels to apply to the service account.
        pub service_account_labels: Option<BTreeMap<String, String>>,
        /// Annotations to apply to the pods.
        pub pod_annotations: Option<BTreeMap<String, String>>,
        /// Labels to apply to the pods.
        pub pod_labels: Option<BTreeMap<String, String>>,

        /// If `forcePromote` is set to the same value as the `status.requestedRolloutHash`,
        /// current rollout will skip waiting for clusters in the new
        /// generation to rehydrate before promoting the new environmentd to
        /// leader.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub force_promote: Option<String>,
        /// This value will force the controller to detect the spec as changed
        /// even if no other changes happened. This can be used to force a rollout
        /// to a new generation even without making any meaningful changes.
        #[serde(default)]
        pub force_rollout: Uuid,
        /// Rollout strategy to use when upgrading this Materialize instance.
        #[serde(default)]
        pub rollout_strategy: MaterializeRolloutStrategy,
        /// The maximum amount of time a rollout may remain in progress before
        /// it is automatically cancelled.
        ///
        /// While a rollout is in progress, the new generation of `environmentd`
        /// runs in a read-only, un-promoted state and holds back compaction via
        /// read holds. Leaving it in this state for too long can cause
        /// incident-inducing load when it is eventually promoted, so the
        /// operator cancels the rollout once this timeout is exceeded: the new
        /// generation is torn down and the previously-active generation
        /// continues serving. A new rollout can then be triggered by setting
        /// `forceRollout` to a new value.
        ///
        /// This does not apply to the `ImmediatelyPromoteCausingDowntime`
        /// rollout strategy or to force-promoted rollouts, since by the time
        /// those are in progress the old generation may already be gone.
        ///
        /// The value is parsed as a human-readable duration, e.g. `24h`,
        /// `90m`, or `1h 30m`. Defaults to [`DEFAULT_ROLLOUT_REQUEST_TIMEOUT`]
        /// when omitted (the API server fills it in); an unparseable value also
        /// falls back to that default.
        #[serde(default)]
        pub rollout_request_timeout: RolloutRequestTimeout,
        /// The name of a secret containing `metadata_backend_url` and `persist_backend_url`.
        /// It may also contain `external_login_password_mz_system`, which will be used as
        /// the password for the `mz_system` user if `authenticatorKind` is `Password`.
        pub backend_secret_name: String,
        /// How to authenticate with Materialize.
        #[serde(default)]
        pub authenticator_kind: AuthenticatorKind,
        /// Whether to enable role based access control. Defaults to false.
        #[serde(default)]
        pub enable_rbac: bool,

        /// The value used by environmentd (via the --environment-id flag) to
        /// uniquely identify this instance. Must be globally unique, and
        /// is required if a license key is not provided.
        /// NOTE: This value MUST NOT be changed in an existing instance,
        /// since it affects things like the way data is stored in the persist
        /// backend.
        #[serde(default)]
        pub environment_id: Uuid,

        /// The name of a ConfigMap containing system parameters in JSON format.
        /// The ConfigMap must contain a `system-params.json` key whose value
        /// is a valid JSON object containing valid system parameters.
        ///
        /// Run `SHOW ALL` in SQL to see a subset of configurable system parameters.
        ///
        /// Example ConfigMap:
        /// ```yaml
        /// data:
        ///   system-params.json: |
        ///     {
        ///       "max_connections": 1000
        ///     }
        /// ```
        pub system_parameter_configmap_name: Option<String>,

        /// The configuration for generating an x509 certificate using cert-manager for balancerd
        /// to present to incoming connections.
        /// The `dnsNames` and `issuerRef` fields are required.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub balancerd_external_certificate_spec: Option<MaterializeCertSpec>,
        /// The configuration for generating an x509 certificate using cert-manager for the console
        /// to present to incoming connections.
        /// The `dnsNames` and `issuerRef` fields are required.
        /// Not yet implemented.
        ///
        /// This field is excluded from the rollout hash and changes will not trigger a rollout.
        pub console_external_certificate_spec: Option<MaterializeCertSpec>,
        /// The cert-manager Issuer or ClusterIssuer to use for database internal communication.
        /// The `issuerRef` field is required.
        /// This currently is only used for environmentd, but will eventually support clusterd.
        /// Not yet implemented.
        pub internal_certificate_spec: Option<MaterializeCertSpec>,
    }

    impl Materialize {
        pub fn generate_rollout_hash(&self) -> String {
            let mut hasher = Sha256::new();
            // Remove fields that don't affect the resources generated per generation,
            // and we don't want to trigger a rollout from.
            let spec = MaterializeSpec {
                environmentd_image_ref: self.spec.environmentd_image_ref.clone(),
                environmentd_extra_args: self.spec.environmentd_extra_args.clone(),
                environmentd_extra_env: self.spec.environmentd_extra_env.clone(),
                environmentd_connection_role_arn: self
                    .spec
                    .environmentd_connection_role_arn
                    .clone(),
                environmentd_resource_requirements: self
                    .spec
                    .environmentd_resource_requirements
                    .clone(),
                environmentd_scratch_volume_storage_requirement: self
                    .spec
                    .environmentd_scratch_volume_storage_requirement
                    .clone(),
                balancerd_resource_requirements: None,
                console_resource_requirements: None,
                balancerd_replicas: None,
                console_replicas: None,
                service_account_name: self.spec.service_account_name.clone(),
                service_account_annotations: self.spec.service_account_annotations.clone(),
                service_account_labels: self.spec.service_account_labels.clone(),
                pod_annotations: self.spec.pod_annotations.clone(),
                pod_labels: self.spec.pod_labels.clone(),
                force_promote: None,
                force_rollout: self.spec.force_rollout,
                rollout_strategy: self.spec.rollout_strategy.clone(),
                rollout_request_timeout: self.spec.rollout_request_timeout.clone(),
                backend_secret_name: self.spec.backend_secret_name.clone(),
                authenticator_kind: self.spec.authenticator_kind,
                enable_rbac: self.spec.enable_rbac,
                environment_id: self.spec.environment_id,
                system_parameter_configmap_name: self.spec.system_parameter_configmap_name.clone(),
                balancerd_external_certificate_spec: None,
                console_external_certificate_spec: None,
                internal_certificate_spec: self.spec.internal_certificate_spec.clone(),
            };
            hasher.update(&serde_json::to_vec(&spec).unwrap());
            if let Some(annotation) = self
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(FORCE_ROLLOUT_ANNOTATION))
            {
                hasher.update(annotation);
            }
            format!("{:x}", hasher.finalize())
        }

        pub fn backend_secret_name(&self) -> String {
            self.spec.backend_secret_name.clone()
        }

        pub fn namespace(&self) -> String {
            self.meta().namespace.clone().unwrap()
        }

        pub fn create_service_account(&self) -> bool {
            self.spec.service_account_name.is_none()
        }

        pub fn service_account_name(&self) -> String {
            self.spec
                .service_account_name
                .clone()
                .unwrap_or_else(|| self.name_unchecked())
        }

        pub fn role_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn role_binding_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn environmentd_statefulset_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("environmentd-{generation}"))
        }

        pub fn environmentd_app_name(&self) -> String {
            "environmentd".to_owned()
        }

        pub fn environmentd_service_name(&self) -> String {
            self.name_prefixed("environmentd")
        }

        pub fn environmentd_service_internal_fqdn(&self) -> String {
            format!(
                "{}.{}.svc.cluster.local",
                self.environmentd_service_name(),
                self.meta().namespace.as_ref().unwrap()
            )
        }

        pub fn environmentd_generation_service_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("environmentd-{generation}"))
        }

        pub fn balancerd_app_name(&self) -> String {
            "balancerd".to_owned()
        }

        pub fn environmentd_certificate_name(&self) -> String {
            self.name_prefixed("environmentd-external")
        }

        pub fn environmentd_certificate_secret_name(&self) -> String {
            self.name_prefixed("environmentd-tls")
        }

        pub fn balancerd_deployment_name(&self) -> String {
            self.name_prefixed("balancerd")
        }

        pub fn balancerd_service_name(&self) -> String {
            self.name_prefixed("balancerd")
        }

        pub fn console_app_name(&self) -> String {
            "console".to_owned()
        }

        pub fn balancerd_external_certificate_name(&self) -> String {
            self.name_prefixed("balancerd-external")
        }

        pub fn balancerd_external_certificate_secret_name(&self) -> String {
            self.name_prefixed("balancerd-external-tls")
        }

        pub fn balancerd_replicas(&self) -> i32 {
            self.spec.balancerd_replicas.unwrap_or(2)
        }

        pub fn console_replicas(&self) -> i32 {
            self.spec.console_replicas.unwrap_or(2)
        }

        pub fn console_configmap_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn console_deployment_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn console_service_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn console_external_certificate_name(&self) -> String {
            self.name_prefixed("console-external")
        }

        pub fn console_external_certificate_secret_name(&self) -> String {
            self.name_prefixed("console-external-tls")
        }

        pub fn persist_pubsub_service_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("persist-pubsub-{generation}"))
        }

        pub fn listeners_configmap_name(&self, generation: u64) -> String {
            self.name_prefixed(&format!("listeners-{generation}"))
        }

        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mz{}-{}", self.resource_id(), suffix)
        }

        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        pub fn system_parameter_configmap_name(&self) -> Option<String> {
            self.spec.system_parameter_configmap_name.clone()
        }

        pub fn environmentd_scratch_volume_storage_requirement(&self) -> Quantity {
            self.spec
                .environmentd_scratch_volume_storage_requirement
                .clone()
                .unwrap_or_else(|| {
                    self.spec
                        .environmentd_resource_requirements
                        .as_ref()
                        .and_then(|requirements| {
                            requirements
                                .requests
                                .as_ref()
                                .or(requirements.limits.as_ref())
                        })
                        // TODO: in cloud, we've been defaulting to twice the
                        // memory limit, but k8s-openapi doesn't seem to
                        // provide any way to parse Quantity values, so there
                        // isn't an easy way to do arithmetic on it
                        .and_then(|requirements| requirements.get("memory").cloned())
                        // TODO: is there a better default to use here?
                        .unwrap_or_else(|| Quantity("4096Mi".to_string()))
                })
        }

        pub fn environment_id(&self, cloud_provider: &str, region: &str) -> String {
            format!(
                "{}-{}-{}-0",
                cloud_provider, region, self.spec.environment_id,
            )
        }

        pub fn rollout_requested(&self) -> bool {
            self.status
                .as_ref()
                .map(|status| status.last_completed_rollout_hash != status.requested_rollout_hash)
                .unwrap_or(false)
        }

        pub fn set_force_promote(&mut self) {
            self.spec.force_promote = Some(self.generate_rollout_hash());
        }

        pub fn should_force_promote(&self) -> bool {
            self.spec.force_promote.as_ref()
                == self
                    .status
                    .as_ref()
                    .and_then(|status| status.requested_rollout_hash.as_ref())
                || self.spec.rollout_strategy
                    == MaterializeRolloutStrategy::ImmediatelyPromoteCausingDowntime
        }

        pub fn conditions_need_update(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return true;
            };
            if status.conditions.is_empty() {
                return true;
            }
            for condition in &status.conditions {
                if condition.observed_generation != self.meta().generation {
                    return true;
                }
            }
            false
        }

        pub fn is_ready_to_promote(&self, rollout_hash: &str) -> bool {
            let Some(status) = self.status.as_ref() else {
                return false;
            };
            if status.conditions.is_empty() {
                return false;
            }
            status
                .conditions
                .iter()
                .any(|condition| condition.reason == "ReadyToPromote")
                && status.requested_rollout_hash.as_deref() == Some(rollout_hash)
        }

        pub fn is_promoting(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return false;
            };
            if status.conditions.is_empty() {
                return false;
            }
            status
                .conditions
                .iter()
                .any(|condition| condition.reason == "Promoting")
        }

        pub fn update_in_progress(&self) -> bool {
            let Some(status) = self.status.as_ref() else {
                return false;
            };
            if status.conditions.is_empty() {
                return false;
            }
            for condition in &status.conditions {
                if condition.type_ == "UpToDate" && condition.status == "Unknown" {
                    return true;
                }
            }
            false
        }

        /// Checks that the given version is greater than or equal
        /// to the existing version, if the existing version
        /// can be parsed.
        pub fn meets_minimum_version(&self, minimum: &Version) -> bool {
            let version = parse_image_ref(&self.spec.environmentd_image_ref);
            match version {
                // Use cmp_precedence() to ignore build metadata per SemVer 2.0.0 spec
                Some(version) => version.cmp_precedence(minimum).is_ge(),
                // In the rare case that we see an image reference
                // that we can't parse, we assume that it satisfies all
                // version checks. Usually these are custom images that have
                // been by a developer on a branch forked from a recent copy
                // of main, and so this works out reasonably well in practice.
                None => {
                    tracing::warn!(
                        image_ref = %self.spec.environmentd_image_ref,
                        "failed to parse image ref",
                    );
                    true
                }
            }
        }

        /// This check isn't strictly required since environmentd will still be able to determine
        /// if the upgrade is allowed or not. However, doing this check allows us to provide
        /// the error as soon as possible and in a more user friendly way.
        pub fn is_valid_upgrade_version(active_version: &Version, next_version: &Version) -> bool {
            // Don't allow rolling back
            // Note: semver comparison handles RC versions correctly:
            // v26.0.0-rc.1 < v26.0.0-rc.2 < v26.0.0
            // Use cmp_precedence() to ignore build metadata
            if next_version.cmp_precedence(active_version) == std::cmp::Ordering::Less {
                return false;
            }

            if active_version.major == 0 {
                if next_version.major != active_version.major {
                    if next_version.major == 26 {
                        // We require customers to upgrade from 0.147.20 (Self Managed 25.2) or v0.164.X (Cloud)
                        // before upgrading to 26.0.0

                        return (active_version.minor == 147 && active_version.patch >= 20)
                            || active_version.minor >= 164;
                    } else {
                        return false;
                    }
                }
                // Self managed 25.1 to 25.2
                if next_version.minor == 147 && active_version.minor == 130 {
                    return true;
                }
                // only allow upgrading a single minor version at a time
                return next_version.minor <= active_version.minor + 1;
            } else if active_version.major >= 26 {
                // For versions 26.X.X and onwards, we deny upgrades past 1 major version of the active version
                return next_version.major <= active_version.major + 1;
            }

            true
        }

        /// Checks if the current environmentd image ref is within the upgrade window of the last
        /// successful rollout.
        pub fn within_upgrade_window(&self) -> bool {
            let active_environmentd_version = self
                .status
                .as_ref()
                .and_then(|status| {
                    status
                        .last_completed_rollout_environmentd_image_ref
                        .as_ref()
                })
                .and_then(|image_ref| parse_image_ref(image_ref));

            if let (Some(next_environmentd_version), Some(active_environmentd_version)) = (
                parse_image_ref(&self.spec.environmentd_image_ref),
                active_environmentd_version,
            ) {
                Self::is_valid_upgrade_version(
                    &active_environmentd_version,
                    &next_environmentd_version,
                )
            } else {
                // If we fail to parse either version,
                // we still allow the upgrade since environmentd will still error if the upgrade is not allowed.
                true
            }
        }

        pub fn status(&self) -> MaterializeStatus {
            self.status.clone().unwrap_or_else(|| {
                let mut status = MaterializeStatus::default();

                status.resource_id = new_resource_id();

                // If we're creating the initial status on an un-soft-deleted
                // Environment we need to ensure that the last active generation
                // is restored, otherwise the env will crash loop indefinitely
                // as its catalog would have durably recorded a greater generation
                if let Some(last_active_generation) = self
                    .annotations()
                    .get(LAST_KNOWN_ACTIVE_GENERATION_ANNOTATION)
                {
                    status.active_generation = last_active_generation
                        .parse()
                        .expect("valid int generation");
                }

                // Initialize the last completed rollout environmentd image ref to
                // the current image ref if not already set.
                status.last_completed_rollout_environmentd_image_ref =
                    Some(self.spec.environmentd_image_ref.clone());

                status
            })
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeStatus {
        /// Resource identifier used as a name prefix to avoid pod name collisions.
        pub resource_id: String,
        /// The generation of Materialize pods actively capable of servicing requests.
        pub active_generation: u64,
        /// The image ref of the environmentd image that was last successfully rolled out.
        /// Used to deny upgrades past 1 major version from the last successful rollout.
        /// When None, we upgrade anyways.
        pub last_completed_rollout_environmentd_image_ref: Option<String>,
        /// The last completed rollout's requestedRolloutHash.
        pub last_completed_rollout_hash: Option<String>,
        /// Hash of a subset of the Materialize spec and other fields.
        /// This is used to determine when the spec has changed and we need to rollout.
        pub requested_rollout_hash: Option<String>,
        pub conditions: Vec<Condition>,
    }

    impl MaterializeStatus {
        pub fn needs_update(&self, other: &Self) -> bool {
            let now = Timestamp::now();
            let mut a = self.clone();
            for condition in &mut a.conditions {
                condition.last_transition_time = Time(now);
            }
            let mut b = other.clone();
            for condition in &mut b.conditions {
                condition.last_transition_time = Time(now);
            }
            a != b
        }
    }

    impl ManagedResource for Materialize {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/organization-name".to_owned(),
                    self.name_unchecked(),
                ),
                (
                    "materialize.cloud/organization-namespace".to_owned(),
                    self.namespace(),
                ),
                (
                    "materialize.cloud/mz-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
            ])
        }

        fn app_name(&self) -> Option<&str> {
            Some("environmentd")
        }
    }

    impl From<v1alpha1::Materialize> for Materialize {
        fn from(value: v1alpha1::Materialize) -> Self {
            let is_promoting = value.is_promoting();
            let service_account_annotations = if let Some(environmentd_iam_role_arn) =
                value.spec.environmentd_iam_role_arn
            {
                let mut annotations = value.spec.service_account_annotations.unwrap_or_default();
                annotations
                    .entry("eks.amazonaws.com/role-arn".to_owned())
                    .or_insert(environmentd_iam_role_arn);
                Some(annotations)
            } else {
                value.spec.service_account_annotations
            };
            let mut mz = Materialize {
                metadata: value.metadata,
                spec: MaterializeSpec {
                    environmentd_image_ref: value.spec.environmentd_image_ref,
                    environmentd_extra_args: value.spec.environmentd_extra_args,
                    environmentd_extra_env: value.spec.environmentd_extra_env,
                    environmentd_connection_role_arn: value.spec.environmentd_connection_role_arn,
                    environmentd_resource_requirements: value
                        .spec
                        .environmentd_resource_requirements,
                    environmentd_scratch_volume_storage_requirement: value
                        .spec
                        .environmentd_scratch_volume_storage_requirement,
                    balancerd_resource_requirements: value.spec.balancerd_resource_requirements,
                    console_resource_requirements: value.spec.console_resource_requirements,
                    balancerd_replicas: value.spec.balancerd_replicas,
                    console_replicas: value.spec.console_replicas,
                    service_account_name: value.spec.service_account_name,
                    service_account_annotations,
                    service_account_labels: value.spec.service_account_labels,
                    pod_annotations: value.spec.pod_annotations,
                    pod_labels: value.spec.pod_labels,
                    force_promote: if value.spec.force_promote.is_empty()
                        || &value.spec.force_promote == "00000000-0000-0000-0000-000000000000"
                    {
                        None
                    } else {
                        Some(value.spec.force_promote.to_string())
                    },
                    force_rollout: value.spec.force_rollout,
                    rollout_strategy: value.spec.rollout_strategy,
                    rollout_request_timeout: value.spec.rollout_request_timeout,
                    backend_secret_name: value.spec.backend_secret_name,
                    authenticator_kind: value.spec.authenticator_kind,
                    enable_rbac: value.spec.enable_rbac,
                    environment_id: value.spec.environment_id,
                    system_parameter_configmap_name: value.spec.system_parameter_configmap_name,
                    balancerd_external_certificate_spec: value
                        .spec
                        .balancerd_external_certificate_spec,
                    console_external_certificate_spec: value.spec.console_external_certificate_spec,
                    internal_certificate_spec: value.spec.internal_certificate_spec,
                },
                status: None,
            };
            let calculated_rollout_hash = mz.generate_rollout_hash();
            let last_completed_rollout_hash = match value
                .status
                .as_ref()
                .and_then(|status| status.last_completed_rollout_hash.to_owned())
            {
                Some(last_completed_rollout_hash) => Some(last_completed_rollout_hash),
                None => {
                    let currently_rolling_out = value
                        .status
                        .as_ref()
                        .map(|status| {
                            status.last_completed_rollout_request != value.spec.request_rollout
                                // If this is the first apply,
                                // these could both be nil and we still need to do a rollout.
                                || status.last_completed_rollout_request.is_nil()
                        })
                        .unwrap_or(true);
                    if currently_rolling_out {
                        // If they store a change, we're going to start over on a new rollout.
                        None
                    } else {
                        Some(calculated_rollout_hash.clone())
                    }
                }
            };
            let requested_rollout_hash = if is_promoting {
                None
            } else {
                Some(calculated_rollout_hash)
            };
            mz.status = value.status.map(|status| MaterializeStatus {
                resource_id: status.resource_id,
                active_generation: status.active_generation,
                last_completed_rollout_environmentd_image_ref: status
                    .last_completed_rollout_environmentd_image_ref,
                last_completed_rollout_hash,
                requested_rollout_hash,
                conditions: status.conditions,
            });
            mz
        }
    }

    /// Partial mirror of [`MaterializeSpec`] for field-wise conversion of
    /// objects that may be incomplete. See [`super::convert_v1alpha1_to_v1`].
    ///
    /// Each field is a [`PartialField`], distinguishing absent from explicit
    /// null from a value, and unknown fields pass through via `extra`, so
    /// serializing reproduces exactly what was deserialized. Adding a field
    /// to [`MaterializeSpec`] breaks the exhaustive destructures in the
    /// `From` impls below until its conversion is decided.
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PartialMaterializeSpec {
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_image_ref: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_extra_args: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_extra_env: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_connection_role_arn: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_resource_requirements: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environmentd_scratch_volume_storage_requirement: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub balancerd_resource_requirements: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub console_resource_requirements: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub balancerd_replicas: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub console_replicas: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub service_account_name: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub service_account_annotations: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub service_account_labels: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub pod_annotations: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub pod_labels: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub force_promote: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub force_rollout: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub rollout_strategy: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub rollout_request_timeout: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub backend_secret_name: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub authenticator_kind: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub enable_rbac: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub environment_id: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub system_parameter_configmap_name: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub balancerd_external_certificate_spec: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub console_external_certificate_spec: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub internal_certificate_spec: PartialField,
        #[serde(flatten)]
        pub extra: serde_json::Map<String, serde_json::Value>,
    }

    /// Partial mirror of [`MaterializeStatus`], see [`PartialMaterializeSpec`].
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PartialMaterializeStatus {
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub resource_id: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub active_generation: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub last_completed_rollout_environmentd_image_ref: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub last_completed_rollout_hash: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub requested_rollout_hash: PartialField,
        #[serde(
            default,
            with = "double_option",
            skip_serializing_if = "Option::is_none"
        )]
        pub conditions: PartialField,
        #[serde(flatten)]
        pub extra: serde_json::Map<String, serde_json::Value>,
    }

    /// Partial mirror of [`Materialize`], see [`PartialMaterializeSpec`].
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct PartialMaterialize {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub api_version: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub kind: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub metadata: Option<serde_json::Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub spec: Option<PartialMaterializeSpec>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub status: Option<PartialMaterializeStatus>,
        #[serde(flatten)]
        pub extra: serde_json::Map<String, serde_json::Value>,
    }

    impl From<MaterializeSpec> for PartialMaterializeSpec {
        fn from(spec: MaterializeSpec) -> Self {
            let MaterializeSpec {
                environmentd_image_ref,
                environmentd_extra_args,
                environmentd_extra_env,
                environmentd_connection_role_arn,
                environmentd_resource_requirements,
                environmentd_scratch_volume_storage_requirement,
                balancerd_resource_requirements,
                console_resource_requirements,
                balancerd_replicas,
                console_replicas,
                service_account_name,
                service_account_annotations,
                service_account_labels,
                pod_annotations,
                pod_labels,
                force_promote,
                force_rollout,
                rollout_strategy,
                rollout_request_timeout,
                backend_secret_name,
                authenticator_kind,
                enable_rbac,
                environment_id,
                system_parameter_configmap_name,
                balancerd_external_certificate_spec,
                console_external_certificate_spec,
                internal_certificate_spec,
            } = spec;
            Self {
                environmentd_image_ref: present(environmentd_image_ref),
                environmentd_extra_args: present_opt(environmentd_extra_args),
                environmentd_extra_env: present_opt(environmentd_extra_env),
                environmentd_connection_role_arn: present_opt(environmentd_connection_role_arn),
                environmentd_resource_requirements: present_opt(environmentd_resource_requirements),
                environmentd_scratch_volume_storage_requirement: present_opt(
                    environmentd_scratch_volume_storage_requirement,
                ),
                balancerd_resource_requirements: present_opt(balancerd_resource_requirements),
                console_resource_requirements: present_opt(console_resource_requirements),
                balancerd_replicas: present_opt(balancerd_replicas),
                console_replicas: present_opt(console_replicas),
                service_account_name: present_opt(service_account_name),
                service_account_annotations: present_opt(service_account_annotations),
                service_account_labels: present_opt(service_account_labels),
                pod_annotations: present_opt(pod_annotations),
                pod_labels: present_opt(pod_labels),
                force_promote: present_opt(force_promote),
                force_rollout: present(force_rollout),
                rollout_strategy: present(rollout_strategy),
                rollout_request_timeout: present(rollout_request_timeout),
                backend_secret_name: present(backend_secret_name),
                authenticator_kind: present(authenticator_kind),
                enable_rbac: present(enable_rbac),
                environment_id: present(environment_id),
                system_parameter_configmap_name: present_opt(system_parameter_configmap_name),
                balancerd_external_certificate_spec: present_opt(
                    balancerd_external_certificate_spec,
                ),
                console_external_certificate_spec: present_opt(console_external_certificate_spec),
                internal_certificate_spec: present_opt(internal_certificate_spec),
                extra: serde_json::Map::new(),
            }
        }
    }

    impl From<MaterializeStatus> for PartialMaterializeStatus {
        fn from(status: MaterializeStatus) -> Self {
            let MaterializeStatus {
                resource_id,
                active_generation,
                last_completed_rollout_environmentd_image_ref,
                last_completed_rollout_hash,
                requested_rollout_hash,
                conditions,
            } = status;
            Self {
                resource_id: present(resource_id),
                active_generation: present(active_generation),
                last_completed_rollout_environmentd_image_ref: present_opt(
                    last_completed_rollout_environmentd_image_ref,
                ),
                last_completed_rollout_hash: present_opt(last_completed_rollout_hash),
                requested_rollout_hash: present_opt(requested_rollout_hash),
                conditions: present(conditions),
                extra: serde_json::Map::new(),
            }
        }
    }

    impl From<Materialize> for PartialMaterialize {
        fn from(mz: Materialize) -> Self {
            let Materialize {
                metadata,
                spec,
                status,
            } = mz;
            Self {
                api_version: Some("materialize.cloud/v1".to_owned()),
                kind: Some("Materialize".into()),
                metadata: Some(
                    serde_json::to_value(metadata).expect("ObjectMeta serializes to JSON"),
                ),
                spec: Some(spec.into()),
                status: status.map(Into::into),
                extra: serde_json::Map::new(),
            }
        }
    }

    impl From<super::v1alpha1::PartialMaterializeSpec> for PartialMaterializeSpec {
        fn from(spec: super::v1alpha1::PartialMaterializeSpec) -> Self {
            let super::v1alpha1::PartialMaterializeSpec {
                environmentd_image_ref,
                environmentd_extra_args,
                environmentd_extra_env,
                environmentd_iam_role_arn,
                environmentd_connection_role_arn,
                environmentd_resource_requirements,
                environmentd_scratch_volume_storage_requirement,
                balancerd_resource_requirements,
                console_resource_requirements,
                balancerd_replicas,
                console_replicas,
                service_account_name,
                service_account_annotations,
                service_account_labels,
                pod_annotations,
                pod_labels,
                request_rollout: _,
                force_promote,
                force_rollout,
                in_place_rollout: _,
                rollout_strategy,
                rollout_request_timeout,
                backend_secret_name,
                authenticator_kind,
                enable_rbac,
                environment_id,
                system_parameter_configmap_name,
                balancerd_external_certificate_spec,
                console_external_certificate_spec,
                internal_certificate_spec,
                extra,
            } = spec;
            let service_account_annotations = merge_environmentd_iam_role_arn(
                service_account_annotations,
                environmentd_iam_role_arn,
            );
            // "" and the nil UUID mean "not force promoting" in v1alpha1,
            // which v1 spells as an absent field.
            let force_promote = match force_promote {
                Some(Some(value)) if value == "" || value == NIL_UUID_STR => None,
                other => other,
            };
            Self {
                environmentd_image_ref,
                environmentd_extra_args,
                environmentd_extra_env,
                environmentd_connection_role_arn,
                environmentd_resource_requirements,
                environmentd_scratch_volume_storage_requirement,
                balancerd_resource_requirements,
                console_resource_requirements,
                balancerd_replicas,
                console_replicas,
                service_account_name,
                service_account_annotations,
                service_account_labels,
                pod_annotations,
                pod_labels,
                force_promote,
                force_rollout,
                rollout_strategy,
                rollout_request_timeout,
                backend_secret_name,
                authenticator_kind,
                enable_rbac,
                environment_id,
                system_parameter_configmap_name,
                balancerd_external_certificate_spec,
                console_external_certificate_spec,
                internal_certificate_spec,
                extra,
            }
        }
    }

    impl From<super::v1alpha1::PartialMaterializeStatus> for PartialMaterializeStatus {
        fn from(status: super::v1alpha1::PartialMaterializeStatus) -> Self {
            let super::v1alpha1::PartialMaterializeStatus {
                resource_id,
                active_generation,
                last_completed_rollout_request: _,
                last_completed_rollout_environmentd_image_ref,
                resources_hash: _,
                last_completed_rollout_hash,
                conditions,
                extra,
            } = status;
            Self {
                resource_id,
                active_generation,
                last_completed_rollout_environmentd_image_ref,
                last_completed_rollout_hash,
                // Derived from the complete object, spliced in by
                // `convert_v1alpha1_to_v1` when the input is complete, left
                // absent for partial objects so a field manager cannot gain
                // ownership of a field it never set.
                requested_rollout_hash: None,
                conditions,
                extra,
            }
        }
    }

    impl From<super::v1alpha1::PartialMaterialize> for PartialMaterialize {
        fn from(mz: super::v1alpha1::PartialMaterialize) -> Self {
            let super::v1alpha1::PartialMaterialize {
                api_version: _,
                kind,
                metadata,
                spec,
                status,
                extra,
            } = mz;
            Self {
                api_version: Some("materialize.cloud/v1".to_owned()),
                kind,
                metadata,
                spec: spec.map(Into::into),
                status: status.map(Into::into),
                extra,
            }
        }
    }
}

/// The nil UUID rendered the way it appears in JSON-encoded specs.
const NIL_UUID_STR: &str = "00000000-0000-0000-0000-000000000000";

/// One field of a partial object: absent (`None`), explicit null
/// (`Some(None)`), or a value (`Some(Some(_))`).
///
/// Values are untyped [`serde_json::Value`]s because conversion must be a
/// total function over anything schema-shaped, including values that do not
/// validate. The type system is used for field *names*: the partial mirror
/// structs convert via exhaustive destructures, so adding a field to a spec
/// without deciding its conversion does not compile.
pub type PartialField = Option<Option<serde_json::Value>>;

/// Serde adapter for [`PartialField`] distinguishing explicit null from an
/// absent field. Use with `#[serde(default, with = "double_option",
/// skip_serializing_if = "Option::is_none")]`.
mod double_option {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn deserialize<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
    where
        T: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(Some)
    }

    pub fn serialize<T, S>(value: &Option<Option<T>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: Serialize,
        S: Serializer,
    {
        match value {
            Some(inner) => inner.serialize(serializer),
            // Unreachable under skip_serializing_if = "Option::is_none".
            None => serializer.serialize_none(),
        }
    }
}

/// A [`PartialField`] holding a value that is always present in the typed
/// struct.
fn present<T: Serialize>(value: T) -> PartialField {
    Some(Some(
        serde_json::to_value(value).expect("CRD field serializes to JSON"),
    ))
}

/// A [`PartialField`] from a typed optional field, absent when `None`.
fn present_opt<T: Serialize>(value: Option<T>) -> PartialField {
    value.map(|value| Some(serde_json::to_value(value).expect("CRD field serializes to JSON")))
}

/// Merges a v1alpha1 `environmentdIamRoleArn` value into the
/// `serviceAccountAnnotations` map as `eks.amazonaws.com/role-arn`, matching
/// the typed conversion. An existing annotation wins. Annotations that are
/// not an object pass through untouched.
fn merge_environmentd_iam_role_arn(
    annotations: PartialField,
    role_arn: PartialField,
) -> PartialField {
    let Some(Some(role_arn)) = role_arn else {
        return annotations;
    };
    let mut map = match annotations {
        Some(Some(serde_json::Value::Object(map))) => map,
        Some(Some(other)) => return Some(Some(other)),
        Some(None) | None => serde_json::Map::new(),
    };
    map.entry("eks.amazonaws.com/role-arn").or_insert(role_arn);
    Some(Some(serde_json::Value::Object(map)))
}

/// Converts a JSON-encoded v1alpha1 Materialize object to v1, for use by the
/// CRD conversion webhook.
///
/// Output fields are exactly the input fields, mapped field-wise, plus
/// derived fields when the input is complete. Server-side apply round-trips
/// each field manager's owned subset of an object through the conversion
/// webhook when it reconciles managed fields recorded at another version.
/// Those subsets routinely lack required fields, so conversion must accept
/// partial objects, and it must not add fields the input did not carry,
/// since a field manager must not gain ownership of fields it never set.
///
/// Fields derived from the complete spec (`status.requestedRolloutHash`
/// here, `spec.requestRollout` in the other direction) cannot be computed
/// from a subset, so they are spliced in from the typed conversion only when
/// the input deserializes as a complete object. The request carries no
/// indicator of partialness, so a subset that happens to contain every
/// required field also gains the derived fields. That is the irreducible
/// ambiguity, and it is limited to exactly those fields.
pub fn convert_v1alpha1_to_v1(
    value: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let complete = serde_json::from_value::<v1alpha1::Materialize>(value.clone()).ok();
    let partial: v1alpha1::PartialMaterialize = serde_json::from_value(value)?;
    let mut converted = v1::PartialMaterialize::from(partial);
    if let Some(complete) = complete {
        let typed = v1::Materialize::from(complete);
        if let (Some(status), Some(typed_status)) = (converted.status.as_mut(), typed.status) {
            status.requested_rollout_hash = present_opt(typed_status.requested_rollout_hash);
            status.last_completed_rollout_hash =
                present_opt(typed_status.last_completed_rollout_hash);
        }
    }
    Ok(serde_json::to_value(converted)?)
}

/// Converts a JSON-encoded v1 Materialize object to v1alpha1, for use by the
/// CRD conversion webhook.
///
/// See [`convert_v1alpha1_to_v1`] for the conversion contract. In this
/// direction the derived fields are `spec.requestRollout` and
/// `status.lastCompletedRolloutRequest`, both deterministic UUIDs derived
/// from rollout hashes of the complete object, and `status.resourcesHash`,
/// which has no v1 counterpart but is required by the v1alpha1 schema
/// whenever a status is present, so complete conversions must carry the
/// typed conversion's placeholder to stay valid at the storage version.
pub fn convert_v1_to_v1alpha1(
    value: serde_json::Value,
) -> Result<serde_json::Value, anyhow::Error> {
    let complete = serde_json::from_value::<v1::Materialize>(value.clone()).ok();
    let partial: v1::PartialMaterialize = serde_json::from_value(value)?;
    let mut converted = v1alpha1::PartialMaterialize::from(partial);
    if let Some(complete) = complete {
        let typed = v1alpha1::Materialize::from(complete);
        if let Some(spec) = converted.spec.as_mut() {
            spec.request_rollout = present(typed.spec.request_rollout);
        }
        if let (Some(status), Some(typed_status)) = (converted.status.as_mut(), typed.status) {
            status.last_completed_rollout_request =
                present(typed_status.last_completed_rollout_request);
            status.resources_hash = present(typed_status.resources_hash);
        }
    }
    Ok(serde_json::to_value(converted)?)
}

fn parse_image_ref(image_ref: &str) -> Option<Version> {
    image_ref
        .rsplit_once(':')
        .and_then(|(_repo, tag)| tag.strip_prefix('v'))
        .and_then(|tag| {
            // To work around Docker tag restrictions, build metadata in
            // a Docker tag is delimited by `--` rather than the SemVer
            // `+` delimiter. So we need to swap the delimiter back to
            // `+` before parsing it as SemVer.
            let tag = tag.replace("--", "+");
            Version::parse(&tag).ok()
        })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
    use k8s_openapi::jiff::Timestamp;
    use kube::core::ObjectMeta;
    use semver::Version;

    use super::v1alpha1::{Materialize, MaterializeSpec, MaterializeStatus};
    use super::{DEFAULT_ROLLOUT_REQUEST_TIMEOUT, FORCE_ROLLOUT_ANNOTATION, RolloutRequestTimeout};

    #[mz_ore::test]
    fn force_rollout_annotation_forces_new_generation() {
        // The force-rollout annotation must feed into both the v1 rollout
        // hash (so that a rollout is requested) and the v1alpha1 force
        // rollout value stamped onto the generated statefulset (so that the
        // requested rollout actually creates a new generation rather than
        // completing as a no-op).
        let mut mz = super::v1::Materialize {
            spec: super::v1::MaterializeSpec {
                environmentd_image_ref: "materialize/environmentd:v26.0.0".to_owned(),
                ..Default::default()
            },
            metadata: ObjectMeta::default(),
            status: None,
        };
        let hash_without_annotation = mz.generate_rollout_hash();
        let force_without_annotation = Materialize::from(mz.clone()).force_rollout_value();

        mz.metadata.annotations = Some(std::collections::BTreeMap::from_iter([(
            FORCE_ROLLOUT_ANNOTATION.to_owned(),
            "a4b56cbb-a13e-4f95-8a9d-425c9ba28576".to_owned(),
        )]));
        assert_ne!(mz.generate_rollout_hash(), hash_without_annotation);
        assert_ne!(
            Materialize::from(mz.clone()).force_rollout_value(),
            force_without_annotation
        );

        // Changing the annotation's value changes both again.
        let hash = mz.generate_rollout_hash();
        let force = Materialize::from(mz.clone()).force_rollout_value();
        mz.metadata.annotations.as_mut().unwrap().insert(
            FORCE_ROLLOUT_ANNOTATION.to_owned(),
            "3f61bf8d-0714-462c-8b3b-3d9a68d0bcba".to_owned(),
        );
        assert_ne!(mz.generate_rollout_hash(), hash);
        assert_ne!(Materialize::from(mz.clone()).force_rollout_value(), force);
    }

    #[mz_ore::test]
    fn meets_minimum_version() {
        let mut mz = Materialize {
            spec: MaterializeSpec {
                environmentd_image_ref:
                    "materialize/environmentd:devel-47116c24b8d0df33d3f60a9ee476aa8d7bce5953"
                        .to_owned(),
                ..Default::default()
            },
            metadata: ObjectMeta {
                ..Default::default()
            },
            status: None,
        };

        // true cases
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.34.0".to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.35.0".to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.34.3".to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd@41af286dc0b172ed2f1ca934fd2278de4a1192302ffa07087cea2682e7d372e3".to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "my.private.registry:5000:v0.34.3".to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.asdf.0".to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref =
            "materialize/environmentd:v0.146.0-dev.0--pr.g5a05a9e4ba873be8adaa528644aaae6e4c7cd29b"
                .to_owned();
        assert!(mz.meets_minimum_version(&Version::parse("0.146.0-dev.0").unwrap()));

        // false cases
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.34.0-dev".to_owned();
        assert!(!mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.33.0".to_owned();
        assert!(!mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
        mz.spec.environmentd_image_ref = "materialize/environmentd:v0.34.0".to_owned();
        assert!(!mz.meets_minimum_version(&Version::parse("1.0.0").unwrap()));
        mz.spec.environmentd_image_ref = "my.private.registry:5000:v0.33.3".to_owned();
        assert!(!mz.meets_minimum_version(&Version::parse("0.34.0").unwrap()));
    }

    #[mz_ore::test]
    fn within_upgrade_window() {
        let mut mz = Materialize {
            spec: MaterializeSpec {
                environmentd_image_ref: "materialize/environmentd:v26.0.0".to_owned(),
                ..Default::default()
            },
            metadata: ObjectMeta {
                ..Default::default()
            },
            status: Some(MaterializeStatus {
                last_completed_rollout_environmentd_image_ref: Some(
                    "materialize/environmentd:v26.0.0".to_owned(),
                ),
                ..Default::default()
            }),
        };

        // Pass: upgrading from 26.0.0 to 27.7.3 (within 1 major version)
        mz.spec.environmentd_image_ref = "materialize/environmentd:v27.7.3".to_owned();
        assert!(mz.within_upgrade_window());

        // Pass: upgrading from 26.0.0 to 27.7.8-dev.0 (within 1 major version, pre-release)
        mz.spec.environmentd_image_ref = "materialize/environmentd:v27.7.8-dev.0".to_owned();
        assert!(mz.within_upgrade_window());

        // Fail: upgrading from 26.0.0 to 28.0.1 (more than 1 major version)
        mz.spec.environmentd_image_ref = "materialize/environmentd:v28.0.1".to_owned();
        assert!(!mz.within_upgrade_window());

        // Pass: upgrading from 26.0.0 to 28.0.1.not_a_valid_version (invalid version, defaults to true)
        mz.spec.environmentd_image_ref =
            "materialize/environmentd:v28.0.1.not_a_valid_version".to_owned();
        assert!(mz.within_upgrade_window());

        // Pass: upgrading from 0.164.0 to 26.1.0 (self managed 25.2 to 26.0)
        mz.status
            .as_mut()
            .unwrap()
            .last_completed_rollout_environmentd_image_ref =
            Some("materialize/environmentd:v0.147.20".to_owned());
        mz.spec.environmentd_image_ref = "materialize/environmentd:v26.1.0".to_owned();
        assert!(mz.within_upgrade_window());

        // Pass: upgrading from 26.11.0-dev.0+b to 26.11.0-dev.0+a (same major.minor.patch.prerelease, different build metadata)
        mz.status
            .as_mut()
            .unwrap()
            .last_completed_rollout_environmentd_image_ref =
            Some("materialize/environmentd:v26.11.0-dev.0+b".to_owned());
        mz.spec.environmentd_image_ref = "materialize/environmentd:v26.11.0-dev.0+a".to_owned();
        assert!(mz.within_upgrade_window());
    }

    #[mz_ore::test]
    fn is_valid_upgrade_version() {
        let success_tests = [
            (Version::new(0, 83, 0), Version::new(0, 83, 0)),
            (Version::new(0, 83, 0), Version::new(0, 84, 0)),
            (Version::new(0, 9, 0), Version::new(0, 10, 0)),
            (Version::new(0, 99, 0), Version::new(0, 100, 0)),
            (Version::new(0, 83, 0), Version::new(0, 83, 1)),
            (Version::new(0, 83, 0), Version::new(0, 83, 2)),
            (Version::new(0, 83, 2), Version::new(0, 83, 10)),
            // 0.147.20 to 26.0.0 represents the Self Managed 25.2 to 26.0 upgrade
            (Version::new(0, 147, 20), Version::new(26, 0, 0)),
            (Version::new(0, 164, 0), Version::new(26, 0, 0)),
            (Version::new(26, 0, 0), Version::new(26, 1, 0)),
            (Version::new(26, 5, 3), Version::new(26, 10, 0)),
            (Version::new(0, 130, 0), Version::new(0, 147, 0)),
        ];
        for (active_version, next_version) in success_tests {
            assert!(
                Materialize::is_valid_upgrade_version(&active_version, &next_version),
                "v{active_version} can upgrade to v{next_version}"
            );
        }

        let failure_tests = [
            (Version::new(0, 83, 0), Version::new(0, 82, 0)),
            (Version::new(0, 83, 3), Version::new(0, 83, 2)),
            (Version::new(0, 83, 3), Version::new(1, 83, 3)),
            (Version::new(0, 83, 0), Version::new(0, 85, 0)),
            (Version::new(26, 0, 0), Version::new(28, 0, 0)),
            (Version::new(0, 130, 0), Version::new(26, 1, 0)),
            // Disallow anything before 0.147.20 to upgrade
            (Version::new(0, 147, 1), Version::new(26, 0, 0)),
            // Disallow anything between 0.148.0 and 0.164.0 to upgrade
            (Version::new(0, 148, 0), Version::new(26, 0, 0)),
        ];
        for (active_version, next_version) in failure_tests {
            assert!(
                !Materialize::is_valid_upgrade_version(&active_version, &next_version),
                "v{active_version} can't upgrade to v{next_version}"
            );
        }
    }

    #[mz_ore::test]
    fn rollout_request_timeout() {
        let mz_with = |timeout: &str| Materialize {
            spec: MaterializeSpec {
                rollout_request_timeout: RolloutRequestTimeout(timeout.to_owned()),
                ..Default::default()
            },
            metadata: ObjectMeta::default(),
            status: None,
        };

        // The default const is a valid duration and resolves to 24h.
        let default = humantime::parse_duration(DEFAULT_ROLLOUT_REQUEST_TIMEOUT).unwrap();
        assert_eq!(default, Duration::from_secs(24 * 60 * 60));

        // The field's Default (used by `MaterializeSpec::default()` and serde's
        // `#[serde(default)]`) is the 24h default, with no empty intermediate.
        assert_eq!(
            RolloutRequestTimeout::default().0,
            DEFAULT_ROLLOUT_REQUEST_TIMEOUT
        );
        assert_eq!(
            Materialize {
                spec: MaterializeSpec::default(),
                metadata: ObjectMeta::default(),
                status: None,
            }
            .rollout_request_timeout(),
            default
        );

        // Parseable values are honored.
        assert_eq!(
            mz_with("1h").rollout_request_timeout(),
            Duration::from_secs(60 * 60)
        );
        assert_eq!(
            mz_with("90m").rollout_request_timeout(),
            Duration::from_secs(90 * 60)
        );
        assert_eq!(
            mz_with("1h 30m").rollout_request_timeout(),
            Duration::from_secs(90 * 60)
        );
        // Unparseable values fall back to the default.
        assert_eq!(mz_with("not a duration").rollout_request_timeout(), default);
    }

    #[mz_ore::test]
    fn rollout_request_timeout_schema_default() {
        // The default must be surfaced in the generated CRD's OpenAPI schema
        // (not just in the Rust helper), so the Kubernetes API server defaults
        // omitted fields and `kubectl explain` shows it.
        let crd = serde_json::to_value(<Materialize as kube::CustomResourceExt>::crd())
            .expect("CRD serializes");
        let default = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["rolloutRequestTimeout"]["default"];
        assert_eq!(
            default,
            &serde_json::json!(DEFAULT_ROLLOUT_REQUEST_TIMEOUT),
            "rolloutRequestTimeout schema default missing/wrong in generated CRD",
        );
    }

    #[mz_ore::test]
    fn rollout_in_progress_since() {
        let now = Timestamp::now();
        let condition = |type_: &str, status: &str| Condition {
            type_: type_.to_owned(),
            status: status.to_owned(),
            last_transition_time: Time(now),
            message: String::new(),
            observed_generation: None,
            reason: "Test".to_owned(),
        };
        let mz_with = |conditions: Vec<Condition>| Materialize {
            spec: MaterializeSpec::default(),
            metadata: ObjectMeta::default(),
            status: Some(MaterializeStatus {
                conditions,
                ..Default::default()
            }),
        };

        // No status at all.
        let mz = Materialize {
            spec: MaterializeSpec::default(),
            metadata: ObjectMeta::default(),
            status: None,
        };
        assert_eq!(mz.rollout_in_progress_since(), None);

        // A timeout-eligible rollout in progress is signalled by an `Unknown`
        // `UpToDate` condition (the Applying and ReadyToPromote phases).
        assert_eq!(
            mz_with(vec![condition("UpToDate", "Unknown")]).rollout_in_progress_since(),
            Some(now)
        );

        // The `Promoting` phase is also `Unknown`, but must NOT be reported:
        // once promoting, the rollout can no longer be cancelled by the
        // timeout.
        assert_eq!(
            mz_with(vec![Condition {
                reason: "Promoting".to_owned(),
                ..condition("UpToDate", "Unknown")
            }])
            .rollout_in_progress_since(),
            None
        );

        // A settled rollout (True/False) is not in progress.
        assert_eq!(
            mz_with(vec![condition("UpToDate", "True")]).rollout_in_progress_since(),
            None
        );
        assert_eq!(
            mz_with(vec![condition("UpToDate", "False")]).rollout_in_progress_since(),
            None
        );
    }

    #[mz_ore::test]
    fn up_to_date_transition_time() {
        // Two distinct, fixed instants so we can tell "carried the old
        // timestamp" apart from "reset to now".
        let stored = Timestamp::from_second(1_000).unwrap();
        let now = Timestamp::from_second(2_000).unwrap();

        let condition = |status: &str| Condition {
            type_: "UpToDate".to_owned(),
            status: status.to_owned(),
            last_transition_time: Time(stored),
            message: String::new(),
            observed_generation: None,
            reason: "Test".to_owned(),
        };
        let mz_with = |conditions: Vec<Condition>| Materialize {
            spec: MaterializeSpec::default(),
            metadata: ObjectMeta::default(),
            status: Some(MaterializeStatus {
                conditions,
                ..Default::default()
            }),
        };

        // No prior condition: use `now`.
        let mz = Materialize {
            spec: MaterializeSpec::default(),
            metadata: ObjectMeta::default(),
            status: None,
        };
        assert_eq!(mz.up_to_date_transition_time("Unknown", now), now);

        // Same status as the prior condition: carry its timestamp forward, so
        // consecutive same-status phases (Applying -> ReadyToPromote) share one
        // timer.
        assert_eq!(
            mz_with(vec![condition("Unknown")]).up_to_date_transition_time("Unknown", now),
            stored
        );

        // Status changed: reset to `now`.
        assert_eq!(
            mz_with(vec![condition("Unknown")]).up_to_date_transition_time("True", now),
            now
        );
    }

    #[mz_ore::test]
    fn active_environmentd_image_ref() {
        const OLD: &str = "materialize/environmentd:v26.0.0";
        const NEW: &str = "materialize/environmentd:v27.0.0";

        let mz_with = |spec_image: &str, status: Option<MaterializeStatus>| Materialize {
            spec: MaterializeSpec {
                environmentd_image_ref: spec_image.to_owned(),
                ..Default::default()
            },
            metadata: ObjectMeta::default(),
            status,
        };

        // No status yet (pre-initial-reconcile): fall back to spec.
        let mz = mz_with(NEW, None);
        assert_eq!(mz.active_environmentd_image_ref(), NEW);

        // Status present but last_completed_rollout_environmentd_image_ref
        // unset (e.g. resource upgraded from older orchestratord that didn't
        // populate the field): fall back to spec.
        let mz = mz_with(
            NEW,
            Some(MaterializeStatus {
                last_completed_rollout_environmentd_image_ref: None,
                ..Default::default()
            }),
        );
        assert_eq!(mz.active_environmentd_image_ref(), NEW);

        // Steady state: spec image == last completed image. Either source is
        // fine; the method must return that image.
        let mz = mz_with(
            NEW,
            Some(MaterializeStatus {
                last_completed_rollout_environmentd_image_ref: Some(NEW.to_owned()),
                ..Default::default()
            }),
        );
        assert_eq!(mz.active_environmentd_image_ref(), NEW);

        // DEP-42 / mid-rollout: spec image == NEW but last_completed_* still
        // holds OLD — either because the user canceled the rollout by
        // reverting only requestRollout, or because the new generation has
        // not yet been promoted. The active environmentd is still OLD, so
        // downstream resources must track OLD. Without this method,
        // balancerd would inherit the spec's NEW image while environmentd
        // still runs OLD, leaving balancerd pods skewed from the running
        // env.
        let mz = mz_with(
            NEW,
            Some(MaterializeStatus {
                last_completed_rollout_environmentd_image_ref: Some(OLD.to_owned()),
                ..Default::default()
            }),
        );
        assert_eq!(mz.active_environmentd_image_ref(), OLD);
    }

    // Server-side apply round-trips each field manager's owned subset of an
    // object through the conversion webhook. Such subsets lack required
    // fields, so conversion must map them field-wise instead of erroring.
    #[mz_ore::test]
    fn convert_partial_v1alpha1_to_v1() {
        let subset = serde_json::json!({
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {"name": "mz", "namespace": "materialize"},
            "spec": {
                "environmentdIamRoleArn": "arn:aws:iam::123456789012:role/mz",
                "requestRollout": "1e6ef7bc-bff2-4bd4-90dc-eda5697bd0e6",
                "inPlaceRollout": false,
                "forcePromote": "",
                "serviceAccountLabels": {"team": "data"},
            },
            "status": {
                "activeGeneration": 3,
                "lastCompletedRolloutRequest": "1e6ef7bc-bff2-4bd4-90dc-eda5697bd0e6",
                "resourcesHash": "abc123",
            },
        });
        let converted = super::convert_v1alpha1_to_v1(subset).unwrap();
        assert_eq!(converted["apiVersion"], "materialize.cloud/v1");
        assert_eq!(converted["kind"], "Materialize");
        assert_eq!(converted["metadata"]["name"], "mz");
        let spec = converted["spec"].as_object().unwrap();
        assert!(!spec.contains_key("requestRollout"));
        assert!(!spec.contains_key("inPlaceRollout"));
        assert!(!spec.contains_key("forcePromote"));
        assert!(!spec.contains_key("environmentdIamRoleArn"));
        assert_eq!(
            spec["serviceAccountAnnotations"]["eks.amazonaws.com/role-arn"],
            "arn:aws:iam::123456789012:role/mz"
        );
        assert_eq!(spec["serviceAccountLabels"]["team"], "data");
        let status = converted["status"].as_object().unwrap();
        assert_eq!(status["activeGeneration"], 3);
        assert!(!status.contains_key("lastCompletedRolloutRequest"));
        assert!(!status.contains_key("resourcesHash"));
        assert!(!status.contains_key("requestedRolloutHash"));

        // A meaningful forcePromote value survives the conversion.
        let subset = serde_json::json!({
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {"name": "mz"},
            "spec": {"forcePromote": "1e6ef7bc-bff2-4bd4-90dc-eda5697bd0e6"},
        });
        let converted = super::convert_v1alpha1_to_v1(subset).unwrap();
        assert_eq!(
            converted["spec"]["forcePromote"],
            "1e6ef7bc-bff2-4bd4-90dc-eda5697bd0e6"
        );
    }

    #[mz_ore::test]
    fn convert_partial_v1_to_v1alpha1() {
        let subset = serde_json::json!({
            "apiVersion": "materialize.cloud/v1",
            "kind": "Materialize",
            "metadata": {"name": "mz"},
            "spec": {"environmentdImageRef": "materialize/environmentd:v26.0.0"},
            "status": {"requestedRolloutHash": "abc123", "activeGeneration": 1},
        });
        let converted = super::convert_v1_to_v1alpha1(subset).unwrap();
        assert_eq!(converted["apiVersion"], "materialize.cloud/v1alpha1");
        assert_eq!(
            converted["spec"]["environmentdImageRef"],
            "materialize/environmentd:v26.0.0"
        );
        let status = converted["status"].as_object().unwrap();
        assert_eq!(status["activeGeneration"], 1);
        assert!(!status.contains_key("requestedRolloutHash"));
        assert!(!status.contains_key("resourcesHash"));
        // Derived fields must not be invented for partial objects, a field
        // manager must not gain ownership of fields it never set.
        assert!(
            !converted["spec"]
                .as_object()
                .unwrap()
                .contains_key("requestRollout")
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `sha256_compress` on OS `linux`
    fn convert_full_v1alpha1_to_v1_derives_fields() {
        let mz = Materialize {
            metadata: ObjectMeta {
                name: Some("mz".to_owned()),
                namespace: Some("materialize".to_owned()),
                ..Default::default()
            },
            spec: MaterializeSpec {
                environmentd_image_ref: "materialize/environmentd:v26.0.0".to_owned(),
                backend_secret_name: "mz-backend".to_owned(),
                ..Default::default()
            },
            status: Some(MaterializeStatus::default()),
        };
        let value = serde_json::to_value(&mz).unwrap();
        let converted = super::convert_v1alpha1_to_v1(value).unwrap();
        assert_eq!(converted["apiVersion"], "materialize.cloud/v1");
        // Complete objects take the typed conversion, which computes fields
        // derived from the whole spec.
        assert!(converted["status"]["requestedRolloutHash"].is_string());
    }

    #[mz_ore::test]
    fn convert_rejects_non_objects() {
        assert!(super::convert_v1alpha1_to_v1(serde_json::json!("not an object")).is_err());
        assert!(super::convert_v1_to_v1alpha1(serde_json::json!(42)).is_err());
        assert!(
            super::convert_v1alpha1_to_v1(serde_json::json!({"spec": "not an object"})).is_err()
        );
    }

    #[mz_ore::test]
    fn convert_preserves_null_vs_absent() {
        let subset = serde_json::json!({
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {"name": "mz"},
            "spec": {
                "environmentdExtraArgs": null,
                "backendSecretName": "mz-backend",
            },
        });
        let converted = super::convert_v1alpha1_to_v1(subset).unwrap();
        let spec = converted["spec"].as_object().unwrap();
        assert!(spec.contains_key("environmentdExtraArgs"));
        assert!(spec["environmentdExtraArgs"].is_null());
        assert!(!spec.contains_key("consoleReplicas"));
    }

    // A subset containing every required field deserializes as a complete
    // object and takes the derived-field splice, but the conversion must
    // still not add defaults or nulls for fields the subset did not carry.
    // Field managers must not gain ownership of fields they never set.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `sha256_compress` on OS `linux`
    fn convert_faithful_output_for_required_field_subsets() {
        let subset = serde_json::json!({
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {"name": "mz"},
            "spec": {
                "environmentdImageRef": "materialize/environmentd:v26.0.0",
                "backendSecretName": "mz-backend",
            },
        });
        let converted = super::convert_v1alpha1_to_v1(subset).unwrap();
        let spec = converted["spec"].as_object().unwrap();
        let mut keys: Vec<_> = spec.keys().cloned().collect();
        keys.sort();
        assert_eq!(keys, ["backendSecretName", "environmentdImageRef"]);
    }

    #[mz_ore::test]
    fn convert_passes_unknown_fields_through() {
        let subset = serde_json::json!({
            "apiVersion": "materialize.cloud/v1alpha1",
            "kind": "Materialize",
            "metadata": {"name": "mz"},
            "spec": {"someFutureField": {"a": 1}},
            "someTopLevelField": true,
        });
        let converted = super::convert_v1alpha1_to_v1(subset).unwrap();
        assert_eq!(converted["spec"]["someFutureField"]["a"], 1);
        assert_eq!(converted["someTopLevelField"], true);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `sha256_compress` on OS `linux`
    fn convert_full_v1_to_v1alpha1_derives_request_rollout() {
        let mz = super::v1::Materialize {
            metadata: ObjectMeta {
                name: Some("mz".to_owned()),
                namespace: Some("materialize".to_owned()),
                ..Default::default()
            },
            spec: super::v1::MaterializeSpec {
                environmentd_image_ref: "materialize/environmentd:v26.0.0".to_owned(),
                backend_secret_name: "mz-backend".to_owned(),
                ..Default::default()
            },
            status: Some(super::v1::MaterializeStatus::default()),
        };
        let expected = Materialize::from(mz.clone()).spec.request_rollout;
        let converted = super::convert_v1_to_v1alpha1(serde_json::to_value(&mz).unwrap()).unwrap();
        assert_eq!(converted["apiVersion"], "materialize.cloud/v1alpha1");
        assert_eq!(
            converted["spec"]["requestRollout"],
            expected.hyphenated().to_string()
        );
        // The status fields required by the v1alpha1 schema but absent from
        // v1 must be spliced in, else the converted object fails validation
        // at the storage version.
        let status = converted["status"].as_object().unwrap();
        assert!(status["resourcesHash"].is_string());
        assert!(status["lastCompletedRolloutRequest"].is_string());
    }

    // The `From<Materialize> for PartialMaterialize` impls are the
    // compile-time guard tying the partial mirrors to the typed structs.
    // Their output must be faithful: unset optional fields stay absent
    // rather than becoming nulls.
    #[mz_ore::test]
    fn partial_mirror_from_typed_omits_unset_fields() {
        let mz = Materialize {
            metadata: ObjectMeta::default(),
            spec: MaterializeSpec {
                environmentd_image_ref: "materialize/environmentd:v26.0.0".to_owned(),
                backend_secret_name: "mz-backend".to_owned(),
                ..Default::default()
            },
            status: None,
        };
        let value = serde_json::to_value(super::v1alpha1::PartialMaterialize::from(mz)).unwrap();
        let spec = value["spec"].as_object().unwrap();
        assert!(!spec.contains_key("balancerdReplicas"));
        for (key, field_value) in spec {
            assert!(!field_value.is_null(), "unexpected null for {key}");
        }
        assert!(!value.as_object().unwrap().contains_key("status"));
    }
}
