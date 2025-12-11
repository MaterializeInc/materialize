// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use k8s_openapi::{
    api::core::v1::{EnvVar, ResourceRequirements},
    apimachinery::pkg::{
        api::resource::Quantity,
        apis::meta::v1::{Condition, Time},
    },
};
use kube::{CustomResource, Resource, ResourceExt};
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::crd::{ManagedResource, MaterializeCertSpec, new_resource_id};
use mz_server_core::listeners::AuthenticatorKind;

pub const LAST_KNOWN_ACTIVE_GENERATION_ANNOTATION: &str =
    "materialize.cloud/last-known-active-generation";

pub mod v1alpha1 {
    use super::*;

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
        /// Users can promote the new generation at any time, even if the new generation pods are
        /// not fully caught up, by setting `forcePromote` to the same value as `requestRollout` in
        /// the Materialize spec.
        ///
        /// {{<warning>}}
        /// Do not leave new generations unpromoted indefinitely.
        ///
        /// The new generation keeps open read holds which prevent compaction. Once promoted or
        /// cancelled, those read holds are released. If left unpromoted for an extended time, this
        /// data can build up, and can cause extreme deletion load on the metadata backend database
        /// when finally promoted or cancelled.
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

    #[derive(
        CustomResource, Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema,
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
        printcolumn = r#"{"name": "ImageRef", "type": "string", "description": "Reference to the Docker image.", "jsonPath": ".spec.environmentdImageRef", "priority": 1}"#,
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
        pub force_promote: Uuid,
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

        pub fn rollout_requested(&self) -> bool {
            self.requested_reconciliation_id()
                != self
                    .status
                    .as_ref()
                    .map_or_else(Uuid::nil, |status| status.last_completed_rollout_request)
        }

        pub fn set_force_promote(&mut self) {
            self.spec.force_promote = self.spec.request_rollout;
        }

        pub fn should_force_promote(&self) -> bool {
            self.spec.force_promote == self.spec.request_rollout
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
                Some(version) => &version >= minimum,
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
            if next_version < active_version {
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
        pub conditions: Vec<Condition>,
    }

    impl MaterializeStatus {
        pub fn needs_update(&self, other: &Self) -> bool {
            let now = chrono::offset::Utc::now();
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
    }
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
    use kube::core::ObjectMeta;
    use semver::Version;

    use super::v1alpha1::{Materialize, MaterializeSpec};

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
        use super::v1alpha1::MaterializeStatus;

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
}
