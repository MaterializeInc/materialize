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
        apis::meta::v1::{Condition, OwnerReference, Time},
    },
};
use kube::{CustomResource, Resource, ResourceExt, api::ObjectMeta};
use rand::Rng;
use rand::distributions::Uniform;
use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use mz_server_core::listeners::AuthenticatorKind;

use crate::crd::generated::cert_manager::certificates::{
    CertificateIssuerRef, CertificateSecretTemplate,
};

pub const LAST_KNOWN_ACTIVE_GENERATION_ANNOTATION: &str =
    "materialize.cloud/last-known-active-generation";

pub mod v1alpha1 {

    use super::*;

    // This is intentionally a subset of the fields of a Certificate.
    // We do not want customers to configure options that may conflict with
    // things we override or expand in our code.
    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeCertSpec {
        // Additional DNS names the cert will be valid for.
        pub dns_names: Option<Vec<String>>,
        // Duration the certificate will be requested for.
        // Value must be in units accepted by Go time.ParseDuration
        // https://golang.org/pkg/time/#ParseDuration.
        pub duration: Option<String>,
        // Duration before expiration the certificate will be renewed.
        // Value must be in units accepted by Go time.ParseDuration
        // https://golang.org/pkg/time/#ParseDuration.
        pub renew_before: Option<String>,
        // Reference to an Issuer or ClusterIssuer that will generate the certificate.
        pub issuer_ref: Option<CertificateIssuerRef>,
        // Additional annotations and labels to include in the Certificate object.
        pub secret_template: Option<CertificateSecretTemplate>,
    }
    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    pub enum MaterializeRolloutStrategy {
        // Default. Create a new generation of pods, leaving the old generation around until the
        // new ones are ready to take over.
        // This minimizes downtime, and is what almost everyone should use.
        WaitUntilReady,

        // WARNING!!!
        // THIS WILL CAUSE YOUR MATERIALIZE INSTANCE TO BE UNAVAILABLE FOR SOME TIME!!!
        // WARNING!!!
        //
        // Tear down the old generation of pods and promote the new generation of pods immediately,
        // without waiting for the new generation of pods to be ready.
        //
        // This strategy should ONLY be used by customers with physical hardware who do not have
        // enough hardware for the WaitUntilReady strategy. If you think you want this, please
        // consult with Materialize engineering to discuss your situation.
        ImmediatelyPromoteCausingDowntime,
    }
    impl Default for MaterializeRolloutStrategy {
        fn default() -> Self {
            Self::WaitUntilReady
        }
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
        // The environmentd image to run
        pub environmentd_image_ref: String,
        // Extra args to pass to the environmentd binary
        pub environmentd_extra_args: Option<Vec<String>>,
        // Extra environment variables to pass to the environmentd binary
        pub environmentd_extra_env: Option<Vec<EnvVar>>,
        // DEPRECATED
        // If running in AWS, override the IAM role to use to give
        // environmentd access to the persist S3 bucket.
        // DEPRECATED
        // Use `service_account_annotations` to set "eks.amazonaws.com/role-arn" instead.
        pub environmentd_iam_role_arn: Option<String>,
        // If running in AWS, override the IAM role to use to support
        // the CREATE CONNECTION feature
        pub environmentd_connection_role_arn: Option<String>,
        // Resource requirements for the environmentd pod
        pub environmentd_resource_requirements: Option<ResourceRequirements>,
        // Amount of disk to allocate, if a storage class is provided
        pub environmentd_scratch_volume_storage_requirement: Option<Quantity>,
        // Resource requirements for the balancerd pod
        pub balancerd_resource_requirements: Option<ResourceRequirements>,
        // Resource requirements for the console pod
        pub console_resource_requirements: Option<ResourceRequirements>,
        // Number of balancerd pods to create
        pub balancerd_replicas: Option<i32>,
        // Number of console pods to create
        pub console_replicas: Option<i32>,

        // Name of the kubernetes service account to use.
        // If not set, we will create one with the same name as this Materialize object.
        pub service_account_name: Option<String>,
        // Annotations to apply to the service account
        //
        // Annotations on service accounts are commonly used by cloud providers for IAM.
        // AWS uses "eks.amazonaws.com/role-arn".
        // Azure uses "azure.workload.identity/client-id", but
        // additionally requires "azure.workload.identity/use": "true" on the pods.
        pub service_account_annotations: Option<BTreeMap<String, String>>,
        // Labels to apply to the service account
        pub service_account_labels: Option<BTreeMap<String, String>>,
        // Annotations to apply to the pods
        pub pod_annotations: Option<BTreeMap<String, String>>,
        // Labels to apply to the pods
        pub pod_labels: Option<BTreeMap<String, String>>,

        // When changes are made to the environmentd resources (either via
        // modifying fields in the spec here or by deploying a new
        // orchestratord version which changes how resources are generated),
        // existing environmentd processes won't be automatically restarted.
        // In order to trigger a restart, the request_rollout field should be
        // set to a new (random) value. Once the rollout completes, the value
        // of status.last_completed_rollout_request will be set to this value
        // to indicate completion.
        //
        // Defaults to a random value in order to ensure that the first
        // generation rollout is automatically triggered.
        #[serde(default)]
        pub request_rollout: Uuid,
        // If force_promote is set to the same value as request_rollout, the
        // current rollout will skip waiting for clusters in the new
        // generation to rehydrate before promoting the new environmentd to
        // leader.
        #[serde(default)]
        pub force_promote: Uuid,
        // This value will be written to an annotation in the generated
        // environmentd statefulset, in order to force the controller to
        // detect the generated resources as changed even if no other changes
        // happened. This can be used to force a rollout to a new generation
        // even without making any meaningful changes.
        #[serde(default)]
        pub force_rollout: Uuid,
        // Deprecated and ignored. Use rollout_strategy instead.
        #[serde(default)]
        pub in_place_rollout: bool,
        // Rollout strategy to use when upgrading this Materialize instance.
        #[serde(default)]
        pub rollout_strategy: MaterializeRolloutStrategy,
        // The name of a secret containing metadata_backend_url and persist_backend_url.
        // It may also contain external_login_password_mz_system, which will be used as
        // the password for the mz_system user if authenticator_kind is Password.
        pub backend_secret_name: String,
        // How to authenticate with Materialize. Valid options are Password and None.
        // If set to Password, the backend secret must contain external_login_password_mz_system.
        #[serde(default)]
        pub authenticator_kind: AuthenticatorKind,
        // Whether to enable role based access control. Defaults to false.
        #[serde(default)]
        pub enable_rbac: bool,

        // The value used by environmentd (via the --environment-id flag) to
        // uniquely identify this instance. Must be globally unique, and
        // is required if a license key is not provided.
        // NOTE: This value MUST NOT be changed in an existing instance,
        // since it affects things like the way data is stored in the persist
        // backend.
        #[serde(default)]
        pub environment_id: Uuid,

        // The configuration for generating an x509 certificate using cert-manager for balancerd
        // to present to incoming connections.
        // The dns_names and issuer_ref fields are required.
        pub balancerd_external_certificate_spec: Option<MaterializeCertSpec>,
        // The configuration for generating an x509 certificate using cert-manager for the console
        // to present to incoming connections.
        // The dns_names and issuer_ref fields are required.
        // Not yet implemented.
        pub console_external_certificate_spec: Option<MaterializeCertSpec>,
        // The cert-manager Issuer or ClusterIssuer to use for database internal communication.
        // The issuer_ref field is required.
        // This currently is only used for environmentd, but will eventually support clusterd.
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

        pub fn default_labels(&self) -> BTreeMap<String, String> {
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

        pub fn managed_resource_meta(&self, name: String) -> ObjectMeta {
            ObjectMeta {
                namespace: Some(self.namespace()),
                name: Some(name),
                labels: Some(self.default_labels()),
                owner_references: Some(vec![owner_reference(self)]),
                ..Default::default()
            }
        }

        pub fn status(&self) -> MaterializeStatus {
            self.status.clone().unwrap_or_else(|| {
                let mut status = MaterializeStatus::default();
                // DNS-1035 names are supposed to be case insensitive,
                // so we define our own character set, rather than use the
                // built-in Alphanumeric distribution from rand, which
                // includes both upper and lowercase letters.
                const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
                status.resource_id = rand::thread_rng()
                    .sample_iter(Uniform::new(0, CHARSET.len()))
                    .take(10)
                    .map(|i| char::from(CHARSET[i]))
                    .collect();

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

                status
            })
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct MaterializeStatus {
        pub resource_id: String,
        pub active_generation: u64,
        pub last_completed_rollout_request: Uuid,
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

fn owner_reference<T: Resource<DynamicType = ()>>(t: &T) -> OwnerReference {
    OwnerReference {
        api_version: T::api_version(&()).to_string(),
        kind: T::kind(&()).to_string(),
        name: t.name_unchecked(),
        uid: t.uid().unwrap(),
        block_owner_deletion: Some(true),
        ..Default::default()
    }
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
}
