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
    api::core::v1::ResourceRequirements,
    apimachinery::pkg::apis::meta::v1::{Condition, OwnerReference, Time},
};
use kube::{api::ObjectMeta, CustomResource, Resource, ResourceExt};

use schemars::JsonSchema;
use semver::Version;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const LAST_KNOWN_ACTIVE_GENERATION_ANNOTATION: &str =
    "materialize.cloud/last-known-active-generation";

pub mod v1alpha1 {
    use super::*;

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
        printcolumn = r#"{"name": "ImageRef", "type": "string", "description": "Reference to the Docker image.", "jsonPath": ".spec.imageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "UpToDate", "type": "string", "description": "Whether the spec has been applied", "jsonPath": ".status.conditions[?(@.type==\"UpToDate\")].status", "priority": 1}"#
    )]
    pub struct MaterializeSpec {
        // The environmentd image to run
        pub environmentd_image_ref: String,
        // Extra args to pass to the environmentd binary
        pub environmentd_extra_args: Option<Vec<String>>,
        // Resource requirements for the environmentd pod
        pub environmentd_resource_requirements: Option<ResourceRequirements>,
        // Resource requirements for the balancerd pod
        pub balancerd_resource_requirements: Option<ResourceRequirements>,

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
        #[serde(default = "Uuid::new_v4")]
        pub request_rollout: Uuid,
        // This value will be written to an annotation in the generated
        // environmentd statefulset, in order to force the controller to
        // detect the generated resources as changed even if no other changes
        // happened. This can be used to force a rollout to a new generation
        // even without making any meaningful changes.
        #[serde(default)]
        pub force_rollout: Uuid,
        // If false (the default), orchestratord will use the leader
        // promotion codepath to minimize downtime during rollouts. If true,
        // it will just kill the environmentd pod directly.
        #[serde(default)]
        pub in_place_rollout: bool,
    }

    impl Materialize {
        pub fn backend_secret_name(&self) -> String {
            format!("{}-materialize-backend", self.name_unchecked())
        }

        pub fn namespace(&self) -> String {
            self.meta().namespace.clone().unwrap()
        }

        pub fn service_account_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn role_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn role_binding_name(&self) -> String {
            self.name_unchecked()
        }

        pub fn environmentd_statefulset_name(&self, generation: u64) -> String {
            format!("{}-environmentd-{generation}", self.name_unchecked())
        }

        pub fn environmentd_service_name(&self) -> String {
            format!("{}-environmentd", self.name_unchecked())
        }

        pub fn environmentd_generation_service_name(&self, generation: u64) -> String {
            format!("{}-environmentd-{generation}", self.name_unchecked())
        }

        pub fn balancerd_deployment_name(&self) -> String {
            format!("{}-balancerd", self.name_unchecked())
        }

        pub fn balancerd_service_name(&self) -> String {
            format!("{}-balancerd", self.name_unchecked())
        }

        pub fn persist_pubsub_service_name(&self, generation: u64) -> String {
            format!("{}-persist-pubsub-{generation}", self.name_unchecked())
        }

        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("{}-{}", self.name_unchecked(), suffix)
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
            ])
        }

        pub fn environment_id(&self, cloud_provider: &str, region: &str) -> String {
            format!("{}-{}-{}-0", cloud_provider, region, self.name_unchecked())
        }

        pub fn requested_reconciliation_id(&self) -> Uuid {
            self.spec.request_rollout
        }

        pub fn in_place_rollout(&self) -> bool {
            self.spec.in_place_rollout
        }

        pub fn rollout_requested(&self) -> bool {
            self.requested_reconciliation_id()
                != self
                    .status
                    .as_ref()
                    .map_or_else(Uuid::nil, |status| status.last_completed_rollout_request)
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
                None => true,
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
