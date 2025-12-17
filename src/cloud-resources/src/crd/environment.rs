// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use kube::{CustomResource, Resource, ResourceExt};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ManagedResource, MaterializeCertSpec, new_resource_id};

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
        kind = "Environment",
        singular = "environment",
        plural = "environments",
        status = "EnvironmentStatus"
    )]
    pub struct EnvironmentSpec {
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

        /// The cert-manager Issuer or ClusterIssuer to use for database internal communication.
        /// The `issuerRef` field is required.
        /// This currently is only used for environmentd, but will eventually support clusterd.
        /// Not yet implemented.
        pub internal_certificate_spec: Option<MaterializeCertSpec>,

        // This can be set to override the randomly chosen resource id
        pub resource_id: Option<String>,
    }

    impl Environment {
        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mz{}-{}", self.resource_id(), suffix)
        }

        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
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

        pub fn app_name(&self) -> String {
            "environmentd".to_owned()
        }

        pub fn balancerd_app_name(&self) -> String {
            "balancerd".to_owned()
        }

        pub fn certificate_name(&self) -> String {
            self.name_prefixed("environmentd-external")
        }

        pub fn certificate_secret_name(&self) -> String {
            self.name_prefixed("environmentd-tls")
        }

        pub fn service_name(&self) -> String {
            self.name_prefixed("environmentd")
        }

        pub fn service_internal_fqdn(&self) -> String {
            format!(
                "{}.{}.svc.cluster.local",
                self.service_name(),
                self.namespace(),
            )
        }

        pub fn status(&self) -> EnvironmentStatus {
            self.status.clone().unwrap_or_else(|| EnvironmentStatus {
                resource_id: self
                    .spec
                    .resource_id
                    .clone()
                    .unwrap_or_else(new_resource_id),
            })
        }
    }

    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
    pub struct EnvironmentStatus {
        /// Resource identifier used as a name prefix to avoid pod name collisions.
        pub resource_id: String,
    }

    impl ManagedResource for Environment {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/mz-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
                (
                    "materialize.cloud/app".to_owned(),
                    "environmentd".to_owned(),
                ),
            ])
        }
    }
}
