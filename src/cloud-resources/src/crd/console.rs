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
    api::core::v1::ResourceRequirements, apimachinery::pkg::apis::meta::v1::Condition,
};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ManagedResource, MaterializeCertSpec, new_resource_id};
use mz_server_core::listeners::AuthenticatorKind;

pub mod v1alpha1 {
    use super::*;

    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub enum HttpConnectionScheme {
        #[default]
        Http,
        Https,
    }

    #[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct BalancerdRef {
        /// The service name for the balancerd service to connect to
        pub service_name: String,
        /// The namespace the balancerd service runs in
        pub namespace: String,
        /// Whether to use HTTP or HTTPS to communicate with Materialize.
        pub scheme: HttpConnectionScheme,
    }

    #[derive(
        CustomResource, Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema,
    )]
    #[serde(rename_all = "camelCase")]
    #[kube(
        namespaced,
        group = "materialize.cloud",
        version = "v1alpha1",
        kind = "Console",
        singular = "console",
        plural = "consoles",
        status = "ConsoleStatus",
        printcolumn = r#"{"name": "ImageRef", "type": "string", "description": "Reference to the Docker image.", "jsonPath": ".spec.consoleImageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "Ready", "type": "string", "description": "Whether the deployment is ready", "jsonPath": ".status.conditions[?(@.type==\"Ready\")].status", "priority": 1}"#
    )]
    pub struct ConsoleSpec {
        /// The console image to run.
        pub console_image_ref: String,
        // Resource requirements for the console pod
        pub resource_requirements: Option<ResourceRequirements>,
        // Number of console pods to create
        pub replicas: Option<i32>,
        // The configuration for generating an x509 certificate using cert-manager for console
        // to present to incoming connections.
        // The dns_names and issuer_ref fields are required.
        pub external_certificate_spec: Option<MaterializeCertSpec>,
        // Annotations to apply to the pods
        pub pod_annotations: Option<BTreeMap<String, String>>,
        // Labels to apply to the pods
        pub pod_labels: Option<BTreeMap<String, String>>,

        // Connection information for the balancerd service to use
        pub balancerd: BalancerdRef,
        /// How to authenticate with Materialize.
        #[serde(default)]
        pub authenticator_kind: AuthenticatorKind,

        // This can be set to override the randomly chosen resource id
        pub resource_id: Option<String>,
    }

    impl Console {
        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mz{}-{}", self.resource_id(), suffix)
        }

        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        pub fn deployment_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn replicas(&self) -> i32 {
            self.spec.replicas.unwrap_or(2)
        }

        pub fn app_name(&self) -> String {
            "console".to_owned()
        }

        pub fn service_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn configmap_name(&self) -> String {
            self.name_prefixed("console")
        }

        pub fn external_certificate_name(&self) -> String {
            self.name_prefixed("console-external")
        }

        pub fn external_certificate_secret_name(&self) -> String {
            self.name_prefixed("console-external-tls")
        }

        pub fn status(&self) -> ConsoleStatus {
            self.status.clone().unwrap_or_else(|| ConsoleStatus {
                resource_id: self
                    .spec
                    .resource_id
                    .clone()
                    .unwrap_or_else(new_resource_id),
                conditions: vec![],
            })
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
    #[serde(rename_all = "camelCase")]
    pub struct ConsoleStatus {
        /// Resource identifier used as a name prefix to avoid pod name collisions.
        pub resource_id: String,

        pub conditions: Vec<Condition>,
    }

    impl ManagedResource for Console {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/mz-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
                ("materialize.cloud/app".to_owned(), "console".to_owned()),
            ])
        }
    }
}
