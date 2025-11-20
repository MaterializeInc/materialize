// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::bail;
use k8s_openapi::{
    api::core::v1::ResourceRequirements, apimachinery::pkg::apis::meta::v1::Condition,
};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::crd::{ManagedResource, MaterializeCertSpec, new_resource_id};

pub mod v1alpha1 {
    use super::*;

    #[derive(Clone, Debug)]
    pub enum Routing<'a> {
        Static(&'a StaticRoutingConfig),
        Frontegg(&'a FronteggRoutingConfig),
    }

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct StaticRoutingConfig {
        pub environmentd_namespace: String,
        pub environmentd_service_name: String,
    }

    #[derive(Clone, Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
    #[serde(rename_all = "camelCase")]
    pub struct FronteggRoutingConfig {
        // TODO
    }

    #[derive(
        CustomResource, Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema,
    )]
    #[serde(rename_all = "camelCase")]
    #[kube(
        namespaced,
        group = "materialize.cloud",
        version = "v1alpha1",
        kind = "Balancer",
        singular = "balancer",
        plural = "balancers",
        status = "BalancerStatus",
        printcolumn = r#"{"name": "ImageRef", "type": "string", "description": "Reference to the Docker image.", "jsonPath": ".spec.balancerdImageRef", "priority": 1}"#,
        printcolumn = r#"{"name": "Ready", "type": "string", "description": "Whether the deployment is ready", "jsonPath": ".status.conditions[?(@.type==\"Ready\")].status", "priority": 1}"#
    )]
    pub struct BalancerSpec {
        /// The balancerd image to run.
        pub balancerd_image_ref: String,
        // Resource requirements for the balancerd pod
        pub resource_requirements: Option<ResourceRequirements>,
        // Number of balancerd pods to create
        pub replicas: Option<i32>,
        // The configuration for generating an x509 certificate using cert-manager for balancerd
        // to present to incoming connections.
        // The dns_names and issuer_ref fields are required.
        pub external_certificate_spec: Option<MaterializeCertSpec>,
        // The configuration for generating an x509 certificate using cert-manager for balancerd
        // to use to communicate with environmentd.
        // The dns_names and issuer_ref fields are required.
        pub internal_certificate_spec: Option<MaterializeCertSpec>,
        // Annotations to apply to the pods
        pub pod_annotations: Option<BTreeMap<String, String>>,
        // Labels to apply to the pods
        pub pod_labels: Option<BTreeMap<String, String>>,

        // Configuration for statically routing traffic
        pub static_routing: Option<StaticRoutingConfig>,
        // Configuration for routing traffic via Frontegg
        pub frontegg_routing: Option<FronteggRoutingConfig>,

        // This can be set to override the randomly chosen resource id
        pub resource_id: Option<String>,
    }

    impl Balancer {
        pub fn name_prefixed(&self, suffix: &str) -> String {
            format!("mz{}-{}", self.resource_id(), suffix)
        }

        pub fn resource_id(&self) -> &str {
            &self.status.as_ref().unwrap().resource_id
        }

        pub fn deployment_name(&self) -> String {
            self.name_prefixed("balancerd")
        }

        pub fn replicas(&self) -> i32 {
            self.spec.replicas.unwrap_or(2)
        }

        pub fn app_name(&self) -> String {
            "balancerd".to_owned()
        }

        pub fn service_name(&self) -> String {
            self.name_prefixed("balancerd")
        }

        pub fn external_certificate_name(&self) -> String {
            self.name_prefixed("balancerd-external")
        }

        pub fn external_certificate_secret_name(&self) -> String {
            self.name_prefixed("balancerd-external-tls")
        }

        pub fn routing(&self) -> anyhow::Result<Routing<'_>> {
            match (&self.spec.static_routing, &self.spec.frontegg_routing) {
                (Some(config), None) => Ok(Routing::Static(config)),
                (None, Some(config)) => Ok(Routing::Frontegg(config)),
                (None, None) => bail!("no routing configuration present"),
                _ => bail!("multiple routing configurations present"),
            }
        }

        pub fn status(&self) -> BalancerStatus {
            self.status.clone().unwrap_or_else(|| BalancerStatus {
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
    pub struct BalancerStatus {
        /// Resource identifier used as a name prefix to avoid pod name collisions.
        pub resource_id: String,

        pub conditions: Vec<Condition>,
    }

    impl ManagedResource for Balancer {
        fn default_labels(&self) -> BTreeMap<String, String> {
            BTreeMap::from_iter([
                (
                    "materialize.cloud/mz-resource-id".to_owned(),
                    self.resource_id().to_owned(),
                ),
                ("materialize.cloud/app".to_owned(), "balancerd".to_owned()),
            ])
        }
    }
}
