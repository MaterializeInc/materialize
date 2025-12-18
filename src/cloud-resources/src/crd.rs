// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kubernetes custom resources

use std::collections::BTreeMap;
use std::time::Duration;

use futures::future::join_all;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ObjectMeta, Patch, PatchParams},
    core::crd::merge_crds,
    runtime::{conditions, wait::await_condition},
};
use rand::{Rng, distr::Uniform};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::crd::generated::cert_manager::certificates::{
    CertificateIssuerRef, CertificateSecretTemplate,
};
use mz_ore::retry::Retry;

pub mod balancer;
pub mod console;
pub mod generated;
pub mod materialize;
#[cfg(feature = "vpc-endpoints")]
pub mod vpc_endpoint;

// This is intentionally a subset of the fields of a Certificate.
// We do not want customers to configure options that may conflict with
// things we override or expand in our code.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaterializeCertSpec {
    /// Additional DNS names the certificate will be valid for.
    pub dns_names: Option<Vec<String>>,
    /// Duration the certificate will be requested for.
    /// Value must be in units accepted by Go
    /// [`time.ParseDuration`](https://golang.org/pkg/time/#ParseDuration).
    pub duration: Option<String>,
    /// Duration before expiration the certificate will be renewed.
    /// Value must be in units accepted by Go
    /// [`time.ParseDuration`](https://golang.org/pkg/time/#ParseDuration).
    pub renew_before: Option<String>,
    /// Reference to an `Issuer` or `ClusterIssuer` that will generate the certificate.
    pub issuer_ref: Option<CertificateIssuerRef>,
    /// Additional annotations and labels to include in the Certificate object.
    pub secret_template: Option<CertificateSecretTemplate>,
}

pub trait ManagedResource: Resource<DynamicType = ()> + Sized {
    fn default_labels(&self) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn managed_resource_meta(&self, name: String) -> ObjectMeta {
        ObjectMeta {
            namespace: Some(self.meta().namespace.clone().unwrap()),
            name: Some(name),
            labels: Some(self.default_labels()),
            owner_references: Some(vec![owner_reference(self)]),
            ..Default::default()
        }
    }
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

#[derive(Debug, Clone)]
pub struct VersionedCrd {
    pub crds: Vec<CustomResourceDefinition>,
    pub stored_version: String,
}

pub async fn register_versioned_crds(
    kube_client: Client,
    versioned_crds: Vec<VersionedCrd>,
    field_manager: &str,
) -> Result<(), anyhow::Error> {
    let crd_futures = versioned_crds
        .into_iter()
        .map(|versioned_crd| register_w_retry(kube_client.clone(), versioned_crd, field_manager));
    for res in join_all(crd_futures).await {
        if res.is_err() {
            return res;
        }
    }
    Ok(())
}

async fn register_w_retry(
    kube_client: Client,
    versioned_crds: VersionedCrd,
    field_manager: &str,
) -> Result<(), anyhow::Error> {
    Retry::default()
        .max_duration(Duration::from_secs(30))
        .clamp_backoff(Duration::from_secs(5))
        .retry_async(|_| async {
            let res = register_custom_resource(
                kube_client.clone(),
                versioned_crds.clone(),
                field_manager,
            )
            .await;
            if let Err(err) = &res {
                warn!(err = %err);
            }
            res
        })
        .await?;
    Ok(())
}

/// Registers a custom resource with Kubernetes,
/// the specification of which is automatically derived from the structs.
async fn register_custom_resource(
    kube_client: Client,
    versioned_crds: VersionedCrd,
    field_manager: &str,
) -> Result<(), anyhow::Error> {
    let crds = versioned_crds.crds;
    let crd_name = format!("{}.{}", &crds[0].spec.names.plural, &crds[0].spec.group);
    info!("Registering {} crd", &crd_name);
    let crd_api = Api::<CustomResourceDefinition>::all(kube_client);
    let crd = merge_crds(crds, &versioned_crds.stored_version).unwrap();
    let crd_json = serde_json::to_string(&serde_json::json!(&crd))?;
    info!(crd_json = %crd_json);
    crd_api
        .patch(
            &crd_name,
            &PatchParams::apply(field_manager).force(),
            &Patch::Apply(crd),
        )
        .await?;
    await_condition(crd_api, &crd_name, conditions::is_crd_established()).await?;
    info!("Done registering {} crd", &crd_name);
    Ok(())
}

pub fn new_resource_id() -> String {
    // DNS-1035 names are supposed to be case insensitive,
    // so we define our own character set, rather than use the
    // built-in Alphanumeric distribution from rand, which
    // includes both upper and lowercase letters.
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    rand::rng()
        .sample_iter(Uniform::new(0, CHARSET.len()).expect("valid range"))
        .take(10)
        .map(|i| char::from(CHARSET[i]))
        .collect()
}
