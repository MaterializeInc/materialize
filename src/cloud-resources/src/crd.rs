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
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceConversion, CustomResourceDefinition,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    Api, Client, Resource, ResourceExt,
    api::{ObjectMeta, Patch, PatchParams},
    core::crd::merge_crds,
    runtime::{conditions, wait::await_condition},
};
use rand::{RngExt, distr::Uniform};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::crd::generated::cert_manager::certificates::{
    CertificateIssuerRef, CertificatePrivateKeyAlgorithm, CertificateSecretTemplate,
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
    /// Optional algorithm to use for the private key. If not specified, a recommended default will be chosen.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key_algorithm: Option<CertificatePrivateKeyAlgorithm>,
    /// Optional size for the private key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub private_key_size: Option<i64>,
}

/// The certificate a component presents to connections from outside the
/// cluster.
///
/// The certificate comes from one of two places:
/// - cert-manager issues it, when `issuerRef` is set here or in the operator's
///   default certificate specs.
/// - An existing Secret holds it, when `secretName` is set.
///
/// `secretName` and `issuerRef` cannot both be set here. When `secretName` is
/// set, it takes precedence over an `issuerRef` from the operator's default
/// certificate specs.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MaterializeExternalCertSpec {
    /// Options for a certificate issued by cert-manager.
    #[serde(flatten)]
    pub cert_manager: MaterializeCertSpec,
    /// Name of an existing Secret in the same namespace that holds the
    /// certificate in its `tls.crt` and `tls.key` keys, such as a
    /// `kubernetes.io/tls` Secret.
    /// When set, the operator does not create a cert-manager Certificate, and
    /// deletes the one it created earlier, if any.
    /// To rotate the certificate, update the Secret in place.
    /// Cannot be set together with `issuerRef`. The other cert-manager options
    /// are ignored, except that `dnsNames` on the balancerd certificate is still
    /// passed to the console.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_name: Option<String>,
}

impl From<MaterializeCertSpec> for MaterializeExternalCertSpec {
    fn from(cert_manager: MaterializeCertSpec) -> Self {
        Self {
            cert_manager,
            secret_name: None,
        }
    }
}

pub trait ManagedResource: Resource<DynamicType = ()> + Sized {
    fn default_labels(&self) -> BTreeMap<String, String> {
        BTreeMap::new()
    }

    fn app_name(&self) -> Option<&str> {
        None
    }

    fn managed_resource_meta(&self, name: String) -> ObjectMeta {
        let mut labels = self.default_labels();
        labels.extend(recommended_k8s_labels(self.app_name()));
        ObjectMeta {
            namespace: Some(self.meta().namespace.clone().unwrap()),
            name: Some(name),
            labels: Some(labels),
            owner_references: Some(vec![owner_reference(self)]),
            ..Default::default()
        }
    }
}

/// Get the recommended Kubernetes labels (app.kubernetes.io/*)
/// WARNING: this is duplicated in src/orchestrator/src/lib.rs and src/orchestratord/src/k8s.rs
pub fn recommended_k8s_labels(app_name: Option<&str>) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/managed-by".to_owned(),
        "materialize-operator".to_owned(),
    );
    labels.insert(
        "app.kubernetes.io/part-of".to_owned(),
        "materialize".to_owned(),
    );
    if let Some(app) = app_name {
        labels.insert("app.kubernetes.io/name".to_owned(), app.to_owned());
        // legacy label
        labels.insert("app".to_owned(), app.to_owned());
    }
    labels
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
    /// Conversion configuration to apply after merging CRDs.
    /// `merge_crds` drops the conversion field, so we must set it after merging.
    pub conversion: Option<CustomResourceConversion>,
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
    let crd_name = format!("{}.{}", crds[0].spec.names.plural, crds[0].spec.group);
    info!("Registering {} crd", &crd_name);
    let crd_api = Api::<CustomResourceDefinition>::all(kube_client);
    let mut crd = merge_crds(crds, &versioned_crds.stored_version).unwrap();
    if let Some(conversion) = versioned_crds.conversion {
        crd.spec.conversion = Some(conversion);
    }
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

// DNS-1035 names are supposed to be case insensitive,
// so we define our own character set, rather than use the
// built-in Alphanumeric distribution from rand, which
// includes both upper and lowercase letters.
const RESOURCE_ID_CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
const RESOURCE_ID_LEN: usize = 10;

pub fn new_resource_id() -> String {
    rand::rng()
        .sample_iter(Uniform::new(0, RESOURCE_ID_CHARSET.len()).expect("valid range"))
        .take(RESOURCE_ID_LEN)
        .map(|i| char::from(RESOURCE_ID_CHARSET[i]))
        .collect()
}

/// Whether `id` has the form of an ID generated by [`new_resource_id`].
pub fn is_valid_resource_id(id: &str) -> bool {
    id.len() == RESOURCE_ID_LEN && id.bytes().all(|b| RESOURCE_ID_CHARSET.contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn external_cert_spec_serde() {
        let spec: MaterializeExternalCertSpec = serde_json::from_value(serde_json::json!({
            "dnsNames": ["mz.example.com"],
            "issuerRef": {"name": "issuer", "kind": "ClusterIssuer"},
        }))
        .unwrap();
        assert_eq!(
            spec.cert_manager.dns_names,
            Some(vec!["mz.example.com".to_owned()])
        );
        assert_eq!(spec.cert_manager.issuer_ref.unwrap().name, "issuer");
        assert_eq!(spec.secret_name, None);

        let spec: MaterializeExternalCertSpec =
            serde_json::from_value(serde_json::json!({"secretName": "wildcard-tls"})).unwrap();
        assert_eq!(spec.secret_name.as_deref(), Some("wildcard-tls"));
        assert_eq!(spec.cert_manager, MaterializeCertSpec::default());

        // Leaving secretName unset serializes exactly like the cert-manager
        // spec, so existing resources compare equal after an upgrade.
        let cert_manager = MaterializeCertSpec {
            dns_names: Some(vec!["mz.example.com".to_owned()]),
            ..Default::default()
        };
        assert_eq!(
            serde_json::to_value(MaterializeExternalCertSpec::from(cert_manager.clone())).unwrap(),
            serde_json::to_value(cert_manager).unwrap(),
        );
    }

    #[mz_ore::test]
    fn external_cert_spec_schema_is_flat() {
        let schema =
            serde_json::to_value(schemars::schema_for!(MaterializeExternalCertSpec)).unwrap();
        let properties = schema["properties"].as_object().unwrap();
        for field in ["dnsNames", "issuerRef", "secretName", "secretTemplate"] {
            assert!(properties.contains_key(field), "missing {field}");
        }
    }
}
