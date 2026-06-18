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
    ByteString,
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceConversion, ServiceReference,
        WebhookClientConfig, WebhookConversion,
    },
};
use kube::{
    Api, Client, CustomResourceExt, Resource, ResourceExt,
    api::{DeleteParams, Patch, PatchParams, PostParams},
    core::Status,
    core::response::StatusSummary,
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::info;

use mz_cloud_resources::crd::{self, VersionedCrd, register_versioned_crds};

const FIELD_MANAGER: &str = "orchestratord.materialize.cloud";

pub async fn get_resource<K>(api: &Api<K>, name: &str) -> Result<Option<K>, anyhow::Error>
where
    K: Resource + Clone + Send + DeserializeOwned + Serialize + std::fmt::Debug + 'static,
    <K as Resource>::DynamicType: Default,
{
    match api.get(name).await {
        Ok(k) => Ok(Some(k)),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub async fn apply_resource<K>(api: &Api<K>, resource: &K) -> Result<K, anyhow::Error>
where
    K: Resource + Clone + Send + DeserializeOwned + Serialize + std::fmt::Debug + 'static,
    <K as Resource>::DynamicType: Default,
{
    Ok(api
        .patch(
            &resource.name_unchecked(),
            &PatchParams::apply(FIELD_MANAGER).force(),
            &Patch::Apply(resource),
        )
        .await?)
}

pub async fn replace_resource<K>(api: &Api<K>, resource: &K) -> Result<K, anyhow::Error>
where
    K: Resource + Clone + Send + DeserializeOwned + Serialize + std::fmt::Debug + 'static,
    <K as Resource>::DynamicType: Default,
{
    if resource.meta().resource_version.is_none() {
        return Err(kube::Error::Api(
            Box::new(Status {
                status: Some(StatusSummary::Failure),
                message: "Must use apply_resource instead of replace_resource to apply fully created resources.".to_string(),
                reason: "BadRequest".to_string(),
                code: 400,
                metadata: None,
                details: None,
            }),
        )
        .into());
    }
    Ok(api
        .replace(&resource.name_unchecked(), &PostParams::default(), resource)
        .await?)
}

pub async fn delete_resource<K>(api: &Api<K>, name: &str) -> Result<(), anyhow::Error>
where
    K: Resource + Clone + Send + DeserializeOwned + Serialize + std::fmt::Debug + 'static,
    <K as Resource>::DynamicType: Default,
{
    match kube::runtime::wait::delete::delete_and_finalize(
        api.clone(),
        name,
        &DeleteParams::foreground(),
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(kube::runtime::wait::delete::Error::Delete(kube::Error::Api(e))) if e.code == 404 => {
            // the resource already doesn't exist
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// Configuration for the conversion webhook that serves the v1 version of the
/// Materialize CRD. When present, the v1 version is registered alongside
/// v1alpha1 with webhook conversion between them; when absent, only v1alpha1
/// is registered.
#[derive(Debug, Clone)]
pub struct ConversionWebhookConfig {
    pub service_name: String,
    pub service_namespace: String,
    pub service_port: u16,
    pub ca_cert_path: String,
}

pub async fn register_crds(
    client: Client,
    additional_crd_columns: Vec<CustomResourceColumnDefinition>,
    conversion_webhook: Option<ConversionWebhookConfig>,
) -> Result<(), anyhow::Error> {
    let (mut mz_crds, mz_conversion) = match conversion_webhook {
        Some(config) => {
            let ca_bytes = tokio::fs::read(config.ca_cert_path).await?;
            let conversion = CustomResourceConversion {
                strategy: "Webhook".to_owned(),
                webhook: Some(WebhookConversion {
                    client_config: Some(WebhookClientConfig {
                        ca_bundle: Some(ByteString(ca_bytes)),
                        service: Some(ServiceReference {
                            name: config.service_name,
                            namespace: config.service_namespace,
                            path: Some("/convert".to_owned()),
                            port: Some(config.service_port.into()),
                        }),
                        url: None,
                    }),
                    conversion_review_versions: vec!["v1".to_owned()],
                }),
            };
            (
                vec![
                    crd::materialize::v1::Materialize::crd(),
                    crd::materialize::v1alpha1::Materialize::crd(),
                ],
                Some(conversion),
            )
        }
        None => (vec![crd::materialize::v1alpha1::Materialize::crd()], None),
    };
    let default_columns = mz_crds[0].spec.versions[0]
        .additional_printer_columns
        .take()
        .expect("should contain ImageRef and UpToDate columns");
    mz_crds[0].spec.versions[0].additional_printer_columns = Some(
        additional_crd_columns
            .into_iter()
            .chain(default_columns)
            .collect(),
    );
    tokio::time::timeout(
        Duration::from_secs(120),
        register_versioned_crds(
            client.clone(),
            vec![
                VersionedCrd {
                    crds: mz_crds,
                    stored_version: String::from("v1alpha1"),
                    conversion: mz_conversion,
                },
                VersionedCrd {
                    crds: vec![crd::balancer::v1alpha1::Balancer::crd()],
                    stored_version: String::from("v1alpha1"),
                    conversion: None,
                },
                VersionedCrd {
                    crds: vec![crd::console::v1alpha1::Console::crd()],
                    stored_version: String::from("v1alpha1"),
                    conversion: None,
                },
                VersionedCrd {
                    crds: vec![crd::vpc_endpoint::v1::VpcEndpoint::crd()],
                    stored_version: String::from("v1"),
                    conversion: None,
                },
            ],
            FIELD_MANAGER,
        ),
    )
    .await??;

    info!("Done rewriting CRDs");

    Ok(())
}

/// Get the recommended Kubernetes labels (app.kubernetes.io/*)
pub fn recommended_k8s_labels(app_name: String) -> BTreeMap<String, String> {
    let mut labels = BTreeMap::new();
    labels.insert(
        "app.kubernetes.io/managed-by".into(),
        "materialize-operator".into(),
    );
    labels.insert("app.kubernetes.io/part-of".into(), "materialize".into());
    labels.insert("app.kubernetes.io/name".into(), app_name.to_owned());
    // legacy label
    labels.insert("app".into(), app_name.to_owned());
    labels
}
