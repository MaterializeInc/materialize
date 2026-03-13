// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{future::ready, time::Duration};

use futures::StreamExt;
use k8s_openapi::{
    ByteString,
    apiextensions_apiserver::pkg::apis::apiextensions::v1::{
        CustomResourceColumnDefinition, CustomResourceConversion, CustomResourceDefinition,
        ServiceReference, WebhookClientConfig, WebhookConversion,
    },
};
use kube::{
    Api, Client, CustomResourceExt, Resource, ResourceExt,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
    core::{Status, response::StatusSummary},
    runtime::{reflector, watcher},
};
use serde::{Serialize, de::DeserializeOwned};
use tracing::{info, warn};

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

pub async fn register_crds(
    client: Client,
    additional_crd_columns: Vec<CustomResourceColumnDefinition>,
    webhook_service_name: String,
    webhook_service_namespace: String,
    webhook_service_port: u16,
    ca_cert_path: String,
) -> Result<(), anyhow::Error> {
    let ca_bytes = tokio::fs::read(ca_cert_path).await?;
    let mut mz_crd = crd::materialize::v1alpha2::Materialize::crd();
    let default_columns = mz_crd.spec.versions[0]
        .additional_printer_columns
        .take()
        .expect("should contain ImageRef and UpToDate columns");
    mz_crd.spec.versions[0].additional_printer_columns = Some(
        additional_crd_columns
            .into_iter()
            .chain(default_columns)
            .collect(),
    );
    mz_crd.spec.conversion = Some(CustomResourceConversion {
        strategy: "Webhook".to_owned(),
        webhook: Some(WebhookConversion {
            client_config: Some(WebhookClientConfig {
                ca_bundle: Some(ByteString(ca_bytes)),
                service: Some(ServiceReference {
                    name: webhook_service_name,
                    namespace: webhook_service_namespace,
                    path: Some("/convert".to_owned()),
                    port: Some(webhook_service_port.into()),
                }),
                url: None,
            }),
            conversion_review_versions: vec!["v1".to_owned()],
        }),
    });
    tokio::time::timeout(
        Duration::from_secs(120),
        register_versioned_crds(
            client.clone(),
            vec![
                VersionedCrd {
                    crds: vec![mz_crd, crd::materialize::v1alpha1::Materialize::crd()],
                    stored_version: String::from("v1alpha2"),
                },
                VersionedCrd {
                    crds: vec![crd::balancer::v1alpha1::Balancer::crd()],
                    stored_version: String::from("v1alpha1"),
                },
                VersionedCrd {
                    crds: vec![crd::console::v1alpha1::Console::crd()],
                    stored_version: String::from("v1alpha1"),
                },
                VersionedCrd {
                    crds: vec![crd::vpc_endpoint::v1::VpcEndpoint::crd()],
                    stored_version: String::from("v1"),
                },
            ],
            FIELD_MANAGER,
        ),
    )
    .await??;

    info!("Done rewriting CRDs");

    Ok(())
}

/// After updating the stored version to v1alpha2 in the CRD, any existing
/// resources that were stored as v1alpha1 need to be re-saved so that they
/// are stored in the new version. This function reads all Materialize
/// resources (triggering conversion via the webhook) and writes them back.
/// If all resources are successfully migrated, it also removes v1alpha1
/// from the CRD's storedVersions.
pub async fn migrate_materialize_storage_version(client: Client) -> Result<(), anyhow::Error> {
    let crd_api = Api::<CustomResourceDefinition>::all(client.clone());
    let mz_crd = crd_api.get("materializes.materialize.cloud").await?;

    let stored_versions = mz_crd
        .status
        .as_ref()
        .and_then(|s| s.stored_versions.as_ref())
        .cloned()
        .unwrap_or_default();

    if !stored_versions.contains(&"v1alpha1".to_string()) {
        info!("No v1alpha1 stored versions found, skipping storage migration");
        return Ok(());
    }

    info!("Migrating stored Materialize resources from v1alpha1 to v1alpha2");

    let mz_api = Api::<crd::materialize::v1alpha2::Materialize>::all(client.clone());
    let mz_list = mz_api.list(&ListParams::default()).await?;

    let mut all_succeeded = true;
    for mz in mz_list.items {
        let name = mz.name_unchecked();
        let namespace = mz.namespace();
        let namespaced_api =
            Api::<crd::materialize::v1alpha2::Materialize>::namespaced(client.clone(), &namespace);
        match replace_resource(&namespaced_api, &mz).await {
            Ok(_) => {
                info!(
                    "Migrated Materialize resource {}/{} to v1alpha2 storage",
                    namespace, name,
                );
            }
            Err(e) => {
                warn!(
                    "Failed to migrate Materialize resource {}/{}: {}",
                    namespace, name, e,
                );
                all_succeeded = false;
            }
        }
    }

    if all_succeeded {
        let new_stored_versions: Vec<String> = stored_versions
            .into_iter()
            .filter(|v| v != "v1alpha1")
            .collect();
        let patch = serde_json::json!({
            "status": {
                "storedVersions": new_stored_versions,
            }
        });
        crd_api
            .patch_status(
                "materializes.materialize.cloud",
                &PatchParams::default(),
                &Patch::Merge(patch),
            )
            .await?;
        info!("Removed v1alpha1 from CRD storedVersions");
    }

    Ok(())
}

pub async fn make_reflector<K>(client: Client) -> reflector::Store<K>
where
    K: kube::Resource<DynamicType = ()>
        + Clone
        + Send
        + Sync
        + DeserializeOwned
        + Serialize
        + std::fmt::Debug
        + 'static,
{
    let api = kube::Api::all(client);
    let (store, writer) = reflector::store();
    let reflector =
        reflector::reflector(writer, watcher(api, watcher::Config::default().timeout(29)));
    mz_ore::task::spawn(
        || format!("{} reflector", K::kind(&Default::default())),
        async {
            reflector
                .for_each(|res| {
                    if let Err(e) = res {
                        warn!("error in {} reflector: {}", K::kind(&Default::default()), e);
                    }
                    ready(())
                })
                .await
        },
    );
    // the only way this can return an error is if we drop the writer,
    // which we do not ever do, so unwrap is fine
    store.wait_until_ready().await.expect("writer dropped");
    store
}
