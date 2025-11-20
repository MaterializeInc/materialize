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
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceColumnDefinition;
use kube::{
    Api, Client, CustomResourceExt, Resource, ResourceExt,
    api::{DeleteParams, Patch, PatchParams},
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

pub async fn apply_resource<K>(api: &Api<K>, resource: &K) -> Result<(), anyhow::Error>
where
    K: Resource + Clone + Send + DeserializeOwned + Serialize + std::fmt::Debug + 'static,
    <K as Resource>::DynamicType: Default,
{
    api.patch(
        &resource.name_unchecked(),
        &PatchParams::apply(FIELD_MANAGER).force(),
        &Patch::Apply(resource),
    )
    .await?;
    Ok(())
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
) -> Result<(), anyhow::Error> {
    let mut mz_crd = crd::materialize::v1alpha1::Materialize::crd();
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
    tokio::time::timeout(
        Duration::from_secs(120),
        register_versioned_crds(
            client.clone(),
            vec![
                VersionedCrd {
                    crds: vec![mz_crd],
                    stored_version: String::from("v1alpha1"),
                },
                VersionedCrd {
                    crds: vec![crd::balancer::v1alpha1::Balancer::crd()],
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
