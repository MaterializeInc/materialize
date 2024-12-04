// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kubernetes custom resources

use std::time::Duration;

use futures::future::join_all;
use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;
use kube::api::{Patch, PatchParams};
use kube::{
    core::crd::merge_crds,
    runtime::{conditions, wait::await_condition},
};
use kube::{Api, Client};
use tracing::{info, warn};

use mz_ore::retry::Retry;

pub mod gen;
pub mod materialize;
#[cfg(feature = "vpc-endpoints")]
pub mod vpc_endpoint;

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
