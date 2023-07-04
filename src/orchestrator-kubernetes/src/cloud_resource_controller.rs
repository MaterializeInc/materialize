// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of K8S objects, such as VpcEndpoints.

use std::collections::BTreeSet;
use std::str::FromStr;

use async_trait::async_trait;
use kube::api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::ResourceExt;
use maplit::btreemap;
use mz_cloud_resources::crd::vpc_endpoint::v1::{VpcEndpoint, VpcEndpointSpec};
use mz_cloud_resources::{CloudResourceController, VpcEndpointConfig};
use mz_repr::GlobalId;

use crate::{KubernetesOrchestrator, FIELD_MANAGER};

#[async_trait]
impl CloudResourceController for KubernetesOrchestrator {
    async fn ensure_vpc_endpoint(
        &self,
        id: GlobalId,
        config: VpcEndpointConfig,
    ) -> Result<(), anyhow::Error> {
        let name = mz_cloud_resources::vpc_endpoint_name(id);
        let mut labels = btreemap! {
            "environmentd.materialize.cloud/connection-id".to_owned() => id.to_string(),
        };
        for (key, value) in &self.config.service_labels {
            labels.insert(key.clone(), value.clone());
        }
        let vpc_endpoint = VpcEndpoint {
            metadata: ObjectMeta {
                labels: Some(labels),
                name: Some(name.clone()),
                namespace: Some(self.kubernetes_namespace.clone()),
                // TODO owner references https://github.com/MaterializeInc/cloud/issues/4408
                //owner_references: todo!(),
                ..Default::default()
            },
            spec: VpcEndpointSpec {
                aws_service_name: config.aws_service_name,
                availability_zone_ids: config.availability_zone_ids,
                role_suffix: match &self.config.aws_external_id_prefix {
                    None => id.to_string(),
                    Some(external_id) => format!("{external_id}_{id}"),
                },
            },
            status: None,
        };
        self.vpc_endpoint_api
            .patch(
                &name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(vpc_endpoint),
            )
            .await?;
        Ok(())
    }

    async fn delete_vpc_endpoint(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        match self
            .vpc_endpoint_api
            .delete(
                &mz_cloud_resources::vpc_endpoint_name(id),
                &DeleteParams::default(),
            )
            .await
        {
            Ok(_) => Ok(()),
            // Ignore already deleted endpoints.
            Err(kube::Error::Api(resp)) if resp.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_vpc_endpoints(&self) -> Result<BTreeSet<GlobalId>, anyhow::Error> {
        Ok(self
            .vpc_endpoint_api
            .list(&ListParams::default())
            .await?
            .iter()
            .filter_map(|vpc_endpoint| {
                vpc_endpoint
                    .name_any()
                    .split_once('-')
                    // Ignore any whom's name can't be parsed into a GlobalId
                    .map(|(_, id_str)| GlobalId::from_str(id_str).ok())
            })
            .flatten()
            .collect())
    }
}
