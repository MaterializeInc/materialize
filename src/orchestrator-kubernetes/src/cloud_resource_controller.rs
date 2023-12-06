// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Management of K8S objects, such as VpcEndpoints.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::StreamExt;
use kube::api::{DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::runtime::{watcher, WatchStreamExt};
use kube::{Api, ResourceExt};
use maplit::btreemap;
use mz_repr::GlobalId;

use mz_cloud_resources::crd::vpc_endpoint::v1::{
    VpcEndpoint, VpcEndpointSpec, VpcEndpointState, VpcEndpointStatus,
};
use mz_cloud_resources::{
    CloudResourceController, CloudResourceReader, VpcEndpointConfig, VpcEndpointEvent,
};

use crate::{util, KubernetesOrchestrator, FIELD_MANAGER};

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

    async fn list_vpc_endpoints(
        &self,
    ) -> Result<BTreeMap<GlobalId, VpcEndpointStatus>, anyhow::Error> {
        let objects = self.vpc_endpoint_api.list(&ListParams::default()).await?;
        let mut endpoints = BTreeMap::new();
        for object in objects {
            let id = match mz_cloud_resources::id_from_vpc_endpoint_name(&object.name_any()) {
                Some(id) => id,
                // Ignore any object whose name can't be parsed as a GlobalId
                None => continue,
            };
            endpoints.insert(id, object.status.unwrap_or_default());
        }
        Ok(endpoints)
    }

    async fn watch_vpc_endpoints(&self) -> BoxStream<'static, VpcEndpointEvent> {
        let stream = watcher(self.vpc_endpoint_api.clone(), watcher::Config::default())
            .touched_objects()
            .filter_map(|object| async move {
                match object {
                    Ok(vpce) => {
                        let connection_id =
                            mz_cloud_resources::id_from_vpc_endpoint_name(&vpce.name_any())?;

                        if let Some(state) = vpce.status.as_ref().and_then(|st| st.state.to_owned())
                        {
                            Some(VpcEndpointEvent {
                                connection_id,
                                status: state,
                                // Use the 'Available' Condition on the VPCE Status to set the event-time, falling back
                                // to now if it's not set
                                time: vpce
                                    .status
                                    .unwrap()
                                    .conditions
                                    .and_then(|c| c.into_iter().find(|c| &c.type_ == "Available"))
                                    .and_then(|condition| Some(condition.last_transition_time.0))
                                    .unwrap_or_else(Utc::now),
                            })
                        } else {
                            // The Status/State is not yet populated on the VpcEndpoint, which means it was just
                            // initialized and hasn't yet been reconciled by the environment-controller
                            // We return an event with an 'unknown' state so that watchers know the VpcEndpoint was created
                            // even if we don't yet have an accurate status
                            Some(VpcEndpointEvent {
                                connection_id,
                                status: VpcEndpointState::Unknown,
                                time: vpce.creation_timestamp()?.0,
                            })
                        }
                        // TODO: Should we also check for the deletion_timestamp on the vpce? That would indicate that the
                        // resource is about to be deleted; however there is already a 'deleted' enum val on VpcEndpointState
                        // which refers to the state of the customer's VPC Endpoint Service, so we'd need to introduce a new state val
                    }
                    Err(error) => {
                        // We assume that errors returned by Kubernetes are usually transient, so we
                        // just log a warning and ignore them otherwise.
                        tracing::warn!("vpc endpoint watch error: {error}");
                        None
                    }
                }
            });
        Box::pin(stream)
    }

    fn reader(&self) -> Arc<dyn CloudResourceReader> {
        let reader = Arc::clone(&self.resource_reader);
        reader
    }
}

#[async_trait]
impl CloudResourceReader for KubernetesOrchestrator {
    async fn read(&self, id: GlobalId) -> Result<VpcEndpointStatus, anyhow::Error> {
        self.resource_reader.read(id).await
    }
}

/// Reads cloud resources managed by a [`KubernetesOrchestrator`].
#[derive(Debug)]
pub struct KubernetesResourceReader {
    vpc_endpoint_api: Api<VpcEndpoint>,
}

impl KubernetesResourceReader {
    /// Constructs a new Kubernetes cloud resource reader.
    ///
    /// The `context` parameter works like
    /// [`KubernetesOrchestratorConfig::context`](crate::KubernetesOrchestratorConfig::context).
    pub async fn new(context: String) -> Result<KubernetesResourceReader, anyhow::Error> {
        let (client, _) = util::create_client(context).await?;
        let vpc_endpoint_api: Api<VpcEndpoint> = Api::default_namespaced(client);
        Ok(KubernetesResourceReader { vpc_endpoint_api })
    }
}

#[async_trait]
impl CloudResourceReader for KubernetesResourceReader {
    async fn read(&self, id: GlobalId) -> Result<VpcEndpointStatus, anyhow::Error> {
        let name = mz_cloud_resources::vpc_endpoint_name(id);
        let endpoint = self.vpc_endpoint_api.get(&name).await?;
        Ok(endpoint.status.unwrap_or_default())
    }
}
