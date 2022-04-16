// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A representative of STORAGE and COMPUTE that maintains summaries of the involved objects.
//!
//! The `Controller` provides the ability to create and manipulate storage and compute instances.
//! Each of Storage and Compute provide their own controllers, accessed through the `storage()`
//! and `compute(instance_id)` methods. It is an error to access a compute instance before it has
//! been created; a single storage instance is always available.
//!
//! The controller also provides a `recv()` method that returns responses from the storage and
//! compute layers, which may remain of value to the interested user. With time, these responses
//! may be thinned down in an effort to make the controller more self contained.
//!
//! Consult the `StorageController` and `ComputeController` documentation for more information
//! about each of these interfaces.

use std::collections::BTreeMap;

use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use maplit::hashmap;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::Timestamp;
use tokio_stream::StreamMap;

use mz_orchestrator::{Orchestrator, ServiceConfig, ServicePort};

use crate::client::GenericClient;
use crate::client::{
    ComputeClient, ComputeCommand, ComputeInstanceId, ComputeResponse, InstanceConfig,
    RemoteClient, Response, StorageResponse,
};
use crate::logging::LoggingConfig;
use crate::{TailBatch, TailResponse};

pub use storage::{StorageController, StorageControllerState};
pub mod storage;
pub use compute::{ComputeController, ComputeControllerMut};
mod compute;

/// Configures an orchestrator for the controller.
pub struct OrchestratorConfig {
    /// The orchestrator implementation to use.
    pub orchestrator: Box<dyn Orchestrator>,
    /// The computed image to use when starting new compute instances.
    pub computed_image: String,
    /// The storage address that compute instances should connect to.
    pub storage_addr: String,
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
///
/// NOTE(benesch): I find the fact that this type is called `Controller` but is
/// referred to as the `dataflow_client` in the coordinator to be very
/// confusing. We should find the one correct name, and use it everywhere!
pub struct Controller<T = mz_repr::Timestamp> {
    orchestrator: OrchestratorConfig,
    storage_controller: Box<dyn StorageController<Timestamp = T>>,
    compute: BTreeMap<ComputeInstanceId, compute::ComputeControllerState<T>>,
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice + Copy + Unpin,
{
    pub async fn create_instance(
        &mut self,
        instance: ComputeInstanceId,
        config: InstanceConfig,
        logging: Option<LoggingConfig>,
    ) -> Result<(), anyhow::Error> {
        // Insert a new compute instance controller.
        self.compute.insert(
            instance,
            compute::ComputeControllerState::new(&logging).await?,
        );

        // Add replicas backing that instance.
        match config {
            InstanceConfig::Remote { replicas } => {
                let mut compute_instance = self.compute_mut(instance).unwrap();
                for (name, hosts) in replicas {
                    let client = RemoteClient::new(&hosts.into_iter().collect::<Vec<_>>());
                    let client: Box<dyn ComputeClient<T>> = Box::new(client);
                    compute_instance.add_replica(name, client).await;
                }
            }
            InstanceConfig::Managed { size: _ } => {
                let OrchestratorConfig {
                    orchestrator,
                    computed_image,
                    storage_addr,
                } = &self.orchestrator;
                let service = orchestrator
                    .namespace("compute")
                    .ensure_service(
                        &format!("cluster-{instance}"),
                        ServiceConfig {
                            image: computed_image.clone(),
                            args: &|ports| {
                                vec![
                                    format!("--storage-addr={storage_addr}"),
                                    format!("--listen-addr=0.0.0.0:{}", ports["controller"]),
                                    format!("0.0.0.0:{}", ports["compute"]),
                                ]
                            },
                            ports: vec![
                                ServicePort {
                                    name: "controller".into(),
                                    port_hint: 2100,
                                },
                                ServicePort {
                                    name: "compute".into(),
                                    port_hint: 2102,
                                },
                            ],
                            // TODO: use `size` to set these.
                            cpu_limit: None,
                            memory_limit: None,
                            // TODO: support sizes large enough to warrant multiple processes.
                            processes: 1,
                            labels: hashmap! {
                                "cluster-id".into() => instance.to_string(),
                                "type".into() => "cluster".into(),
                            },
                        },
                    )
                    .await?;
                let client = RemoteClient::new(&service.addresses("controller"));
                let client: Box<dyn ComputeClient<T>> = Box::new(client);
                self.compute_mut(instance)
                    .unwrap()
                    .add_replica("default".into(), client)
                    .await;
            }
        }

        Ok(())
    }
    pub async fn drop_instance(
        &mut self,
        instance: ComputeInstanceId,
    ) -> Result<(), anyhow::Error> {
        if let Some(mut compute) = self.compute.remove(&instance) {
            self.orchestrator
                .orchestrator
                .namespace("compute")
                .drop_service(&format!("cluster-{instance}"))
                .await?;
            compute.client.send(ComputeCommand::DropInstance).await?;
        }
        Ok(())
    }
}

impl<T> Controller<T> {
    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> &dyn StorageController<Timestamp = T> {
        &*self.storage_controller
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(&mut self) -> &mut dyn StorageController<Timestamp = T> {
        &mut *self.storage_controller
    }

    /// Acquires an immutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute(&self, instance: ComputeInstanceId) -> Option<ComputeController<T>> {
        let compute = self.compute.get(&instance)?;
        Some(ComputeController {
            _instance: instance,
            compute,
            storage_controller: self.storage(),
        })
    }

    /// Acquires a mutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute_mut(&mut self, instance: ComputeInstanceId) -> Option<ComputeControllerMut<T>> {
        let compute = self.compute.get_mut(&instance)?;
        Some(ComputeControllerMut {
            instance,
            compute,
            storage_controller: &mut *self.storage_controller,
        })
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice,
{
    pub async fn recv(&mut self) -> Result<Option<Response<T>>, anyhow::Error> {
        let mut compute_stream: StreamMap<_, _> = self
            .compute
            .iter_mut()
            .map(|(id, compute)| (*id, compute.client.as_stream()))
            .collect();
        tokio::select! {
            Some((instance, response)) = compute_stream.next() => {
                drop(compute_stream);
                let response = response?;
                match &response {
                    ComputeResponse::FrontierUppers(updates) => {
                        self.compute_mut(instance)
                            // TODO: determine if this is an error, or perhaps just a late
                            // response about a terminated instance.
                            .expect("Reference to absent instance")
                            .update_write_frontiers(updates)
                            .await?;
                    }
                    ComputeResponse::PeekResponse(uuid, _response) => {
                        self.compute_mut(instance)
                            .expect("Reference to absent instance")
                            .remove_peeks(std::iter::once(*uuid))
                            .await?;
                    }
                    ComputeResponse::TailResponse(global_id, response) => {
                        let mut changes = timely::progress::ChangeBatch::new();
                        match response {
                            TailResponse::Batch(TailBatch { lower, upper, .. }) => {
                                changes.extend(upper.iter().map(|time| (time.clone(), 1)));
                                changes.extend(lower.iter().map(|time| (time.clone(), -1)));
                            }
                            TailResponse::DroppedAt(frontier) => {
                                // The tail will not be written to again, but we should not confuse that
                                // with the source of the TAIL being complete through this time.
                                changes.extend(frontier.iter().map(|time| (time.clone(), -1)));
                            }
                        }
                        self.compute_mut(instance)
                            .expect("Reference to absent instance")
                            .update_write_frontiers(&[(*global_id, changes)])
                            .await?;
                    }
                }
                Ok(Some(Response::Compute(response)))
            }
            response = self.storage_controller.recv() => {
                let response = response?;
                match &response {
                    Some(StorageResponse::TimestampBindings(feedback)) => {

                        // !!! The ordering is important here. We can never
                        // communicate a new upper or save them in a local
                        // datastructure before we durably record ts bindings
                        // as otherwise a crash could cause dataloss

                        self.storage_controller
                            .persist_timestamp_bindings(&feedback)
                            .await?;

                        self.storage_controller
                            .update_write_frontiers(&feedback.changes)
                            .await?;
                    }
                    Some(StorageResponse::LinearizedTimestamps(_)) => {
                        // Nothing to do here.
                    }
                    None => (),
                }
                Ok(response.map(Response::Storage))
            }
            else => Ok(None),
        }
    }
}

impl<T> Controller<T> {
    /// Create a new controller from a client it should wrap.
    pub fn new<S: StorageController<Timestamp = T> + 'static>(
        orchestrator: OrchestratorConfig,
        storage_controller: S,
    ) -> Self {
        Self {
            orchestrator,
            storage_controller: Box::new(storage_controller),
            compute: BTreeMap::default(),
        }
    }
}

use std::sync::Arc;

/// Compaction policies for collections maintained by `Controller`.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub enum ReadPolicy<T> {
    /// Maintain the collection as valid from this frontier onward.
    ValidFrom(Antichain<T>),
    /// Maintain the collection as valid from a function of the write frontier.
    ///
    /// This function will only be re-evaluated when the write frontier changes.
    /// If the intended behavior is to change in response to external signals,
    /// consider using the `ValidFrom` variant to manually pilot compaction.
    ///
    /// The `Arc` makes the function cloneable.
    LagWriteFrontier(
        #[derivative(Debug = "ignore")] Arc<dyn Fn(AntichainRef<T>) -> Antichain<T> + Send + Sync>,
    ),
    /// Allows one to express multiple read policies, taking the least of
    /// the resulting frontiers.
    Multiple(Vec<ReadPolicy<T>>),
}

impl ReadPolicy<mz_repr::Timestamp> {
    /// Creates a read policy that lags the write frontier by the indicated amount, rounded down to a multiple of that amount.
    ///
    /// The rounding down is done to reduce the number of changes the capability undergoes, with the thinking
    /// being that if you are ok with `lag`, then getting something between `lag` and `2 x lag` should be ok.
    pub fn lag_writes_by(lag: mz_repr::Timestamp) -> Self {
        Self::LagWriteFrontier(Arc::new(move |upper| {
            if upper.is_empty() {
                Antichain::from_elem(Timestamp::minimum())
            } else {
                // Subtract the lag from the time, and then round down to a multiple thereof to cut chatter.
                let mut time = upper[0];
                time = time.saturating_sub(lag);
                time = time.saturating_sub(time % lag);
                Antichain::from_elem(time)
            }
        }))
    }
}

impl<T: Timestamp> ReadPolicy<T> {
    pub fn frontier(&self, write_frontier: AntichainRef<T>) -> Antichain<T> {
        match self {
            ReadPolicy::ValidFrom(frontier) => frontier.clone(),
            ReadPolicy::LagWriteFrontier(logic) => logic(write_frontier),
            ReadPolicy::Multiple(policies) => {
                let mut frontier = Antichain::new();
                for policy in policies.iter() {
                    for time in policy.frontier(write_frontier).iter() {
                        frontier.insert(time.clone());
                    }
                }
                frontier
            }
        }
    }
}
