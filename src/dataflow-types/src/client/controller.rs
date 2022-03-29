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

use anyhow::bail;
use derivative::Derivative;
use differential_dataflow::lattice::Lattice;
use futures::StreamExt;
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::Timestamp;
use tokio_stream::StreamMap;

use mz_orchestrator::{Orchestrator, ServiceConfig};

use crate::client::GenericClient;
use crate::client::{
    ComputeClient, ComputeCommand, ComputeInstanceId, ComputeResponse, ComputeWrapperClient,
    InstanceConfig, RemoteClient, Response, StorageResponse, VirtualComputeHost,
};
use crate::logging::LoggingConfig;

pub use storage::{StorageController, StorageControllerState};
pub mod storage;
pub use compute::{ComputeController, ComputeControllerMut};
mod compute;

/// Configures an orchestrator for the controller.
pub struct OrchestratorConfig {
    /// The orchestrator implementation to use.
    pub orchestrator: Box<dyn Orchestrator>,
    /// The dataflowd image to use when starting new compute instances.
    pub dataflowd_image: String,
    /// The storage address that compute instances should connect to.
    pub storage_addr: String,
    /// The number of storage workers in the storage instance.
    ///
    /// TODO(benesch,antiguru): make this something that is discovered in the
    /// handshake between compute and storage.
    pub storage_workers: usize,
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
///
/// NOTE(benesch): I find the fact that this type is called `Controller` but is
/// referred to as the `dataflow_client` in the coordinator to be very
/// confusing. We should find the one correct name, and use it everywhere!
pub struct Controller<T = mz_repr::Timestamp> {
    orchestrator: Option<OrchestratorConfig>,
    storage_controller: Box<dyn StorageController<Timestamp = T>>,
    compute: BTreeMap<ComputeInstanceId, compute::ComputeControllerState<T>>,
    virtual_compute_host: VirtualComputeHost<T>,
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
            InstanceConfig::Virtual => {
                let client = self.virtual_compute_host.create_instance(instance);
                self.compute_mut(instance)
                    .unwrap()
                    .add_replica("default".into(), client)
                    .await;
            }
            InstanceConfig::Remote { replicas } => {
                let mut compute_instance = self.compute_mut(instance).unwrap();
                for (name, hosts) in replicas {
                    let client = RemoteClient::new(&hosts.into_iter().collect::<Vec<_>>());
                    let client = ComputeWrapperClient::new(client, instance);
                    let client: Box<dyn ComputeClient<T>> = Box::new(client);
                    compute_instance.add_replica(name, client).await;
                }
            }
            InstanceConfig::Managed { size: _ } => {
                let OrchestratorConfig {
                    orchestrator,
                    storage_addr,
                    storage_workers,
                    dataflowd_image,
                } = match &mut self.orchestrator {
                    Some(orchestrator) => orchestrator,
                    // TODO(benesch): bailing here is too late. Something
                    // earlier needs to recognize when we can't create managed
                    // instances.
                    _ => bail!("cannot create managed instances in this configuration"),
                };
                let service = orchestrator
                    .namespace("compute")
                    .ensure_service(
                        &format!("instance-{instance}"),
                        ServiceConfig {
                            image: dataflowd_image.clone(),
                            args: vec![
                                "--runtime=compute".into(),
                                format!("--storage-workers={storage_workers}"),
                                format!("--storage-addr={storage_addr}"),
                                "0.0.0.0:2101".into(),
                            ],
                            ports: vec![2101, 6876],
                            // TODO: use `size` to set these.
                            cpu_limit: None,
                            memory_limit: None,
                            // TODO: support sizes large enough to warrant multiple processes.
                            processes: 1,
                        },
                    )
                    .await?;
                let addrs: Vec<_> = service
                    .hosts()
                    .iter()
                    .map(|h| format!("{h}:6876"))
                    .collect();
                let client = RemoteClient::new(&addrs);
                let client = ComputeWrapperClient::new(client, instance);
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
            if let Some(OrchestratorConfig { orchestrator, .. }) = &mut self.orchestrator {
                orchestrator
                    .namespace("compute")
                    .drop_service(&format!("instance-{instance}"))
                    .await?;
            }
            compute.client.send(ComputeCommand::DropInstance).await?;
            self.virtual_compute_host.drop_instance(instance);
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
        let response: Option<Response<T>> = tokio::select! {
            Some((instance, response)) = compute_stream.next() => {
                Some(Response::Compute(response?, instance))
            }
            response = self.storage_controller.recv() => {
                response?.map(Response::Storage)
            }
            else => None,
        };
        drop(compute_stream);
        if let Some(response) = response.as_ref() {
            match response {
                Response::Compute(ComputeResponse::FrontierUppers(updates), instance) => {
                    self.compute_mut(*instance)
                        // TODO: determine if this is an error, or perhaps just a late
                        // response about a terminated instance.
                        .expect("Reference to absent instance")
                        .update_write_frontiers(updates)
                        .await?;
                }
                Response::Compute(ComputeResponse::PeekResponse(uuid, _response), instance) => {
                    self.compute_mut(*instance)
                        .expect("Reference to absent instance")
                        .remove_peeks(std::iter::once(*uuid))
                        .await?;
                }
                Response::Storage(StorageResponse::TimestampBindings(feedback)) => {
                    self.storage_controller
                        .update_write_frontiers(&feedback.changes)
                        .await?;

                    self.storage_mut()
                        .persist_timestamp_bindings(&feedback)
                        .await?;
                }
                _ => {}
            }
        }
        Ok(response)
    }
}

impl<T> Controller<T> {
    /// Create a new controller from a client it should wrap.
    pub fn new<S: StorageController<Timestamp = T> + 'static>(
        orchestrator: Option<OrchestratorConfig>,
        storage_controller: S,
        virtual_compute_host: VirtualComputeHost<T>,
    ) -> Self {
        Self {
            orchestrator,
            storage_controller: Box::new(storage_controller),
            compute: BTreeMap::default(),
            virtual_compute_host,
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
