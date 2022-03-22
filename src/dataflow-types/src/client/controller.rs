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
use timely::progress::frontier::{Antichain, AntichainRef};
use timely::progress::Timestamp;
use tokio_stream::StreamMap;

use crate::client::{
    ComputeClient, ComputeCommand, ComputeInstanceId, ComputeResponse, ComputeWrapperClient,
    InstanceConfig, RemoteClient, Response, StorageClient, StorageResponse, VirtualComputeHost,
};

pub use storage::{StorageController, StorageControllerMut, StorageControllerState};
mod storage;
pub use compute::{ComputeController, ComputeControllerMut};
mod compute;

/// A client that maintains soft state and validates commands, in addition to forwarding them.
///
/// NOTE(benesch): I find the fact that this type is called `Controller` but is
/// referred to as the `dataflow_client` in the coordinator to be very
/// confusing. We should find the one correct name, and use it everywhere!
pub struct Controller<T = mz_repr::Timestamp> {
    storage: StorageControllerState<T>,
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
    ) -> Result<(), anyhow::Error> {
        let (mut client, logging) = match config {
            InstanceConfig::Virtual { logging } => {
                let client = self.virtual_compute_host.create_instance(instance);
                (client, logging)
            }
            InstanceConfig::Remote { hosts, logging } => {
                let client = RemoteClient::connect(&hosts).await?;
                let client: Box<dyn ComputeClient<T>> =
                    Box::new(ComputeWrapperClient::new(client, instance));
                (client, logging)
            }
        };
        client
            .send(ComputeCommand::CreateInstance(logging.clone()))
            .await?;
        self.compute.insert(
            instance,
            compute::ComputeControllerState::new(client, &logging),
        );
        Ok(())
    }
    pub async fn drop_instance(
        &mut self,
        instance: ComputeInstanceId,
    ) -> Result<(), anyhow::Error> {
        if let Some(mut compute) = self.compute.remove(&instance) {
            compute.client.send(ComputeCommand::DropInstance).await?;
            self.virtual_compute_host.drop_instance(instance);
        }
        Ok(())
    }
}

impl<T> Controller<T> {
    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> StorageController<T> {
        StorageController {
            storage: &self.storage,
        }
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(&mut self) -> StorageControllerMut<T> {
        StorageControllerMut {
            storage: &mut self.storage,
        }
    }

    /// Acquires an immutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute(&self, instance: ComputeInstanceId) -> Option<ComputeController<T>> {
        let compute = self.compute.get(&instance)?;
        // A compute instance contains `self.storage` so that it can form a `StorageController` if it needs.
        Some(ComputeController {
            _instance: instance,
            compute,
            storage: &self.storage,
        })
    }

    /// Acquires a mutable handle to a controller for the indicated compute instance, if it exists.
    #[inline]
    pub fn compute_mut(&mut self, instance: ComputeInstanceId) -> Option<ComputeControllerMut<T>> {
        let compute = self.compute.get_mut(&instance)?;
        // A compute instance contains `self.storage` so that it can form a `StorageController` if it needs.
        Some(ComputeControllerMut {
            instance,
            compute,
            storage: &mut self.storage,
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
            response = self.storage.client.recv() => {
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
                        .await;
                }
                Response::Compute(ComputeResponse::PeekResponse(uuid, _response), instance) => {
                    self.compute_mut(*instance)
                        .expect("Reference to absent instance")
                        .remove_peeks(std::iter::once(*uuid))
                        .await;
                }
                Response::Storage(StorageResponse::TimestampBindings(feedback)) => {
                    self.storage_mut()
                        .update_write_frontiers(&feedback.changes)
                        .await;
                }
                _ => {}
            }
        }
        Ok(response)
    }
}

impl<T> Controller<T> {
    /// Create a new controller from a client it should wrap.
    pub fn new(
        storage_client: Box<dyn StorageClient<T>>,
        virtual_compute_host: VirtualComputeHost<T>,
    ) -> Self {
        Self {
            storage: StorageControllerState::new(storage_client),
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
