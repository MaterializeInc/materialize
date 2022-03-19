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

use crate::client::{
    Client, Command, ComputeCommand, ComputeInstanceId, ComputeResponse, InstanceConfig, Response,
    StorageResponse,
};
use crate::logging::LoggingConfig;

pub use storage::{StorageController, StorageControllerMut, StorageControllerState};
mod storage;
pub use compute::{ComputeController, ComputeControllerMut};
mod compute;

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<C, T = mz_repr::Timestamp> {
    /// The underlying client,
    client: C,
    storage: StorageControllerState<T>,
    compute: BTreeMap<ComputeInstanceId, compute::ComputeControllerState<T>>,
}

impl<C: Client, T> Controller<C, T>
where
    T: Timestamp + Lattice,
{
    pub async fn create_instance(
        &mut self,
        instance: ComputeInstanceId,
        config: InstanceConfig,
        logging: Option<LoggingConfig>,
    ) -> Result<(), anyhow::Error> {
        self.compute
            .insert(instance, compute::ComputeControllerState::new(&logging));
        self.client
            .send(Command::Compute(
                ComputeCommand::CreateInstance(config, logging),
                instance,
            ))
            .await
    }
    pub async fn drop_instance(
        &mut self,
        instance: ComputeInstanceId,
    ) -> Result<(), anyhow::Error> {
        self.compute.remove(&instance);
        self.client
            .send(Command::Compute(ComputeCommand::DropInstance, instance))
            .await
    }
}

impl<C: Client<T>, T> Controller<C, T> {
    /// Acquires an immutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage(&self) -> StorageController<T> {
        StorageController {
            storage: &self.storage,
        }
    }

    /// Acquires a mutable handle to a controller for the storage instance.
    #[inline]
    pub fn storage_mut(&mut self) -> StorageControllerMut<C, T> {
        StorageControllerMut {
            storage: &mut self.storage,
            client: &mut self.client,
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
    pub fn compute_mut(
        &mut self,
        instance: ComputeInstanceId,
    ) -> Option<ComputeControllerMut<C, T>> {
        let compute = self.compute.get_mut(&instance)?;
        // A compute instance contains `self.storage` so that it can form a `StorageController` if it needs.
        Some(ComputeControllerMut {
            instance,
            compute,
            storage: &mut self.storage,
            client: &mut self.client,
        })
    }
}

impl<C: Client<T>, T: Timestamp + Lattice> Controller<C, T> {
    pub async fn recv(&mut self) -> Option<Response<T>> {
        let response = self.client.next().await;
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
                Response::Storage(StorageResponse::TimestampBindings(feedback)) => {
                    self.storage_mut()
                        .update_write_frontiers(&feedback.changes)
                        .await;
                }
                _ => {}
            }
        }
        response
    }
}

impl<C> Controller<C> {
    /// Create a new controller from a client it should wrap.
    pub fn new(client: C) -> Self {
        Self {
            client,
            storage: StorageControllerState::new(),
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
    LagWriteFrontier(#[derivative(Debug = "ignore")] Arc<dyn Fn(AntichainRef<T>) -> Antichain<T>>),
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
