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

use std::mem;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use differential_dataflow::lattice::Lattice;
use serde::{Deserialize, Serialize};
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use uuid::Uuid;

use mz_build_info::BuildInfo;
use mz_compute_client::command::ReplicaId;
use mz_compute_client::controller::{
    ActiveComputeController, ComputeController, ComputeControllerResponse,
};
use mz_compute_client::response::{PeekResponse, SubscribeResponse};
use mz_compute_client::service::{ComputeClient, ComputeGrpcClient};
use mz_orchestrator::Orchestrator;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistLocation;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::{GlobalId, TimestampManipulation};
use mz_storage::controller::StorageController;
use mz_storage::protocol::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse,
};

/// Configures a controller.
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// The orchestrator implementation to use.
    pub orchestrator: Arc<dyn Orchestrator>,
    /// The persist location where all storage collections will be written to.
    pub persist_location: PersistLocation,
    /// A process-global cache of (blob_uri, consensus_uri) ->
    /// PersistClient.
    /// This is intentionally shared between workers.
    pub persist_clients: Arc<Mutex<PersistClientCache>>,
    /// The stash URL for the storage controller.
    pub storage_stash_url: String,
    /// The storaged image to use when starting new storage processes.
    pub storaged_image: String,
    /// The computed image to use when starting new compute processes.
    pub computed_image: String,
}

/// Responses that [`Controller`] can produce.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ControllerResponse<T = mz_repr::Timestamp> {
    /// The worker's response to a specified (by connection id) peek.
    ///
    /// Additionally, an `OpenTelemetryContext` to forward trace information
    /// back into coord. This allows coord traces to be children of work
    /// done in compute!
    PeekResponse(Uuid, PeekResponse, OpenTelemetryContext),
    /// The worker's next response to a specified subscribe.
    SubscribeResponse(GlobalId, SubscribeResponse<T>),
    /// Notification that we have received a message from the given compute replica
    /// at the given time.
    ComputeReplicaHeartbeat(ReplicaId, DateTime<Utc>),
}

impl<T> From<ComputeControllerResponse<T>> for ControllerResponse<T> {
    fn from(r: ComputeControllerResponse<T>) -> ControllerResponse<T> {
        match r {
            ComputeControllerResponse::PeekResponse(uuid, peek, otel_ctx) => {
                ControllerResponse::PeekResponse(uuid, peek, otel_ctx)
            }
            ComputeControllerResponse::SubscribeResponse(id, tail) => {
                ControllerResponse::SubscribeResponse(id, tail)
            }
            ComputeControllerResponse::ReplicaHeartbeat(id, when) => {
                ControllerResponse::ComputeReplicaHeartbeat(id, when)
            }
        }
    }
}

/// Whether one of the underlying controllers is ready for their `process`
/// method to be called.
#[derive(Default)]
enum Readiness {
    /// No underlying controllers are ready.
    #[default]
    NotReady,
    /// The storage controller is ready.
    Storage,
    /// The compute controller is ready.
    Compute,
}

/// A client that maintains soft state and validates commands, in addition to forwarding them.
pub struct Controller<T = mz_repr::Timestamp> {
    pub storage: Box<dyn StorageController<Timestamp = T>>,
    pub compute: ComputeController<T>,
    readiness: Readiness,
}

impl<T> Controller<T> {
    pub fn active_compute(&mut self) -> ActiveComputeController<T> {
        self.compute.activate(&mut *self.storage)
    }
}

impl<T> Controller<T>
where
    T: Timestamp + Lattice,
    ComputeGrpcClient: ComputeClient<T>,
{
    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before implementing prior commands,
    /// and so it is important for a user to invoke this method as soon as it is comfortable.
    /// This method can be invoked immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.storage.initialization_complete();
        self.compute.initialization_complete();
    }

    /// Waits until the controller is ready to process a response.
    ///
    /// This method may block for an arbitrarily long time.
    ///
    /// When the method returns, the owner should call [`Controller::ready`] to
    /// process the ready message.
    ///
    /// This method is cancellation safe.
    pub async fn ready(&mut self) {
        if let Readiness::NotReady = self.readiness {
            // The underlying `ready` methods are cancellation safe, so it is
            // safe to construct this `select!`.
            tokio::select! {
                () = self.storage.ready() => {
                    self.readiness = Readiness::Storage;
                }
                () = self.compute.ready() => {
                    self.readiness = Readiness::Compute;
                }
            }
        }
    }

    /// Processes the work queued by [`Controller::ready`].
    ///
    /// This method is guaranteed to return "quickly" unless doing so would
    /// compromise the correctness of the system.
    ///
    /// This method is **not** guaranteed to be cancellation safe. It **must**
    /// be awaited to completion.
    pub async fn process(&mut self) -> Result<Option<ControllerResponse<T>>, anyhow::Error> {
        match mem::take(&mut self.readiness) {
            Readiness::NotReady => Ok(None),
            Readiness::Storage => {
                self.storage.process().await?;
                Ok(None)
            }
            Readiness::Compute => {
                let response = self.active_compute().process().await?;
                Ok(response.map(Into::into))
            }
        }
    }
}

impl<T> Controller<T>
where
    T: Timestamp
        + Lattice
        + TotalOrder
        + TryInto<i64>
        + TryFrom<i64>
        + Codec64
        + Unpin
        + TimestampManipulation,
    <T as TryInto<i64>>::Error: std::fmt::Debug,
    <T as TryFrom<i64>>::Error: std::fmt::Debug,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
    mz_storage::controller::Controller<T>: StorageController<Timestamp = T>,
{
    /// Creates a new controller.
    pub async fn new(config: ControllerConfig) -> Self {
        let storage_controller = mz_storage::controller::Controller::new(
            config.build_info,
            config.storage_stash_url,
            config.persist_location,
            config.persist_clients,
            config.orchestrator.namespace("storage"),
            config.storaged_image,
        )
        .await;

        let compute_controller = ComputeController::new(
            config.build_info,
            config.orchestrator.namespace("compute"),
            config.computed_image,
        );

        Self {
            storage: Box::new(storage_controller),
            compute: compute_controller,
            readiness: Readiness::NotReady,
        }
    }
}
