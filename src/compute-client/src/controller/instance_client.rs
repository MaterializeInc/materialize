// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A client for communicating with a compute instance task.
//!
//! This module provides the public interface for external callers (like the adapter)
//! to interact with compute instances directly for operations like peek sequencing.
//! It also has a number of `pub(super)` functions, which are for the `ComputeController`'s benefit.

use std::sync::Arc;

use mz_build_info::BuildInfo;
use mz_cluster_client::WallclockLagFn;
use mz_compute_types::ComputeInstanceId;
use mz_dyncfg::ConfigSet;
use mz_expr::RowSetFinishing;
use mz_ore::now::NowFn;
use mz_ore::tracing::OpenTelemetryContext;
use mz_persist_types::PersistLocation;
use mz_repr::{GlobalId, RelationDesc, Row};
use mz_storage_types::read_holds::{self, ReadHold};
use thiserror::Error;
use timely::progress::{Antichain, ChangeBatch};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tracing::debug_span;
use uuid::Uuid;

use crate::controller::error::CollectionLookupError;
use crate::controller::instance::{Command, Instance, SharedCollectionState};
use crate::controller::{
    ComputeControllerResponse, ComputeControllerTimestamp, IntrospectionUpdates, ReplicaId,
    StorageCollections,
};
use crate::logging::LogVariant;
use crate::metrics::InstanceMetrics;
use crate::protocol::command::PeekTarget;
use crate::protocol::response::PeekResponse;

/// Error indicating the instance has shut down.
#[derive(Error, Debug)]
#[error("the instance has shut down")]
pub struct InstanceShutDown;

/// Errors arising during peek processing.
#[derive(Error, Debug)]
pub enum PeekError {
    /// The replica that the peek was issued against does not exist.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    /// The read hold that was passed in is against the wrong collection.
    #[error("read hold ID does not match peeked collection: {0}")]
    ReadHoldIdMismatch(GlobalId),
    /// The read hold that was passed in is for a later time than the peek's timestamp.
    #[error("insufficient read hold provided: {0}")]
    ReadHoldInsufficient(GlobalId),
    /// The peek's target instance has shut down.
    #[error("the instance has shut down")]
    InstanceShutDown,
}

impl From<InstanceShutDown> for PeekError {
    fn from(_error: InstanceShutDown) -> Self {
        Self::InstanceShutDown
    }
}

/// A client for an `Instance` task.
#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
pub struct InstanceClient<T: ComputeControllerTimestamp> {
    /// A sender for commands for the instance.
    command_tx: mpsc::UnboundedSender<Command<T>>,
    /// A sender for read hold changes for collections installed on the instance.
    #[derivative(Debug = "ignore")]
    read_hold_tx: read_holds::ChangeTx<T>,
}

impl<T: ComputeControllerTimestamp> InstanceClient<T> {
    pub(super) fn read_hold_tx(&self) -> read_holds::ChangeTx<T> {
        Arc::clone(&self.read_hold_tx)
    }

    /// Call a method to be run on the instance task, by sending a message to the instance.
    /// Does not wait for a response message.
    pub(super) fn call<F>(&self, f: F) -> Result<(), InstanceShutDown>
    where
        F: FnOnce(&mut Instance<T>) + Send + 'static,
    {
        let otel_ctx = OpenTelemetryContext::obtain();
        self.command_tx
            .send(Box::new(move |instance| {
                let _span = debug_span!("instance_client::call").entered();
                otel_ctx.attach_as_parent();

                f(instance)
            }))
            .map_err(|_send_error| InstanceShutDown)
    }

    /// Call a method to be run on the instance task, by sending a message to the instance and
    /// waiting for a response message.
    pub(super) async fn call_sync<F, R>(&self, f: F) -> Result<R, InstanceShutDown>
    where
        F: FnOnce(&mut Instance<T>) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let otel_ctx = OpenTelemetryContext::obtain();
        self.command_tx
            .send(Box::new(move |instance| {
                let _span = debug_span!("instance_client::call_sync").entered();
                otel_ctx.attach_as_parent();
                let result = f(instance);
                let _ = tx.send(result);
            }))
            .map_err(|_send_error| InstanceShutDown)?;

        rx.await.map_err(|_| InstanceShutDown)
    }

    pub(super) fn spawn(
        id: ComputeInstanceId,
        build_info: &'static BuildInfo,
        storage: StorageCollections<T>,
        peek_stash_persist_location: PersistLocation,
        arranged_logs: Vec<(LogVariant, GlobalId, SharedCollectionState<T>)>,
        metrics: InstanceMetrics,
        now: NowFn,
        wallclock_lag: WallclockLagFn<T>,
        dyncfg: Arc<ConfigSet>,
        response_tx: mpsc::UnboundedSender<ComputeControllerResponse<T>>,
        introspection_tx: mpsc::UnboundedSender<IntrospectionUpdates>,
        read_only: bool,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let read_hold_tx: read_holds::ChangeTx<_> = {
            let command_tx = command_tx.clone();
            Arc::new(move |id, change: ChangeBatch<_>| {
                let cmd: Command<_> = {
                    let change = change.clone();
                    Box::new(move |i| i.apply_read_hold_change(id, change.clone()))
                };
                command_tx.send(cmd).map_err(|_| SendError((id, change)))
            })
        };

        mz_ore::task::spawn(
            || format!("compute-instance-{id}"),
            Instance::new(
                build_info,
                storage,
                peek_stash_persist_location,
                arranged_logs,
                metrics,
                now,
                wallclock_lag,
                dyncfg,
                command_rx,
                response_tx,
                Arc::clone(&read_hold_tx),
                introspection_tx,
                read_only,
            )
            .run(),
        );

        Self {
            command_tx,
            read_hold_tx,
        }
    }

    /// Acquires a `ReadHold` and collection write frontier for each of the identified compute
    /// collections.
    pub async fn acquire_read_holds_and_collection_write_frontiers(
        &self,
        ids: Vec<GlobalId>,
    ) -> Result<Vec<(GlobalId, ReadHold<T>, Antichain<T>)>, CollectionLookupError> {
        self.call_sync(move |i| {
            let mut result = Vec::new();
            for id in ids.into_iter() {
                result.push((
                    id,
                    i.acquire_read_hold(id)?,
                    i.collection_write_frontier(id)?,
                ));
            }
            Ok(result)
        })
        .await?
    }

    /// Issue a peek by calling into the instance task.
    ///
    /// If this returns an error, then it didn't modify any `Instance` state.
    pub async fn peek(
        &self,
        peek_target: PeekTarget,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: T,
        result_desc: RelationDesc,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_read_hold: ReadHold<T>,
        target_replica: Option<ReplicaId>,
        peek_response_tx: oneshot::Sender<PeekResponse>,
    ) -> Result<(), PeekError> {
        self.call_sync(move |i| {
            i.peek(
                peek_target,
                literal_constraints,
                uuid,
                timestamp,
                result_desc,
                finishing,
                map_filter_project,
                target_read_hold,
                target_replica,
                peek_response_tx,
            )
        })
        .await?
    }
}
