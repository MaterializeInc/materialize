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
use mz_repr::{GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::read_holds::{self, ReadHold};
use thiserror::Error;
use timely::progress::{Antichain, ChangeBatch};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tracing::debug_span;
use uuid::Uuid;

use crate::controller::error::CollectionMissing;
use crate::controller::instance::{Command, Instance, SharedCollectionState};
use crate::controller::peek_lifecycle::PeekLifecycle;
use crate::controller::{
    ComputeControllerResponse, IntrospectionUpdates, ReplicaId, StorageCollections,
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

/// Errors arising from [`InstanceClient::acquire_read_holds_and_collection_write_frontiers`].
#[derive(Error, Debug)]
pub enum AcquireReadHoldsError {
    /// The compute collection does not exist.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// The instance has shut down.
    #[error("the instance has shut down")]
    InstanceShutDown,
}

impl From<CollectionMissing> for AcquireReadHoldsError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

impl From<InstanceShutDown> for AcquireReadHoldsError {
    fn from(_error: InstanceShutDown) -> Self {
        Self::InstanceShutDown
    }
}

/// A client for an `Instance` task.
#[derive(Clone, derivative::Derivative)]
#[derivative(Debug)]
pub struct InstanceClient {
    /// A sender for commands for the instance.
    command_tx: mpsc::UnboundedSender<Command>,
    /// A sender for read hold changes for collections installed on the instance.
    #[derivative(Debug = "ignore")]
    read_hold_tx: read_holds::ChangeTx,
}

impl InstanceClient {
    pub(super) fn read_hold_tx(&self) -> read_holds::ChangeTx {
        Arc::clone(&self.read_hold_tx)
    }

    /// Call a method to be run on the instance task, by sending a message to the instance.
    /// Does not wait for a response message.
    pub(super) fn call<F>(&self, f: F) -> Result<(), InstanceShutDown>
    where
        F: FnOnce(&mut Instance) + Send + 'static,
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
        F: FnOnce(&mut Instance) -> R + Send + 'static,
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
        storage: StorageCollections,
        peek_stash_persist_location: PersistLocation,
        arranged_logs: Vec<(LogVariant, GlobalId, SharedCollectionState)>,
        metrics: InstanceMetrics,
        now: NowFn,
        wallclock_lag: WallclockLagFn<Timestamp>,
        dyncfg: Arc<ConfigSet>,
        response_tx: mpsc::UnboundedSender<ComputeControllerResponse>,
        introspection_tx: mpsc::UnboundedSender<IntrospectionUpdates>,
        read_only: bool,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::unbounded_channel();

        let read_hold_tx: read_holds::ChangeTx = {
            let command_tx = command_tx.clone();
            Arc::new(move |id, change: ChangeBatch<_>| {
                let cmd: Command = {
                    let change = change.clone();
                    Box::new(move |i| i.apply_read_hold_change(id, change))
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
    ) -> Result<Vec<(GlobalId, ReadHold, Antichain<Timestamp>)>, AcquireReadHoldsError> {
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

    /// Issues a peek on the instance task.
    ///
    /// With a `lifecycle`, issue is ordered against cancellation and terminal
    /// notifications go to the lifecycle, not the coordinator. If the peek was
    /// already retired, sends the saved response through `peek_response_tx`
    /// without issuing compute work and returns `Ok(())`. The caller must retire
    /// the lifecycle if this returns `Err`, if it drops this future, or if the
    /// instance shuts down.
    pub async fn peek(
        &self,
        peek_target: PeekTarget,
        literal_constraints: Option<Vec<Row>>,
        uuid: Uuid,
        timestamp: Timestamp,
        result_desc: RelationDesc,
        finishing: RowSetFinishing,
        map_filter_project: mz_expr::SafeMfpPlan,
        target_read_hold: ReadHold,
        target_replica: Option<ReplicaId>,
        peek_response_tx: oneshot::Sender<PeekResponse>,
        lifecycle: Option<Arc<PeekLifecycle>>,
    ) -> Result<(), PeekError> {
        let client = self.clone();
        self.call_sync(move |i| {
            let mut response_tx = Some(peek_response_tx);
            let issue = || {
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
                    response_tx.take().expect("peek issued once"),
                    lifecycle.clone(),
                )
            };
            let Some(lifecycle) = &lifecycle else {
                return issue();
            };
            // Run the ordering critical section on the instance task, not
            // around call_sync's await, which would block cancellation.
            let retired = lifecycle.issue(issue, move |reason| {
                // A shut down instance has no peek left to cancel.
                let _ = client.call(move |i| i.cancel_peek(uuid, reason));
            })?;
            if let Some(response) = retired {
                let _ = response_tx
                    .take()
                    .expect("retired peek was not issued")
                    .send(response);
            }
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Poll;

    use mz_expr::{MapFilterProject, RowSetFinishing, SafeMfpPlan};
    use mz_repr::{GlobalId, RelationDesc, Timestamp};
    use mz_storage_types::read_holds::ReadHold;
    use timely::progress::Antichain;
    use tokio::sync::{mpsc, oneshot};
    use uuid::Uuid;

    use crate::controller::PeekNotification;
    use crate::controller::instance::tests::peek_test_instance;
    use crate::controller::peek_lifecycle::PeekLifecycle;
    use crate::protocol::command::PeekTarget;
    use crate::protocol::response::{PeekError, PeekResponse};

    use super::InstanceClient;

    #[mz_ore::test(tokio::test)]
    async fn cancellation_before_instance_processes_issue_delivers_result() {
        for response in [
            PeekResponse::Canceled,
            PeekResponse::Error(PeekError::unstructured("dependency dropped")),
        ] {
            let (mut instance, mut coordinator) = peek_test_instance();
            let (command_tx, mut command_rx) = mpsc::unbounded_channel();
            let client = InstanceClient {
                command_tx,
                read_hold_tx: Arc::new(|_, _| Ok(())),
            };
            let (retired_tx, mut retired_rx) = oneshot::channel();
            let lifecycle = Arc::new(PeekLifecycle::new(move |outcome| {
                retired_tx.send(outcome).unwrap();
            }));
            let (result_tx, result_rx) = oneshot::channel();
            // A mismatched hold would fail validation if Instance::peek ran.
            let hold = ReadHold::new(
                GlobalId::User(2),
                Antichain::from_elem(Timestamp::MIN),
                Arc::new(|_, _| Ok(())),
            );
            let peek = client.peek(
                PeekTarget::Index {
                    id: GlobalId::User(1),
                },
                None,
                Uuid::new_v4(),
                Timestamp::MIN,
                RelationDesc::empty(),
                RowSetFinishing::trivial(0),
                SafeMfpPlan::from_mfp(MapFilterProject::new(0)),
                hold,
                None,
                result_tx,
                Some(Arc::clone(&lifecycle)),
            );
            futures::pin_mut!(peek);
            assert!(matches!(futures::poll!(&mut peek), Poll::Pending));
            let issue = command_rx.try_recv().expect("issue queued");
            lifecycle.cancel(response.clone());
            assert_eq!(retired_rx.try_recv().unwrap(), PeekNotification::Canceled);
            issue(&mut instance);
            peek.await
                .expect("canceled issue succeeds without validation");
            assert_eq!(result_rx.await.unwrap(), response);
            assert!(
                command_rx.try_recv().is_err(),
                "unissued peek needs no cancel command"
            );
            assert!(coordinator.try_recv().is_err());
        }
    }
}
