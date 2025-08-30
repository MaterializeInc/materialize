// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Diff, Row};
use mz_storage_client::client::AppendOnlyUpdate;
use mz_storage_client::controller::{IntrospectionType, StorageController, StorageWriteOp};
use mz_storage_types::controller::StorageError;
use timely::progress::Timestamp;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

pub type IntrospectionUpdates = (IntrospectionType, Vec<(Row, Diff)>);

/// Spawn a task sinking introspection updates produced by the compute controller to storage.
pub fn spawn_introspection_sink<T: Timestamp>(
    mut rx: mpsc::UnboundedReceiver<IntrospectionUpdates>,
    storage_controller: &dyn StorageController<Timestamp = T>,
) {
    let sink = IntrospectionSink::new(storage_controller);

    mz_ore::task::spawn(|| "compute-introspection-sink", async move {
        info!("running introspection sink task");

        while let Some((type_, updates)) = rx.recv().await {
            sink.send(type_, updates);
        }

        info!("introspection sink task shutting down");
    });
}

type Notifier<T> = oneshot::Sender<Result<(), StorageError<T>>>;
type AppendOnlySender<T> = mpsc::UnboundedSender<(Vec<AppendOnlyUpdate>, Notifier<T>)>;
type DifferentialSender<T> = mpsc::UnboundedSender<(StorageWriteOp, Notifier<T>)>;

/// A sink for introspection updates produced by the compute controller.
///
/// The sender is connected to the storage controller's CollectionManager, which writes received
/// updates to persist.
#[derive(Debug)]
struct IntrospectionSink<T> {
    /// Sender for [`IntrospectionType::Frontiers`] updates.
    frontiers_tx: DifferentialSender<T>,
    /// Sender for [`IntrospectionType::ReplicaFrontiers`] updates.
    replica_frontiers_tx: DifferentialSender<T>,
    /// Sender for [`IntrospectionType::ComputeDependencies`] updates.
    compute_dependencies_tx: DifferentialSender<T>,
    /// Sender for [`IntrospectionType::ComputeMaterializedViewRefreshes`] updates.
    compute_materialized_view_refreshes_tx: DifferentialSender<T>,
    /// Sender for [`IntrospectionType::WallclockLagHistory`] updates.
    wallclock_lag_history_tx: AppendOnlySender<T>,
    /// Sender for [`IntrospectionType::WallclockLagHistogram`] updates.
    wallclock_lag_histogram_tx: AppendOnlySender<T>,
}

impl<T: Timestamp> IntrospectionSink<T> {
    /// Create a new `IntrospectionSink`.
    pub fn new(storage_controller: &dyn StorageController<Timestamp = T>) -> Self {
        use IntrospectionType::*;
        Self {
            frontiers_tx: storage_controller.differential_introspection_tx(Frontiers),
            replica_frontiers_tx: storage_controller
                .differential_introspection_tx(ReplicaFrontiers),
            compute_dependencies_tx: storage_controller
                .differential_introspection_tx(ComputeDependencies),
            compute_materialized_view_refreshes_tx: storage_controller
                .differential_introspection_tx(ComputeMaterializedViewRefreshes),
            wallclock_lag_history_tx: storage_controller
                .append_only_introspection_tx(WallclockLagHistory),
            wallclock_lag_histogram_tx: storage_controller
                .append_only_introspection_tx(WallclockLagHistogram),
        }
    }

    /// Send a batch of updates of the given introspection type.
    pub fn send(&self, type_: IntrospectionType, updates: Vec<(Row, Diff)>) {
        let send_append_only = |tx: &AppendOnlySender<_>, updates: Vec<_>| {
            let updates = updates.into_iter().map(AppendOnlyUpdate::Row).collect();
            let (notifier, _) = oneshot::channel();
            let _ = tx.send((updates, notifier));
        };
        let send_differential = |tx: &DifferentialSender<_>, updates: Vec<_>| {
            let op = StorageWriteOp::Append { updates };
            let (notifier, _) = oneshot::channel();
            let _ = tx.send((op, notifier));
        };

        use IntrospectionType::*;
        match type_ {
            Frontiers => send_differential(&self.frontiers_tx, updates),
            ReplicaFrontiers => send_differential(&self.replica_frontiers_tx, updates),
            ComputeDependencies => send_differential(&self.compute_dependencies_tx, updates),
            ComputeMaterializedViewRefreshes => {
                send_differential(&self.compute_materialized_view_refreshes_tx, updates);
            }
            WallclockLagHistory => send_append_only(&self.wallclock_lag_history_tx, updates),
            WallclockLagHistogram => send_append_only(&self.wallclock_lag_histogram_tx, updates),
            _ => panic!("unexpected introspection type: {type_:?}"),
        }
    }
}
