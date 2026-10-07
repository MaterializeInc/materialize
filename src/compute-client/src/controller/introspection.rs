// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::{soft_assert_or_log, soft_panic_or_log};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_client::client::AppendOnlyUpdate;
use mz_storage_client::controller::{IntrospectionType, StorageController, StorageWriteOp};
use mz_storage_types::controller::StorageError;
use timely::PartialOrder;
use timely::progress::Antichain;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

pub type IntrospectionUpdates = (IntrospectionType, Vec<(Row, Diff)>);

/// Returns `(last_completed_refresh, next_refresh)` for a REFRESH materialized view.
///
/// `initial_as_of` is the committed first refresh, not the installation as-of.
/// `write_frontier` must be known. An empty frontier means completion, not an
/// absent observation. A finite frontier beyond the first refresh is reported
/// directly as the next refresh.
pub fn refresh_introspection(
    refresh_schedule: &RefreshSchedule,
    initial_as_of: &Antichain<Timestamp>,
    write_frontier: &Antichain<Timestamp>,
) -> (Datum<'static>, Datum<'static>) {
    if write_frontier.is_empty() {
        let last_completed_refresh = if let Some(last_refresh) = refresh_schedule.last_refresh() {
            last_refresh.into()
        } else {
            // For REFRESH EVERY, saturating roundup puts a refresh at MAX.
            Timestamp::MAX.into()
        };
        (last_completed_refresh, Datum::Null)
    } else if PartialOrder::less_equal(write_frontier, initial_as_of) {
        let initial_as_of = initial_as_of
            .as_option()
            .expect("initial_as_of can't be [], because then there would be no refreshes at all");
        let first_refresh = refresh_schedule
            .round_up_timestamp(*initial_as_of)
            .expect("sequencing makes sure that REFRESH MVs always have a first refresh");
        soft_assert_or_log!(
            first_refresh == *initial_as_of,
            "initial_as_of should be set to the first refresh"
        );
        (Datum::Null, first_refresh.into())
    } else {
        let write_frontier = write_frontier.as_option().expect("checked above");
        let last_completed_refresh = refresh_schedule
            .round_down_timestamp_m1(*write_frontier)
            .map_or_else(
                || {
                    soft_panic_or_log!(
                        "rounding down should have returned the first refresh or later"
                    );
                    Datum::Null
                },
                |last_completed_refresh| last_completed_refresh.into(),
            );
        (last_completed_refresh, (*write_frontier).into())
    }
}

/// Spawn a task sinking introspection updates produced by the compute controller to storage.
pub fn spawn_introspection_sink(
    mut rx: mpsc::UnboundedReceiver<IntrospectionUpdates>,
    storage_controller: &dyn StorageController,
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

type Notifier = oneshot::Sender<Result<(), StorageError>>;
type AppendOnlySender = mpsc::UnboundedSender<(Vec<AppendOnlyUpdate>, Notifier)>;
type DifferentialSender = mpsc::UnboundedSender<(StorageWriteOp, Notifier)>;

/// A sink for introspection updates produced by the compute controller.
///
/// The sender is connected to the storage controller's CollectionManager, which writes received
/// updates to persist.
#[derive(Debug)]
struct IntrospectionSink {
    /// Sender for [`IntrospectionType::Frontiers`] updates.
    frontiers_tx: DifferentialSender,
    /// Sender for [`IntrospectionType::ReplicaFrontiers`] updates.
    replica_frontiers_tx: DifferentialSender,
    /// Sender for [`IntrospectionType::ComputeDependencies`] updates.
    compute_dependencies_tx: DifferentialSender,
    /// Sender for [`IntrospectionType::ComputeMaterializedViewRefreshes`] updates.
    compute_materialized_view_refreshes_tx: DifferentialSender,
    /// Sender for [`IntrospectionType::WallclockLagHistory`] updates.
    wallclock_lag_history_tx: AppendOnlySender,
    /// Sender for [`IntrospectionType::WallclockLagHistogram`] updates.
    wallclock_lag_histogram_tx: AppendOnlySender,
}

impl IntrospectionSink {
    /// Create a new `IntrospectionSink`.
    pub fn new(storage_controller: &dyn StorageController) -> Self {
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
        let send_append_only = |tx: &AppendOnlySender, updates: Vec<_>| {
            let updates = updates.into_iter().map(AppendOnlyUpdate::Row).collect();
            let (notifier, _) = oneshot::channel();
            let _ = tx.send((updates, notifier));
        };
        let send_differential = |tx: &DifferentialSender, updates: Vec<_>| {
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
