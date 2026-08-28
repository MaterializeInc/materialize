// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Coordinator-side support machinery for (frontend) read-then write.
//!
//! TODO(aljoscha): Write submission still goes through the coordinator. In the
//! long run we want a group-commit task that runs independently, so that
//! session tasks can submit write requests to it directly.

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::memory::objects::CatalogItem;
use mz_repr::CatalogItemId;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::catalog::CatalogItemType;
use mz_sql::plan::SubscribeOutput;
use mz_storage_client::client::TableData;
use smallvec::smallvec;
use tokio::sync::mpsc;
use tracing::Span;

use crate::PeekResponseUnary;
use crate::active_compute_sink::{ActiveComputeSink, ActiveSubscribe, ActiveSubscribeOwner};
use crate::catalog::Catalog;
use crate::command::WriteAttemptKind;
use crate::coord::Coordinator;
use crate::coord::appends::{
    InternalWriteResponder, PendingWriteTxn, TableWriteCmd, TimestampedWriteRequest,
    UserWriteResponder, WriteResult, WriteTarget,
};
use crate::error::AdapterError;
use mz_ore::soft_panic_or_log;

/// Adds `id` to the worklist the first time it is seen, enforcing the
/// dependency bound.
///
/// Deduping at enqueue time keeps `seen` and `stack` proportional to the number
/// of distinct objects, not the number of dependency edges. A diamond-shaped
/// graph is validated once per object.
fn enqueue(
    seen: &mut BTreeSet<CatalogItemId>,
    stack: &mut Vec<CatalogItemId>,
    id: CatalogItemId,
    max_rw_dependencies: usize,
) -> Result<(), AdapterError> {
    if seen.insert(id) {
        if seen.len() > max_rw_dependencies {
            return Err(AdapterError::ReadThenWriteDependencyLimitExceeded {
                max_rw_dependencies,
            });
        }
        stack.push(id);
    }
    Ok(())
}

impl Coordinator {
    /// Creates a subscribe that writes no `mz_subscriptions` row.
    ///
    /// The dataflow is otherwise ordinary and shows up in replica
    /// introspection like any other.
    ///
    /// Takes ownership of `read_holds` and drops them only once the dataflow is
    /// shipped, so the `since` cannot advance past `as_of` in between.
    ///
    /// Answers through `response_tx`, with an error if the owning connection
    /// went away or if a dependency was dropped since the plan was optimized.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn handle_create_internal_subscribe(
        &mut self,
        df_desc: crate::optimize::LirDataflowDescription,
        cluster_id: mz_compute_types::ComputeInstanceId,
        replica_id: Option<mz_cluster_client::ReplicaId>,
        depends_on: BTreeSet<GlobalId>,
        as_of: Timestamp,
        arity: usize,
        sink_id: GlobalId,
        owner: ActiveSubscribeOwner,
        start_time: mz_ore::now::EpochMillis,
        read_holds: crate::ReadHolds,
        response_tx: tokio::sync::oneshot::Sender<
            Result<mpsc::UnboundedReceiver<PeekResponseUnary>, AdapterError>,
        >,
    ) {
        match &owner {
            // The client may have disconnected while we waited for the semaphore.
            ActiveSubscribeOwner::Session { conn_id, .. } => {
                if !self.active_conns.contains_key(conn_id) {
                    let _ = response_tx.send(Err(AdapterError::Canceled));
                    return;
                }
            }
            // Background work has no connection to lose.
            ActiveSubscribeOwner::Background => {}
        }

        let (tx, rx) = mpsc::unbounded_channel();

        let active_subscribe = ActiveSubscribe {
            owner,
            channel: tx,
            backlog_accounting: std::sync::Arc::new(std::sync::Mutex::new(
                crate::active_compute_sink::SubscribeBacklogAccounting::default(),
            )),
            // This internal subscribe is drained by the coordinator for OCC
            // read-then-write, not by a slow external client, so the slow-client
            // backlog budget must not apply. A large read-then-write read set
            // (e.g. an UPDATE that rewrites every row of a big table)
            // legitimately exceeds the budget, so bounding it here would
            // spuriously retire the statement with `SubscribeFellBehind`.
            max_buffered_bytes: usize::MAX,
            emit_progress: true, // We need progress updates for OCC
            as_of,
            arity,
            cluster_id,
            depends_on,
            start_time,
            output: SubscribeOutput::Diffs,
            internal: true, // no mz_subscriptions row and no active-subscribes metric
        };
        active_subscribe.initialize();

        // Ship the dataflow before registering the sink, so a failure has
        // nothing to unwind.
        //
        // Creation can fail here: the plan was optimized against a catalog
        // snapshot taken off the coordinator loop, so a dependency can be
        // dropped before this message is handled. That makes it a conflict to
        // report rather than an invariant violation, hence `try_ship_dataflow`.
        if let Err(err) = self
            .try_ship_dataflow(df_desc, cluster_id, replica_id)
            .await
        {
            let _ = response_tx.send(Err(
                AdapterError::concurrent_dependency_drop_from_dataflow_creation_error(err),
            ));
            return;
        }

        self.add_active_compute_sink(sink_id, ActiveComputeSink::Subscribe(active_subscribe))
            .await;

        if response_tx.send(Ok(rx)).is_err() {
            // The receiver is gone, so cancellation or a statement timeout
            // dropped the caller's future between the command being sent and
            // this handler running. Retire the sink here, rather than leave a
            // dataflow running against a closed channel. The cancel path
            // retires it too, but only because its command is queued behind
            // ours, and that ordering is not ours to depend on.
            self.drop_internal_subscribe(sink_id).await;
            return;
        }

        // Drop read holds only after `ship_dataflow` returns, so the since
        // can't advance past `as_of` before the dataflow is running.
        drop(read_holds);
    }

    /// Enqueues a write attempt, answering through `result_tx`.
    ///
    /// `write_ts` picks the path. `Some` names a timestamp the diffs are only
    /// valid at and goes straight to the committer, pinned to the `GlobalId`
    /// validated here. `None` is a blind write that rides the next group
    /// commit, whose staging re-checks the target generation.
    pub(crate) fn handle_attempt_write(
        &mut self,
        attempt: WriteAttemptKind,
        target_id: mz_repr::CatalogItemId,
        target_global_id: GlobalId,
        diffs: Vec<(Row, Diff)>,
        result_tx: tokio::sync::oneshot::Sender<WriteResult>,
    ) {
        let result = InternalWriteResponder::new(result_tx);
        match &attempt {
            WriteAttemptKind::Session { conn_id, .. } => {
                if !self.active_conns.contains_key(conn_id) {
                    result.send(WriteResult::Canceled);
                    return;
                }
            }
            WriteAttemptKind::Background { .. } => {}
        }
        if self.controller.read_only() {
            result.send(WriteResult::ReadOnly);
            return;
        }

        let current_global_id = self
            .catalog()
            .try_get_entry(&target_id)
            .map(|entry| entry.latest_global_id());
        if current_global_id != Some(target_global_id) {
            result.send(WriteResult::TargetChanged);
            return;
        }

        let table_data = TableData::Rows(diffs);
        let timestamped = match &attempt {
            WriteAttemptKind::Session { write_ts, .. } => *write_ts,
            WriteAttemptKind::Background { write_ts } => Some(*write_ts),
        };
        match timestamped {
            Some(target_timestamp) => {
                let request = TimestampedWriteRequest {
                    appends: vec![(target_global_id, vec![table_data])],
                    target_timestamp,
                    result,
                    span: Span::current(),
                };
                if self
                    .group_committer_tx
                    .send(TableWriteCmd::TimestampedWrite(request))
                    .is_err()
                {
                    tracing::warn!("group committer task gone, dropping timestamped write");
                }
            }
            // Only a session reaches this: `WriteAttemptKind::Background` always
            // names a timestamp, and group commit needs a connection to answer
            // through.
            None => {
                let WriteAttemptKind::Session { conn_id, .. } = attempt else {
                    soft_panic_or_log!("background write reached the blind write path");
                    result.send(WriteResult::Indeterminate);
                    return;
                };
                let writes = BTreeMap::from([(target_id, smallvec![table_data])]);
                self.pending_writes.push(PendingWriteTxn::User {
                    span: Span::current(),
                    writes,
                    write_locks: None,
                    responder: UserWriteResponder::Internal {
                        conn_id,
                        target: WriteTarget {
                            item_id: target_id,
                            global_id: target_global_id,
                        },
                        result,
                    },
                });
                self.trigger_group_commit();
            }
        }
    }

    /// Drop an internal subscribe.
    pub(crate) async fn drop_internal_subscribe(&mut self, sink_id: GlobalId) {
        // Use drop_compute_sink instead of remove_active_compute_sink to also
        // cancel the dataflow on the compute side, not just remove bookkeeping.
        let _ = self.drop_compute_sink(sink_id).await;
    }
}

/// Which dependency rules a read-then-write is held to.
#[derive(Clone, Copy, Debug)]
pub(crate) enum DependencyPolicy {
    /// A user statement whose relation leaves must be writable user tables.
    UserDml,
    /// Coordinator-authored work, which may read system objects across time domains.
    SystemReads,
}

/// Validates all transitive dependencies of a read-then-write selection.
///
/// User DML requires every relation leaf to be a writable user table. System
/// reads accept supported system objects across time domains. Both reject
/// `mz_now()`.
///
/// The first invalid or temporal dependency encountered short-circuits with the corresponding
/// error. Traversal is bounded at `max_rw_dependencies` distinct objects, returning
/// [`AdapterError::ReadThenWriteDependencyLimitExceeded`] if exceeded.
pub(crate) fn validate_read_then_write_dependencies(
    catalog: &Catalog,
    ids: impl IntoIterator<Item = CatalogItemId>,
    max_rw_dependencies: usize,
    policy: DependencyPolicy,
) -> Result<(), AdapterError> {
    use CatalogItemType::*;
    use mz_catalog::memory::objects;

    // Iterative worklist rather than recursion. Dependency chains are user
    // controlled and can be arbitrarily deep (e.g. a long chain of stacked
    // views), so recursing risks a stack overflow on the coordinator thread.
    let mut seen = BTreeSet::new();
    let mut stack = Vec::new();
    for id in ids {
        enqueue(&mut seen, &mut stack, id, max_rw_dependencies)?;
    }
    while let Some(id) = stack.pop() {
        let Some(entry) = catalog.try_get_entry(&id) else {
            return Err(AdapterError::InvalidTableMutationSelection {
                object_name: id.to_string(),
                object_type: "unknown".to_string(),
            });
        };

        if let CatalogItem::View(objects::View {
            locally_optimized_expr: optimized_expr,
            ..
        })
        | CatalogItem::MaterializedView(objects::MaterializedView {
            locally_optimized_expr: optimized_expr,
            ..
        }) = entry.item()
        {
            if optimized_expr.contains_temporal() {
                return Err(AdapterError::Unsupported(
                    "calls to mz_now in write statements",
                ));
            }
        }

        let item_type = entry.item().typ();
        let ids_to_check = entry.item().query_dependencies();
        let is_writable_table = matches!(
            entry.item(),
            CatalogItem::Table(objects::Table {
                data_source: objects::TableDataSource::TableWrites { .. },
                ..
            })
        );
        let valid = match policy {
            DependencyPolicy::UserDml => match item_type {
                typ @ (Func | View | MaterializedView) => id.is_user() || matches!(typ, Func),
                Source | Secret | Connection => false,
                // Cannot select from sinks or indexes.
                Sink | MetricSink | Index | ForeignKey => unreachable!(),
                Table => id.is_user() && is_writable_table,
                Type => true,
            },
            DependencyPolicy::SystemReads => {
                id.is_system()
                    && matches!(
                        item_type,
                        Func | View | MaterializedView | Source | Table | Type
                    )
            }
        };
        if !valid {
            let object_name = catalog.resolve_full_name(entry.name(), None).to_string();
            let object_type = match item_type {
                // We only need the disallowed types here; the allowed types are handled above.
                Source => "source",
                Secret => "secret",
                Connection => "connection",
                Table => {
                    if !id.is_user() {
                        "system table"
                    } else if is_writable_table {
                        "user table"
                    } else if entry.source_export_details().is_some() {
                        "source-export table"
                    } else {
                        "source-backed table"
                    }
                }
                View if id.is_user() => "user view",
                View => "system view",
                MaterializedView if id.is_user() => "user materialized view",
                MaterializedView => "system materialized view",
                _ => "invalid dependency",
            };
            return Err(AdapterError::InvalidTableMutationSelection {
                object_name,
                object_type: object_type.to_string(),
            });
        }
        for dep in ids_to_check {
            enqueue(&mut seen, &mut stack, dep, max_rw_dependencies)?;
        }
    }
    Ok(())
}
