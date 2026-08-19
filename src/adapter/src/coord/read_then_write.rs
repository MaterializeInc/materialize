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
use crate::coord::Coordinator;
use crate::coord::appends::{
    InternalWriteResponder, PendingWriteTxn, TableWriteCmd, TimestampedWriteRequest,
    UserWriteResponder, WriteResult, WriteTarget,
};
use crate::error::AdapterError;

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
        // Client disconnected while waiting for the semaphore. Background work
        // has no connection to lose.
        if let ActiveSubscribeOwner::Session { conn_id, .. } = &owner {
            if !self.active_conns.contains_key(conn_id) {
                let _ = response_tx.send(Err(AdapterError::Canceled));
                return;
            }
        }

        let (tx, rx) = mpsc::unbounded_channel();

        let active_subscribe = ActiveSubscribe {
            owner,
            channel: tx,
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
    ///
    /// `conn_id` is the connection the write is cancelled with. Coordinator
    /// background work passes `None`, and always names a timestamp, since the
    /// blind path needs a connection to answer through.
    pub(crate) fn handle_attempt_write(
        &mut self,
        conn_id: Option<mz_adapter_types::connection::ConnectionId>,
        target_id: mz_repr::CatalogItemId,
        target_global_id: GlobalId,
        diffs: Vec<(Row, Diff)>,
        write_ts: Option<Timestamp>,
        result_tx: tokio::sync::oneshot::Sender<WriteResult>,
    ) {
        let result = InternalWriteResponder::new(result_tx);
        if let Some(conn_id) = &conn_id {
            if !self.active_conns.contains_key(conn_id) {
                result.send(WriteResult::Canceled);
                return;
            }
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
        match write_ts {
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
            None => {
                let writes = BTreeMap::from([(target_id, smallvec![table_data])]);
                self.pending_writes.push(PendingWriteTxn::User {
                    span: Span::current(),
                    writes,
                    write_locks: None,
                    responder: UserWriteResponder::Internal {
                        conn_id: conn_id.expect("blind writes come from a session"),
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

/// Validates that all dependencies are valid for read-then-write operations.
///
/// Ensures all objects the selection transitively depends on (seeded by `ids`) are valid for
/// `ReadThenWrite` operations:
///
/// - They do not refer to any objects whose notion of time moves differently than that of
///   user tables. This limitation is meant to ensure no writes occur between this read and the
///   subsequent write.
/// - They do not use mz_now(), whose time produced during read will differ from the write
///   timestamp.
///
/// The first invalid or temporal dependency encountered short-circuits with the corresponding
/// error. Traversal is bounded at `max_rw_dependencies` distinct objects, returning
/// [`AdapterError::ReadThenWriteDependencyLimitExceeded`] if exceeded.
pub(crate) fn validate_read_then_write_dependencies(
    catalog: &Catalog,
    ids: impl IntoIterator<Item = CatalogItemId>,
    max_rw_dependencies: usize,
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
        let mut ids_to_check = Vec::new();
        let valid = match catalog.try_get_entry(&id) {
            Some(entry) => {
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
                match entry.item().typ() {
                    typ @ (Func | View | MaterializedView) => {
                        ids_to_check.extend(entry.uses());
                        let valid_id = id.is_user() || matches!(typ, Func);
                        valid_id
                    }
                    Source | Secret | Connection => false,
                    // Cannot select from sinks or indexes.
                    Sink | MetricSink | Index => unreachable!(),
                    Table => {
                        if !id.is_user() {
                            // We can't read from non-user tables
                            false
                        } else {
                            // We can't read from tables that are source-exports
                            entry.source_export_details().is_none()
                        }
                    }
                    Type => true,
                }
            }
            None => false,
        };
        if !valid {
            let (object_name, object_type) = match catalog.try_get_entry(&id) {
                Some(entry) => {
                    let object_name = catalog.resolve_full_name(entry.name(), None).to_string();
                    let object_type = match entry.item().typ() {
                        // We only need the disallowed types here; the allowed types are handled above.
                        Source => "source",
                        Secret => "secret",
                        Connection => "connection",
                        Table => {
                            if !id.is_user() {
                                "system table"
                            } else {
                                "source-export table"
                            }
                        }
                        View => "system view",
                        MaterializedView => "system materialized view",
                        _ => "invalid dependency",
                    };
                    (object_name, object_type.to_string())
                }
                None => (id.to_string(), "unknown".to_string()),
            };
            return Err(AdapterError::InvalidTableMutationSelection {
                object_name,
                object_type,
            });
        }
        for dep in ids_to_check {
            enqueue(&mut seen, &mut stack, dep, max_rw_dependencies)?;
        }
    }
    Ok(())
}
