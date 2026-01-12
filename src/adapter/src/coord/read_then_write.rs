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
//! N.B. It's a bit annoying that we still have the write submission go through
//! the coordinator. We can imagine in the long run we want a group-commit task
//! that runs independently, and we can directly submit write requests there.

use std::collections::BTreeSet;

use mz_catalog::memory::objects::CatalogItem;
use mz_repr::CatalogItemId;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_sql::catalog::CatalogItemType;
use mz_sql::plan::SubscribeOutput;
use tokio::sync::mpsc;

use crate::PeekResponseUnary;
use crate::active_compute_sink::{ActiveComputeSink, ActiveSubscribe};
use crate::catalog::Catalog;
use crate::coord::Coordinator;
use crate::coord::appends::WriteResult;
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
    /// Handle a Command to create an internal subscribe.
    ///
    /// Internal subscribes are not visible in introspection collections. They
    /// are initially used for frontend-sequenced read-then-write
    /// (DELETE/UPDATE/INSERT ...SELECT) via OCC.
    ///
    /// This is called from the frontend OCC implementation after it has
    /// acquired the semaphore permit. We create the subscribe here (on the
    /// coordinator) and return the channel to the caller.
    ///
    /// The `read_holds` parameter contains the read holds for this specific
    /// operation. They are passed directly through the stages (not via the
    /// connection-keyed txn_read_holds map) to avoid issues where multiple
    /// operations on the same connection could interfere with each other's
    /// holds.
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
        conn_id: mz_adapter_types::connection::ConnectionId,
        session_uuid: uuid::Uuid,
        start_time: mz_ore::now::EpochMillis,
        read_holds: crate::ReadHolds,
        response_tx: tokio::sync::oneshot::Sender<
            Result<mpsc::UnboundedReceiver<PeekResponseUnary>, AdapterError>,
        >,
    ) {
        // Client disconnected while waiting for the semaphore.
        if !self.active_conns.contains_key(&conn_id) {
            let _ = response_tx.send(Err(AdapterError::Canceled));
            return;
        }

        let (tx, rx) = mpsc::unbounded_channel();

        let active_subscribe = ActiveSubscribe {
            conn_id: conn_id.clone(),
            session_uuid,
            channel: tx,
            emit_progress: true, // We need progress updates for OCC
            as_of,
            arity,
            cluster_id,
            depends_on,
            start_time,
            output: SubscribeOutput::Diffs,
            internal: true, // skip builtin table updates and metrics
        };
        active_subscribe.initialize();

        let write_notify_fut = self
            .add_active_compute_sink(sink_id, ActiveComputeSink::Subscribe(active_subscribe))
            .await;
        let ship_dataflow_fut = self.ship_dataflow(df_desc, cluster_id, replica_id);
        let ((), ()) = futures::future::join(write_notify_fut, ship_dataflow_fut).await;

        let _ = response_tx.send(Ok(rx));

        // Drop read holds only after `ship_dataflow` returns, so the since
        // can't advance past `as_of` before the dataflow is running.
        drop(read_holds);
    }

    /// Enqueues a write attempt from the frontend OCC loop.
    pub(crate) fn handle_attempt_write(
        &mut self,
        conn_id: mz_adapter_types::connection::ConnectionId,
        target_id: mz_repr::CatalogItemId,
        target_global_id: GlobalId,
        diffs: Vec<(Row, Diff)>,
        write_ts: Option<Timestamp>,
        result_tx: tokio::sync::oneshot::Sender<WriteResult>,
    ) {
        use crate::coord::appends::{
            InternalWriteResponder, PendingWriteTxn, TableWriteCmd, TimestampedWriteRequest,
            UserWriteResponder,
        };
        use mz_storage_client::client::TableData;
        use smallvec::smallvec;
        use std::collections::BTreeMap;
        use tracing::Span;

        let result = InternalWriteResponder::new(result_tx);
        if !self.active_conns.contains_key(&conn_id) {
            result.send(WriteResult::Canceled);
            return;
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
                    responder: UserWriteResponder::Internal { conn_id, result },
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
                    Sink | Index => unreachable!(),
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
