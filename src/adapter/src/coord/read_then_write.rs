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

    /// Handle a write attempt from the frontend OCC read-then-write loop.
    ///
    /// If `write_ts` is `Some`, submits an `InternalTimestamped` write targeting
    /// that timestamp. If `None`, submits a regular `User` write whose
    /// timestamp the oracle picks at group-commit time (a blind write).
    pub(crate) async fn handle_attempt_write(
        &mut self,
        conn_id: mz_adapter_types::connection::ConnectionId,
        target_id: mz_repr::CatalogItemId,
        diffs: Vec<(Row, Diff)>,
        write_ts: Option<Timestamp>,
        result_tx: tokio::sync::oneshot::Sender<WriteResult>,
    ) {
        use crate::coord::appends::{
            InternalTimestampedWrite, PendingWriteTxn, UserWriteResponder,
        };
        use mz_ore::soft_panic_or_log;
        use mz_storage_client::client::TableData;
        use smallvec::smallvec;
        use std::collections::BTreeMap;
        use tracing::Span;

        // `try_frontend_read_then_write` checks read-only at the entry point,
        // so reaching here in read-only mode means that check is stale (e.g.
        // the PeekClient's `read_only` snapshot from session startup doesn't
        // match current controller state). Surface it to the caller rather
        // than panicking the coordinator.
        if self.controller.read_only() {
            soft_panic_or_log!(
                "attempting OCC read-then-write in read-only mode: write_ts={:?}, target_id={:?}",
                write_ts,
                target_id,
            );
            let _ = result_tx.send(WriteResult::ReadOnly);
            return;
        }

        let table_data = TableData::Rows(diffs);
        let writes = BTreeMap::from([(target_id, smallvec![table_data])]);

        match write_ts {
            Some(write_ts) => {
                // Reject early if the oracle has already advanced past the
                // requested timestamp; the caller will retry.
                let next_eligible_write_ts = self.peek_local_write_ts().await.step_forward();
                if write_ts < next_eligible_write_ts {
                    let _ = result_tx.send(WriteResult::TimestampPassed {
                        target_timestamp: write_ts,
                        current_write_ts: next_eligible_write_ts,
                    });
                    return;
                }

                tracing::trace!(?writes, ?write_ts, "about to attempt read-then-write");

                self.pending_writes
                    .push(PendingWriteTxn::InternalTimestamped(
                        InternalTimestampedWrite {
                            conn_id,
                            span: Span::current(),
                            writes,
                            target_timestamp: write_ts,
                            result_tx,
                        },
                    ));
            }
            None => {
                // Blind write: the coordinator picks the timestamp via the
                // oracle during group commit. Goes through the regular `User`
                // path (including write-lock acquisition), with the result
                // delivered via the oneshot channel instead of a session.
                tracing::trace!(?writes, "about to attempt blind read-then-write");

                self.pending_writes.push(PendingWriteTxn::User {
                    span: Span::current(),
                    writes,
                    write_locks: None,
                    responder: UserWriteResponder::Internal { conn_id, result_tx },
                });
            }
        }
        self.trigger_group_commit();
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
/// Ensures all objects the selection depends on are valid for `ReadThenWrite` operations:
///
/// - They do not refer to any objects whose notion of time moves differently than that of
///   user tables. This limitation is meant to ensure no writes occur between this read and the
///   subsequent write.
/// - They do not use mz_now(), whose time produced during read will differ from the write
///   timestamp.
pub(crate) fn validate_read_then_write_dependencies(
    catalog: &Catalog,
    id: &CatalogItemId,
) -> Result<(), AdapterError> {
    use CatalogItemType::*;
    use mz_catalog::memory::objects;
    let mut ids_to_check = Vec::new();
    let valid = match catalog.try_get_entry(id) {
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
        let (object_name, object_type) = match catalog.try_get_entry(id) {
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
    for id in ids_to_check {
        validate_read_then_write_dependencies(catalog, &id)?;
    }
    Ok(())
}
