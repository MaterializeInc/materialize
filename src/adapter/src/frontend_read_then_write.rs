// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Frontend sequencing for read-then-write operations.
//!
//! This module implements INSERT [...] SELECT FROM [...], DELETE and UPDATE
//! operations using a subscribe with optimistic concurrency control (OCC),
//! sequenced from the session task rather than the Coordinator. This reduces
//! coordinator bottlenecking.
//!
//! The approach is:
//! 1. Validate and optimize MIR locally
//! 2. Determine timestamp via coordinator
//! 3. Optimize LIR locally
//! 4. Acquire OCC semaphore
//! 5. Create subscribe via Coordinator Command
//! 6. Run OCC loop (receive diffs, attempt write, retry on conflict)
//! 7. Return result
//!
//! ## Rollout note
//!
//! The `FRONTEND_READ_THEN_WRITE` dyncfg is read once at process startup and
//! fixed for the lifetime of the `environmentd` process. This avoids a
//! mixed-mode window where both the lock-based coordinator path and this OCC
//! path are active concurrently — the coordinator path acquires write locks to
//! prevent concurrent writes between its read and write phases, but this OCC
//! path does not use write locks, so concurrent operation of both paths could
//! allow an OCC write to slip between a coordinator-path reader's read and
//! write.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::{NonZeroI64, NonZeroUsize};
use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::consolidation;
use itertools::Itertools;
use mz_catalog::memory::error::ErrorKind;
use mz_cluster_client::ReplicaId;
use mz_compute_types::ComputeInstanceId;
use mz_expr::{CollectionPlan, Id, LocalId, MirRelationExpr, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_ore::soft_panic_or_log;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{
    CatalogItemId, Diff, GlobalId, IntoRowIterator, RelationDesc, Row, RowArena, Timestamp,
};
use mz_sql::catalog::CatalogError;
use mz_sql::plan::{self, MutationKind, Params, QueryWhen};
use mz_sql::session::metadata::SessionMetadata;
use qcell::QCell;
use timely::progress::Antichain;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::command::{Command, ExecuteResponse};
use crate::coord::appends::WriteResult;
use crate::coord::read_then_write::validate_read_then_write_dependencies;
use crate::coord::timestamp_selection::TimestampProvider;
use crate::coord::{Coordinator, ExecuteContextGuard, TargetCluster};
use crate::error::AdapterError;
use crate::optimize::Optimize;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, EvalTime, ExprPrep, ExprPrepOneShot};
use crate::session::{LifecycleTimestamps, Session, TransactionOps};
use crate::statement_logging::{
    PreparedStatementLoggingInfo, StatementLifecycleEvent, StatementLoggingId,
};
use crate::{PeekClient, PeekResponseUnary, TimelineContext, optimize};

/// A handle to an internal subscribe (not visible in introspection collections
/// like `mz_subscriptions`). A `Drop` impl ensures the subscribe's dataflow is
/// cleaned up when dropped.
pub(crate) struct SubscribeHandle {
    rx: mpsc::UnboundedReceiver<PeekResponseUnary>,
    sink_id: GlobalId,
    /// Wrapped in `Option` so we can move it out in `Drop`.
    client: Option<crate::Client>,
}

impl SubscribeHandle {
    /// Receive the next message from the subscribe, waiting if necessary.
    pub async fn recv(&mut self) -> Option<PeekResponseUnary> {
        self.rx.recv().await
    }

    /// Try to receive a message without waiting.
    pub fn try_recv(&mut self) -> Result<PeekResponseUnary, mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Drop for SubscribeHandle {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            // Fire-and-forget: if the coordinator is gone, the subscribe will
            // be cleaned up when the process exits anyway.
            client.send(Command::DropInternalSubscribe {
                sink_id: self.sink_id,
            });
        }
    }
}

impl PeekClient {
    /// Execute a read-then-write operation using frontend sequencing.
    ///
    /// Called by session code when the frontend_read_then_write dyncfg is
    /// enabled.
    pub(crate) async fn frontend_read_then_write(
        &mut self,
        session: &mut Session,
        plan: plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
        params: &Params,
        logging: &Arc<QCell<PreparedStatementLoggingInfo>>,
        lifecycle_timestamps: Option<LifecycleTimestamps>,
        outer_ctx_extra: &mut Option<ExecuteContextGuard>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let catalog = self.catalog_snapshot("frontend_read_then_write").await;

        // The guard's `Drop` impl emits `Aborted` if the inner future is
        // dropped mid-flight, so the end-execution event is never skipped.
        let logging_guard = self.begin_statement_logging(
            session,
            params,
            logging,
            &catalog,
            lifecycle_timestamps,
            outer_ctx_extra,
        );

        let result = self
            .frontend_read_then_write_inner(
                session,
                plan,
                target_cluster,
                &catalog,
                logging_guard.id(),
            )
            .await;

        logging_guard.retire_with_result(&result);

        result
    }

    /// Separated from the outer function so the statement-logging guard always
    /// retires on the same return path.
    async fn frontend_read_then_write_inner(
        &mut self,
        session: &mut Session,
        mut plan: plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
        catalog: &Arc<Catalog>,
        statement_logging_id: Option<StatementLoggingId>,
    ) -> Result<ExecuteResponse, AdapterError> {
        let validation_result =
            self.validate_read_then_write(catalog, session, &plan, target_cluster)?;

        let ValidationResult {
            cluster_id,
            replica_id,
            timeline,
            depends_on,
            table_desc,
        } = validation_result;

        if let Some(logging_id) = statement_logging_id {
            self.log_set_cluster(logging_id, cluster_id);
        }

        // Read-then-write is rejected in explicit transaction blocks (checked
        // in SessionClient::try_frontend_read_then_write), so we're always in
        // an implicit (autocommit) transaction here. The actual data is written
        // via the coordinator's group commit path, bypassing session transaction
        // ops. The empty Writes(vec![]) just marks this as a write transaction
        // in the session state machine so auto-commit handles it correctly.
        // This is safe because there's no ROLLBACK opportunity in an implicit
        // transaction.
        debug_assert!(
            session.transaction().is_implicit(),
            "read-then-write should be rejected in explicit transactions"
        );
        session.add_transaction_ops(TransactionOps::Writes(vec![]))?;

        // Prepare expressions (resolve unmaterializable functions like
        // current_user())
        let style = ExprPrepOneShot {
            logical_time: EvalTime::NotAvailable, // We already errored out on mz_now above.
            session,
            catalog_state: catalog.state(),
        };
        for expr in plan
            .assignments
            .values_mut()
            .chain(plan.returning.iter_mut())
        {
            style.prep_scalar_expr(expr)?;
        }

        let (optimizer, global_mir_plan) =
            self.optimize_mir_read_then_write(catalog, session, &plan, cluster_id)?;

        // Acquire the OCC semaphore permit *before* acquiring read holds in
        // `frontend_determine_timestamp`. Under contention, waiters will
        // otherwise sit on read holds on the RTW's read dependencies for the
        // entire time they are queued, pinning compaction on those
        // collections. Waiting on the permit first keeps queued operations
        // hold-free; once we have a permit we proceed to acquire the read
        // holds needed for the rest of the operation.
        //
        // The semaphore is owned by the coordinator and outlives every
        // session task, so `acquire_owned` cannot return `Err` in practice.
        let permit = Arc::clone(&self.occ_write_semaphore)
            .acquire_owned()
            .await
            .expect("semaphore is never closed during coordinator lifetime");

        // Determine timestamp and acquire read holds.
        let oracle_read_ts = self.oracle_read_ts(&timeline).await?;
        let bundle = global_mir_plan.id_bundle(cluster_id);
        let (determination, read_holds) = self
            .frontend_determine_timestamp(
                session,
                &bundle,
                &QueryWhen::FreshestTableWrite,
                cluster_id,
                &timeline,
                oracle_read_ts,
                None,
            )
            .await?;

        let as_of = determination.timestamp_context.timestamp_or_default();

        let global_lir_plan =
            self.optimize_lir_read_then_write(optimizer, global_mir_plan, as_of)?;

        // Log optimization finished
        if let Some(logging_id) = statement_logging_id {
            self.log_lifecycle_event(logging_id, StatementLifecycleEvent::OptimizationFinished);
        }

        let sink_id = global_lir_plan.sink_id();
        let target_id = plan.id;
        let kind = plan.kind.clone();
        let returning = plan.returning.clone();

        let (df_desc, _df_meta) = global_lir_plan.unapply();

        let arity = df_desc
            .sink_exports
            .values()
            .next()
            .expect("has sink")
            .from_desc
            .arity();

        let conn_id = session.conn_id().clone();
        let session_uuid = session.uuid();
        let start_time = (self.statement_logging_frontend.now)();
        let max_result_size = catalog.system_config().max_result_size();
        let max_occ_retries = usize::cast_from(catalog.system_config().max_occ_retries());
        let statement_timeout = *session.vars().statement_timeout();

        // Linearize the read BEFORE subscribing or writing: block until
        // the oracle for this query's timeline has advanced to `as_of`.
        //
        // The up-front ordering is load-bearing. If `as_of` is in the far
        // future (e.g. reading from a `REFRESH AT` MV with a far-future
        // since), submitting a write at `chosen_ts >= as_of` would have
        // the group commit bump the oracle to that far-future value,
        // stalling every subsequent write on the `EpochMilliseconds`
        // timeline until then. By waiting here, a pathological RTW hits
        // `statement_timeout` and returns without ever touching the
        // oracle.
        self.ensure_read_linearized(&timeline, as_of).await?;

        let subscribe_handle = self
            .create_internal_subscribe(
                Box::new(df_desc),
                cluster_id,
                replica_id,
                depends_on.clone(),
                as_of,
                arity,
                sink_id,
                conn_id.clone(),
                session_uuid,
                start_time,
                read_holds,
            )
            .await?;

        let (retry_count, result) = self
            .run_occ_loop(
                subscribe_handle,
                target_id,
                kind,
                returning,
                max_result_size,
                max_occ_retries,
                table_desc,
                statement_timeout,
                conn_id,
                statement_logging_id,
                as_of,
            )
            .await;

        self.coordinator_client()
            .metrics()
            .occ_retry_count
            .observe(f64::from(u32::try_from(retry_count).unwrap_or(u32::MAX)));

        // Release the OCC permit only after the OCC loop has fully completed
        // (success, failure, or timeout). Holding it for the entire operation
        // is what bounds concurrency; an early drop would let a waiter start
        // its subscribe while we are still consolidating diffs and retrying.
        drop(permit);

        let result = result?;

        Ok(result)
    }

    /// Validate a read-then-write operation.
    fn validate_read_then_write(
        &self,
        catalog: &Arc<Catalog>,
        session: &Session,
        plan: &plan::ReadThenWritePlan,
        target_cluster: TargetCluster,
    ) -> Result<ValidationResult, AdapterError> {
        // Disallow mz_now in any position because read time and write time differ.
        let contains_temporal = plan.selection.contains_temporal()?
            || plan.assignments.values().any(|e| e.contains_temporal())
            || plan.returning.iter().any(|e| e.contains_temporal());
        if contains_temporal {
            return Err(AdapterError::Unsupported(
                "calls to mz_now in write statements",
            ));
        }

        // Validate read dependencies. The plan was built against an earlier
        // catalog snapshot; an item it depends on may have been dropped by
        // concurrent DDL before we got here.
        for gid in plan.selection.depends_on() {
            let item_id = catalog.try_resolve_item_id(&gid).ok_or_else(|| {
                AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: ErrorKind::Sql(CatalogError::UnknownItem(gid.to_string())),
                })
            })?;
            validate_read_then_write_dependencies(catalog, &item_id)?;
        }

        let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
        let cluster_id = cluster.id;

        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let replica_id = session
            .vars()
            .cluster_replica()
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        let depends_on = plan.selection.depends_on();
        let timeline = catalog.validate_timeline_context(depends_on.iter().copied())?;

        // Get the table descriptor for constraint validation. The plan's
        // target table may have been dropped by concurrent DDL between
        // planning and here, so tolerate a missing entry.
        let table_desc = match catalog.try_get_entry(&plan.id) {
            Some(entry) => entry
                .relation_desc_latest()
                .expect("table has desc")
                .into_owned(),
            None => {
                return Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                    kind: ErrorKind::Sql(CatalogError::UnknownItem(plan.id.to_string())),
                }));
            }
        };

        Ok(ValidationResult {
            cluster_id,
            replica_id,
            timeline,
            depends_on,
            table_desc,
        })
    }

    /// Optimize MIR for a read-then-write operation.
    fn optimize_mir_read_then_write(
        &self,
        catalog: &Arc<Catalog>,
        session: &dyn SessionMetadata,
        plan: &plan::ReadThenWritePlan,
        cluster_id: ComputeInstanceId,
    ) -> Result<
        (
            optimize::subscribe::Optimizer,
            optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
        ),
        AdapterError,
    > {
        // `finishing` is unused: the OCC path emits raw diffs and
        // `apply_mutation_to_mir` handles update projection.
        let plan::ReadThenWritePlan {
            id: _,
            selection,
            finishing: _,
            assignments,
            kind,
            returning: _,
        } = plan;

        let expr = selection.clone().lower(catalog.system_config(), None)?;
        let mut expr = apply_mutation_to_mir(expr, kind, assignments);

        // Resolve unmaterializable functions (now(), current_user, ...) before
        // the subscribe optimizer sees them: it uses `ExprPrepMaintained`,
        // which rejects them, but our subscribe is a one-shot read so we can
        // resolve them to constants. `mz_now()` is rejected upstream by
        // `validate_read_then_write`.
        let style = ExprPrepOneShot {
            logical_time: EvalTime::NotAvailable,
            session,
            catalog_state: catalog.state(),
        };
        expr.try_visit_scalars_mut(&mut |s| style.prep_scalar_expr(s))?;

        let compute_instance = ComputeInstanceSnapshot::new_without_collections(cluster_id);
        let (_, view_id) = self.transient_id_gen.allocate_id();
        let (_, sink_id) = self.transient_id_gen.allocate_id();
        let debug_name = format!("frontend-read-then-write-subscribe-{}", sink_id);
        let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
            .override_from(&catalog.get_cluster(cluster_id).config.features());

        let mut optimizer = optimize::subscribe::Optimizer::new(
            Arc::<Catalog>::clone(catalog),
            compute_instance,
            view_id,
            sink_id,
            true, // with_snapshot
            None, // up_to
            debug_name,
            optimizer_config,
            self.optimizer_metrics.clone(),
        );

        let expr_typ = expr.typ();
        let sql_typ = mz_repr::SqlRelationType::from_repr(&expr_typ);
        let column_names: Vec<String> = (0..sql_typ.column_types.len())
            .map(|i| format!("column{}", i))
            .collect();
        let relation_desc = RelationDesc::new(sql_typ, column_names.iter().map(|s| s.as_str()));

        // MIR => MIR optimization (global). We use optimize_mir because the
        // mutation has already been applied in MIR, so we bypass the normal
        // SubscribePlan path which expects HIR.
        let global_mir_plan = optimizer.optimize_mir(expr, relation_desc)?;

        Ok((optimizer, global_mir_plan))
    }

    /// Optimize LIR for a read-then-write operation.
    fn optimize_lir_read_then_write(
        &self,
        mut optimizer: optimize::subscribe::Optimizer,
        global_mir_plan: optimize::subscribe::GlobalMirPlan<optimize::subscribe::Unresolved>,
        as_of: Timestamp,
    ) -> Result<optimize::subscribe::GlobalLirPlan, AdapterError> {
        let global_mir_plan = global_mir_plan.resolve(Antichain::from_elem(as_of));
        let global_lir_plan = optimizer.optimize(global_mir_plan)?;
        Ok(global_lir_plan)
    }

    /// Get the oracle read timestamp hint for the timeline of this query.
    async fn oracle_read_ts(
        &mut self,
        timeline: &TimelineContext,
    ) -> Result<Option<Timestamp>, AdapterError> {
        // See `ensure_read_linearized` for why `get_timeline` is the right
        // function here: the write target lives on `EpochMilliseconds`, so
        // we want that oracle's read_ts as the hint for timestamp
        // selection even when the read side is MV-only
        // (`TimestampDependent`).
        let timeline = <Coordinator as TimestampProvider>::get_timeline(timeline);

        match timeline {
            Some(timeline) => {
                let oracle = self.ensure_oracle(timeline).await?;
                Ok(Some(oracle.read_ts().await))
            }
            None => Ok(None),
        }
    }

    /// Block until the oracle for this query's timeline has advanced to
    /// `as_of`. Returns immediately if it already has.
    ///
    /// This implements the strict-serializable read guarantee for RTW:
    /// once this returns, any session observing the oracle sees a read
    /// timestamp at least as large as `as_of`, so reads at `as_of` (and
    /// writes derived from them) cannot appear to "go backwards" relative
    /// to subsequent queries.
    async fn ensure_read_linearized(
        &mut self,
        timeline: &TimelineContext,
        as_of: Timestamp,
    ) -> Result<(), AdapterError> {
        // Pick the oracle this RTW operates against. `timeline` is derived
        // from the read side (`plan.selection.depends_on()`), so an
        // MV-only read produces `TimestampDependent` — an MV itself
        // doesn't pin the query to any source timeline. The write target,
        // however, is always a Table living on `EpochMilliseconds`, and
        // future readers of that table will consult the
        // `EpochMilliseconds` oracle, so linearization must target
        // `EpochMilliseconds` regardless of the read side.
        //
        // `get_timeline` encodes that defaulting (`TimestampDependent` →
        // `Some(EpochMilliseconds)`). `TimelineContext::timeline()`
        // answers a different question ("is there a source-forced
        // timeline?") and would return `None` for MV-only reads,
        // silently skipping linearization.
        let tl = match <Coordinator as TimestampProvider>::get_timeline(timeline) {
            Some(tl) => tl,
            None => return Ok(()),
        };

        let oracle = self.ensure_oracle(tl).await?;

        loop {
            let oracle_ts = oracle.read_ts().await;
            if as_of <= oracle_ts {
                return Ok(());
            }

            // Sleep for roughly the difference between as_of and the current
            // oracle timestamp. Since timestamps are epoch milliseconds, the
            // difference is the approximate wall-clock time we need to wait.
            // Cap at 1s to avoid very long sleeps if clocks are skewed,
            // matching the cap in `message_linearize_reads`.
            let wait_ms = u64::from(as_of.saturating_sub(oracle_ts));
            let wait = Duration::from_millis(wait_ms).min(Duration::from_secs(1));
            tokio::time::sleep(wait).await;
        }
    }

    /// Creates an internal subscribe that does not appear in introspection
    /// tables. Returns a [`SubscribeHandle`] that ensures cleanup on drop.
    async fn create_internal_subscribe(
        &self,
        df_desc: Box<optimize::LirDataflowDescription>,
        cluster_id: ComputeInstanceId,
        replica_id: Option<ReplicaId>,
        depends_on: BTreeSet<GlobalId>,
        as_of: Timestamp,
        arity: usize,
        sink_id: GlobalId,
        conn_id: mz_adapter_types::connection::ConnectionId,
        session_uuid: Uuid,
        start_time: mz_ore::now::EpochMillis,
        read_holds: crate::ReadHolds,
    ) -> Result<SubscribeHandle, AdapterError> {
        let rx: mpsc::UnboundedReceiver<PeekResponseUnary> = self
            .call_coordinator(|tx| Command::CreateInternalSubscribe {
                df_desc,
                cluster_id,
                replica_id,
                depends_on,
                as_of,
                arity,
                sink_id,
                conn_id,
                session_uuid,
                start_time,
                read_holds,
                tx,
            })
            .await?;

        Ok(SubscribeHandle {
            rx,
            sink_id,
            client: Some(self.coordinator_client().clone()),
        })
    }

    /// Run the OCC loop: drain the subscribe at `as_of`, apply the
    /// mutation, and submit the resulting diffs as a write.
    ///
    /// Semantically this is a SELECT at `as_of` followed by an INSERT.
    /// Because we hold no write lock, a concurrent writer may bump the
    /// target table's upper past our chosen write timestamp, in which
    /// case the coordinator returns `WriteResult::TimestampPassed`; we
    /// then wait for the subscribe to advance and retry, up to
    /// `max_occ_retries` times.
    ///
    /// Read linearization is the caller's responsibility: `as_of` must
    /// already be linearized (oracle read_ts >= `as_of`) on entry. See
    /// `ensure_read_linearized` at the call site.
    ///
    /// Returns `(retry_count, result)` so the caller can record OCC retry
    /// metrics regardless of whether the operation succeeded or failed.
    async fn run_occ_loop(
        &self,
        mut subscribe_handle: SubscribeHandle,
        target_id: CatalogItemId,
        kind: MutationKind,
        returning: Vec<MirScalarExpr>,
        max_result_size: u64,
        max_occ_retries: usize,
        table_desc: RelationDesc,
        statement_timeout: Duration,
        conn_id: mz_adapter_types::connection::ConnectionId,
        statement_logging_id: Option<StatementLoggingId>,
        as_of: Timestamp,
    ) -> (usize, Result<ExecuteResponse, AdapterError>) {
        // Timeout of 0 is equivalent to "off", meaning we will wait "forever."
        let effective_timeout = if statement_timeout == Duration::ZERO {
            Duration::MAX
        } else {
            statement_timeout
        };
        let start_time = Instant::now();

        let mut state = OccState::new();

        // Correctness invariant for retries:
        //
        // `all_diffs` accumulates *all* rows ever received from the subscribe,
        // across retries. The subscribe emits a snapshot (at the as_of
        // timestamp) followed by incremental updates. We consolidate on every
        // progress message (flattening timestamps to MIN first), so after
        // consolidation `all_diffs` always represents "what the query returns
        // as of the latest progress timestamp" — old snapshot rows that were
        // retracted by newer updates cancel out, and new rows appear. This is
        // exactly the set of diffs we want to write.
        //
        // Consolidating on every progress also means the NoRowsMatched check
        // works correctly across retries: if the consolidated result becomes
        // logically empty (all diffs cancel out), `all_diffs` will be empty
        // and we early-return without attempting a write.
        let result = loop {
            // Check for timeout
            let remaining = effective_timeout.saturating_sub(start_time.elapsed());
            if remaining.is_zero() {
                // Guard handles cleanup on drop.
                break Err(AdapterError::StatementTimeout);
            }

            let msg = match tokio::time::timeout(remaining, subscribe_handle.recv()).await {
                Ok(Some(msg)) => msg,
                Ok(None) => {
                    // Channel closed cleanly: the SELECT is constant (no
                    // table dependency). Submit the accumulated diffs as a
                    // blind write — the oracle picks the timestamp at group
                    // commit, so we just flatten to `Timestamp::MIN` for
                    // `consolidate_updates`.
                    state.consolidate(Timestamp::MIN);
                    if state.all_diffs.is_empty() {
                        break build_no_rows_response(&kind, &returning);
                    }
                    let success_response =
                        match self.build_success_response(&kind, &returning, &state.all_diffs) {
                            Ok(response) => response,
                            Err(e) => break Err(e),
                        };
                    let diffs = state
                        .all_diffs
                        .iter()
                        .map(|(row, _ts, diff)| (row.clone(), *diff))
                        .collect_vec();
                    let result = self
                        .call_coordinator(|tx| Command::AttemptWrite {
                            conn_id: conn_id.clone(),
                            target_id,
                            diffs,
                            write_ts: None,
                            tx,
                        })
                        .await;
                    match result {
                        WriteResult::Success { timestamp } => {
                            if let Some(id) = statement_logging_id {
                                self.log_set_timestamp(id, timestamp);
                            }
                            break Ok(success_response);
                        }
                        WriteResult::TimestampPassed { .. } => {
                            // Unreachable: blind writes use
                            // `UserWriteResponder::Internal`, which group
                            // commit never resolves to `TimestampPassed`.
                            soft_panic_or_log!(
                                "blind read-then-write unexpectedly got TimestampPassed"
                            );
                            break Err(AdapterError::Internal(
                                "blind write unexpectedly got TimestampPassed".into(),
                            ));
                        }
                        WriteResult::Canceled => break Err(AdapterError::Canceled),
                        WriteResult::ReadOnly => break Err(AdapterError::ReadOnly),
                    }
                }
                Err(_) => {
                    // Timed out
                    break Err(AdapterError::StatementTimeout);
                }
            };

            match process_message(msg, &mut state, as_of, max_result_size, &table_desc) {
                ProcessResult::Continue { ready_to_write } => {
                    if !ready_to_write {
                        continue;
                    }

                    // Drain pending messages before attempting write
                    let drain_err = loop {
                        match subscribe_handle.try_recv() {
                            Ok(msg) => {
                                match process_message(
                                    msg,
                                    &mut state,
                                    as_of,
                                    max_result_size,
                                    &table_desc,
                                ) {
                                    ProcessResult::Continue { .. } => {}
                                    ProcessResult::NoRowsMatched => {
                                        break Some(build_no_rows_response(&kind, &returning));
                                    }
                                    ProcessResult::Error(e) => {
                                        break Some(Err(e));
                                    }
                                }
                            }
                            Err(mpsc::error::TryRecvError::Empty) => break None,
                            // The subscribe can finish (coordinator drops the
                            // sender after `process_response` returns true)
                            // between our last recv() and this drain. This is
                            // benign — all buffered messages have already been
                            // consumed via the Ok(msg) arm above.
                            Err(mpsc::error::TryRecvError::Disconnected) => break None,
                        }
                    };
                    if let Some(result) = drain_err {
                        break result;
                    }

                    let write_ts = state
                        .current_upper
                        .expect("must have seen progress to be ready to write");

                    // Consolidate any rows received during the drain
                    // (the bulk was already consolidated on the last progress).
                    state.consolidate(write_ts);

                    let success_response =
                        match self.build_success_response(&kind, &returning, &state.all_diffs) {
                            Ok(response) => response,
                            Err(e) => break Err(e),
                        };

                    // Submit write.
                    //
                    // perf: clones every row on each attempt. Under contention
                    // we retry up to `max_occ_retries` times (default 1000),
                    // so a large DELETE/UPDATE under heavy contention can do a
                    // lot of row-cloning work. If this shows up in profiles,
                    // consider storing `Arc<Row>` in `all_diffs` to make the
                    // per-attempt copy cheap.
                    let result = self
                        .call_coordinator(|tx| Command::AttemptWrite {
                            conn_id: conn_id.clone(),
                            target_id,
                            diffs: state
                                .all_diffs
                                .iter()
                                .map(|(row, _ts, diff)| (row.clone(), *diff))
                                .collect_vec(),
                            write_ts: Some(write_ts),
                            tx,
                        })
                        .await;

                    match result {
                        WriteResult::Success { timestamp } => {
                            if let Some(id) = statement_logging_id {
                                self.log_set_timestamp(id, timestamp);
                            }
                            // N.B. subscribe_handle is dropped here, which
                            // fires off the cleanup message.
                            break Ok(success_response);
                        }
                        WriteResult::TimestampPassed {
                            current_write_ts, ..
                        } => {
                            // Do not advance `state.current_upper` (and
                            // therefore `write_ts`) from `current_write_ts`.
                            // The diffs in `all_diffs` are only known to be
                            // correct as of subscribe progress we have actually
                            // observed. Retrying at a newer oracle timestamp
                            // before subscribe progress catches up would risk
                            // applying stale diffs at the wrong timestamp. So
                            // on `TimestampPassed` we wait for the subscribe to
                            // progress and retry using that observed frontier.
                            state.retry_count += 1;
                            if state.retry_count >= max_occ_retries {
                                // High contention is a user-visible
                                // condition, not an internal invariant
                                // violation. Surface it as
                                // `Unstructured` so it doesn't trip
                                // internal-error alerts.
                                break Err(AdapterError::Unstructured(anyhow::anyhow!(
                                    "read-then-write exceeded maximum retry attempts under contention",
                                )));
                            }
                            tracing::debug!(
                                retry_count = state.retry_count,
                                write_ts = %write_ts,
                                current_write_ts = %current_write_ts,
                                "OCC write conflict, retrying"
                            );
                            continue;
                        }
                        WriteResult::Canceled => break Err(AdapterError::Canceled),
                        WriteResult::ReadOnly => break Err(AdapterError::ReadOnly),
                    }
                }
                ProcessResult::NoRowsMatched => {
                    break build_no_rows_response(&kind, &returning);
                }
                ProcessResult::Error(e) => {
                    break Err(e);
                }
            }
        };

        (state.retry_count, result)
    }

    /// Build the success response after a successful write.
    fn build_success_response(
        &self,
        kind: &MutationKind,
        returning: &[MirScalarExpr],
        all_diffs: &[(Row, Timestamp, Diff)],
    ) -> Result<ExecuteResponse, AdapterError> {
        if returning.is_empty() {
            // For UPDATE each changed row produces a retraction (-1) and an
            // insertion (+1), so we divide by 2 below.
            let row_count = all_diffs
                .iter()
                .map(|(_, _, diff)| diff.into_inner().unsigned_abs())
                .sum::<u64>();
            let row_count =
                usize::try_from(row_count).expect("positive row count must fit in usize");

            return Ok(match kind {
                MutationKind::Delete => ExecuteResponse::Deleted(row_count),
                MutationKind::Update => ExecuteResponse::Updated(row_count / 2),
                MutationKind::Insert => ExecuteResponse::Inserted(row_count),
            });
        }

        let mut returning_rows = Vec::new();
        let arena = RowArena::new();

        for (row, _ts, diff) in all_diffs {
            let include = match kind {
                MutationKind::Delete => diff.is_negative(),
                MutationKind::Update | MutationKind::Insert => diff.is_positive(),
            };

            if !include {
                continue;
            }

            let mut returning_row = Row::with_capacity(returning.len());
            let mut packer = returning_row.packer();
            let datums: Vec<_> = row.iter().collect();

            for expr in returning {
                match expr.eval(&datums, &arena) {
                    Ok(datum) => packer.push(datum),
                    Err(err) => return Err(err.into()),
                }
            }

            let multiplicity = NonZeroUsize::try_from(
                NonZeroI64::try_from(diff.into_inner().abs()).expect("diff is non-zero"),
            )
            .map_err(AdapterError::from)?;

            returning_rows.push((returning_row, multiplicity));
        }

        let rows: Vec<Row> = returning_rows
            .into_iter()
            .flat_map(|(row, count)| std::iter::repeat(row).take(count.get()))
            .collect();
        Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        })
    }
}

/// Result of validating a read-then-write operation.
struct ValidationResult {
    cluster_id: ComputeInstanceId,
    replica_id: Option<ReplicaId>,
    timeline: TimelineContext,
    depends_on: BTreeSet<GlobalId>,
    /// The table descriptor, used for constraint validation.
    table_desc: RelationDesc,
}

/// Accumulated state for the OCC loop in `run_occ_loop`.
struct OccState {
    all_diffs: Vec<(Row, Timestamp, Diff)>,
    current_upper: Option<Timestamp>,
    retry_count: usize,
    byte_size: u64,
}

impl OccState {
    fn new() -> Self {
        Self {
            all_diffs: Vec::new(),
            current_upper: None,
            retry_count: 0,
            byte_size: 0,
        }
    }

    /// Forward all diff timestamps to `target_ts` and consolidate.
    ///
    /// After consolidation, `all_diffs` represents the net state of the
    /// query as of `target_ts`. Rows that were retracted by newer updates
    /// cancel out, and `byte_size` is recomputed to reflect the
    /// consolidated data.
    fn consolidate(&mut self, target_ts: Timestamp) {
        for (_, ts, _) in self.all_diffs.iter_mut() {
            *ts = target_ts;
        }
        consolidation::consolidate_updates(&mut self.all_diffs);
        self.byte_size = self
            .all_diffs
            .iter()
            .map(|(row, _, _)| u64::cast_from(row.byte_len()))
            .sum();
    }
}

/// Result of processing a single subscribe message in the OCC loop.
enum ProcessResult {
    Continue { ready_to_write: bool },
    NoRowsMatched,
    Error(AdapterError),
}

/// Process one subscribe message, updating `state` in place.
///
/// Data rows are accumulated into `state.all_diffs` (with per-row constraint
/// and max-result-size checks). Progress messages trigger consolidation and
/// can promote the accumulated diffs to "ready to write".
fn process_message(
    response: PeekResponseUnary,
    state: &mut OccState,
    as_of: Timestamp,
    max_result_size: u64,
    table_desc: &RelationDesc,
) -> ProcessResult {
    match response {
        PeekResponseUnary::Rows(mut rows) => {
            let mut saw_progress = false;

            while let Some(row) = rows.next() {
                let mut datums = row.iter();

                // Extract mz_timestamp (SubscribeOutput::Diffs format:
                // mz_timestamp, mz_progressed, mz_diff, ...data columns...).
                //
                // Format drift would mean we'd silently commit an incorrect
                // write, so surface every shape mismatch as an internal
                // error rather than panicking the process.
                let Some(ts_datum) = datums.next() else {
                    return ProcessResult::Error(AdapterError::Internal(
                        "missing mz_timestamp in subscribe output".into(),
                    ));
                };
                let ts = match ts_datum {
                    mz_repr::Datum::Numeric(n) => match n.0.try_into() {
                        Ok(ts_u64) => Timestamp::new(ts_u64),
                        Err(_) => {
                            return ProcessResult::Error(AdapterError::Internal(format!(
                                "mz_timestamp in subscribe output is not a valid u64: {n}"
                            )));
                        }
                    },
                    other => {
                        return ProcessResult::Error(AdapterError::Internal(format!(
                            "unexpected mz_timestamp datum: {other:?}"
                        )));
                    }
                };

                // Extract mz_progressed
                let Some(progressed_datum) = datums.next() else {
                    return ProcessResult::Error(AdapterError::Internal(
                        "missing mz_progressed in subscribe output".into(),
                    ));
                };
                let is_progress = matches!(progressed_datum, mz_repr::Datum::True);

                if is_progress {
                    state.current_upper = Some(ts);
                    saw_progress = true;

                    // Consolidate incrementally on each progress
                    // message. This keeps memory bounded by the
                    // consolidated size and makes the byte_size check
                    // below accurate (except for rows received between
                    // two progress messages, which is a small window).
                    state.consolidate(ts);

                    // The very first progress message we receive is
                    // always at `as_of`, emitted synchronously by
                    // `ActiveSubscribe::initialize` *before* any data
                    // batch is processed. At that point `all_diffs` is
                    // empty by construction, regardless of whether the
                    // snapshot is actually empty, so we must not
                    // conclude `NoRowsMatched` from it. Progress
                    // messages emitted later from `process_response`
                    // are gated on `batch.upper > as_of`, so any
                    // progress with `ts > as_of` is past the initial
                    // one and an empty `all_diffs` then genuinely
                    // means no rows matched. See
                    // `src/adapter/src/active_compute_sink.rs` for
                    // the emission order.
                    if ts > as_of && state.all_diffs.is_empty() {
                        return ProcessResult::NoRowsMatched;
                    }
                } else {
                    let Some(diff_datum) = datums.next() else {
                        return ProcessResult::Error(AdapterError::Internal(
                            "missing mz_diff in subscribe output".into(),
                        ));
                    };
                    let diff = match diff_datum {
                        mz_repr::Datum::Int64(d) => Diff::from(d),
                        other => {
                            return ProcessResult::Error(AdapterError::Internal(format!(
                                "unexpected mz_diff datum while processing read-then-write: {other:?}"
                            )));
                        }
                    };

                    let data_row = Row::pack(datums);

                    // Validate constraints for rows being added (positive diff)
                    if diff.is_positive() {
                        for (idx, datum) in data_row.iter().enumerate() {
                            if let Err(e) = table_desc.constraints_met(idx, &datum) {
                                return ProcessResult::Error(e.into());
                            }
                        }
                    }

                    state.byte_size = state
                        .byte_size
                        .saturating_add(u64::cast_from(data_row.byte_len()));
                    if state.byte_size > max_result_size {
                        return ProcessResult::Error(AdapterError::ResultSize(format!(
                            "result exceeds max size of {}",
                            max_result_size
                        )));
                    }
                    state.all_diffs.push((data_row, ts, diff));
                }
            }

            // We're ready to write once we've seen a progress
            // message and have accumulated any diffs. Data rows can
            // only arrive *after* the initial progress at `as_of`
            // (see the note in the progress branch), so a non-empty
            // `all_diffs` here implies we're past the initial
            // progress.
            let ready_to_write = saw_progress && !state.all_diffs.is_empty();
            ProcessResult::Continue { ready_to_write }
        }
        PeekResponseUnary::Error(e) => {
            ProcessResult::Error(AdapterError::Unstructured(anyhow::anyhow!(e)))
        }
        PeekResponseUnary::DependencyDropped(dep) => ProcessResult::Error(
            AdapterError::Unstructured(anyhow::anyhow!(dep.query_terminated_error())),
        ),
        PeekResponseUnary::Canceled => ProcessResult::Error(AdapterError::Canceled),
    }
}

/// Build the response returned when no rows matched the selection.
fn build_no_rows_response(
    kind: &MutationKind,
    returning: &[MirScalarExpr],
) -> Result<ExecuteResponse, AdapterError> {
    if !returning.is_empty() {
        let rows: Vec<Row> = vec![];
        return Ok(ExecuteResponse::SendingRowsImmediate {
            rows: Box::new(rows.into_row_iter()),
        });
    }
    Ok(match kind {
        MutationKind::Delete => ExecuteResponse::Deleted(0),
        MutationKind::Update => ExecuteResponse::Updated(0),
        MutationKind::Insert => ExecuteResponse::Inserted(0),
    })
}

/// Transform a MIR expression to produce the appropriate diffs for a mutation.
///
/// - DELETE: Negates the expression to produce `(row, -1)` diffs
/// - UPDATE: Unions negated old rows with mapped new rows to produce both
///   `(old_row, -1)` and `(new_row, +1)` diffs
fn apply_mutation_to_mir(
    expr: MirRelationExpr,
    kind: &MutationKind,
    assignments: &BTreeMap<usize, MirScalarExpr>,
) -> MirRelationExpr {
    match kind {
        MutationKind::Delete => MirRelationExpr::Negate {
            input: Box::new(expr),
        },
        MutationKind::Update => {
            let arity = expr.arity();

            // Find a fresh LocalId that won't conflict with any in the expression.
            //
            // Invariant: `Let` and `LetRec` are the only MIR nodes that *bind*
            // LocalIds; `Get` references them but does not introduce new ones.
            // So scanning just those two node kinds and picking `max + 1` is
            // guaranteed to produce an id unused by the subtree.
            let mut max_id = 0_u64;
            expr.visit_pre(|e| match e {
                MirRelationExpr::Let { id, .. } => {
                    max_id = std::cmp::max(max_id, id.into());
                }
                MirRelationExpr::LetRec { ids, .. } => {
                    for id in ids {
                        max_id = std::cmp::max(max_id, id.into());
                    }
                }
                _ => {}
            });
            let binding_id = LocalId::new(max_id + 1);

            let get_binding = MirRelationExpr::Get {
                id: Id::Local(binding_id),
                typ: expr.typ(),
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            };

            // Build map expressions
            let map_scalars: Vec<MirScalarExpr> = (0..arity)
                .map(|i| {
                    assignments
                        .get(&i)
                        .cloned()
                        .unwrap_or_else(|| MirScalarExpr::column(i))
                })
                .collect();

            let new_rows = get_binding
                .clone()
                .map(map_scalars)
                .project((arity..2 * arity).collect());

            let old_rows = MirRelationExpr::Negate {
                input: Box::new(get_binding),
            };

            let body = new_rows.union(old_rows);

            MirRelationExpr::Let {
                id: binding_id,
                value: Box::new(expr),
                body: Box::new(body),
            }
        }
        // INSERT: rows pass through unchanged; the subscribe emits them with diff +1.
        MutationKind::Insert => expr,
    }
}
