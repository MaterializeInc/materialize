// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_adapter_types::dyncfgs::CONSTRAINT_BASED_TIMESTAMP_SELECTION;
use mz_adapter_types::timestamp_selection::ConstraintBasedTimestampSelection;
use mz_compute_types::ComputeInstanceId;
use mz_expr::CollectionPlan;
use mz_ore::cast::CastLossy;
use mz_repr::Timestamp;
use mz_repr::optimize::OverrideFrom;
use mz_sql::catalog::CatalogCluster;
use mz_sql::plan::{Plan, QueryWhen};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::Statement;
use mz_transform::EmptyStatisticsOracle;
use opentelemetry::trace::TraceContextExt;
use tracing::{Span, debug};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::catalog::CatalogState;
use crate::coord::peek::{FastPathPlan, PeekPlan};
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::coord::{Coordinator, ExplainContext, TargetCluster};
use crate::optimize::Optimize;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::{
    AdapterError, AdapterNotice, CollectionIdBundle, ExecuteResponse, ReadHolds, SessionClient,
    TimelineContext, TimestampContext, TimestampProvider, optimize,
};
use crate::{coord, metrics};

impl SessionClient {
    pub(crate) async fn try_frontend_peek_inner(
        &mut self,
        portal_name: &str,
        session: &mut Session,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
        if session.transaction().is_in_multi_statement_transaction() {
            // TODO(peek-seq): handle multi-statement transactions
            debug!(
                "Bailing out from try_frontend_peek_inner, because is_in_multi_statement_transaction"
            );
            return Ok(None);
        }

        if session.vars().emit_timestamp_notice() {
            // TODO(peek-seq): implement this. See end of peek_finish
            debug!("Bailing out from try_frontend_peek_inner, because emit_timestamp_notice");
            return Ok(None);
        }

        // # From handle_execute

        if session.vars().emit_trace_id_notice() {
            let span_context = tracing::Span::current()
                .context()
                .span()
                .span_context()
                .clone();
            if span_context.is_valid() {
                session.add_notice(AdapterNotice::QueryTrace {
                    trace_id: span_context.trace_id(),
                });
            }
        }

        // This is from handle_execute_inner, but we do it already here because of lifetime issues,
        // and also to be able to give a catalog to `verify_portal`.
        //
        // TODO(peek-seq): This snapshot is wasted when we end up bailing out from the frontend peek
        // sequencing. I think the best way to solve this is with that optimization where we
        // continuously keep a catalog snapshot in the session, and only get a new one when the
        // catalog revision has changed, which we could see with an atomic read.
        let catalog = self.catalog_snapshot("try_frontend_peek").await;

        if let Err(_) = Coordinator::verify_portal(&*catalog, session, portal_name) {
            // TODO(peek-seq): Don't fall back to the coordinator's peek sequencing here, but retire already.
            debug!(
                "Bailing out from try_frontend_peek_inner, because verify_portal returned an error"
            );
            return Ok(None);
        }

        // TODO(peek-seq): statement logging (and then enable it in various tests)
        let (stmt, params) = {
            let portal = session
                .get_portal_unverified(portal_name)
                .expect("called verify_portal above");
            let params = portal.parameters.clone();
            let stmt = portal.stmt.clone();
            (stmt, params)
        };

        let stmt = match stmt {
            Some(stmt) => stmt,
            None => {
                debug!("try_frontend_peek_inner succeeded on an empty query");
                return Ok(Some(ExecuteResponse::EmptyQuery));
            }
        };

        if !matches!(*stmt, Statement::Select(_)) {
            debug!("Bailing out from try_frontend_peek_inner, because it's not a SELECT");
            return Ok(None);
        }

        let session_type = metrics::session_type_label_value(session.user());
        let stmt_type = metrics::statement_type_label_value(&stmt);

        // # From handle_execute_inner

        let conn_catalog = catalog.for_session(session);
        // `resolved_ids` should be derivable from `stmt`. If `stmt` is transformed to remove/add
        // IDs, then `resolved_ids` should be updated to also remove/add those IDs.
        let (stmt, resolved_ids) = mz_sql::names::resolve(&conn_catalog, (*stmt).clone())?;

        let pcx = session.pcx();
        let plan = mz_sql::plan::plan(Some(pcx), &conn_catalog, stmt, &params, &resolved_ids)?;
        let select_plan = match &plan {
            Plan::Select(select_plan) => select_plan,
            _ => {
                debug!("Bailing out from try_frontend_peek_inner, because it's not a SELECT");
                return Ok(None);
            }
        };
        let explain_ctx = ExplainContext::None; // EXPLAIN is not handled here for now, only SELECT

        // # From sequence_plan

        // We have checked the plan kind above.
        assert!(plan.allowed_in_read_only());

        let target_cluster = match session.transaction().cluster() {
            // Use the current transaction's cluster.
            Some(cluster_id) => TargetCluster::Transaction(cluster_id),
            // If there isn't a current cluster set for a transaction, then try to auto route.
            None => {
                coord::catalog_serving::auto_run_on_catalog_server(&conn_catalog, session, &plan)
            }
        };
        let (cluster, target_cluster_id, target_cluster_name) = {
            let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
            (cluster, cluster.id, &cluster.name)
        };

        // TODO(peek-seq): statement logging: set_statement_execution_cluster

        if let Err(e) = coord::catalog_serving::check_cluster_restrictions(
            target_cluster_name.as_str(),
            &conn_catalog,
            &plan,
        ) {
            return Err(e);
        }

        if let Err(e) = rbac::check_plan(
            &conn_catalog,
            |_id| {
                // This is only used by `Plan::SideEffectingFunc`, so it is irrelevant for us here
                // TODO(peek-seq): refactor `check_plan` to make this nicer
                unreachable!()
            },
            session,
            &plan,
            Some(target_cluster_id),
            &resolved_ids,
        ) {
            return Err(e.into());
        }

        // Check if we're still waiting for any of the builtin table appends from when we
        // started the Session to complete.
        //
        // (This is done slightly earlier in the normal peek sequencing, but we have to be past the
        // last use of `conn_catalog` here.)
        if let Some(_) = coord::appends::waiting_on_startup_appends(&*catalog, session, &plan) {
            // TODO(peek-seq): Don't fall back to the coordinator's peek sequencing here, but call
            // `defer_op`.
            debug!("Bailing out from try_frontend_peek_inner, because waiting_on_startup_appends");
            return Ok(None);
        }

        let max_query_result_size = Some(session.vars().max_query_result_size());

        // # From sequence_peek

        if session.vars().emit_plan_insights_notice() {
            // TODO(peek-seq): We'll need to do this when we want the frontend peek sequencing to
            // take over from the old sequencing code.
            debug!("Bailing out from try_frontend_peek_inner, because emit_plan_insights_notice");
            return Ok(None);
        }

        // # From peek_validate

        //let compute_instance_snapshot = self.peek_client().snapshot(cluster.id()).await.unwrap();
        let compute_instance_snapshot =
            ComputeInstanceSnapshot::new_without_collections(cluster.id());

        let (_, view_id) = self.peek_client().transient_id_gen.allocate_id();
        let (_, index_id) = self.peek_client().transient_id_gen.allocate_id();

        let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
            .override_from(&catalog.get_cluster(cluster.id()).config.features());

        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let mut optimizer = optimize::peek::Optimizer::new(
            Arc::clone(&catalog),
            compute_instance_snapshot.clone(),
            select_plan.finishing.clone(),
            view_id,
            index_id,
            optimizer_config,
            self.peek_client().optimizer_metrics.clone(),
        );

        let target_replica_name = session.vars().cluster_replica();
        let mut target_replica = target_replica_name
            .map(|name| {
                cluster
                    .replica_id(name)
                    .ok_or(AdapterError::UnknownClusterReplica {
                        cluster_name: cluster.name.clone(),
                        replica_name: name.to_string(),
                    })
            })
            .transpose()?;

        let source_ids = select_plan.source.depends_on();
        // TODO(peek-seq): validate_timeline_context can be expensive in real scenarios (not in
        // simple benchmarks), because it traverses transitive dependencies even of indexed views and
        // materialized views (also traversing their MIR plans).
        let mut timeline_context = catalog.validate_timeline_context(source_ids.iter().copied())?;
        if matches!(timeline_context, TimelineContext::TimestampIndependent)
            && select_plan.source.contains_temporal()?
        {
            // If the source IDs are timestamp independent but the query contains temporal functions,
            // then the timeline context needs to be upgraded to timestamp dependent. This is
            // required because `source_ids` doesn't contain functions.
            timeline_context = TimelineContext::TimestampDependent;
        }

        let notices = coord::sequencer::check_log_reads(
            &catalog,
            cluster,
            &source_ids,
            &mut target_replica,
            session.vars(),
        )?;
        session.add_notices(notices);

        // # From peek_linearize_timestamp

        let isolation_level = session.vars().transaction_isolation().clone();
        let timeline = Coordinator::get_timeline(&timeline_context);
        let needs_linearized_read_ts =
            Coordinator::needs_linearized_read_ts(&isolation_level, &select_plan.when);

        let oracle_read_ts = match timeline {
            Some(timeline) if needs_linearized_read_ts => {
                let oracle = self.peek_client_mut().ensure_oracle(timeline).await?;
                let oracle_read_ts = oracle.read_ts().await;
                Some(oracle_read_ts)
            }
            Some(_) | None => None,
        };

        // # From peek_real_time_recency

        // TODO(peek-seq): Real-time recency is slow anyhow, so we don't handle it in frontend peek
        // sequencing for now.
        let vars = session.vars();
        if vars.real_time_recency()
            && vars.transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            debug!("Bailing out from try_frontend_peek_inner, because of real time recency");
            return Ok(None);
        }
        let real_time_recency_ts: Option<mz_repr::Timestamp> = None;

        // # From peek_timestamp_read_hold

        let dataflow_builder = DataflowBuilder::new(catalog.state(), compute_instance_snapshot);
        let input_id_bundle = dataflow_builder.sufficient_collections(source_ids.clone());

        // ## From sequence_peek_timestamp

        let in_immediate_multi_stmt_txn = session
            .transaction()
            .in_immediate_multi_stmt_txn(&select_plan.when);

        // Fetch or generate a timestamp for this query and fetch or acquire read holds.
        let (determination, read_holds) = match session.get_transaction_timestamp_determination() {
            // Use the transaction's timestamp if it exists and this isn't an AS OF query.
            Some(
                determination @ TimestampDetermination {
                    timestamp_context: TimestampContext::TimelineTimestamp { .. },
                    ..
                },
            ) if in_immediate_multi_stmt_txn => (determination, None),
            _ => {
                let determine_bundle = if in_immediate_multi_stmt_txn {
                    // TODO(peek-seq): handle multi-statement transactions
                    // needs timedomain_for, which needs DataflowBuilder / index oracle / sufficient_collections
                    debug!(
                        "Bailing out from try_frontend_peek_inner, because of in_immediate_multi_stmt_txn"
                    );
                    return Ok(None);
                } else {
                    // If not in a transaction, use the source.
                    &input_id_bundle
                };
                let (determination, read_holds) = self
                    .frontend_determine_timestamp(
                        catalog.state(),
                        session,
                        determine_bundle,
                        &select_plan.when,
                        target_cluster_id,
                        &timeline_context,
                        oracle_read_ts,
                        real_time_recency_ts,
                    )
                    .await?;
                // We only need read holds if the read depends on a timestamp.
                let read_holds = match determination.timestamp_context.timestamp() {
                    Some(_ts) => Some(read_holds),
                    None => {
                        // We don't need the read holds and shouldn't add them
                        // to the txn.
                        //
                        // TODO(peek-seq): Handle this within determine_timestamp?
                        drop(read_holds);
                        None
                    }
                };
                (determination, read_holds)
            }
        };

        // The old peek sequencing's sequence_peek_timestamp does two more things here:
        // the txn_read_holds stuff and session.add_transaction_ops. We do these later in the new
        // peek sequencing code, because at this point we might still bail out from the new peek
        // sequencing, in which case we don't want the mentioned side effects to happen.

        // # From peek_optimize

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let timestamp_context = determination.timestamp_context.clone();
        if session.vars().enable_session_cardinality_estimates() {
            debug!(
                "Bailing out from try_frontend_peek_inner, because of enable_session_cardinality_estimates"
            );
            return Ok(None);
        }
        let stats = Box::new(EmptyStatisticsOracle);
        let session_meta = session.meta();
        let now = catalog.config().now.clone();
        let select_plan = select_plan.clone();

        // TODO(peek-seq): if explain_ctx.needs_plan_insights() ...

        let span = Span::current();

        let (global_lir_plan, _optimization_finished_at) = match mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                span.in_scope(|| {
                    let raw_expr = select_plan.source.clone();

                    // HIR ⇒ MIR lowering and MIR optimization (local)
                    let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                    // Attach resolved context required to continue the pipeline.
                    let local_mir_plan =
                        local_mir_plan.resolve(timestamp_context.clone(), &session_meta, stats);
                    // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                    let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                    let optimization_finished_at = now();

                    // TODO(peek-seq): plan_insights stuff

                    Ok::<_, AdapterError>((global_lir_plan, optimization_finished_at))
                })
            },
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(adapter_error)) => {
                return Err(adapter_error);
            }
            Err(_join_error) => {
                // Should only happen if the runtime is shutting down, because we
                // - never call `abort`;
                // - catch panics with `catch_unwind_optimize`.
                return Err(AdapterError::Unstructured(anyhow::anyhow!(
                    "peek optimization aborted, because the system is shutting down"
                )));
            }
        };

        // # From peek_finish

        // TODO(peek-seq): statement logging

        let (peek_plan, df_meta, typ) = global_lir_plan.unapply();

        // TODO(peek-seq): plan_insights stuff

        // This match is based on what `implement_fast_path_peek_plan` supports.
        let fast_path_plan = match peek_plan {
            PeekPlan::SlowPath(_) => {
                debug!("Bailing out from try_frontend_peek_inner, because it's a slow-path peek");
                return Ok(None);
            }
            PeekPlan::FastPath(p @ FastPathPlan::Constant(_, _))
            | PeekPlan::FastPath(p @ FastPathPlan::PeekExisting(_, _, _, _)) => p,
            PeekPlan::FastPath(FastPathPlan::PeekPersist(_, _, _)) => {
                debug!(
                    "Bailing out from try_frontend_peek_inner, because it's a Persist fast path peek"
                );
                return Ok(None);
            }
        };

        // Warning: Do not bail out from the new peek sequencing after this point, because the
        // following has side effects.

        coord::sequencer::emit_optimizer_notices(&*catalog, session, &df_meta.optimizer_notices);

        // # We do the second half of sequence_peek_timestamp, as mentioned above.

        // TODO(peek-seq): txn_read_holds stuff. Add SessionClient::txn_read_holds.

        // (This TODO is copied from the old peek sequencing.)
        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let mut transaction_determination = determination.clone();
        let requires_linearization = (&explain_ctx).into();
        if select_plan.when.is_transactional() {
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id: target_cluster_id,
                requires_linearization,
            })?;
        } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
            // If the query uses AS OF, then ignore the timestamp.
            transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
            session.add_transaction_ops(TransactionOps::Peeks {
                determination: transaction_determination,
                cluster_id: target_cluster_id,
                requires_linearization,
            })?;
        };

        // TODO(peek-seq): move this up to the beginning of the function when we have eliminated all
        // the fallbacks to the old peek sequencing. Currently, it has to be here to avoid
        // double-counting a fallback situation, but this has the drawback that if we error out
        // from this function then we don't count the peek at all.
        session
            .metrics()
            .query_total(&[session_type, stmt_type])
            .inc();

        // # Now back to peek_finish

        // TODO(peek-seq): statement logging

        let max_result_size = catalog.system_config().max_result_size();

        let row_set_finishing_seconds = session.metrics().row_set_finishing_seconds().clone();

        // Implement the peek, and capture the response.
        let resp = self
            .peek_client_mut()
            .implement_fast_path_peek_plan(
                fast_path_plan,
                determination.timestamp_context.timestamp_or_default(),
                select_plan.finishing,
                target_cluster_id,
                target_replica,
                typ,
                max_result_size,
                max_query_result_size,
                row_set_finishing_seconds,
                read_holds,
            )
            .await?;

        Ok(Some(resp))
    }

    /// (Similar to Coordinator::determine_timestamp)
    /// Determines the timestamp for a query, acquires read holds that ensure the
    /// query remains executable at that time, and returns those.
    /// The caller is responsible for eventually dropping those read holds.
    ///
    /// Note: self is taken &mut because of the lazy fetching in `get_compute_instance_client`.
    pub(crate) async fn frontend_determine_timestamp(
        &mut self,
        catalog_state: &CatalogState,
        session: &Session,
        id_bundle: &CollectionIdBundle,
        when: &QueryWhen,
        compute_instance: ComputeInstanceId,
        timeline_context: &TimelineContext,
        oracle_read_ts: Option<Timestamp>,
        real_time_recency_ts: Option<Timestamp>,
    ) -> Result<(TimestampDetermination<Timestamp>, ReadHolds<Timestamp>), AdapterError> {
        // this is copy-pasted from Coordinator

        let constraint_based = ConstraintBasedTimestampSelection::from_str(
            &CONSTRAINT_BASED_TIMESTAMP_SELECTION.get(catalog_state.system_config().dyncfgs()),
        );

        let isolation_level = session.vars().transaction_isolation();

        let (read_holds, upper) = self
            .peek_client_mut()
            .acquire_read_holds_and_collection_write_frontiers(id_bundle)
            .await
            .expect("missing collection");
        let (det, read_holds) = <Coordinator as TimestampProvider>::determine_timestamp_for_inner(
            session,
            id_bundle,
            when,
            compute_instance,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            isolation_level,
            &constraint_based,
            read_holds,
            upper.clone(),
        )?;

        session
            .metrics()
            .determine_timestamp(&[
                match det.respond_immediately() {
                    true => "true",
                    false => "false",
                },
                isolation_level.as_str(),
                &compute_instance.to_string(),
                constraint_based.as_str(),
            ])
            .inc();
        if !det.respond_immediately()
            && isolation_level == &IsolationLevel::StrictSerializable
            && real_time_recency_ts.is_none()
        {
            // Note down the difference between StrictSerializable and Serializable into a metric.
            if let Some(strict) = det.timestamp_context.timestamp() {
                let (serializable_det, _tmp_read_holds) =
                    <Coordinator as TimestampProvider>::determine_timestamp_for_inner(
                        session,
                        id_bundle,
                        when,
                        compute_instance,
                        timeline_context,
                        oracle_read_ts,
                        real_time_recency_ts,
                        isolation_level,
                        &constraint_based,
                        read_holds.clone(),
                        upper,
                    )?;
                if let Some(serializable) = serializable_det.timestamp_context.timestamp() {
                    session
                        .metrics()
                        .timestamp_difference_for_strict_serializable_ms(&[
                            &compute_instance.to_string().as_ref(),
                            constraint_based.as_str(),
                        ])
                        .observe(f64::cast_lossy(u64::from(
                            strict.saturating_sub(*serializable),
                        )));
                }
            }
        }

        Ok((det, read_holds))
    }
}
