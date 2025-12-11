// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use itertools::{Either, Itertools};
use mz_adapter_types::dyncfgs::CONSTRAINT_BASED_TIMESTAMP_SELECTION;
use mz_adapter_types::timestamp_selection::ConstraintBasedTimestampSelection;
use mz_compute_types::ComputeInstanceId;
use mz_expr::{CollectionPlan, ResultSpec};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::CollectionExt;
use mz_ore::now::EpochMillis;
use mz_ore::{soft_assert_eq_or_log, soft_assert_or_log};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, GlobalId, IntoRowIterator, Timestamp};
use mz_sql::catalog::CatalogCluster;
use mz_sql::plan::{self, Plan, QueryWhen};
use mz_sql::rbac;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql::session::vars::IsolationLevel;
use mz_sql_parser::ast::{CopyDirection, ExplainStage, Statement};
use mz_transform::EmptyStatisticsOracle;
use mz_transform::dataflow::DataflowMetainfo;
use opentelemetry::trace::TraceContextExt;
use tracing::{Span, debug};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::catalog::CatalogState;
use crate::command::Command;
use crate::coord::peek::PeekPlan;
use crate::coord::sequencer::{eval_copy_to_uri, statistics_oracle};
use crate::coord::timeline::timedomain_for;
use crate::coord::timestamp_selection::TimestampDetermination;
use crate::coord::{Coordinator, CopyToContext, ExplainContext, ExplainPlanContext, TargetCluster};
use crate::explain::insights::PlanInsightsContext;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::{ComputeInstanceSnapshot, DataflowBuilder};
use crate::optimize::{Optimize, OptimizerError};
use crate::session::{Session, TransactionOps, TransactionStatus};
use crate::{
    AdapterError, AdapterNotice, CollectionIdBundle, ExecuteResponse, PeekClient, ReadHolds,
    TimelineContext, TimestampContext, TimestampProvider, optimize,
};
use crate::{coord, metrics};

impl PeekClient {
    pub(crate) async fn try_frontend_peek_inner(
        &mut self,
        portal_name: &str,
        session: &mut Session,
    ) -> Result<Option<ExecuteResponse>, AdapterError> {
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

        // TODO(peek-seq): This snapshot is wasted when we end up bailing out from the frontend peek
        // sequencing. We could solve this is with that optimization where we
        // continuously keep a catalog snapshot in the session, and only get a new one when the
        // catalog revision has changed, which we could see with an atomic read.
        // But anyhow, this problem will just go away when we reach the point that we never fall
        // back to the old sequencing.
        let catalog = self.catalog_snapshot("try_frontend_peek_inner").await;

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
                // The portal is a session-level thing, so it couldn't have concurrently disappeared
                // since the above verification.
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

        // Before planning, check if this is a statement type we can handle.
        match &*stmt {
            Statement::Select(_)
            | Statement::ExplainAnalyzeObject(_)
            | Statement::ExplainAnalyzeCluster(_) => {
                // These are always fine, just continue.
                // Note: EXPLAIN ANALYZE will `plan` to `Plan::Select`.
            }
            Statement::ExplainPlan(explain_stmt) => {
                // Only handle ExplainPlan for SELECT statements.
                // We don't want to handle e.g. EXPLAIN CREATE MATERIALIZED VIEW here, because that
                // requires purification before planning, which the frontend peek sequencing doesn't
                // do.
                match &explain_stmt.explainee {
                    mz_sql_parser::ast::Explainee::Select(..) => {
                        // This is a SELECT, continue
                    }
                    _ => {
                        debug!(
                            "Bailing out from try_frontend_peek_inner, because EXPLAIN is not for a SELECT query"
                        );
                        return Ok(None);
                    }
                }
            }
            Statement::ExplainPushdown(explain_stmt) => {
                // Only handle EXPLAIN FILTER PUSHDOWN for SELECT statements
                match &explain_stmt.explainee {
                    mz_sql_parser::ast::Explainee::Select(..) => {}
                    _ => {
                        debug!(
                            "Bailing out from try_frontend_peek_inner, because EXPLAIN FILTER PUSHDOWN is not for a SELECT query"
                        );
                        return Ok(None);
                    }
                }
            }
            Statement::Copy(copy_stmt) => {
                match &copy_stmt.direction {
                    CopyDirection::To => {
                        // This is COPY TO, continue
                    }
                    CopyDirection::From => {
                        debug!(
                            "Bailing out from try_frontend_peek_inner, because COPY FROM is not supported"
                        );
                        return Ok(None);
                    }
                }
            }
            _ => {
                debug!(
                    "Bailing out from try_frontend_peek_inner, because statement type is not supported"
                );
                return Ok(None);
            }
        }

        let session_type = metrics::session_type_label_value(session.user());
        let stmt_type = metrics::statement_type_label_value(&stmt);

        // # From handle_execute_inner

        let conn_catalog = catalog.for_session(session);
        // (`resolved_ids` should be derivable from `stmt`. If `stmt` is later transformed to
        // remove/add IDs, then `resolved_ids` should be updated to also remove/add those IDs.)
        let (stmt, resolved_ids) = mz_sql::names::resolve(&conn_catalog, (*stmt).clone())?;

        let pcx = session.pcx();
        let plan = mz_sql::plan::plan(Some(pcx), &conn_catalog, stmt, &params, &resolved_ids)?;
        let (select_plan, explain_ctx, copy_to_ctx) = match &plan {
            Plan::Select(select_plan) => {
                let explain_ctx = if session.vars().emit_plan_insights_notice() {
                    let optimizer_trace = OptimizerTrace::new(ExplainStage::PlanInsights.paths());
                    ExplainContext::PlanInsightsNotice(optimizer_trace)
                } else {
                    ExplainContext::None
                };
                (select_plan, explain_ctx, None)
            }
            Plan::ExplainPlan(plan::ExplainPlanPlan {
                stage,
                format,
                config,
                explainee:
                    plan::Explainee::Statement(plan::ExplaineeStatement::Select { broken, plan, desc }),
            }) => {
                // Create OptimizerTrace to collect optimizer plans
                let optimizer_trace = OptimizerTrace::new(stage.paths());
                let explain_ctx = ExplainContext::Plan(ExplainPlanContext {
                    broken: *broken,
                    config: config.clone(),
                    format: *format,
                    stage: *stage,
                    replan: None,
                    desc: Some(desc.clone()),
                    optimizer_trace,
                });
                (plan, explain_ctx, None)
            }
            // COPY TO S3
            Plan::CopyTo(plan::CopyToPlan {
                select_plan,
                desc,
                to,
                connection,
                connection_id,
                format,
                max_file_size,
            }) => {
                let uri = eval_copy_to_uri(to.clone(), session, catalog.state())?;

                // (output_batch_count will be set later)
                let copy_to_ctx = CopyToContext {
                    desc: desc.clone(),
                    uri,
                    connection: connection.clone(),
                    connection_id: *connection_id,
                    format: format.clone(),
                    max_file_size: *max_file_size,
                    output_batch_count: None,
                };

                (select_plan, ExplainContext::None, Some(copy_to_ctx))
            }
            Plan::ExplainPushdown(plan::ExplainPushdownPlan { explainee }) => {
                // Only handle EXPLAIN FILTER PUSHDOWN for SELECT statements
                match explainee {
                    plan::Explainee::Statement(plan::ExplaineeStatement::Select {
                        broken: false,
                        plan,
                        desc: _,
                    }) => {
                        let explain_ctx = ExplainContext::Pushdown;
                        (plan, explain_ctx, None)
                    }
                    _ => {
                        debug!(
                            "Bailing out from try_frontend_peek_inner, because EXPLAIN FILTER PUSHDOWN is not for a SELECT query or is EXPLAIN BROKEN"
                        );
                        return Ok(None);
                    }
                }
            }
            _ => {
                debug!(
                    "Bailing out from try_frontend_peek_inner, because the Plan is not a SELECT, EXPLAIN SELECT, EXPLAIN FILTER PUSHDOWN, or COPY TO"
                );
                return Ok(None);
            }
        };

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

        coord::catalog_serving::check_cluster_restrictions(
            target_cluster_name.as_str(),
            &conn_catalog,
            &plan,
        )?;

        rbac::check_plan(
            &conn_catalog,
            None::<fn(u32) -> Option<RoleId>>,
            session,
            &plan,
            Some(target_cluster_id),
            &resolved_ids,
        )?;

        // Check if we're still waiting for any of the builtin table appends from when we
        // started the Session to complete.
        //
        // (This is done slightly earlier in the normal peek sequencing, but we have to be past the
        // last use of `conn_catalog` here.)
        if let Some(_) = coord::appends::waiting_on_startup_appends(&*catalog, session, &plan) {
            // TODO(peek-seq): Don't fall back to the coordinator's peek sequencing here, but call
            // `defer_op`. Needs `ExecuteContext`.
            // This fallback is currently causing a bug: `waiting_on_startup_appends` has the
            // side effect that it already clears the wait flag, and therefore the old peek
            // sequencing that we fall back to here won't do waiting. This is tested by
            // `test_mz_sessions` and `test_pg_cancel_dropped_role`, where I've disabled the
            // frontend peek sequencing for now. This bug will just go away once we don't fall back
            // to the old peek sequencing here, but properly call `defer_op` instead.
            debug!("Bailing out from try_frontend_peek_inner, because waiting_on_startup_appends");
            return Ok(None);
        }

        let max_query_result_size = Some(session.vars().max_query_result_size());

        // # From sequence_peek

        // # From peek_validate

        let compute_instance_snapshot =
            ComputeInstanceSnapshot::new_without_collections(cluster.id());

        let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
            .override_from(&catalog.get_cluster(cluster.id()).config.features())
            .override_from(&explain_ctx);

        if cluster.replicas().next().is_none() && explain_ctx.needs_cluster() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let (_, view_id) = self.transient_id_gen.allocate_id();
        let (_, index_id) = self.transient_id_gen.allocate_id();

        let mut optimizer = if let Some(mut copy_to_ctx) = copy_to_ctx {
            // COPY TO path: calculate output_batch_count and create copy_to optimizer
            let worker_counts = cluster.replicas().map(|r| {
                let loc = &r.config.location;
                loc.workers().unwrap_or_else(|| loc.num_processes())
            });
            let max_worker_count = match worker_counts.max() {
                Some(count) => u64::cast_from(count),
                None => {
                    return Err(AdapterError::NoClusterReplicasAvailable {
                        name: cluster.name.clone(),
                        is_managed: cluster.is_managed(),
                    });
                }
            };
            copy_to_ctx.output_batch_count = Some(max_worker_count);

            Either::Right(optimize::copy_to::Optimizer::new(
                Arc::clone(&catalog),
                compute_instance_snapshot.clone(),
                view_id,
                copy_to_ctx,
                optimizer_config,
                self.optimizer_metrics.clone(),
            ))
        } else {
            // SELECT/EXPLAIN path: create peek optimizer
            Either::Left(optimize::peek::Optimizer::new(
                Arc::clone(&catalog),
                compute_instance_snapshot.clone(),
                select_plan.finishing.clone(),
                view_id,
                index_id,
                optimizer_config,
                self.optimizer_metrics.clone(),
            ))
        };

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
                let oracle = self.ensure_oracle(timeline).await?;
                let oracle_read_ts = oracle.read_ts().await;
                Some(oracle_read_ts)
            }
            Some(_) | None => None,
        };

        // # From peek_real_time_recency

        let vars = session.vars();
        let real_time_recency_ts: Option<Timestamp> = if vars.real_time_recency()
            && vars.transaction_isolation() == &IsolationLevel::StrictSerializable
            && !session.contains_read_timestamp()
        {
            // Only call the coordinator when we actually need real-time recency
            self.call_coordinator(|tx| Command::DetermineRealTimeRecentTimestamp {
                source_ids: source_ids.clone(),
                real_time_recency_timeout: *vars.real_time_recency_timeout(),
                tx,
            })
            .await?
        } else {
            None
        };

        // # From peek_timestamp_read_hold

        let dataflow_builder = DataflowBuilder::new(catalog.state(), compute_instance_snapshot);
        let input_id_bundle = dataflow_builder.sufficient_collections(source_ids.clone());

        // ## From sequence_peek_timestamp

        // Warning: This will be false for AS OF queries, even if we are otherwise inside a
        // multi-statement transaction. (It's also false for FreshestTableWrite, which is currently
        // only read-then-write queries, which can't be part of multi-statement transactions, so
        // FreshestTableWrite doesn't matter.)
        //
        // TODO(peek-seq): It's not totally clear to me what the intended semantics are for AS OF
        // queries inside a transaction: We clearly can't use the transaction timestamp, but the old
        // peek sequencing still does a timedomain validation. The new peek sequencing does not do
        // timedomain validation for AS OF queries, which seems more natural. But I'm thinking that
        // it would be the cleanest to just simply disallow AS OF queries inside transactions.
        let in_immediate_multi_stmt_txn = session
            .transaction()
            .in_immediate_multi_stmt_txn(&select_plan.when);

        // Fetch or generate a timestamp for this query and fetch or acquire read holds.
        let (determination, read_holds) = match session.get_transaction_timestamp_determination() {
            // Use the transaction's timestamp if it exists and this isn't an AS OF query.
            // (`in_immediate_multi_stmt_txn` is false for AS OF queries.)
            Some(
                determination @ TimestampDetermination {
                    timestamp_context: TimestampContext::TimelineTimestamp { .. },
                    ..
                },
            ) if in_immediate_multi_stmt_txn => {
                // This is a subsequent (non-AS OF, non-constant) query in a multi-statement
                // transaction. We now:
                // - Validate that the query only accesses collections within the transaction's
                //   timedomain (which we know from the stored read holds).
                // - Use the transaction's stored timestamp determination.
                // - Use the (relevant subset of the) transaction's read holds.

                let txn_read_holds_opt = self
                    .call_coordinator(|tx| Command::GetTransactionReadHoldsBundle {
                        conn_id: session.conn_id().clone(),
                        tx,
                    })
                    .await;

                if let Some(txn_read_holds) = txn_read_holds_opt {
                    let allowed_id_bundle = txn_read_holds.id_bundle();
                    let outside = input_id_bundle.difference(&allowed_id_bundle);

                    // Queries without a timestamp and timeline can belong to any existing timedomain.
                    if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
                        let valid_names =
                            allowed_id_bundle.resolve_names(&*catalog, session.conn_id());
                        let invalid_names = outside.resolve_names(&*catalog, session.conn_id());
                        return Err(AdapterError::RelationOutsideTimeDomain {
                            relations: invalid_names,
                            names: valid_names,
                        });
                    }

                    // Extract the subset of read holds for the collections this query accesses.
                    let read_holds = txn_read_holds.subset(&input_id_bundle);

                    (determination, read_holds)
                } else {
                    // This should never happen: we're in a subsequent query of a multi-statement
                    // transaction (we have a transaction timestamp), but the coordinator has no
                    // transaction read holds stored. This indicates a bug in the transaction
                    // handling.
                    return Err(AdapterError::Internal(
                        "Missing transaction read holds for multi-statement transaction"
                            .to_string(),
                    ));
                }
            }
            _ => {
                // There is no timestamp determination yet for this transaction. Either:
                // - We are not in a multi-statement transaction.
                // - This is the first (non-AS OF) query in a multi-statement transaction.
                // - This is an AS OF query.
                // - This is a constant query (`TimestampContext::NoTimestamp`).

                let timedomain_bundle;
                let determine_bundle = if in_immediate_multi_stmt_txn {
                    // This is the first (non-AS OF) query in a multi-statement transaction.
                    // Determine a timestamp that will be valid for anything in any schema
                    // referenced by the first query.
                    timedomain_bundle = timedomain_for(
                        &*catalog,
                        &dataflow_builder,
                        &source_ids,
                        &timeline_context,
                        session.conn_id(),
                        target_cluster_id,
                    )?;
                    &timedomain_bundle
                } else {
                    // Simply use the inputs of the current query.
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

                // If this is the first (non-AS OF) query in a multi-statement transaction, store
                // the read holds in the coordinator, so subsequent queries can validate against
                // them.
                if in_immediate_multi_stmt_txn {
                    self.call_coordinator(|tx| Command::StoreTransactionReadHolds {
                        conn_id: session.conn_id().clone(),
                        read_holds: read_holds.clone(),
                        tx,
                    })
                    .await;
                }

                (determination, read_holds)
            }
        };

        {
            // Assert that we have a read hold for all the collections in our `input_id_bundle`.
            for id in input_id_bundle.iter() {
                let s = read_holds.storage_holds.contains_key(&id);
                let c = read_holds
                    .compute_ids()
                    .map(|(_instance, coll)| coll)
                    .contains(&id);
                soft_assert_or_log!(
                    s || c,
                    "missing read hold for collection {} in `input_id_bundle",
                    id
                );
            }

            // Assert that each part of the `input_id_bundle` corresponds to the right part of
            // `read_holds`.
            for id in input_id_bundle.storage_ids.iter() {
                soft_assert_or_log!(
                    read_holds.storage_holds.contains_key(id),
                    "missing storage read hold for collection {} in `input_id_bundle",
                    id
                );
            }
            for id in input_id_bundle
                .compute_ids
                .iter()
                .flat_map(|(_instance, colls)| colls)
            {
                soft_assert_or_log!(
                    read_holds
                        .compute_ids()
                        .map(|(_instance, coll)| coll)
                        .contains(id),
                    "missing compute read hold for collection {} in `input_id_bundle",
                    id,
                );
            }
        }

        // (TODO(peek-seq): The below TODO is copied from the old peek sequencing. We should resolve
        // this when we decide what to with `AS OF` in transactions.)
        // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
        // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
        // set the transaction ops. Decide and document what our policy should be on AS OF queries.
        // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
        // what's going on there. This should probably get a small design document.

        // We only track the peeks in the session if the query doesn't use AS
        // OF or we're inside an explicit transaction. The latter case is
        // necessary to support PG's `BEGIN` semantics, whose behavior can
        // depend on whether or not reads have occurred in the txn.
        let requires_linearization = (&explain_ctx).into();
        let mut transaction_determination = determination.clone();
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

        // # From peek_optimize

        let stats = statistics_oracle(
            session,
            &source_ids,
            &determination.timestamp_context.antichain(),
            true,
            catalog.system_config(),
            &*self.storage_collections,
        )
        .await
        .unwrap_or_else(|_| Box::new(EmptyStatisticsOracle));

        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let timestamp_context = determination.timestamp_context.clone();
        let session_meta = session.meta();
        let now = catalog.config().now.clone();
        let select_plan = select_plan.clone();
        let target_cluster_name = target_cluster_name.clone();
        let needs_plan_insights = explain_ctx.needs_plan_insights();
        let determination_for_pushdown = if matches!(explain_ctx, ExplainContext::Pushdown) {
            // This is a hairy data structure, so avoid this clone if we are not in
            // EXPLAIN FILTER PUSHDOWN.
            Some(determination.clone())
        } else {
            None
        };

        let span = Span::current();

        // Prepare data for plan insights if needed
        let catalog_for_insights = if needs_plan_insights {
            Some(Arc::clone(&catalog))
        } else {
            None
        };
        let mut compute_instances = BTreeMap::new();
        if needs_plan_insights {
            for user_cluster in catalog.user_clusters() {
                let snapshot = ComputeInstanceSnapshot::new_without_collections(user_cluster.id);
                compute_instances.insert(user_cluster.name.clone(), snapshot);
            }
        }

        // Enum for branching among various execution steps after optimization
        enum Execution {
            Peek {
                global_lir_plan: optimize::peek::GlobalLirPlan,
                optimization_finished_at: EpochMillis,
                plan_insights_optimizer_trace: Option<OptimizerTrace>,
                insights_ctx: Option<Box<PlanInsightsContext>>,
            },
            CopyToS3 {
                global_lir_plan: optimize::copy_to::GlobalLirPlan,
                source_ids: BTreeSet<GlobalId>,
            },
            ExplainPlan {
                df_meta: DataflowMetainfo,
                explain_ctx: ExplainPlanContext,
                optimizer: optimize::peek::Optimizer,
                insights_ctx: Option<Box<PlanInsightsContext>>,
            },
            ExplainPushdown {
                imports: BTreeMap<GlobalId, mz_expr::MapFilterProject>,
                determination: TimestampDetermination<Timestamp>,
            },
        }

        let source_ids_for_closure = source_ids.clone();
        let optimization_result = mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                span.in_scope(|| {
                    let _dispatch_guard = explain_ctx.dispatch_guard();

                    let raw_expr = select_plan.source.clone();

                    // The purpose of wrapping the following in a closure is to control where the
                    // `?`s return from, so that even when a `catch_unwind_optimize` call fails,
                    // we can still handle `EXPLAIN BROKEN`.
                    let pipeline = || -> Result<Either<optimize::peek::GlobalLirPlan, optimize::copy_to::GlobalLirPlan>, OptimizerError> {
                        match optimizer.as_mut() {
                            Either::Left(optimizer) => {
                                // SELECT/EXPLAIN path
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr.clone())?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan =
                                    local_mir_plan.resolve(timestamp_context.clone(), &session_meta, stats);
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
                                Ok(Either::Left(global_lir_plan))
                            }
                            Either::Right(optimizer) => {
                                // COPY TO path
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr.clone())?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan =
                                    local_mir_plan.resolve(timestamp_context.clone(), &session_meta, stats);
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
                                Ok(Either::Right(global_lir_plan))
                            }
                        }
                    };

                    let global_lir_plan_result = pipeline();
                    let optimization_finished_at = now();

                    let create_insights_ctx = |optimizer: &optimize::peek::Optimizer, is_notice: bool| -> Option<Box<PlanInsightsContext>> {
                        if !needs_plan_insights {
                            return None;
                        }

                        let catalog = catalog_for_insights.as_ref()?;

                        let enable_re_optimize = if needs_plan_insights {
                            // Disable any plan insights that use the optimizer if we only want the
                            // notice and plan optimization took longer than the threshold. This is
                            // to prevent a situation where optimizing takes a while and there are
                            // lots of clusters, which would delay peek execution by the product of
                            // those.
                            //
                            // (This heuristic doesn't work well, see #9492.)
                            let opt_limit = mz_adapter_types::dyncfgs::PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION
                                .get(catalog.system_config().dyncfgs());
                            !(is_notice && optimizer.duration() > opt_limit)
                        } else {
                            false
                        };

                        Some(Box::new(PlanInsightsContext {
                            stmt: select_plan.select.as_deref().map(Clone::clone).map(Statement::Select),
                            raw_expr: raw_expr.clone(),
                            catalog: Arc::clone(catalog),
                            compute_instances,
                            target_instance: target_cluster_name,
                            metrics: optimizer.metrics().clone(),
                            finishing: optimizer.finishing().clone(),
                            optimizer_config: optimizer.config().clone(),
                            session: session_meta,
                            timestamp_context,
                            view_id: optimizer.select_id(),
                            index_id: optimizer.index_id(),
                            enable_re_optimize,
                        }))
                    };

                    match global_lir_plan_result {
                        Ok(Either::Left(global_lir_plan)) => {
                            // SELECT/EXPLAIN path
                            let optimizer = optimizer.unwrap_left();
                            match explain_ctx {
                                ExplainContext::Plan(explain_ctx) => {
                                    let (_, df_meta, _) = global_lir_plan.unapply();
                                    let insights_ctx = create_insights_ctx(&optimizer, false);
                                    Ok(Execution::ExplainPlan {
                                        df_meta,
                                        explain_ctx,
                                        optimizer,
                                        insights_ctx,
                                    })
                                }
                                ExplainContext::None => {
                                    Ok(Execution::Peek {
                                        global_lir_plan,
                                        optimization_finished_at,
                                        plan_insights_optimizer_trace: None,
                                        insights_ctx: None,
                                    })
                                }
                                ExplainContext::PlanInsightsNotice(optimizer_trace) => {
                                    let insights_ctx = create_insights_ctx(&optimizer, true);
                                    Ok(Execution::Peek {
                                        global_lir_plan,
                                        optimization_finished_at,
                                        plan_insights_optimizer_trace: Some(optimizer_trace),
                                        insights_ctx,
                                    })
                                }
                                ExplainContext::Pushdown => {
                                    let (plan, _, _) = global_lir_plan.unapply();
                                    let imports = match plan {
                                        PeekPlan::SlowPath(plan) => plan
                                            .desc
                                            .source_imports
                                            .into_iter()
                                            .filter_map(|(id, (desc, _, _upper))| {
                                                desc.arguments.operators.map(|mfp| (id, mfp))
                                            })
                                            .collect(),
                                        PeekPlan::FastPath(_) => std::collections::BTreeMap::default(),
                                    };
                                    Ok(Execution::ExplainPushdown {
                                        imports,
                                        determination: determination_for_pushdown.expect("it's present for the ExplainPushdown case"),
                                    })
                                }
                            }
                        }
                        Ok(Either::Right(global_lir_plan)) => {
                            // COPY TO S3 path
                            Ok(Execution::CopyToS3 {
                                global_lir_plan,
                                source_ids: source_ids_for_closure,
                            })
                        }
                        Err(err) => {
                            if optimizer.is_right() {
                                // COPY TO has no EXPLAIN BROKEN support
                                return Err(err);
                            }
                            // SELECT/EXPLAIN error handling
                            let optimizer = optimizer.expect_left("checked above");
                            if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                                if explain_ctx.broken {
                                    // EXPLAIN BROKEN: log error and continue with defaults
                                    tracing::error!("error while handling EXPLAIN statement: {}", err);
                                    Ok(Execution::ExplainPlan {
                                        df_meta: Default::default(),
                                        explain_ctx,
                                        optimizer,
                                        insights_ctx: None,
                                    })
                                } else {
                                    Err(err)
                                }
                            } else {
                                Err(err)
                            }
                        }
                    }
                })
            },
        )
        .await
        .map_err(|optimizer_error| AdapterError::Internal(format!("internal error in optimizer: {}", optimizer_error)))?;

        // Handle the optimization result: either generate EXPLAIN output or continue with execution
        match optimization_result {
            Execution::ExplainPlan {
                df_meta,
                explain_ctx,
                optimizer,
                insights_ctx,
            } => {
                let rows = coord::sequencer::explain_plan_inner(
                    session,
                    &catalog,
                    df_meta,
                    explain_ctx,
                    optimizer,
                    insights_ctx,
                )
                .await?;

                Ok(Some(ExecuteResponse::SendingRowsImmediate {
                    rows: Box::new(rows.into_row_iter()),
                }))
            }
            Execution::ExplainPushdown {
                imports,
                determination,
            } => {
                // # From peek_explain_pushdown

                let as_of = determination.timestamp_context.antichain();
                let mz_now = determination
                    .timestamp_context
                    .timestamp()
                    .map(|t| ResultSpec::value(Datum::MzTimestamp(*t)))
                    .unwrap_or_else(ResultSpec::value_all);

                Ok(Some(
                    coord::sequencer::explain_pushdown_future_inner(
                        session,
                        &*catalog,
                        &self.storage_collections,
                        as_of,
                        mz_now,
                        imports,
                    )
                    .await
                    .await?,
                ))
            }
            Execution::Peek {
                global_lir_plan,
                optimization_finished_at: _optimization_finished_at,
                plan_insights_optimizer_trace,
                insights_ctx,
            } => {
                // Continue with normal execution
                // # From peek_finish

                // TODO(peek-seq): statement logging

                let (peek_plan, df_meta, typ) = global_lir_plan.unapply();

                // Warning: Do not bail out from the new peek sequencing after this point, because the
                // following has side effects. TODO(peek-seq): remove this comment once we never
                // bail out to the old sequencing.

                coord::sequencer::emit_optimizer_notices(
                    &*catalog,
                    session,
                    &df_meta.optimizer_notices,
                );

                // Generate plan insights notice if needed
                if let Some(trace) = plan_insights_optimizer_trace {
                    let target_cluster = catalog.get_cluster(target_cluster_id);
                    let features = OptimizerFeatures::from(catalog.system_config())
                        .override_from(&target_cluster.config.features());
                    let insights = trace
                        .into_plan_insights(
                            &features,
                            &catalog.for_session(session),
                            Some(select_plan.finishing.clone()),
                            Some(target_cluster),
                            df_meta.clone(),
                            insights_ctx,
                        )
                        .await?;
                    session.add_notice(AdapterNotice::PlanInsights(insights));
                }

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

                let response = match peek_plan {
                    PeekPlan::FastPath(fast_path_plan) => {
                        let row_set_finishing_seconds =
                            session.metrics().row_set_finishing_seconds().clone();

                        let peek_stash_read_batch_size_bytes =
                            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_BATCH_SIZE_BYTES
                                .get(catalog.system_config().dyncfgs());
                        let peek_stash_read_memory_budget_bytes =
                            mz_compute_types::dyncfgs::PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES
                                .get(catalog.system_config().dyncfgs());

                        self.implement_fast_path_peek_plan(
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
                            peek_stash_read_batch_size_bytes,
                            peek_stash_read_memory_budget_bytes,
                        )
                        .await?
                    }
                    PeekPlan::SlowPath(dataflow_plan) => {
                        {
                            // Assert that we have some read holds for all the imports of the dataflow.
                            for id in dataflow_plan.desc.source_imports.keys() {
                                soft_assert_or_log!(
                                    read_holds.storage_holds.contains_key(id),
                                    "missing read hold for the source import {}",
                                    id
                                );
                            }
                            for id in dataflow_plan.desc.index_imports.keys() {
                                soft_assert_or_log!(
                                    read_holds
                                        .compute_ids()
                                        .map(|(_instance, coll)| coll)
                                        .contains(id),
                                    "missing read hold for the index import {}",
                                    id,
                                );
                            }

                            // Also check the holds against the as_of.
                            for (id, h) in read_holds.storage_holds.iter() {
                                let as_of = dataflow_plan
                                    .desc
                                    .as_of
                                    .clone()
                                    .expect("dataflow has an as_of")
                                    .into_element();
                                soft_assert_or_log!(
                                    h.since().less_equal(&as_of),
                                    "storage read hold at {:?} for collection {} is not enough for as_of {:?}",
                                    h.since(),
                                    id,
                                    as_of
                                );
                            }
                            for ((instance, id), h) in read_holds.compute_holds.iter() {
                                soft_assert_eq_or_log!(
                                    *instance,
                                    target_cluster_id,
                                    "the read hold on {} is on the wrong cluster",
                                    id
                                );
                                let as_of = dataflow_plan
                                    .desc
                                    .as_of
                                    .clone()
                                    .expect("dataflow has an as_of")
                                    .into_element();
                                soft_assert_or_log!(
                                    h.since().less_equal(&as_of),
                                    "compute read hold at {:?} for collection {} is not enough for as_of {:?}",
                                    h.since(),
                                    id,
                                    as_of
                                );
                            }
                        }

                        self.call_coordinator(|tx| Command::ExecuteSlowPathPeek {
                            dataflow_plan: Box::new(dataflow_plan),
                            determination,
                            finishing: select_plan.finishing,
                            compute_instance: target_cluster_id,
                            target_replica,
                            intermediate_result_type: typ,
                            source_ids,
                            conn_id: session.conn_id().clone(),
                            max_result_size,
                            max_query_result_size,
                            tx,
                        })
                        .await?
                    }
                };

                Ok(Some(match select_plan.copy_to {
                    None => response,
                    // COPY TO STDOUT
                    Some(format) => ExecuteResponse::CopyTo {
                        format,
                        resp: Box::new(response),
                    },
                }))
            }
            Execution::CopyToS3 {
                global_lir_plan,
                source_ids,
            } => {
                let (df_desc, df_meta) = global_lir_plan.unapply();

                coord::sequencer::emit_optimizer_notices(
                    &*catalog,
                    session,
                    &df_meta.optimizer_notices,
                );

                let response = self
                    .call_coordinator(|tx| Command::ExecuteCopyTo {
                        df_desc: Box::new(df_desc),
                        compute_instance: target_cluster_id,
                        target_replica,
                        source_ids,
                        conn_id: session.conn_id().clone(),
                        tx,
                    })
                    .await?;

                Ok(Some(response))
            }
        }
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
            .acquire_read_holds_and_least_valid_write(id_bundle)
            .await
            .map_err(|err| {
                AdapterError::concurrent_dependency_drop_from_collection_lookup_error(
                    err,
                    compute_instance,
                )
            })?;
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
                            compute_instance.to_string().as_ref(),
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
