// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;

use futures::stream::FuturesOrdered;
use futures::TryFutureExt;
use http::Uri;
use itertools::Either;
use maplit::btreemap;
use mz_cluster_client::ReplicaId;
use mz_compute_client::protocol::command::PeekTarget;
use mz_compute_client::protocol::response::PeekResponse;
use mz_compute_types::ComputeInstanceId;
use mz_controller_types::ClusterId;
use mz_expr::{CollectionPlan, EvalError, ResultSpec, RowSetFinishing};
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{instrument, task};
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{Datum, GlobalId, Row, RowArena, Timestamp};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::{CatalogCluster, SessionCatalog};
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_catalog::memory::objects::CatalogItem;
use mz_persist_client::stats::{SnapshotPartStats, SnapshotPartsStats};
use mz_sql::plan::QueryWhen;
use mz_sql::plan::{self, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::stats::RelationPartStats;
use mz_transform::EmptyStatisticsOracle;
use tracing::Instrument;
use tracing::{event, warn, Level};
use uuid::Uuid;

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyTo};
use crate::catalog::Catalog;
use crate::command::ExecuteResponse;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::{
    self, FastPathPlan, PeekComputeInstances, PeekDataflowPlan, PeekIds, PeekPlan, PeekSubmitPeek,
    PeekTimestampOracle, PeekTimestamps, PlannedPeek,
};
use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::timeline::TimelineContext;
use crate::coord::timestamp_selection::{
    TimestampContext, TimestampDetermination, TimestampProvider,
};
use crate::coord::{
    Coordinator, CopyToContext, ExecuteContext, ExplainContext, ExplainPlanContext, Message,
    PeekStage, PeekStageCopyTo, PeekStageExplainPlan, PeekStageExplainPushdown, PeekStageFinish,
    PeekStageLinearizeTimestamp, PeekStageOptimize, PeekStageRealTimeRecency,
    PeekStageTimestampReadHold, PeekStageValidate, PlanValidity, RealTimeRecencyContext,
    TargetCluster,
};
use crate::error::AdapterError;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::notice::AdapterNotice;
use crate::optimize::dataflows::{prep_scalar_expr, DataflowBuilder, EvalTime, ExprPrepStyle};
use crate::optimize::metrics::OptimizerMetrics;
use crate::optimize::{self, Optimize};
use crate::session::{
    RequireLinearization, Session, SessionMeta, TransactionOps, TransactionStatus,
};
use crate::statement_logging::{
    StatementEndedExecutionReason, StatementExecutionStrategy, StatementLifecycleEvent,
};
use crate::util::ResultExt;
use crate::PeekResponseUnary;

// /// Processes as many `peek` stages as possible.
// #[instrument]
// pub(crate) async fn execute_peek_stage(
//     &mut self,
//     mut ctx: ExecuteContext,
//     root_otel_ctx: OpenTelemetryContext,
//     mut stage: PeekStage,
// ) {
//     use PeekStage::*;
//
//     // Process the current stage and allow for processing the next.
//     loop {
//         event!(Level::TRACE, stage = format!("{:?}", stage));
//
//         // Always verify peek validity. This is cheap, and prevents programming errors
//         // if we move any stages off thread.
//         if let Some(validity) = stage.validity() {
//             if let Err(err) = validity.check(self.catalog()) {
//                 ctx.retire(Err(err));
//                 return;
//             }
//         }
//
//         (ctx, stage) = match stage {
//             Validate(stage) => {
//                 let owned_catalog = self.owned_catalog();
//                 let next = return_if_err!(
//                     Self::peek_stage_validate(&mut *self, owned_catalog, ctx.session_mut(), stage),
//                     ctx
//                 );
//
//                 (ctx, PeekStage::LinearizeTimestamp(next))
//             }
//             LinearizeTimestamp(stage) => {
//                 self.peek_stage_linearize_timestamp(ctx, root_otel_ctx.clone(), stage)
//                     .await;
//                 return;
//             }
//             RealTimeRecency(stage) => {
//                 match self.peek_stage_real_time_recency(ctx, root_otel_ctx.clone(), stage) {
//                     Some((ctx, next)) => (ctx, PeekStage::TimestampReadHold(next)),
//                     None => return,
//                 }
//             }
//             TimestampReadHold(stage) => {
//                 let next = return_if_err!(
//                     self.peek_stage_timestamp_read_hold(ctx.session_mut(), stage)
//                         .await,
//                     ctx
//                 );
//                 (ctx, PeekStage::Optimize(next))
//             }
//             Optimize(stage) => {
//                 self.peek_stage_optimize(ctx, root_otel_ctx.clone(), stage)
//                     .await;
//                 return;
//             }
//             Finish(stage) => {
//                 let res = self.peek_stage_finish(&mut ctx, stage).await;
//                 ctx.retire(res);
//                 return;
//             }
//             CopyTo(stage) => {
//                 self.peek_stage_copy_to_dataflow(ctx, stage).await;
//                 return;
//             }
//             ExplainPlan(stage) => {
//                 let result = self.peek_stage_explain_plan(&mut ctx, stage);
//                 ctx.retire(result);
//                 return;
//             }
//             ExplainPushdown(stage) => {
//                 let result = self.peek_stage_explain_pushdown(&mut ctx, stage).await;
//                 ctx.retire(result);
//                 return;
//             }
//         }
//     }
// }

/// Do some simple validation. We must defer most of it until after any off-thread work.
#[instrument]
pub fn peek_stage_validate<C: PeekIds + PeekTimestamps + PeekComputeInstances>(
    mut client: C,
    session: &Session,
    catalog: &Arc<Catalog>,
    optimizer_metrics: OptimizerMetrics,
    PeekStageValidate {
        plan,
        target_cluster,
        copy_to_ctx,
        explain_ctx,
    }: PeekStageValidate,
) -> Result<PeekStageLinearizeTimestamp, AdapterError> {
    // Collect optimizer parameters.
    let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
    let compute_instance = client
        .instance_snapshot(cluster.id())
        .expect("compute instance does not exist");
    let view_id = client.allocate_transient_id()?;
    let optimizer_config = optimize::OptimizerConfig::from(catalog.system_config())
        .override_from(
            catalog
                .get_cluster(cluster.id())
                .config
                .features()
                .expect("heyoo"),
        )
        .override_from(&explain_ctx);

    if cluster.replicas().next().is_none() {
        return Err(AdapterError::NoClusterReplicasAvailable(
            cluster.name.clone(),
        ));
    }

    let optimizer = match copy_to_ctx {
        None => {
            // Collect optimizer parameters specific to the peek::Optimizer.
            let compute_instance = client
                .instance_snapshot(cluster.id())
                .expect("compute instance does not exist");
            let view_id = client.allocate_transient_id()?;
            let index_id = client.allocate_transient_id()?;

            // Build an optimizer for this SELECT.
            Either::Left(optimize::peek::Optimizer::new(
                Arc::clone(&catalog),
                compute_instance,
                plan.finishing.clone(),
                view_id,
                index_id,
                optimizer_config,
                optimizer_metrics.clone(),
            ))
        }
        Some(mut copy_to_ctx) => {
            // Getting the max worker count across replicas
            // and using that value for the number of batches to
            // divide the copy output into.
            let max_worker_count = match cluster
                .replicas()
                .map(|r| r.config.location.workers())
                .max()
            {
                Some(count) => u64::cast_from(count),
                None => {
                    return Err(AdapterError::NoClusterReplicasAvailable(
                        cluster.name.clone(),
                    ))
                }
            };
            copy_to_ctx.output_batch_count = Some(max_worker_count);
            // Build an optimizer for this COPY TO.
            Either::Right(optimize::copy_to::Optimizer::new(
                Arc::clone(&catalog),
                compute_instance,
                view_id,
                copy_to_ctx,
                optimizer_config,
                optimizer_metrics.clone(),
            ))
        }
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

    let source_ids = plan.source.depends_on();
    let mut timeline_context =
        Coordinator::validate_timeline_context(catalog.as_ref(), source_ids.clone())?;
    if matches!(timeline_context, TimelineContext::TimestampIndependent)
        && plan.source.contains_temporal()?
    {
        // If the source IDs are timestamp independent but the query contains temporal functions,
        // then the timeline context needs to be upgraded to timestamp dependent. This is
        // required because `source_ids` doesn't contain functions.
        timeline_context = TimelineContext::TimestampDependent;
    }

    let notices = check_log_reads(
        &catalog,
        cluster,
        &source_ids,
        &mut target_replica,
        session.vars(),
    )?;
    session.add_notices(notices);

    let validity = PlanValidity {
        transient_revision: catalog.transient_revision(),
        dependency_ids: source_ids.clone(),
        cluster_id: Some(cluster.id()),
        replica_id: target_replica,
        role_metadata: session.role_metadata().clone(),
    };

    Ok(PeekStageLinearizeTimestamp {
        validity,
        plan,
        source_ids,
        target_replica,
        timeline_context,
        optimizer,
        explain_ctx,
    })
}

/// Possibly linearize a timestamp from a `TimestampOracle`.
#[instrument]
pub async fn peek_stage_linearize_timestamp<C: PeekTimestampOracle>(
    mut client: C,
    session: &Session,
    PeekStageLinearizeTimestamp {
        validity,
        source_ids,
        plan,
        target_replica,
        timeline_context,
        optimizer,
        explain_ctx,
    }: PeekStageLinearizeTimestamp,
) -> PeekStageRealTimeRecency {
    let isolation_level = session.vars().transaction_isolation().clone();
    let timeline = Coordinator::get_timeline(&timeline_context);
    let needs_linearized_read_ts =
        Coordinator::needs_linearized_read_ts(&isolation_level, &plan.when);

    let oracle_read_ts = match timeline {
        Some(timeline) if needs_linearized_read_ts => {
            let oracle = client.get_timestamp_oracle(&timeline).await;

            // We ship the timestamp oracle off to an async task, so that we
            // don't block the main task while we wait.

            let oracle_read_ts = oracle.read_ts().await;

            Some(oracle_read_ts)
        }
        Some(_) | None => None,
    };

    let next_stage = PeekStageRealTimeRecency {
        validity,
        plan,
        source_ids,
        target_replica,
        timeline_context,
        oracle_read_ts,
        optimizer,
        explain_ctx,
    };

    next_stage
}

#[instrument]
pub fn peek_stage_real_time_recency<C>(
    mut client: C,
    PeekStageRealTimeRecency {
        validity,
        plan,
        source_ids,
        target_replica,
        timeline_context,
        oracle_read_ts,
        optimizer,
        explain_ctx,
    }: PeekStageRealTimeRecency,
) -> PeekStageTimestampReadHold {
    PeekStageTimestampReadHold {
        validity,
        plan,
        target_replica,
        timeline_context,
        source_ids,
        optimizer,
        explain_ctx,
        oracle_read_ts,
        real_time_recency_ts: None,
    }
}

/// Determine a read timestamp and create appropriate read holds.
#[instrument]
pub async fn peek_stage_timestamp_read_hold<C: PeekTimestamps + PeekComputeInstances + Copy>(
    client: C,
    session: &mut Session,
    catalog: &Arc<Catalog>,
    PeekStageTimestampReadHold {
        mut validity,
        plan,
        source_ids,
        target_replica,
        timeline_context,
        oracle_read_ts,
        real_time_recency_ts,
        optimizer,
        explain_ctx,
    }: PeekStageTimestampReadHold,
) -> Result<PeekStageOptimize, AdapterError> {
    let cluster_id = match optimizer.as_ref() {
        Either::Left(optimizer) => optimizer.cluster_id(),
        Either::Right(optimizer) => optimizer.cluster_id(),
    };

    let compute = client
        .instance_snapshot(cluster_id)
        .expect("compute instance does not exist");
    let dataflow_builder = DataflowBuilder::new(catalog.state(), compute);

    let id_bundle = dataflow_builder.sufficient_collections(&source_ids);

    // Although we have added `sources.depends_on()` to the validity already, also add the
    // sufficient collections for safety.
    validity.dependency_ids.extend(id_bundle.iter());

    let determination = sequence_peek_timestamp(
        client,
        catalog,
        session,
        &plan.when,
        cluster_id,
        timeline_context,
        oracle_read_ts,
        &id_bundle,
        &source_ids,
        real_time_recency_ts,
        (&explain_ctx).into(),
    )
    .await?;

    Ok(PeekStageOptimize {
        validity,
        plan,
        source_ids,
        id_bundle,
        target_replica,
        determination,
        optimizer,
        explain_ctx,
    })
}

/// Determines the query timestamp and acquires read holds on dependent sources
/// if necessary.
#[instrument]
pub(super) async fn sequence_peek_timestamp<C: PeekTimestamps + PeekComputeInstances + Copy>(
    client: C,
    catalog: &Arc<Catalog>,
    session: &mut Session,
    when: &QueryWhen,
    cluster_id: ClusterId,
    timeline_context: TimelineContext,
    oracle_read_ts: Option<Timestamp>,
    source_bundle: &CollectionIdBundle,
    source_ids: &BTreeSet<GlobalId>,
    real_time_recency_ts: Option<Timestamp>,
    requires_linearization: RequireLinearization,
) -> Result<TimestampDetermination<Timestamp>, AdapterError> {
    let in_immediate_multi_stmt_txn = session.transaction().in_immediate_multi_stmt_txn(when);
    let timedomain_bundle;

    // Fetch or generate a timestamp for this query and what the read holds would be if we need to set
    // them.
    let determination = match session.get_transaction_timestamp_determination() {
        // Use the transaction's timestamp if it exists and this isn't an AS OF query.
        Some(
            determination @ TimestampDetermination {
                timestamp_context: TimestampContext::TimelineTimestamp { .. },
                ..
            },
        ) if in_immediate_multi_stmt_txn => determination,
        _ => {
            let determine_bundle = if in_immediate_multi_stmt_txn {
                // In a transaction, determine a timestamp that will be valid for anything in
                // any schema referenced by the first query.
                timedomain_bundle = Coordinator::timedomain_for(
                    client,
                    catalog.as_ref(),
                    source_ids,
                    &timeline_context,
                    session.conn_id(),
                    cluster_id,
                )?;

                &timedomain_bundle
            } else {
                // If not in a transaction, use the source.
                source_bundle
            };
            let isolation_level = session.vars().transaction_isolation();
            let determination = client
                .determine_timestamp(
                    catalog.state(),
                    session,
                    determine_bundle,
                    when,
                    cluster_id,
                    &timeline_context,
                    oracle_read_ts,
                    real_time_recency_ts,
                    isolation_level,
                )
                .await?;
            determination
        }
    };

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
    if when.is_transactional() {
        session.add_transaction_ops(TransactionOps::Peeks {
            determination: transaction_determination,
            cluster_id,
            requires_linearization,
        })?;
    } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
        // If the query uses AS OF, then ignore the timestamp.
        transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
        session.add_transaction_ops(TransactionOps::Peeks {
            determination: transaction_determination,
            cluster_id,
            requires_linearization,
        })?;
    };

    Ok(determination)
}

#[instrument]
pub async fn peek_stage_optimize<C>(
    _client: C,
    session: &mut Session,
    PeekStageOptimize {
        validity,
        plan,
        source_ids,
        id_bundle,
        target_replica,
        determination,
        mut optimizer,
        explain_ctx,
    }: PeekStageOptimize,
) -> Result<PeekStage, AdapterError> {
    let timestamp_context = determination.timestamp_context.clone();
    let stats = Box::new(EmptyStatisticsOracle);

    let session_meta = session.meta();

    let pipeline = || -> Result<Either<optimize::peek::GlobalLirPlan, optimize::copy_to::GlobalLirPlan>, AdapterError> {
                    let _dispatch_guard = explain_ctx.dispatch_guard();

                    let raw_expr = plan.source.clone();

                    match optimizer.as_mut() {
                        // Optimize SELECT statement.
                        Either::Left(optimizer) => {

                            if let Some(global_lir_plan) = session.peek_optimizer_cache.get(&raw_expr) {
                                Ok(Either::Left(global_lir_plan.clone()))
                            } else {
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr.clone())?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan = local_mir_plan.resolve(timestamp_context, &session_meta, stats);
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                                session.peek_optimizer_cache.insert(raw_expr, global_lir_plan.clone());

                                Ok(Either::Left(global_lir_plan))
                            }

                        }
                        // Optimize COPY TO statement.
                        Either::Right(optimizer) => {
                            // HIR ⇒ MIR lowering and MIR optimization (local and global)
                            let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                            // Attach resolved context required to continue the pipeline.
                            let local_mir_plan = local_mir_plan.resolve(timestamp_context, &session_meta, stats);
                            // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                            let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                            Ok(Either::Right(global_lir_plan))
                        }
                    }
                };

    let stage = match pipeline() {
        Ok(Either::Left(global_lir_plan)) => {
            let optimizer = optimizer.unwrap_left();
            match explain_ctx {
                ExplainContext::Plan(explain_ctx) => {
                    let (_, df_meta, _) = global_lir_plan.unapply();
                    PeekStage::ExplainPlan(PeekStageExplainPlan {
                        validity,
                        optimizer,
                        df_meta,
                        explain_ctx,
                    })
                }
                ExplainContext::PlanInsightsNotice(optimizer_trace) => {
                    PeekStage::Finish(PeekStageFinish {
                        validity,
                        plan,
                        id_bundle,
                        target_replica,
                        source_ids,
                        determination,
                        optimizer,
                        plan_insights_optimizer_trace: Some(optimizer_trace),
                        global_lir_plan,
                    })
                }
                ExplainContext::None => PeekStage::Finish(PeekStageFinish {
                    validity,
                    plan,
                    id_bundle,
                    target_replica,
                    source_ids,
                    determination,
                    optimizer,
                    plan_insights_optimizer_trace: None,
                    global_lir_plan,
                }),
                ExplainContext::Pushdown => {
                    let (plan, _, _) = global_lir_plan.unapply();
                    let imports = match plan {
                        PeekPlan::SlowPath(plan) => plan
                            .desc
                            .source_imports
                            .into_iter()
                            .filter_map(|(id, (desc, _))| {
                                desc.arguments.operators.map(|mfp| (id, mfp))
                            })
                            .collect(),
                        PeekPlan::FastPath(_) => BTreeMap::default(),
                    };
                    PeekStage::ExplainPushdown(PeekStageExplainPushdown {
                        validity,
                        determination,
                        imports,
                    })
                }
            }
        }
        Ok(Either::Right(global_lir_plan)) => {
            let optimizer = optimizer.unwrap_right();
            PeekStage::CopyTo(PeekStageCopyTo {
                validity,
                optimizer,
                global_lir_plan,
            })
        }
        // Internal optimizer errors are handled differently
        // depending on the caller.
        Err(err) => {
            let Some(optimizer) = optimizer.left() else {
                // In `COPY TO` contexts, immediately retire the
                // execution with the error.
                return Err(err.into());
            };
            let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                // In `sequence_~` contexts, immediately retire the
                // execution with the error.
                return Err(err.into());
            };

            if explain_ctx.broken {
                // In `EXPLAIN BROKEN` contexts, just log the error
                // and move to the next stage with default
                // parameters.
                tracing::error!("error while handling EXPLAIN statement: {}", err);
                PeekStage::ExplainPlan(PeekStageExplainPlan {
                    validity,
                    optimizer,
                    df_meta: Default::default(),
                    explain_ctx,
                })
            } else {
                // In regular `EXPLAIN` contexts, immediately retire
                // the execution with the error.
                return Err(err.into());
            }
        }
    };

    Ok(stage)
}

#[instrument]
pub async fn peek_stage_finish<C: PeekSubmitPeek<mz_repr::Timestamp>>(
    client: C,
    session: &mut Session,
    catalog: &Arc<Catalog>,
    PeekStageFinish {
        validity: _,
        plan,
        id_bundle,
        target_replica,
        source_ids,
        determination,
        optimizer,
        plan_insights_optimizer_trace,
        global_lir_plan,
    }: PeekStageFinish,
) -> Result<ExecuteResponse, AdapterError> {
    let conn_id = session.conn_id().clone();

    let (peek_plan, df_meta, typ) = global_lir_plan.unapply();
    let source_arity = typ.arity();

    // self.emit_optimizer_notices(&*session, &df_meta.optimizer_notices);

    let target_cluster = catalog.get_cluster(optimizer.cluster_id());

    let features = OptimizerFeatures::from(catalog.system_config())
        .override_from(&target_cluster.config.features());

    if let Some(trace) = plan_insights_optimizer_trace {
        let insights = trace.into_plan_insights(
            &features,
            &catalog.for_session(session),
            Some(plan.finishing),
            Some(target_cluster.name.as_str()),
            df_meta,
        )?;
        session.add_notice(AdapterNotice::PlanInsights(insights));
    }

    let planned_peek = PlannedPeek {
        plan: peek_plan,
        determination: determination.clone(),
        conn_id,
        source_arity,
        source_ids,
    };

    if let Some(transient_index_id) = match &planned_peek.plan {
        peek::PeekPlan::FastPath(_) => None,
        peek::PeekPlan::SlowPath(PeekDataflowPlan { id, .. }) => Some(id),
    } {
        // if let Some(statement_logging_id) = ctx.extra.contents() {
        // self.set_transient_index_id(statement_logging_id, *transient_index_id);
        // }
    }

    // if let Some(uuid) = ctx.extra().contents() {
    //     let ts = determination.timestamp_context.timestamp_or_default();
    //     let mut transitive_storage_deps = BTreeSet::new();
    //     let mut transitive_compute_deps = BTreeSet::new();
    //     for id in id_bundle
    //         .iter()
    //         .flat_map(|id| catalog.state().transitive_uses(id))
    //     {
    //         match catalog.state().get_entry(&id).item() {
    //             CatalogItem::Table(_) | CatalogItem::Source(_) => {
    //                 transitive_storage_deps.insert(id);
    //             }
    //             CatalogItem::MaterializedView(_) | CatalogItem::Index(_) => {
    //                 transitive_compute_deps.insert(id);
    //             }
    //             _ => {}
    //         }
    //     }
    // self.controller.install_storage_watch_set(
    //     transitive_storage_deps,
    //     ts,
    //     Box::new((uuid, StatementLifecycleEvent::StorageDependenciesFinished)),
    // );
    // self.controller.install_compute_watch_set(
    //     transitive_compute_deps,
    //     ts,
    //     Box::new((uuid, StatementLifecycleEvent::ComputeDependenciesFinished)),
    // )
    // }
    let max_query_result_size = std::cmp::min(
        session.vars().max_query_result_size(),
        catalog.system_config().max_result_size(),
    );

    // Implement the peek, and capture the response.
    let resp = implement_peek_plan(
        client,
        // ctx.extra_mut(),
        planned_peek,
        optimizer.finishing().clone(),
        optimizer.cluster_id(),
        target_replica,
        max_query_result_size,
    )
    .await?;

    // if ctx.session().vars().emit_timestamp_notice() {
    //     let explanation = self.explain_timestamp(
    //         ctx.session(),
    //         optimizer.cluster_id(),
    //         &id_bundle,
    //         determination,
    //     );
    //     ctx.session()
    //         .add_notice(AdapterNotice::QueryTimestamp { explanation });
    // }
    //
    match plan.copy_to {
        None => Ok(resp),
        Some(format) => Ok(ExecuteResponse::CopyTo {
            format,
            resp: Box::new(resp),
        }),
    }
}

/// Implements a peek plan produced by `create_plan` above.
#[mz_ore::instrument(level = "debug")]
pub async fn implement_peek_plan<C: PeekSubmitPeek<mz_repr::Timestamp>>(
    mut client: C,
    // ctx_extra: &mut ExecuteContextExtra,
    plan: PlannedPeek,
    finishing: RowSetFinishing,
    compute_instance: ComputeInstanceId,
    target_replica: Option<ReplicaId>,
    max_result_size: u64,
) -> Result<crate::ExecuteResponse, AdapterError> {
    let PlannedPeek {
        plan: fast_path,
        determination,
        conn_id,
        source_arity,
        source_ids,
    } = plan;

    // If the dataflow optimizes to a constant expression, we can immediately return the result.
    if let PeekPlan::FastPath(FastPathPlan::Constant(rows, _)) = fast_path {
        let mut rows = match rows {
            Ok(rows) => rows,
            Err(e) => return Err(e.into()),
        };
        // Consolidate down the results to get correct totals.
        differential_dataflow::consolidation::consolidate(&mut rows);

        let mut results = Vec::new();
        for (row, count) in rows {
            if count < 0 {
                Err(EvalError::InvalidParameterValue(format!(
                    "Negative multiplicity in constant result: {}",
                    count
                )))?
            };
            if count > 0 {
                let count = usize::cast_from(
                    u64::try_from(count).expect("known to be positive from check above"),
                );
                results.push((
                    row,
                    NonZeroUsize::new(count).expect("known to be non-zero from check above"),
                ));
            }
        }
        let results = finishing.finish(results, max_result_size);
        let (ret, reason) = match results {
            Ok(rows) => {
                let rows_returned = u64::cast_from(rows.len());
                (
                    Ok(Coordinator::send_immediate_rows(rows)),
                    StatementEndedExecutionReason::Success {
                        rows_returned: Some(rows_returned),
                        execution_strategy: Some(StatementExecutionStrategy::Constant),
                    },
                )
            }
            Err(error) => (
                Err(AdapterError::ResultSize(error.clone())),
                StatementEndedExecutionReason::Errored { error },
            ),
        };
        // self.retire_execution(reason, std::mem::take(ctx_extra));
        return ret;
    }

    let timestamp = determination.timestamp_context.timestamp_or_default();
    // if let Some(id) = ctx_extra.contents() {
    //     self.set_statement_execution_timestamp(id, timestamp)
    // }

    // The remaining cases are a peek into a maintained arrangement, or building a dataflow.
    // In both cases we will want to peek, and the main difference is that we might want to
    // build a dataflow and drop it once the peek is issued. The peeks are also constructed
    // differently.

    // If we must build the view, ship the dataflow.
    let (peek_command, drop_dataflow, is_fast_path, peek_target): (_, Option<GlobalId>, _, _) =
        match fast_path {
            PeekPlan::FastPath(FastPathPlan::PeekExisting(
                _coll_id,
                idx_id,
                literal_constraints,
                map_filter_project,
            )) => (
                (idx_id, literal_constraints, timestamp, map_filter_project),
                None,
                true,
                PeekTarget::Index { id: idx_id },
            ),
            PeekPlan::FastPath(FastPathPlan::PeekPersist(coll_id, map_filter_project)) => {
                todo!("persist fast path");
                // let peek_command = (coll_id, None, timestamp, map_filter_project);
                // let metadata = self
                //     .controller
                //     .storage
                //     .collection_metadata(coll_id)
                //     .expect("storage collection for fast-path peek")
                //     .clone();
                // (
                //     peek_command,
                //     None,
                //     true,
                //     PeekTarget::Persist {
                //         id: coll_id,
                //         metadata,
                //     },
                // )
            }
            PeekPlan::SlowPath(PeekDataflowPlan {
                desc: dataflow,
                // n.b. this index_id identifies a transient index the
                // caller created, so it is guaranteed to be on
                // `compute_instance`.
                id: index_id,
                key: index_key,
                permutation: index_permutation,
                thinned_arity: index_thinned_arity,
            }) => {
                todo!("slow path");
                // let output_ids = dataflow.export_ids().collect();
                //
                // // Very important: actually create the dataflow (here, so we can destructure).
                // self.controller
                //     .active_compute()
                //     .create_dataflow(compute_instance, dataflow)
                //     .unwrap_or_terminate("cannot fail to create dataflows");
                // self.initialize_compute_read_policies(
                //     output_ids,
                //     compute_instance,
                //     // Disable compaction so that nothing can compact before the peek occurs below.
                //     CompactionWindow::DisableCompaction,
                // )
                // .await;
                //
                // // Create an identity MFP operator.
                // let mut map_filter_project = mz_expr::MapFilterProject::new(source_arity);
                // map_filter_project.permute(index_permutation, index_key.len() + index_thinned_arity);
                // let map_filter_project = mfp_to_safe_plan(map_filter_project)?;
                // (
                //     (
                //         index_id, // transient identifier produced by `dataflow_plan`.
                //         None,
                //         timestamp,
                //         map_filter_project,
                //     ),
                //     Some(index_id),
                //     false,
                //     PeekTarget::Index { id: index_id },
                // )
            }
            _ => {
                unreachable!()
            }
        };

    // Endpoints for sending and receiving peek responses.
    let (rows_tx, rows_rx) = tokio::sync::oneshot::channel();

    // Generate unique UUID. Guaranteed to be unique to all pending peeks, there's an very
    // small but unlikely chance that it's not unique to completed peeks.
    let mut uuid = Uuid::new_v4();
    // while self.pending_peeks.contains_key(&uuid) {
    //     uuid = Uuid::new_v4();
    // }

    // The peek is ready to go for both cases, fast and non-fast.
    // Stash the response mechanism, and broadcast dataflow construction.
    // self.pending_peeks.insert(
    //     uuid,
    //     PendingPeek {
    //         conn_id: conn_id.clone(),
    //         cluster_id: compute_instance,
    //         depends_on: source_ids,
    //         ctx_extra: std::mem::take(ctx_extra),
    //         is_fast_path,
    //     },
    // );
    // self.client_pending_peeks
    //     .entry(conn_id)
    //     .or_default()
    //     .insert(uuid, compute_instance);
    // let (id, literal_constraints, timestamp, map_filter_project) = peek_command;

    // self.controller
    //     .active_compute()
    //     .peek(
    //         compute_instance,
    //         id,
    //         literal_constraints,
    //         uuid,
    //         timestamp,
    //         finishing.clone(),
    //         map_filter_project,
    //         target_replica,
    //         peek_target,
    //         rows_tx,
    //     )
    //     .unwrap_or_terminate("cannot fail to peek");

    let (id, literal_constraints, timestamp, map_filter_project) = peek_command;
    client
        .peek(
            compute_instance,
            id,
            literal_constraints,
            uuid,
            timestamp,
            finishing.clone(),
            map_filter_project,
            target_replica,
            peek_target,
            rows_tx,
        )
        .await
        .unwrap_or_terminate("cannot fail to peek");

    // Prepare the receiver to return as a response.
    let rows_rx = rows_rx.map_ok_or_else(
        |e| PeekResponseUnary::Error(e.to_string()),
        move |resp| match resp {
            PeekResponse::Rows(rows) => match finishing.finish(rows, max_result_size) {
                Ok(rows) => PeekResponseUnary::Rows(rows),
                Err(e) => PeekResponseUnary::Error(e),
            },
            PeekResponse::Canceled => PeekResponseUnary::Canceled,
            PeekResponse::Error(e) => PeekResponseUnary::Error(e),
        },
    );

    // If it was created, drop the dataflow once the peek command is sent.
    // if let Some(index_id) = drop_dataflow {
    //     self.remove_compute_ids_from_timeline(vec![(compute_instance, index_id)]);
    //     self.drop_indexes(vec![(compute_instance, index_id)]);
    // }

    Ok(crate::ExecuteResponse::SendingRows {
        future: Box::pin(rows_rx),
    })
}

// #[instrument]
// async fn peek_stage_copy_to_dataflow(
//     &mut self,
//     ctx: ExecuteContext,
//     PeekStageCopyTo {
//         validity,
//         optimizer,
//         global_lir_plan,
//     }: PeekStageCopyTo,
// ) {
//     let sink_id = global_lir_plan.sink_id();
//     let cluster_id = optimizer.cluster_id();
//
//     let (df_desc, df_meta) = global_lir_plan.unapply();
//
//     self.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);
//
//     // Callback for the active copy to.
//     let active_copy_to = ActiveCopyTo {
//         ctx,
//         cluster_id,
//         depends_on: validity.dependency_ids.clone(),
//     };
//     // Add metadata for the new COPY TO.
//     drop(
//         self.add_active_compute_sink(sink_id, ActiveComputeSink::CopyTo(active_copy_to))
//             .await,
//     );
//
//     // Ship dataflow.
//     self.ship_dataflow(df_desc, cluster_id).await;
// }
//
// #[instrument]
// fn peek_stage_explain_plan(
//     &mut self,
//     ctx: &mut ExecuteContext,
//     PeekStageExplainPlan {
//         optimizer,
//         df_meta,
//         explain_ctx:
//             ExplainPlanContext {
//                 config,
//                 format,
//                 stage,
//                 desc,
//                 optimizer_trace,
//                 ..
//             },
//         ..
//     }: PeekStageExplainPlan,
// ) -> Result<ExecuteResponse, AdapterError> {
//     let desc = desc.expect("RelationDesc for SelectPlan in EXPLAIN mode");
//
//     let session_catalog = self.catalog().for_session(ctx.session());
//     let expr_humanizer = {
//         let transient_items = btreemap! {
//             optimizer.select_id() => TransientItem::new(
//                 Some(vec![GlobalId::Explain.to_string()]),
//                 Some(desc.iter_names().map(|c| c.to_string()).collect()),
//             )
//         };
//         ExprHumanizerExt::new(transient_items, &session_catalog)
//     };
//
//     let finishing = if optimizer.finishing().is_trivial(desc.arity()) {
//         None
//     } else {
//         Some(optimizer.finishing().clone())
//     };
//
//     let target_cluster = self.catalog().get_cluster(optimizer.cluster_id());
//     let features = optimizer.config().features.clone();
//
//     let rows = optimizer_trace.into_rows(
//         format,
//         &config,
//         &features,
//         &expr_humanizer,
//         finishing,
//         Some(target_cluster.name.as_str()),
//         df_meta,
//         stage,
//         plan::ExplaineeStatementKind::Select,
//     )?;
//
//     Ok(Self::send_immediate_rows(rows))
// }
//
// #[instrument]
// async fn peek_stage_explain_pushdown(
//     &mut self,
//     ctx: &mut ExecuteContext,
//     stage: PeekStageExplainPushdown,
// ) -> Result<ExecuteResponse, AdapterError> {
//     use futures::stream::TryStreamExt;
//
//     let as_of = stage.determination.timestamp_context.antichain();
//     let mz_now = stage
//         .determination
//         .timestamp_context
//         .timestamp()
//         .map(|t| ResultSpec::value(Datum::MzTimestamp(*t)))
//         .unwrap_or_else(ResultSpec::value_all);
//
//     let futures: FuturesOrdered<_> = stage
//         .imports
//         .into_iter()
//         .map(|(gid, mfp)| {
//             let as_of = &as_of;
//             let mz_now = &mz_now;
//             let this = &self;
//             let ctx = &*ctx;
//             async move {
//                 let catalog_entry = this.catalog.get_entry(&gid);
//                 let full_name = this
//                     .catalog
//                     .for_session(&ctx.session)
//                     .resolve_full_name(&catalog_entry.name);
//                 let name = format!("{}", full_name);
//                 let relation_desc = catalog_entry
//                     .item
//                     .desc_opt()
//                     .expect("source should have a proper desc");
//                 let snapshot_stats: SnapshotPartsStats = this
//                     .controller
//                     .storage
//                     .snapshot_parts_stats(gid, as_of.clone())
//                     .await?;
//
//                 let mut total_bytes = 0;
//                 let mut total_parts = 0;
//                 let mut selected_bytes = 0;
//                 let mut selected_parts = 0;
//                 for SnapshotPartStats {
//                     encoded_size_bytes: bytes,
//                     stats,
//                 } in &snapshot_stats.parts
//                 {
//                     let bytes = u64::cast_from(*bytes);
//                     total_bytes += bytes;
//                     total_parts += 1u64;
//                     let selected = match stats {
//                         None => true,
//                         Some(stats) => {
//                             let stats = stats.decode();
//                             let stats = RelationPartStats::new(
//                                 name.as_str(),
//                                 &snapshot_stats.metrics.pushdown.part_stats,
//                                 &relation_desc,
//                                 &stats,
//                             );
//                             stats.may_match_mfp(mz_now.clone(), &mfp)
//                         }
//                     };
//
//                     if selected {
//                         selected_bytes += bytes;
//                         selected_parts += 1u64;
//                     }
//                 }
//                 Ok::<_, AdapterError>(Row::pack_slice(&[
//                     name.as_str().into(),
//                     total_bytes.into(),
//                     selected_bytes.into(),
//                     total_parts.into(),
//                     selected_parts.into(),
//                 ]))
//             }
//         })
//         .collect();
//
//     let rows = futures.try_collect().await?;
//
//     Ok(ExecuteResponse::SendingRowsImmediate { rows })
// }
//
// /// Determines the query timestamp and acquires read holds on dependent sources
// /// if necessary.
// #[instrument]
// pub(super) async fn sequence_peek_timestamp(
//     &mut self,
//     session: &mut Session,
//     when: &QueryWhen,
//     cluster_id: ClusterId,
//     timeline_context: TimelineContext,
//     oracle_read_ts: Option<Timestamp>,
//     source_bundle: &CollectionIdBundle,
//     source_ids: &BTreeSet<GlobalId>,
//     real_time_recency_ts: Option<Timestamp>,
//     requires_linearization: RequireLinearization,
// ) -> Result<TimestampDetermination<Timestamp>, AdapterError> {
//     let in_immediate_multi_stmt_txn = session.transaction().in_immediate_multi_stmt_txn(when);
//     let timedomain_bundle;
//
//     // Fetch or generate a timestamp for this query and what the read holds would be if we need to set
//     // them.
//     let (determination, read_holds) = match session.get_transaction_timestamp_determination() {
//         // Use the transaction's timestamp if it exists and this isn't an AS OF query.
//         Some(
//             determination @ TimestampDetermination {
//                 timestamp_context: TimestampContext::TimelineTimestamp { .. },
//                 ..
//             },
//         ) if in_immediate_multi_stmt_txn => (determination, None),
//         _ => {
//             let determine_bundle = if in_immediate_multi_stmt_txn {
//                 // In a transaction, determine a timestamp that will be valid for anything in
//                 // any schema referenced by the first query.
//                 timedomain_bundle = Self::timedomain_for(
//                     &*self,
//                     self.catalog(),
//                     source_ids,
//                     &timeline_context,
//                     session.conn_id(),
//                     cluster_id,
//                 )?;
//
//                 &timedomain_bundle
//             } else {
//                 // If not in a transaction, use the source.
//                 source_bundle
//             };
//             let (determination, read_holds) = self
//                 .determine_timestamp(
//                     session,
//                     determine_bundle,
//                     when,
//                     cluster_id,
//                     &timeline_context,
//                     oracle_read_ts,
//                     real_time_recency_ts,
//                 )
//                 .await?;
//             // We only need read holds if the read depends on a timestamp.
//             let read_holds = match determination.timestamp_context.timestamp() {
//                 Some(_ts) => Some(read_holds),
//                 None => {
//                     // We don't need the read holds and shouldn't add them
//                     // to the txn.
//                     //
//                     // TODO: Handle this within determine_timestamp.
//                     drop(read_holds);
//                     None
//                 }
//             };
//             (determination, read_holds)
//         }
//     };
//
//     // Always either verify the current statement ids are within the existing
//     // transaction's read hold set (timedomain), or create the read holds if this is the
//     // first statement in a transaction (or this is a single statement transaction).
//     // This must happen even if this is an `AS OF` query as well. There are steps after
//     // this that happen off thread, so no matter the kind of statement or transaction,
//     // we must acquire read holds here so they are held until the off-thread work
//     // returns to the coordinator.
//
//     if let Some(txn_reads) = self.txn_read_holds.get(session.conn_id()) {
//         // Find referenced ids not in the read hold. A reference could be caused by a
//         // user specifying an object in a different schema than the first query. An
//         // index could be caused by a CREATE INDEX after the transaction started.
//         let allowed_id_bundle = txn_reads.id_bundle();
//
//         // We don't need the read holds that determine_timestamp acquired
//         // for us.
//         drop(read_holds);
//
//         let outside = source_bundle.difference(&allowed_id_bundle);
//         // Queries without a timestamp and timeline can belong to any existing timedomain.
//         if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
//             let valid_names = self.resolve_collection_id_bundle_names(session, &allowed_id_bundle);
//             let invalid_names = self.resolve_collection_id_bundle_names(session, &outside);
//             return Err(AdapterError::RelationOutsideTimeDomain {
//                 relations: invalid_names,
//                 names: valid_names,
//             });
//         }
//     } else if let Some(read_holds) = read_holds {
//         self.store_transaction_read_holds(session, read_holds);
//     }
//
//     // TODO: Checking for only `InTransaction` and not `Implied` (also `Started`?) seems
//     // arbitrary and we don't recall why we did it (possibly an error!). Change this to always
//     // set the transaction ops. Decide and document what our policy should be on AS OF queries.
//     // Maybe they shouldn't be allowed in transactions at all because it's hard to explain
//     // what's going on there. This should probably get a small design document.
//
//     // We only track the peeks in the session if the query doesn't use AS
//     // OF or we're inside an explicit transaction. The latter case is
//     // necessary to support PG's `BEGIN` semantics, whose behavior can
//     // depend on whether or not reads have occurred in the txn.
//     let mut transaction_determination = determination.clone();
//     if when.is_transactional() {
//         session.add_transaction_ops(TransactionOps::Peeks {
//             determination: transaction_determination,
//             cluster_id,
//             requires_linearization,
//         })?;
//     } else if matches!(session.transaction(), &TransactionStatus::InTransaction(_)) {
//         // If the query uses AS OF, then ignore the timestamp.
//         transaction_determination.timestamp_context = TimestampContext::NoTimestamp;
//         session.add_transaction_ops(TransactionOps::Peeks {
//             determination: transaction_determination,
//             cluster_id,
//             requires_linearization,
//         })?;
//     };
//
//     Ok(determination)
// }
