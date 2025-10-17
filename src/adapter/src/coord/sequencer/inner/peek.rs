// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;

use http::Uri;
use itertools::Either;
use maplit::btreemap;
use mz_adapter_types::dyncfgs::PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION;
use mz_catalog::memory::objects::CatalogItem;
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_controller_types::ClusterId;
use mz_expr::{CollectionPlan, ResultSpec};
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{Datum, GlobalId, RowArena, Timestamp};
use mz_sql::ast::{ExplainStage, Statement};
use mz_sql::catalog::CatalogCluster;
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_sql::plan::QueryWhen;
use mz_sql::plan::{self, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_transform::EmptyStatisticsOracle;
use tokio::sync::oneshot;
use tracing::warn;
use tracing::{Instrument, Span};

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyTo};
use crate::command::ExecuteResponse;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::{self, PeekDataflowPlan, PeekPlan, PlannedPeek};
use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::timeline::TimelineContext;
use crate::coord::timestamp_selection::{
    TimestampContext, TimestampDetermination, TimestampProvider,
};
use crate::coord::{
    Coordinator, CopyToContext, ExecuteContext, ExplainContext, ExplainPlanContext, Message,
    PeekStage, PeekStageCopyTo, PeekStageExplainPlan, PeekStageExplainPushdown, PeekStageFinish,
    PeekStageLinearizeTimestamp, PeekStageOptimize, PeekStageRealTimeRecency,
    PeekStageTimestampReadHold, PlanValidity, StageResult, Staged, TargetCluster, WatchSetResponse,
};
use crate::error::AdapterError;
use crate::explain::insights::PlanInsightsContext;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::notice::AdapterNotice;
use crate::optimize::dataflows::{EvalTime, ExprPrepStyle, prep_scalar_expr};
use crate::optimize::{self, Optimize};
use crate::session::{RequireLinearization, Session, TransactionOps, TransactionStatus};
use crate::statement_logging::StatementLifecycleEvent;

impl Staged for PeekStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            PeekStage::LinearizeTimestamp(stage) => &mut stage.validity,
            PeekStage::RealTimeRecency(stage) => &mut stage.validity,
            PeekStage::TimestampReadHold(stage) => &mut stage.validity,
            PeekStage::Optimize(stage) => &mut stage.validity,
            PeekStage::Finish(stage) => &mut stage.validity,
            PeekStage::ExplainPlan(stage) => &mut stage.validity,
            PeekStage::ExplainPushdown(stage) => &mut stage.validity,
            PeekStage::CopyToPreflight(stage) => &mut stage.validity,
            PeekStage::CopyToDataflow(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            PeekStage::LinearizeTimestamp(stage) => {
                coord.peek_linearize_timestamp(ctx.session(), stage).await
            }
            PeekStage::RealTimeRecency(stage) => {
                coord.peek_real_time_recency(ctx.session(), stage).await
            }
            PeekStage::TimestampReadHold(stage) => {
                coord.peek_timestamp_read_hold(ctx.session_mut(), stage)
            }
            PeekStage::Optimize(stage) => coord.peek_optimize(ctx.session(), stage).await,
            PeekStage::Finish(stage) => coord.peek_finish(ctx, stage).await,
            PeekStage::ExplainPlan(stage) => coord.peek_explain_plan(ctx.session(), stage).await,
            PeekStage::ExplainPushdown(stage) => {
                coord.peek_explain_pushdown(ctx.session(), stage).await
            }
            PeekStage::CopyToPreflight(stage) => coord.peek_copy_to_preflight(stage).await,
            PeekStage::CopyToDataflow(stage) => coord.peek_copy_to_dataflow(ctx, stage).await,
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::PeekStageReady {
            ctx,
            span,
            stage: self,
        }
    }

    fn cancel_enabled(&self) -> bool {
        true
    }
}

impl Coordinator {
    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    #[instrument]
    pub(crate) async fn sequence_peek(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::SelectPlan,
        target_cluster: TargetCluster,
        max_query_result_size: Option<u64>,
    ) {
        let explain_ctx = if ctx.session().vars().emit_plan_insights_notice() {
            let optimizer_trace = OptimizerTrace::new(ExplainStage::PlanInsights.paths());
            ExplainContext::PlanInsightsNotice(optimizer_trace)
        } else {
            ExplainContext::None
        };

        let stage = return_if_err!(
            self.peek_validate(
                ctx.session(),
                plan,
                target_cluster,
                None,
                explain_ctx,
                max_query_result_size
            ),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn sequence_copy_to(
        &mut self,
        ctx: ExecuteContext,
        plan::CopyToPlan {
            select_plan,
            desc,
            to,
            connection,
            connection_id,
            format,
            max_file_size,
        }: plan::CopyToPlan,
        target_cluster: TargetCluster,
    ) {
        let eval_uri = |to: HirScalarExpr| -> Result<Uri, AdapterError> {
            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session: ctx.session(),
                catalog_state: self.catalog().state(),
            };
            let mut to = to.lower_uncorrelated()?;
            prep_scalar_expr(&mut to, style)?;
            let temp_storage = RowArena::new();
            let evaled = to.eval_pop(&[], &temp_storage, &mut Vec::new())?;
            if evaled == Datum::Null {
                coord_bail!("COPY TO target value can not be null");
            }
            let to_url = match Uri::from_str(evaled.unwrap_str()) {
                Ok(url) => {
                    if url.scheme_str() != Some("s3") {
                        coord_bail!("only 's3://...' urls are supported as COPY TO target");
                    }
                    url
                }
                Err(e) => coord_bail!("could not parse COPY TO target url: {}", e),
            };
            Ok(to_url)
        };

        let uri = return_if_err!(eval_uri(to), ctx);

        let stage = return_if_err!(
            self.peek_validate(
                ctx.session(),
                select_plan,
                target_cluster,
                Some(CopyToContext {
                    desc,
                    uri,
                    connection,
                    connection_id,
                    format,
                    max_file_size,
                    // This will be set in `peek_stage_validate` stage below.
                    output_batch_count: None,
                }),
                ExplainContext::None,
                Some(ctx.session().vars().max_query_result_size()),
            ),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_peek(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
        target_cluster: TargetCluster,
    ) {
        let plan::Explainee::Statement(stmt) = explainee else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };
        let plan::ExplaineeStatement::Select { broken, plan, desc } = stmt else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        let stage = return_if_err!(
            self.peek_validate(
                ctx.session(),
                plan,
                target_cluster,
                None,
                ExplainContext::Plan(ExplainPlanContext {
                    broken,
                    config,
                    format,
                    stage,
                    replan: None,
                    desc: Some(desc),
                    optimizer_trace,
                }),
                Some(ctx.session().vars().max_query_result_size()),
            ),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    /// Do some simple validation. We must defer most of it until after any off-thread work.
    #[instrument]
    pub fn peek_validate(
        &self,
        session: &Session,
        plan: mz_sql::plan::SelectPlan,
        target_cluster: TargetCluster,
        copy_to_ctx: Option<CopyToContext>,
        explain_ctx: ExplainContext,
        max_query_result_size: Option<u64>,
    ) -> Result<PeekStage, AdapterError> {
        // Collect optimizer parameters.
        let catalog = self.owned_catalog();
        let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
        let compute_instance = self
            .instance_snapshot(cluster.id())
            .expect("compute instance does not exist");
        let (_, view_id) = self.allocate_transient_id();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster.id()).config.features())
            .override_from(&explain_ctx);

        if cluster.replicas().next().is_none() && explain_ctx.needs_cluster() {
            return Err(AdapterError::NoClusterReplicasAvailable {
                name: cluster.name.clone(),
                is_managed: cluster.is_managed(),
            });
        }

        let optimizer = match copy_to_ctx {
            None => {
                // Collect optimizer parameters specific to the peek::Optimizer.
                let (_, view_id) = self.allocate_transient_id();
                let (_, index_id) = self.allocate_transient_id();

                // Build an optimizer for this SELECT.
                Either::Left(optimize::peek::Optimizer::new(
                    Arc::clone(&catalog),
                    compute_instance,
                    plan.finishing.clone(),
                    view_id,
                    index_id,
                    optimizer_config,
                    self.optimizer_metrics(),
                ))
            }
            Some(mut copy_to_ctx) => {
                // Getting the max worker count across replicas
                // and using that value for the number of batches to
                // divide the copy output into.
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
                // Build an optimizer for this COPY TO.
                Either::Right(optimize::copy_to::Optimizer::new(
                    Arc::clone(&catalog),
                    compute_instance,
                    view_id,
                    copy_to_ctx,
                    optimizer_config,
                    self.optimizer_metrics(),
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
        let mut timeline_context = self
            .catalog()
            .validate_timeline_context(source_ids.iter().copied())?;
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

        let dependencies = source_ids
            .iter()
            .map(|id| self.catalog.resolve_item_id(id))
            .collect();
        let validity = PlanValidity::new(
            catalog.transient_revision(),
            dependencies,
            Some(cluster.id()),
            target_replica,
            session.role_metadata().clone(),
        );

        Ok(PeekStage::LinearizeTimestamp(PeekStageLinearizeTimestamp {
            validity,
            plan,
            max_query_result_size,
            source_ids,
            target_replica,
            timeline_context,
            optimizer,
            explain_ctx,
        }))
    }

    /// Possibly linearize a timestamp from a `TimestampOracle`.
    #[instrument]
    async fn peek_linearize_timestamp(
        &self,
        session: &Session,
        PeekStageLinearizeTimestamp {
            validity,
            source_ids,
            plan,
            max_query_result_size,
            target_replica,
            timeline_context,
            optimizer,
            explain_ctx,
        }: PeekStageLinearizeTimestamp,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        let isolation_level = session.vars().transaction_isolation().clone();
        let timeline = Coordinator::get_timeline(&timeline_context);
        let needs_linearized_read_ts =
            Coordinator::needs_linearized_read_ts(&isolation_level, &plan.when);

        let build_stage = move |oracle_read_ts: Option<Timestamp>| PeekStageRealTimeRecency {
            validity,
            plan,
            max_query_result_size,
            source_ids,
            target_replica,
            timeline_context,
            oracle_read_ts,
            optimizer,
            explain_ctx,
        };

        match timeline {
            Some(timeline) if needs_linearized_read_ts => {
                let oracle = self.get_timestamp_oracle(&timeline);

                // We ship the timestamp oracle off to an async task, so that we
                // don't block the main task while we wait.

                let span = Span::current();
                Ok(StageResult::Handle(mz_ore::task::spawn(
                    || "linearize timestamp",
                    async move {
                        let oracle_read_ts = oracle.read_ts().await;
                        let stage = build_stage(Some(oracle_read_ts));
                        let stage = PeekStage::RealTimeRecency(stage);
                        Ok(Box::new(stage))
                    }
                    .instrument(span),
                )))
            }
            Some(_) | None => {
                let stage = build_stage(None);
                let stage = PeekStage::RealTimeRecency(stage);
                Ok(StageResult::Immediate(Box::new(stage)))
            }
        }
    }

    /// Determine a read timestamp and create appropriate read holds.
    #[instrument]
    fn peek_timestamp_read_hold(
        &mut self,
        session: &mut Session,
        PeekStageTimestampReadHold {
            mut validity,
            plan,
            max_query_result_size,
            source_ids,
            target_replica,
            timeline_context,
            oracle_read_ts,
            real_time_recency_ts,
            optimizer,
            explain_ctx,
        }: PeekStageTimestampReadHold,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        let cluster_id = match optimizer.as_ref() {
            Either::Left(optimizer) => optimizer.cluster_id(),
            Either::Right(optimizer) => optimizer.cluster_id(),
        };
        let id_bundle = self
            .dataflow_builder(cluster_id)
            .sufficient_collections(source_ids.iter().copied());

        // Although we have added `sources.depends_on()` to the validity already, also add the
        // sufficient collections for safety.
        let item_ids = id_bundle
            .iter()
            .map(|id| self.catalog().resolve_item_id(&id));
        validity.extend_dependencies(item_ids);

        let determination = self.sequence_peek_timestamp(
            session,
            &plan.when,
            cluster_id,
            timeline_context,
            oracle_read_ts,
            &id_bundle,
            &source_ids,
            real_time_recency_ts,
            (&explain_ctx).into(),
        )?;

        let stage = PeekStage::Optimize(PeekStageOptimize {
            validity,
            plan,
            max_query_result_size,
            source_ids,
            id_bundle,
            target_replica,
            determination,
            optimizer,
            explain_ctx,
        });
        Ok(StageResult::Immediate(Box::new(stage)))
    }

    #[instrument]
    async fn peek_optimize(
        &self,
        session: &Session,
        PeekStageOptimize {
            validity,
            plan,
            max_query_result_size,
            source_ids,
            id_bundle,
            target_replica,
            determination,
            mut optimizer,
            explain_ctx,
        }: PeekStageOptimize,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let timestamp_context = determination.timestamp_context.clone();
        let stats = self
            .statistics_oracle(session, &source_ids, &timestamp_context.antichain(), true)
            .await
            .unwrap_or_else(|_| Box::new(EmptyStatisticsOracle));
        let session = session.meta();
        let now = self.catalog().config().now.clone();
        let catalog = self.owned_catalog();
        let mut compute_instances = BTreeMap::new();
        if explain_ctx.needs_plan_insights() {
            // There's a chance for index skew (indexes were created/deleted between stages) from the
            // original plan, but that seems acceptable for insights.
            for cluster in self.catalog().user_clusters() {
                let snapshot = self.instance_snapshot(cluster.id).expect("must exist");
                compute_instances.insert(cluster.name.clone(), snapshot);
            }
        }

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                span.in_scope(|| {
                    let pipeline = || -> Result<Either<optimize::peek::GlobalLirPlan, optimize::copy_to::GlobalLirPlan>, AdapterError> {
                        let _dispatch_guard = explain_ctx.dispatch_guard();

                        let raw_expr = plan.source.clone();

                        match optimizer.as_mut() {
                            // Optimize SELECT statement.
                            Either::Left(optimizer) => {
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan = local_mir_plan.resolve(timestamp_context.clone(), &session, stats);
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                                Ok(Either::Left(global_lir_plan))
                            }
                            // Optimize COPY TO statement.
                            Either::Right(optimizer) => {
                                // HIR ⇒ MIR lowering and MIR optimization (local)
                                let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                                // Attach resolved context required to continue the pipeline.
                                let local_mir_plan = local_mir_plan.resolve(timestamp_context.clone(), &session, stats);
                                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                                let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                                Ok(Either::Right(global_lir_plan))
                            }
                        }
                    };

                    let pipeline_result = pipeline();
                    let optimization_finished_at = now();

                    let stage = match pipeline_result {
                        Ok(Either::Left(global_lir_plan)) => {
                            let optimizer = optimizer.unwrap_left();
                            // Enable fast path cluster calculation for slow path plans.
                            let needs_plan_insights = explain_ctx.needs_plan_insights();
                            // Disable anything that uses the optimizer if we only want the notice and
                            // plan optimization took longer than the threshold. This is to prevent a
                            // situation where optimizing takes a while and there a lots of clusters,
                            // which would delay peek execution by the product of those.
                            let opt_limit = PLAN_INSIGHTS_NOTICE_FAST_PATH_CLUSTERS_OPTIMIZE_DURATION
                                .get(catalog.system_config().dyncfgs());
                            let target_instance = catalog
                                .get_cluster(optimizer.cluster_id())
                                .name
                                .clone();
                            let enable_re_optimize =
                                !(matches!(explain_ctx, ExplainContext::PlanInsightsNotice(_))
                                    && optimizer.duration() > opt_limit);
                            let insights_ctx = needs_plan_insights.then(|| PlanInsightsContext {
                                stmt: plan.select.as_deref().map(Clone::clone).map(Statement::Select),
                                raw_expr: plan.source.clone(),
                                catalog,
                                compute_instances,
                                target_instance,
                                metrics: optimizer.metrics().clone(),
                                finishing: optimizer.finishing().clone(),
                                optimizer_config: optimizer.config().clone(),
                                session,
                                timestamp_context,
                                view_id: optimizer.select_id(),
                                index_id: optimizer.index_id(),
                                enable_re_optimize,
                            }).map(Box::new);
                            match explain_ctx {
                                ExplainContext::Plan(explain_ctx) => {
                                    let (_, df_meta, _) = global_lir_plan.unapply();
                                    PeekStage::ExplainPlan(PeekStageExplainPlan {
                                        validity,
                                        optimizer,
                                        df_meta,
                                        explain_ctx,
                                        insights_ctx,
                                    })
                                }
                                ExplainContext::PlanInsightsNotice(optimizer_trace) => {
                                    PeekStage::Finish(PeekStageFinish {
                                        validity,
                                        plan,
                                        max_query_result_size,
                                        id_bundle,
                                        target_replica,
                                        source_ids,
                                        determination,
                                        cluster_id: optimizer.cluster_id(),
                                        finishing: optimizer.finishing().clone(),
                                        plan_insights_optimizer_trace: Some(optimizer_trace),
                                        global_lir_plan,
                                        optimization_finished_at,
                                        insights_ctx,
                                    })
                                }
                                ExplainContext::None => PeekStage::Finish(PeekStageFinish {
                                    validity,
                                    plan,
                                    max_query_result_size,
                                    id_bundle,
                                    target_replica,
                                    source_ids,
                                    determination,
                                    cluster_id: optimizer.cluster_id(),
                                    finishing: optimizer.finishing().clone(),
                                    plan_insights_optimizer_trace: None,
                                    global_lir_plan,
                                    optimization_finished_at,
                                    insights_ctx,
                                }),
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
                            PeekStage::CopyToPreflight(PeekStageCopyTo {
                                validity,
                                optimizer,
                                global_lir_plan,
                                optimization_finished_at,
                                source_ids,
                            })
                        }
                        // Internal optimizer errors are handled differently
                        // depending on the caller.
                        Err(err) => {
                            let Some(optimizer) = optimizer.left() else {
                                // In `COPY TO` contexts, immediately retire the
                                // execution with the error.
                                return Err(err);
                            };
                            let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                                // In `sequence_~` contexts, immediately retire the
                                // execution with the error.
                                return Err(err);
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
                                    insights_ctx: None,
                                })
                            } else {
                                // In regular `EXPLAIN` contexts, immediately retire
                                // the execution with the error.
                                return Err(err);
                            }
                        }
                    };
                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn peek_real_time_recency(
        &self,
        session: &Session,
        PeekStageRealTimeRecency {
            validity,
            plan,
            max_query_result_size,
            source_ids,
            target_replica,
            timeline_context,
            oracle_read_ts,
            optimizer,
            explain_ctx,
        }: PeekStageRealTimeRecency,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        let item_ids: Vec<_> = source_ids
            .iter()
            .map(|gid| self.catalog.resolve_item_id(gid))
            .collect();
        let fut = self
            .determine_real_time_recent_timestamp(session, item_ids.into_iter())
            .await?;

        match fut {
            Some(fut) => {
                let span = Span::current();
                Ok(StageResult::Handle(mz_ore::task::spawn(
                    || "peek real time recency",
                    async move {
                        let real_time_recency_ts = fut.await?;
                        let stage = PeekStage::TimestampReadHold(PeekStageTimestampReadHold {
                            validity,
                            plan,
                            max_query_result_size,
                            target_replica,
                            timeline_context,
                            source_ids,
                            optimizer,
                            explain_ctx,
                            oracle_read_ts,
                            real_time_recency_ts: Some(real_time_recency_ts),
                        });
                        Ok(Box::new(stage))
                    }
                    .instrument(span),
                )))
            }
            None => Ok(StageResult::Immediate(Box::new(
                PeekStage::TimestampReadHold(PeekStageTimestampReadHold {
                    validity,
                    plan,
                    max_query_result_size,
                    target_replica,
                    timeline_context,
                    source_ids,
                    optimizer,
                    explain_ctx,
                    oracle_read_ts,
                    real_time_recency_ts: None,
                }),
            ))),
        }
    }

    #[instrument]
    async fn peek_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        PeekStageFinish {
            validity: _,
            plan,
            max_query_result_size,
            id_bundle,
            target_replica,
            source_ids,
            determination,
            cluster_id,
            finishing,
            plan_insights_optimizer_trace,
            global_lir_plan,
            optimization_finished_at,
            insights_ctx,
        }: PeekStageFinish,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        if let Some(id) = ctx.extra.contents() {
            self.record_statement_lifecycle_event(
                &id,
                &StatementLifecycleEvent::OptimizationFinished,
                optimization_finished_at,
            );
        }

        let session = ctx.session_mut();
        let conn_id = session.conn_id().clone();

        let (peek_plan, df_meta, typ) = global_lir_plan.unapply();
        let source_arity = typ.arity();

        self.emit_optimizer_notices(&*session, &df_meta.optimizer_notices);

        let target_cluster = self.catalog().get_cluster(cluster_id);

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features());

        if let Some(trace) = plan_insights_optimizer_trace {
            let insights = trace
                .into_plan_insights(
                    &features,
                    &self.catalog().for_session(session),
                    Some(plan.finishing),
                    Some(target_cluster),
                    df_meta,
                    insights_ctx,
                )
                .await?;
            session.add_notice(AdapterNotice::PlanInsights(insights));
        }

        let planned_peek = PlannedPeek {
            plan: peek_plan,
            determination: determination.clone(),
            conn_id: conn_id.clone(),
            intermediate_result_type: typ,
            source_arity,
            source_ids,
        };

        if let Some(transient_index_id) = match &planned_peek.plan {
            peek::PeekPlan::FastPath(_) => None,
            peek::PeekPlan::SlowPath(PeekDataflowPlan { id, .. }) => Some(id),
        } {
            if let Some(statement_logging_id) = ctx.extra.contents() {
                self.set_transient_index_id(statement_logging_id, *transient_index_id);
            }
        }

        if let Some(uuid) = ctx.extra().contents() {
            let ts = determination.timestamp_context.timestamp_or_default();
            let mut transitive_storage_deps = BTreeSet::new();
            let mut transitive_compute_deps = BTreeSet::new();
            for item_id in id_bundle
                .iter()
                .map(|gid| self.catalog.state().get_entry_by_global_id(&gid).id())
                .flat_map(|id| self.catalog.state().transitive_uses(id))
            {
                let entry = self.catalog.state().get_entry(&item_id);
                match entry.item() {
                    // TODO(alter_table): Adding all of the GlobalIds for an object is incorrect.
                    // For example, this peek may depend on just a single version of a table, but
                    // we would add dependencies on all versions of said table. Doing this is okay
                    // for now since we can't yet version tables, but should get fixed.
                    CatalogItem::Table(_) | CatalogItem::Source(_) => {
                        transitive_storage_deps.extend(entry.global_ids());
                    }
                    CatalogItem::MaterializedView(_) | CatalogItem::Index(_) => {
                        transitive_compute_deps.extend(entry.global_ids());
                    }
                    _ => {}
                }
            }
            self.install_storage_watch_set(
                conn_id.clone(),
                transitive_storage_deps,
                ts,
                WatchSetResponse::StatementDependenciesReady(
                    uuid,
                    StatementLifecycleEvent::StorageDependenciesFinished,
                ),
            );
            self.install_compute_watch_set(
                conn_id,
                transitive_compute_deps,
                ts,
                WatchSetResponse::StatementDependenciesReady(
                    uuid,
                    StatementLifecycleEvent::ComputeDependenciesFinished,
                ),
            )
        }

        let max_result_size = self.catalog().system_config().max_result_size();

        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(
                ctx.extra_mut(),
                planned_peek,
                finishing,
                cluster_id,
                target_replica,
                max_result_size,
                max_query_result_size,
            )
            .await?;

        if ctx.session().vars().emit_timestamp_notice() {
            let explanation =
                self.explain_timestamp(ctx.session(), cluster_id, &id_bundle, determination);
            ctx.session()
                .add_notice(AdapterNotice::QueryTimestamp { explanation });
        }

        let resp = match plan.copy_to {
            None => resp,
            Some(format) => ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            },
        };
        Ok(StageResult::Response(resp))
    }

    #[instrument]
    async fn peek_copy_to_preflight(
        &mut self,
        copy_to: PeekStageCopyTo,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        let connection_context = self.connection_context().clone();
        Ok(StageResult::Handle(mz_ore::task::spawn(
            || "peek copy to preflight",
            async {
                let sinks = &copy_to.global_lir_plan.df_desc().sink_exports;
                if sinks.len() != 1 {
                    return Err(AdapterError::Internal(
                        "expected exactly one copy to s3 sink".into(),
                    ));
                }
                let (sink_id, sink_desc) = sinks
                    .first_key_value()
                    .expect("known to be exactly one copy to s3 sink");
                match &sink_desc.connection {
                    ComputeSinkConnection::CopyToS3Oneshot(conn) => {
                        mz_storage_types::sinks::s3_oneshot_sink::preflight(
                            connection_context,
                            &conn.aws_connection,
                            &conn.upload_info,
                            conn.connection_id,
                            *sink_id,
                        )
                        .await?;
                        Ok(Box::new(PeekStage::CopyToDataflow(copy_to)))
                    }
                    _ => Err(AdapterError::Internal(
                        "expected copy to s3 oneshot sink".into(),
                    )),
                }
            },
        )))
    }

    #[instrument]
    async fn peek_copy_to_dataflow(
        &mut self,
        ctx: &ExecuteContext,
        PeekStageCopyTo {
            validity: _,
            optimizer,
            global_lir_plan,
            optimization_finished_at,
            source_ids,
        }: PeekStageCopyTo,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        if let Some(id) = ctx.extra.contents() {
            self.record_statement_lifecycle_event(
                &id,
                &StatementLifecycleEvent::OptimizationFinished,
                optimization_finished_at,
            );
        }

        let sink_id = global_lir_plan.sink_id();
        let cluster_id = optimizer.cluster_id();

        let (df_desc, df_meta) = global_lir_plan.unapply();

        self.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

        // Callback for the active copy to.
        let (tx, rx) = oneshot::channel();
        let active_copy_to = ActiveCopyTo {
            conn_id: ctx.session().conn_id().clone(),
            tx,
            cluster_id,
            depends_on: source_ids,
        };
        // Add metadata for the new COPY TO. CopyTo returns a `ready` future, so it is safe to drop.
        drop(
            self.add_active_compute_sink(sink_id, ActiveComputeSink::CopyTo(active_copy_to))
                .await,
        );

        // Ship dataflow.
        self.ship_dataflow(df_desc, cluster_id, None).await;

        let span = Span::current();
        Ok(StageResult::HandleRetire(mz_ore::task::spawn(
            || "peek copy to dataflow",
            async {
                let res = rx.await;
                match res {
                    Ok(res) => res,
                    Err(_) => Err(AdapterError::Internal("copy to sender dropped".into())),
                }
            }
            .instrument(span),
        )))
    }

    #[instrument]
    async fn peek_explain_plan(
        &self,
        session: &Session,
        PeekStageExplainPlan {
            optimizer,
            insights_ctx,
            df_meta,
            explain_ctx:
                ExplainPlanContext {
                    config,
                    format,
                    stage,
                    desc,
                    optimizer_trace,
                    ..
                },
            ..
        }: PeekStageExplainPlan,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        let desc = desc.expect("RelationDesc for SelectPlan in EXPLAIN mode");

        let session_catalog = self.catalog().for_session(session);
        let expr_humanizer = {
            let transient_items = btreemap! {
                optimizer.select_id() => TransientItem::new(
                    Some(vec![GlobalId::Explain.to_string()]),
                    Some(desc.iter_names().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let finishing = if optimizer.finishing().is_trivial(desc.arity()) {
            None
        } else {
            Some(optimizer.finishing().clone())
        };

        let target_cluster = self.catalog().get_cluster(optimizer.cluster_id());
        let features = optimizer.config().features.clone();

        let rows = optimizer_trace
            .into_rows(
                format,
                &config,
                &features,
                &expr_humanizer,
                finishing,
                Some(target_cluster),
                df_meta,
                stage,
                plan::ExplaineeStatementKind::Select,
                insights_ctx,
            )
            .await?;

        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }

    #[instrument]
    async fn peek_explain_pushdown(
        &self,
        session: &Session,
        stage: PeekStageExplainPushdown,
    ) -> Result<StageResult<Box<PeekStage>>, AdapterError> {
        let as_of = stage.determination.timestamp_context.antichain();
        let mz_now = stage
            .determination
            .timestamp_context
            .timestamp()
            .map(|t| ResultSpec::value(Datum::MzTimestamp(*t)))
            .unwrap_or_else(ResultSpec::value_all);
        let fut = self
            .render_explain_pushdown_prepare(session, as_of, mz_now, stage.imports)
            .await;
        let span = Span::current();
        Ok(StageResult::HandleRetire(mz_ore::task::spawn(
            || "peek explain pushdown",
            fut.instrument(span),
        )))
    }

    /// Determines the query timestamp and acquires read holds on dependent sources
    /// if necessary.
    #[instrument]
    pub(super) fn sequence_peek_timestamp(
        &mut self,
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
                    // In a transaction, determine a timestamp that will be valid for anything in
                    // any schema referenced by the first query.
                    timedomain_bundle = self.timedomain_for(
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
                let (determination, read_holds) = self.determine_timestamp(
                    session,
                    determine_bundle,
                    when,
                    cluster_id,
                    &timeline_context,
                    oracle_read_ts,
                    real_time_recency_ts,
                )?;
                // We only need read holds if the read depends on a timestamp.
                let read_holds = match determination.timestamp_context.timestamp() {
                    Some(_ts) => Some(read_holds),
                    None => {
                        // We don't need the read holds and shouldn't add them
                        // to the txn.
                        //
                        // TODO: Handle this within determine_timestamp.
                        drop(read_holds);
                        None
                    }
                };
                (determination, read_holds)
            }
        };

        // Always either verify the current statement ids are within the existing
        // transaction's read hold set (timedomain), or create the read holds if this is the
        // first statement in a transaction (or this is a single statement transaction).
        // This must happen even if this is an `AS OF` query as well. There are steps after
        // this that happen off thread, so no matter the kind of statement or transaction,
        // we must acquire read holds here so they are held until the off-thread work
        // returns to the coordinator.

        if let Some(txn_reads) = self.txn_read_holds.get(session.conn_id()) {
            // Find referenced ids not in the read hold. A reference could be caused by a
            // user specifying an object in a different schema than the first query. An
            // index could be caused by a CREATE INDEX after the transaction started.
            let allowed_id_bundle = txn_reads.id_bundle();

            // We don't need the read holds that determine_timestamp acquired
            // for us.
            drop(read_holds);

            let outside = source_bundle.difference(&allowed_id_bundle);
            // Queries without a timestamp and timeline can belong to any existing timedomain.
            if determination.timestamp_context.contains_timestamp() && !outside.is_empty() {
                let valid_names =
                    self.resolve_collection_id_bundle_names(session, &allowed_id_bundle);
                let invalid_names = self.resolve_collection_id_bundle_names(session, &outside);
                return Err(AdapterError::RelationOutsideTimeDomain {
                    relations: invalid_names,
                    names: valid_names,
                });
            }
        } else if let Some(read_holds) = read_holds {
            self.store_transaction_read_holds(session, read_holds);
        }

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
}
