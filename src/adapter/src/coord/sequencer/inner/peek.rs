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

use futures::stream::FuturesOrdered;
use http::Uri;
use itertools::Either;
use maplit::btreemap;
use mz_controller_types::ClusterId;
use mz_expr::{CollectionPlan, ResultSpec};
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{instrument, task};
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{Datum, GlobalId, IntoRowIterator, Row, RowArena, Timestamp};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::{CatalogCluster, SessionCatalog};
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_catalog::memory::objects::CatalogItem;
use mz_persist_client::stats::SnapshotPartStats;
use mz_sql::plan::QueryWhen;
use mz_sql::plan::{self, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_storage_types::stats::RelationPartStats;
use mz_transform::EmptyStatisticsOracle;
use tracing::Instrument;
use tracing::{event, warn, Level};

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
    PeekStageTimestampReadHold, PeekStageValidate, PlanValidity, RealTimeRecencyContext,
    TargetCluster,
};
use crate::error::AdapterError;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::notice::AdapterNotice;
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::optimize::{self, Optimize};
use crate::session::{RequireLinearization, Session, TransactionOps, TransactionStatus};
use crate::statement_logging::StatementLifecycleEvent;

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
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));

        let explain_ctx = if ctx.session().vars().emit_plan_insights_notice() {
            let optimizer_trace = OptimizerTrace::new(ExplainStage::PlanInsights.paths());
            ExplainContext::PlanInsightsNotice(optimizer_trace)
        } else {
            ExplainContext::None
        };

        self.execute_peek_stage(
            ctx,
            OpenTelemetryContext::obtain(),
            PeekStage::Validate(PeekStageValidate {
                plan,
                target_cluster,
                copy_to_ctx: None,
                explain_ctx,
            }),
        )
        .await;
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
            let evaled = to.eval(&[], &temp_storage)?;
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

        self.execute_peek_stage(
            ctx,
            OpenTelemetryContext::obtain(),
            PeekStage::Validate(PeekStageValidate {
                plan: select_plan,
                target_cluster,
                copy_to_ctx: Some(CopyToContext {
                    desc,
                    uri,
                    connection,
                    connection_id,
                    format,
                    max_file_size,
                    // This will be set in `peek_stage_validate` stage below.
                    output_batch_count: None,
                }),
                explain_ctx: ExplainContext::None,
            }),
        )
        .await;
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

        self.execute_peek_stage(
            ctx,
            OpenTelemetryContext::obtain(),
            PeekStage::Validate(PeekStageValidate {
                plan,
                target_cluster,
                copy_to_ctx: None,
                explain_ctx: ExplainContext::Plan(ExplainPlanContext {
                    broken,
                    config,
                    format,
                    stage,
                    replan: None,
                    desc: Some(desc),
                    optimizer_trace,
                }),
            }),
        )
        .await;
    }

    /// Processes as many `peek` stages as possible.
    #[instrument]
    pub(crate) async fn execute_peek_stage(
        &mut self,
        mut ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        mut stage: PeekStage,
    ) {
        use PeekStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            event!(Level::TRACE, stage = format!("{:?}", stage));

            // Always verify peek validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                if let Err(err) = validity.check(self.catalog()) {
                    ctx.retire(Err(err));
                    return;
                }
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next =
                        return_if_err!(self.peek_stage_validate(ctx.session_mut(), stage), ctx);

                    (ctx, PeekStage::LinearizeTimestamp(next))
                }
                LinearizeTimestamp(stage) => {
                    self.peek_stage_linearize_timestamp(ctx, root_otel_ctx.clone(), stage)
                        .await;
                    return;
                }
                RealTimeRecency(stage) => {
                    match self.peek_stage_real_time_recency(ctx, root_otel_ctx.clone(), stage) {
                        Some((ctx, next)) => (ctx, PeekStage::TimestampReadHold(next)),
                        None => return,
                    }
                }
                TimestampReadHold(stage) => {
                    let next = return_if_err!(
                        self.peek_stage_timestamp_read_hold(ctx.session_mut(), stage)
                            .await,
                        ctx
                    );
                    (ctx, PeekStage::Optimize(next))
                }
                Optimize(stage) => {
                    self.peek_stage_optimize(ctx, root_otel_ctx.clone(), stage)
                        .await;
                    return;
                }
                Finish(stage) => {
                    let res = self.peek_stage_finish(&mut ctx, stage).await;
                    ctx.retire(res);
                    return;
                }
                CopyTo(stage) => {
                    self.peek_stage_copy_to_dataflow(ctx, stage).await;
                    return;
                }
                ExplainPlan(stage) => {
                    let result = self.peek_stage_explain_plan(&mut ctx, stage);
                    ctx.retire(result);
                    return;
                }
                ExplainPushdown(stage) => {
                    self.peek_stage_explain_pushdown(ctx, stage).await;
                    return;
                }
            }
        }
    }

    /// Do some simple validation. We must defer most of it until after any off-thread work.
    #[instrument]
    fn peek_stage_validate(
        &mut self,
        session: &Session,
        PeekStageValidate {
            plan,
            target_cluster,
            copy_to_ctx,
            explain_ctx,
        }: PeekStageValidate,
    ) -> Result<PeekStageLinearizeTimestamp, AdapterError> {
        // Collect optimizer parameters.
        let catalog = self.owned_catalog();
        let cluster = catalog.resolve_target_cluster(target_cluster, session)?;
        let compute_instance = self
            .instance_snapshot(cluster.id())
            .expect("compute instance does not exist");
        let view_id = self.allocate_transient_id()?;
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(cluster.id()).config.features())
            .override_from(&explain_ctx);

        if cluster.replicas().next().is_none() && explain_ctx.needs_cluster() {
            return Err(AdapterError::NoClusterReplicasAvailable(
                cluster.name.clone(),
            ));
        }

        let optimizer = match copy_to_ctx {
            None => {
                // Collect optimizer parameters specific to the peek::Optimizer.
                let compute_instance = self
                    .instance_snapshot(cluster.id())
                    .expect("compute instance does not exist");
                let view_id = self.allocate_transient_id()?;
                let index_id = self.allocate_transient_id()?;

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
        let mut timeline_context = self.validate_timeline_context(source_ids.clone())?;
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
    async fn peek_stage_linearize_timestamp(
        &mut self,
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
        PeekStageLinearizeTimestamp {
            validity,
            source_ids,
            plan,
            target_replica,
            timeline_context,
            optimizer,
            explain_ctx,
        }: PeekStageLinearizeTimestamp,
    ) {
        let isolation_level = ctx.session.vars().transaction_isolation().clone();
        let timeline = Coordinator::get_timeline(&timeline_context);
        let needs_linearized_read_ts =
            Coordinator::needs_linearized_read_ts(&isolation_level, &plan.when);

        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let build_stage = move |oracle_read_ts: Option<Timestamp>| PeekStageRealTimeRecency {
            validity,
            plan,
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

                let span = tracing::debug_span!("linearized timestamp task");
                mz_ore::task::spawn(|| "linearized timestamp task", async move {
                    let oracle_read_ts = oracle.read_ts().instrument(span).await;
                    let stage = build_stage(Some(oracle_read_ts));

                    let stage = PeekStage::RealTimeRecency(stage);
                    // Ignore errors if the coordinator has shut down.
                    let _ = internal_cmd_tx.send(Message::PeekStageReady {
                        ctx,
                        otel_ctx: root_otel_ctx,
                        stage,
                    });
                });
            }
            Some(_) | None => {
                let stage = build_stage(None);
                let stage = PeekStage::RealTimeRecency(stage);
                // Ignore errors if the coordinator has shut down.
                let _ = internal_cmd_tx.send(Message::PeekStageReady {
                    ctx,
                    otel_ctx: root_otel_ctx,
                    stage,
                });
            }
        }
    }

    /// Determine a read timestamp and create appropriate read holds.
    #[instrument]
    async fn peek_stage_timestamp_read_hold(
        &mut self,
        session: &mut Session,
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
        let id_bundle = self
            .dataflow_builder(cluster_id)
            .sufficient_collections(&source_ids);

        // Although we have added `sources.depends_on()` to the validity already, also add the
        // sufficient collections for safety.
        validity.dependency_ids.extend(id_bundle.iter());

        let determination = self
            .sequence_peek_timestamp(
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

    #[instrument]
    async fn peek_stage_optimize(
        &mut self,
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
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
    ) {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let timestamp_context = determination.timestamp_context.clone();
        let stats = self
            .statistics_oracle(
                ctx.session(),
                &source_ids,
                &timestamp_context.antichain(),
                true,
            )
            .await
            .unwrap_or_else(|_| Box::new(EmptyStatisticsOracle));
        let session = ctx.session().meta();

        mz_ore::task::spawn_blocking(
            || "optimize peek",
            move || {
                let pipeline = || -> Result<Either<optimize::peek::GlobalLirPlan, optimize::copy_to::GlobalLirPlan>, AdapterError> {
                    let _dispatch_guard = explain_ctx.dispatch_guard();

                    let raw_expr = plan.source.clone();

                    match optimizer.as_mut() {
                        // Optimize SELECT statement.
                        Either::Left(optimizer) => {
                            // HIR ⇒ MIR lowering and MIR optimization (local)
                            let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                            // Attach resolved context required to continue the pipeline.
                            let local_mir_plan = local_mir_plan.resolve(timestamp_context, &session, stats);
                            // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                            let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;

                            Ok(Either::Left(global_lir_plan))
                        }
                        // Optimize COPY TO statement.
                        Either::Right(optimizer) => {
                            // HIR ⇒ MIR lowering and MIR optimization (local and global)
                            let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                            // Attach resolved context required to continue the pipeline.
                            let local_mir_plan = local_mir_plan.resolve(timestamp_context, &session, stats);
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
                            return ctx.retire(Err(err.into()));
                        };
                        let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                            // In `sequence_~` contexts, immediately retire the
                            // execution with the error.
                            return ctx.retire(Err(err.into()));
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
                            return ctx.retire(Err(err.into()));
                        }
                    }
                };

                // Ignore errors if the coordinator has shut down.
                let _ = internal_cmd_tx.send(Message::PeekStageReady {
                    ctx,
                    otel_ctx: root_otel_ctx,
                    stage,
                });
            },
        );
    }

    #[instrument]
    fn peek_stage_real_time_recency(
        &mut self,
        ctx: ExecuteContext,
        root_otel_ctx: OpenTelemetryContext,
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
    ) -> Option<(ExecuteContext, PeekStageTimestampReadHold)> {
        match self.recent_timestamp(ctx.session(), source_ids.iter().cloned()) {
            Some(fut) => {
                let internal_cmd_tx = self.internal_cmd_tx.clone();
                let conn_id = ctx.session().conn_id().clone();
                self.pending_real_time_recency_timestamp.insert(
                    conn_id.clone(),
                    RealTimeRecencyContext::Peek {
                        ctx,
                        plan,
                        root_otel_ctx,
                        target_replica,
                        timeline_context,
                        oracle_read_ts: oracle_read_ts.clone(),
                        source_ids,
                        optimizer,
                        explain_ctx,
                    },
                );
                task::spawn(|| "real_time_recency_peek", async move {
                    let real_time_recency_ts = fut.await;
                    // It is not an error for these results to be ready after `internal_cmd_rx` has been dropped.
                    let result = internal_cmd_tx.send(Message::RealTimeRecencyTimestamp {
                        conn_id: conn_id.clone(),
                        real_time_recency_ts,
                        validity,
                    });
                    if let Err(e) = result {
                        warn!("internal_cmd_rx dropped before we could send: {:?}", e);
                    }
                });
                None
            }
            None => Some((
                ctx,
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
                },
            )),
        }
    }

    #[instrument]
    async fn peek_stage_finish(
        &mut self,
        ctx: &mut ExecuteContext,
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
        let session = ctx.session_mut();
        let conn_id = session.conn_id().clone();

        let (peek_plan, df_meta, typ) = global_lir_plan.unapply();
        let source_arity = typ.arity();

        self.emit_optimizer_notices(&*session, &df_meta.optimizer_notices);

        let target_cluster = self.catalog().get_cluster(optimizer.cluster_id());

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features());

        if let Some(trace) = plan_insights_optimizer_trace {
            let insights = trace.into_plan_insights(
                &features,
                &self.catalog().for_session(session),
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
            if let Some(statement_logging_id) = ctx.extra.contents() {
                self.set_transient_index_id(statement_logging_id, *transient_index_id);
            }
        }

        if let Some(uuid) = ctx.extra().contents() {
            let ts = determination.timestamp_context.timestamp_or_default();
            let mut transitive_storage_deps = BTreeSet::new();
            let mut transitive_compute_deps = BTreeSet::new();
            for id in id_bundle
                .iter()
                .flat_map(|id| self.catalog.state().transitive_uses(id))
            {
                match self.catalog.state().get_entry(&id).item() {
                    CatalogItem::Table(_) | CatalogItem::Source(_) => {
                        transitive_storage_deps.insert(id);
                    }
                    CatalogItem::MaterializedView(_) | CatalogItem::Index(_) => {
                        transitive_compute_deps.insert(id);
                    }
                    _ => {}
                }
            }
            self.controller.install_storage_watch_set(
                transitive_storage_deps,
                ts,
                Box::new((uuid, StatementLifecycleEvent::StorageDependenciesFinished)),
            );
            self.controller.install_compute_watch_set(
                transitive_compute_deps,
                ts,
                Box::new((uuid, StatementLifecycleEvent::ComputeDependenciesFinished)),
            )
        }

        let max_query_size = ctx.session().vars().max_query_result_size();
        let max_result_size = self.catalog().system_config().max_result_size();

        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(
                ctx.extra_mut(),
                planned_peek,
                optimizer.finishing().clone(),
                optimizer.cluster_id(),
                target_replica,
                max_result_size,
                Some(max_query_size),
            )
            .await?;

        if ctx.session().vars().emit_timestamp_notice() {
            let explanation = self.explain_timestamp(
                ctx.session(),
                optimizer.cluster_id(),
                &id_bundle,
                determination,
            );
            ctx.session()
                .add_notice(AdapterNotice::QueryTimestamp { explanation });
        }

        match plan.copy_to {
            None => Ok(resp),
            Some(format) => Ok(ExecuteResponse::CopyTo {
                format,
                resp: Box::new(resp),
            }),
        }
    }

    #[instrument]
    async fn peek_stage_copy_to_dataflow(
        &mut self,
        ctx: ExecuteContext,
        PeekStageCopyTo {
            validity,
            optimizer,
            global_lir_plan,
        }: PeekStageCopyTo,
    ) {
        let sink_id = global_lir_plan.sink_id();
        let cluster_id = optimizer.cluster_id();

        let (df_desc, df_meta) = global_lir_plan.unapply();

        self.emit_optimizer_notices(ctx.session(), &df_meta.optimizer_notices);

        // Callback for the active copy to.
        let active_copy_to = ActiveCopyTo {
            ctx,
            cluster_id,
            depends_on: validity.dependency_ids.clone(),
        };
        // Add metadata for the new COPY TO.
        drop(
            self.add_active_compute_sink(sink_id, ActiveComputeSink::CopyTo(active_copy_to))
                .await,
        );

        // Ship dataflow.
        self.ship_dataflow(df_desc, cluster_id).await;
    }

    #[instrument]
    fn peek_stage_explain_plan(
        &mut self,
        ctx: &mut ExecuteContext,
        PeekStageExplainPlan {
            optimizer,
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
    ) -> Result<ExecuteResponse, AdapterError> {
        let desc = desc.expect("RelationDesc for SelectPlan in EXPLAIN mode");

        let session_catalog = self.catalog().for_session(ctx.session());
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

        let rows = optimizer_trace.into_rows(
            format,
            &config,
            &features,
            &expr_humanizer,
            finishing,
            Some(target_cluster.name.as_str()),
            df_meta,
            stage,
            plan::ExplaineeStatementKind::Select,
        )?;

        Ok(Self::send_immediate_rows(rows))
    }

    #[instrument]
    async fn peek_stage_explain_pushdown(
        &mut self,
        ctx: ExecuteContext,
        stage: PeekStageExplainPushdown,
    ) {
        let explain_timeout = *ctx.session().vars().statement_timeout();
        let as_of = stage.determination.timestamp_context.antichain();
        let mz_now = stage
            .determination
            .timestamp_context
            .timestamp()
            .map(|t| ResultSpec::value(Datum::MzTimestamp(*t)))
            .unwrap_or_else(ResultSpec::value_all);

        let mut futures = FuturesOrdered::new();
        for (gid, mfp) in stage.imports {
            let catalog_entry = self.catalog.get_entry(&gid);
            let full_name = self
                .catalog
                .for_session(&ctx.session)
                .resolve_full_name(&catalog_entry.name);
            let name = format!("{}", full_name);
            let relation_desc = catalog_entry
                .item
                .desc_opt()
                .expect("source should have a proper desc")
                .into_owned();
            let stats_future = self
                .controller
                .storage
                .snapshot_parts_stats(gid, as_of.clone())
                .await;

            let mz_now = mz_now.clone();
            // These futures may block if the source is not yet readable at the as-of;
            // stash them in `futures` and only block on them in a separate task.
            futures.push_back(async move {
                let snapshot_stats = match stats_future.await {
                    Ok(stats) => stats,
                    Err(e) => return Err(e),
                };
                let mut total_bytes = 0;
                let mut total_parts = 0;
                let mut selected_bytes = 0;
                let mut selected_parts = 0;
                for SnapshotPartStats {
                    encoded_size_bytes: bytes,
                    stats,
                } in &snapshot_stats.parts
                {
                    let bytes = u64::cast_from(*bytes);
                    total_bytes += bytes;
                    total_parts += 1u64;
                    let selected = match stats {
                        None => true,
                        Some(stats) => {
                            let stats = stats.decode();
                            let stats = RelationPartStats::new(
                                name.as_str(),
                                &snapshot_stats.metrics.pushdown.part_stats,
                                &relation_desc,
                                &stats,
                            );
                            stats.may_match_mfp(mz_now.clone(), &mfp)
                        }
                    };

                    if selected {
                        selected_bytes += bytes;
                        selected_parts += 1u64;
                    }
                }
                Ok(Row::pack_slice(&[
                    name.as_str().into(),
                    total_bytes.into(),
                    selected_bytes.into(),
                    total_parts.into(),
                    selected_parts.into(),
                ]))
            });
        }

        task::spawn(|| "explain filter pushdown", async move {
            use futures::TryStreamExt;
            let res = match tokio::time::timeout(explain_timeout, futures.try_collect::<Vec<_>>())
                .await
            {
                Ok(Ok(rows)) => Ok(ExecuteResponse::SendingRowsImmediate {
                    rows: Box::new(rows.into_row_iter()),
                }),
                Ok(Err(err)) => Err(err.into()),
                Err(_) => Err(AdapterError::StatementTimeout),
            };

            ctx.retire(res);
        });
    }

    /// Determines the query timestamp and acquires read holds on dependent sources
    /// if necessary.
    #[instrument]
    pub(super) async fn sequence_peek_timestamp(
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
                let (determination, read_holds) = self
                    .determine_timestamp(
                        session,
                        determine_bundle,
                        when,
                        cluster_id,
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
