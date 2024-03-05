// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use http::Uri;
use itertools::Either;
use maplit::btreemap;
use mz_controller_types::ClusterId;
use mz_expr::CollectionPlan;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::{Datum, GlobalId, RowArena, Timestamp};
use mz_sql::catalog::CatalogCluster;
// Import `plan` module, but only import select elements to avoid merge conflicts on use statements.
use mz_catalog::memory::objects::CatalogItem;
use mz_sql::plan::QueryWhen;
use mz_sql::plan::{self, HirScalarExpr};
use mz_sql::session::metadata::SessionMetadata;
use mz_transform::EmptyStatisticsOracle;
use tracing::Instrument;
use tracing::{event, warn, Level};

use crate::active_compute_sink::{ActiveComputeSink, ActiveCopyTo};
use crate::command::ExecuteResponse;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::peek::{self, PeekDataflowPlan, PlannedPeek};
use crate::coord::sequencer::inner::{check_log_reads, return_if_err};
use crate::coord::timeline::TimelineContext;
use crate::coord::timestamp_selection::{
    TimestampContext, TimestampDetermination, TimestampProvider,
};
use crate::coord::{
    Coordinator, CopyToContext, ExecuteContext, ExplainContext, ExplainPlanContext, Message,
    PeekStage, PeekStageCopyTo, PeekStageExplainPlan, PeekStageFinish, PeekStageLinearizeTimestamp,
    PeekStageOptimize, PeekStageRealTimeRecency, PeekStageTimestampReadHold, PeekStageValidate,
    PlanValidity, RealTimeRecencyContext, TargetCluster,
};
use crate::error::AdapterError;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::notice::AdapterNotice;
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::optimize::{self, Optimize, OverrideFrom};
use crate::session::{RequireLinearization, Session, TransactionOps, TransactionStatus};
use crate::statement_logging::StatementLifecycleEvent;

impl Coordinator {
    /// Sequence a peek, determining a timestamp and the most efficient dataflow interaction.
    ///
    /// Peeks are sequenced by assigning a timestamp for evaluation, and then determining and
    /// deploying the most efficient evaluation plan. The peek could evaluate to a constant,
    /// be a simple read out of an existing arrangement, or required a new dataflow to build
    /// the results to return.
    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_peek(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::SelectPlan,
        target_cluster: TargetCluster,
    ) {
        event!(Level::TRACE, plan = format!("{:?}", plan));

        self.execute_peek_stage(
            ctx,
            OpenTelemetryContext::obtain(),
            PeekStage::Validate(PeekStageValidate {
                plan,
                target_cluster,
                copy_to_ctx: None,
                explain_ctx: ExplainContext::None,
            }),
        )
        .await;
    }

    #[mz_ore::instrument(level = "debug")]
    pub(crate) async fn sequence_copy_to(
        &mut self,
        ctx: ExecuteContext,
        plan::CopyToPlan {
            select_plan,
            desc,
            to,
            connection,
            connection_id,
            format_params,
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
                    format_params,
                    max_file_size,
                }),
                explain_ctx: ExplainContext::None,
            }),
        )
        .await;
    }

    #[mz_ore::instrument(level = "debug")]
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
        let optimizer_trace = OptimizerTrace::new(broken, stage.path());

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
    #[mz_ore::instrument(level = "debug")]
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
            }
        }
    }

    /// Do some simple validation. We must defer most of it until after any off-thread work.
    #[mz_ore::instrument(level = "debug")]
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
                ))
            }
            Some(copy_to_ctx) => {
                // Build an optimizer for this COPY TO.
                Either::Right(optimize::copy_to::Optimizer::new(
                    Arc::clone(&catalog),
                    compute_instance,
                    view_id,
                    copy_to_ctx,
                    optimizer_config,
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

        if cluster.replicas().next().is_none() {
            return Err(AdapterError::NoClusterReplicasAvailable(
                cluster.name.clone(),
            ));
        }

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
    #[mz_ore::instrument(level = "debug")]
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
        let linearized_timeline =
            Coordinator::get_linearized_timeline(&isolation_level, &plan.when, &timeline_context);

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

        match linearized_timeline {
            Some(timeline) => {
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
            None => {
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
    #[mz_ore::instrument(level = "debug")]
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

    #[mz_ore::instrument(level = "debug")]
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
                        if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                            let (_, df_meta, _) = global_lir_plan.unapply();
                            PeekStage::ExplainPlan(PeekStageExplainPlan {
                                validity,
                                select_id: optimizer.select_id(),
                                finishing: optimizer.finishing().clone(),
                                df_meta,
                                explain_ctx,
                            })
                        } else {
                            PeekStage::Finish(PeekStageFinish {
                                validity,
                                plan,
                                id_bundle,
                                target_replica,
                                source_ids,
                                determination,
                                optimizer,
                                global_lir_plan,
                            })
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
                                select_id: optimizer.select_id(),
                                finishing: optimizer.finishing().clone(),
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

    #[mz_ore::instrument(level = "debug")]
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

    #[mz_ore::instrument(level = "debug")]
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
            global_lir_plan,
        }: PeekStageFinish,
    ) -> Result<ExecuteResponse, AdapterError> {
        let session = ctx.session_mut();
        let conn_id = session.conn_id().clone();

        let (peek_plan, df_meta, typ) = global_lir_plan.unapply();
        let source_arity = typ.arity();

        self.emit_optimizer_notices(&*session, &df_meta.optimizer_notices);

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
            self.controller.install_watch_set(
                transitive_storage_deps,
                ts,
                Box::new((uuid, StatementLifecycleEvent::StorageDependenciesFinished)),
            );
            self.controller.install_watch_set(
                transitive_compute_deps,
                ts,
                Box::new((uuid, StatementLifecycleEvent::ComputeDependenciesFinished)),
            );
        }
        let max_query_result_size = std::cmp::min(
            ctx.session().vars().max_query_result_size(),
            self.catalog().system_config().max_result_size(),
        );

        // Implement the peek, and capture the response.
        let resp = self
            .implement_peek_plan(
                ctx.extra_mut(),
                planned_peek,
                optimizer.finishing().clone(),
                optimizer.cluster_id(),
                target_replica,
                max_query_result_size,
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

    #[mz_ore::instrument(level = "debug")]
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

    fn peek_stage_explain_plan(
        &mut self,
        ctx: &mut ExecuteContext,
        PeekStageExplainPlan {
            df_meta,
            select_id,
            finishing,
            explain_ctx:
                ExplainPlanContext {
                    broken,
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
                select_id => TransientItem::new(
                    Some(GlobalId::Explain.to_string()),
                    Some(GlobalId::Explain.to_string()),
                    Some(desc.iter_names().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let finishing = if finishing.is_trivial(desc.arity()) {
            None
        } else {
            Some(finishing)
        };

        let rows = optimizer_trace.into_rows(
            format,
            &config,
            &expr_humanizer,
            finishing,
            df_meta,
            stage,
            plan::ExplaineeStatementKind::Select,
        )?;

        if broken {
            tracing_core::callsite::rebuild_interest_cache();
        }

        Ok(Self::send_immediate_rows(rows))
    }

    /// Determines the query timestamp and acquires read holds on dependent sources
    /// if necessary.
    #[mz_ore::instrument(level = "debug")]
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
        let (determination, potential_read_holds) =
            match session.get_transaction_timestamp_determination() {
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
                    let determination = self
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
                    // We only need read holds if the read depends on a timestamp. We don't set the
                    // read holds here because it makes the code a bit more clear to handle the two
                    // cases for "is this the first statement in a transaction?" in an if/else block
                    // below.
                    let read_holds = determination
                        .timestamp_context
                        .timestamp()
                        .map(|timestamp| (timestamp.clone(), determine_bundle));
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
            // Transactions involving peeks will acquire read holds at most once.
            assert_eq!(txn_reads.len(), 1);
            let txn_reads = &txn_reads[0];
            // Find referenced ids not in the read hold. A reference could be caused by a
            // user specifying an object in a different schema than the first query. An
            // index could be caused by a CREATE INDEX after the transaction started.
            let allowed_id_bundle = txn_reads.id_bundle();
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
        } else if let Some((timestamp, bundle)) = potential_read_holds {
            self.acquire_read_holds_auto_cleanup(session, timestamp, bundle, true)
                .expect("able to acquire read holds at the time that we just got from `determine_timestamp`");
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
