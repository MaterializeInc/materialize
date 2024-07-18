// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use maplit::btreemap;
use mz_catalog::memory::objects::{CatalogItem, Index};
use mz_ore::instrument;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{Datum, Row};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateIndexExplain, CreateIndexFinish, CreateIndexOptimize, CreateIndexStage,
    ExplainContext, ExplainPlanContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::explain::explain_dataflow;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext, TimestampProvider};

impl Staged for CreateIndexStage {
    type Ctx = ExecuteContext;

    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Optimize(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
            Self::Explain(stage) => &mut stage.validity,
        }
    }

    async fn stage(
        self,
        coord: &mut Coordinator,
        ctx: &mut ExecuteContext,
    ) -> Result<StageResult<Box<Self>>, AdapterError> {
        match self {
            CreateIndexStage::Optimize(stage) => coord.create_index_optimize(stage).await,
            CreateIndexStage::Finish(stage) => {
                coord.create_index_finish(ctx.session(), stage).await
            }
            CreateIndexStage::Explain(stage) => {
                coord.create_index_explain(ctx.session(), stage).await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateIndexStageReady {
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
    #[instrument]
    pub(crate) async fn sequence_create_index(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateIndexPlan,
        resolved_ids: ResolvedIds,
    ) {
        let stage = return_if_err!(
            self.create_index_validate(plan, resolved_ids, ExplainContext::None),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_create_index(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::Statement(stmt) = explainee else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };
        let plan::ExplaineeStatement::CreateIndex { broken, plan } = stmt else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        // Not used in the EXPLAIN path so it's OK to generate a dummy value.
        let resolved_ids = ResolvedIds(Default::default());

        let explain_ctx = ExplainContext::Plan(ExplainPlanContext {
            broken,
            config,
            format,
            stage,
            replan: None,
            desc: None,
            optimizer_trace,
        });
        let stage = return_if_err!(
            self.create_index_validate(plan, resolved_ids, explain_ctx),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_replan_index(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::ReplanIndex(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::Index(item) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };

        let create_sql = item.create_sql.clone();
        let plan_result = self
            .catalog_mut()
            .deserialize_plan_with_enable_for_item_parsing(&create_sql, true);
        let (plan, resolved_ids) = return_if_err!(plan_result, ctx);

        let plan::Plan::CreateIndex(plan) = plan else {
            unreachable!() // We are parsing the `create_sql` of an `Index` item.
        };

        // It is safe to assume that query optimization will always succeed, so
        // for now we statically assume `broken = false`.
        let broken = false;

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        let explain_ctx = ExplainContext::Plan(ExplainPlanContext {
            broken,
            config,
            format,
            stage,
            replan: Some(id),
            desc: None,
            optimizer_trace,
        });
        let stage = return_if_err!(
            self.create_index_validate(plan, resolved_ids, explain_ctx),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_index(
        &mut self,
        ctx: &ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::Explainee::Index(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let cluster_id = if let CatalogItem::Index(index) = self.catalog().get_entry(&id).item() {
            index.cluster_id
        } else {
            unreachable!(); // Asserted in `plan_explain_plan`.
        };

        // TODO(mgree): calculate statistics (need a timestamp)
        let timeline_context = self.get_timeline_context(id);
        let stats_when = &plan::QueryWhen::Immediately;

        let id_bundle = self
            .dataflow_builder(cluster_id)
            .sufficient_collections(std::iter::once(&id));

        let stats_ts = self.determine_timestamp_for(
            &ctx.session,
            &id_bundle,
            stats_when,
            cluster_id,
            &timeline_context,
            None,
            None,
            &mz_sql::session::vars::IsolationLevel::Serializable,
        );

        let CatalogItem::Index(index) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };
        // ??? is this the right way to do this?
        let source_ids = self.catalog().get_entry(&id).uses();

        let cardinality_stats = match stats_ts {
            Ok((ts, _read_holds)) => {
                eprintln!(
                    "MGREE stats_ts.respond_immediately = {}",
                    ts.respond_immediately()
                );
                let timestamp_context = ts.timestamp_context;

                let stats = self
                    .statistics_oracle(
                        &ctx.session,
                        &source_ids,
                        &timestamp_context.antichain(),
                        true,
                    )
                    .await
                    .unwrap_or_else(|e| {
                        eprintln!("MGREE statistics_oracle = Err({e:?})");
                        Box::new(mz_transform::EmptyStatisticsOracle)
                    });

                stats.as_map()
            }
            Err(e) => {
                eprintln!("MGREE stats_ts = Err({e:?})");
                BTreeMap::new()
            }
        };

        let Some(dataflow_metainfo) = self.catalog().try_get_dataflow_metainfo(&id) else {
            if !id.is_system() {
                tracing::error!("cannot find dataflow metainformation for index {id} in catalog");
            }
            coord_bail!("cannot find dataflow metainformation for index {id} in catalog");
        };

        let target_cluster = self.catalog().get_cluster(index.cluster_id);

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features())
            .override_from(&config.features);

        let explain = match stage {
            ExplainStage::GlobalPlan => {
                let Some(plan) = self.catalog().try_get_optimized_plan(&id).cloned() else {
                    tracing::error!("cannot find {stage} for index {id} in catalog");
                    coord_bail!("cannot find {stage} for index in catalog");
                };

                explain_dataflow(
                    plan,
                    format,
                    &config,
                    &features,
                    &self.catalog().for_session(ctx.session()),
                    cardinality_stats,
                    Some(target_cluster.name.as_str()),
                    dataflow_metainfo,
                )?
            }
            ExplainStage::PhysicalPlan => {
                let Some(plan) = self.catalog().try_get_physical_plan(&id).cloned() else {
                    tracing::error!("cannot find {stage} for index {id} in catalog");
                    coord_bail!("cannot find {stage} for index in catalog");
                };
                explain_dataflow(
                    plan,
                    format,
                    &config,
                    &features,
                    &self.catalog().for_session(ctx.session()),
                    cardinality_stats,
                    Some(target_cluster.name.as_str()),
                    dataflow_metainfo,
                )?
            }
            _ => {
                coord_bail!("cannot EXPLAIN {} FOR INDEX", stage);
            }
        };

        let row = Row::pack_slice(&[Datum::from(explain.as_str())]);

        Ok(Self::send_immediate_rows(row))
    }

    // `explain_ctx` is an optional context set iff the state machine is initiated from
    // sequencing an EXPLAIN for this statement.
    #[instrument]
    fn create_index_validate(
        &mut self,
        plan: plan::CreateIndexPlan,
        resolved_ids: ResolvedIds,
        explain_ctx: ExplainContext,
    ) -> Result<CreateIndexStage, AdapterError> {
        let validity =
            PlanValidity::require_transient_revision(self.catalog().transient_revision());
        Ok(CreateIndexStage::Optimize(CreateIndexOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }))
    }

    #[instrument]
    async fn create_index_optimize(
        &mut self,
        CreateIndexOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }: CreateIndexOptimize,
    ) -> Result<StageResult<Box<CreateIndexStage>>, AdapterError> {
        let plan::CreateIndexPlan {
            index: plan::Index { cluster_id, .. },
            ..
        } = &plan;

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");
        let exported_index_id = if let ExplainContext::None = explain_ctx {
            self.catalog_mut().allocate_user_id().await?
        } else {
            self.allocate_transient_id()
        };
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(*cluster_id).config.features())
            .override_from(&explain_ctx);

        // Build an optimizer for this INDEX.
        let mut optimizer = optimize::index::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            exported_index_id,
            optimizer_config,
            self.optimizer_metrics(),
        );
        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create index",
            move || {
                span.in_scope(|| {
                    let mut pipeline = || -> Result<(
                    optimize::index::GlobalMirPlan,
                    optimize::index::GlobalLirPlan,
                ), AdapterError> {
                    let _dispatch_guard = explain_ctx.dispatch_guard();

                    let index_plan =
                        optimize::index::Index::new(&plan.name, &plan.index.on, &plan.index.keys);

                    // MIR ⇒ MIR optimization (global)
                    let global_mir_plan = optimizer.catch_unwind_optimize(index_plan)?;
                    // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                    let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                    Ok((global_mir_plan, global_lir_plan))
                };

                    let stage = match pipeline() {
                        Ok((global_mir_plan, global_lir_plan)) => {
                            if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                                let (_, df_meta) = global_lir_plan.unapply();
                                CreateIndexStage::Explain(CreateIndexExplain {
                                    validity,
                                    exported_index_id,
                                    plan,
                                    df_meta,
                                    explain_ctx,
                                })
                            } else {
                                CreateIndexStage::Finish(CreateIndexFinish {
                                    validity,
                                    exported_index_id,
                                    plan,
                                    resolved_ids,
                                    global_mir_plan,
                                    global_lir_plan,
                                })
                            }
                        }
                        // Internal optimizer errors are handled differently
                        // depending on the caller.
                        Err(err) => {
                            let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                                // In `sequence_~` contexts, immediately error.
                                return Err(err);
                            };

                            if explain_ctx.broken {
                                // In `EXPLAIN BROKEN` contexts, just log the error
                                // and move to the next stage with default
                                // parameters.
                                tracing::error!("error while handling EXPLAIN statement: {}", err);
                                CreateIndexStage::Explain(CreateIndexExplain {
                                    validity,
                                    exported_index_id,
                                    plan,
                                    df_meta: Default::default(),
                                    explain_ctx,
                                })
                            } else {
                                // In regular `EXPLAIN` contexts, immediately error.
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
    async fn create_index_finish(
        &mut self,
        session: &Session,
        CreateIndexFinish {
            exported_index_id,
            plan:
                plan::CreateIndexPlan {
                    name,
                    index:
                        plan::Index {
                            create_sql,
                            on,
                            keys,
                            cluster_id,
                            compaction_window,
                        },
                    if_not_exists,
                },
            resolved_ids,
            global_mir_plan,
            global_lir_plan,
            ..
        }: CreateIndexFinish,
    ) -> Result<StageResult<Box<CreateIndexStage>>, AdapterError> {
        let ops = vec![catalog::Op::CreateItem {
            id: exported_index_id,
            name: name.clone(),
            item: CatalogItem::Index(Index {
                create_sql,
                keys,
                on,
                conn_id: None,
                resolved_ids,
                cluster_id,
                is_retained_metrics_object: false,
                custom_logical_compaction_window: compaction_window,
            }),
            owner_id: *self.catalog().get_entry(&on).owner_id(),
        }];

        // Pre-allocate a vector of transient GlobalIds for each notice.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .take(global_lir_plan.df_meta().optimizer_notices.len())
            .collect::<Vec<_>>();

        let transact_result = self
            .catalog_transact_with_side_effects(Some(session), ops, |coord| async {
                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(exported_index_id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(exported_index_id, global_lir_plan.df_desc().clone());

                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                // Timestamp selection
                let id_bundle = dataflow_import_id_bundle(&df_desc, cluster_id);

                // We're putting in place read holds, such that ship_dataflow,
                // below, which calls update_read_capabilities, can successfully
                // do so. Otherwise, the since of dependencies might move along
                // concurrently, pulling the rug from under us!
                //
                // TODO: Maybe in the future, pass those holds on to compute, to
                // hold on to them and downgrade when possible?
                let read_holds = coord.acquire_read_holds(&id_bundle);
                let since = coord.least_valid_read(&read_holds);
                df_desc.set_as_of(since);

                // Emit notices.
                coord.emit_optimizer_notices(session, &df_meta.optimizer_notices);

                // Return a metainfo with rendered notices.
                let df_meta =
                    coord
                        .catalog()
                        .render_notices(df_meta, notice_ids, Some(exported_index_id));
                coord
                    .catalog_mut()
                    .set_dataflow_metainfo(exported_index_id, df_meta.clone());

                if coord.catalog().state().system_config().enable_mz_notices() {
                    // Initialize a container for builtin table updates.
                    let mut builtin_table_updates =
                        Vec::with_capacity(df_meta.optimizer_notices.len());
                    // Collect optimization hint updates.
                    coord.catalog().state().pack_optimizer_notices(
                        &mut builtin_table_updates,
                        df_meta.optimizer_notices.iter(),
                        1,
                    );
                    // Write collected optimization hints to the builtin tables.
                    let builtin_updates_fut = coord
                        .builtin_table_update()
                        .execute(builtin_table_updates)
                        .await;

                    let ship_dataflow_fut = coord.ship_dataflow(df_desc, cluster_id);

                    futures::future::join(builtin_updates_fut, ship_dataflow_fut).await;
                } else {
                    coord.ship_dataflow(df_desc, cluster_id).await;
                }

                // Drop read holds after the dataflow has been shipped, at which
                // point compute will have put in its own read holds.
                drop(read_holds);

                coord.update_compute_base_read_policy(
                    cluster_id,
                    exported_index_id,
                    compaction_window.unwrap_or_default().into(),
                );
            })
            .await;

        match transact_result {
            Ok(_) => Ok(StageResult::Response(ExecuteResponse::CreatedIndex)),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "index",
                });
                Ok(StageResult::Response(ExecuteResponse::CreatedIndex))
            }
            Err(err) => Err(err),
        }
    }

    #[instrument]
    async fn create_index_explain(
        &mut self,
        session: &Session,
        CreateIndexExplain {
            exported_index_id,
            plan: plan::CreateIndexPlan { name, index, .. },
            df_meta,
            explain_ctx:
                ExplainPlanContext {
                    config,
                    format,
                    stage,
                    optimizer_trace,
                    ..
                },
            ..
        }: CreateIndexExplain,
    ) -> Result<StageResult<Box<CreateIndexStage>>, AdapterError> {
        let session_catalog = self.catalog().for_session(session);
        let expr_humanizer = {
            let on_entry = self.catalog.get_entry(&index.on);
            let full_name = self.catalog.resolve_full_name(&name, on_entry.conn_id());
            let on_desc = on_entry
                .desc(&full_name)
                .expect("can only create indexes on items with a valid description");

            let transient_items = btreemap! {
                exported_index_id => TransientItem::new(
                    Some(full_name.into_parts()),
                    Some(on_desc.iter_names().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let target_cluster = self.catalog().get_cluster(index.cluster_id);

        let features = OptimizerFeatures::from(self.catalog().system_config())
            .override_from(&target_cluster.config.features())
            .override_from(&config.features);

        let rows = optimizer_trace
            .into_rows(
                format,
                &config,
                &features,
                &expr_humanizer,
                None,
                Some(target_cluster),
                df_meta,
                stage,
                plan::ExplaineeStatementKind::CreateIndex,
                None,
            )
            .await?;

        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }
}
