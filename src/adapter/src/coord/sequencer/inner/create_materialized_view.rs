// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use maplit::btreemap;
use mz_adapter_types::compaction::CompactionWindow;
use mz_catalog::memory::objects::{CatalogItem, MaterializedView};
use mz_expr::refresh_schedule::RefreshSchedule;
use mz_expr::CollectionPlan;
use mz_ore::collections::CollectionExt;
use mz_ore::instrument;
use mz_ore::soft_panic_or_log;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::Datum;
use mz_repr::Row;
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;
use mz_sql_parser::ast;
use mz_sql_parser::ast::display::AstDisplay;
use mz_storage_client::controller::{CollectionDescription, DataSource, DataSourceOther};
use timely::progress::Antichain;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateMaterializedViewExplain, CreateMaterializedViewFinish,
    CreateMaterializedViewOptimize, CreateMaterializedViewStage, ExplainContext,
    ExplainPlanContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::explain::explain_dataflow;
use crate::explain::explain_plan;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::dataflows::dataflow_import_id_bundle;
use crate::optimize::{self, Optimize, OverrideFrom};
use crate::session::Session;
use crate::util::ResultExt;
use crate::{catalog, AdapterNotice, CollectionIdBundle, ExecuteContext, TimestampProvider};

impl Staged for CreateMaterializedViewStage {
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
            CreateMaterializedViewStage::Optimize(stage) => {
                coord.create_materialized_view_optimize(stage).await
            }
            CreateMaterializedViewStage::Finish(stage) => {
                coord
                    .create_materialized_view_finish(ctx.session(), stage)
                    .await
            }
            CreateMaterializedViewStage::Explain(stage) => {
                coord.create_materialized_view_explain(ctx.session(), stage)
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateMaterializedViewStageReady {
            ctx,
            span,
            stage: self,
        }
    }
}

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
    ) {
        let stage = return_if_err!(
            self.create_materialized_view_validate(
                ctx.session(),
                plan,
                resolved_ids,
                ExplainContext::None
            ),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_create_materialized_view(
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
        let plan::ExplaineeStatement::CreateMaterializedView { broken, plan } = stmt else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(broken, stage.path());

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
            self.create_materialized_view_validate(ctx.session(), plan, resolved_ids, explain_ctx),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_replan_materialized_view(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::ReplanMaterializedView(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::MaterializedView(item) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };

        let state = self.catalog().state();
        let plan_result = state.deserialize_plan(id, item.create_sql.clone(), true);
        let (plan, resolved_ids) = return_if_err!(plan_result, ctx);

        let plan::Plan::CreateMaterializedView(plan) = plan else {
            unreachable!() // We are parsing the `create_sql` of a `MaterializedView` item.
        };

        // It is safe to assume that query optimization will always succeed, so
        // for now we statically assume `broken = false`.
        let broken = false;

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(broken, stage.path());

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
            self.create_materialized_view_validate(ctx.session(), plan, resolved_ids, explain_ctx,),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(super) fn explain_materialized_view(
        &mut self,
        ctx: &ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::Explainee::MaterializedView(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::MaterializedView(view) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };

        let Some(dataflow_metainfo) = self.catalog().try_get_dataflow_metainfo(&id) else {
            tracing::error!(
                "cannot find dataflow metainformation for materialized view {id} in catalog"
            );
            coord_bail!(
                "cannot find dataflow metainformation for materialized view {id} in catalog"
            );
        };

        let explain = match stage {
            ExplainStage::RawPlan => explain_plan(
                view.raw_expr.clone(),
                format,
                &config,
                &self.catalog().for_session(ctx.session()),
            )?,
            ExplainStage::LocalPlan => explain_plan(
                view.optimized_expr.as_inner().clone(),
                format,
                &config,
                &self.catalog().for_session(ctx.session()),
            )?,
            ExplainStage::GlobalPlan => {
                let Some(plan) = self.catalog().try_get_optimized_plan(&id).cloned() else {
                    tracing::error!("cannot find {stage} for materialized view {id} in catalog");
                    coord_bail!("cannot find {stage} for materialized view in catalog");
                };
                explain_dataflow(
                    plan,
                    format,
                    &config,
                    &self.catalog().for_session(ctx.session()),
                    dataflow_metainfo,
                )?
            }
            ExplainStage::PhysicalPlan => {
                let Some(plan) = self.catalog().try_get_physical_plan(&id).cloned() else {
                    tracing::error!("cannot find {stage} for materialized view {id} in catalog");
                    coord_bail!("cannot find {stage} for materialized view in catalog");
                };
                explain_dataflow(
                    plan,
                    format,
                    &config,
                    &self.catalog().for_session(ctx.session()),
                    dataflow_metainfo,
                )?
            }
            _ => {
                coord_bail!("cannot EXPLAIN {} FOR MATERIALIZED VIEW", stage);
            }
        };

        let rows = vec![Row::pack_slice(&[Datum::from(explain.as_str())])];

        Ok(Self::send_immediate_rows(rows))
    }

    #[instrument]
    fn create_materialized_view_validate(
        &mut self,
        session: &Session,
        plan: plan::CreateMaterializedViewPlan,
        resolved_ids: ResolvedIds,
        // An optional context set iff the state machine is initiated from
        // sequencing an EXPLAIN for this statement.
        explain_ctx: ExplainContext,
    ) -> Result<CreateMaterializedViewStage, AdapterError> {
        let plan::CreateMaterializedViewPlan {
            materialized_view:
                plan::MaterializedView {
                    expr,
                    cluster_id,
                    refresh_schedule,
                    ..
                },
            ambiguous_columns,
            ..
        } = &plan;

        // Validate any references in the materialized view's expression. We do
        // this on the unoptimized plan to better reflect what the user typed.
        // We want to reject queries that depend on log sources, for example,
        // even if we can *technically* optimize that reference away.
        let expr_depends_on = expr.depends_on();
        self.validate_timeline_context(expr_depends_on.iter().cloned())?;
        self.validate_system_column_references(*ambiguous_columns, &expr_depends_on)?;
        // Materialized views are not allowed to depend on log sources, as replicas
        // are not producing the same definite collection for these.
        // TODO(teskje): Remove this check once arrangement-based log sources
        // are replaced with persist-based ones.
        let log_names = expr_depends_on
            .iter()
            .flat_map(|id| self.catalog().introspection_dependencies(*id))
            .map(|id| self.catalog().get_entry(&id).name().item.clone())
            .collect::<Vec<_>>();
        if !log_names.is_empty() {
            return Err(AdapterError::InvalidLogDependency {
                object_type: "materialized view".into(),
                log_names,
            });
        }

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: expr_depends_on.clone(),
            cluster_id: Some(*cluster_id),
            replica_id: None,
            role_metadata: session.role_metadata().clone(),
        };

        // Acquire read holds at all the REFRESH AT times.
        // Note that we already acquired a possibly non-precise read hold at mz_now() in the purification,
        // if any of the REFRESH options involve mz_now(). But now we can acquire precise read holds, because by now
        // the REFRESH AT expressions have been evaluated, so we can handle something like
        // `mz_now()::text::int8 + 10000`;
        if let Some(refresh_schedule) = refresh_schedule {
            if !refresh_schedule.ats.is_empty() && matches!(explain_ctx, ExplainContext::None) {
                let ids = self
                    .index_oracle(*cluster_id)
                    .sufficient_collections(resolved_ids.0.iter());
                for refresh_at_ts in &refresh_schedule.ats {
                    match self.acquire_read_holds_auto_cleanup(session, *refresh_at_ts, &ids, true)
                    {
                        Ok(()) => {}
                        Err(earliest_possible) => {
                            return Err(AdapterError::InputNotReadableAtRefreshAtTime(
                                *refresh_at_ts,
                                earliest_possible,
                            ));
                        }
                    };
                }
            }
        }

        Ok(CreateMaterializedViewStage::Optimize(
            CreateMaterializedViewOptimize {
                validity,
                plan,
                resolved_ids,
                explain_ctx,
            },
        ))
    }

    #[instrument]
    async fn create_materialized_view_optimize(
        &mut self,
        CreateMaterializedViewOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }: CreateMaterializedViewOptimize,
    ) -> Result<StageResult<Box<CreateMaterializedViewStage>>, AdapterError> {
        let plan::CreateMaterializedViewPlan {
            name,
            materialized_view:
                plan::MaterializedView {
                    column_names,
                    cluster_id,
                    non_null_assertions,
                    refresh_schedule,
                    ..
                },
            ..
        } = &plan;

        // Collect optimizer parameters.
        let compute_instance = self
            .instance_snapshot(*cluster_id)
            .expect("compute instance does not exist");
        let exported_sink_id = if let ExplainContext::None = explain_ctx {
            self.catalog_mut().allocate_user_id().await?
        } else {
            self.allocate_transient_id()?
        };
        let internal_view_id = self.allocate_transient_id()?;
        let debug_name = self.catalog().resolve_full_name(name, None).to_string();
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&self.catalog.get_cluster(*cluster_id).config.features())
            .override_from(&explain_ctx);

        // Build an optimizer for this MATERIALIZED VIEW.
        let mut optimizer = optimize::materialized_view::Optimizer::new(
            self.owned_catalog(),
            compute_instance,
            exported_sink_id,
            internal_view_id,
            column_names.clone(),
            non_null_assertions.clone(),
            refresh_schedule.clone(),
            debug_name,
            optimizer_config,
        );

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create materialized view",
            move || {
                span.in_scope(|| {
                    let mut pipeline = || -> Result<(
                        optimize::materialized_view::LocalMirPlan,
                        optimize::materialized_view::GlobalMirPlan,
                        optimize::materialized_view::GlobalLirPlan,
                    ), AdapterError> {
                        let _dispatch_guard = explain_ctx.dispatch_guard();

                        let raw_expr = plan.materialized_view.expr.clone();

                        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local and global)
                        let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                        let global_mir_plan = optimizer.catch_unwind_optimize(local_mir_plan.clone())?;
                        // MIR ⇒ LIR lowering and LIR ⇒ LIR optimization (global)
                        let global_lir_plan = optimizer.catch_unwind_optimize(global_mir_plan.clone())?;

                        Ok((local_mir_plan, global_mir_plan, global_lir_plan))
                    };

                    let stage = match pipeline() {
                        Ok((local_mir_plan, global_mir_plan, global_lir_plan)) => {
                            if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                                let (_, df_meta) = global_lir_plan.unapply();
                                CreateMaterializedViewStage::Explain(
                                    CreateMaterializedViewExplain {
                                        validity,
                                        exported_sink_id,
                                        plan,
                                        df_meta,
                                        explain_ctx,
                                    },
                                )
                            } else {
                                CreateMaterializedViewStage::Finish(CreateMaterializedViewFinish {
                                    validity,
                                    exported_sink_id,
                                    plan,
                                    resolved_ids,
                                    local_mir_plan,
                                    global_mir_plan,
                                    global_lir_plan,
                                })
                            }
                        }
                        // Internal optimizer errors are handled differently
                        // depending on the caller.
                        Err(err) => {
                            let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                                // In `sequence_~` contexts, immediately return the error.
                                return Err(err.into());
                            };

                            if explain_ctx.broken {
                                // In `EXPLAIN BROKEN` contexts, just log the error
                                // and move to the next stage with default
                                // parameters.
                                tracing::error!("error while handling EXPLAIN statement: {}", err);
                                CreateMaterializedViewStage::Explain(
                                    CreateMaterializedViewExplain {
                                        validity,
                                        exported_sink_id,
                                        plan,
                                        df_meta: Default::default(),
                                        explain_ctx,
                                    },
                                )
                            } else {
                                // In regular `EXPLAIN` contexts, immediately return the error.
                                return Err(err.into());
                            }
                        }
                    };

                    Ok(Box::new(stage))
                })
            },
        )))
    }

    #[instrument]
    async fn create_materialized_view_finish(
        &mut self,
        session: &Session,
        CreateMaterializedViewFinish {
            exported_sink_id,
            plan:
                plan::CreateMaterializedViewPlan {
                    name,
                    materialized_view:
                        plan::MaterializedView {
                            mut create_sql,
                            expr: raw_expr,
                            cluster_id,
                            non_null_assertions,
                            compaction_window,
                            refresh_schedule,
                            ..
                        },
                    drop_ids,
                    if_not_exists,
                    ..
                },
            resolved_ids,
            local_mir_plan,
            global_mir_plan,
            global_lir_plan,
            ..
        }: CreateMaterializedViewFinish,
    ) -> Result<StageResult<Box<CreateMaterializedViewStage>>, AdapterError> {
        // Timestamp selection
        let id_bundle = dataflow_import_id_bundle(global_lir_plan.df_desc(), cluster_id);
        let (as_of, until) = self.select_timestamps(id_bundle, refresh_schedule.as_ref())?;

        // Update the `create_sql` with the selected `as_of`. This is how we make sure the `as_of`
        // is persisted to the catalog and can be relied on during bootstrapping.
        if let Some(as_of_ts) = as_of.as_option() {
            let stmt = mz_sql::parse::parse(&create_sql)
                .expect("create_sql is valid")
                .into_element()
                .ast;
            let ast::Statement::CreateMaterializedView(mut stmt) = stmt else {
                panic!("unexpected statement type");
            };
            stmt.as_of = Some(as_of_ts.into());
            create_sql = stmt.to_ast_string_stable();
        }

        let ops = itertools::chain(
            drop_ids
                .into_iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(id))),
            std::iter::once(catalog::Op::CreateItem {
                id: exported_sink_id,
                oid: self.catalog_mut().allocate_oid()?,
                name: name.clone(),
                item: CatalogItem::MaterializedView(MaterializedView {
                    create_sql,
                    raw_expr,
                    optimized_expr: local_mir_plan.expr(),
                    desc: global_lir_plan.desc().clone(),
                    resolved_ids,
                    cluster_id,
                    non_null_assertions,
                    custom_logical_compaction_window: compaction_window,
                    refresh_schedule,
                    initial_as_of: Some(as_of.clone()),
                }),
                owner_id: *session.current_role_id(),
            }),
        )
        .collect::<Vec<_>>();

        // Pre-allocate a vector of transient GlobalIds for each notice.
        let notice_ids = std::iter::repeat_with(|| self.allocate_transient_id())
            .take(global_lir_plan.df_meta().optimizer_notices.len())
            .collect::<Result<Vec<_>, _>>()?;

        let transact_result = self
            .catalog_transact_with_side_effects(Some(session), ops, |coord| async {
                // Save plan structures.
                coord
                    .catalog_mut()
                    .set_optimized_plan(exported_sink_id, global_mir_plan.df_desc().clone());
                coord
                    .catalog_mut()
                    .set_physical_plan(exported_sink_id, global_lir_plan.df_desc().clone());

                let output_desc = global_lir_plan.desc().clone();
                let (mut df_desc, df_meta) = global_lir_plan.unapply();

                df_desc.set_as_of(as_of.clone());
                df_desc.until = until;

                // Emit notices.
                coord.emit_optimizer_notices(session, &df_meta.optimizer_notices);

                // Return a metainfo with rendered notices.
                let df_meta =
                    coord
                        .catalog()
                        .render_notices(df_meta, notice_ids, Some(exported_sink_id));
                coord
                    .catalog_mut()
                    .set_dataflow_metainfo(exported_sink_id, df_meta.clone());

                // Announce the creation of the materialized view source.
                coord
                    .controller
                    .storage
                    .create_collections(
                        None,
                        vec![(
                            exported_sink_id,
                            CollectionDescription {
                                desc: output_desc,
                                data_source: DataSource::Other(DataSourceOther::Compute),
                                since: Some(as_of),
                                status_collection_id: None,
                            },
                        )],
                    )
                    .await
                    .unwrap_or_terminate("cannot fail to append");

                coord
                    .initialize_storage_read_policies(
                        vec![exported_sink_id],
                        compaction_window.unwrap_or(CompactionWindow::Default),
                    )
                    .await;

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

                    let ((), ()) =
                        futures::future::join(builtin_updates_fut, ship_dataflow_fut).await;
                } else {
                    coord.ship_dataflow(df_desc, cluster_id).await;
                }
            })
            .await;

        match transact_result {
            Ok(_) => Ok(ExecuteResponse::CreatedMaterializedView),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "materialized view",
                    });
                Ok(ExecuteResponse::CreatedMaterializedView)
            }
            Err(err) => Err(err),
        }
        .map(StageResult::Response)
    }

    /// Select the initial `as_of` and `until` frontiers for a materialized view.
    fn select_timestamps(
        &self,
        id_bundle: CollectionIdBundle,
        refresh_schedule: Option<&RefreshSchedule>,
    ) -> Result<(Antichain<mz_repr::Timestamp>, Antichain<mz_repr::Timestamp>), AdapterError> {
        // Normally, `as_of` should be the least_valid_read.
        let mut as_of = self.least_valid_read(&id_bundle);

        // But for MVs with non-trivial REFRESH schedules, it's important to set the `as_of` to the
        // first refresh. This is because we'd like queries on the MV to block until the first
        // refresh (rather than to show an empty MV).
        if let Some(refresh_schedule) = &refresh_schedule {
            if let Some(as_of_ts) = as_of.as_option() {
                if let Some(rounded_up_ts) = refresh_schedule.round_up_timestamp(*as_of_ts) {
                    as_of = Antichain::from_elem(rounded_up_ts);
                } else {
                    let last_refresh = refresh_schedule.last_refresh().expect(
                        "if round_up_timestamp returned None, then there should be a last refresh",
                    );
                    return Err(AdapterError::MaterializedViewWouldNeverRefresh(
                        last_refresh,
                        *as_of_ts,
                    ));
                }
            } else {
                // The `as_of` should never be empty, because then the MV would be unreadable.
                soft_panic_or_log!("creating a materialized view with an empty `as_of`");
            }
        }

        // If we have a refresh schedule that has a last refresh, then set the `until` to the last refresh.
        // (If the `try_step_forward` fails, then no need to set an `until`, because it's not possible to get any data
        // beyond that last refresh time, because there are no times beyond that time.)
        let until_ts = refresh_schedule
            .and_then(|s| s.last_refresh())
            .and_then(|r| r.try_step_forward());
        let until = Antichain::from_iter(until_ts);

        Ok((as_of, until))
    }

    #[instrument]
    fn create_materialized_view_explain(
        &mut self,
        session: &Session,
        CreateMaterializedViewExplain {
            exported_sink_id,
            plan:
                plan::CreateMaterializedViewPlan {
                    name,
                    materialized_view: plan::MaterializedView { column_names, .. },
                    ..
                },
            df_meta,
            explain_ctx:
                ExplainPlanContext {
                    broken,
                    config,
                    format,
                    stage,
                    optimizer_trace,
                    ..
                },
            ..
        }: CreateMaterializedViewExplain,
    ) -> Result<StageResult<Box<CreateMaterializedViewStage>>, AdapterError> {
        let session_catalog = self.catalog().for_session(session);
        let expr_humanizer = {
            let full_name = self.catalog().resolve_full_name(&name, None);
            let transient_items = btreemap! {
                exported_sink_id => TransientItem::new(
                    Some(full_name.to_string()),
                    Some(full_name.item.to_string()),
                    Some(column_names.iter().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let rows = optimizer_trace.into_rows(
            format,
            &config,
            &expr_humanizer,
            None,
            df_meta,
            stage,
            plan::ExplaineeStatementKind::CreateMaterializedView,
        )?;

        if broken {
            tracing_core::callsite::rebuild_interest_cache();
        }

        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }
}
