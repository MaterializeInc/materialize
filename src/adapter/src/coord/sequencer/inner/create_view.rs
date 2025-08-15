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
use mz_catalog::memory::objects::{CatalogItem, View};
use mz_expr::CollectionPlan;
use mz_ore::instrument;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::optimize::{OptimizerFeatures, OverrideFrom};
use mz_repr::{Datum, RelationDesc, Row};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{self};
use mz_sql::session::metadata::SessionMetadata;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateViewExplain, CreateViewFinish, CreateViewOptimize, CreateViewStage,
    ExplainContext, ExplainPlanContext, Message, PlanValidity, StageResult, Staged,
};
use crate::error::AdapterError;
use crate::explain::explain_plan;
use crate::explain::optimizer_trace::OptimizerTrace;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{AdapterNotice, ExecuteContext, catalog};

impl Staged for CreateViewStage {
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
            CreateViewStage::Optimize(stage) => coord.create_view_optimize(stage).await,
            CreateViewStage::Finish(stage) => coord.create_view_finish(ctx.session(), stage).await,
            CreateViewStage::Explain(stage) => {
                coord.create_view_explain(ctx.session(), stage).await
            }
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateViewStageReady {
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
    pub(crate) async fn sequence_create_view(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) {
        let stage = return_if_err!(
            self.create_view_validate(plan, resolved_ids, ExplainContext::None),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_create_view(
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
        let plan::ExplaineeStatement::CreateView { broken, plan } = stmt else {
            // This is currently asserted in the `sequence_explain_plan` code that
            // calls this method.
            unreachable!()
        };

        // Create an OptimizerTrace instance to collect plans emitted when
        // executing the optimizer pipeline.
        let optimizer_trace = OptimizerTrace::new(stage.paths());

        // Not used in the EXPLAIN path so it's OK to generate a dummy value.
        let resolved_ids = ResolvedIds::empty();

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
            self.create_view_validate(plan, resolved_ids, explain_ctx),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) async fn explain_replan_view(
        &mut self,
        ctx: ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) {
        let plan::Explainee::ReplanView(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::View(item) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };
        let gid = item.global_id();

        let create_sql = item.create_sql.clone();
        let plan_result = self
            .catalog_mut()
            .deserialize_plan_with_enable_for_item_parsing(&create_sql, true);
        let (plan, resolved_ids) = return_if_err!(plan_result, ctx);

        let plan::Plan::CreateView(plan) = plan else {
            unreachable!() // We are parsing the `create_sql` of a `View` item.
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
            replan: Some(gid),
            desc: None,
            optimizer_trace,
        });
        let stage = return_if_err!(
            self.create_view_validate(plan, resolved_ids, explain_ctx),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    pub(crate) fn explain_view(
        &mut self,
        ctx: &ExecuteContext,
        plan::ExplainPlanPlan {
            stage,
            format,
            config,
            explainee,
        }: plan::ExplainPlanPlan,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::Explainee::View(id) = explainee else {
            unreachable!() // Asserted in `sequence_explain_plan`.
        };
        let CatalogItem::View(view) = self.catalog().get_entry(&id).item() else {
            unreachable!() // Asserted in `plan_explain_plan`.
        };

        let target_cluster = None; // Views don't have a target cluster.

        let features =
            OptimizerFeatures::from(self.catalog().system_config()).override_from(&config.features);

        let cardinality_stats = BTreeMap::new();

        let explain = match stage {
            ExplainStage::RawPlan => explain_plan(
                view.raw_expr.as_ref().clone(),
                format,
                &config,
                &features,
                &self.catalog().for_session(ctx.session()),
                cardinality_stats,
                target_cluster,
            )?,
            ExplainStage::LocalPlan => explain_plan(
                view.optimized_expr.as_inner().clone(),
                format,
                &config,
                &features,
                &self.catalog().for_session(ctx.session()),
                cardinality_stats,
                target_cluster,
            )?,
            _ => {
                coord_bail!("cannot EXPLAIN {} FOR VIEW", stage);
            }
        };

        let row = Row::pack_slice(&[Datum::from(explain.as_str())]);

        Ok(Self::send_immediate_rows(row))
    }

    #[instrument]
    fn create_view_validate(
        &mut self,
        plan: plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
        // An optional context set iff the state machine is initiated from
        // sequencing an EXPLAIN for this statement.
        explain_ctx: ExplainContext,
    ) -> Result<CreateViewStage, AdapterError> {
        let plan::CreateViewPlan {
            view: plan::View { expr, .. },
            ambiguous_columns,
            ..
        } = &plan;

        // Validate any references in the view's expression. We do this on the
        // unoptimized plan to better reflect what the user typed. We want to
        // reject queries that depend on a relation in the wrong timeline, for
        // example, even if we can *technically* optimize that reference away.
        let expr_depends_on = expr.depends_on();
        self.catalog()
            .validate_timeline_context(expr_depends_on.iter().copied())?;
        self.validate_system_column_references(*ambiguous_columns, &expr_depends_on)?;

        let validity =
            PlanValidity::require_transient_revision(self.catalog().transient_revision());

        Ok(CreateViewStage::Optimize(CreateViewOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }))
    }

    #[instrument]
    async fn create_view_optimize(
        &mut self,
        CreateViewOptimize {
            validity,
            plan,
            resolved_ids,
            explain_ctx,
        }: CreateViewOptimize,
    ) -> Result<StageResult<Box<CreateViewStage>>, AdapterError> {
        let id_ts = self.get_catalog_write_ts().await;
        let (item_id, global_id) = self.catalog_mut().allocate_user_id(id_ts).await?;

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&explain_ctx);

        // Build an optimizer for this VIEW.
        let mut optimizer =
            optimize::view::Optimizer::new(optimizer_config, Some(self.optimizer_metrics()));

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create view",
            move || {
                span.in_scope(|| {
                    let mut pipeline =
                        || -> Result<mz_expr::OptimizedMirRelationExpr, AdapterError> {
                            let _dispatch_guard = explain_ctx.dispatch_guard();

                            // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
                            let raw_expr = plan.view.expr.clone();
                            let optimized_expr = optimizer.catch_unwind_optimize(raw_expr)?;

                            Ok(optimized_expr)
                        };

                    let stage = match pipeline() {
                        Ok(optimized_expr) => {
                            if let ExplainContext::Plan(explain_ctx) = explain_ctx {
                                CreateViewStage::Explain(CreateViewExplain {
                                    validity,
                                    id: global_id,
                                    plan,
                                    explain_ctx,
                                })
                            } else {
                                CreateViewStage::Finish(CreateViewFinish {
                                    validity,
                                    item_id,
                                    global_id,
                                    plan,
                                    optimized_expr,
                                    resolved_ids,
                                })
                            }
                        }
                        // Internal optimizer errors are handled differently
                        // depending on the caller.
                        Err(err) => {
                            let ExplainContext::Plan(explain_ctx) = explain_ctx else {
                                // In `sequence_~` contexts, immediately return the error.
                                return Err(err);
                            };

                            if explain_ctx.broken {
                                // In `EXPLAIN BROKEN` contexts, just log the error
                                // and move to the next stage with default
                                // parameters.
                                tracing::error!("error while handling EXPLAIN statement: {}", err);
                                CreateViewStage::Explain(CreateViewExplain {
                                    validity,
                                    id: global_id,
                                    plan,
                                    explain_ctx,
                                })
                            } else {
                                // In regular `EXPLAIN` contexts, immediately return the error.
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
    async fn create_view_finish(
        &mut self,
        session: &Session,
        CreateViewFinish {
            item_id,
            global_id,
            plan:
                plan::CreateViewPlan {
                    name,
                    view:
                        plan::View {
                            create_sql,
                            expr: raw_expr,
                            dependencies,
                            column_names,
                            temporary,
                        },
                    drop_ids,
                    if_not_exists,
                    ..
                },
            optimized_expr,
            resolved_ids,
            ..
        }: CreateViewFinish,
    ) -> Result<StageResult<Box<CreateViewStage>>, AdapterError> {
        let ops = vec![
            catalog::Op::DropObjects(
                drop_ids
                    .iter()
                    .map(|id| catalog::DropObjectInfo::Item(*id))
                    .collect(),
            ),
            catalog::Op::CreateItem {
                id: item_id,
                name: name.clone(),
                item: CatalogItem::View(View {
                    create_sql: create_sql.clone(),
                    global_id,
                    raw_expr: raw_expr.into(),
                    desc: RelationDesc::new(optimized_expr.typ(), column_names.clone()),
                    optimized_expr: optimized_expr.into(),
                    conn_id: if temporary {
                        Some(session.conn_id().clone())
                    } else {
                        None
                    },
                    resolved_ids: resolved_ids.clone(),
                    dependencies: dependencies.clone(),
                }),
                owner_id: *session.current_role_id(),
            },
        ];

        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(StageResult::Response(ExecuteResponse::CreatedView)),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "view",
                });
                Ok(StageResult::Response(ExecuteResponse::CreatedView))
            }
            Err(err) => Err(err),
        }
    }

    #[instrument]
    async fn create_view_explain(
        &mut self,
        session: &Session,
        CreateViewExplain {
            id,
            plan:
                plan::CreateViewPlan {
                    name,
                    view: plan::View { column_names, .. },
                    ..
                },
            explain_ctx:
                ExplainPlanContext {
                    config,
                    format,
                    stage,
                    optimizer_trace,
                    ..
                },
            ..
        }: CreateViewExplain,
    ) -> Result<StageResult<Box<CreateViewStage>>, AdapterError> {
        let session_catalog = self.catalog().for_session(session);
        let expr_humanizer = {
            let full_name = self.catalog().resolve_full_name(&name, None);
            let transient_items = btreemap! {
                id => TransientItem::new(
                    Some(full_name.into_parts()),
                    Some(column_names.iter().map(|c| c.to_string()).collect()),
                )
            };
            ExprHumanizerExt::new(transient_items, &session_catalog)
        };

        let features =
            OptimizerFeatures::from(self.catalog().system_config()).override_from(&config.features);

        let rows = optimizer_trace
            .into_rows(
                format,
                &config,
                &features,
                &expr_humanizer,
                None,
                None, // Views don't have a target cluster.
                Default::default(),
                stage,
                plan::ExplaineeStatementKind::CreateView,
                None,
            )
            .await?;

        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }
}
