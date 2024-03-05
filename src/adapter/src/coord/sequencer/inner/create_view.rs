// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use maplit::btreemap;
use mz_catalog::memory::objects::{CatalogItem, View};
use mz_expr::CollectionPlan;
use mz_ore::instrument;
use mz_repr::explain::{ExprHumanizerExt, TransientItem};
use mz_repr::{Datum, RelationDesc, Row};
use mz_sql::ast::ExplainStage;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
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
use crate::optimize::{self, Optimize, OverrideFrom};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext};

impl Staged for CreateViewStage {
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
            CreateViewStage::Explain(stage) => coord.create_view_explain(ctx.session(), stage),
        }
    }

    fn message(self, ctx: ExecuteContext, span: Span) -> Message {
        Message::CreateViewStageReady {
            ctx,
            span,
            stage: self,
        }
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
            self.create_view_validate(ctx.session(), plan, resolved_ids, ExplainContext::None),
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
            self.create_view_validate(ctx.session(), plan, resolved_ids, explain_ctx),
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

        let state = self.catalog().state();
        let plan_result = state.deserialize_plan(id, item.create_sql.clone(), true);
        let (plan, resolved_ids) = return_if_err!(plan_result, ctx);

        let plan::Plan::CreateView(plan) = plan else {
            unreachable!() // We are parsing the `create_sql` of a `View` item.
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
            self.create_view_validate(ctx.session(), plan, resolved_ids, explain_ctx),
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
            _ => {
                coord_bail!("cannot EXPLAIN {} FOR VIEW", stage);
            }
        };

        let rows = vec![Row::pack_slice(&[Datum::from(explain.as_str())])];

        Ok(Self::send_immediate_rows(rows))
    }

    #[instrument]
    fn create_view_validate(
        &mut self,
        session: &Session,
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
        self.validate_timeline_context(expr_depends_on.iter().copied())?;
        self.validate_system_column_references(*ambiguous_columns, &expr_depends_on)?;

        let validity = PlanValidity {
            transient_revision: self.catalog().transient_revision(),
            dependency_ids: expr_depends_on.clone(),
            cluster_id: None,
            replica_id: None,
            role_metadata: session.role_metadata().clone(),
        };

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
        let id = self.catalog_mut().allocate_user_id().await?;

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config())
            .override_from(&explain_ctx);

        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config);

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
                                    id,
                                    plan,
                                    explain_ctx,
                                })
                            } else {
                                CreateViewStage::Finish(CreateViewFinish {
                                    validity,
                                    id,
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
                                return Err(err.into());
                            };

                            if explain_ctx.broken {
                                // In `EXPLAIN BROKEN` contexts, just log the error
                                // and move to the next stage with default
                                // parameters.
                                tracing::error!("error while handling EXPLAIN statement: {}", err);
                                CreateViewStage::Explain(CreateViewExplain {
                                    validity,
                                    id,
                                    plan,
                                    explain_ctx,
                                })
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
    async fn create_view_finish(
        &mut self,
        session: &Session,
        CreateViewFinish {
            id,
            plan:
                plan::CreateViewPlan {
                    name,
                    view:
                        plan::View {
                            create_sql,
                            expr: raw_expr,
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
        let ops = itertools::chain(
            drop_ids
                .iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(*id))),
            std::iter::once(catalog::Op::CreateItem {
                id,
                oid: self.catalog_mut().allocate_oid()?,
                name: name.clone(),
                item: CatalogItem::View(View {
                    create_sql: create_sql.clone(),
                    raw_expr,
                    desc: RelationDesc::new(optimized_expr.typ(), column_names.clone()),
                    optimized_expr,
                    conn_id: if temporary {
                        Some(session.conn_id().clone())
                    } else {
                        None
                    },
                    resolved_ids: resolved_ids.clone(),
                }),
                owner_id: *session.current_role_id(),
            }),
        )
        .collect::<Vec<_>>();

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
    fn create_view_explain(
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
                    broken,
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
            Default::default(),
            stage,
            plan::ExplaineeStatementKind::CreateView,
        )?;

        if broken {
            tracing_core::callsite::rebuild_interest_cache();
        }

        Ok(StageResult::Response(Self::send_immediate_rows(rows)))
    }
}
