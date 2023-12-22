// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_catalog::memory::objects::{CatalogItem, View};
use mz_expr::CollectionPlan;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::RelationDesc;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan::{self};

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateViewFinish, CreateViewOptimize, CreateViewStage, CreateViewValidate,
    Message, PlanValidity,
};
use crate::error::AdapterError;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_view(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) {
        self.sequence_create_view_stage(
            ctx,
            CreateViewStage::Validate(CreateViewValidate { plan, resolved_ids }),
            OpenTelemetryContext::obtain(),
        )
        .await;
    }

    /// Processes as many `create view` stages as possible.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn sequence_create_view_stage(
        &mut self,
        mut ctx: ExecuteContext,
        mut stage: CreateViewStage,
        otel_ctx: OpenTelemetryContext,
    ) {
        use CreateViewStage::*;

        // Process the current stage and allow for processing the next.
        loop {
            // Always verify plan validity. This is cheap, and prevents programming errors
            // if we move any stages off thread.
            if let Some(validity) = stage.validity() {
                return_if_err!(validity.check(self.catalog()), ctx);
            }

            (ctx, stage) = match stage {
                Validate(stage) => {
                    let next = return_if_err!(self.create_view_validate(ctx.session(), stage), ctx);
                    (ctx, CreateViewStage::Optimize(next))
                }
                Optimize(stage) => {
                    self.create_view_optimize(ctx, stage, otel_ctx).await;
                    return;
                }
                Finish(stage) => {
                    let result = self.create_view_finish(&mut ctx, stage).await;
                    ctx.retire(result);
                    return;
                }
            }
        }
    }

    fn create_view_validate(
        &mut self,
        session: &Session,
        CreateViewValidate { plan, resolved_ids }: CreateViewValidate,
    ) -> Result<CreateViewOptimize, AdapterError> {
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

        Ok(CreateViewOptimize {
            validity,
            plan,
            resolved_ids,
        })
    }

    async fn create_view_optimize(
        &mut self,
        ctx: ExecuteContext,
        CreateViewOptimize {
            validity,
            plan,
            resolved_ids,
        }: CreateViewOptimize,
        otel_ctx: OpenTelemetryContext,
    ) {
        // Generate data structures that can be moved to another task where we will perform possibly
        // expensive optimizations.
        let internal_cmd_tx = self.internal_cmd_tx.clone();

        let id = return_if_err!(self.catalog_mut().allocate_user_id().await, ctx);

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config);

        let span = tracing::debug_span!("optimize create view task");

        mz_ore::task::spawn_blocking(
            || "optimize create view",
            move || {
                let _guard = span.enter();

                // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
                let raw_expr = plan.view.expr.clone();
                let optimized_expr = return_if_err!(optimizer.optimize(raw_expr), ctx);

                let stage = CreateViewStage::Finish(CreateViewFinish {
                    validity,
                    id,
                    plan,
                    optimized_expr,
                    resolved_ids,
                });

                let _ = internal_cmd_tx.send(Message::CreateViewStageReady {
                    ctx,
                    otel_ctx,
                    stage,
                });
            },
        );
    }

    async fn create_view_finish(
        &mut self,
        ctx: &mut ExecuteContext,
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
    ) -> Result<ExecuteResponse, AdapterError> {
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
                        Some(ctx.session().conn_id().clone())
                    } else {
                        None
                    },
                    resolved_ids: resolved_ids.clone(),
                }),
                owner_id: *ctx.session().current_role_id(),
            }),
        )
        .collect::<Vec<_>>();

        match self.catalog_transact(Some(ctx.session()), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedView),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                ctx.session()
                    .add_notice(AdapterNotice::ObjectAlreadyExists {
                        name: name.item,
                        ty: "view",
                    });
                Ok(ExecuteResponse::CreatedView)
            }
            Err(err) => Err(err),
        }
    }
}
