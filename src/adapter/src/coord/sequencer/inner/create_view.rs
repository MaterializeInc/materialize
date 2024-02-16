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
use mz_ore::instrument;
use mz_repr::RelationDesc;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan::{self};
use mz_sql::session::metadata::SessionMetadata;
use tracing::Span;

use crate::command::ExecuteResponse;
use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{
    Coordinator, CreateViewFinish, CreateViewOptimize, CreateViewStage, Message, PlanValidity,
    StageResult, Staged,
};
use crate::error::AdapterError;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice, ExecuteContext};

impl Staged for CreateViewStage {
    fn validity(&mut self) -> &mut PlanValidity {
        match self {
            Self::Optimize(stage) => &mut stage.validity,
            Self::Finish(stage) => &mut stage.validity,
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
            self.create_view_validate(ctx.session(), plan, resolved_ids),
            ctx
        );
        self.sequence_staged(ctx, Span::current(), stage).await;
    }

    #[instrument]
    fn create_view_validate(
        &mut self,
        session: &Session,
        plan: plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
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
        }))
    }

    #[instrument]
    async fn create_view_optimize(
        &mut self,
        CreateViewOptimize {
            validity,
            plan,
            resolved_ids,
        }: CreateViewOptimize,
    ) -> Result<StageResult<Box<CreateViewStage>>, AdapterError> {
        let id = self.catalog_mut().allocate_user_id().await?;

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        let span = Span::current();
        Ok(StageResult::Handle(mz_ore::task::spawn_blocking(
            || "optimize create view",
            move || {
                span.in_scope(|| {
                    // Build an optimizer for this VIEW.
                    let mut optimizer = optimize::view::Optimizer::new(optimizer_config);
                    // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
                    let raw_expr = plan.view.expr.clone();
                    let optimized_expr = optimizer.catch_unwind_optimize(raw_expr)?;
                    Ok(Box::new(CreateViewStage::Finish(CreateViewFinish {
                        validity,
                        id,
                        plan,
                        optimized_expr,
                        resolved_ids,
                    })))
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
}
