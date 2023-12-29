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
use mz_repr::RelationDesc;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{ObjectId, ResolvedIds};
use mz_sql::plan::{self, CreateViewPlan, Optimized};

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::{catalog, AdapterNotice, ExecuteContext, ExecuteResponse};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_view(
        &mut self,
        mut ctx: ExecuteContext,
        plan: CreateViewPlan<Optimized>,
        resolved_ids: ResolvedIds,
    ) {
        return_if_err!(self.create_view_validate(&plan), ctx);
        let result = self.create_view_finish(&mut ctx, plan, resolved_ids).await;
        ctx.retire(result);
        return;
    }

    fn create_view_validate(
        &mut self,
        plan: &CreateViewPlan<Optimized>,
    ) -> Result<(), AdapterError> {
        let plan::CreateViewPlan {
            view:
                plan::View {
                    expr: (raw_expr, _optimized_expr),
                    ..
                },
            ambiguous_columns,
            ..
        } = &plan;

        // Validate any references in the view's expression. We do this on the
        // unoptimized plan to better reflect what the user typed. We want to
        // reject queries that depend on a relation in the wrong timeline, for
        // example, even if we can *technically* optimize that reference away.
        let expr_depends_on = raw_expr.depends_on();
        self.validate_timeline_context(expr_depends_on.iter().copied())?;
        self.validate_system_column_references(*ambiguous_columns, &expr_depends_on)?;

        Ok(())
    }

    async fn create_view_finish(
        &mut self,
        ctx: &mut ExecuteContext,
        CreateViewPlan {
            name,
            view:
                plan::View {
                    create_sql,
                    expr: (raw_expr, optimized_expr),
                    column_names,
                    temporary,
                },
            drop_ids,
            if_not_exists,
            ..
        }: CreateViewPlan<Optimized>,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let id = self.catalog_mut().allocate_user_id().await?;

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
