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
use mz_sql::plan::{self};

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::{catalog, AdapterNotice};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_create_view_off_thread(
        &mut self,
        session: &mut Session,
        plan: plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        ::tracing::info!("sequence_create_view_off_thread");

        let if_not_exists = plan.if_not_exists;
        let ops = self
            .generate_view_ops_off_thread(session, &plan, resolved_ids)
            .await?;
        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedView),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind:
                    mz_catalog::memory::error::ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: plan.name.item,
                    ty: "view",
                });
                Ok(ExecuteResponse::CreatedView)
            }
            Err(err) => Err(err),
        }
    }

    async fn generate_view_ops_off_thread(
        &mut self,
        session: &Session,
        plan::CreateViewPlan {
            name,
            view,
            drop_ids,
            ambiguous_columns,
            ..
        }: &plan::CreateViewPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<Vec<catalog::Op>, AdapterError> {
        // Validate any references in the view's expression. We do this on the
        // unoptimized plan to better reflect what the user typed. We want to
        // reject queries that depend on a relation in the wrong timeline, for
        // example, even if we can *technically* optimize that reference away.
        let depends_on = view.expr.depends_on();
        self.validate_timeline_context(depends_on.iter().copied())?;
        self.validate_system_column_references(*ambiguous_columns, &depends_on)?;

        let mut ops = vec![];

        ops.extend(
            drop_ids
                .iter()
                .map(|id| catalog::Op::DropObject(ObjectId::Item(*id))),
        );

        let view_id = self.catalog_mut().allocate_user_id().await?;
        let view_oid = self.catalog_mut().allocate_oid()?;
        let raw_expr = view.expr.clone();

        // Collect optimizer parameters.
        let optimizer_config = optimize::OptimizerConfig::from(self.catalog().system_config());

        // Build an optimizer for this VIEW.
        let mut optimizer = optimize::view::Optimizer::new(optimizer_config);

        // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
        let optimized_expr = optimizer.optimize(raw_expr.clone())?;

        let view = View {
            create_sql: view.create_sql.clone(),
            raw_expr,
            desc: RelationDesc::new(optimized_expr.typ(), view.column_names.clone()),
            optimized_expr,
            conn_id: if view.temporary {
                Some(session.conn_id().clone())
            } else {
                None
            },
            resolved_ids,
        };
        ops.push(catalog::Op::CreateItem {
            id: view_id,
            oid: view_oid,
            name: name.clone(),
            item: CatalogItem::View(view),
            owner_id: *session.current_role_id(),
        });

        Ok(ops)
    }
}
