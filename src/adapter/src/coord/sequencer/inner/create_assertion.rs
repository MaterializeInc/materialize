// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{Assertion, CatalogItem};
use mz_expr::CollectionPlan;
use mz_ore::instrument;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{DependencyIds, ResolvedIds};
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;

use crate::catalog;
use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::session::Session;
use crate::AdapterNotice;

impl Coordinator {
    /// Sequences `CREATE ASSERTION`.
    ///
    /// Stage 1 is declaration-only: this inserts a durable
    /// [`CatalogItem::Assertion`] into the catalog. It deliberately does **not**
    /// optimize the violations query, ship a compute dataflow, or allocate a
    /// storage collection; those land in Stage 2. Gated behind
    /// `enable_create_assertion`, which `plan_create_assertion` already requires,
    /// so by the time we are here the flag was on at plan time.
    #[instrument]
    pub(crate) async fn sequence_create_assertion(
        &mut self,
        session: &Session,
        plan: plan::CreateAssertionPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateAssertionPlan {
            name,
            assertion:
                plan::Assertion {
                    create_sql,
                    expr: raw_expr,
                    cluster_id,
                    target_replica,
                    // `dependencies` and `column_names` are recomputed/omitted here;
                    // see below.
                    ..
                },
            if_not_exists,
        } = plan;

        // Validate that the violations query doesn't reference objects in an
        // incompatible timeline, mirroring `CREATE VIEW`/`CREATE MATERIALIZED
        // VIEW`. We do this on the unoptimized expression to reflect what the
        // user wrote.
        let depends_on = raw_expr.depends_on();
        self.catalog()
            .validate_timeline_context(depends_on.iter().copied())?;

        let (item_id, global_id) = self.allocate_user_id().await?;

        let raw_expr = Arc::new(raw_expr);
        // Resolve dependencies from the HIR, exactly as `parse_item_inner` does
        // when rebuilding the item from `create_sql` on bootstrap, so the
        // create-time and re-planned items are identical.
        let dependencies = raw_expr
            .depends_on()
            .into_iter()
            .map(|gid| self.catalog().get_entry_by_global_id(&gid).id())
            .collect();

        let item = CatalogItem::Assertion(Assertion {
            create_sql,
            global_id,
            raw_expr,
            resolved_ids,
            dependencies: DependencyIds(dependencies),
            cluster_id,
            target_replica,
        });

        let ops = vec![catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item,
            owner_id: *session.current_role_id(),
        }];

        match self.catalog_transact(Some(session), ops).await {
            Ok(()) => Ok(ExecuteResponse::CreatedAssertion),
            Err(AdapterError::Catalog(Error {
                kind: ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "assertion",
                });
                Ok(ExecuteResponse::CreatedAssertion)
            }
            Err(err) => Err(err),
        }
    }
}
