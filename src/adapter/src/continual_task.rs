// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common methods for `CONTINUAL TASK`s.

use std::sync::Arc;

use mz_catalog::memory::objects::ContinualTask;
use mz_expr::Id;
use mz_expr::visit::Visit;
use mz_repr::GlobalId;
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{self, HirRelationExpr};
use timely::progress::Antichain;

use crate::AdapterError;

pub fn ct_item_from_plan(
    plan: plan::CreateContinualTaskPlan,
    global_id: GlobalId,
    resolved_ids: ResolvedIds,
) -> Result<ContinualTask, AdapterError> {
    let plan::CreateContinualTaskPlan {
        name: _,
        placeholder_id,
        desc,
        input_id,
        with_snapshot,
        continual_task:
            plan::MaterializedView {
                create_sql,
                cluster_id,
                expr: mut raw_expr,
                dependencies,
                column_names: _,
                non_null_assertions: _,
                compaction_window: _,
                refresh_schedule: _,
                as_of,
                replacing: _,
            },
    } = plan;

    // Replace any placeholder LocalIds.
    if let Some(placeholder_id) = placeholder_id {
        raw_expr.visit_mut_post(&mut |expr| match expr {
            HirRelationExpr::Get { id, .. } if *id == Id::Local(placeholder_id) => {
                *id = Id::Global(global_id);
            }
            _ => {}
        })?;
    }
    // TODO(alter_table): `dependencies` doesn't include the `CatalogItemId` for self and we can't
    // look it up in the Catalog from it's `GlobalId` because we haven't yet added this item.

    Ok(ContinualTask {
        create_sql,
        global_id,
        input_id,
        with_snapshot,
        raw_expr: Arc::new(raw_expr),
        desc,
        resolved_ids,
        dependencies,
        cluster_id,
        initial_as_of: as_of.map(Antichain::from_elem),
    })
}
