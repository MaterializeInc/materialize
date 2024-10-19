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
use mz_expr::visit::Visit;
use mz_expr::{CollectionPlan, Id};
use mz_repr::GlobalId;
use mz_sql::names::ResolvedIds;
use mz_sql::plan::{self, HirRelationExpr};
use timely::progress::Antichain;

use crate::catalog::CatalogState;
use crate::AdapterError;

pub fn ct_item_from_plan(
    catalog: &CatalogState,
    plan: plan::CreateContinualTaskPlan,
    global_id: GlobalId,
    resolved_ids: ResolvedIds,
) -> Result<ContinualTask, AdapterError> {
    let plan::CreateContinualTaskPlan {
        name: _,
        placeholder_id,
        desc,
        input_id,
        continual_task:
            plan::MaterializedView {
                create_sql,
                cluster_id,
                expr: mut raw_expr,
                dependencies: _,
                column_names: _,
                non_null_assertions: _,
                compaction_window: _,
                refresh_schedule: _,
                as_of,
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
    // Re-resolve the dependencies after updating the raw_expr.
    let dependencies = raw_expr
        .depends_on()
        .into_iter()
        .map(|gid| catalog.get_entry_by_global_id(&gid).item_id())
        .collect();

    Ok(ContinualTask {
        create_sql,
        global_id,
        input_id,
        raw_expr: Arc::new(raw_expr),
        desc,
        resolved_ids,
        dependencies,
        cluster_id,
        initial_as_of: as_of.map(Antichain::from_elem),
    })
}
