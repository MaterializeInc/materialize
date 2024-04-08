// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Derive insights for plans.

use std::collections::BTreeMap;
use std::fmt::Debug;

use mz_compute_types::dataflows::DataflowDescription;
use mz_expr::OptimizedMirRelationExpr;
use mz_repr::explain::ExprHumanizer;
use mz_repr::GlobalId;
use serde::Serialize;

use crate::coord::peek::FastPathPlan;

/// Insights about an optimized plan.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanInsights {
    /// Collections imported by the plan.
    ///
    /// Each key is the ID of an imported collection, and each value contains
    /// further insights about each collection and how it is used by the plan.
    pub imports: BTreeMap<String, ImportInsights>,
}

/// Insights about an imported collection in a plan.
#[derive(Clone, Debug, Serialize)]
pub struct ImportInsights {
    /// The full name of the imported collection.
    pub name: Name,
    /// The type of the imported collection.
    #[serde(rename = "type")]
    pub ty: ImportType,
}

/// The type of an imported collection.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ImportType {
    /// A compute collection--i.e., an index.
    Compute,
    /// A storage collection: a table, source, or materialized view.
    Storage,
}

/// The name of a collection.
#[derive(Debug, Clone, Serialize)]
pub struct Name {
    /// The database name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    /// The schema name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// The item name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub item: Option<String>,
}

pub fn plan_insights(
    humanizer: &dyn ExprHumanizer,
    global_plan: Option<DataflowDescription<OptimizedMirRelationExpr>>,
    fast_path_plan: Option<FastPathPlan>,
) -> Option<PlanInsights> {
    match (global_plan, fast_path_plan) {
        (None, None) => None,
        (None | Some(_), Some(fast_path_plan)) => {
            Some(fast_path_insights(humanizer, fast_path_plan))
        }
        (Some(global_plan), None) => Some(global_insights(humanizer, global_plan)),
    }
}

fn fast_path_insights(humanizer: &dyn ExprHumanizer, plan: FastPathPlan) -> PlanInsights {
    let mut insights = PlanInsights::default();
    match plan {
        FastPathPlan::Constant { .. } => (),
        FastPathPlan::PeekExisting(_, id, _, _) => {
            add_import_insights(&mut insights, humanizer, id, ImportType::Compute)
        }
        FastPathPlan::PeekPersist(id, _) => {
            add_import_insights(&mut insights, humanizer, id, ImportType::Storage)
        }
    }
    insights
}

fn global_insights(
    humanizer: &dyn ExprHumanizer,
    plan: DataflowDescription<OptimizedMirRelationExpr>,
) -> PlanInsights {
    let mut insights = PlanInsights::default();
    for (id, _) in plan.source_imports {
        add_import_insights(&mut insights, humanizer, id, ImportType::Storage)
    }
    for (id, _) in plan.index_imports {
        add_import_insights(&mut insights, humanizer, id, ImportType::Compute)
    }
    insights
}

fn add_import_insights(
    insights: &mut PlanInsights,
    humanizer: &dyn ExprHumanizer,
    id: GlobalId,
    ty: ImportType,
) {
    insights.imports.insert(
        id.to_string(),
        ImportInsights {
            name: structured_name(humanizer, id),
            ty,
        },
    );
}

fn structured_name(humanizer: &dyn ExprHumanizer, id: GlobalId) -> Name {
    let mut parts = humanizer.humanize_id_parts(id).unwrap_or(Vec::new());
    Name {
        item: parts.pop(),
        schema: parts.pop(),
        database: parts.pop(),
    }
}
