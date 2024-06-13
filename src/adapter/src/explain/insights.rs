// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Derive insights for plans.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use mz_compute_types::dataflows::DataflowDescription;
use mz_expr::{OptimizedMirRelationExpr, RowSetFinishing};
use mz_repr::explain::ExprHumanizer;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_transform::EmptyStatisticsOracle;
use serde::Serialize;

use crate::catalog::Catalog;
use crate::coord::peek::{FastPathPlan, PeekPlan};
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::metrics::OptimizerMetrics;
use crate::optimize::{self, Optimize, OptimizerConfig, OptimizerError};
use crate::session::SessionMeta;
use crate::TimestampContext;

/// Information needed to compute PlanInsights.
#[derive(Debug)]
pub struct PlanInsightsContext {
    pub raw_expr: HirRelationExpr,
    pub catalog: Arc<Catalog>,
    // Snapshots of all user compute instances.
    //
    // TODO: Avoid populating this if not needed. Maybe make this a method that can return a
    // ComputeInstanceSnapshot for a given cluster.
    pub compute_instances: BTreeMap<String, ComputeInstanceSnapshot>,
    pub metrics: OptimizerMetrics,
    pub finishing: RowSetFinishing,
    pub optimizer_config: OptimizerConfig,
    pub session: SessionMeta,
    pub timestamp_context: TimestampContext<Timestamp>,
    pub view_id: GlobalId,
    pub index_id: GlobalId,
}

/// Insights about an optimized plan.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanInsights {
    /// Collections imported by the plan.
    ///
    /// Each key is the ID of an imported collection, and each value contains
    /// further insights about each collection and how it is used by the plan.
    pub imports: BTreeMap<String, ImportInsights>,
    /// If this plan is not fast path, this is the set of cluster names that would render this as fast path.
    pub fast_path_clusters: BTreeSet<String>,
}

impl PlanInsights {
    pub fn compute_fast_path_clusters(&mut self, ctx: PlanInsightsContext) {
        for (name, compute_instance) in ctx.compute_instances {
            let mut optimizer = optimize::peek::Optimizer::new(
                Arc::clone(&ctx.catalog),
                compute_instance,
                ctx.finishing.clone(),
                ctx.view_id,
                ctx.index_id,
                ctx.optimizer_config.clone(),
                ctx.metrics.clone(),
            );

            let res: Result<_, OptimizerError> = (|| {
                // HIR ⇒ MIR lowering and MIR optimization (local)
                let local_mir_plan = optimizer.catch_unwind_optimize(ctx.raw_expr.clone())?;
                // Attach resolved context required to continue the pipeline.
                let local_mir_plan = local_mir_plan.resolve(
                    ctx.timestamp_context.clone(),
                    &ctx.session,
                    Box::new(EmptyStatisticsOracle {}),
                );
                // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
                Ok(global_lir_plan)
            })();
            let Ok(plan) = res else {
                continue;
            };
            let (plan, _, _) = plan.unapply();
            if matches!(plan, PeekPlan::FastPath(_)) {
                self.fast_path_clusters.insert(name);
            }
        }
    }
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
