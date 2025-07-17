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
use std::sync::Arc;

use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_expr::{AccessStrategy, Id, MirRelationExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_ore::num::NonNeg;
use mz_repr::explain::ExprHumanizer;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::ast::Statement;
use mz_sql::names::Aug;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use mz_sql::session::metadata::SessionMetadata;
use mz_transform::EmptyStatisticsOracle;
use serde::Serialize;

use crate::TimestampContext;
use crate::catalog::Catalog;
use crate::coord::peek::{FastPathPlan, PeekPlan};
use crate::optimize::dataflows::ComputeInstanceSnapshot;
use crate::optimize::{self, Optimize, OptimizerConfig, OptimizerError};
use crate::session::SessionMeta;

/// Information needed to compute PlanInsights.
#[derive(Debug)]
pub struct PlanInsightsContext {
    pub stmt: Option<Statement<Aug>>,
    pub raw_expr: HirRelationExpr,
    pub catalog: Arc<Catalog>,
    // Snapshots of all user compute instances.
    //
    // TODO: Avoid populating this if not needed. Maybe make this a method that can return a
    // ComputeInstanceSnapshot for a given cluster.
    pub compute_instances: BTreeMap<String, ComputeInstanceSnapshot>,
    pub target_instance: String,
    pub metrics: OptimizerMetrics,
    pub finishing: RowSetFinishing,
    pub optimizer_config: OptimizerConfig,
    pub session: SessionMeta,
    pub timestamp_context: TimestampContext<Timestamp>,
    pub view_id: GlobalId,
    pub index_id: GlobalId,
    pub enable_re_optimize: bool,
}

/// Insights about an optimized plan.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PlanInsights {
    /// Collections imported by the plan.
    ///
    /// Each key is the ID of an imported collection, and each value contains
    /// further insights about each collection and how it is used by the plan.
    pub imports: BTreeMap<String, ImportInsights>,
    /// If this plan is not fast path, this is the map of cluster names to indexes that would render
    /// this as fast path. That is: if this query were run on the cluster of the key, it would be
    /// fast because it would use the index of the value.
    pub fast_path_clusters: BTreeMap<String, Option<FastPathCluster>>,
    /// For the current cluster, whether adding a LIMIT <= this will result in a fast path.
    pub fast_path_limit: Option<usize>,
    /// Names of persist sources over which a count(*) is done.
    pub persist_count: Vec<Name>,
}

#[derive(Clone, Debug, Serialize)]
pub struct FastPathCluster {
    index: Name,
    on: Name,
}

impl PlanInsights {
    pub async fn compute_fast_path_clusters(
        &mut self,
        humanizer: &dyn ExprHumanizer,
        ctx: Box<PlanInsightsContext>,
    ) {
        // Warning: This function is currently dangerous, because it does optimizer work
        // proportional to the number of clusters, and does so on the main coordinator task.
        // see https://github.com/MaterializeInc/database-issues/issues/9492
        if !ctx.optimizer_config.features.enable_fast_path_plan_insights {
            return;
        }
        let session: Arc<dyn SessionMetadata + Send> = Arc::new(ctx.session);
        let tasks = ctx
            .compute_instances
            .into_iter()
            .map(|(name, compute_instance)| {
                let raw_expr = ctx.raw_expr.clone();
                let mut finishing = ctx.finishing.clone();
                // For the current cluster, try adding a LIMIT to see if it fast paths with PeekPersist.
                if name == ctx.target_instance {
                    finishing.limit = Some(NonNeg::try_from(1).expect("non-negitave"));
                }
                let session = Arc::clone(&session);
                let timestamp_context = ctx.timestamp_context.clone();
                let mut optimizer = optimize::peek::Optimizer::new(
                    Arc::clone(&ctx.catalog),
                    compute_instance,
                    finishing,
                    ctx.view_id,
                    ctx.index_id,
                    ctx.optimizer_config.clone(),
                    ctx.metrics.clone(),
                );
                mz_ore::task::spawn_blocking(
                    || "compute fast path clusters",
                    move || {
                        // HIR ⇒ MIR lowering and MIR optimization (local)
                        let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
                        // Attach resolved context required to continue the pipeline.
                        let local_mir_plan = local_mir_plan.resolve(
                            timestamp_context,
                            &*session,
                            Box::new(EmptyStatisticsOracle {}),
                        );
                        // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global)
                        let global_lir_plan = optimizer.catch_unwind_optimize(local_mir_plan)?;
                        Ok::<_, OptimizerError>((name, global_lir_plan))
                    },
                )
            });
        for task in tasks {
            let res = task.await;
            let Ok(Ok((name, plan))) = res else {
                continue;
            };
            let (plan, _, _) = plan.unapply();
            if let PeekPlan::FastPath(plan) = plan {
                // Same-cluster optimization is the LIMIT check.
                if name == ctx.target_instance {
                    self.fast_path_limit =
                        Some(ctx.optimizer_config.features.persist_fast_path_limit);
                    continue;
                }
                let idx_name = if let FastPathPlan::PeekExisting(_, idx_id, _, _) = plan {
                    let idx_entry = ctx.catalog.get_entry_by_global_id(&idx_id);
                    Some(FastPathCluster {
                        index: structured_name(humanizer, idx_id),
                        on: structured_name(
                            humanizer,
                            idx_entry.index().expect("must be index").on,
                        ),
                    })
                } else {
                    // This shouldn't ever happen (changing the cluster should not affect whether a
                    // fast path of type constant or persist peek is created), but protect against
                    // it anyway.
                    None
                };
                self.fast_path_clusters.insert(name, idx_name);
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
        FastPathPlan::PeekPersist(id, _, _) => {
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
    for BuildDesc { plan, .. } in plan.objects_to_build {
        // Search for a count(*) over a persist read.
        plan.visit_pre(|expr| {
            let MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                ..
            } = expr
            else {
                return;
            };
            if !group_key.is_empty() {
                return;
            }
            let MirRelationExpr::Project { input, outputs } = &**input else {
                return;
            };
            if !outputs.is_empty() {
                return;
            }
            let MirRelationExpr::Get {
                id: Id::Global(id),
                access_strategy: AccessStrategy::Persist,
                ..
            } = &**input
            else {
                return;
            };
            let [aggregate] = aggregates.as_slice() else {
                return;
            };
            if !aggregate.is_count_asterisk() {
                return;
            }
            let name = structured_name(humanizer, *id);
            insights.persist_count.push(name);
        });
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
