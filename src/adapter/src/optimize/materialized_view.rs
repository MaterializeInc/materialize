// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE MATERIALIZED VIEW` statements.
//!
//! Note that, in contrast to other optimization pipelines, timestamp selection is not part of
//! MV optimization. Instead users are expected to separately set the as-of on the optimized
//! `DataflowDescription` received from `GlobalLirPlan::unapply`. Reasons for choosing to exclude
//! timestamp selection from the MV optimization pipeline are:
//!
//!  (a) MVs don't support non-empty `until` frontiers, so they don't provide opportunity for
//!      optimizations based on the selected timestamp.
//!  (b) We want to generate dataflow plans early during environment bootstrapping, before we have
//!      access to all information required for timestamp selection.
//!
//! None of this is set in stone though. If we find an opportunity for optimizing MVs based on
//! their timestamps, we'll want to make timestamp selection part of the MV optimization again and
//! find a different approach to bootstrapping.
//!
//! See also MaterializeInc/materialize#22940.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::trace_plan;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{ColumnName, GlobalId, RelationDesc};
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::TransformCtx;
use timely::progress::Antichain;

use crate::catalog::Catalog;
use crate::optimize::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle,
};
use crate::optimize::metrics::OptimizerMetrics;
use crate::optimize::{
    optimize_mir_local, trace_plan, LirDataflowDescription, MirDataflowDescription, Optimize,
    OptimizeMode, OptimizerConfig, OptimizerError,
};

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported materialized view sink.
    sink_id: GlobalId,
    /// A transient GlobalId to be used when constructing the dataflow.
    view_id: GlobalId,
    /// The resulting column names.
    column_names: Vec<ColumnName>,
    /// Output columns that are asserted to be not null in the `CREATE VIEW`
    /// statement.
    non_null_assertions: Vec<usize>,
    /// Refresh schedule, e.g., `REFRESH EVERY '1 day'`
    refresh_schedule: Option<RefreshSchedule>,
    /// A human-readable name exposed internally (useful for debugging).
    debug_name: String,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        sink_id: GlobalId,
        view_id: GlobalId,
        column_names: Vec<ColumnName>,
        non_null_assertions: Vec<usize>,
        refresh_schedule: Option<RefreshSchedule>,
        debug_name: String,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            sink_id,
            view_id,
            column_names,
            non_null_assertions,
            refresh_schedule,
            debug_name,
            config,
            metrics,
            duration: Default::default(),
        }
    }
}

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
#[derive(Clone, Debug)]
pub struct LocalMirPlan {
    expr: MirRelationExpr,
    df_meta: DataflowMetainfo,
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`LocalMirPlan`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone, Debug)]
pub struct GlobalMirPlan {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalMirPlan {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone, Debug)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalLirPlan {
    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }

    pub fn desc(&self) -> &RelationDesc {
        let sink_exports = &self.df_desc.sink_exports;
        let sink = sink_exports.values().next().expect("valid sink");
        &sink.from_desc
    }
}

impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx =
            TransformCtx::local(&self.config.features, &self.typecheck_ctx, &mut df_meta);
        let expr = optimize_mir_local(expr, &mut transform_ctx)?.into_inner();

        self.duration += time.elapsed();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalMirPlan { expr, df_meta })
    }
}

impl LocalMirPlan {
    pub fn expr(&self) -> OptimizedMirRelationExpr {
        OptimizedMirRelationExpr(self.expr.clone())
    }
}

/// This is needed only because the pipeline in the bootstrap code starts from an
/// [`OptimizedMirRelationExpr`] attached to a [`mz_catalog::memory::objects::CatalogItem`].
impl Optimize<OptimizedMirRelationExpr> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, expr: OptimizedMirRelationExpr) -> Result<Self::To, OptimizerError> {
        let expr = expr.into_inner();
        let df_meta = DataflowMetainfo::default();
        self.optimize(LocalMirPlan { expr, df_meta })
    }
}

impl Optimize<LocalMirPlan> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, plan: LocalMirPlan) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let expr = OptimizedMirRelationExpr(plan.expr);
        let mut df_meta = plan.df_meta;

        let mut rel_typ = expr.typ();
        for &i in self.non_null_assertions.iter() {
            rel_typ.column_types[i].nullable = false;
        }
        let rel_desc = RelationDesc::new(rel_typ, self.column_names.clone());

        let mut df_builder = {
            let catalog = self.catalog.state();
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(self.debug_name.clone());

        df_builder.import_view_into_dataflow(&self.view_id, &expr, &mut df_desc)?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        let sink_description = ComputeSinkDesc {
            from: self.view_id,
            from_desc: rel_desc.clone(),
            connection: ComputeSinkConnection::Persist(PersistSinkConnection {
                value_desc: rel_desc,
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::default(),
            non_null_assertions: self.non_null_assertions.clone(),
            refresh_schedule: self.refresh_schedule.clone(),
        };
        df_desc.export_sink(self.sink_id, sink_description);

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepStyle::Index;
        df_desc.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        // Construct TransformCtx for global optimization.
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &mz_transform::EmptyStatisticsOracle, // TODO: wire proper stats
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx)?;

        if self.config.mode == OptimizeMode::Explain {
            // Collect the list of indexes used by the dataflow at this point.
            trace_plan!(at: "global", &df_meta.used_indexes(&df_desc));
        }

        self.duration += time.elapsed();

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let GlobalMirPlan {
            mut df_desc,
            df_meta,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0, &self.config.features)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(df_desc, &self.config.features)?;

        // Trace the pipeline output under `optimize`.
        trace_plan(&df_desc);

        self.duration += time.elapsed();
        self.metrics
            .observe_e2e_optimization_time("materialized_view", self.duration);

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}
