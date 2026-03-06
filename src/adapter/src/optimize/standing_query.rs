// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE STANDING QUERY` statements.
//!
//! Based on the materialized view optimizer, but with support for extra source
//! imports (the parameter collection) that don't have catalog entries.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::trace_plan;
use mz_repr::{ColumnName, GlobalId, RelationDesc, SqlRelationType};
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_sql::plan::HirRelationExpr;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{SharedTypecheckingContext, empty_typechecking_context};
use timely::progress::Antichain;

use crate::coord::infer_sql_type_for_catalog;
use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, ExprPrep, ExprPrepMaintained,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode, OptimizerCatalog,
    OptimizerConfig, OptimizerError, optimize_mir_local, trace_plan,
};

pub struct Optimizer {
    /// A representation typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: SharedTypecheckingContext,
    /// A snapshot of the catalog state.
    catalog: Arc<dyn OptimizerCatalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported sink.
    sink_id: GlobalId,
    /// A transient GlobalId to be used when constructing the dataflow.
    view_id: GlobalId,
    /// The resulting column names.
    column_names: Vec<ColumnName>,
    /// A human-readable name exposed internally (useful for debugging).
    debug_name: String,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
    /// Extra source imports to pre-register in the DataflowDescription before
    /// resolving `Get` references. This allows expressions to reference storage
    /// collections that don't have catalog entries (e.g. the parameter
    /// collection).
    extra_source_imports: BTreeMap<GlobalId, SqlRelationType>,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<dyn OptimizerCatalog>,
        compute_instance: ComputeInstanceSnapshot,
        sink_id: GlobalId,
        view_id: GlobalId,
        column_names: Vec<ColumnName>,
        debug_name: String,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
        extra_source_imports: BTreeMap<GlobalId, SqlRelationType>,
    ) -> Self {
        Self {
            typecheck_ctx: empty_typechecking_context(),
            catalog,
            compute_instance,
            sink_id,
            view_id,
            column_names,
            debug_name,
            config,
            metrics,
            duration: Default::default(),
            extra_source_imports,
        }
    }
}

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
#[derive(Clone, Debug)]
pub struct LocalMirPlan {
    expr: MirRelationExpr,
    df_meta: DataflowMetainfo,
    typ: SqlRelationType,
}

impl LocalMirPlan {
    pub fn expr(&self) -> OptimizedMirRelationExpr {
        OptimizedMirRelationExpr(self.expr.clone())
    }
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

    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}

// HIR ⇒ MIR lowering + local MIR optimization
impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        // Trace the pipeline input under `optimize/raw`.
        trace_plan!(at: "raw", &expr);

        // HIR ⇒ MIR lowering and decorrelation
        let mir_expr = expr.clone().lower(&self.config, Some(&self.metrics))?;

        // MIR ⇒ MIR optimization (local)
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
            Some(self.view_id),
        );
        let mir_expr = optimize_mir_local(mir_expr, &mut transform_ctx)?.into_inner();
        let typ = infer_sql_type_for_catalog(&expr, &mir_expr);

        self.duration += time.elapsed();

        Ok(LocalMirPlan {
            expr: mir_expr,
            df_meta,
            typ,
        })
    }
}

// Embed in DataflowDescription + global MIR optimization.
// Same as the MV optimizer but pre-imports extra sources before resolving Get references.
impl Optimize<LocalMirPlan> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, plan: LocalMirPlan) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let expr = OptimizedMirRelationExpr(plan.expr);
        let mut df_meta = plan.df_meta;

        let rel_typ = plan.typ;
        let rel_desc = RelationDesc::new(rel_typ, self.column_names.clone());

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(self.debug_name.clone());

        // Pre-import extra sources (the parameter collection) so they are
        // found by `is_imported` and skip catalog lookup.
        for (id, typ) in &self.extra_source_imports {
            df_desc.import_source(*id, typ.clone(), false);
        }

        df_builder.import_view_into_dataflow(
            &self.view_id,
            &expr,
            &mut df_desc,
            &self.config.features,
        )?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        let sink_description = ComputeSinkDesc {
            from: self.view_id,
            from_desc: rel_desc,
            connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection::default()),
            with_snapshot: true,
            up_to: Antichain::default(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
        };
        df_desc.export_sink(self.sink_id, sink_description);

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepMaintained;
        df_desc.visit_children(
            |r| style.prep_relation_expr(r),
            |s| style.prep_scalar_expr(s),
        )?;

        // Construct TransformCtx for global optimization.
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &mz_transform::EmptyStatisticsOracle, // TODO: wire proper stats
            &self.config.features,
            &self.typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx, false)?;

        if self.config.mode == OptimizeMode::Explain {
            // Collect the list of indexes used by the dataflow at this point.
            trace_plan!(at: "global", &df_meta.used_indexes(&df_desc));
        }

        self.duration += time.elapsed();

        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

// MIR ⇒ LIR lowering + LIR optimization
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
            .observe_e2e_optimization_time("standing_query", self.duration);

        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}
