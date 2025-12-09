// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE INDEX` statements.
//!
//! Note that, in contrast to other optimization pipelines, timestamp selection is not part of
//! index optimization. Instead users are expected to separately set the as-of on the optimized
//! `DataflowDescription` received from `GlobalLirPlan::unapply`. Reasons for choosing to exclude
//! timestamp selection from the index optimization pipeline are:
//!
//!  (a) Indexes don't support non-empty `until` frontiers, so they don't provide opportunity for
//!      optimizations based on the selected timestamp.
//!  (b) We want to generate dataflow plans early during environment bootstrapping, before we have
//!      access to all information required for timestamp selection.
//!
//! None of this is set in stone though. If we find an opportunity for optimizing indexes based on
//! their timestamps, we'll want to make timestamp selection part of the index optimization again
//! and find a different approach to bootstrapping.
//!
//! See also MaterializeInc/materialize#22940.

use std::sync::Arc;
use std::time::{Duration, Instant};

use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_repr::GlobalId;
use mz_repr::explain::trace_plan;
use mz_sql::names::QualifiedItemName;
use mz_sql::optimizer_metrics::OptimizerMetrics;
use mz_transform::TransformCtx;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::notice::{IndexAlreadyExists, IndexKeyEmpty};
use mz_transform::reprtypecheck::{
    SharedContext as ReprTypecheckContext, empty_context as empty_repr_context,
};

use crate::optimize::dataflows::{
    ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle, ExprPrepStyleMaintained,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode, OptimizerCatalog,
    OptimizerConfig, OptimizerError, trace_plan,
};

pub struct Optimizer {
    /// A representation typechecking context to use throughout the optimizer pipeline.
    repr_typecheck_ctx: ReprTypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<dyn OptimizerCatalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported index arrangement.
    exported_index_id: GlobalId,
    /// Optimizer config.
    config: OptimizerConfig,
    /// Optimizer metrics.
    metrics: OptimizerMetrics,
    /// The time spent performing optimization so far.
    duration: Duration,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<dyn OptimizerCatalog>,
        compute_instance: ComputeInstanceSnapshot,
        exported_index_id: GlobalId,
        config: OptimizerConfig,
        metrics: OptimizerMetrics,
    ) -> Self {
        Self {
            repr_typecheck_ctx: empty_repr_context(),
            catalog,
            compute_instance,
            exported_index_id,
            config,
            metrics,
            duration: Default::default(),
        }
    }
}

/// A wrapper of index parts needed to start the optimization process.
pub struct Index {
    name: QualifiedItemName,
    on: GlobalId,
    keys: Vec<mz_expr::MirScalarExpr>,
}

impl Index {
    /// Construct a new [`Index`]. Arguments are recorded as-is.
    pub fn new(name: QualifiedItemName, on: GlobalId, keys: Vec<mz_expr::MirScalarExpr>) -> Self {
        Self { name, on, keys }
    }
}

/// The (sealed intermediate) result after:
///
/// 1. embedding an [`Index`] into a [`MirDataflowDescription`],
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
}

impl Optimize<Index> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, index: Index) -> Result<Self::To, OptimizerError> {
        let time = Instant::now();

        let on_entry = self.catalog.get_entry(&index.on);
        let full_name = self
            .catalog
            .resolve_full_name(&index.name, on_entry.conn_id());
        let on_desc = on_entry
            .relation_desc()
            .expect("can only create indexes on items with a valid description");

        let mut df_builder = {
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(&*self.catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(full_name.to_string());

        df_builder.import_into_dataflow(&index.on, &mut df_desc, &self.config.features)?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        let index_desc = IndexDesc {
            on_id: index.on,
            key: index.keys.clone(),
        };
        df_desc.export_index(self.exported_index_id, index_desc, on_desc.typ().clone());

        // Prepare expressions in the assembled dataflow.
        let style = ExprPrepStyleMaintained;
        df_desc.visit_children(
            |r| style.prep_relation_expr(r),
            |s| style.prep_scalar_expr(s),
        )?;

        // Construct TransformCtx for global optimization.
        let mut df_meta = DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::global(
            &df_builder,
            &mz_transform::EmptyStatisticsOracle, // TODO: wire proper stats
            &self.config.features,
            &self.repr_typecheck_ctx,
            &mut df_meta,
            Some(&mut self.metrics),
        );
        // Run global optimization.
        mz_transform::optimize_dataflow(&mut df_desc, &mut transform_ctx, false)?;

        if self.config.mode == OptimizeMode::Explain {
            // Collect the list of indexes used by the dataflow at this point.
            trace_plan!(at: "global", &df_meta.used_indexes(&df_desc));
        }

        // Emit a notice if we are trying to create an empty index.
        if index.keys.is_empty() {
            df_meta.push_optimizer_notice_dedup(IndexKeyEmpty);
        }

        // Emit a notice for each available index identical to the one we are
        // currently optimizing.
        for (index_id, idx) in df_builder
            .indexes_on(index.on)
            .filter(|(_id, idx)| idx.keys.as_ref() == &index.keys)
        {
            df_meta.push_optimizer_notice_dedup(IndexAlreadyExists {
                index_id,
                index_key: idx.keys.to_vec(),
                index_on_id: idx.on,
                exported_index_id: self.exported_index_id,
            });
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
            .observe_e2e_optimization_time("index", self.duration);

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
