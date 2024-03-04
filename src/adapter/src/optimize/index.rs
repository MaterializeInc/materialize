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

use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_repr::explain::trace_plan;
use mz_repr::GlobalId;
use mz_sql::names::QualifiedItemName;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::notice::{IndexAlreadyExists, IndexKeyEmpty};
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};

use crate::catalog::Catalog;
use crate::optimize::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle,
};
use crate::optimize::{
    trace_plan, LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode,
    OptimizerConfig, OptimizerError,
};

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    _typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported index arrangement.
    exported_index_id: GlobalId,
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        exported_index_id: GlobalId,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            _typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            exported_index_id,
            config,
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
    pub fn new(
        name: &QualifiedItemName,
        on: &GlobalId,
        keys: &Vec<mz_expr::MirScalarExpr>,
    ) -> Self {
        Self {
            name: name.clone(),
            on: on.clone(),
            keys: keys.clone(),
        }
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
        let state = self.catalog.state();
        let on_entry = state.get_entry(&index.on);
        let full_name = state.resolve_full_name(&index.name, on_entry.conn_id());
        let on_desc = on_entry
            .desc(&full_name)
            .expect("can only create indexes on items with a valid description");

        let mut df_builder = {
            let catalog = self.catalog.state();
            let compute = self.compute_instance.clone();
            DataflowBuilder::new(catalog, compute).with_config(&self.config)
        };
        let mut df_desc = MirDataflowDescription::new(full_name.to_string());

        df_builder.import_into_dataflow(&index.on, &mut df_desc)?;
        df_builder.maybe_reoptimize_imported_views(&mut df_desc, &self.config)?;

        for desc in df_desc.objects_to_build.iter_mut() {
            prep_relation_expr(&mut desc.plan, ExprPrepStyle::Index)?;
        }

        let mut index_desc = IndexDesc {
            on_id: index.on,
            key: index.keys.clone(),
        };

        for key in index_desc.key.iter_mut() {
            prep_scalar_expr(key, ExprPrepStyle::Index)?;
        }

        df_desc.export_index(self.exported_index_id, index_desc, on_desc.typ().clone());

        // Optimize the dataflow across views, and any other ways that appeal.
        let mut df_meta = mz_transform::optimize_dataflow(
            &mut df_desc,
            &df_builder,
            &mz_transform::EmptyStatisticsOracle,
            self.config.enable_eager_delta_joins,
        )?;

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
            .filter(|(_id, idx)| idx.keys == index.keys)
        {
            df_meta.push_optimizer_notice_dedup(IndexAlreadyExists {
                index_id,
                index_key: idx.keys.clone(),
                index_on_id: idx.on,
            });
        }

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
        } = plan;

        // Ensure all expressions are normalized before finalizing.
        for build in df_desc.objects_to_build.iter_mut() {
            normalize_lets(&mut build.plan.0)?
        }

        // Finalize the dataflow. This includes:
        // - MIR ⇒ LIR lowering
        // - LIR ⇒ LIR transforms
        let df_desc = Plan::finalize_dataflow(
            df_desc,
            self.config.enable_consolidate_after_union_negate,
            self.config.enable_reduce_mfp_fusion,
        )
        .map_err(OptimizerError::Internal)?;

        // Trace the pipeline output under `optimize`.
        trace_plan(&df_desc);

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
