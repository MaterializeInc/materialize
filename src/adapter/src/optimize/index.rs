// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE INDEX` statements.

use std::sync::Arc;

use maplit::btreemap;
use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_compute_types::ComputeInstanceId;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::QualifiedItemName;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::optimizer_notices::OptimizerNotice;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use timely::progress::Antichain;

use crate::catalog::Catalog;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::CollectionIdBundle;

pub struct OptimizeIndex {
    /// A typechecking context to use throughout the optimizer pipeline.
    _typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the compute instance that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported index arrangement.
    exported_index_id: GlobalId,
    // Optimizer config.
    config: OptimizerConfig,
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
#[derive(Clone)]
pub struct GlobalMirPlan<T: Clone> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    ts_info: T,
}

/// Timestamp information type for [`GlobalMirPlan`] structs representing an
/// optimization result without a resolved timestamp.
#[derive(Clone)]
pub struct Unresolved {
    compute_instance_id: ComputeInstanceId,
}

/// Timestamp information type for [`GlobalMirPlan`] structs representing an
/// optimization result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`MirDataflowDescription`] of the
/// surrounding [`GlobalMirPlan`] when we call `resolve()`.
#[derive(Clone)]
pub struct Resolved;

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone)]
pub struct GlobalLirPlan {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl OptimizeIndex {
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

impl Optimize<Index> for OptimizeIndex {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize(&mut self, index: Index) -> Result<Self::To, OptimizerError> {
        let state = self.catalog.state();
        let on_entry = state.get_entry(&index.on);
        let full_name = state.resolve_full_name(&index.name, on_entry.conn_id());
        let on_desc = on_entry
            .desc(&full_name)
            .expect("can only create indexes on items with a valid description");

        let mut df_builder = DataflowBuilder::new(state, self.compute_instance.clone());
        let mut df_desc = MirDataflowDescription::new(full_name.to_string());

        df_builder.import_into_dataflow(&index.on, &mut df_desc)?;
        df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

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
        )?;

        if index.keys.is_empty() {
            df_meta.push_optimizer_notice_dedup(OptimizerNotice::IndexKeyEmpty);
        }

        // Return.
        Ok(GlobalMirPlan {
            df_desc,
            df_meta,
            ts_info: Unresolved {
                compute_instance_id: self.compute_instance.instance_id(),
            },
        })
    }
}

impl<T: Clone> GlobalMirPlan<T> {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}

impl GlobalMirPlan<Unresolved> {
    /// Produces the [`GlobalMirPlan`] with [`Resolved`] timestamp required for
    /// the next stage.
    pub fn resolve(mut self, as_of: Antichain<Timestamp>) -> GlobalMirPlan<Resolved> {
        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(as_of);

        GlobalMirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            ts_info: Resolved,
        }
    }

    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self) -> CollectionIdBundle {
        let storage_ids = self.df_desc.source_imports.keys().copied().collect();
        let compute_ids = self.df_desc.index_imports.keys().copied().collect();
        CollectionIdBundle {
            storage_ids,
            compute_ids: btreemap! {self.compute_instance_id() => compute_ids},
        }
    }

    /// Returns the [`ComputeInstanceId`] against which we should resolve the
    /// timestamp for the next stage.
    pub fn compute_instance_id(&self) -> ComputeInstanceId {
        self.ts_info.compute_instance_id
    }
}

impl Optimize<GlobalMirPlan<Resolved>> for OptimizeIndex {
    type To = GlobalLirPlan;

    fn optimize(&mut self, plan: GlobalMirPlan<Resolved>) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            ts_info: _,
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
            self.config.enable_specialized_arrangements,
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan { df_desc, df_meta })
    }
}

impl GlobalLirPlan {
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }

    pub fn df_desc(&self) -> &LirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}
