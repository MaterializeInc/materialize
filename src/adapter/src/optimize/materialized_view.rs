// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `CREATE MATERIALIZED VIEW` statements.

use std::marker::PhantomData;
use std::sync::Arc;

use maplit::btreemap;
use mz_compute_types::dataflows::BuildDesc;
use mz_compute_types::plan::Plan;
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, PersistSinkConnection};
use mz_compute_types::ComputeInstanceId;
use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
use mz_repr::explain::trace_plan;
use mz_repr::{ColumnName, GlobalId, RelationDesc, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::Optimizer as TransformOptimizer;
use timely::progress::Antichain;
use tracing::{span, Level};

use crate::catalog::Catalog;
use crate::coord::dataflows::{
    prep_relation_expr, ComputeInstanceSnapshot, DataflowBuilder, ExprPrepStyle,
};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::CollectionIdBundle;

pub struct Optimizer {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the cluster that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// A durable GlobalId to be used with the exported materialized view sink.
    exported_sink_id: GlobalId,
    /// A transient GlobalId to be used when constructing the dataflow.
    internal_view_id: GlobalId,
    /// The resulting column names.
    column_names: Vec<ColumnName>,
    /// Output columns that are asserted to be not null in the `CREATE VIEW`
    /// statement.
    non_null_assertions: Vec<usize>,
    /// A human-readable name exposed internally (useful for debugging).
    debug_name: String,
    // Optimizer config.
    config: OptimizerConfig,
}

impl Optimizer {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        exported_sink_id: GlobalId,
        internal_view_id: GlobalId,
        column_names: Vec<ColumnName>,
        non_null_assertions: Vec<usize>,
        debug_name: String,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            exported_sink_id,
            internal_view_id,
            column_names,
            non_null_assertions,
            debug_name,
            config,
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }
}

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
#[derive(Clone)]
pub struct LocalMirPlan {
    expr: MirRelationExpr,
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`LocalMirPlan`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone)]
pub struct GlobalMirPlan {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
}

impl GlobalMirPlan {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
#[derive(Clone)]
pub struct GlobalLirPlan<T: Clone> {
    df_desc: LirDataflowDescription,
    df_meta: DataflowMetainfo,
    phantom: PhantomData<T>,
}

impl<T: Clone> GlobalLirPlan<T> {
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

    /// Computes the [`CollectionIdBundle`] of the wrapped dataflow.
    pub fn id_bundle(&self, compute_instance_id: ComputeInstanceId) -> CollectionIdBundle {
        let storage_ids = self.df_desc.source_imports.keys().copied().collect();
        let compute_ids = self.df_desc.index_imports.keys().copied().collect();
        CollectionIdBundle {
            storage_ids,
            compute_ids: btreemap! {compute_instance_id => compute_ids},
        }
    }
}

/// Marker type for [`GlobalLirPlan`] structs representing an optimization
/// result without a resolved timestamp.
#[derive(Clone)]
pub struct Unresolved;

/// Marker type for [`GlobalLirPlan`] structs representing an optimization
/// result with a resolved timestamp.
///
/// The actual timestamp value is set in the [`LirDataflowDescription`] of the
/// surrounding [`GlobalLirPlan`] when we call `resolve()`.
#[derive(Clone)]
pub struct Resolved;

impl Optimize<HirRelationExpr> for Optimizer {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        let expr = span!(target: "optimizer", Level::DEBUG, "local").in_scope(|| {
            let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
            let expr = optimizer.optimize(expr)?.into_inner();

            // Trace the result of this phase.
            trace_plan(&expr);

            Ok::<_, OptimizerError>(expr)
        })?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalMirPlan { expr })
    }
}

impl LocalMirPlan {
    pub fn expr(&self) -> OptimizedMirRelationExpr {
        OptimizedMirRelationExpr(self.expr.clone())
    }
}

/// This is needed only because the pipeline in the bootstrap code starts from an
/// [`OptimizedMirRelationExpr`] attached to a [`crate::catalog::CatalogItem`].
impl Optimize<OptimizedMirRelationExpr> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, expr: OptimizedMirRelationExpr) -> Result<Self::To, OptimizerError> {
        let expr = expr.into_inner();
        self.optimize(LocalMirPlan { expr })
    }
}

impl Optimize<LocalMirPlan> for Optimizer {
    type To = GlobalMirPlan;

    fn optimize(&mut self, plan: LocalMirPlan) -> Result<Self::To, OptimizerError> {
        let expr = OptimizedMirRelationExpr(plan.expr);

        let mut rel_typ = expr.typ();
        for &i in self.non_null_assertions.iter() {
            rel_typ.column_types[i].nullable = false;
        }
        let rel_desc = RelationDesc::new(rel_typ, self.column_names.clone());

        let mut df_builder =
            DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());
        let mut df_desc = MirDataflowDescription::new(self.debug_name.clone());

        df_builder.import_view_into_dataflow(&self.internal_view_id, &expr, &mut df_desc)?;
        df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

        for BuildDesc { plan, .. } in &mut df_desc.objects_to_build {
            prep_relation_expr(self.catalog.state(), plan, ExprPrepStyle::Index)?;
        }

        let sink_description = ComputeSinkDesc {
            from: self.internal_view_id,
            from_desc: rel_desc.clone(),
            connection: ComputeSinkConnection::Persist(PersistSinkConnection {
                value_desc: rel_desc,
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::default(),
            non_null_assertions: self.non_null_assertions.clone(),
        };

        let df_meta = df_builder.build_sink_dataflow_into(
            &mut df_desc,
            self.exported_sink_id,
            sink_description,
        )?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan { df_desc, df_meta })
    }
}

impl Optimize<GlobalMirPlan> for Optimizer {
    type To = GlobalLirPlan<Unresolved>;

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
            self.config.enable_specialized_arrangements,
        )
        .map_err(OptimizerError::Internal)?;

        // Return the plan at the end of this `optimize` step.
        Ok(GlobalLirPlan {
            df_desc,
            df_meta,
            phantom: PhantomData::<Unresolved>,
        })
    }
}

impl GlobalLirPlan<Unresolved> {
    /// Produces the [`GlobalLirPlan`] with [`Resolved`] timestamp.
    pub fn resolve(mut self, as_of: Antichain<Timestamp>) -> GlobalLirPlan<Resolved> {
        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(as_of);

        // The only dataflow exports are sinks with an empty `up_to` frontier,
        // so the `df_desc.until` should also be the empty frontier.
        assert!(self.df_desc.index_exports.is_empty());
        for (_, sink) in &self.df_desc.sink_exports {
            assert!(sink.up_to.is_empty());
        }
        // The `until` is set to the empty frontier in the `df_desc` constructor.
        assert!(self.df_desc.until.is_empty());

        GlobalLirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            phantom: PhantomData::<Resolved>,
        }
    }
}

impl GlobalLirPlan<Resolved> {
    /// Unwraps the parts of the final result of the optimization pipeline.
    pub fn unapply(self) -> (LirDataflowDescription, DataflowMetainfo) {
        (self.df_desc, self.df_meta)
    }
}
