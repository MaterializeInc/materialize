// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `SELECT` statements.

use std::fmt::Debug;
use std::sync::Arc;

use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_compute_types::ComputeInstanceId;
use mz_expr::{MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, RelationType, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::{Optimizer as TransformOptimizer, StatisticsOracle};
use timely::progress::Antichain;
use tracing::{span, warn, Level};

use crate::catalog::Catalog;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, EvalTime,
    ExprPrepStyle,
};
use crate::coord::peek::{create_fast_path_plan, FastPathPlan};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizeMode, OptimizerConfig,
    OptimizerError,
};
use crate::session::Session;

pub struct OptimizePeek {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the compute instance that will run the dataflows.
    pub compute_instance: ComputeInstanceSnapshot,
    /// Optional row-set finishing to be applied to the final result.
    pub finishing: RowSetFinishing,
    /// A transient GlobalId to be used when constructing the dataflow.
    pub select_id: GlobalId,
    /// A transient GlobalId to be used when constructing a PeekPlan.
    pub index_id: GlobalId,
    // Optimizer config.
    config: OptimizerConfig,
}

// A bogey `Debug` implementation that hides fields. This is needed to make the
// `event!` call in `sequence_peek_stage` not emit a lot of data.
//
// For now, we skip almost all fields, but we might revisit that bit if it turns
// out that we really need those for debugging purposes.
impl Debug for OptimizePeek {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizePeek")
            .field("config", &self.config)
            .finish()
    }
}

/// Placeholder type for [`LocalMirPlan`] and [`GlobalMirPlan`] structs
/// representing an optimization result without context.
pub struct Unresolved;

/// The (sealed intermediate) result after HIR ⇒ MIR lowering and decorrelation
/// and MIR optimization.
#[derive(Clone)]
pub struct LocalMirPlan<T = Unresolved> {
    expr: MirRelationExpr,
    cx_info: T,
}

impl<T> LocalMirPlan<T> {
    pub fn expr(&self) -> &MirRelationExpr {
        &self.expr
    }
}

/// Context information type for [`LocalMirPlan`] structs representing an
/// optimization result with a resolved timestamp and an enclosing [`Session`].
pub struct ResolvedLocal<'s> {
    stats: Box<dyn StatisticsOracle>,
    session: &'s Session,
}

/// The (sealed intermediate) result after:
///
/// 1. embedding a [`LocalMirPlan`] into a [`MirDataflowDescription`],
/// 2. transitively inlining referenced views, and
/// 3. jointly optimizing the `MIR` plans in the [`MirDataflowDescription`].
#[derive(Clone)]
pub struct GlobalMirPlan<T = Unresolved> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    cx_info: T,
}

impl GlobalMirPlan {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}

impl Debug for GlobalMirPlan<Unresolved> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalMirPlan")
            .field("df_desc", &self.df_desc)
            .field("df_meta", &self.df_meta)
            .finish()
    }
}

/// Context information type for [`GlobalMirPlan`] structs representing an
/// optimization result with a resolved timestamp and an enclosing [`Session`].
#[derive(Clone)]
pub struct ResolvedGlobal<'s> {
    session: &'s Session,
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
pub enum GlobalLirPlan {
    FastPath {
        plan: FastPathPlan,
        df_meta: DataflowMetainfo,
        typ: RelationType,
    },
    SlowPath {
        df_desc: LirDataflowDescription,
        df_meta: DataflowMetainfo,
        typ: RelationType,
    },
}

impl GlobalLirPlan {
    /// Return the output type for this [`GlobalLirPlan`].
    pub fn typ(&self) -> &RelationType {
        match self {
            Self::FastPath { typ, .. } => typ,
            Self::SlowPath { typ, .. } => typ,
        }
    }

    /// Return the default output key for this [`GlobalLirPlan`].
    pub fn key(&self) -> Vec<MirScalarExpr> {
        self.typ()
            .default_key()
            .into_iter()
            .map(MirScalarExpr::Column)
            .collect()
    }
}

impl OptimizePeek {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        finishing: RowSetFinishing,
        select_id: GlobalId,
        index_id: GlobalId,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            finishing,
            select_id,
            index_id,
            config,
        }
    }

    pub fn cluster_id(&self) -> ComputeInstanceId {
        self.compute_instance.instance_id()
    }

    pub fn index_id(&self) -> GlobalId {
        self.index_id
    }
}

impl Optimize<HirRelationExpr> for OptimizePeek {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // HIR ⇒ MIR lowering and decorrelation
        let expr = expr.lower(&self.config)?;

        // MIR ⇒ MIR optimization (local)
        self.optimize(expr)
    }
}

impl Optimize<MirRelationExpr> for OptimizePeek {
    type To = LocalMirPlan;

    fn optimize(&mut self, expr: MirRelationExpr) -> Result<Self::To, OptimizerError> {
        // MIR ⇒ MIR optimization (local)
        let expr = span!(target: "optimizer", Level::DEBUG, "local").in_scope(|| {
            let optimizer = TransformOptimizer::logical_optimizer(&self.typecheck_ctx);
            let expr = optimizer.optimize(expr)?.into_inner();

            // Trace the result of this phase.
            trace_plan(&expr);

            Ok::<_, OptimizerError>(expr)
        })?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(LocalMirPlan {
            expr,
            cx_info: Unresolved,
        })
    }
}

impl LocalMirPlan<Unresolved> {
    /// Produces the [`LocalMirPlan`] with [`ResolvedLocal`] contextual
    /// information required for the next stage.
    pub fn resolve(
        self,
        session: &Session,
        stats: Box<dyn StatisticsOracle>,
    ) -> LocalMirPlan<ResolvedLocal> {
        LocalMirPlan {
            expr: self.expr,
            cx_info: ResolvedLocal { session, stats },
        }
    }
}

impl<'s> Optimize<LocalMirPlan<ResolvedLocal<'s>>> for OptimizePeek {
    type To = GlobalMirPlan<Unresolved>;

    fn optimize(
        &mut self,
        plan: LocalMirPlan<ResolvedLocal<'s>>,
    ) -> Result<Self::To, OptimizerError> {
        let LocalMirPlan {
            expr,
            cx_info: ResolvedLocal { stats, session },
        } = plan;

        let expr = OptimizedMirRelationExpr(expr);

        // We create a dataflow and optimize it, to determine if we can avoid building it.
        // This can happen if the result optimizes to a constant, or to a `Get` expression
        // around a maintained arrangement.
        let typ = expr.typ();
        let key = typ
            .default_key()
            .iter()
            .map(|k| MirScalarExpr::Column(*k))
            .collect();

        // The assembled dataflow contains a view and an index of that view.
        let mut df_builder =
            DataflowBuilder::new(self.catalog.state(), self.compute_instance.clone());

        let debug_name = format!("oneshot-select-{}", self.select_id);
        let mut df_desc = MirDataflowDescription::new(debug_name.to_string());

        df_builder.import_view_into_dataflow(&self.select_id, &expr, &mut df_desc)?;
        df_builder.reoptimize_imported_views(&mut df_desc, &self.config)?;

        // Resolve all unmaterializable function calls except mz_now(), because
        // we don't yet have a timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session,
            catalog_state: self.catalog.state(),
        };
        df_desc.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        // TODO(aalexandrov): Instead of conditioning here we should really
        // reconsider how to render multi-plan peek dataflows. The main
        // difficulty here is rendering the optional finishing bit.
        if self.config.mode != OptimizeMode::Explain {
            df_desc.export_index(
                self.index_id,
                IndexDesc {
                    on_id: self.select_id,
                    key,
                },
                typ,
            );
        }

        let df_meta = mz_transform::optimize_dataflow(&mut df_desc, &df_builder, &*stats)?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan {
            df_desc,
            df_meta,
            cx_info: Unresolved,
        })
    }
}

impl GlobalMirPlan<Unresolved> {
    /// Produces the [`GlobalMirPlan`] with [`ResolvedGlobal`] contextual
    /// information required for the next stage.
    ///
    /// We need to resolve timestamps before the `GlobalMirPlan ⇒ GlobalLirPlan`
    /// optimization stage in order to profit from possible single-time
    /// optimizations in the `Plan::finalize_dataflow` call.
    pub fn resolve(
        mut self,
        as_of: Antichain<Timestamp>,
        session: &Session,
    ) -> GlobalMirPlan<ResolvedGlobal> {
        // Set the `as_of` and `until` timestamps for the dataflow.
        self.df_desc.set_as_of(as_of.clone());

        // Use the the opportunity to name an `until` frontier that will prevent
        // work we needn't perform. By default, `until` will be
        // `Antichain::new()`, which prevents no updates and is safe.
        if let Some(as_of) = as_of.as_option() {
            if let Some(until) = as_of.checked_add(1) {
                self.df_desc.until = Antichain::from_elem(until);
            } else {
                warn!(as_of = %as_of, "as_of + 1 overflow");
            }
        }

        GlobalMirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            cx_info: ResolvedGlobal { session },
        }
    }
}

impl<'s> Optimize<GlobalMirPlan<ResolvedGlobal<'s>>> for OptimizePeek {
    type To = GlobalLirPlan;

    fn optimize(
        &mut self,
        plan: GlobalMirPlan<ResolvedGlobal<'s>>,
    ) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            cx_info: ResolvedGlobal { session },
        } = plan;

        // Get the single timestamp representing the `as_of` time.
        let as_of = df_desc
            .as_of
            .clone()
            .expect("as_of antichain")
            .into_option()
            .expect("unique as_of element");

        // Resolve all unmaterializable function calls including mz_now().
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(as_of),
            session,
            catalog_state: self.catalog.state(),
        };
        df_desc.visit_children(
            |r| prep_relation_expr(r, style),
            |s| prep_scalar_expr(s, style),
        )?;

        // TODO: grab this bit from the index exports once it's always
        // created.
        let typ = df_desc
            .objects_to_build
            .iter()
            .find(|build_desc| build_desc.id == self.select_id)
            .map(|x| x.plan.typ())
            .expect("oneshot-select type");

        match create_fast_path_plan(
            &mut df_desc,
            self.select_id,
            Some(&self.finishing),
            self.config.persist_fast_path_limit,
        )? {
            Some(plan) => {
                // An ugly way to prevent panics when explaining the physical
                // plan of a fast-path query. TODO: get rid of this.
                if self.config.mode == OptimizeMode::Explain {
                    // Finalize the dataflow. This includes:
                    // - MIR ⇒ LIR lowering
                    // - LIR ⇒ LIR transforms
                    let _ = Plan::<Timestamp>::finalize_dataflow(
                        df_desc,
                        self.config.enable_consolidate_after_union_negate,
                        self.config.enable_specialized_arrangements,
                    )
                    .map_err(OptimizerError::Internal)?;
                }

                // Return a variant indicating that we should use a fast path.
                Ok(GlobalLirPlan::FastPath { plan, df_meta, typ })
            }
            None => {
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

                // Return a variant indicating that we should use a slow path.
                Ok(GlobalLirPlan::SlowPath {
                    df_desc,
                    df_meta,
                    typ,
                })
            }
        }
    }
}
