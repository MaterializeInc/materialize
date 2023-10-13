// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer implementation for `SELECT` statements.

#![allow(unused)] // TODO: remove

use std::sync::Arc;

use mz_compute_types::dataflows::IndexDesc;
use mz_compute_types::plan::Plan;
use mz_expr::{MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing};
use mz_repr::explain::trace_plan;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::plan::HirRelationExpr;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::normalize_lets::normalize_lets;
use mz_transform::typecheck::{empty_context, SharedContext as TypecheckContext};
use mz_transform::{EmptyStatisticsOracle, Optimizer as TransformOptimizer};
use tracing::{span, Level};

use crate::catalog::Catalog;
use crate::coord::dataflows::{
    prep_relation_expr, prep_scalar_expr, ComputeInstanceSnapshot, DataflowBuilder, EvalTime,
    ExprPrepStyle,
};
use crate::coord::peek::{create_fast_path_plan, FastPathPlan};
use crate::optimize::{
    LirDataflowDescription, MirDataflowDescription, Optimize, OptimizerConfig, OptimizerError,
};
use crate::session::Session;
use crate::TimestampContext;

pub struct OptimizePeek<'ctx> {
    /// A typechecking context to use throughout the optimizer pipeline.
    typecheck_ctx: TypecheckContext,
    /// A snapshot of the catalog state.
    catalog: Arc<Catalog>,
    /// A snapshot of the compute instance that will run the dataflows.
    compute_instance: ComputeInstanceSnapshot,
    /// Optional row-set finishing to be applied to the final result.
    finishing: Option<&'ctx RowSetFinishing>,
    /// A transient GlobalId to be used when constructing the dataflow.
    select_id: GlobalId,
    /// A transient GlobalId to be used when constructing a PeekPlan.
    index_id: GlobalId,
    /// An instance of the session that in which the optimizer will run.
    session: &'ctx mut Session,
    // Optimizer config.
    config: OptimizerConfig,
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
pub struct GlobalMirPlan<T> {
    df_desc: MirDataflowDescription,
    df_meta: DataflowMetainfo,
    ts_info: T,
}

/// Timestamp information type for [`GlobalMirPlan`] structs representing an
/// optimization result without a resolved timestamp.
pub struct Unresolved<'ctx> {
    session: &'ctx mut Session,
}

/// Timestamp information type for [`GlobalMirPlan`] structs representing an
/// optimization result with a resolved timestamp.
#[derive(Clone)]
pub struct Resolved {
    timestamp_ctx: TimestampContext<Timestamp>,
}

/// The (final) result after MIR ⇒ LIR lowering and optimizing the resulting
/// `DataflowDescription` with `LIR` plans.
pub enum GlobalLirPlan {
    FastPath {
        plan: FastPathPlan,
        df_meta: DataflowMetainfo,
    },
    SlowPath {
        df_desc: LirDataflowDescription,
        df_meta: DataflowMetainfo,
    },
}

impl<'ctx> OptimizePeek<'ctx> {
    pub fn new(
        catalog: Arc<Catalog>,
        compute_instance: ComputeInstanceSnapshot,
        finishing: Option<&'ctx RowSetFinishing>,
        select_id: GlobalId,
        index_id: GlobalId,
        session: &'ctx mut Session,
        config: OptimizerConfig,
    ) -> Self {
        Self {
            typecheck_ctx: empty_context(),
            catalog,
            compute_instance,
            finishing,
            select_id,
            index_id,
            session,
            config,
        }
    }
}

impl<'ctx> Optimize<'static, HirRelationExpr> for OptimizePeek<'ctx> {
    type To = LocalMirPlan;

    fn optimize<'s: 'ctx>(&'s mut self, expr: HirRelationExpr) -> Result<Self::To, OptimizerError> {
        // HIR ⇒ MIR lowering and decorrelation
        let config = mz_sql::plan::OptimizerConfig {};
        let expr = expr.optimize_and_lower(&config)?;

        // MIR ⇒ MIR optimization (local)
        let expr = span!(target: "optimizer", Level::TRACE, "local").in_scope(|| {
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

impl<'ctx> Optimize<'ctx, LocalMirPlan> for OptimizePeek<'ctx> {
    type To = GlobalMirPlan<Unresolved<'ctx>>;

    fn optimize<'s: 'ctx>(&'s mut self, plan: LocalMirPlan) -> Result<Self::To, OptimizerError> {
        let LocalMirPlan { expr } = plan;

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

        let mut df_desc = MirDataflowDescription::new("explanation".to_string());

        df_builder.import_view_into_dataflow(
            &self.select_id,
            &OptimizedMirRelationExpr(expr),
            &mut df_desc,
        )?;

        // Resolve all unmaterializable function calls except mz_now(), because we don't yet have a
        // timestamp.
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Deferred,
            session: self.session,
        };
        df_desc.visit_children(
            |r| prep_relation_expr(self.catalog.state(), r, style),
            |s| prep_scalar_expr(self.catalog.state(), s, style),
        )?;

        df_desc.export_index(
            self.index_id,
            IndexDesc {
                on_id: self.select_id,
                key,
            },
            typ,
        );

        // TODO: proper stats needs exact timestamp at the moment.
        // However, we don't want to get it so early.
        let stats = EmptyStatisticsOracle;
        let df_meta = mz_transform::optimize_dataflow(&mut df_desc, &df_builder, &stats)?;

        // Return the (sealed) plan at the end of this optimization step.
        Ok(GlobalMirPlan {
            df_desc,
            df_meta,
            ts_info: Unresolved {
                session: self.session,
            },
        })
    }
}

impl<T> GlobalMirPlan<T> {
    pub fn df_desc(&self) -> &MirDataflowDescription {
        &self.df_desc
    }

    pub fn df_meta(&self) -> &DataflowMetainfo {
        &self.df_meta
    }
}

impl<'ctx> GlobalMirPlan<Unresolved<'ctx>> {
    /// Produces the [`GlobalMirPlan`] with [`Resolved`] timestamp required for
    /// the next stage.
    pub fn into_timestamp_plan(
        mut self,
        ctx: TimestampContext<Timestamp>,
    ) -> GlobalMirPlan<Resolved> {
        // Set the `as_of` timestamp for the dataflow.
        self.df_desc.set_as_of(ctx.antichain());

        GlobalMirPlan {
            df_desc: self.df_desc,
            df_meta: self.df_meta,
            ts_info: Resolved { timestamp_ctx: ctx },
        }
    }

    /// Get the [`Session`] of the [`Optimize`] instance that produced this
    /// plan.
    pub fn session(&mut self) -> &mut Session {
        self.ts_info.session
    }
}

impl<'ctx> Optimize<'ctx, GlobalMirPlan<Resolved>> for OptimizePeek<'ctx> {
    type To = GlobalLirPlan;

    fn optimize<'s: 'ctx>(
        &'s mut self,
        plan: GlobalMirPlan<Resolved>,
    ) -> Result<Self::To, OptimizerError> {
        let GlobalMirPlan {
            mut df_desc,
            df_meta,
            ts_info: Resolved { timestamp_ctx },
        } = plan;

        // Resolve all unmaterializable function calls including mz_now().
        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::Time(timestamp_ctx.timestamp_or_default()),
            session: self.session,
        };
        df_desc.visit_children(
            |r| prep_relation_expr(self.catalog.state(), r, style),
            |s| prep_scalar_expr(self.catalog.state(), s, style),
        )?;

        match create_fast_path_plan(
            &mut df_desc,
            self.select_id,
            self.finishing,
            self.config.persist_fast_path_limit,
        )? {
            Some(plan) => {
                // Return a variant indicating that we should use a fast path.
                Ok(GlobalLirPlan::FastPath { plan, df_meta })
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
                    self.config.enable_monotonic_oneshot_selects,
                    self.config.enable_specialized_arrangements,
                )
                .map_err(OptimizerError::Internal)?;

                // Return the plan at the end of this `optimize` step.
                Ok(GlobalLirPlan::SlowPath { df_desc, df_meta })
            }
        }
    }
}
