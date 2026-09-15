// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Optimizer interface to the adapter and coordinator code.
//!
//! The goal of this crate is to abstract optimizer specifics behind a
//! high-level interface that is ready to be consumed by the coordinator code in
//! a future-proof way (that is, the API is taking the upcoming evolution of
//! these components into account).
//!
//! The contents of this crate should have minimal dependencies to the rest of
//! the coordinator code so we can pull them out as a separate crate in the
//! future without too much effort.
//!
//! The main type in this module is a very simple [`Optimize`] trait which
//! allows us to adhere to the following principles:
//!
//! - Implementors of this trait are structs that encapsulate all context
//!   required to optimize a statement of type `T` end-to-end (for example
//!   [`materialized_view::Optimizer`] for `T` = `MaterializedView`).
//! - Each struct implements [`Optimize`] once for each optimization stage. The
//!   `From` type represents the input of the stage and `Self::To` the
//!   associated stage output. This allows to have more than one entrypoints to
//!   a pipeline.
//! - The concrete types used for stage results are opaque structs that are
//!   specific to the pipeline of that statement type.
//!   - We use different structs even if two statement types might have
//!     structurally identical intermediate results. This ensures that client
//!     code cannot first execute some optimization stages for one type and then
//!     some stages for a different type.
//!   - The only way to construct such a struct is by running the [`Optimize`]
//!     stage that produces it. This ensures that client code cannot interfere
//!     with the pipeline.
//!   - In general, the internals of these structs can be accessed only behind a
//!     shared reference. This ensures that client code can look up information
//!     from intermediate stages but cannot modify it.
//!   - Timestamp selection is modeled as a conversion between structs that are
//!     adjacent in the pipeline using a method called `resolve`.
//!   - The struct representing the result of the final stage of the
//!     optimization pipeline can be destructed to access its internals with a
//!     method called `unapply`.
//! - The `Send` trait bounds on the `Self` and `From` types ensure that
//!   [`Optimize`] instances can be passed to different threads (this is
//!   required of off-thread optimization).
//!
//! For details, see the `20230714_optimizer_interface.md` design doc in this
//! repository.

pub mod copy_to;
pub mod dataflows;
pub mod index;
pub mod materialized_view;
pub mod metric_sink;
pub mod peek;
pub mod subscribe;
pub mod view;

use std::fmt::Debug;

use mz_adapter_types::connection::ConnectionId;
use mz_catalog::memory::objects::{CatalogCollectionEntry, CatalogEntry, Index};
use mz_compute_types::ComputeInstanceId;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::LirRelationExpr;
use mz_controller_types::ClusterId;
use mz_expr::OptimizedMirRelationExpr;
use mz_repr::optimize::OverrideFrom;
use mz_repr::{CatalogItemId, GlobalId};
use mz_sql::names::{FullItemName, QualifiedItemName};
use mz_sql::plan::HirRelationExpr;
use mz_sql::session::metadata::SessionMetadata;
use mz_transform::StatisticsOracle;

pub use mz_catalog::optimize::{
    Optimize, OptimizeMode, OptimizerConfig, OptimizerError, optimize_mir_local, trace_plan,
};

use crate::TimestampContext;
use crate::coord::ExplainContext;

// Alias types
// -----------

/// A type for a [`DataflowDescription`] backed by `Mir~` plans. Used internally
/// by the optimizer implementations.
type MirDataflowDescription = DataflowDescription<OptimizedMirRelationExpr>;
/// A type for a [`DataflowDescription`] backed by `Lir~` plans.
pub type LirDataflowDescription = DataflowDescription<LirRelationExpr>;

// One-shot peek optimizer dispatch
// --------------------------------

/// The optimizer driving a one-shot statement through the peek sequencing state
/// machine.
///
/// `SELECT` and `EXPLAIN` are optimized by the [`peek::Optimizer`], while `COPY
/// TO` is optimized by the [`copy_to::Optimizer`]. Both share the same
/// surrounding state machine (timestamp selection, read holds, off-thread
/// optimization, …), so this enum lets that shared machinery carry either
/// optimizer without caring which one it is. The variants are kept distinct
/// (rather than abstracted behind a trait object) because the downstream stages
/// need to recover the concrete optimizer and its statement-specific result.
#[derive(Debug)]
pub enum PeekOptimizer {
    /// Optimizer for `SELECT` and `EXPLAIN` statements.
    Select(peek::Optimizer),
    /// Optimizer for `COPY TO` statements.
    CopyTo(copy_to::Optimizer),
}

/// The global LIR plan produced by [`PeekOptimizer::optimize`], tagged with the
/// path that produced it.
#[derive(Debug)]
pub enum PeekGlobalLirPlan {
    /// The result of the `SELECT`/`EXPLAIN` pipeline.
    Select(peek::GlobalLirPlan),
    /// The result of the `COPY TO` pipeline.
    CopyTo(copy_to::GlobalLirPlan),
}

impl PeekOptimizer {
    /// The cluster that will run the optimized dataflow.
    pub fn cluster_id(&self) -> ComputeInstanceId {
        match self {
            PeekOptimizer::Select(optimizer) => optimizer.cluster_id(),
            PeekOptimizer::CopyTo(optimizer) => optimizer.cluster_id(),
        }
    }

    /// Runs the full one-shot optimization pipeline end-to-end:
    ///
    /// 1. HIR ⇒ MIR lowering and local MIR optimization,
    /// 2. timestamp resolution,
    /// 3. global MIR optimization, MIR ⇒ LIR lowering, and global LIR
    ///    optimization.
    ///
    /// The pipeline shape is identical for both variants; only the concrete
    /// (statement-specific) plan types differ, so the steps are shared via
    /// [`optimize_oneshot`].
    pub fn optimize(
        &mut self,
        raw_expr: HirRelationExpr,
        timestamp_ctx: TimestampContext,
        session: &dyn SessionMetadata,
        stats: Box<dyn StatisticsOracle>,
    ) -> Result<PeekGlobalLirPlan, OptimizerError> {
        match self {
            PeekOptimizer::Select(optimizer) => {
                let plan = optimize_oneshot(optimizer, raw_expr, |local_mir_plan| {
                    local_mir_plan.resolve(timestamp_ctx, session, stats)
                })?;
                Ok(PeekGlobalLirPlan::Select(plan))
            }
            PeekOptimizer::CopyTo(optimizer) => {
                let plan = optimize_oneshot(optimizer, raw_expr, |local_mir_plan| {
                    local_mir_plan.resolve(timestamp_ctx, session, stats)
                })?;
                Ok(PeekGlobalLirPlan::CopyTo(plan))
            }
        }
    }

    /// Consumes `self`, returning the inner [`peek::Optimizer`] if this is the
    /// `SELECT`/`EXPLAIN` path and `None` otherwise.
    pub fn into_select(self) -> Option<peek::Optimizer> {
        match self {
            PeekOptimizer::Select(optimizer) => Some(optimizer),
            PeekOptimizer::CopyTo(_) => None,
        }
    }

    /// Consumes `self`, returning the inner [`copy_to::Optimizer`] if this is
    /// the `COPY TO` path and `None` otherwise.
    pub fn into_copy_to(self) -> Option<copy_to::Optimizer> {
        match self {
            PeekOptimizer::CopyTo(optimizer) => Some(optimizer),
            PeekOptimizer::Select(_) => None,
        }
    }
}

/// Runs the shared one-shot optimization pipeline for a single optimizer.
///
/// This factors out the (otherwise duplicated) HIR ⇒ local MIR ⇒ resolve ⇒
/// global LIR sequence that is common to the `SELECT`/`EXPLAIN` and `COPY TO`
/// paths. The `resolve` closure attaches the timestamp/session/stats context to
/// the local plan; it is path-specific only in the concrete plan type it
/// operates on.
pub(crate) fn optimize_oneshot<O, LocalPlan, ResolvedPlan, GlobalPlan>(
    optimizer: &mut O,
    raw_expr: HirRelationExpr,
    resolve: impl FnOnce(LocalPlan) -> ResolvedPlan,
) -> Result<GlobalPlan, OptimizerError>
where
    O: Optimize<HirRelationExpr, To = LocalPlan> + Optimize<ResolvedPlan, To = GlobalPlan>,
{
    // HIR ⇒ MIR lowering and MIR optimization (local).
    let local_mir_plan = optimizer.catch_unwind_optimize(raw_expr)?;
    // Attach resolved context required to continue the pipeline.
    let resolved_mir_plan = resolve(local_mir_plan);
    // MIR optimization (global), MIR ⇒ LIR lowering, and LIR optimization (global).
    optimizer.catch_unwind_optimize(resolved_mir_plan)
}

/// Applies EXPLAIN parameter and feature overrides to [`OptimizerConfig`].
impl OverrideFrom<ExplainContext> for OptimizerConfig {
    fn override_from(mut self, ctx: &ExplainContext) -> Self {
        let ExplainContext::Plan(ctx) = ctx else {
            return self; // Return immediately for all other contexts.
        };

        // Override general parameters.
        self.mode = OptimizeMode::Explain;
        self.replan = ctx.replan;
        self.no_fast_path = ctx.config.no_fast_path;

        // Override feature flags that can be enabled in the EXPLAIN config.
        self.features = self.features.override_from(&ctx.config.features);

        // Return the final result.
        self
    }
}

// OptimizerCatalog
// ===============

pub trait OptimizerCatalog: Debug + Send + Sync {
    fn get_entry(&self, id: &GlobalId) -> CatalogCollectionEntry;
    fn get_entry_by_item_id(&self, id: &CatalogItemId) -> &CatalogEntry;
    fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<&ConnectionId>,
    ) -> FullItemName;

    /// Returns all indexes on the given object and cluster known in the
    /// catalog.
    fn get_indexes_on(
        &self,
        id: GlobalId,
        cluster: ClusterId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &Index)> + '_>;
}
