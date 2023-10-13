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
//!   [`OptimizeMaterializedView`] for `T` = `MaterializedView`).
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
//!     adjacent in the pipeline.
//!   - The struct representing the result of the final stage of the
//!     optimization pipeline can be destructed to access its internals.
//! - The `Send + Sync` trait bounds on the `Self` and `From` types ensure that
//!   [`Optimize`] instances can be passed to different threads.
//!
//! For details, see the `20230714_optimizer_interface.md` design doc in this
//! repository.

mod index;
mod materialized_view;
mod peek;
mod subscribe;
mod view;

// Re-export optimzier structs
pub use index::{Index, OptimizeIndex};
pub use materialized_view::OptimizeMaterializedView;
pub use peek::OptimizePeek;
pub use subscribe::OptimizeSubscribe;
pub use view::OptimizeView;

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::OptimizedMirRelationExpr;
use mz_sql::plan::PlanError;
use mz_sql::session::vars::SystemVars;
use mz_transform::TransformError;

use crate::AdapterError;

/// A trait that represents an optimization stage.
///
/// The trait is implemented by structs that encapsulate the context needed to
/// run an end-to-end optimization pipeline for a specific statement type
/// (`Index`, `View`, `MaterializedView`, `Subscribe`, `Select`).
///
/// Each implementation represents a concrete optimization stage for a fixed
/// statement type that consumes an input of type `From` and produces output of
/// type `Self::To`.
///
/// The generic lifetime `'ctx` models the lifetime of the optimizer context and
/// can be passed to the optimizer struct and the `Self::To` types.
///
/// The `'s: 'ctx` bound in the `optimize` method call ensures that an optimizer
/// instance can run an optimization stage that produces a `Self::To` with
/// `&'ctx` references.
pub trait Optimize<'ctx, From>: Send + Sync
where
    From: Send + Sync,
{
    type To: Send + Sync + 'ctx;

    /// Execute the optimization stage, transforming the input plan of type
    /// `From` to an output plan of type `To`.
    fn optimize<'s: 'ctx>(&'s mut self, plan: From) -> Result<Self::To, OptimizerError>;

    /// Execute the optimization stage and panic if an error occurs.
    ///
    /// See [`Optimize::optimize`].
    fn must_optimize<'s: 'ctx>(&'s mut self, expr: From) -> Self::To {
        match self.optimize(expr) {
            Ok(ok) => ok,
            Err(err) => panic!("must_optimize call failed: {err}"),
        }
    }
}

// Feature flags for the optimizer.
pub struct OptimizerConfig {
    /// Enable consolidation of unions that happen immediately after negate.
    ///
    /// The refinement happens in the LIR ⇒ LIR phase.
    pub enable_consolidate_after_union_negate: bool,
    /// Enable relaxing of consolidation enforcers for optimization pipelines
    /// that produce dataflows that run a computation for a single timestamp
    /// (such as SELECTs).
    ///
    /// The refinement happens in the LIR ⇒ LIR phase.
    pub enable_monotonic_oneshot_selects: bool,
    /// Enable collecting type information so that rendering can type-specialize
    /// arrangements.
    ///
    /// The collection of type information happens in MIR ⇒ LIR lowering.
    pub enable_specialized_arrangements: bool,
    /// An exclusive upper bound on the number of results we may return from a
    /// Persist fast-path peek. Required by the `create_fast_path_plan` call in
    /// [`OptimizePeek`].
    pub persist_fast_path_limit: usize,
}

impl From<&SystemVars> for OptimizerConfig {
    fn from(vars: &SystemVars) -> Self {
        Self {
            enable_consolidate_after_union_negate: vars.enable_consolidate_after_union_negate(),
            enable_monotonic_oneshot_selects: vars.enable_monotonic_oneshot_selects(),
            enable_specialized_arrangements: vars.enable_specialized_arrangements(),
            persist_fast_path_limit: vars.persist_fast_path_limit(),
        }
    }
}

/// A type for a [`DataflowDescription`] backed by `Mir~` plans. Used internally
/// by the optimizer implementations.
type MirDataflowDescription = DataflowDescription<OptimizedMirRelationExpr>;
/// A type for a [`DataflowDescription`] backed by `Lir~` plans. Used internally
/// by the optimizer implementations.
type LirDataflowDescription = DataflowDescription<Plan>;

/// Error types that can be generated during optimization.
#[derive(Debug, thiserror::Error)]
pub enum OptimizerError {
    // TODO: change dataflows.rs error types and reverse this ownership.
    #[error("{0}")]
    AdapterError(#[from] AdapterError),
    #[error("{0}")]
    PlanError(#[from] PlanError),
    #[error("{0}")]
    TransformError(#[from] TransformError),
    #[error("internal optimizer error: {0}")]
    Internal(String),
}

// TODO: create a dedicated AdapterError::OptimizerError variant.
impl From<OptimizerError> for AdapterError {
    fn from(value: OptimizerError) -> Self {
        match value {
            OptimizerError::AdapterError(err) => err,
            err => AdapterError::Internal(err.to_string()),
        }
    }
}
