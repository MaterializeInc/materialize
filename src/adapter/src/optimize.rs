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
pub mod peek;
pub mod subscribe;
pub mod view;

use std::panic::AssertUnwindSafe;

use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::plan::Plan;
use mz_expr::{EvalError, MirRelationExpr, OptimizedMirRelationExpr, UnmaterializableFunc};
use mz_ore::stack::RecursionLimitError;
use mz_repr::adt::timestamp::TimestampError;
use mz_repr::optimize::{OptimizerFeatureOverrides, OptimizerFeatures, OverrideFrom};
use mz_repr::GlobalId;
use mz_sql::plan::PlanError;
use mz_sql::session::vars::SystemVars;
use mz_transform::{TransformCtx, TransformError};

// Alias types
// -----------

/// A type for a [`DataflowDescription`] backed by `Mir~` plans. Used internally
/// by the optimizer implementations.
type MirDataflowDescription = DataflowDescription<OptimizedMirRelationExpr>;
/// A type for a [`DataflowDescription`] backed by `Lir~` plans. Used internally
/// by the optimizer implementations.
type LirDataflowDescription = DataflowDescription<Plan>;

// Core API
// --------

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
pub trait Optimize<From>: Send
where
    From: Send,
{
    type To: Send;

    /// Execute the optimization stage, transforming the input plan of type
    /// `From` to an output plan of type `To`.
    fn optimize(&mut self, plan: From) -> Result<Self::To, OptimizerError>;

    /// Like [`Self::optimize`], but additionally ensures that panics occurring
    /// in the [`Self::optimize`] call are caught and demoted to an
    /// [`OptimizerError::Internal`] error.
    #[mz_ore::instrument(target = "optimizer", level = "debug", name = "optimize")]
    fn catch_unwind_optimize(&mut self, plan: From) -> Result<Self::To, OptimizerError> {
        match mz_ore::panic::catch_unwind(AssertUnwindSafe(|| self.optimize(plan))) {
            Ok(result) => {
                match result.map_err(Into::into) {
                    Err(OptimizerError::TransformError(TransformError::CallerShouldPanic(msg))) => {
                        // Promote a `CallerShouldPanic` error from the result
                        // to a proper panic. This is needed in order to ensure
                        // that `mz_unsafe.mz_panic('forced panic')` calls still
                        // panic the caller.
                        panic!("{}", msg)
                    }
                    result => result,
                }
            }
            Err(_) => {
                let msg = "unexpected panic during query optimization".to_string();
                Err(OptimizerError::Internal(msg))
            }
        }
    }

    /// Execute the optimization stage and panic if an error occurs.
    ///
    /// See [`Optimize::optimize`].
    #[allow(dead_code)] // This function is never used, but it's useful to keep around.
    fn must_optimize(&mut self, expr: From) -> Self::To {
        match self.optimize(expr) {
            Ok(ok) => ok,
            Err(err) => panic!("must_optimize call failed: {err}"),
        }
    }
}

// Optimizer configuration
// -----------------------

/// Feature flags for the optimizer.
///
/// To add a new feature flag, do the following steps:
///
/// 1. To make the flag available to all stages in our [`Optimize`] pipelines
///    and allow engineers to set a system-wide override:
///    1. Add the flag to the `optimizer_feature_flags!(...)` macro call.
///    2. Add the flag to the `feature_flags!(...)` macro call and extend the
///       `From<&SystemVars>` implementation for [`OptimizerFeatures`].
///
/// 2. To enable `EXPLAIN ... WITH(...)` overrides which will allow engineers to
///    inspect plan differences before deploying the optimizer changes:
///    1. Add the flag to the `ExplainPlanOptionName` definition.
///    2. Add the flag to the `generate_extracted_config!(ExplainPlanOption,
///       ...)` macro call.
///    3. Extend the `TryFrom<ExplainPlanOptionExtracted>` implementation for
///       [`mz_repr::explain::ExplainConfig`].
///
/// 3. To enable `CLUSTER ... FEATURES(...)` overrides which will allow
///    engineers to experiment with runtime differences before deploying the
///    optimizer changes:
///    1. Add the flag to the `ClusterFeatureName` definition.
///    2. Add the flag to the `generate_extracted_config!(ClusterFeature, ...)`
///       macro call.
///    3. Extend the `let optimizer_feature_overrides = ...` call in
///       `plan_create_cluster`.
#[derive(Clone, Debug)]
pub struct OptimizerConfig {
    /// The mode in which the optimizer runs.
    pub mode: OptimizeMode,
    /// If the [`GlobalId`] is set the optimizer works in "replan" mode.
    ///
    /// This means that it will not consider catalog items (more specifically
    /// indexes) with [`GlobalId`] greater or equal than the one provided here.
    pub replan: Option<GlobalId>,
    /// Show the slow path plan even if a fast path plan was created. Useful for debugging.
    /// Enforced if `timing` is set.
    pub no_fast_path: bool,
    /// Optimizer feature flags.
    pub features: OptimizerFeatures,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OptimizeMode {
    /// A mode where the optimized statement is executed.
    Execute,
    /// A mode where the optimized statement is explained.
    Explain,
}

impl From<&SystemVars> for OptimizerConfig {
    fn from(vars: &SystemVars) -> Self {
        Self {
            mode: OptimizeMode::Execute,
            replan: None,
            no_fast_path: false,
            features: OptimizerFeatures::from(vars),
        }
    }
}

/// Override [`OptimizerConfig::features`] from [`OptimizerFeatureOverrides`].
impl OverrideFrom<OptimizerFeatureOverrides> for OptimizerConfig {
    fn override_from(mut self, overrides: &OptimizerFeatureOverrides) -> Self {
        self.features = self.features.override_from(overrides);
        self
    }
}

/// [`OptimizerConfig`] overrides coming from an [`ExplainContext`].
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

impl From<&OptimizerConfig> for mz_sql::plan::HirToMirConfig {
    fn from(config: &OptimizerConfig) -> Self {
        Self {
            enable_new_outer_join_lowering: config.features.enable_new_outer_join_lowering,
            enable_variadic_left_join_lowering: config.features.enable_variadic_left_join_lowering,
            enable_outer_join_null_filter: config.features.enable_outer_join_null_filter,
            enable_value_window_function_fusion: config
                .features
                .enable_value_window_function_fusion,
        }
    }
}

// OptimizerError
// ===============

/// Error types that can be generated during optimization.
#[derive(Debug, thiserror::Error)]
pub enum OptimizerError {
    #[error("{0}")]
    PlanError(#[from] PlanError),
    #[error("{0}")]
    RecursionLimitError(#[from] RecursionLimitError),
    #[error("{0}")]
    TransformError(#[from] TransformError),
    #[error("{0}")]
    EvalError(#[from] EvalError),
    #[error("cannot materialize call to {0}")]
    UnmaterializableFunction(UnmaterializableFunc),
    #[error("cannot call {func} in {context} ")]
    UncallableFunction {
        func: UnmaterializableFunc,
        context: &'static str,
    },
    #[error("internal optimizer error: {0}")]
    Internal(String),
}

impl From<String> for OptimizerError {
    fn from(msg: String) -> Self {
        Self::Internal(msg)
    }
}

impl OptimizerError {
    pub fn detail(&self) -> Option<String> {
        match self {
            Self::UnmaterializableFunction(UnmaterializableFunc::CurrentTimestamp) => {
                Some("See: https://materialize.com/docs/sql/functions/now_and_mz_now/".into())
            }
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::UnmaterializableFunction(UnmaterializableFunc::CurrentTimestamp) => {
                Some("In temporal filters `mz_now()` may work instead.".into())
            }
            _ => None,
        }
    }
}

impl From<TimestampError> for OptimizerError {
    fn from(value: TimestampError) -> Self {
        OptimizerError::EvalError(EvalError::from(value))
    }
}

impl From<anyhow::Error> for OptimizerError {
    fn from(value: anyhow::Error) -> Self {
        OptimizerError::Internal(value.to_string())
    }
}

// Tracing helpers
// ---------------

#[mz_ore::instrument(target = "optimizer", level = "debug", name = "local")]
fn optimize_mir_local(
    expr: MirRelationExpr,
    ctx: &mut TransformCtx,
) -> Result<OptimizedMirRelationExpr, OptimizerError> {
    #[allow(deprecated)]
    let optimizer = mz_transform::Optimizer::logical_optimizer(ctx);
    let expr = optimizer.optimize(expr, ctx)?;

    // Trace the result of this phase.
    mz_repr::explain::trace_plan(expr.as_inner());

    Ok::<_, OptimizerError>(expr)
}

macro_rules! trace_plan {
    (at: $span:literal, $plan:expr) => {
        tracing::debug_span!(target: "optimizer", $span).in_scope(|| {
            mz_repr::explain::trace_plan($plan);
        });
    }
}

use trace_plan;

use crate::coord::ExplainContext;
