// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared local optimization pipelines and configuration.

pub mod view;

use mz_adapter_types::dyncfgs::PERSIST_FAST_PATH_ORDER;
use mz_compute_types::dyncfgs::SUBSCRIBE_SNAPSHOT_OPTIMIZATION;
use mz_expr::{
    EvalError, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, UnmaterializableFunc,
};
use mz_ore::stack::RecursionLimitError;
use mz_repr::GlobalId;
use mz_repr::adt::timestamp::TimestampError;
use mz_repr::optimize::{OptimizerFeatureOverrides, OptimizerFeatures, OverrideFrom};
use mz_sql::plan::PlanError;
use mz_sql::session::vars::SystemVars;
use mz_transform::{MaybeShouldPanic, TransformCtx, TransformError};

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
pub trait Optimize<From> {
    type To;

    /// Execute the optimization stage, transforming the input plan of type
    /// `From` to an output plan of type `To`.
    fn optimize(&mut self, plan: From) -> Result<Self::To, OptimizerError>;

    /// Like [`Self::optimize`], but additionally ensures that panics occurring
    /// in the [`Self::optimize`] call are caught and demoted to an
    /// [`OptimizerError::Internal`] error.
    ///
    /// Additionally, if the result of the optimization is an error (not a panic) that indicates we
    /// should panic, then panic.
    #[mz_ore::instrument(target = "optimizer", level = "debug", name = "optimize")]
    fn catch_unwind_optimize(&mut self, plan: From) -> Result<Self::To, OptimizerError> {
        mz_transform::catch_unwind_optimize(|| self.optimize(plan))
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
    // If set, allow some additional queries down the Persist fast path when we believe
    // the orderings are compatible.
    persist_fast_path_order: bool,
    // Enable calculating with_snapshot metadata for subscribes.
    subscribe_snapshot_optimization: bool,
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

impl OptimizerConfig {
    /// Whether compatible orderings may use the Persist fast path.
    pub fn persist_fast_path_order(&self) -> bool {
        self.persist_fast_path_order
    }

    /// Whether to calculate `with_snapshot` metadata for subscribes.
    pub fn subscribe_snapshot_optimization(&self) -> bool {
        self.subscribe_snapshot_optimization
    }
}

impl From<&SystemVars> for OptimizerConfig {
    fn from(vars: &SystemVars) -> Self {
        Self {
            mode: OptimizeMode::Execute,
            replan: None,
            no_fast_path: false,
            persist_fast_path_order: PERSIST_FAST_PATH_ORDER.get(vars.dyncfgs()),
            subscribe_snapshot_optimization: SUBSCRIBE_SNAPSHOT_OPTIMIZATION.get(vars.dyncfgs()),
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

impl From<&OptimizerConfig> for mz_sql::plan::HirToMirConfig {
    fn from(config: &OptimizerConfig) -> Self {
        Self {
            enable_new_outer_join_lowering: config.features.enable_new_outer_join_lowering,
            enable_variadic_left_join_lowering: config.features.enable_variadic_left_join_lowering,
            enable_cast_elimination: config.features.enable_cast_elimination,
            enable_simplify_quantified_comparisons: config
                .features
                .enable_simplify_quantified_comparisons,
            enable_fixed_correlated_cte_lowering: config
                .features
                .enable_fixed_correlated_cte_lowering,
            enable_simplify_from_less_existence: config
                .features
                .enable_simplify_from_less_existence,
        }
    }
}

/// Behavior to prepare relation and scalar expressions for use in a dataflow.
pub trait ExprPrep {
    /// Prepare a relation expression.
    fn prep_relation_expr(&self, expr: &mut OptimizedMirRelationExpr)
    -> Result<(), OptimizerError>;

    /// Prepare a scalar expression.
    fn prep_scalar_expr(&self, expr: &mut MirScalarExpr) -> Result<(), OptimizerError>;
}

/// A no-op expression preparer.
pub struct ExprPrepNoop;
impl ExprPrep for ExprPrepNoop {
    fn prep_relation_expr(&self, _: &mut OptimizedMirRelationExpr) -> Result<(), OptimizerError> {
        Ok(())
    }
    fn prep_scalar_expr(&self, _expr: &mut MirScalarExpr) -> Result<(), OptimizerError> {
        Ok(())
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
    #[error("access to function {0} is restricted")]
    RestrictedFunction(UnmaterializableFunc),
    #[error("{0}")]
    UnsupportedTemporalExpression(String),
    /// This is a specific kind of internal error. It's distinct from `Internal`, because we want to
    /// catch it and swallow it in some cases.
    #[error("internal optimizer error: MfpPlan couldn't be converted into SafeMfpPlan")]
    InternalUnsafeMfpPlan(String),
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
            Self::RestrictedFunction(_) => Some(
                "Access to system catalog objects is restricted for this role. \
                Contact your administrator if you need access."
                    .into(),
            ),
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

impl MaybeShouldPanic for OptimizerError {
    fn should_panic(&self) -> Option<String> {
        match self {
            OptimizerError::TransformError(TransformError::CallerShouldPanic(msg)) => {
                Some(msg.to_string())
            }
            _ => None,
        }
    }
}

/// Keeps SQL-level type identity while using optimized nullability and keys.
pub fn infer_sql_type_for_catalog(
    hir_expr: &mz_sql::plan::HirRelationExpr,
    mir_expr: &MirRelationExpr,
) -> mz_repr::SqlRelationType {
    let mut typ = hir_expr.top_level_typ();
    typ.backport_nullability_and_keys(&mir_expr.typ());
    typ
}

// Tracing helpers
// ---------------

/// Runs local MIR optimization and traces the resulting plan.
#[mz_ore::instrument(target = "optimizer", level = "debug", name = "local")]
pub fn optimize_mir_local(
    expr: MirRelationExpr,
    ctx: &mut TransformCtx,
) -> Result<OptimizedMirRelationExpr, OptimizerError> {
    fail::fail_point!("optimize_mir_local");

    #[allow(deprecated)]
    let optimizer = mz_transform::Optimizer::logical_optimizer(ctx);
    let expr = optimizer.optimize(expr, ctx)?;

    // Trace the result of this phase.
    mz_repr::explain::trace_plan(expr.as_inner());

    Ok::<_, OptimizerError>(expr)
}

/// This is just a wrapper around [mz_transform::Optimizer::constant_optimizer],
/// running it, and tracing the result plan.
#[mz_ore::instrument(target = "optimizer", level = "debug", name = "constant")]
pub fn optimize_mir_constant(
    expr: MirRelationExpr,
    ctx: &mut TransformCtx,
    limit: bool,
) -> Result<MirRelationExpr, OptimizerError> {
    let optimizer = mz_transform::Optimizer::constant_optimizer(ctx, limit);
    let expr = optimizer.optimize(expr, ctx)?;

    // Trace the result of this phase.
    mz_repr::explain::trace_plan(expr.as_inner());

    Ok::<_, OptimizerError>(expr.0)
}

/// Traces a plan under an optimizer stage span.
#[macro_export]
macro_rules! optimizer_trace_plan {
    (at: $span:literal, $plan:expr) => {
        tracing::debug_span!(target: "optimizer", $span).in_scope(|| {
            mz_repr::explain::trace_plan($plan);
        });
    }
}

pub use crate::optimizer_trace_plan as trace_plan;
