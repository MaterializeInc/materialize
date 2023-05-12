// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A set of traits for modeling things that can be explained by a
//! SQL `EXPLAIN` statement.
//!
//! The main trait in this module is [`Explain`].
//!
//! An explainable subject `S` implements [`Explain`], and as part of that:
//!
//! 1. Fixes the *context type* required for the explanation.
//!    in [`Explain::Context`].
//! 2. Fixes the *explanation type* for each [`ExplainFormat`]
//!    in [`Explain::Text`], [`Explain::Json`], ....
//! 3. Provides *an explanation type constructor* for each supported
//!    [`ExplainFormat`] from references to `S`, [`ExplainConfig` ],
//!    and the current [`Explain::Context`] in
//!    [`Explain::explain_text`], [`Explain::explain_json`], ....
//!
//! The same *explanation type* can be shared by more than one
//! [`ExplainFormat`].
//!
//! Use [`UnsupportedFormat`] and the default `explain_$format`
//! constructor for [`Explain`] to indicate that the implementation does
//! not support this `$format`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use mz_ore::stack::RecursionLimitError;
use mz_ore::str::Indent;

use crate::{ColumnType, GlobalId, ScalarType};

use self::dot::{dot_string, DisplayDot};
use self::json::{json_string, DisplayJson};
use self::text::{text_string, DisplayText};

pub mod dot;
pub mod json;
pub mod text;
#[cfg(feature = "tracing_")]
pub mod tracing;

#[cfg(feature = "tracing_")]
pub use self::tracing::trace_plan;

/// Possible output formats for an explanation.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ExplainFormat {
    Text,
    Json,
    Dot,
}

impl fmt::Display for ExplainFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExplainFormat::Text => f.write_str("TEXT"),
            ExplainFormat::Json => f.write_str("JSON"),
            ExplainFormat::Dot => f.write_str("DOT"),
        }
    }
}

/// A zero-variant enum to be used as the explanation type in the
/// [`Explain`] implementation for all formats that are not supported
/// for `Self`.
#[allow(missing_debug_implementations)]
pub enum UnsupportedFormat {}

/// The type of errors that may occur when an [`Explain::explain`]
/// call goes wrong.
#[derive(Debug)]
pub enum ExplainError {
    UnsupportedFormat(ExplainFormat),
    FormatError(fmt::Error),
    AnyhowError(anyhow::Error),
    RecursionLimitError(RecursionLimitError),
    SerdeJsonError(serde_json::Error),
    LinearChainsPlusRecursive,
    UnknownError(String),
}

impl fmt::Display for ExplainError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error while rendering explain output: ")?;
        match self {
            ExplainError::UnsupportedFormat(format) => {
                write!(f, "{} format is not supported", format)
            }
            ExplainError::FormatError(error) => {
                write!(f, "{}", error)
            }
            ExplainError::AnyhowError(error) => {
                write!(f, "{}", error)
            }
            ExplainError::RecursionLimitError(error) => {
                write!(f, "{}", error)
            }
            ExplainError::SerdeJsonError(error) => {
                write!(f, "{}", error)
            }
            ExplainError::LinearChainsPlusRecursive => {
                write!(
                    f,
                    "The linear_chains option is not supported with WITH MUTUALLY RECURSIVE. \
                If you would like to see added support, then please comment at \
                https://github.com/MaterializeInc/materialize/issues/19012."
                )
            }
            ExplainError::UnknownError(error) => {
                write!(f, "{}", error)
            }
        }
    }
}

impl From<fmt::Error> for ExplainError {
    fn from(error: fmt::Error) -> Self {
        ExplainError::FormatError(error)
    }
}

impl From<anyhow::Error> for ExplainError {
    fn from(error: anyhow::Error) -> Self {
        ExplainError::AnyhowError(error)
    }
}

impl From<RecursionLimitError> for ExplainError {
    fn from(error: RecursionLimitError) -> Self {
        ExplainError::RecursionLimitError(error)
    }
}

impl From<serde_json::Error> for ExplainError {
    fn from(error: serde_json::Error) -> Self {
        ExplainError::SerdeJsonError(error)
    }
}

/// A set of options for controlling the output of [`Explain`] implementations.
#[derive(Clone, Debug)]
pub struct ExplainConfig {
    /// Show the number of columns.
    pub arity: bool,
    /// Render implemented MIR `Join` nodes in a way which reflects the implementation.
    pub join_impls: bool,
    /// Show the sets of unique keys.
    pub keys: bool,
    /// Restrict output trees to linear chains. Ignored if `raw_plans` is set.
    pub linear_chains: bool,
    /// Show the `non_negative` in the explanation if it is supported by the backing IR.
    pub non_negative: bool,
    /// Show the slow path plan even if a fast path plan was created. Useful for debugging.
    /// Enforced if `timing` is set.
    pub no_fast_path: bool,
    /// Don't normalize plans before explaining them.
    pub raw_plans: bool,
    /// Disable virtual syntax in the explanation.
    pub raw_syntax: bool,
    /// Show the `subtree_size` attribute in the explanation if it is supported by the backing IR.
    pub subtree_size: bool,
    /// Print optimization timings.
    pub timing: bool,
    /// Show the `type` attribute in the explanation.
    pub types: bool,
    /// Show MFP pushdown information.
    pub mfp_pushdown: bool,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            arity: false,
            join_impls: true,
            keys: false,
            linear_chains: false,
            non_negative: false,
            no_fast_path: true,
            raw_plans: true,
            raw_syntax: true,
            subtree_size: false,
            timing: false,
            types: false,
            mfp_pushdown: false,
        }
    }
}

impl ExplainConfig {
    pub fn requires_attributes(&self) -> bool {
        self.subtree_size || self.non_negative || self.arity || self.types || self.keys
    }
}

impl TryFrom<BTreeSet<String>> for ExplainConfig {
    type Error = anyhow::Error;
    fn try_from(mut flags: BTreeSet<String>) -> Result<Self, anyhow::Error> {
        // If `WITH(raw)` is specified, ensure that the config will be as
        // representative for the original plan as possible.
        if flags.remove("raw") {
            flags.insert("raw_plans".into());
            flags.insert("raw_syntax".into());
        }
        let result = ExplainConfig {
            arity: flags.remove("arity"),
            join_impls: flags.remove("join_impls"),
            keys: flags.remove("keys"),
            linear_chains: flags.remove("linear_chains") && !flags.contains("raw_plans"),
            non_negative: flags.remove("non_negative"),
            no_fast_path: flags.remove("no_fast_path") || flags.contains("timing"),
            raw_plans: flags.remove("raw_plans"),
            raw_syntax: flags.remove("raw_syntax"),
            subtree_size: flags.remove("subtree_size"),
            timing: flags.remove("timing"),
            types: flags.remove("types"),
            mfp_pushdown: flags.remove("mfp_pushdown"),
        };
        if flags.is_empty() {
            Ok(result)
        } else {
            anyhow::bail!("unsupported 'EXPLAIN ... WITH' flags: {:?}", flags)
        }
    }
}

/// The type of object to be explained
#[derive(Clone, Debug)]
pub enum Explainee {
    /// An object that will be served using a dataflow
    Dataflow(GlobalId),
    /// The object to be explained is a one-off query and may or may not served
    /// using a dataflow.
    Query,
}

/// A trait that provides a unified interface for objects that
/// can be explained.
///
/// All possible subjects of the various forms of an `EXPLAIN`
/// SQL statement should implement this trait.
pub trait Explain<'a>: 'a {
    /// The type of the immutable context in which
    /// the explanation will be rendered.
    type Context;

    /// The explanation type produced by a successful
    /// [`Explain::explain_text`] call.
    type Text: DisplayText;

    /// The explanation type produced by a successful
    /// [`Explain::explain_json`] call.
    type Json: DisplayJson;

    /// The explanation type produced by a successful
    /// [`Explain::explain_json`] call.
    type Dot: DisplayDot;

    /// Explain an instance of [`Self`] within the given
    /// [`Explain::Context`].
    ///
    /// Implementors should never have the need to not rely on
    /// this default implementation.
    ///
    /// # Errors
    ///
    /// If the given `format` is not supported, the implementation
    /// should return an [`ExplainError::UnsupportedFormat`].
    ///
    /// If an [`ExplainConfig`] parameter cannot be honored, the
    /// implementation should silently ignore this paramter and
    /// proceed without returning a [`Result::Err`].
    fn explain(
        &'a mut self,
        format: &'a ExplainFormat,
        context: &'a Self::Context,
    ) -> Result<String, ExplainError> {
        match format {
            ExplainFormat::Text => self.explain_text(context).map(|e| text_string(&e)),
            ExplainFormat::Json => self.explain_json(context).map(|e| json_string(&e)),
            ExplainFormat::Dot => self.explain_dot(context).map(|e| dot_string(&e)),
        }
    }

    /// Construct a [`Result::Ok`] of the [`Explain::Text`] format
    /// from the config and the context.
    ///
    /// # Errors
    ///
    /// If the [`ExplainFormat::Text`] is not supported, the implementation
    /// should return an [`ExplainError::UnsupportedFormat`].
    ///
    /// If an [`ExplainConfig`] parameter cannot be honored, the
    /// implementation should silently ignore this paramter and
    /// proceed without returning a [`Result::Err`].
    #[allow(unused_variables)]
    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        Err(ExplainError::UnsupportedFormat(ExplainFormat::Text))
    }

    /// Construct a [`Result::Ok`] of the [`Explain::Json`] format
    /// from the config and the context.
    ///
    /// # Errors
    ///
    /// If the [`ExplainFormat::Text`] is not supported, the implementation
    /// should return an [`ExplainError::UnsupportedFormat`].
    ///
    /// If an [`ExplainConfig`] parameter cannot be honored, the
    /// implementation should silently ignore this paramter and
    /// proceed without returning a [`Result::Err`].
    #[allow(unused_variables)]
    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Json, ExplainError> {
        Err(ExplainError::UnsupportedFormat(ExplainFormat::Json))
    }

    /// Construct a [`Result::Ok`] of the [`Explain::Dot`] format
    /// from the config and the context.
    ///
    /// # Errors
    ///
    /// If the [`ExplainFormat::Dot`] is not supported, the implementation
    /// should return an [`ExplainError::UnsupportedFormat`].
    ///
    /// If an [`ExplainConfig`] parameter cannot be honored, the
    /// implementation should silently ignore this paramter and
    /// proceed without returning a [`Result::Err`].
    #[allow(unused_variables)]
    fn explain_dot(&'a mut self, context: &'a Self::Context) -> Result<Self::Dot, ExplainError> {
        Err(ExplainError::UnsupportedFormat(ExplainFormat::Dot))
    }
}

/// A helper struct which will most commonly be used as the generic
/// rendering context type `C` for various `Explain$Format`
/// implementations.
#[derive(Debug)]
pub struct RenderingContext<'a> {
    pub indent: Indent,
    pub humanizer: &'a dyn ExprHumanizer,
}

impl<'a> RenderingContext<'a> {
    pub fn new(indent: Indent, humanizer: &'a dyn ExprHumanizer) -> RenderingContext {
        RenderingContext { indent, humanizer }
    }
}

impl<'a> AsMut<Indent> for RenderingContext<'a> {
    fn as_mut(&mut self) -> &mut Indent {
        &mut self.indent
    }
}

impl<'a> AsRef<&'a dyn ExprHumanizer> for RenderingContext<'a> {
    fn as_ref(&self) -> &&'a dyn ExprHumanizer {
        &self.humanizer
    }
}
#[allow(missing_debug_implementations)]
pub struct PlanRenderingContext<'a, T> {
    pub indent: Indent,
    pub humanizer: &'a dyn ExprHumanizer,
    pub annotations: BTreeMap<&'a T, Attributes>,
    pub config: &'a ExplainConfig,
}

impl<'a, T> PlanRenderingContext<'a, T> {
    pub fn new(
        indent: Indent,
        humanizer: &'a dyn ExprHumanizer,
        annotations: BTreeMap<&'a T, Attributes>,
        config: &'a ExplainConfig,
    ) -> PlanRenderingContext<'a, T> {
        PlanRenderingContext {
            indent,
            humanizer,
            annotations,
            config,
        }
    }
}

impl<'a, T> AsMut<Indent> for PlanRenderingContext<'a, T> {
    fn as_mut(&mut self) -> &mut Indent {
        &mut self.indent
    }
}

impl<'a, T> AsRef<&'a dyn ExprHumanizer> for PlanRenderingContext<'a, T> {
    fn as_ref(&self) -> &&'a dyn ExprHumanizer {
        &self.humanizer
    }
}

/// A trait for humanizing components of an expression.
///
/// This will be most often used as part of the rendering context
/// type for various `Display$Format` implementation.
pub trait ExprHumanizer: fmt::Debug {
    /// Attempts to return the a human-readable string for the relation
    /// identified by `id`.
    fn humanize_id(&self, id: GlobalId) -> Option<String>;

    /// Same as above, but without qualifications, e.g., only `foo` for `materialize.public.foo`.
    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String>;

    /// Returns a human-readable name for the specified scalar type.
    fn humanize_scalar_type(&self, ty: &ScalarType) -> String;

    /// Returns a human-readable name for the specified scalar type.
    fn humanize_column_type(&self, typ: &ColumnType) -> String {
        format!(
            "{}{}",
            self.humanize_scalar_type(&typ.scalar_type),
            if typ.nullable { "?" } else { "" }
        )
    }
}

/// A bare-minimum implementation of [`ExprHumanizer`].
///
/// The `DummyHumanizer` does a poor job of humanizing expressions. It is
/// intended for use in contexts where polish is not required, like in tests or
/// while debugging.
#[derive(Debug)]
pub struct DummyHumanizer;

impl ExprHumanizer for DummyHumanizer {
    fn humanize_id(&self, _: GlobalId) -> Option<String> {
        // Returning `None` allows the caller to fall back to displaying the
        // ID, if they so desire.
        None
    }

    fn humanize_id_unqualified(&self, _id: GlobalId) -> Option<String> {
        None
    }

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        // The debug implementation is better than nothing.
        format!("{:?}", ty)
    }
}

/// Pretty-prints a list of indices.
#[derive(Debug)]
pub struct Indices<'a>(pub &'a [usize]);

/// Pretty-prints a list of scalar expressions that may have runs of column
/// indices as a comma-separated list interleaved with interval expressions.
///
/// Interval expressions are used only for runs of three or more elements.
#[derive(Debug)]
pub struct CompactScalarSeq<'a, T: ScalarOps>(pub &'a [T]);

pub trait ScalarOps {
    fn match_col_ref(&self) -> Option<usize>;

    fn references(&self, col_ref: usize) -> bool;
}

/// A somewhat ad-hoc way to keep carry a plan with a set
/// of attributes derived for each node in that plan.
#[allow(missing_debug_implementations)]
pub struct AnnotatedPlan<'a, T> {
    pub plan: &'a T,
    pub annotations: BTreeMap<&'a T, Attributes>,
}

/// A container for derived attributes.
#[derive(Clone, Default, Debug)]
pub struct Attributes {
    pub non_negative: Option<bool>,
    pub subtree_size: Option<usize>,
    pub arity: Option<usize>,
    pub types: Option<String>,
    pub keys: Option<String>,
}

impl fmt::Display for Attributes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("//");
        if let Some(subtree_size) = &self.subtree_size {
            builder.field("subtree_size", subtree_size);
        }
        if let Some(non_negative) = &self.non_negative {
            builder.field("non_negative", non_negative);
        }
        if let Some(arity) = &self.arity {
            builder.field("arity", arity);
        }
        if let Some(types) = &self.types {
            builder.field("types", types);
        }
        if let Some(keys) = &self.keys {
            builder.field("keys", keys);
        }
        builder.finish()
    }
}

/// A set of indexes that are used in the explained plan.
#[derive(Debug)]
pub struct UsedIndexes(Vec<GlobalId>);

impl UsedIndexes {
    pub fn new(values: Vec<GlobalId>) -> UsedIndexes {
        UsedIndexes(values)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Environment {
        name: String,
    }

    impl Default for Environment {
        fn default() -> Self {
            Environment {
                name: "test env".to_string(),
            }
        }
    }

    struct Frontiers<T> {
        since: T,
        upper: T,
    }

    impl<T> Frontiers<T> {
        fn new(since: T, upper: T) -> Self {
            Self { since, upper }
        }
    }

    struct ExplainContext<'a> {
        env: &'a mut Environment,
        config: &'a ExplainConfig,
        frontiers: Frontiers<u64>,
    }

    /// A test IR that should be the subject of explanations.
    struct TestExpr {
        lhs: i32,
        rhs: i32,
    }

    struct TestExplanation<'a> {
        expr: &'a TestExpr,
        context: &'a ExplainContext<'a>,
    }

    impl<'a> DisplayText for TestExplanation<'a> {
        fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let lhs = &self.expr.lhs;
            let rhs = &self.expr.rhs;
            writeln!(f, "expr = {lhs} + {rhs}")?;

            if self.context.config.timing {
                let since = &self.context.frontiers.since;
                let upper = &self.context.frontiers.upper;
                writeln!(f, "at t ∊ [{since}, {upper})")?;
            }

            let name = &self.context.env.name;
            writeln!(f, "env = {name}")?;

            Ok(())
        }
    }

    impl<'a> Explain<'a> for TestExpr {
        type Context = ExplainContext<'a>;
        type Text = TestExplanation<'a>;
        type Json = UnsupportedFormat;
        type Dot = UnsupportedFormat;

        fn explain_text(
            &'a mut self,
            context: &'a Self::Context,
        ) -> Result<Self::Text, ExplainError> {
            Ok(TestExplanation {
                expr: self,
                context,
            })
        }
    }

    fn do_explain(
        env: &mut Environment,
        frontiers: Frontiers<u64>,
    ) -> Result<String, ExplainError> {
        let mut expr = TestExpr { lhs: 1, rhs: 2 };

        let format = ExplainFormat::Text;
        let config = &ExplainConfig {
            arity: false,
            join_impls: false,
            keys: false,
            linear_chains: false,
            non_negative: false,
            no_fast_path: false,
            raw_plans: false,
            raw_syntax: false,
            subtree_size: false,
            timing: true,
            types: false,
            mfp_pushdown: false,
        };
        let context = ExplainContext {
            env,
            config,
            frontiers,
        };

        expr.explain(&format, &context)
    }

    #[test]
    fn test_mutable_context() {
        let mut env = Environment::default();
        let frontiers = Frontiers::<u64>::new(3, 7);

        let act = do_explain(&mut env, frontiers);
        let exp = "expr = 1 + 2\nat t ∊ [3, 7)\nenv = test env\n".to_string();

        assert!(act.is_ok());
        assert_eq!(act.unwrap(), exp);
    }
}
