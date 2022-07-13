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

use std::{collections::HashSet, fmt};

use mz_ore::str::Indent;

use crate::{ColumnType, GlobalId, ScalarType};

/// Wraps a reference to a type that implements `Display$Format` for a specific
/// [`ExplainFormat`] and implements [`fmt::Display`] by delegating to this
/// implementation.
///
/// For context-independent `Display$Format` implementations, the explanation
/// type can be a reference to the type that implements [`Explain`].
///
/// For context-dependent `Display$Format` implementations, the explanation
/// type also needs a reference to the context (see [`Explain::Context`]).
///
/// The same type can implement more than one `Display$Format` trait and
/// thereby be plugged as an `Explanation` for more than one output format.
#[allow(missing_debug_implementations)]
enum Explanation<'a, Text = UnsupportedFormat, Json = UnsupportedFormat, Dot = UnsupportedFormat> {
    Text(&'a Text),
    Json(&'a Json),
    Dot(&'a Dot),
}

/// Wraps an instance of type `T` together with a lambda that produces its
/// display context `C`.
///
/// Used internally by all `$format_string_at(T, Fn() -> C)` implementations.
struct DisplayWithContext<'a, T, C, F: Fn() -> C> {
    t: &'a T,
    f: F,
}

impl<'a, T, J, D> fmt::Display for Explanation<'a, T, J, D>
where
    T: DisplayText,
    J: DisplayJson,
    D: DisplayDot,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Explanation::Text(explain) => explain.fmt_text(f, &mut ()),
            Explanation::Json(explain) => explain.fmt_json(f, &mut ()),
            Explanation::Dot(explain) => explain.fmt_dot(f, &mut ()),
        }
    }
}

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

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Text`].
pub trait DisplayText<C = ()>
where
    Self: Sized,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result;
}

pub fn text_string<T: DisplayText<()>>(t: &T) -> String {
    Explanation::<'_, T, UnsupportedFormat, UnsupportedFormat>::Text(t).to_string()
}

pub fn text_string_at<'a, T: DisplayText<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    text_string(&DisplayWithContext { t, f })
}

impl<T: DisplayText<C>, C, F: Fn() -> C> DisplayText<()> for DisplayWithContext<'_, T, C, F> {
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = (self.f)();
        self.t.fmt_text(f, &mut ctx)
    }
}

impl<A, C> DisplayText<C> for Option<A>
where
    A: DisplayText<C>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        if let Some(val) = self {
            val.fmt_text(f, ctx)
        } else {
            fmt::Result::Ok(())
        }
    }
}

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Json`].
pub trait DisplayJson<C = ()>
where
    Self: Sized,
{
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result;
}

pub fn json_string<T: DisplayJson<()>>(t: &T) -> String {
    Explanation::<'_, UnsupportedFormat, T, UnsupportedFormat>::Json(t).to_string()
}

pub fn json_string_at<'a, T: DisplayJson<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    json_string(&DisplayWithContext { t, f })
}

impl<T: DisplayJson<C>, C, F: Fn() -> C> DisplayJson<()> for DisplayWithContext<'_, T, C, F> {
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = (self.f)();
        self.t.fmt_json(f, &mut ctx)
    }
}

impl<A, C> DisplayJson<C> for Option<A>
where
    A: DisplayText<C>,
{
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        if let Some(val) = self {
            val.fmt_text(f, ctx)
        } else {
            fmt::Result::Ok(())
        }
    }
}

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Dot`].
pub trait DisplayDot<C = ()>
where
    Self: Sized,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result;
}

pub fn dot_string<T: DisplayDot<()>>(t: &T) -> String {
    Explanation::<'_, UnsupportedFormat, UnsupportedFormat, T>::Dot(t).to_string()
}

pub fn dot_string_at<'a, T: DisplayDot<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    dot_string(&DisplayWithContext { t, f })
}

impl<T: DisplayDot<C>, C, F: Fn() -> C> DisplayDot<()> for DisplayWithContext<'_, T, C, F> {
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        let mut ctx = (self.f)();
        self.t.fmt_dot(f, &mut ctx)
    }
}

impl<A, C> DisplayDot<C> for Option<A>
where
    A: DisplayDot<C>,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        if let Some(val) = self {
            val.fmt_dot(f, ctx)
        } else {
            fmt::Result::Ok(())
        }
    }
}

/// A zero-variant enum to be used as the explanation type in the
/// [`Explain`] implementation for all formats that are not supported
/// for `Self`.
#[allow(missing_debug_implementations)]
pub enum UnsupportedFormat {}

impl DisplayText for UnsupportedFormat {
    fn fmt_text(&self, _f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        unreachable!()
    }
}

impl DisplayJson for UnsupportedFormat {
    fn fmt_json(&self, _f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        unreachable!()
    }
}

impl DisplayDot for UnsupportedFormat {
    fn fmt_dot(&self, _f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        unreachable!()
    }
}

/// The type of errors that may occur when an [`Explain::explain`]
/// call goes wrong.
#[derive(Debug)]
pub enum ExplainError {
    UnsupportedFormat(ExplainFormat),
    FormatError(fmt::Error),
    AnyhowError(anyhow::Error),
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

/// A configuration supported by all [`Explain`] implementations
/// derived in this crate.
#[derive(Debug)]
pub struct ExplainConfig {
    pub types: bool,
    pub timing: bool,
}

impl TryFrom<HashSet<String>> for ExplainConfig {
    type Error = anyhow::Error;
    fn try_from(mut config_flags: HashSet<String>) -> Result<Self, anyhow::Error> {
        let result = ExplainConfig {
            types: config_flags.remove("types"),
            timing: config_flags.remove("timing"),
        };
        if config_flags.is_empty() {
            Ok(result)
        } else {
            anyhow::bail!("unsupported 'EXPLAIN ... WITH' flags: {:?}", config_flags)
        }
    }
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
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<String, ExplainError> {
        match format {
            ExplainFormat::Text => self.explain_text(config, context).map(|e| text_string(&e)),
            ExplainFormat::Json => self.explain_json(config, context).map(|e| json_string(&e)),
            ExplainFormat::Dot => self.explain_dot(config, context).map(|e| dot_string(&e)),
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
    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
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
    fn explain_json(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Json, ExplainError> {
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
    fn explain_dot(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Dot, ExplainError> {
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

/// A trait for humanizing components of an expression.
///
/// This will be most often used as part of the rendering context
/// type for various `Display$Format` implementation.
pub trait ExprHumanizer: fmt::Debug {
    /// Attempts to return the a human-readable string for the relation
    /// identified by `id`.
    fn humanize_id(&self, id: GlobalId) -> Option<String>;

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

    fn humanize_scalar_type(&self, ty: &ScalarType) -> String {
        // The debug implementation is better than nothing.
        format!("{:?}", ty)
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
        frontiers: Frontiers<u64>,
    }

    /// A test IR that should be the subject of explanations.
    struct TestExpr {
        lhs: i32,
        rhs: i32,
    }

    struct TestExplanation<'a> {
        expr: &'a TestExpr,
        config: &'a ExplainConfig,
        context: &'a ExplainContext<'a>,
    }

    impl<'a> DisplayText for TestExplanation<'a> {
        fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let lhs = &self.expr.lhs;
            let rhs = &self.expr.rhs;
            writeln!(f, "expr = {lhs} + {rhs}")?;

            if self.config.timing {
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
            config: &'a super::ExplainConfig,
            context: &'a Self::Context,
        ) -> Result<Self::Text, ExplainError> {
            Ok(TestExplanation {
                expr: self,
                config,
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
        let config = ExplainConfig {
            timing: true,
            types: false,
        };
        let context = ExplainContext { env, frontiers };

        expr.explain(&format, &config, &context)
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
