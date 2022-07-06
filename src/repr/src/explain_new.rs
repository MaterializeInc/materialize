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
pub enum Explanation<
    'a,
    Text = UnsupportedFormat,
    Json = UnsupportedFormat,
    Dot = UnsupportedFormat,
> {
    Text(&'a Text),
    Json(&'a Json),
    Dot(&'a Dot),
}

impl<'a, T, J, D> fmt::Display for Explanation<'a, T, J, D>
where
    T: DisplayText,
    J: DisplayJson,
    D: DisplayDot,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Explanation::Text(explain) => explain.fmt_text(f),
            Explanation::Json(explain) => explain.fmt_json(f),
            Explanation::Dot(explain) => explain.fmt_dot(f),
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
pub trait DisplayText
where
    Self: Sized,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    fn str_text(&self) -> String {
        Explanation::<'_, Self, UnsupportedFormat, UnsupportedFormat>::Text(self).to_string()
    }
}

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Json`].
pub trait DisplayJson
where
    Self: Sized,
{
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    fn str_json(&self) -> String {
        Explanation::<'_, UnsupportedFormat, Self, UnsupportedFormat>::Json(self).to_string()
    }
}

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Dot`].
pub trait DisplayDot
where
    Self: Sized,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    fn str_dot(&self) -> String {
        Explanation::<'_, UnsupportedFormat, UnsupportedFormat, Self>::Dot(self).to_string()
    }
}

/// A zero-variant enum to be used as the explanation type in the
/// [`Explain`] implementation for all formats that are not supported
/// for `Self`.
#[allow(missing_debug_implementations)]
pub enum UnsupportedFormat {}

impl DisplayText for UnsupportedFormat {
    fn fmt_text(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        unreachable!()
    }
}

impl DisplayJson for UnsupportedFormat {
    fn fmt_json(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        unreachable!()
    }
}

impl DisplayDot for UnsupportedFormat {
    fn fmt_dot(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// The type of the configuration supported by the [`Explanation`]
/// variants that can be produced for this type.
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
            ExplainFormat::Text => self.explain_text(config, context).map(|e| e.str_text()),
            ExplainFormat::Json => self.explain_json(config, context).map(|e| e.str_json()),
            ExplainFormat::Dot => self.explain_dot(config, context).map(|e| e.str_dot()),
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
        fn fmt_text(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
