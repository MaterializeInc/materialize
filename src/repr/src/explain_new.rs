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

use mz_ore::{stack::RecursionLimitError, str::Indent, str::IndentLike};
use serde::{Serialize, Serializer};

use crate::{ColumnType, GlobalId, Row, ScalarType};

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

impl<A, C> DisplayText<C> for Box<A>
where
    A: DisplayText<C>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        self.as_ref().fmt_text(f, ctx)
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

/// Render a type `t: T` as [`ExplainFormat::Text`].
///
/// # Panics
///
/// Panics if the [`DisplayText::fmt_text`] call returns a [`fmt::Error`].
pub fn text_string<T: DisplayText>(t: &T) -> String {
    struct TextString<'a, T>(&'a T);

    impl<'a, F: DisplayText> fmt::Display for TextString<'a, F> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_text(f, &mut ())
        }
    }

    TextString::<'_>(t).to_string()
}

/// Apply `f: F` to create a rendering context of type `C` and render the given
/// tree `t: T` within that context.
/// # Panics
///
/// Panics if the [`DisplayText::fmt_text`] call returns a [`fmt::Error`].
pub fn text_string_at<'a, T: DisplayText<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    struct TextStringAt<'a, T, C, F: Fn() -> C> {
        t: &'a T,
        f: F,
    }

    impl<T: DisplayText<C>, C, F: Fn() -> C> DisplayText<()> for TextStringAt<'_, T, C, F> {
        fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let mut ctx = (self.f)();
            self.t.fmt_text(f, &mut ctx)
        }
    }

    text_string(&TextStringAt { t, f })
}

/// Creates a type whose [`fmt::Display`] implementation outputs each item in
/// `iter` separated by `separator`.
///
/// The difference between this and [`mz_ore::str::separated`] is that the latter
/// requires the iterator items to implement [`fmt::Display`], whereas this version
/// wants them to implement [`DisplayText<C>`] for some rendering context `C` which
/// implements [`Default`].
pub fn separated_text<'a, I, C>(separator: &'a str, iter: I) -> impl fmt::Display + 'a
where
    I: IntoIterator,
    I::IntoIter: Clone + 'a,
    I::Item: DisplayText<C> + 'a,
    C: Default + 'a,
{
    struct Separated<'a, I, C> {
        separator: &'a str,
        iter: I,
        phantom: std::marker::PhantomData<C>,
    }

    impl<'a, I, C> fmt::Display for Separated<'a, I, C>
    where
        C: Default,
        I: Iterator + Clone,
        I::Item: DisplayText<C>,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            for (i, item) in self.iter.clone().enumerate() {
                if i != 0 {
                    write!(f, "{}", self.separator)?;
                }
                item.fmt_text(f, &mut C::default())?;
            }
            Ok(())
        }
    }

    Separated {
        separator,
        iter: iter.into_iter(),
        phantom: std::marker::PhantomData::<C>,
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

impl<A, C> DisplayJson<C> for Box<A>
where
    A: DisplayJson<C>,
{
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        self.as_ref().fmt_json(f, ctx)
    }
}

impl<A, C> DisplayJson<C> for Option<A>
where
    A: DisplayJson<C>,
{
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        if let Some(val) = self {
            val.fmt_json(f, ctx)
        } else {
            fmt::Result::Ok(())
        }
    }
}

/// Render a type `t: T` as [`ExplainFormat::Json`].
///
/// # Panics
///
/// Panics if the [`DisplayJson::fmt_json`] call returns a [`fmt::Error`].
pub fn json_string<T: DisplayJson<()>>(t: &T) -> String {
    struct JsonString<'a, T>(&'a T);

    impl<'a, T: DisplayJson> fmt::Display for JsonString<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_json(f, &mut ())
        }
    }

    JsonString::<'_>(t).to_string()
}

/// Apply `f: F` to create a rendering context of type `C` and render the given
/// tree `t: T` within that context.
/// # Panics
///
/// Panics if the [`DisplayJson::fmt_json`] call returns a [`fmt::Error`].
pub fn json_string_at<'a, T: DisplayJson<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    struct JsonStringAt<'a, T, C, F: Fn() -> C> {
        t: &'a T,
        f: F,
    }

    impl<T: DisplayJson<C>, C, F: Fn() -> C> DisplayJson<()> for JsonStringAt<'_, T, C, F> {
        fn fmt_json(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let mut ctx = (self.f)();
            self.t.fmt_json(f, &mut ctx)
        }
    }

    json_string(&JsonStringAt { t, f })
}

impl DisplayJson<()> for String {
    fn fmt_json(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        f.write_str(self)
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

impl<A, C> DisplayDot<C> for Box<A>
where
    A: DisplayDot<C>,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        self.as_ref().fmt_dot(f, ctx)
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

/// Render a type `t: T` as [`ExplainFormat::Dot`].
///
/// # Panics
///
/// Panics if the [`DisplayDot::fmt_dot`] call returns a [`fmt::Error`].
pub fn dot_string<T: DisplayDot<()>>(t: &T) -> String {
    struct DotString<'a, T>(&'a T);

    impl<'a, T: DisplayDot> fmt::Display for DotString<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_dot(f, &mut ())
        }
    }

    DotString::<'_>(t).to_string()
}

/// Apply `f: F` to create a rendering context of type `C` and render the given
/// type `t: T` as [`ExplainFormat::Dot`] within that context.
///
/// # Panics
///
/// Panics if the [`DisplayDot::fmt_dot`] call returns a [`fmt::Error`].
pub fn dot_string_at<'a, T: DisplayDot<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    struct DotStringAt<'a, T, C, F: Fn() -> C> {
        t: &'a T,
        f: F,
    }

    impl<T: DisplayDot<C>, C, F: Fn() -> C> DisplayDot<()> for DotStringAt<'_, T, C, F> {
        fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let mut ctx = (self.f)();
            self.t.fmt_dot(f, &mut ctx)
        }
    }

    dot_string(&DotStringAt { t, f })
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
    RecursionLimitError(RecursionLimitError),
    SerdeJsonError(serde_json::Error),
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
#[derive(Debug)]
pub struct ExplainConfig {
    /// Show the number of columns.
    pub arity: bool,
    /// Render implemented MIR `Join` nodes in a way which reflects the implementation.
    pub join_impls: bool,
    /// Show the sets of unique keys.
    pub keys: bool,
    /// Show the `non_negative` in the explanation if it is supported by the backing IR.
    pub non_negative: bool,
    /// Show the slow path plan even if a fast path plan was created. Useful for debugging.
    pub no_fast_path: bool,
    /// Don't normalize plans before explaining them.
    pub raw_plans: bool,
    /// Disable virtual syntax in the explanation.
    pub raw_syntax: bool,
    /// Show the `subtree_size` attribute in the explanation if it is supported by the backing IR.
    pub subtree_size: bool,
    /// Print optimization timings (currently unsupported).
    pub timing: bool,
    /// Show the `type` attribute in the explanation.
    pub types: bool,
}

impl ExplainConfig {
    pub fn requires_attributes(&self) -> bool {
        self.subtree_size || self.non_negative || self.arity || self.types || self.keys
    }
}

impl TryFrom<HashSet<String>> for ExplainConfig {
    type Error = anyhow::Error;
    fn try_from(mut config_flags: HashSet<String>) -> Result<Self, anyhow::Error> {
        // If `WITH(raw)` is specified, ensure that the config will be as
        // representative for the original plan as possible.
        if config_flags.remove("raw") {
            config_flags.insert("raw_plans".into());
            config_flags.insert("raw_syntax".into());
        }
        let result = ExplainConfig {
            arity: config_flags.remove("arity"),
            join_impls: config_flags.remove("join_impls"),
            keys: config_flags.remove("keys"),
            non_negative: config_flags.remove("non_negative"),
            no_fast_path: config_flags.remove("no_fast_path"),
            raw_plans: config_flags.remove("raw_plans"),
            raw_syntax: config_flags.remove("raw_syntax"),
            subtree_size: config_flags.remove("subtree_size"),
            timing: config_flags.remove("timing"),
            types: config_flags.remove("types"),
        };
        if config_flags.is_empty() {
            Ok(result)
        } else {
            anyhow::bail!("unsupported 'EXPLAIN ... WITH' flags: {:?}", config_flags)
        }
    }
}

/// The type of object to be explained
#[derive(Debug)]
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

#[derive(Debug)]
/// A wrapper around [Row] so that [serde_json] can produce human-readable
/// output without changing the default serialization for Row.
pub struct JSONRow(Row);

impl JSONRow {
    pub fn new(row: Row) -> Self {
        Self(row)
    }
}

impl Serialize for JSONRow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let row = self.0.unpack();
        row.serialize(serializer)
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

fn write_first_rows(
    f: &mut fmt::Formatter<'_>,
    first_rows: &Vec<(&Row, &crate::Diff)>,
    ctx: &mut Indent,
) -> fmt::Result {
    for (row, diff) in first_rows {
        if **diff == 1 {
            writeln!(f, "{}- {}", ctx, row)?;
        } else {
            writeln!(f, "{}- ({} x {})", ctx, row, diff)?;
        }
    }
    Ok(())
}

pub fn fmt_text_constant_rows<'a, I>(
    f: &mut fmt::Formatter<'_>,
    mut rows: I,
    ctx: &mut Indent,
) -> fmt::Result
where
    I: Iterator<Item = (&'a Row, &'a crate::Diff)>,
{
    let mut row_count = 0;
    let mut first_rows = Vec::with_capacity(20);
    for _ in 0..20 {
        if let Some((row, diff)) = rows.next() {
            row_count += diff;
            first_rows.push((row, diff));
        }
    }
    let rest_of_row_count = rows.into_iter().map(|(_, diff)| diff).sum::<crate::Diff>();
    if rest_of_row_count != 0 {
        writeln!(f, "{}total_rows: {}", ctx, row_count + rest_of_row_count)?;
        writeln!(f, "{}first_rows:", ctx)?;
        ctx.indented(move |ctx| write_first_rows(f, &first_rows, ctx))?;
    } else {
        write_first_rows(f, &first_rows, ctx)?;
    }

    Ok(())
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
            arity: false,
            join_impls: false,
            keys: false,
            non_negative: false,
            no_fast_path: false,
            raw_plans: false,
            raw_syntax: false,
            subtree_size: false,
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

    #[test]
    fn test_json_row_serialization() {
        use crate::adt::array::ArrayDimension;
        use crate::Datum;
        // a 2 x 3 array
        let mut array = Row::default();
        array
            .packer()
            .push_array(
                &[
                    ArrayDimension {
                        lower_bound: 1,
                        length: 2,
                    },
                    ArrayDimension {
                        lower_bound: 1,
                        length: 3,
                    },
                ],
                [
                    Datum::Int32(12),
                    Datum::Int32(20),
                    Datum::Int32(-312),
                    Datum::Int32(0),
                    Datum::Int32(-42),
                    Datum::Int32(1231),
                ]
                .into_iter(),
            )
            .unwrap();
        let mut list_datum = Row::default();
        list_datum.packer().push_list_with(|row| {
            row.push(Datum::UInt32(0));
            row.push(Datum::Int64(10));
        });
        let mut map_datum = Row::default();
        map_datum.packer().push_dict_with(|row| {
            row.push(Datum::String("hello"));
            row.push(Datum::Int16(-1));
            row.push(Datum::String("world"));
            row.push(Datum::Int16(1000));
        });

        // For ease of reading the expected output, construct a vec with
        // type of datum + the expected output for that datm
        let all_types_of_datum = vec![
            (Datum::True, "true"),
            (Datum::False, "false"),
            (Datum::Null, "null"),
            (Datum::Dummy, r#""Dummy""#),
            (Datum::JsonNull, r#""JsonNull""#),
            (Datum::UInt8(32), "32"),
            (Datum::from(0.1_f32), "0.1"),
            (Datum::from(-1.23), "-1.23"),
            (
                Datum::Date(chrono::NaiveDate::from_ymd(2022, 8, 3)),
                r#""2022-08-03""#,
            ),
            (
                Datum::Time(chrono::NaiveTime::from_hms(12, 10, 22)),
                r#""12:10:22""#,
            ),
            (
                Datum::Timestamp(chrono::NaiveDateTime::from_timestamp(1023123, 234)),
                r#""1970-01-12T20:12:03.000000234""#,
            ),
            (
                Datum::TimestampTz(chrono::DateTime::from_utc(
                    chrono::NaiveDateTime::from_timestamp(90234242, 234),
                    chrono::Utc,
                )),
                r#""1972-11-10T09:04:02.000000234Z""#,
            ),
            (
                Datum::Uuid(uuid::uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8")),
                r#""67e55044-10b1-426f-9247-bb680e5fe0c8""#,
            ),
            (Datum::Bytes(&[127, 23, 4]), "[127,23,4]"),
            (
                Datum::Interval(crate::adt::interval::Interval {
                    months: 1,
                    days: 2,
                    micros: 10,
                }),
                r#"{"months":1,"days":2,"micros":10}"#,
            ),
            (
                Datum::from(crate::adt::numeric::Numeric::from(10.234)),
                r#""10.234""#,
            ),
            (array.unpack_first(), "[[12,20,-312],[0,-42,1231]]"),
            (list_datum.unpack_first(), "[0,10]"),
            (map_datum.unpack_first(), r#"{"hello":-1,"world":1000}"#),
        ];
        let (data, strings): (Vec<_>, Vec<_>) = all_types_of_datum.into_iter().unzip();
        let row = JSONRow(Row::pack(data.into_iter()));
        let result = serde_json::to_string(&row);
        assert!(result.is_ok());
        let expected = format!("[{}]", strings.join(","));
        assert_eq!(result.unwrap(), expected);
    }
}
