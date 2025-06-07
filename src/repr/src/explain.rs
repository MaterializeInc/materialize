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

use itertools::Itertools;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::{Display, Formatter};

use mz_ore::stack::RecursionLimitError;
use mz_ore::str::{Indent, bracketed, separated};

use crate::explain::dot::{DisplayDot, dot_string};
use crate::explain::json::{DisplayJson, json_string};
use crate::explain::text::{DisplayText, text_string};
use crate::optimize::OptimizerFeatureOverrides;
use crate::{ColumnType, GlobalId, ScalarType};

pub mod dot;
pub mod json;
pub mod text;
#[cfg(feature = "tracing")]
pub mod tracing;

#[cfg(feature = "tracing")]
pub use crate::explain::tracing::trace_plan;

/// Possible output formats for an explanation.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
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
                    "The linear_chains option is not supported with WITH MUTUALLY RECURSIVE."
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
    // Analyses:
    // (These are shown only if the Analysis is supported by the backing IR.)
    /// Show the `SubtreeSize` Analysis in the explanation.
    pub subtree_size: bool,
    /// Show the number of columns, i.e., the `Arity` Analysis.
    pub arity: bool,
    /// Show the types, i.e., the `RelationType` Analysis.
    pub types: bool,
    /// Show the sets of unique keys, i.e., the `UniqueKeys` Analysis.
    pub keys: bool,
    /// Show the `NonNegative` Analysis.
    pub non_negative: bool,
    /// Show the `Cardinality` Analysis.
    pub cardinality: bool,
    /// Show the `ColumnNames` Analysis.
    pub column_names: bool,
    /// Show the `Equivalences` Analysis.
    pub equivalences: bool,
    // TODO: add an option to show the `Monotonic` Analysis. This is non-trivial, because this
    // Analysis needs the set of monotonic GlobalIds, which are cumbersome to pass around.

    // Other display options:
    /// Render implemented MIR `Join` nodes in a way which reflects the implementation.
    pub join_impls: bool,
    /// Use inferred column names when rendering scalar and aggregate expressions.
    pub humanized_exprs: bool,
    /// Restrict output trees to linear chains. Ignored if `raw_plans` is set.
    pub linear_chains: bool,
    /// Show the slow path plan even if a fast path plan was created. Useful for debugging.
    /// Enforced if `timing` is set.
    pub no_fast_path: bool,
    /// Don't print optimizer hints.
    pub no_notices: bool,
    /// Show node IDs in physical plans.
    pub node_ids: bool,
    /// Don't normalize plans before explaining them.
    pub raw_plans: bool,
    /// Disable virtual syntax in the explanation.
    pub raw_syntax: bool,
    /// Use verbose syntax in the explanation.
    pub verbose_syntax: bool,
    /// Anonymize literals in the plan.
    pub redacted: bool,
    /// Print optimization timings.
    pub timing: bool,
    /// Show MFP pushdown information.
    pub filter_pushdown: bool,

    /// Optimizer feature flags.
    pub features: OptimizerFeatureOverrides,
}

impl Default for ExplainConfig {
    fn default() -> Self {
        Self {
            // Don't redact in debug builds and in CI.
            redacted: !mz_ore::assert::soft_assertions_enabled(),
            arity: false,
            cardinality: false,
            column_names: false,
            filter_pushdown: false,
            humanized_exprs: false,
            join_impls: true,
            keys: false,
            linear_chains: false,
            no_fast_path: true,
            no_notices: false,
            node_ids: false,
            non_negative: false,
            raw_plans: true,
            raw_syntax: false,
            verbose_syntax: false,
            subtree_size: false,
            timing: false,
            types: false,
            equivalences: false,
            features: Default::default(),
        }
    }
}

impl ExplainConfig {
    pub fn requires_analyses(&self) -> bool {
        self.subtree_size
            || self.non_negative
            || self.arity
            || self.types
            || self.keys
            || self.cardinality
            || self.column_names
            || self.equivalences
    }
}

/// The type of object to be explained
#[derive(Clone, Debug)]
pub enum Explainee {
    /// An existing materialized view.
    MaterializedView(GlobalId),
    /// An existing index.
    Index(GlobalId),
    /// An object that will be served using a dataflow.
    ///
    /// This variant is deprecated and will be removed in database-issues#5301.
    Dataflow(GlobalId),
    /// The object to be explained is a one-off query and may or may not be
    /// served using a dataflow.
    Select,
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
    /// implementation should silently ignore this parameter and
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
    /// implementation should silently ignore this parameter and
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
    /// If the [`ExplainFormat::Json`] is not supported, the implementation
    /// should return an [`ExplainError::UnsupportedFormat`].
    ///
    /// If an [`ExplainConfig`] parameter cannot be honored, the
    /// implementation should silently ignore this parameter and
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
    /// implementation should silently ignore this parameter and
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
    pub fn new(indent: Indent, humanizer: &'a dyn ExprHumanizer) -> RenderingContext<'a> {
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
    pub annotations: BTreeMap<&'a T, Analyses>,
    pub config: &'a ExplainConfig,
}

impl<'a, T> PlanRenderingContext<'a, T> {
    pub fn new(
        indent: Indent,
        humanizer: &'a dyn ExprHumanizer,
        annotations: BTreeMap<&'a T, Analyses>,
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
    /// Attempts to return a human-readable string for the relation
    /// identified by `id`.
    fn humanize_id(&self, id: GlobalId) -> Option<String>;

    /// Same as above, but without qualifications, e.g., only `foo` for `materialize.public.foo`.
    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String>;

    /// Like [`Self::humanize_id`], but returns the constituent parts of the
    /// name as individual elements.
    fn humanize_id_parts(&self, id: GlobalId) -> Option<Vec<String>>;

    /// Returns a human-readable name for the specified scalar type.
    /// Used in, e.g., EXPLAIN and error msgs, in which case exact Postgres compatibility is less
    /// important than showing as much detail as possible. Also used in `pg_typeof`, where Postgres
    /// compatibility is more important.
    fn humanize_scalar_type(&self, ty: &ScalarType, postgres_compat: bool) -> String;

    /// Returns a human-readable name for the specified column type.
    /// Used in, e.g., EXPLAIN and error msgs, in which case exact Postgres compatibility is less
    /// important than showing as much detail as possible. Also used in `pg_typeof`, where Postgres
    /// compatibility is more important.
    fn humanize_column_type(&self, typ: &ColumnType, postgres_compat: bool) -> String {
        format!(
            "{}{}",
            self.humanize_scalar_type(&typ.scalar_type, postgres_compat),
            if typ.nullable { "?" } else { "" }
        )
    }

    /// Returns a vector of column names for the relation identified by `id`.
    fn column_names_for_id(&self, id: GlobalId) -> Option<Vec<String>>;

    /// Returns the `#column` name for the relation identified by `id`.
    fn humanize_column(&self, id: GlobalId, column: usize) -> Option<String>;

    /// Returns whether the specified id exists.
    fn id_exists(&self, id: GlobalId) -> bool;
}

/// An [`ExprHumanizer`] that extends the `inner` instance with shadow items
/// that are reported as present, even though they might not exist in `inner`.
#[derive(Debug)]
pub struct ExprHumanizerExt<'a> {
    /// A map of custom items that might not exist in the backing `inner`
    /// humanizer, but are reported as present by this humanizer instance.
    items: BTreeMap<GlobalId, TransientItem>,
    /// The inner humanizer used to resolve queries for [GlobalId] values not
    /// present in the `items` map.
    inner: &'a dyn ExprHumanizer,
}

impl<'a> ExprHumanizerExt<'a> {
    pub fn new(items: BTreeMap<GlobalId, TransientItem>, inner: &'a dyn ExprHumanizer) -> Self {
        Self { items, inner }
    }
}

impl<'a> ExprHumanizer for ExprHumanizerExt<'a> {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        match self.items.get(&id) {
            Some(item) => item
                .humanized_id_parts
                .as_ref()
                .map(|parts| parts.join(".")),
            None => self.inner.humanize_id(id),
        }
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        match self.items.get(&id) {
            Some(item) => item
                .humanized_id_parts
                .as_ref()
                .and_then(|parts| parts.last().cloned()),
            None => self.inner.humanize_id_unqualified(id),
        }
    }

    fn humanize_id_parts(&self, id: GlobalId) -> Option<Vec<String>> {
        match self.items.get(&id) {
            Some(item) => item.humanized_id_parts.clone(),
            None => self.inner.humanize_id_parts(id),
        }
    }

    fn humanize_scalar_type(&self, ty: &ScalarType, postgres_compat: bool) -> String {
        self.inner.humanize_scalar_type(ty, postgres_compat)
    }

    fn column_names_for_id(&self, id: GlobalId) -> Option<Vec<String>> {
        match self.items.get(&id) {
            Some(item) => item.column_names.clone(),
            None => self.inner.column_names_for_id(id),
        }
    }

    fn humanize_column(&self, id: GlobalId, column: usize) -> Option<String> {
        match self.items.get(&id) {
            Some(item) => match &item.column_names {
                Some(column_names) => Some(column_names[column].clone()),
                None => None,
            },
            None => self.inner.humanize_column(id, column),
        }
    }

    fn id_exists(&self, id: GlobalId) -> bool {
        self.items.contains_key(&id) || self.inner.id_exists(id)
    }
}

/// A description of a catalog item that does not exist, but can be reported as
/// present in the catalog by a [`ExprHumanizerExt`] instance that has it in its
/// `items` list.
#[derive(Debug)]
pub struct TransientItem {
    humanized_id_parts: Option<Vec<String>>,
    column_names: Option<Vec<String>>,
}

impl TransientItem {
    pub fn new(humanized_id_parts: Option<Vec<String>>, column_names: Option<Vec<String>>) -> Self {
        Self {
            humanized_id_parts,
            column_names,
        }
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

    fn humanize_id_parts(&self, _id: GlobalId) -> Option<Vec<String>> {
        None
    }

    fn humanize_scalar_type(&self, ty: &ScalarType, _postgres_compat: bool) -> String {
        // The debug implementation is better than nothing.
        format!("{:?}", ty)
    }

    fn column_names_for_id(&self, _id: GlobalId) -> Option<Vec<String>> {
        None
    }

    fn humanize_column(&self, _id: GlobalId, _column: usize) -> Option<String> {
        None
    }

    fn id_exists(&self, _id: GlobalId) -> bool {
        false
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
pub struct CompactScalarSeq<'a, T: ScalarOps>(pub &'a [T]); // TODO(cloud#8196) remove this

/// Pretty-prints a list of scalar expressions that may have runs of column
/// indices as a comma-separated list interleaved with interval expressions.
///
/// Interval expressions are used only for runs of three or more elements.
#[derive(Debug)]
pub struct CompactScalars<T, I>(pub I)
where
    T: ScalarOps,
    I: Iterator<Item = T> + Clone;

pub trait ScalarOps {
    fn match_col_ref(&self) -> Option<usize>;

    fn references(&self, col_ref: usize) -> bool;
}

/// A somewhat ad-hoc way to keep carry a plan with a set
/// of analyses derived for each node in that plan.
#[allow(missing_debug_implementations)]
pub struct AnnotatedPlan<'a, T> {
    pub plan: &'a T,
    pub annotations: BTreeMap<&'a T, Analyses>,
}

/// A container for derived analyses.
#[derive(Clone, Default, Debug)]
pub struct Analyses {
    pub non_negative: Option<bool>,
    pub subtree_size: Option<usize>,
    pub arity: Option<usize>,
    pub types: Option<Option<Vec<ColumnType>>>,
    pub keys: Option<Vec<Vec<usize>>>,
    pub cardinality: Option<String>,
    pub column_names: Option<Vec<String>>,
    pub equivalences: Option<String>,
}

#[derive(Debug, Clone)]
pub struct HumanizedAnalyses<'a> {
    analyses: &'a Analyses,
    humanizer: &'a dyn ExprHumanizer,
    config: &'a ExplainConfig,
}

impl<'a> HumanizedAnalyses<'a> {
    pub fn new<T>(analyses: &'a Analyses, ctx: &PlanRenderingContext<'a, T>) -> Self {
        Self {
            analyses,
            humanizer: ctx.humanizer,
            config: ctx.config,
        }
    }
}

impl<'a> Display for HumanizedAnalyses<'a> {
    // Analysis rendering is guarded by the ExplainConfig flag for each
    // Analysis. This is needed because we might have derived Analysis that
    // are not explicitly requested (such as column_names), in which case we
    // don't want to display them.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut builder = f.debug_struct("//");

        if self.config.subtree_size {
            let subtree_size = self.analyses.subtree_size.expect("subtree_size");
            builder.field("subtree_size", &subtree_size);
        }

        if self.config.non_negative {
            let non_negative = self.analyses.non_negative.expect("non_negative");
            builder.field("non_negative", &non_negative);
        }

        if self.config.arity {
            let arity = self.analyses.arity.expect("arity");
            builder.field("arity", &arity);
        }

        if self.config.types {
            let types = match self.analyses.types.as_ref().expect("types") {
                Some(types) => {
                    let types = types
                        .into_iter()
                        .map(|c| self.humanizer.humanize_column_type(c, false))
                        .collect::<Vec<_>>();

                    bracketed("(", ")", separated(", ", types)).to_string()
                }
                None => "(<error>)".to_string(),
            };
            builder.field("types", &types);
        }

        if self.config.keys {
            let keys = self
                .analyses
                .keys
                .as_ref()
                .expect("keys")
                .into_iter()
                .map(|key| bracketed("[", "]", separated(", ", key)).to_string());
            let keys = bracketed("(", ")", separated(", ", keys)).to_string();
            builder.field("keys", &keys);
        }

        if self.config.cardinality {
            let cardinality = self.analyses.cardinality.as_ref().expect("cardinality");
            builder.field("cardinality", cardinality);
        }

        if self.config.column_names {
            let column_names = self.analyses.column_names.as_ref().expect("column_names");
            let column_names = column_names.into_iter().enumerate().map(|(i, c)| {
                if c.is_empty() {
                    Cow::Owned(format!("#{i}"))
                } else {
                    Cow::Borrowed(c)
                }
            });
            let column_names = bracketed("(", ")", separated(", ", column_names)).to_string();
            builder.field("column_names", &column_names);
        }

        if self.config.equivalences {
            let equivs = self.analyses.equivalences.as_ref().expect("equivalences");
            builder.field("equivs", equivs);
        }

        builder.finish()
    }
}

/// A set of indexes that are used in the explained plan.
///
/// Each element consists of the following components:
/// 1. The id of the index.
/// 2. A vector of [IndexUsageType] denoting how the index is used in the plan.
///
/// Using a `BTreeSet` here ensures a deterministic iteration order, which in turn ensures that
/// the corresponding EXPLAIN output is deterministic as well.
#[derive(Clone, Debug, Default)]
pub struct UsedIndexes(BTreeSet<(GlobalId, Vec<IndexUsageType>)>);

impl UsedIndexes {
    pub fn new(values: BTreeSet<(GlobalId, Vec<IndexUsageType>)>) -> UsedIndexes {
        UsedIndexes(values)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(Debug, Clone, Arbitrary, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum IndexUsageType {
    /// Read the entire index.
    FullScan,
    /// Differential join. The work is proportional to the number of matches.
    DifferentialJoin,
    /// Delta join
    DeltaJoin(DeltaJoinIndexUsageType),
    /// `IndexedFilter`, e.g., something like `WHERE x = 42` with an index on `x`.
    /// This also stores the id of the index that we want to do the lookup from. (This id is already
    /// chosen by `LiteralConstraints`, and then `IndexUsageType::Lookup` communicates this inside
    /// `CollectIndexRequests` from the `IndexedFilter` to the `Get`.)
    Lookup(GlobalId),
    /// This is a rare case that happens when the user creates an index that is identical to an
    /// existing one (i.e., on the same object, and with the same keys). We'll re-use the
    /// arrangement of the existing index. The plan is an `ArrangeBy` + `Get`, where the `ArrangeBy`
    /// is requesting the same key as an already existing index. (`export_index` is what inserts
    /// this `ArrangeBy`.)
    PlanRootNoArrangement,
    /// The index is used for directly writing to a sink. Can happen with a SUBSCRIBE to an indexed
    /// view.
    SinkExport,
    /// The index is used for creating a new index. Note that either a `FullScan` or a
    /// `PlanRootNoArrangement` usage will always accompany an `IndexExport` usage.
    IndexExport,
    /// When a fast path peek has a LIMIT, but no ORDER BY, then we read from the index only as many
    /// records (approximately), as the OFFSET + LIMIT needs.
    /// Note: When a fast path peek does a lookup and also has a limit, the usage type will be
    /// `Lookup`. However, the smart limiting logic will still apply.
    FastPathLimit,
    /// We saw a dangling `ArrangeBy`, i.e., where we have no idea what the arrangement will be used
    /// for. This is an internal error. Can be a bug either in `CollectIndexRequests`, or some
    /// other transform that messed up the plan. It's also possible that somebody is trying to add
    /// an `ArrangeBy` marking for some operator other than a `Join`. (Which is fine, but please
    /// update `CollectIndexRequests`.)
    DanglingArrangeBy,
    /// Internal error in `CollectIndexRequests` or a failed attempt to look up
    /// an index in `DataflowMetainfo::used_indexes`.
    Unknown,
}

/// In a snapshot, one arrangement of the first input is scanned, all the other arrangements (of the
/// first input, and of all other inputs) only get lookups.
/// When later input batches are arriving, all inputs are fully read.
#[derive(Debug, Clone, Arbitrary, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DeltaJoinIndexUsageType {
    Unknown,
    Lookup,
    FirstInputFullScan,
}

impl std::fmt::Display for IndexUsageType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                IndexUsageType::FullScan => "*** full scan ***",
                IndexUsageType::Lookup(_idx_id) => "lookup",
                IndexUsageType::DifferentialJoin => "differential join",
                IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::FirstInputFullScan) =>
                    "delta join 1st input (full scan)",
                // Technically, this is a lookup only for a snapshot. For later update batches, all
                // records are read. However, I wrote lookup here, because in most cases the
                // lookup/scan distinction matters only for a snapshot. This is because for arriving
                // update records, something in the system will always do work proportional to the
                // number of records anyway. In other words, something is always scanning new
                // updates, but we can avoid scanning records again and again in snapshots.
                IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::Lookup) => "delta join lookup",
                IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::Unknown) =>
                    "*** INTERNAL ERROR (unknown delta join usage) ***",
                IndexUsageType::PlanRootNoArrangement => "plan root (no new arrangement)",
                IndexUsageType::SinkExport => "sink export",
                IndexUsageType::IndexExport => "index export",
                IndexUsageType::FastPathLimit => "fast path limit",
                IndexUsageType::DanglingArrangeBy => "*** INTERNAL ERROR (dangling ArrangeBy) ***",
                IndexUsageType::Unknown => "*** INTERNAL ERROR (unknown usage) ***",
            }
        )
    }
}

impl IndexUsageType {
    pub fn display_vec<'a, I>(usage_types: I) -> impl Display + Sized + 'a
    where
        I: IntoIterator<Item = &'a IndexUsageType>,
    {
        separated(", ", usage_types.into_iter().sorted().dedup())
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;

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
            redacted: false,
            arity: false,
            cardinality: false,
            column_names: false,
            filter_pushdown: false,
            humanized_exprs: false,
            join_impls: false,
            keys: false,
            linear_chains: false,
            no_fast_path: false,
            no_notices: false,
            node_ids: false,
            non_negative: false,
            raw_plans: false,
            raw_syntax: false,
            verbose_syntax: true,
            subtree_size: false,
            equivalences: false,
            timing: true,
            types: false,
            features: Default::default(),
        };
        let context = ExplainContext {
            env,
            config,
            frontiers,
        };

        expr.explain(&format, &context)
    }

    #[mz_ore::test]
    fn test_mutable_context() {
        let mut env = Environment::default();
        let frontiers = Frontiers::<u64>::new(3, 7);

        let act = do_explain(&mut env, frontiers);
        let exp = "expr = 1 + 2\nat t ∊ [3, 7)\nenv = test env\n".to_string();

        assert_ok!(act);
        assert_eq!(act.unwrap(), exp);
    }
}
