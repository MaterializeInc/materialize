// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type rules.
//!
//! Each function infers the column types of one operator's output from the column types of
//! its inputs, and checks the scalar expressions the operator evaluates. Literals must be
//! instances of their declared types, filter and `If` conditions `bool`, temporal bounds
//! `mz_timestamp`, and top-k limits `int8`.
//!
//! With the allowlist enforced, every function application must have a typing rule in
//! [`super::allowlist`] that its argument types satisfy, and the function's output type is
//! computed from those types. Without it, functions are typed by their output types only.
//!
//! A column type is a [`ColType`]. Checks skip columns whose type is not [`ColType::Known`].
//! [`ColType::Bottom`] marks columns no row reaches, where skipping checks loses nothing.
//! [`ColType::Unknown`] marks columns that rows may reach. With the allowlist enforced, every
//! site that produces an unknown type reports an error, including a type larger than
//! [`TYPE_SIZE_LIMIT`], and a function application or check that meets an unknown type
//! reports one too, so a plan that passes with the allowlist enforced has a known type for
//! every column a row reaches. Without the allowlist, unknown types pass every check.
//!
//! Input columns come as `Option<&[ColType]>`, where `None` stands for an input no row
//! reaches, whose columns are all [`ColType::Bottom`]. That avoids allocating vectors of
//! arities that the plan states but no input confirms.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use mz_expr::{AggregateFunc, Columns as _, EvalError, MapFilterProject, MfpPlan, TableFunc};
use mz_repr::{Datum, ReprColumnType, ReprScalarType, StableRow};

use crate::plan::join::{JoinClosure, JoinPlan};
use crate::plan::reduce::{BasicPlan, HierarchicalPlan, KeyValPlan, ReducePlan};
use crate::plan::scalar::LirScalarExpr;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan};

use super::Rule;
use super::allowlist::{
    TypingRule, aggregate_typing_rule, scalar_typing_rule, table_func_typing_rule,
};
use super::arity::{Issues, JoinPath};
use super::arrangements::Available;

/// What is known about the type of a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ColType {
    /// No row is known to reach the column. The identity of [`union`].
    Bottom,
    /// Rows may reach the column, but its type is not known.
    Unknown,
    /// The column has this type.
    Known(ReprColumnType),
}

/// The most nodes a column type the validator tracks may have. Larger types are treated as
/// unknown, which bounds the work of cloning, comparing and unifying a column type by a
/// constant.
pub(super) const TYPE_SIZE_LIMIT: usize = 256;

/// The number of nodes of `typ`, or `None` if it exceeds [`TYPE_SIZE_LIMIT`]. Costs work
/// proportional to its result.
pub(super) fn type_size(typ: &ReprColumnType) -> Option<usize> {
    let mut size: usize = 0;
    let mut todo = vec![&typ.scalar_type];
    while let Some(t) = todo.pop() {
        size += 1;
        if size > TYPE_SIZE_LIMIT {
            return None;
        }
        match t {
            ReprScalarType::Array(t)
            | ReprScalarType::List { element_type: t }
            | ReprScalarType::Map { value_type: t }
            | ReprScalarType::Range { element_type: t } => todo.push(t),
            ReprScalarType::Record { fields } => {
                // Every pending node is counted eventually, so a record whose fields alone
                // exceed the limit is rejected before they are queued.
                let pending = size.saturating_add(todo.len()).saturating_add(fields.len());
                if pending > TYPE_SIZE_LIMIT {
                    return None;
                }
                todo.extend(fields.iter().map(|f| &f.scalar_type));
            }
            _ => {}
        }
    }
    Some(size)
}

impl ColType {
    /// `typ` as a known column type, or unknown if it exceeds [`TYPE_SIZE_LIMIT`].
    pub(super) fn of(typ: ReprColumnType) -> ColType {
        match type_size(&typ) {
            Some(_) => ColType::Known(typ),
            None => ColType::Unknown,
        }
    }

    /// The units of work charged for handling this column type once.
    pub(super) fn size(&self) -> usize {
        match self {
            ColType::Known(t) => type_size(t).unwrap_or(TYPE_SIZE_LIMIT),
            ColType::Bottom | ColType::Unknown => 1,
        }
    }

    fn known(&self) -> Option<&ReprColumnType> {
        match self {
            ColType::Known(t) => Some(t),
            ColType::Bottom | ColType::Unknown => None,
        }
    }
}

/// The type of column `c` of `input`.
pub(super) fn col(input: Option<&[ColType]>, c: usize) -> ColType {
    match input {
        None => ColType::Bottom,
        // Out-of-range columns have been reported by the arity rules.
        Some(types) => types.get(c).cloned().unwrap_or(ColType::Unknown),
    }
}

/// The type of a column that takes its values from columns of types `a` and `b`, or the two
/// scalar types if they differ.
pub(super) fn union(a: &ColType, b: &ColType) -> Result<ColType, (ReprScalarType, ReprScalarType)> {
    match (a, b) {
        (ColType::Bottom, x) | (x, ColType::Bottom) => Ok(x.clone()),
        (ColType::Unknown, _) | (_, ColType::Unknown) => Ok(ColType::Unknown),
        (ColType::Known(a), ColType::Known(b)) => a
            .union(b)
            .map(ColType::Known)
            .map_err(|_| (a.scalar_type.clone(), b.scalar_type.clone())),
    }
}

/// Merges the column types `next` of a collection into `acc`, column by column.
///
/// Returns the first column whose scalar types differ, with the type in `acc` and the type
/// in `next`. Collections of different arities have been reported by the arity rules.
pub(super) fn union_into(
    acc: &mut Option<Vec<ColType>>,
    next: Option<&[ColType]>,
) -> Result<(), (usize, ReprScalarType, ReprScalarType)> {
    let Some(next) = next else { return Ok(()) };
    let Some(acc) = acc else {
        *acc = Some(next.to_vec());
        return Ok(());
    };
    for (c, n) in next.iter().enumerate() {
        let Some(a) = acc.get_mut(c) else { break };
        *a = union(a, n).map_err(|(a, n)| (c, a, n))?;
    }
    Ok(())
}

/// The scalar type every non-null datum of `datum`'s kind has, or `None` if datums of the
/// kind belong to several scalar types, or `datum` is null.
///
/// `allow_ambiguous` admits strings, numerics and booleans, which can also be `jsonb`,
/// under their non-JSON type.
fn datum_type(datum: Datum, allow_ambiguous: bool) -> Option<ReprScalarType> {
    use ReprScalarType as T;
    Some(match datum {
        Datum::False | Datum::True if allow_ambiguous => T::Bool,
        Datum::String(_) if allow_ambiguous => T::String,
        Datum::Numeric(_) if allow_ambiguous => T::Numeric,
        Datum::JsonNull => T::Jsonb,
        Datum::Int16(_) => T::Int16,
        Datum::Int32(_) => T::Int32,
        Datum::Int64(_) => T::Int64,
        Datum::UInt8(_) => T::UInt8,
        Datum::UInt16(_) => T::UInt16,
        Datum::UInt32(_) => T::UInt32,
        Datum::UInt64(_) => T::UInt64,
        Datum::Float32(_) => T::Float32,
        Datum::Float64(_) => T::Float64,
        Datum::Date(_) => T::Date,
        Datum::Time(_) => T::Time,
        Datum::Timestamp(_) => T::Timestamp,
        Datum::TimestampTz(_) => T::TimestampTz,
        Datum::Interval(_) => T::Interval,
        Datum::Bytes(_) => T::Bytes,
        Datum::Uuid(_) => T::Uuid,
        Datum::MzTimestamp(_) => T::MzTimestamp,
        Datum::MzAclItem(_) => T::MzAclItem,
        Datum::AclItem(_) => T::AclItem,
        // Element types cannot be read off empty or all-null containers, and a list can
        // also be a record or `jsonb`.
        Datum::False
        | Datum::True
        | Datum::String(_)
        | Datum::Numeric(_)
        | Datum::Array(_)
        | Datum::List(_)
        | Datum::Map(_)
        | Datum::Range(_)
        | Datum::Dummy
        | Datum::Null => return None,
    })
}

/// What a constant column's datums say about its type.
enum ConstantColumn {
    /// Only nulls so far.
    Null,
    Known(ReprScalarType),
    /// A datum whose type cannot be inferred.
    Uninferable,
    /// Datums of different types, which has been reported.
    Mixed,
}

/// Columns that an expression reads, by index.
type Columns<'a> = &'a dyn Fn(usize) -> ColType;

/// Infers and checks types, for user plans if `enforce` is set.
#[derive(Clone, Copy, Debug)]
pub(super) struct Typer {
    pub(super) enforce: bool,
}

impl Typer {
    /// `typ` as a known column type, or unknown if it exceeds [`TYPE_SIZE_LIMIT`], which is
    /// reported if the allowlist is enforced, since no check runs on an unknown type.
    pub(super) fn of(self, issues: &mut Issues, what: &str, typ: ReprColumnType) -> ColType {
        let t = ColType::of(typ);
        if self.enforce && t == ColType::Unknown {
            issues.push((
                Rule::Type,
                format!("{what} has a type of more than {TYPE_SIZE_LIMIT} nodes"),
            ));
        }
        t
    }

    /// Reports `t` unless it is known to have type `expected`, or no row reaches it. An
    /// unknown type is reported only if the allowlist is enforced.
    fn expect(self, issues: &mut Issues, what: &str, t: &ColType, expected: ReprScalarType) {
        match t {
            ColType::Known(t) if t.scalar_type != expected => issues.push((
                Rule::Type,
                format!("{what} has type {:?}, not {expected:?}", t.scalar_type),
            )),
            ColType::Unknown if self.enforce => issues.push((
                Rule::Type,
                format!("{what} has an unknown type, not {expected:?}"),
            )),
            ColType::Known(_) | ColType::Unknown | ColType::Bottom => {}
        }
    }

    /// Computes a function's output type.
    ///
    /// Without the allowlist, `output_type` runs on whatever types the plan applies the
    /// function to, and some implementations panic on unexpected input types, for example
    /// through `unwrap_list_element_type`. The panic is caught so that validation does not
    /// panic. The allowlisted functions' output types do not panic, see
    /// [`super::allowlist`].
    fn output(
        self,
        issues: &mut Issues,
        what: &str,
        name: &str,
        f: impl FnOnce() -> ReprColumnType,
    ) -> ColType {
        match mz_ore::panic::catch_unwind(AssertUnwindSafe(f)) {
            Ok(t) => self.of(issues, &format!("{what}: output of function {name}"), t),
            Err(_) => {
                if self.enforce {
                    issues.push((
                        Rule::Type,
                        format!("{what}: cannot compute the output type of function {name}"),
                    ));
                }
                ColType::Unknown
            }
        }
    }

    /// Checks `args` against the typing rule of a function, if the allowlist is enforced.
    ///
    /// Returns whether the application is admitted. With the allowlist enforced, an argument
    /// of unknown type is never admitted, since the rule cannot check it. An argument no row
    /// reaches is admitted, since the function is never evaluated on it.
    fn admit(
        self,
        issues: &mut Issues,
        what: &str,
        name: &str,
        rule: Option<TypingRule>,
        args: &[ColType],
    ) -> bool {
        if !self.enforce {
            return true;
        }
        let Some(rule) = rule else {
            issues.push((
                Rule::Function,
                format!("{what}: function {name} is not supported in user plans"),
            ));
            return false;
        };
        if let Some(i) = args.iter().position(|a| *a == ColType::Unknown) {
            issues.push((
                Rule::Type,
                format!("{what}: function {name} has argument {i} of unknown type"),
            ));
            return false;
        }
        let args: Vec<_> = args.iter().map(ColType::known).collect();
        match rule.check(&args) {
            Ok(()) => true,
            Err(expected) => {
                issues.push((
                    Rule::Type,
                    format!("{what}: function {name} expects {expected}"),
                ));
                false
            }
        }
    }

    /// The type of a literal, checking that its value is an instance of its declared type.
    fn literal(
        self,
        issues: &mut Issues,
        what: &str,
        value: &Result<StableRow, EvalError>,
        typ: &ReprColumnType,
    ) -> ColType {
        let what = format!("{what} literal");
        let Ok(row) = value else {
            return self.of(issues, &what, typ.clone());
        };
        // Evaluation unpacks the first datum and panics if there is none.
        let mut datums = row.iter();
        let (Some(datum), None) = (datums.next(), datums.next()) else {
            issues.push((
                Rule::Shape,
                format!(
                    "{what} holds {} datums, but a literal holds exactly one",
                    row.iter().count()
                ),
            ));
            return ColType::Unknown;
        };
        if datum.is_instance_of(typ) {
            self.of(issues, &what, typ.clone())
        } else {
            let nullability = if datum.is_null() { " non-null" } else { "" };
            issues.push((
                Rule::Type,
                format!(
                    "{what} is not an instance of its declared{nullability} type {:?}",
                    typ.scalar_type
                ),
            ));
            ColType::Unknown
        }
    }

    /// Infers the type of `expr` over the columns `cols`, and checks every application in it.
    ///
    /// Walks the expression iteratively, in post-order.
    pub(super) fn scalar(
        self,
        issues: &mut Issues,
        what: &str,
        expr: &LirScalarExpr,
        cols: Columns<'_>,
    ) -> ColType {
        enum Step<'a> {
            Visit(&'a LirScalarExpr),
            Apply(&'a LirScalarExpr),
        }
        let mut todo = vec![Step::Visit(expr)];
        let mut done: Vec<ColType> = Vec::new();
        while let Some(step) = todo.pop() {
            match step {
                Step::Visit(LirScalarExpr::Column(c, _)) => done.push(cols(*c)),
                Step::Visit(LirScalarExpr::Literal(value, typ)) => {
                    done.push(self.literal(issues, what, value, typ))
                }
                Step::Visit(e) => {
                    todo.push(Step::Apply(e));
                    todo.extend(e.children().rev().map(Step::Visit));
                }
                Step::Apply(e) => {
                    let n = e.children().count();
                    let args = done.split_off(done.len().saturating_sub(n));
                    done.push(self.apply(issues, what, e, args));
                }
            }
        }
        done.pop().unwrap_or(ColType::Unknown)
    }

    /// The type of the call or `If` node `e`, given the types of its arguments.
    fn apply(
        self,
        issues: &mut Issues,
        what: &str,
        e: &LirScalarExpr,
        args: Vec<ColType>,
    ) -> ColType {
        if let LirScalarExpr::If { .. } = e {
            let [cond, then, els] = args.as_slice() else {
                return ColType::Unknown;
            };
            self.expect(
                issues,
                &format!("{what} If condition"),
                cond,
                ReprScalarType::Bool,
            );
            return union(then, els).unwrap_or_else(|(a, b)| {
                issues.push((
                    Rule::Type,
                    format!("{what} If branches have types {a:?} and {b:?}"),
                ));
                ColType::Unknown
            });
        }
        let name = match e {
            LirScalarExpr::CallUnary { func, .. } => func.to_string(),
            LirScalarExpr::CallBinary { func, .. } => func.to_string(),
            LirScalarExpr::CallVariadic { func, .. } => func.to_string(),
            LirScalarExpr::Column(..) | LirScalarExpr::Literal(..) | LirScalarExpr::If { .. } => {
                return ColType::Unknown;
            }
        };
        if !self.admit(issues, what, &name, scalar_typing_rule(e), &args) {
            return ColType::Unknown;
        }
        if args.contains(&ColType::Bottom) {
            return ColType::Bottom;
        }
        let Some(known) = args
            .iter()
            .map(|a| a.known().cloned())
            .collect::<Option<Vec<_>>>()
        else {
            return ColType::Unknown;
        };
        self.output(issues, what, &name, || match e {
            LirScalarExpr::CallUnary { func, .. } => func.output_type(
                known
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| ReprScalarType::Bool.nullable(true)),
            ),
            LirScalarExpr::CallBinary { func, .. } => func.output_type(&known),
            LirScalarExpr::CallVariadic { func, .. } => func.output_type(known),
            // Handled above.
            LirScalarExpr::Column(..) | LirScalarExpr::Literal(..) | LirScalarExpr::If { .. } => {
                ReprScalarType::Bool.nullable(true)
            }
        })
    }

    /// Types the expressions and checks the predicates of `mfp` over `input`, and returns the
    /// types of the computed columns.
    fn map_filter(
        self,
        issues: &mut Issues,
        what: &str,
        mfp: &MapFilterProject<LirScalarExpr>,
        input: Columns<'_>,
    ) -> Vec<ColType> {
        let input_arity = mfp.input_arity;
        let mut computed: Vec<ColType> = Vec::with_capacity(mfp.expressions.len());
        for (i, expr) in mfp.expressions.iter().enumerate() {
            let t = {
                let cols = mfp_columns(input, input_arity, &computed);
                self.scalar(issues, &format!("{what} expression {i}"), expr, &cols)
            };
            computed.push(t);
        }
        {
            let cols = mfp_columns(input, input_arity, &computed);
            for (position, predicate) in &mfp.predicates {
                let what = format!("{what} predicate at position {position}");
                let t = self.scalar(issues, &what, predicate, &cols);
                self.expect(issues, &what, &t, ReprScalarType::Bool);
            }
        }
        computed
    }

    /// Checks `mfp` over `input`, and returns the types of its output.
    pub(super) fn mfp(
        self,
        issues: &mut Issues,
        what: &str,
        mfp: &MapFilterProject<LirScalarExpr>,
        input: Columns<'_>,
    ) -> Vec<ColType> {
        let computed = self.map_filter(issues, what, mfp, input);
        let cols = mfp_columns(input, mfp.input_arity, &computed);
        mfp.projection.iter().map(|c| cols(*c)).collect()
    }

    /// Checks `plan` over `input`, including its temporal bounds, and returns the types of its
    /// output.
    pub(super) fn mfp_plan(
        self,
        issues: &mut Issues,
        what: &str,
        plan: &MfpPlan<LirScalarExpr>,
        input: Columns<'_>,
    ) -> Vec<ColType> {
        let (mfp, lower, upper) = plan.as_parts();
        let computed = self.map_filter(issues, what, mfp, input);
        let cols = mfp_columns(input, mfp.input_arity, &computed);
        // `MfpPlan::evaluate` panics on a bound that is neither null nor an `mz_timestamp`.
        for bound in lower.iter().chain(upper) {
            let what = format!("{what} temporal bound");
            let t = self.scalar(issues, &what, bound, &cols);
            self.expect(issues, &what, &t, ReprScalarType::MzTimestamp);
        }
        mfp.projection.iter().map(|c| cols(*c)).collect()
    }

    /// The types of the expressions `exprs` over `input`.
    fn exprs(
        self,
        issues: &mut Issues,
        what: &str,
        exprs: &[LirScalarExpr],
        input: Option<&[ColType]>,
    ) -> Vec<ColType> {
        exprs
            .iter()
            .enumerate()
            .map(|(i, e)| self.scalar(issues, &format!("{what} {i}"), e, &|c| col(input, c)))
            .collect()
    }

    /// The types of the rows an arrangement of `input` by `key` presents: the key columns
    /// followed by the input columns the key does not reference, as in
    /// `permutation_for_arrangement`.
    fn arranged(
        self,
        issues: &mut Issues,
        what: &str,
        key: &[LirScalarExpr],
        input: Option<&[ColType]>,
    ) -> Option<Vec<ColType>> {
        let input = input?;
        let mut types = self.exprs(issues, what, key, Some(input));
        types.extend(thinned(key, input));
        Some(types)
    }

    /// The types of the rows an operator reading `input` through `key` sees.
    fn read(
        self,
        issues: &mut Issues,
        what: &str,
        key: Option<&[LirScalarExpr]>,
        input: Option<&[ColType]>,
    ) -> Option<Vec<ColType>> {
        match key {
            None => input.map(<[ColType]>::to_vec),
            Some(key) => self.arranged(issues, what, key, input),
        }
    }

    /// `Constant`: types each column from its datums.
    pub(super) fn constant<T, D>(
        self,
        issues: &mut Issues,
        rows: &[(StableRow, T, D)],
    ) -> Option<Vec<ColType>> {
        let arity = rows.first()?.0.iter().count();
        // Each column's evidence, and whether it holds a null.
        let mut columns: Vec<(ConstantColumn, bool)> =
            (0..arity).map(|_| (ConstantColumn::Null, false)).collect();
        for (row, _, _) in rows {
            for (c, datum) in row.iter().enumerate() {
                // Rows of other arities have been reported by the arity rules.
                let Some((column, nullable)) = columns.get_mut(c) else {
                    break;
                };
                if datum.is_null() {
                    *nullable = true;
                    continue;
                }
                // Without the allowlist, a datum that may be `jsonb` leaves its column
                // unknown, because the plan's readers may expect either type.
                *column = match (&*column, datum_type(datum, self.enforce)) {
                    (ConstantColumn::Mixed, _) => continue,
                    (_, None) => ConstantColumn::Uninferable,
                    (ConstantColumn::Uninferable, Some(_)) => continue,
                    (ConstantColumn::Null, Some(t)) => ConstantColumn::Known(t),
                    (ConstantColumn::Known(prev), Some(t)) if *prev == t => continue,
                    (ConstantColumn::Known(prev), Some(t)) => {
                        issues.push((
                            Rule::Type,
                            format!("constant column {c} mixes types {prev:?} and {t:?}"),
                        ));
                        ConstantColumn::Mixed
                    }
                };
            }
        }
        let types = columns
            .into_iter()
            .enumerate()
            .map(|(c, (column, nullable))| match column {
                ConstantColumn::Known(t) => self.of(
                    issues,
                    &format!("constant column {c}"),
                    t.nullable(nullable),
                ),
                ConstantColumn::Null => {
                    if self.enforce {
                        issues.push((
                            Rule::Type,
                            format!(
                                "cannot infer the type of an all-null constant column (column {c})"
                            ),
                        ));
                    }
                    ColType::Unknown
                }
                ConstantColumn::Uninferable => {
                    if self.enforce {
                        issues.push((
                            Rule::Type,
                            format!("cannot infer the type of constant column {c}"),
                        ));
                    }
                    ColType::Unknown
                }
                ConstantColumn::Mixed => ColType::Unknown,
            })
            .collect();
        Some(types)
    }
}

/// The columns an MFP's expressions read: the input columns, then the computed ones.
fn mfp_columns<'a>(
    input: Columns<'a>,
    input_arity: usize,
    computed: &'a [ColType],
) -> impl Fn(usize) -> ColType + 'a {
    move |c| {
        if c < input_arity {
            input(c)
        } else {
            computed
                .get(c - input_arity)
                .cloned()
                .unwrap_or(ColType::Unknown)
        }
    }
}

/// The types of the input columns an arrangement by `key` keeps as values.
fn thinned<'a>(key: &[LirScalarExpr], input: &'a [ColType]) -> impl Iterator<Item = ColType> + 'a {
    let in_key: BTreeSet<usize> = key.iter().filter_map(|k| k.as_column()).collect();
    input
        .iter()
        .enumerate()
        .filter(move |(c, _)| !in_key.contains(c))
        .map(|(_, t)| t.clone())
}

/// Operator rules. Each takes the column types of the inputs, `None` for an input no row
/// reaches, and returns the column types of the output in the same form.
impl Typer {
    /// `Get`: the types of the output, given the types of the bound collection.
    pub(super) fn get(
        self,
        issues: &mut Issues,
        plan: &GetPlan,
        binding: Option<&[ColType]>,
    ) -> Option<Vec<ColType>> {
        match plan {
            GetPlan::PassArrangements => binding.map(<[ColType]>::to_vec),
            GetPlan::Arrangement(key, _val, mfp) => {
                let read = self.arranged(issues, "Get arrangement key", key, binding);
                Some(self.mfp_plan(issues, "Get MFP", mfp, &|c| col(read.as_deref(), c)))
            }
            GetPlan::Collection(mfp) => {
                Some(self.mfp_plan(issues, "Get MFP", mfp, &|c| col(binding, c)))
            }
        }
    }

    /// `Mfp`: the types of the output.
    pub(super) fn mfp_node(
        self,
        issues: &mut Issues,
        plan: &MfpPlan<LirScalarExpr>,
        input_key_val: Option<&[LirScalarExpr]>,
        input: Option<&[ColType]>,
    ) -> Option<Vec<ColType>> {
        // Rendering hands the input through unchanged when the MFP is the identity.
        if plan.is_identity() {
            return input.map(<[ColType]>::to_vec);
        }
        let read = self.read(issues, "Mfp input key", input_key_val, input);
        Some(self.mfp_plan(issues, "Mfp", plan, &|c| col(read.as_deref(), c)))
    }

    /// The column types a table function produces from arguments of types `args`, or the
    /// type of every output column.
    fn table_func(
        self,
        issues: &mut Issues,
        func: &TableFunc,
        args: &[ColType],
    ) -> Result<Vec<ColType>, ColType> {
        let name = func.to_string();
        let what = "FlatMap";
        if !self.admit(issues, what, &name, table_func_typing_rule(func), args) {
            return Err(ColType::Unknown);
        }
        if args.contains(&ColType::Bottom) {
            return Err(ColType::Bottom);
        }
        let arity = func.checked_output_arity();
        let output = mz_ore::panic::catch_unwind(AssertUnwindSafe(|| func.output_type()));
        let mut types: Vec<ColType> = match output {
            Ok(typ) if Some(typ.arity()) == arity => typ
                .column_types
                .into_iter()
                .enumerate()
                .map(|(i, t)| {
                    self.of(
                        issues,
                        &format!("{what}: output column {i} of function {name}"),
                        t,
                    )
                })
                .collect(),
            _ => {
                if self.enforce {
                    issues.push((
                        Rule::Type,
                        format!("{what}: cannot compute the output type of function {name}"),
                    ));
                }
                return Err(ColType::Unknown);
            }
        };
        // `wrap` passes its arguments through, so their nullability reaches its output
        // whatever its declared types say.
        if let TableFunc::Wrap { width, .. } = func {
            for (i, arg) in args.iter().enumerate() {
                let Some(t) = types.get_mut(i.checked_rem(*width).unwrap_or(0)) else {
                    continue;
                };
                *t = match (&*t, arg) {
                    (ColType::Known(t), ColType::Known(a)) => ColType::Known(ReprColumnType {
                        scalar_type: t.scalar_type.clone(),
                        nullable: t.nullable || a.nullable,
                    }),
                    _ => ColType::Unknown,
                };
            }
        }
        Ok(types)
    }

    /// `FlatMap`: the types of the output.
    pub(super) fn flat_map(
        self,
        issues: &mut Issues,
        input_key: Option<&[LirScalarExpr]>,
        exprs: &[LirScalarExpr],
        func: &TableFunc,
        mfp_after: &MfpPlan<LirScalarExpr>,
        input: Option<&[ColType]>,
    ) -> Vec<ColType> {
        let read = self.read(issues, "FlatMap input key", input_key, input);
        let args = self.exprs(issues, "FlatMap argument", exprs, read.as_deref());
        let produced = self.table_func(issues, func, &args);
        // The MFP sees the input row followed by the table function's output.
        let row = |c: usize| match &read {
            None => ColType::Bottom,
            Some(read) if c < read.len() => read[c].clone(),
            Some(read) => match &produced {
                Ok(types) => types
                    .get(c - read.len())
                    .cloned()
                    .unwrap_or(ColType::Unknown),
                Err(fill) => fill.clone(),
            },
        };
        self.mfp_plan(issues, "FlatMap MFP", mfp_after, &row)
    }

    /// Checks a join closure over rows of types `row`, and returns the types of its output.
    fn closure(
        self,
        issues: &mut Issues,
        what: &str,
        closure: &JoinClosure,
        row: &[ColType],
    ) -> Vec<ColType> {
        for (k, class) in closure.ready_equivalences.iter().enumerate() {
            let types = self.exprs(issues, &format!("{what} equivalence"), class, Some(row));
            if self.enforce {
                if let Some(i) = types.iter().position(|t| *t == ColType::Unknown) {
                    issues.push((
                        Rule::Type,
                        format!("{what} equivalence class {k} expression {i} has an unknown type"),
                    ));
                }
            }
            let mut known = types.iter().filter_map(ColType::known);
            if let Some(first) = known.next() {
                if let Some(other) = known.find(|t| t.scalar_type != first.scalar_type) {
                    issues.push((
                        Rule::Type,
                        format!(
                            "{what} equivalence class {k} relates types {:?} and {:?}",
                            first.scalar_type, other.scalar_type
                        ),
                    ));
                }
            }
        }
        self.mfp(issues, what, &closure.before, &|c| col(Some(row), c))
    }

    /// The types of the output of one join path, given the types of the join's inputs.
    fn join_path(
        self,
        issues: &mut Issues,
        what: &str,
        path: &JoinPath<'_>,
        linear: bool,
        inputs: &[&[ColType]],
        forms: &[&Available],
    ) -> Option<Vec<ColType>> {
        let source = *inputs.get(path.source_relation)?;
        // A linear join without an initial closure streams an existing arrangement of its
        // source by the first stage's stream key, whatever `source_key` says, see
        // `arrangements::join`.
        let streams_arrangement = linear
            && path.initial_closure.is_none()
            && path.stages.first().is_some_and(|stage| {
                forms
                    .get(path.source_relation)
                    .is_some_and(|f| f.keys.contains(stage.stream_key))
            });
        let source_what = format!("{what} source key");
        let mut stream = match path.initial_closure {
            Some(closure) => {
                let read = self.read(issues, &source_what, path.source_key, Some(source));
                let read = read.unwrap_or_default();
                self.closure(issues, &format!("{what} initial closure"), closure, &read)
            }
            None if streams_arrangement => source.to_vec(),
            None => self
                .read(issues, &source_what, path.source_key, Some(source))
                .unwrap_or_default(),
        };
        for (i, stage) in path.stages.iter().enumerate() {
            let what = format!("{what} stage {i}");
            let lookup = *inputs.get(stage.lookup_relation)?;
            let stream_key = self.exprs(
                issues,
                &format!("{what} stream key"),
                stage.stream_key,
                Some(&stream),
            );
            let lookup_key = self.exprs(
                issues,
                &format!("{what} lookup key"),
                stage.lookup_key,
                Some(lookup),
            );
            // The closure sees the key, the stream values, and the lookup values.
            let mut row = Vec::new();
            // Keys of different lengths have been reported by the arity rules.
            for (j, s) in stream_key.iter().enumerate() {
                let Some(l) = lookup_key.get(j) else { break };
                row.push(union(s, l).unwrap_or_else(|(s, l)| {
                    issues.push((
                        Rule::Type,
                        format!(
                            "{what} joins stream key column {j} of type {s:?} with a lookup key column of type {l:?}"
                        ),
                    ));
                    ColType::Unknown
                }));
            }
            row.extend(stage.stream_thinning.iter().map(|c| col(Some(&stream), *c)));
            row.extend(thinned(stage.lookup_key, lookup));
            stream = self.closure(issues, &format!("{what} closure"), stage.closure, &row);
        }
        if let Some(closure) = path.final_closure {
            stream = self.closure(issues, &format!("{what} final closure"), closure, &stream);
        }
        Some(stream)
    }

    /// `Join`: the types of the output, given the types of the inputs.
    ///
    /// An input no row reaches makes the output one no row reaches.
    pub(super) fn join(
        self,
        issues: &mut Issues,
        plan: &JoinPlan,
        inputs: &[Option<&[ColType]>],
        forms: &[&Available],
    ) -> Option<Vec<ColType>> {
        let inputs: Vec<&[ColType]> = inputs.iter().copied().collect::<Option<_>>()?;
        match plan {
            JoinPlan::Linear(plan) => self.join_path(
                issues,
                "linear join",
                &JoinPath::linear(plan),
                true,
                &inputs,
                forms,
            ),
            JoinPlan::Delta(plan) => {
                let mut output = None;
                for (i, path) in JoinPath::delta(plan).iter().enumerate() {
                    let what = format!("delta join path {i}");
                    let types = self.join_path(issues, &what, path, false, &inputs, forms);
                    if let Err((c, expected, t)) = union_into(&mut output, types.as_deref()) {
                        issues.push((
                            Rule::Type,
                            format!(
                                "{what} produces column {c} of type {t:?}, but an earlier path produces type {expected:?}"
                            ),
                        ));
                    }
                }
                output
            }
        }
    }

    /// The type of an aggregate over arguments of type `input`.
    fn aggregate(
        self,
        issues: &mut Issues,
        what: &str,
        func: &AggregateFunc,
        input: ColType,
    ) -> ColType {
        let name = func.name();
        let args = std::slice::from_ref(&input);
        if !self.admit(issues, what, name, aggregate_typing_rule(func), args) {
            return ColType::Unknown;
        }
        match input {
            ColType::Known(t) => self.output(issues, what, name, || func.output_type(t)),
            other => other,
        }
    }

    /// `Reduce`: the types of the output.
    ///
    /// The value plan produces one column per aggregate, which is the aggregate's argument.
    pub(super) fn reduce(
        self,
        issues: &mut Issues,
        input_key: Option<&[LirScalarExpr]>,
        key_val_plan: &KeyValPlan,
        plan: &ReducePlan,
        mfp_after: &MapFilterProject<LirScalarExpr>,
        input: Option<&[ColType]>,
    ) -> Vec<ColType> {
        let read = self.read(issues, "Reduce input key", input_key, input);
        let read_cols = |c| col(read.as_deref(), c);
        let key = self.mfp(
            issues,
            "Reduce key plan",
            &key_val_plan.key_plan,
            &read_cols,
        );
        let values = self.mfp(
            issues,
            "Reduce value plan",
            &key_val_plan.val_plan,
            &read_cols,
        );
        let funcs: Vec<&AggregateFunc> = match plan {
            ReducePlan::Distinct => vec![],
            ReducePlan::Accumulable(p) => p.full_aggrs.iter().map(|a| &a.func).collect(),
            ReducePlan::Hierarchical(HierarchicalPlan::Monotonic(p)) => {
                p.aggr_funcs.iter().collect()
            }
            ReducePlan::Hierarchical(HierarchicalPlan::Bucketed(p)) => {
                p.aggr_funcs.iter().collect()
            }
            ReducePlan::Basic(BasicPlan::Single(p)) => vec![&p.expr.func],
            ReducePlan::Basic(BasicPlan::Multiple(aggrs)) => {
                aggrs.iter().map(|a| &a.func).collect()
            }
        };
        let mut row = key.clone();
        for (i, func) in funcs.into_iter().enumerate() {
            let input = values.get(i).cloned().unwrap_or(ColType::Unknown);
            row.push(self.aggregate(issues, &format!("Reduce aggregate {i}"), func, input));
        }
        let output = self.mfp(issues, "Reduce MFP", mfp_after, &|c| col(Some(&row), c));
        // A distinct reduction applies `mfp_after` as a filter and outputs only the key.
        if matches!(plan, ReducePlan::Distinct) {
            key
        } else {
            output
        }
    }

    /// `TopK`: checks the limit, and returns the types of the output, which are those of the
    /// input.
    pub(super) fn top_k(
        self,
        issues: &mut Issues,
        plan: &TopKPlan,
        input: Option<&[ColType]>,
    ) -> Option<Vec<ColType>> {
        let limit = match plan {
            TopKPlan::MonotonicTop1(_) => None,
            TopKPlan::MonotonicTopK(p) => p.limit.as_ref(),
            TopKPlan::Basic(p) => p.limit.as_ref(),
        };
        if let Some(limit) = limit {
            // Rendering unwraps the limit as an `int8`.
            let t = self.scalar(issues, "TopK limit", limit, &|c| col(input, c));
            self.expect(issues, "TopK limit", &t, ReprScalarType::Int64);
        }
        input.map(<[ColType]>::to_vec)
    }

    /// `ArrangeBy`: checks the keys it evaluates and the rows it forms, and returns the types
    /// of the output.
    ///
    /// Rendering applies `input_mfp` to the arrangement named by `input_key` only when it
    /// has to form the raw collection, and the formed rows then replace the input's rows.
    pub(super) fn arrange_by(
        self,
        issues: &mut Issues,
        input_key: Option<&[LirScalarExpr]>,
        input_mfp: &MfpPlan<LirScalarExpr>,
        forms: &AvailableCollections,
        available: &Available,
        input: Option<&[ColType]>,
    ) -> Option<Vec<ColType>> {
        for (key, _, _) in &forms.arranged {
            self.exprs(issues, "ArrangeBy key", key, input);
        }
        let read = self.read(issues, "ArrangeBy input key", input_key, input);
        let formed = self.mfp_plan(issues, "ArrangeBy input MFP", input_mfp, &|c| {
            col(read.as_deref(), c)
        });
        let input = input?;
        let needs_raw = forms.raw
            || forms
                .arranged
                .iter()
                .any(|(key, _, _)| !available.keys.contains(key));
        if available.raw || !needs_raw {
            return Some(input.to_vec());
        }
        let mut output = Vec::with_capacity(input.len());
        // An input MFP of another arity has been reported by the arity rules.
        for (c, i) in input.iter().enumerate() {
            let Some(f) = formed.get(c) else { break };
            output.push(union(i, f).unwrap_or_else(|(i, f)| {
                issues.push((
                    Rule::Type,
                    format!(
                        "ArrangeBy input MFP forms column {c} of type {f:?}, but its input column {c} has type {i:?}"
                    ),
                ));
                ColType::Unknown
            }));
        }
        Some(output)
    }

    /// Checks the MFP of a sink's `from_arrangement` over an arrangement of a collection of
    /// types `input` by `key`.
    pub(super) fn sink_arrangement(
        self,
        issues: &mut Issues,
        key: &[LirScalarExpr],
        mfp: &MfpPlan<LirScalarExpr>,
        input: Option<&[ColType]>,
    ) {
        let read = self.arranged(issues, "sink arrangement key", key, input);
        self.mfp_plan(issues, "sink MFP", mfp, &|c| col(read.as_deref(), c));
    }
}
