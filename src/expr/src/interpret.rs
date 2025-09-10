// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt::Debug;

use mz_repr::{Datum, Row, RowArena, SqlColumnType, SqlRelationType, SqlScalarType};

use crate::{
    BinaryFunc, EvalError, MapFilterProject, MfpPlan, MirScalarExpr, UnaryFunc,
    UnmaterializableFunc, VariadicFunc,
};

/// An inclusive range of non-null datum values.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Values<'a> {
    /// This range contains no values.
    Empty,
    /// An inclusive range. Invariant: the first element is always <= the second.
    // TODO: a variant for small sets of data would avoid losing precision here.
    Within(Datum<'a>, Datum<'a>),
    /// Constraints on structured fields, useful for recursive structures like maps.
    /// Fields that are not present in the map default to Values::All.
    // TODO: consider using this variant, or similar, for Datum::List.
    Nested(BTreeMap<Datum<'a>, ResultSpec<'a>>),
    /// This range might contain any value. Since we're overapproximating, this is often used
    /// as a "safe fallback" when we can't determine the right boundaries for a range.
    All,
}

impl<'a> Values<'a> {
    fn just(a: Datum<'a>) -> Values<'a> {
        match a {
            Datum::Map(datum_map) => Values::Nested(
                datum_map
                    .iter()
                    .map(|(key, val)| (key.into(), ResultSpec::value(val)))
                    .collect(),
            ),
            other => Self::Within(other, other),
        }
    }

    fn union(self, other: Values<'a>) -> Values<'a> {
        match (self, other) {
            (Values::Empty, r) => r,
            (r, Values::Empty) => r,
            (Values::Within(a0, a1), Values::Within(b0, b1)) => {
                Values::Within(a0.min(b0), a1.max(b1))
            }
            (Values::Nested(mut a), Values::Nested(mut b)) => {
                a.retain(|datum, values| {
                    if let Some(other_values) = b.remove(datum) {
                        *values = values.clone().union(other_values);
                    }
                    *values != ResultSpec::anything()
                });
                if a.is_empty() {
                    Values::All
                } else {
                    Values::Nested(a)
                }
            }
            _ => Values::All,
        }
    }

    fn intersect(self, other: Values<'a>) -> Values<'a> {
        match (self, other) {
            (Values::Empty, _) => Values::Empty,
            (_, Values::Empty) => Values::Empty,
            (Values::Within(a0, a1), Values::Within(b0, b1)) => {
                let min = a0.max(b0);
                let max = a1.min(b1);
                if min <= max {
                    Values::Within(min, max)
                } else {
                    Values::Empty
                }
            }
            (Values::Nested(mut a), Values::Nested(b)) => {
                for (datum, other_spec) in b {
                    let spec = a.entry(datum).or_insert_with(ResultSpec::anything);
                    *spec = spec.clone().intersect(other_spec);
                }
                Values::Nested(a)
            }
            (Values::All, v) => v,
            (v, Values::All) => v,
            (Values::Nested(_), Values::Within(_, _)) => Values::Empty,
            (Values::Within(_, _), Values::Nested(_)) => Values::Empty,
        }
    }

    fn may_contain(&self, value: Datum<'a>) -> bool {
        match self {
            Values::Empty => false,
            Values::Within(min, max) => *min <= value && value <= *max,
            Values::All => true,
            Values::Nested(field_map) => match value {
                Datum::Map(datum_map) => {
                    datum_map
                        .iter()
                        .all(|(key, val)| match field_map.get(&key.into()) {
                            None => true,
                            Some(nested) => nested.may_contain(val),
                        })
                }
                _ => false,
            },
        }
    }
}

/// An approximation of the set of values an expression might have, including whether or not it
/// might be null or an error value. This is generally an _overapproximation_, in the sense that
/// [ResultSpec::may_contain] may return true even if the argument is not included in the set.
/// (However, it should never return false when the value _is_ included!)
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ResultSpec<'a> {
    /// True if the expression may evaluate to [Datum::Null].
    nullable: bool,
    /// True if the expression may evaluate to an error.
    fallible: bool,
    /// The range of possible (non-null) values that the expression may evaluate to.
    values: Values<'a>,
}

impl<'a> ResultSpec<'a> {
    /// No results match this spec. (For example, an empty table.)
    pub fn nothing() -> Self {
        ResultSpec {
            nullable: false,
            fallible: false,
            values: Values::Empty,
        }
    }

    /// Every result matches this spec.
    pub fn anything() -> Self {
        ResultSpec {
            nullable: true,
            fallible: true,
            values: Values::All,
        }
    }

    /// Every result matches this spec.
    pub fn any_infallible() -> Self {
        ResultSpec {
            nullable: true,
            fallible: false,
            values: Values::All,
        }
    }

    /// A spec that only matches null.
    pub fn null() -> Self {
        ResultSpec {
            nullable: true,
            ..Self::nothing()
        }
    }

    /// A spec that only matches error values.
    pub fn fails() -> Self {
        ResultSpec {
            fallible: true,
            ..Self::nothing()
        }
    }

    /// A spec that matches all values of a given type.
    pub fn has_type(col: &SqlColumnType, fallible: bool) -> ResultSpec<'a> {
        let values = match &col.scalar_type {
            SqlScalarType::Bool => Values::Within(Datum::False, Datum::True),
            // TODO: add bounds for other bounded types, like integers
            _ => Values::All,
        };
        ResultSpec {
            nullable: col.nullable,
            fallible,
            values,
        }
    }

    /// A spec that only matches the given value.
    pub fn value(value: Datum<'a>) -> ResultSpec<'a> {
        match value {
            Datum::Null => Self::null(),
            nonnull => ResultSpec {
                values: Values::just(nonnull),
                ..Self::nothing()
            },
        }
    }

    /// A spec that matches values between the given (non-null) min and max.
    pub fn value_between(min: Datum<'a>, max: Datum<'a>) -> ResultSpec<'a> {
        assert!(!min.is_null());
        assert!(!max.is_null());
        if min <= max {
            ResultSpec {
                values: Values::Within(min, max),
                ..ResultSpec::nothing()
            }
        } else {
            ResultSpec::nothing()
        }
    }

    /// A spec that matches any non-null value.
    pub fn value_all() -> ResultSpec<'a> {
        ResultSpec {
            values: Values::All,
            ..ResultSpec::nothing()
        }
    }

    /// A spec that matches Datum::Maps of the given type.
    pub fn map_spec(map: BTreeMap<Datum<'a>, ResultSpec<'a>>) -> ResultSpec<'a> {
        ResultSpec {
            values: Values::Nested(map),
            ..ResultSpec::nothing()
        }
    }

    /// Given two specs, returns a new spec that matches anything that either original spec would match.
    pub fn union(self, other: ResultSpec<'a>) -> ResultSpec<'a> {
        ResultSpec {
            nullable: self.nullable || other.nullable,
            fallible: self.fallible || other.fallible,
            values: self.values.union(other.values),
        }
    }

    /// Given two specs, returns a new spec that only matches things that both original specs would match.
    pub fn intersect(self, other: ResultSpec<'a>) -> ResultSpec<'a> {
        ResultSpec {
            nullable: self.nullable && other.nullable,
            fallible: self.fallible && other.fallible,
            values: self.values.intersect(other.values),
        }
    }

    /// Check if a particular value matches the spec.
    pub fn may_contain(&self, value: Datum<'a>) -> bool {
        if value == Datum::Null {
            return self.nullable;
        }

        self.values.may_contain(value)
    }

    /// Check if an error value matches the spec.
    pub fn may_fail(&self) -> bool {
        self.fallible
    }

    /// This method "maps" a function across the `ResultSpec`.
    ///
    /// As mentioned above, `ResultSpec` represents an approximate set of results.
    /// If we actually stored each result in the set, `flat_map` could be implemented by passing
    /// each result to the function one-by-one and unioning the resulting sets. This is possible
    /// when our values set is empty or contains a single datum, but when it contains a range,
    /// we can't enumerate all possible values of the set. We handle this by:
    /// - tracking whether the function is monotone, in which case we can map the range by just
    ///   mapping the endpoints;
    /// - using a safe default when we can't infer a tighter bound on the set, eg. [Self::anything].
    fn flat_map(
        &self,
        is_monotone: bool,
        mut result_map: impl FnMut(Result<Datum<'a>, EvalError>) -> ResultSpec<'a>,
    ) -> ResultSpec<'a> {
        let null_spec = if self.nullable {
            result_map(Ok(Datum::Null))
        } else {
            ResultSpec::nothing()
        };

        let error_spec = if self.fallible {
            // Since we only care about whether / not an error is possible, and not the specific
            // error, create an arbitrary error here.
            // NOTE! This assumes that functions do not discriminate on the type of the error.
            let map_err = result_map(Err(EvalError::Internal("".into())));
            let raise_err = ResultSpec::fails();
            // SQL has a very loose notion of evaluation order: https://www.postgresql.org/docs/current/sql-expressions.html#SYNTAX-EXPRESS-EVAL
            // Here, we account for the possibility that the expression is evaluated strictly,
            // raising the error, or that it's evaluated lazily by the result_map function
            // (which may return a non-error result even when given an error as input).
            raise_err.union(map_err)
        } else {
            ResultSpec::nothing()
        };

        let values_spec = match self.values {
            Values::Empty => ResultSpec::nothing(),
            // If this range contains a single datum, just call the function.
            Values::Within(min, max) if min == max => result_map(Ok(min)),
            // If this is a range of booleans, we know all the values... just try them.
            Values::Within(Datum::False, Datum::True) => {
                result_map(Ok(Datum::False)).union(result_map(Ok(Datum::True)))
            }
            // Otherwise, if our function is monotonic, we can try mapping the input
            // range to an output range.
            Values::Within(min, max) if is_monotone => {
                let min_result = result_map(Ok(min));
                let max_result = result_map(Ok(max));
                match (min_result, max_result) {
                    // If both endpoints give us a range, the result is a union of those ranges.
                    (
                        ResultSpec {
                            nullable: false,
                            fallible: false,
                            values: a_values,
                        },
                        ResultSpec {
                            nullable: false,
                            fallible: false,
                            values: b_values,
                        },
                    ) => ResultSpec {
                        nullable: false,
                        fallible: false,
                        values: a_values.union(b_values),
                    },
                    // If both endpoints are null, we assume the whole range maps to null.
                    (
                        ResultSpec {
                            nullable: true,
                            fallible: false,
                            values: Values::Empty,
                        },
                        ResultSpec {
                            nullable: true,
                            fallible: false,
                            values: Values::Empty,
                        },
                    ) => ResultSpec::null(),
                    // Otherwise we can't assume anything about the output.
                    _ => ResultSpec::anything(),
                }
            }
            // TODO: we could return a narrower result for eg. `Values::Nested` with all-`Within` fields.
            Values::Within(_, _) | Values::Nested(_) | Values::All => ResultSpec::anything(),
        };

        null_spec.union(error_spec).union(values_spec)
    }
}

/// [Abstract interpretation](https://en.wikipedia.org/wiki/Abstract_interpretation) for
/// [MirScalarExpr].
///
/// [MirScalarExpr::eval] implements a "concrete interpreter" for the expression type: given an
/// expression and specific column values as input, it returns a specific value for the output.
/// This could be reimplemented using this trait... but it's most useful for "abstract"
/// interpretations of the expression, where we generalize about sets of possible inputs and outputs.
/// See [Trace] and [ColumnSpecs] for how this can be useful in practice.
pub trait Interpreter {
    type Summary: Clone + Debug + Sized;

    /// A column of the input row.
    fn column(&self, id: usize) -> Self::Summary;

    /// A literal value.
    /// (Stored as a row, because we can't own a Datum.)
    fn literal(&self, result: &Result<Row, EvalError>, col_type: &SqlColumnType) -> Self::Summary;

    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `MirScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    fn unmaterializable(&self, func: &UnmaterializableFunc) -> Self::Summary;

    /// A function call that takes one expression as an argument.
    fn unary(&self, func: &UnaryFunc, expr: Self::Summary) -> Self::Summary;

    /// A function call that takes two expressions as arguments.
    fn binary(&self, func: &BinaryFunc, left: Self::Summary, right: Self::Summary)
    -> Self::Summary;

    /// A function call that takes an arbitrary number of arguments.
    fn variadic(&self, func: &VariadicFunc, exprs: Vec<Self::Summary>) -> Self::Summary;

    /// Conditionally evaluated expressions.
    fn cond(&self, cond: Self::Summary, then: Self::Summary, els: Self::Summary) -> Self::Summary;

    /// Evaluate an entire expression, by delegating to the fine-grained methods on [Interpreter].
    fn expr(&self, expr: &MirScalarExpr) -> Self::Summary {
        match expr {
            MirScalarExpr::Column(id, _name) => self.column(*id),
            MirScalarExpr::Literal(value, col_type) => self.literal(value, col_type),
            MirScalarExpr::CallUnmaterializable(func) => self.unmaterializable(func),
            MirScalarExpr::CallUnary { func, expr } => {
                let expr_range = self.expr(expr);
                self.unary(func, expr_range)
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                let expr1_range = self.expr(expr1);
                let expr2_range = self.expr(expr2);
                self.binary(func, expr1_range, expr2_range)
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                let exprs: Vec<_> = exprs.into_iter().map(|e| self.expr(e)).collect();
                self.variadic(func, exprs)
            }
            MirScalarExpr::If { cond, then, els } => {
                let cond_range = self.expr(cond);
                let then_range = self.expr(then);
                let els_range = self.expr(els);
                self.cond(cond_range, then_range, els_range)
            }
        }
    }

    /// Specifically, this evaluates the map and filters stages of an MFP: summarize each of the
    /// map expressions, then `and` together all of the filters.
    fn mfp_filter(&self, mfp: &MapFilterProject) -> Self::Summary {
        let mfp_eval = MfpEval::new(self, mfp.input_arity, &mfp.expressions);
        // NB: self should not be used after this point!
        let predicates = mfp
            .predicates
            .iter()
            .map(|(_, e)| mfp_eval.expr(e))
            .collect();
        mfp_eval.variadic(&VariadicFunc::And, predicates)
    }

    /// Similar to [Self::mfp_filter], but includes the additional temporal filters that have been
    /// broken out.
    fn mfp_plan_filter(&self, plan: &MfpPlan) -> Self::Summary {
        let mfp_eval = MfpEval::new(self, plan.mfp.input_arity, &plan.mfp.expressions);
        // NB: self should not be used after this point!
        let mut results: Vec<_> = plan
            .mfp
            .predicates
            .iter()
            .map(|(_, e)| mfp_eval.expr(e))
            .collect();
        let mz_now = mfp_eval.unmaterializable(&UnmaterializableFunc::MzNow);
        for bound in &plan.lower_bounds {
            let bound_range = mfp_eval.expr(bound);
            let result = mfp_eval.binary(&BinaryFunc::Lte, bound_range, mz_now.clone());
            results.push(result);
        }
        for bound in &plan.upper_bounds {
            let bound_range = mfp_eval.expr(bound);
            let result = mfp_eval.binary(&BinaryFunc::Gte, bound_range, mz_now.clone());
            results.push(result);
        }
        self.variadic(&VariadicFunc::And, results)
    }
}

/// Wrap another interpreter, but tack a few extra columns on at the end. An internal implementation
/// detail of `eval_mfp` and `eval_mfp_plan`.
pub(crate) struct MfpEval<'a, E: Interpreter + ?Sized> {
    evaluator: &'a E,
    input_arity: usize,
    expressions: Vec<E::Summary>,
}

impl<'a, E: Interpreter + ?Sized> MfpEval<'a, E> {
    pub(crate) fn new(evaluator: &'a E, input_arity: usize, expressions: &[MirScalarExpr]) -> Self {
        let mut mfp_eval = MfpEval {
            evaluator,
            input_arity,
            expressions: vec![],
        };
        for expr in expressions {
            let result = mfp_eval.expr(expr);
            mfp_eval.expressions.push(result);
        }
        mfp_eval
    }
}

impl<'a, E: Interpreter + ?Sized> Interpreter for MfpEval<'a, E> {
    type Summary = E::Summary;

    fn column(&self, id: usize) -> Self::Summary {
        if id < self.input_arity {
            self.evaluator.column(id)
        } else {
            self.expressions[id - self.input_arity].clone()
        }
    }

    fn literal(&self, result: &Result<Row, EvalError>, col_type: &SqlColumnType) -> Self::Summary {
        self.evaluator.literal(result, col_type)
    }

    fn unmaterializable(&self, func: &UnmaterializableFunc) -> Self::Summary {
        self.evaluator.unmaterializable(func)
    }

    fn unary(&self, func: &UnaryFunc, expr: Self::Summary) -> Self::Summary {
        self.evaluator.unary(func, expr)
    }

    fn binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary {
        self.evaluator.binary(func, left, right)
    }

    fn variadic(&self, func: &VariadicFunc, exprs: Vec<Self::Summary>) -> Self::Summary {
        self.evaluator.variadic(func, exprs)
    }

    fn cond(&self, cond: Self::Summary, then: Self::Summary, els: Self::Summary) -> Self::Summary {
        self.evaluator.cond(cond, then, els)
    }
}

/// A unary function we've added special-case handling for; including:
/// - A three-argument function, taking and returning [ResultSpec]s. This
///   overrides the default function-handling logic entirely.
/// - Metadata on whether / not this function is pushdownable. See [Trace].
struct SpecialUnary {
    map_fn: for<'a, 'b> fn(&'b ColumnSpecs<'a>, ResultSpec<'a>) -> ResultSpec<'a>,
    pushdownable: bool,
}

impl SpecialUnary {
    /// Returns the special-case handling for a particular function, if it exists.
    fn for_func(func: &UnaryFunc) -> Option<SpecialUnary> {
        /// Eager in the same sense as `func.rs` uses the term; this assumes that
        /// nulls and errors propagate up, and we only need to define the behaviour
        /// on values.
        fn eagerly<'b>(
            spec: ResultSpec<'b>,
            value_fn: impl FnOnce(Values<'b>) -> ResultSpec<'b>,
        ) -> ResultSpec<'b> {
            let result = match spec.values {
                Values::Empty => ResultSpec::nothing(),
                other => value_fn(other),
            };
            ResultSpec {
                fallible: spec.fallible || result.fallible,
                nullable: spec.nullable || result.nullable,
                values: result.values,
            }
        }
        match func {
            UnaryFunc::TryParseMonotonicIso8601Timestamp(_) => Some(SpecialUnary {
                map_fn: |specs, range| {
                    let expr = MirScalarExpr::CallUnary {
                        func: UnaryFunc::TryParseMonotonicIso8601Timestamp(
                            crate::func::TryParseMonotonicIso8601Timestamp,
                        ),
                        expr: Box::new(MirScalarExpr::column(0)),
                    };
                    let eval = |d| specs.eval_result(expr.eval(&[d], specs.arena));

                    eagerly(range, |values| {
                        match values {
                            Values::Within(a, b) if a == b => eval(a),
                            Values::Within(a, b) => {
                                let spec = eval(a).union(eval(b));
                                let values_spec = if spec.nullable {
                                    // At least one of the endpoints of the range wasn't a valid
                                    // timestamp. We can't compute a precise range in this case.
                                    // If we used the general is_monotone handling, that code would
                                    // incorrectly assume the whole range mapped to null if each
                                    // endpoint did.
                                    ResultSpec::value_all()
                                } else {
                                    spec
                                };
                                // A range of strings will always contain strings that don't parse
                                // as timestamps - so unlike the general case, we'll assume null
                                // is present in every range of output values.
                                values_spec.union(ResultSpec::null())
                            }
                            // Otherwise, assume the worst: this function may return either a valid
                            // value or null.
                            _ => ResultSpec::any_infallible(),
                        }
                    })
                },
                pushdownable: true,
            }),
            _ => None,
        }
    }
}

/// A binary function we've added special-case handling for; including:
/// - A two-argument function, taking and returning [ResultSpec]s. This overrides the
///   default function-handling logic entirely.
/// - Metadata on whether / not this function is pushdownable. See [Trace].
struct SpecialBinary {
    map_fn: for<'a> fn(ResultSpec<'a>, ResultSpec<'a>) -> ResultSpec<'a>,
    pushdownable: (bool, bool),
}

impl SpecialBinary {
    /// Returns the special-case handling for a particular function, if it exists.
    fn for_func(func: &BinaryFunc) -> Option<SpecialBinary> {
        /// Eager in the same sense as `func.rs` uses the term; this assumes that
        /// nulls and errors propagate up, and we only need to define the behaviour
        /// on values.
        fn eagerly<'b>(
            left: ResultSpec<'b>,
            right: ResultSpec<'b>,
            value_fn: impl FnOnce(Values<'b>, Values<'b>) -> ResultSpec<'b>,
        ) -> ResultSpec<'b> {
            let result = match (left.values, right.values) {
                (Values::Empty, _) | (_, Values::Empty) => ResultSpec::nothing(),
                (l, r) => value_fn(l, r),
            };
            ResultSpec {
                fallible: left.fallible || right.fallible || result.fallible,
                nullable: left.nullable || right.nullable || result.nullable,
                values: result.values,
            }
        }

        fn jsonb_get_string<'b>(
            left: ResultSpec<'b>,
            right: ResultSpec<'b>,
            stringify: bool,
        ) -> ResultSpec<'b> {
            eagerly(left, right, |left, right| {
                let nested_spec = match (left, right) {
                    (Values::Nested(mut map_spec), Values::Within(key, key2)) if key == key2 => {
                        map_spec.remove(&key)
                    }
                    _ => None,
                };

                if let Some(field_spec) = nested_spec {
                    if stringify {
                        // We only preserve value-range information when stringification
                        // is a noop. (Common in real queries.)
                        let values = match field_spec.values {
                            Values::Empty => Values::Empty,
                            Values::Within(min @ Datum::String(_), max @ Datum::String(_)) => {
                                Values::Within(min, max)
                            }
                            Values::Within(_, _) | Values::Nested(_) | Values::All => Values::All,
                        };
                        ResultSpec {
                            values,
                            ..field_spec
                        }
                    } else {
                        field_spec
                    }
                } else {
                    // The implementation of `jsonb_get_string` always returns
                    // `Ok(...)`. Morally, everything has a string
                    // representation, and the worst you can get is a NULL,
                    // which maps to a NULL.
                    ResultSpec::any_infallible()
                }
            })
        }

        fn eq<'b>(left: ResultSpec<'b>, right: ResultSpec<'b>) -> ResultSpec<'b> {
            eagerly(left, right, |left, right| {
                // `eq` might return true if there's any overlap between the range of its two arguments...
                let maybe_true = match left.clone().intersect(right.clone()) {
                    Values::Empty => ResultSpec::nothing(),
                    _ => ResultSpec::value(Datum::True),
                };

                // ...and may return false if the union contains at least two distinct values.
                // Note that the `Empty` case is handled by `eagerly` above.
                let maybe_false = match left.union(right) {
                    Values::Within(a, b) if a == b => ResultSpec::nothing(),
                    _ => ResultSpec::value(Datum::False),
                };

                maybe_true.union(maybe_false)
            })
        }

        match func {
            BinaryFunc::JsonbGetString => Some(SpecialBinary {
                map_fn: |l, r| jsonb_get_string(l, r, false),
                pushdownable: (true, false),
            }),
            BinaryFunc::JsonbGetStringStringify => Some(SpecialBinary {
                map_fn: |l, r| jsonb_get_string(l, r, true),
                pushdownable: (true, false),
            }),
            BinaryFunc::Eq => Some(SpecialBinary {
                map_fn: eq,
                pushdownable: (true, true),
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ColumnSpec<'a> {
    pub col_type: SqlColumnType,
    pub range: ResultSpec<'a>,
}

/// An interpreter that:
/// - stores both the type and the range of possible values for every column and
///   unmaterializable function. (See the `push_` methods.)
/// - given an expression (or MFP, etc.), returns the range of possible results that evaluating that
///   expression might have. (See the `eval_` methods.)
#[derive(Clone, Debug)]
pub struct ColumnSpecs<'a> {
    pub relation: &'a SqlRelationType,
    pub columns: Vec<ResultSpec<'a>>,
    pub unmaterializables: BTreeMap<UnmaterializableFunc, ResultSpec<'a>>,
    pub arena: &'a RowArena,
}

impl<'a> ColumnSpecs<'a> {
    // Interpreting a variadic function can lead to exponential blowup: there are up to 4 possibly-
    // interesting values for each argument (error, null, range bounds...) and in the worst case
    // we may need to test every combination. We mitigate that here in two ways:
    // - Adding a linear-time optimization for associative functions like AND, OR, COALESCE.
    // - Limiting the number of arguments we'll pass on to eval. If this limit is crossed, we'll
    //   return our default / safe overapproximation instead.
    const MAX_EVAL_ARGS: usize = 6;

    /// Create a new, empty set of column specs. (Initially, the only assumption we make about the
    /// data in the column is that it matches the type.)
    pub fn new(relation: &'a SqlRelationType, arena: &'a RowArena) -> Self {
        let columns = relation
            .column_types
            .iter()
            .map(|ct| ResultSpec::has_type(ct, false))
            .collect();
        ColumnSpecs {
            relation,
            columns,
            unmaterializables: Default::default(),
            arena,
        }
    }

    /// Restrict the set of possible values in a given column. (By intersecting it with the existing
    /// spec.)
    pub fn push_column(&mut self, id: usize, update: ResultSpec<'a>) {
        let range = self.columns.get_mut(id).expect("valid column id");
        *range = range.clone().intersect(update);
    }

    /// Restrict the set of possible values a given unmaterializable func might return. (By
    /// intersecting it with the existing spec.)
    pub fn push_unmaterializable(&mut self, func: UnmaterializableFunc, update: ResultSpec<'a>) {
        let range = self
            .unmaterializables
            .entry(func.clone())
            .or_insert_with(|| ResultSpec::has_type(&func.output_type(), true));
        *range = range.clone().intersect(update);
    }

    fn eval_result<'b, E>(&self, result: Result<Datum<'b>, E>) -> ResultSpec<'a> {
        match result {
            Ok(Datum::Null) => ResultSpec {
                nullable: true,
                ..ResultSpec::nothing()
            },
            Ok(d) => ResultSpec {
                values: Values::just(self.arena.make_datum(|packer| packer.push(d))),
                ..ResultSpec::nothing()
            },
            Err(_) => ResultSpec {
                fallible: true,
                ..ResultSpec::nothing()
            },
        }
    }

    fn set_literal(expr: &mut MirScalarExpr, update: Result<Datum, EvalError>) {
        match expr {
            MirScalarExpr::Literal(literal, col_type) => match update {
                Err(error) => *literal = Err(error),
                Ok(datum) => {
                    assert!(
                        datum.is_instance_of_sql(col_type),
                        "{datum:?} must be an instance of {col_type:?}"
                    );
                    match literal {
                        // Reuse the allocation if we can
                        Ok(row) => row.packer().push(datum),
                        literal => *literal = Ok(Row::pack_slice(&[datum])),
                    }
                }
            },
            _ => panic!("not a literal"),
        }
    }

    fn set_argument(expr: &mut MirScalarExpr, arg: usize, value: Result<Datum, EvalError>) {
        match (expr, arg) {
            (MirScalarExpr::CallUnary { expr, .. }, 0) => Self::set_literal(expr, value),
            (MirScalarExpr::CallBinary { expr1, .. }, 0) => Self::set_literal(expr1, value),
            (MirScalarExpr::CallBinary { expr2, .. }, 1) => Self::set_literal(expr2, value),
            (MirScalarExpr::CallVariadic { exprs, .. }, n) if n < exprs.len() => {
                Self::set_literal(&mut exprs[n], value)
            }
            _ => panic!("illegal argument for expression"),
        }
    }

    /// A literal with the given type and a trivial default value. Callers should ensure that
    /// [Self::set_literal] is called on the resulting expression to give it a meaningful value
    /// before evaluating.
    fn placeholder(col_type: SqlColumnType) -> MirScalarExpr {
        MirScalarExpr::Literal(Err(EvalError::Internal("".into())), col_type)
    }
}

impl<'a> Interpreter for ColumnSpecs<'a> {
    type Summary = ColumnSpec<'a>;

    fn column(&self, id: usize) -> Self::Summary {
        let col_type = self.relation.column_types[id].clone();
        let range = self.columns[id].clone();
        ColumnSpec { col_type, range }
    }

    fn literal(&self, result: &Result<Row, EvalError>, col_type: &SqlColumnType) -> Self::Summary {
        let col_type = col_type.clone();
        let range = self.eval_result(result.as_ref().map(|row| {
            self.arena
                .make_datum(|packer| packer.push(row.unpack_first()))
        }));
        ColumnSpec { col_type, range }
    }

    fn unmaterializable(&self, func: &UnmaterializableFunc) -> Self::Summary {
        let col_type = func.output_type();
        let range = self
            .unmaterializables
            .get(func)
            .cloned()
            .unwrap_or_else(|| ResultSpec::has_type(&func.output_type(), true));
        ColumnSpec { col_type, range }
    }

    fn unary(&self, func: &UnaryFunc, summary: Self::Summary) -> Self::Summary {
        let fallible = func.could_error() || summary.range.fallible;
        let mapped_spec = if let Some(special) = SpecialUnary::for_func(func) {
            (special.map_fn)(self, summary.range)
        } else {
            let is_monotone = func.is_monotone();
            let mut expr = MirScalarExpr::CallUnary {
                func: func.clone(),
                expr: Box::new(Self::placeholder(summary.col_type.clone())),
            };
            summary.range.flat_map(is_monotone, |datum| {
                Self::set_argument(&mut expr, 0, datum);
                self.eval_result(expr.eval(&[], self.arena))
            })
        };

        let col_type = func.output_type(summary.col_type);

        let range = mapped_spec.intersect(ResultSpec::has_type(&col_type, fallible));
        ColumnSpec { col_type, range }
    }

    fn binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary {
        let (left_monotonic, right_monotonic) = func.is_monotone();
        let fallible = func.could_error() || left.range.fallible || right.range.fallible;

        let mapped_spec = if let Some(special) = SpecialBinary::for_func(func) {
            (special.map_fn)(left.range, right.range)
        } else {
            let mut expr = MirScalarExpr::CallBinary {
                func: func.clone(),
                expr1: Box::new(Self::placeholder(left.col_type.clone())),
                expr2: Box::new(Self::placeholder(right.col_type.clone())),
            };
            left.range.flat_map(left_monotonic, |left_result| {
                Self::set_argument(&mut expr, 0, left_result);
                right.range.flat_map(right_monotonic, |right_result| {
                    Self::set_argument(&mut expr, 1, right_result);
                    self.eval_result(expr.eval(&[], self.arena))
                })
            })
        };

        let col_type = func.output_type(left.col_type, right.col_type);

        let range = mapped_spec.intersect(ResultSpec::has_type(&col_type, fallible));
        ColumnSpec { col_type, range }
    }

    fn variadic(&self, func: &VariadicFunc, args: Vec<Self::Summary>) -> Self::Summary {
        let fallible = func.could_error() || args.iter().any(|s| s.range.fallible);
        if func.is_associative() && args.len() > 2 {
            // To avoid a combinatorial explosion, evaluate large variadic calls as a series of
            // smaller ones, since associativity guarantees we'll get compatible results.
            return args
                .into_iter()
                .reduce(|a, b| self.variadic(func, vec![a, b]))
                .expect("reducing over a non-empty argument list");
        }

        let mapped_spec = if args.len() >= Self::MAX_EVAL_ARGS {
            ResultSpec::anything()
        } else {
            fn eval_loop<'a>(
                is_monotonic: bool,
                expr: &mut MirScalarExpr,
                args: &[ColumnSpec<'a>],
                index: usize,
                datum_map: &mut impl FnMut(&MirScalarExpr) -> ResultSpec<'a>,
            ) -> ResultSpec<'a> {
                if index >= args.len() {
                    datum_map(expr)
                } else {
                    args[index].range.flat_map(is_monotonic, |datum| {
                        ColumnSpecs::set_argument(expr, index, datum);
                        eval_loop(is_monotonic, expr, args, index + 1, datum_map)
                    })
                }
            }

            let mut fn_expr = MirScalarExpr::CallVariadic {
                func: func.clone(),
                exprs: args
                    .iter()
                    .map(|spec| Self::placeholder(spec.col_type.clone()))
                    .collect(),
            };
            eval_loop(func.is_monotone(), &mut fn_expr, &args, 0, &mut |expr| {
                self.eval_result(expr.eval(&[], self.arena))
            })
        };

        let col_types = args.into_iter().map(|spec| spec.col_type).collect();
        let col_type = func.output_type(col_types);

        let range = mapped_spec.intersect(ResultSpec::has_type(&col_type, fallible));

        ColumnSpec { col_type, range }
    }

    fn cond(&self, cond: Self::Summary, then: Self::Summary, els: Self::Summary) -> Self::Summary {
        let col_type = SqlColumnType {
            scalar_type: then.col_type.scalar_type,
            nullable: then.col_type.nullable || els.col_type.nullable,
        };

        let range = cond
            .range
            .flat_map(true, |datum| match datum {
                Ok(Datum::True) => then.range.clone(),
                Ok(Datum::False) => els.range.clone(),
                _ => ResultSpec::fails(),
            })
            .intersect(ResultSpec::has_type(&col_type, true));

        ColumnSpec { col_type, range }
    }
}

/// An interpreter that returns whether or not a particular expression is "pushdownable".
/// Broadly speaking, an expression is pushdownable if the result of evaluating the expression
/// depends on the range of possible column values in a way that `ColumnSpecs` is able to reason about.
///
/// In practice, we internally need to distinguish between expressions that are trivially predicable
/// (because they're constant) and expressions that depend on the column ranges themselves.
/// See the [TraceSummary] variants for those distinctions, and [TraceSummary::pushdownable] for
/// the overall assessment.
#[derive(Debug)]
pub struct Trace;

/// A summary type for the [Trace] interpreter.
///
/// The ordering of this type is meaningful: the "smaller" the summary, the more information we have
/// about the possible values of the expression. This means we can eg. use `max` in the
/// interpreter below to find the summary for a function-call expression based on the summaries
/// of its arguments.
#[derive(Copy, Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum TraceSummary {
    /// The expression is constant: we can evaluate it without any runtime information.
    /// This corresponds to a `ResultSpec` of a single value.
    Constant,
    /// The expression depends on runtime information, but in "predictable" way... ie. if we know
    /// the range of possible values for all columns and unmaterializable functions, we can
    /// predict the possible values of the output.
    /// This corresponds to a `ResultSpec` of a perhaps range of values.
    Dynamic,
    /// The expression depends on runtime information in an unpredictable way.
    /// This corresponds to a `ResultSpec::value_all()` or something similarly vague.
    Unknown,
}

impl TraceSummary {
    /// We say that a function is "pushdownable" for a particular
    /// argument if `ColumnSpecs` can determine the spec of the function's output given the input spec for
    /// that argument. (In practice, this is true when either the function is monotone in that argument
    /// or it's been special-cased in the interpreter.)
    fn apply_fn(self, pushdownable: bool) -> Self {
        match self {
            TraceSummary::Constant => TraceSummary::Constant,
            TraceSummary::Dynamic => match pushdownable {
                true => TraceSummary::Dynamic,
                false => TraceSummary::Unknown,
            },
            TraceSummary::Unknown => TraceSummary::Unknown,
        }
    }

    /// We say that an expression is "pushdownable" if it's either constant or dynamic.
    pub fn pushdownable(self) -> bool {
        match self {
            TraceSummary::Constant | TraceSummary::Dynamic => true,
            TraceSummary::Unknown => false,
        }
    }
}

impl Interpreter for Trace {
    type Summary = TraceSummary;

    fn column(&self, _id: usize) -> Self::Summary {
        TraceSummary::Dynamic
    }

    fn literal(
        &self,
        _result: &Result<Row, EvalError>,
        _col_type: &SqlColumnType,
    ) -> Self::Summary {
        TraceSummary::Constant
    }

    fn unmaterializable(&self, _func: &UnmaterializableFunc) -> Self::Summary {
        TraceSummary::Dynamic
    }

    fn unary(&self, func: &UnaryFunc, expr: Self::Summary) -> Self::Summary {
        let pushdownable = match SpecialUnary::for_func(func) {
            None => func.is_monotone(),
            Some(special) => special.pushdownable,
        };
        expr.apply_fn(pushdownable)
    }

    fn binary(
        &self,
        func: &BinaryFunc,
        left: Self::Summary,
        right: Self::Summary,
    ) -> Self::Summary {
        let (left_pushdownable, right_pushdownable) = match SpecialBinary::for_func(func) {
            None => func.is_monotone(),
            Some(special) => special.pushdownable,
        };
        left.apply_fn(left_pushdownable)
            .max(right.apply_fn(right_pushdownable))
    }

    fn variadic(&self, func: &VariadicFunc, exprs: Vec<Self::Summary>) -> Self::Summary {
        if !func.is_associative() && exprs.len() >= ColumnSpecs::MAX_EVAL_ARGS {
            // We can't efficiently evaluate functions with very large argument lists;
            // see the comment on ColumnSpecs::MAX_EVAL_ARGS for details.
            return TraceSummary::Unknown;
        }

        let pushdownable_fn = func.is_monotone();
        exprs
            .into_iter()
            .map(|pushdownable_arg| pushdownable_arg.apply_fn(pushdownable_fn))
            .max()
            .unwrap_or(TraceSummary::Constant)
    }

    fn cond(&self, cond: Self::Summary, then: Self::Summary, els: Self::Summary) -> Self::Summary {
        // We don't actually need to be able to predict the condition precisely to predict the output,
        // since we can union the ranges of the two branches for a conservative estimate.
        let cond = cond.min(TraceSummary::Dynamic);
        cond.max(then).max(els)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use mz_repr::adt::datetime::DateTimeUnits;
    use mz_repr::{Datum, PropDatum, RowArena, SqlScalarType};
    use proptest::prelude::*;
    use proptest::sample::{Index, select};

    use crate::func::*;
    use crate::{BinaryFunc, MirScalarExpr, UnaryFunc};

    use super::*;

    #[derive(Debug)]
    struct ExpressionData {
        relation_type: SqlRelationType,
        specs: Vec<ResultSpec<'static>>,
        rows: Vec<Row>,
        expr: MirScalarExpr,
    }

    // Currently there's no good way to check whether a particular function accepts a particular
    // type as argument, which means we need to list everything out explicitly here. Restrict our interest
    // to a reasonable number of functions, to keep things tractable
    // TODO: replace this with function-level info once it's available.
    const NUM_TYPE: SqlScalarType = SqlScalarType::Numeric { max_scale: None };
    static SCALAR_TYPES: &[SqlScalarType] = &[
        SqlScalarType::Bool,
        SqlScalarType::Jsonb,
        NUM_TYPE,
        SqlScalarType::Date,
        SqlScalarType::Timestamp { precision: None },
        SqlScalarType::MzTimestamp,
        SqlScalarType::String,
    ];

    const INTERESTING_UNARY_FUNCS: &[UnaryFunc] = {
        &[
            UnaryFunc::CastNumericToMzTimestamp(CastNumericToMzTimestamp),
            UnaryFunc::NegNumeric(NegNumeric),
            UnaryFunc::CastJsonbToNumeric(CastJsonbToNumeric(None)),
            UnaryFunc::CastJsonbToBool(CastJsonbToBool),
            UnaryFunc::CastJsonbToString(CastJsonbToString),
            UnaryFunc::DateTruncTimestamp(DateTruncTimestamp(DateTimeUnits::Epoch)),
            UnaryFunc::ExtractTimestamp(ExtractTimestamp(DateTimeUnits::Epoch)),
            UnaryFunc::ExtractDate(ExtractDate(DateTimeUnits::Epoch)),
            UnaryFunc::Not(Not),
            UnaryFunc::IsNull(IsNull),
            UnaryFunc::IsFalse(IsFalse),
            UnaryFunc::TryParseMonotonicIso8601Timestamp(TryParseMonotonicIso8601Timestamp),
        ]
    };

    fn unary_typecheck(func: &UnaryFunc, arg: &SqlColumnType) -> bool {
        use UnaryFunc::*;
        match func {
            CastNumericToMzTimestamp(_) | NegNumeric(_) => arg.scalar_type.base_eq(&NUM_TYPE),
            CastJsonbToNumeric(_) | CastJsonbToBool(_) | CastJsonbToString(_) => {
                arg.scalar_type.base_eq(&SqlScalarType::Jsonb)
            }
            ExtractTimestamp(_) | DateTruncTimestamp(_) => arg
                .scalar_type
                .base_eq(&SqlScalarType::Timestamp { precision: None }),
            ExtractDate(_) => arg.scalar_type.base_eq(&SqlScalarType::Date),
            Not(_) => arg.scalar_type.base_eq(&SqlScalarType::Bool),
            IsNull(_) => true,
            TryParseMonotonicIso8601Timestamp(_) => arg.scalar_type.base_eq(&SqlScalarType::String),
            _ => false,
        }
    }

    const INTERESTING_BINARY_FUNCS: &[BinaryFunc] = {
        use BinaryFunc::*;
        &[
            AddTimestampInterval,
            AddNumeric,
            SubNumeric,
            MulNumeric,
            DivNumeric,
            Eq,
            Lt,
            Gt,
            Lte,
            Gte,
            DateTruncTimestamp,
            JsonbGetString,
            JsonbGetStringStringify,
        ]
    };

    fn binary_typecheck(func: &BinaryFunc, arg0: &SqlColumnType, arg1: &SqlColumnType) -> bool {
        use BinaryFunc::*;
        match func {
            AddTimestampInterval => {
                arg0.scalar_type
                    .base_eq(&SqlScalarType::Timestamp { precision: None })
                    && arg1.scalar_type.base_eq(&SqlScalarType::Interval)
            }
            AddNumeric | SubNumeric | MulNumeric | DivNumeric => {
                arg0.scalar_type.base_eq(&NUM_TYPE) && arg1.scalar_type.base_eq(&NUM_TYPE)
            }
            Eq | Lt | Gt | Lte | Gte => arg0.scalar_type.base_eq(&arg1.scalar_type),
            DateTruncTimestamp => {
                arg0.scalar_type.base_eq(&SqlScalarType::String)
                    && arg1
                        .scalar_type
                        .base_eq(&SqlScalarType::Timestamp { precision: None })
            }
            JsonbGetString | JsonbGetStringStringify => {
                arg0.scalar_type.base_eq(&SqlScalarType::Jsonb)
                    && arg1.scalar_type.base_eq(&SqlScalarType::String)
            }
            _ => false,
        }
    }

    const INTERESTING_VARIADIC_FUNCS: &[VariadicFunc] = {
        use VariadicFunc::*;
        &[Coalesce, Greatest, Least, And, Or, Concat, ConcatWs]
    };

    fn variadic_typecheck(func: &VariadicFunc, args: &[SqlColumnType]) -> bool {
        use VariadicFunc::*;
        fn all_eq<'a>(
            iter: impl IntoIterator<Item = &'a SqlColumnType>,
            other: &SqlScalarType,
        ) -> bool {
            iter.into_iter().all(|t| t.scalar_type.base_eq(other))
        }
        match func {
            Coalesce | Greatest | Least => match args {
                [] => true,
                [first, rest @ ..] => all_eq(rest, &first.scalar_type),
            },
            And | Or => all_eq(args, &SqlScalarType::Bool),
            Concat => all_eq(args, &SqlScalarType::String),
            ConcatWs => args.len() > 1 && all_eq(args, &SqlScalarType::String),
            _ => false,
        }
    }

    fn gen_datums_for_type(typ: &SqlColumnType) -> BoxedStrategy<Datum<'static>> {
        let mut values: Vec<Datum<'static>> = typ.scalar_type.interesting_datums().collect();
        if typ.nullable {
            values.push(Datum::Null)
        }
        select(values).boxed()
    }

    fn gen_column() -> impl Strategy<Value = (SqlColumnType, Datum<'static>, ResultSpec<'static>)> {
        let col_type = (select(SCALAR_TYPES), any::<bool>())
            .prop_map(|(t, b)| t.nullable(b))
            .prop_filter("need at least one value", |c| {
                c.scalar_type.interesting_datums().count() > 0
            });

        let result_spec = select(vec![
            ResultSpec::nothing(),
            ResultSpec::null(),
            ResultSpec::anything(),
            ResultSpec::value_all(),
        ]);

        (col_type, result_spec).prop_flat_map(|(col, result_spec)| {
            gen_datums_for_type(&col).prop_map(move |datum| {
                let result_spec = result_spec.clone().union(ResultSpec::value(datum));
                (col.clone(), datum, result_spec)
            })
        })
    }

    fn gen_expr_for_relation(
        relation: &SqlRelationType,
    ) -> BoxedStrategy<(MirScalarExpr, SqlColumnType)> {
        let column_gen = {
            let column_types = relation.column_types.clone();
            any::<Index>()
                .prop_map(move |idx| {
                    let id = idx.index(column_types.len());
                    (MirScalarExpr::column(id), column_types[id].clone())
                })
                .boxed()
        };

        let literal_gen = (select(SCALAR_TYPES), any::<bool>())
            .prop_map(|(s, b)| s.nullable(b))
            .prop_flat_map(|ct| {
                let error_gen = any::<EvalError>().prop_map(Err).boxed();
                let value_gen = gen_datums_for_type(&ct)
                    .prop_map(move |datum| Ok(Row::pack_slice(&[datum])))
                    .boxed();
                error_gen.prop_union(value_gen).prop_map(move |result| {
                    (MirScalarExpr::Literal(result, ct.clone()), ct.clone())
                })
            })
            .boxed();

        column_gen
            .prop_union(literal_gen)
            .prop_recursive(4, 64, 8, |self_gen| {
                let unary_gen = (select(INTERESTING_UNARY_FUNCS), self_gen.clone())
                    .prop_filter_map("unary func", |(func, (expr_in, type_in))| {
                        if !unary_typecheck(&func, &type_in) {
                            return None;
                        }
                        let type_out = func.output_type(type_in);
                        let expr_out = MirScalarExpr::CallUnary {
                            func,
                            expr: Box::new(expr_in),
                        };
                        Some((expr_out, type_out))
                    })
                    .boxed();
                let binary_gen = (
                    select(INTERESTING_BINARY_FUNCS),
                    self_gen.clone(),
                    self_gen.clone(),
                )
                    .prop_filter_map(
                        "binary func",
                        |(func, (expr_left, type_left), (expr_right, type_right))| {
                            if !binary_typecheck(&func, &type_left, &type_right) {
                                return None;
                            }
                            let type_out = func.output_type(type_left, type_right);
                            let expr_out = MirScalarExpr::CallBinary {
                                func,
                                expr1: Box::new(expr_left),
                                expr2: Box::new(expr_right),
                            };
                            Some((expr_out, type_out))
                        },
                    )
                    .boxed();
                let variadic_gen = (
                    select(INTERESTING_VARIADIC_FUNCS),
                    prop::collection::vec(self_gen, 1..4),
                )
                    .prop_filter_map("variadic func", |(func, exprs)| {
                        let (exprs_in, type_in): (_, Vec<_>) = exprs.into_iter().unzip();
                        if !variadic_typecheck(&func, &type_in) {
                            return None;
                        }
                        let type_out = func.output_type(type_in);
                        let expr_out = MirScalarExpr::CallVariadic {
                            func,
                            exprs: exprs_in,
                        };
                        Some((expr_out, type_out))
                    })
                    .boxed();

                unary_gen
                    .prop_union(binary_gen)
                    .boxed()
                    .prop_union(variadic_gen)
            })
            .boxed()
    }

    fn gen_expr_data() -> impl Strategy<Value = ExpressionData> {
        let columns = prop::collection::vec(gen_column(), 1..10);
        columns.prop_flat_map(|data| {
            let (columns, datums, specs): (Vec<_>, Vec<_>, Vec<_>) = data.into_iter().multiunzip();
            let relation = SqlRelationType::new(columns);
            let row = Row::pack_slice(&datums);
            gen_expr_for_relation(&relation).prop_map(move |(expr, _)| ExpressionData {
                relation_type: relation.clone(),
                specs: specs.clone(),
                rows: vec![row.clone()],
                expr,
            })
        })
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn test_trivial_spec_matches() {
        fn check(datum: PropDatum) -> Result<(), TestCaseError> {
            let datum: Datum = (&datum).into();
            let spec = if datum.is_null() {
                ResultSpec::null()
            } else {
                ResultSpec::value(datum)
            };
            assert!(spec.may_contain(datum));
            Ok(())
        }

        proptest!(|(datum in mz_repr::arb_datum())| {
            check(datum)?;
        });

        assert!(ResultSpec::fails().may_fail());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
    fn test_equivalence() {
        fn check(data: ExpressionData) -> Result<(), TestCaseError> {
            let ExpressionData {
                relation_type,
                specs,
                rows,
                expr,
            } = data;

            // We want to ensure that the spec we get when evaluating an expression using
            // `ColumnSpecs` always contains the _actual_ value of that column when evaluated with
            // eval. (This is an important correctness property of abstract interpretation.)
            let arena = RowArena::new();
            let mut interpreter = ColumnSpecs::new(&relation_type, &arena);
            for (id, spec) in specs.into_iter().enumerate() {
                interpreter.push_column(id, spec);
            }

            let spec = interpreter.expr(&expr);

            for row in &rows {
                let datums: Vec<_> = row.iter().collect();
                let eval_result = expr.eval(&datums, &arena);
                match eval_result {
                    Ok(value) => {
                        assert!(spec.range.may_contain(value))
                    }
                    Err(_) => {
                        assert!(spec.range.may_fail());
                    }
                }
            }

            Ok(())
        }

        proptest!(|(data in gen_expr_data())| {
            check(data)?;
        });
    }

    #[mz_ore::test]
    fn test_mfp() {
        // Regression test for https://github.com/MaterializeInc/database-issues/issues/5736
        use MirScalarExpr::*;

        let mfp = MapFilterProject {
            expressions: vec![],
            predicates: vec![
                // Always fails on the known input range
                (
                    1,
                    CallUnary {
                        func: UnaryFunc::IsNull(IsNull),
                        expr: Box::new(CallBinary {
                            func: BinaryFunc::MulInt32,
                            expr1: Box::new(MirScalarExpr::column(0)),
                            expr2: Box::new(MirScalarExpr::column(0)),
                        }),
                    },
                ),
                // Always returns false on the known input range
                (
                    1,
                    CallBinary {
                        func: BinaryFunc::Eq,
                        expr1: Box::new(MirScalarExpr::column(0)),
                        expr2: Box::new(Literal(
                            Ok(Row::pack_slice(&[Datum::Int32(1727694505)])),
                            SqlScalarType::Int32.nullable(false),
                        )),
                    },
                ),
            ],
            projection: vec![],
            input_arity: 1,
        };

        let relation = SqlRelationType::new(vec![SqlScalarType::Int32.nullable(true)]);
        let arena = RowArena::new();
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(0, ResultSpec::value(Datum::Int32(-1294725158)));
        let spec = interpreter.mfp_filter(&mfp);
        assert!(spec.range.may_fail());
    }

    #[mz_ore::test]
    fn test_concat() {
        let expr = MirScalarExpr::CallVariadic {
            func: VariadicFunc::Concat,
            exprs: vec![
                MirScalarExpr::column(0),
                MirScalarExpr::literal_ok(Datum::String("a"), SqlScalarType::String),
                MirScalarExpr::literal_ok(Datum::String("b"), SqlScalarType::String),
            ],
        };

        let relation = SqlRelationType::new(vec![SqlScalarType::String.nullable(false)]);
        let arena = RowArena::new();
        let interpreter = ColumnSpecs::new(&relation, &arena);
        let spec = interpreter.expr(&expr);
        assert!(spec.range.may_contain(Datum::String("blab")));
    }

    #[mz_ore::test]
    fn test_eval_range() {
        // Example inspired by the tumbling windows temporal filter in the docs
        let period_ms = MirScalarExpr::Literal(
            Ok(Row::pack_slice(&[Datum::Int64(10)])),
            SqlScalarType::Int64.nullable(false),
        );
        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::Gte,
            expr1: Box::new(MirScalarExpr::CallUnmaterializable(
                UnmaterializableFunc::MzNow,
            )),
            expr2: Box::new(MirScalarExpr::CallUnary {
                func: UnaryFunc::CastInt64ToMzTimestamp(CastInt64ToMzTimestamp),
                expr: Box::new(MirScalarExpr::CallBinary {
                    func: BinaryFunc::MulInt64,
                    expr1: Box::new(period_ms.clone()),
                    expr2: Box::new(MirScalarExpr::CallBinary {
                        func: BinaryFunc::DivInt64,
                        expr1: Box::new(MirScalarExpr::column(0)),
                        expr2: Box::new(period_ms),
                    }),
                }),
            }),
        };
        let relation = SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false)]);

        {
            // Non-overlapping windows
            let arena = RowArena::new();
            let mut interpreter = ColumnSpecs::new(&relation, &arena);
            interpreter.push_unmaterializable(
                UnmaterializableFunc::MzNow,
                ResultSpec::value_between(
                    Datum::MzTimestamp(10.into()),
                    Datum::MzTimestamp(20.into()),
                ),
            );
            interpreter.push_column(0, ResultSpec::value_between(30i64.into(), 40i64.into()));

            let range_out = interpreter.expr(&expr).range;
            assert!(range_out.may_contain(Datum::False));
            assert!(!range_out.may_contain(Datum::True));
            assert!(!range_out.may_contain(Datum::Null));
            assert!(!range_out.may_fail());
        }

        {
            // Overlapping windows
            let arena = RowArena::new();
            let mut interpreter = ColumnSpecs::new(&relation, &arena);
            interpreter.push_unmaterializable(
                UnmaterializableFunc::MzNow,
                ResultSpec::value_between(
                    Datum::MzTimestamp(10.into()),
                    Datum::MzTimestamp(35.into()),
                ),
            );
            interpreter.push_column(0, ResultSpec::value_between(30i64.into(), 40i64.into()));

            let range_out = interpreter.expr(&expr).range;
            assert!(range_out.may_contain(Datum::False));
            assert!(range_out.may_contain(Datum::True));
            assert!(!range_out.may_contain(Datum::Null));
            assert!(!range_out.may_fail());
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_jsonb() {
        let arena = RowArena::new();

        let expr = MirScalarExpr::CallUnary {
            func: UnaryFunc::CastJsonbToNumeric(CastJsonbToNumeric(None)),
            expr: Box::new(MirScalarExpr::CallBinary {
                func: BinaryFunc::JsonbGetString,
                expr1: Box::new(MirScalarExpr::column(0)),
                expr2: Box::new(MirScalarExpr::Literal(
                    Ok(Row::pack_slice(&["ts".into()])),
                    SqlScalarType::String.nullable(false),
                )),
            }),
        };

        let relation = SqlRelationType::new(vec![SqlScalarType::Jsonb.nullable(true)]);
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::map_spec(
                [(
                    "ts".into(),
                    ResultSpec::value_between(
                        Datum::Numeric(100.into()),
                        Datum::Numeric(300.into()),
                    ),
                )]
                .into_iter()
                .collect(),
            ),
        );

        let range_out = interpreter.expr(&expr).range;
        assert!(!range_out.may_contain(Datum::Numeric(0.into())));
        assert!(range_out.may_contain(Datum::Numeric(200.into())));
        assert!(!range_out.may_contain(Datum::Numeric(400.into())));
    }

    #[mz_ore::test]
    fn test_like() {
        let arena = RowArena::new();

        let expr = MirScalarExpr::CallUnary {
            func: UnaryFunc::IsLikeMatch(IsLikeMatch(
                crate::like_pattern::compile("%whatever%", true).unwrap(),
            )),
            expr: Box::new(MirScalarExpr::column(0)),
        };

        let relation = SqlRelationType::new(vec![SqlScalarType::String.nullable(true)]);
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::value_between(Datum::String("aardvark"), Datum::String("zebra")),
        );

        let range_out = interpreter.expr(&expr).range;
        assert!(
            !range_out.fallible,
            "like function should not error on non-error input"
        );
        assert!(range_out.may_contain(Datum::True));
        assert!(range_out.may_contain(Datum::False));
        assert!(range_out.may_contain(Datum::Null));
    }

    #[mz_ore::test]
    fn test_try_parse_monotonic_iso8601_timestamp() {
        use chrono::NaiveDateTime;

        let arena = RowArena::new();

        let expr = MirScalarExpr::CallUnary {
            func: UnaryFunc::TryParseMonotonicIso8601Timestamp(TryParseMonotonicIso8601Timestamp),
            expr: Box::new(MirScalarExpr::column(0)),
        };

        let relation = SqlRelationType::new(vec![SqlScalarType::String.nullable(true)]);
        // Test the case where we have full timestamps as bounds.
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::value_between(
                Datum::String("2024-01-11T00:00:00.000Z"),
                Datum::String("2024-01-11T20:00:00.000Z"),
            ),
        );

        let timestamp = |ts| {
            Datum::Timestamp(
                NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S")
                    .unwrap()
                    .try_into()
                    .unwrap(),
            )
        };

        let range_out = interpreter.expr(&expr).range;
        assert!(!range_out.fallible);
        assert!(range_out.nullable);
        assert!(!range_out.may_contain(timestamp("2024-01-10T10:00:00")));
        assert!(range_out.may_contain(timestamp("2024-01-11T10:00:00")));
        assert!(!range_out.may_contain(timestamp("2024-01-12T10:00:00")));

        // Test the case where we have truncated / useless bounds.
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::value_between(Datum::String("2024-01-1"), Datum::String("2024-01-2")),
        );

        let range_out = interpreter.expr(&expr).range;
        assert!(!range_out.fallible);
        assert!(range_out.nullable);
        assert!(range_out.may_contain(timestamp("2024-01-10T10:00:00")));
        assert!(range_out.may_contain(timestamp("2024-01-11T10:00:00")));
        assert!(range_out.may_contain(timestamp("2024-01-12T10:00:00")));

        // Test the case where only one bound is truncated
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::value_between(
                Datum::String("2024-01-1"),
                Datum::String("2024-01-12T10:00:00"),
            )
            .union(ResultSpec::null()),
        );

        let range_out = interpreter.expr(&expr).range;
        assert!(!range_out.fallible);
        assert!(range_out.nullable);
        assert!(range_out.may_contain(timestamp("2024-01-10T10:00:00")));
        assert!(range_out.may_contain(timestamp("2024-01-11T10:00:00")));
        assert!(range_out.may_contain(timestamp("2024-01-12T10:00:00")));

        // Test the case where the upper and lower bound are identical
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::value_between(
                Datum::String("2024-01-11T10:00:00.000Z"),
                Datum::String("2024-01-11T10:00:00.000Z"),
            ),
        );

        let range_out = interpreter.expr(&expr).range;
        assert!(!range_out.fallible);
        assert!(!range_out.nullable);
        assert!(!range_out.may_contain(timestamp("2024-01-10T10:00:00")));
        assert!(range_out.may_contain(timestamp("2024-01-11T10:00:00")));
        assert!(!range_out.may_contain(timestamp("2024-01-12T10:00:00")));
    }

    #[mz_ore::test]
    fn test_inequality() {
        let arena = RowArena::new();

        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::Gte,
            expr1: Box::new(MirScalarExpr::column(0)),
            expr2: Box::new(MirScalarExpr::CallUnmaterializable(
                UnmaterializableFunc::MzNow,
            )),
        };

        let relation = SqlRelationType::new(vec![SqlScalarType::MzTimestamp.nullable(true)]);
        let mut interpreter = ColumnSpecs::new(&relation, &arena);
        interpreter.push_column(
            0,
            ResultSpec::value_between(
                Datum::MzTimestamp(1704736444949u64.into()),
                Datum::MzTimestamp(1704736444949u64.into()),
            )
            .union(ResultSpec::null()),
        );
        interpreter.push_unmaterializable(
            UnmaterializableFunc::MzNow,
            ResultSpec::value_between(
                Datum::MzTimestamp(1704738791000u64.into()),
                Datum::MzTimestamp(18446744073709551615u64.into()),
            ),
        );

        let range_out = interpreter.expr(&expr).range;
        assert!(
            !range_out.fallible,
            "<= function should not error on non-error input"
        );
        assert!(!range_out.may_contain(Datum::True));
        assert!(range_out.may_contain(Datum::False));
        assert!(range_out.may_contain(Datum::Null));
    }

    #[mz_ore::test]
    fn test_trace() {
        use super::Trace;

        let expr = MirScalarExpr::CallBinary {
            func: BinaryFunc::Gte,
            expr1: Box::new(MirScalarExpr::column(0)),
            expr2: Box::new(MirScalarExpr::CallBinary {
                func: BinaryFunc::AddInt64(AddInt64),
                expr1: Box::new(MirScalarExpr::column(1)),
                expr2: Box::new(MirScalarExpr::CallUnary {
                    func: UnaryFunc::NegInt64(NegInt64),
                    expr: Box::new(MirScalarExpr::column(3)),
                }),
            }),
        };
        let summary = Trace.expr(&expr);
        assert!(summary.pushdownable());
    }
}
