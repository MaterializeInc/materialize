// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::fmt;
use std::mem;

use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_ore::collections::CollectionExt;
use mz_ore::str::separated;
use mz_pgrepr::TypeFromOidError;
use mz_repr::adt::array::InvalidArrayError;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::regex::Regex;
use mz_repr::proto::{ProtoRepr, TryFromProtoError, TryIntoIfSome};
use mz_repr::strconv::{ParseError, ParseHexError};
use mz_repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};

use self::func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use crate::scalar::func::parse_timezone;
use crate::visit::{Visit, VisitChildren};

pub mod func;
pub mod like_pattern;

include!(concat!(env!("OUT_DIR"), "/mz_expr.scalar.rs"));

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect)]
pub enum MirScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Result<Row, EvalError>, ColumnType),
    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `MirScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    CallUnmaterializable(UnmaterializableFunc),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: UnaryFunc,
        expr: Box<MirScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<MirScalarExpr>,
        expr2: Box<MirScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<MirScalarExpr>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        cond: Box<MirScalarExpr>,
        then: Box<MirScalarExpr>,
        els: Box<MirScalarExpr>,
    },
}

impl MirScalarExpr {
    pub fn columns(is: &[usize]) -> Vec<MirScalarExpr> {
        is.iter().map(|i| MirScalarExpr::Column(*i)).collect()
    }

    pub fn column(column: usize) -> Self {
        MirScalarExpr::Column(column)
    }

    pub fn literal(res: Result<Datum, EvalError>, typ: ScalarType) -> Self {
        let typ = typ.nullable(matches!(res, Ok(Datum::Null)));
        let row = res.map(|datum| Row::pack_slice(&[datum]));
        MirScalarExpr::Literal(row, typ)
    }

    pub fn literal_ok(datum: Datum, typ: ScalarType) -> Self {
        MirScalarExpr::literal(Ok(datum), typ)
    }

    pub fn literal_null(typ: ScalarType) -> Self {
        MirScalarExpr::literal_ok(Datum::Null, typ)
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        MirScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        MirScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn if_then_else(self, t: Self, f: Self) -> Self {
        MirScalarExpr::If {
            cond: Box::new(self),
            then: Box::new(t),
            els: Box::new(f),
        }
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        self.visit_mut_post(&mut |e| {
            if let MirScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
        });
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute_map(&mut self, permutation: &std::collections::HashMap<usize, usize>) {
        self.visit_mut_post(&mut |e| {
            if let MirScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit_post(&mut |e| {
            if let MirScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn take(&mut self) -> Self {
        mem::replace(self, MirScalarExpr::literal_null(ScalarType::String))
    }

    pub fn as_literal(&self) -> Option<Result<Datum, &EvalError>> {
        if let MirScalarExpr::Literal(lit, _column_type) = self {
            Some(lit.as_ref().map(|row| row.unpack_first()))
        } else {
            None
        }
    }

    pub fn as_literal_str(&self) -> Option<&str> {
        match self.as_literal() {
            Some(Ok(Datum::String(s))) => Some(s),
            _ => None,
        }
    }

    pub fn as_literal_err(&self) -> Option<&EvalError> {
        self.as_literal().and_then(|lit| lit.err())
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, MirScalarExpr::Literal(_, _))
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Ok(Datum::True)) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Ok(Datum::False)) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Ok(Datum::Null)) == self.as_literal()
    }

    pub fn is_literal_ok(&self) -> bool {
        matches!(self, MirScalarExpr::Literal(Ok(_), _typ))
    }

    pub fn is_literal_err(&self) -> bool {
        matches!(self, MirScalarExpr::Literal(Err(_), _typ))
    }

    /// If self is a column, return the column index, otherwise `None`.
    pub fn as_column(&self) -> Option<usize> {
        if let MirScalarExpr::Column(c) = self {
            Some(*c)
        } else {
            None
        }
    }

    /// Reduces a complex expression where possible.
    ///
    /// Also canonicalizes the expression.
    ///
    /// ```rust
    /// use mz_expr::{BinaryFunc, MirScalarExpr};
    /// use mz_repr::{ColumnType, Datum, RelationType, ScalarType};
    ///
    /// let expr_0 = MirScalarExpr::Column(0);
    /// let expr_t = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
    /// let expr_f = MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool);
    ///
    /// let mut test =
    /// expr_t
    ///     .clone()
    ///     .call_binary(expr_f.clone(), BinaryFunc::And)
    ///     .if_then_else(expr_0, expr_t.clone());
    ///
    /// let input_type = RelationType::new(vec![ScalarType::Int32.nullable(false)]);
    /// test.reduce(&input_type);
    /// assert_eq!(test, expr_t);
    /// ```
    pub fn reduce(&mut self, relation_type: &RelationType) {
        let temp_storage = &RowArena::new();
        let eval = |e: &MirScalarExpr| {
            MirScalarExpr::literal(e.eval(&[], temp_storage), e.typ(&relation_type).scalar_type)
        };

        // 1) Simplifications that introduce and propagate constants run in a
        //    loop until `self` no longer changes.
        let mut old_self = MirScalarExpr::column(0);
        while old_self != *self {
            old_self = self.clone();
            self.visit_mut_pre_post(
                &mut |e| {
                    match e {
                        // 1a) Decompose IsNull expressions into a disjunction
                        // of simpler IsNull subexpressions
                        MirScalarExpr::CallUnary { func, expr } => {
                            if *func == UnaryFunc::IsNull(func::IsNull) {
                                if let Some(expr) = expr.decompose_is_null() {
                                    *e = expr
                                }
                            }
                        }
                        _ => {}
                    };
                    None
                },
                &mut |e| match e {
                    // 1b) Evaluate and pull up constants
                    MirScalarExpr::Column(_)
                    | MirScalarExpr::Literal(_, _)
                    | MirScalarExpr::CallUnmaterializable(_) => (),
                    MirScalarExpr::CallUnary { func, expr } => {
                        if expr.is_literal() {
                            *e = eval(e);
                        } else if let UnaryFunc::RecordGet(func::RecordGet(i)) = *func {
                            if let MirScalarExpr::CallVariadic {
                                func: VariadicFunc::RecordCreate { .. },
                                exprs,
                            } = &mut **expr
                            {
                                *e = exprs.swap_remove(i);
                            }
                        }
                    }
                    MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                        if expr1.is_literal() && expr2.is_literal() {
                            *e = eval(e);
                        } else if (expr1.is_literal_null() || expr2.is_literal_null())
                            && func.propagates_nulls()
                        {
                            *e = MirScalarExpr::literal_null(e.typ(relation_type).scalar_type);
                        } else if let Some(err) = expr1.as_literal_err() {
                            *e = MirScalarExpr::literal(
                                Err(err.clone()),
                                e.typ(&relation_type).scalar_type,
                            );
                        } else if let Some(err) = expr2.as_literal_err() {
                            *e = MirScalarExpr::literal(
                                Err(err.clone()),
                                e.typ(&relation_type).scalar_type,
                            );
                        } else if let BinaryFunc::IsLikeMatch { case_insensitive } = func {
                            if expr2.is_literal() {
                                // We can at least precompile the regex.
                                let pattern = expr2.as_literal_str().unwrap();
                                *e = match like_pattern::compile(&pattern, *case_insensitive) {
                                    Ok(matcher) => expr1.take().call_unary(UnaryFunc::IsLikeMatch(
                                        func::IsLikeMatch(matcher),
                                    )),
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(&relation_type).scalar_type,
                                    ),
                                };
                            }
                        } else if let BinaryFunc::IsRegexpMatch { case_insensitive } = func {
                            if let MirScalarExpr::Literal(Ok(row), _) = &**expr2 {
                                let flags = if *case_insensitive { "i" } else { "" };
                                *e = match func::build_regex(row.unpack_first().unwrap_str(), flags)
                                {
                                    Ok(regex) => expr1.take().call_unary(UnaryFunc::IsRegexpMatch(
                                        func::IsRegexpMatch(Regex(regex)),
                                    )),
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(&relation_type).scalar_type,
                                    ),
                                };
                            }
                        } else if *func == BinaryFunc::ExtractInterval && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractInterval(func::ExtractInterval(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractTime && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractTime(func::ExtractTime(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractTimestamp && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractTimestamp(func::ExtractTimestamp(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractTimestampTz && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractTimestampTz(func::ExtractTimestampTz(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::ExtractDate && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::ExtractDate(func::ExtractDate(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartInterval && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartInterval(func::DatePartInterval(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartTime && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartTime(func::DatePartTime(units)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartTimestamp && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartTimestamp(func::DatePartTimestamp(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DatePartTimestampTz && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartTimestampTz(
                                        func::DatePartTimestampTz(units),
                                    ),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DateTruncTimestamp && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DateTruncTimestamp(func::DateTruncTimestamp(
                                        units,
                                    )),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::DateTruncTimestampTz && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DateTruncTimestampTz(
                                        func::DateTruncTimestampTz(units),
                                    ),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(_) => MirScalarExpr::literal(
                                    Err(EvalError::UnknownUnits(units.to_owned())),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::TimezoneTimestamp && expr1.is_literal() {
                            // If the timezone argument is a literal, and we're applying the function on many rows at the same
                            // time we really don't want to parse it again and again, so we parse it once and embed it into the
                            // UnaryFunc enum. The memory footprint of Timezone is small (8 bytes).
                            let tz = expr1.as_literal_str().unwrap();
                            *e = match parse_timezone(tz) {
                                Ok(tz) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::TimezoneTimestamp(func::TimezoneTimestamp(tz)),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(err) => MirScalarExpr::literal(
                                    Err(err),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if *func == BinaryFunc::TimezoneTimestampTz && expr1.is_literal() {
                            let tz = expr1.as_literal_str().unwrap();
                            *e = match parse_timezone(tz) {
                                Ok(tz) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::TimezoneTimestampTz(
                                        func::TimezoneTimestampTz(tz),
                                    ),
                                    expr: Box::new(expr2.take()),
                                },
                                Err(err) => MirScalarExpr::literal(
                                    Err(err),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            }
                        } else if let BinaryFunc::TimezoneTime { wall_time } = func {
                            if expr1.is_literal() {
                                let tz = expr1.as_literal_str().unwrap();
                                *e = match parse_timezone(tz) {
                                    Ok(tz) => MirScalarExpr::CallUnary {
                                        func: UnaryFunc::TimezoneTime(func::TimezoneTime {
                                            tz,
                                            wall_time: *wall_time,
                                        }),
                                        expr: Box::new(expr2.take()),
                                    },
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(&relation_type).scalar_type,
                                    ),
                                }
                            }
                        } else if *func == BinaryFunc::And {
                            // If we are here, not both inputs are literals.
                            if expr1.is_literal_false() || expr2.is_literal_true() {
                                *e = expr1.take();
                            } else if expr2.is_literal_false() || expr1.is_literal_true() {
                                *e = expr2.take();
                            } else if expr1 == expr2 {
                                *e = expr1.take();
                            }
                        } else if *func == BinaryFunc::Or {
                            // If we are here, not both inputs are literals.
                            if expr1.is_literal_true() || expr2.is_literal_false() {
                                *e = expr1.take();
                            } else if expr2.is_literal_true() || expr1.is_literal_false() {
                                *e = expr2.take();
                            } else if expr1 == expr2 {
                                *e = expr1.take();
                            }
                        }
                    }
                    MirScalarExpr::CallVariadic { func, exprs } => {
                        if *func == VariadicFunc::Coalesce {
                            // If all inputs are null, output is null. This check must
                            // be done before `exprs.retain...` because `e.typ` requires
                            // > 0 `exprs` remain.
                            if exprs.iter().all(|expr| expr.is_literal_null()) {
                                *e = MirScalarExpr::literal_null(e.typ(&relation_type).scalar_type);
                                return;
                            }

                            // Remove any null values if not all values are null.
                            exprs.retain(|e| !e.is_literal_null());

                            // Find the first argument that is a literal or non-nullable
                            // column. All arguments after it get ignored, so throw them
                            // away. This intentionally throws away errors that can
                            // never happen.
                            if let Some(i) = exprs
                                .iter()
                                .position(|e| e.is_literal() || !e.typ(&relation_type).nullable)
                            {
                                exprs.truncate(i + 1);
                            }

                            // Deduplicate arguments in cases like `coalesce(#0, #0)`.
                            let mut prior_exprs = HashSet::new();
                            exprs.retain(|e| prior_exprs.insert(e.clone()));

                            if let Some(expr) = exprs.iter_mut().find(|e| e.is_literal_err()) {
                                // One of the remaining arguments is an error, so
                                // just replace the entire coalesce with that error.
                                *e = expr.take();
                            } else if exprs.len() == 1 {
                                // Only one argument, so the coalesce is a no-op.
                                *e = exprs[0].take();
                            }
                        } else if exprs.iter().all(|e| e.is_literal()) {
                            *e = eval(e);
                        } else if func.propagates_nulls()
                            && exprs.iter().any(|e| e.is_literal_null())
                        {
                            *e = MirScalarExpr::literal_null(e.typ(&relation_type).scalar_type);
                        } else if let Some(err) = exprs.iter().find_map(|e| e.as_literal_err()) {
                            *e = MirScalarExpr::literal(
                                Err(err.clone()),
                                e.typ(&relation_type).scalar_type,
                            );
                        } else if *func == VariadicFunc::RegexpMatch
                            && exprs[1].is_literal()
                            && exprs.get(2).map_or(true, |e| e.is_literal())
                        {
                            let needle = exprs[1].as_literal_str().unwrap();
                            let flags = match exprs.len() {
                                3 => exprs[2].as_literal_str().unwrap(),
                                _ => "",
                            };
                            *e = match func::build_regex(needle, flags) {
                                Ok(regex) => mem::take(exprs).into_first().call_unary(
                                    UnaryFunc::RegexpMatch(func::RegexpMatch(Regex(regex))),
                                ),
                                Err(err) => MirScalarExpr::literal(
                                    Err(err),
                                    e.typ(&relation_type).scalar_type,
                                ),
                            };
                        }
                    }
                    MirScalarExpr::If { cond, then, els } => {
                        if let Some(literal) = cond.as_literal() {
                            match literal {
                                Ok(Datum::True) => *e = then.take(),
                                Ok(Datum::False) | Ok(Datum::Null) => *e = els.take(),
                                Err(err) => {
                                    *e = MirScalarExpr::Literal(
                                        Err(err.clone()),
                                        then.typ(relation_type)
                                            .union(&els.typ(relation_type))
                                            .unwrap(),
                                    )
                                }
                                _ => unreachable!(),
                            }
                        } else if then == els {
                            *e = then.take();
                        } else if then.is_literal_ok() && els.is_literal_ok() {
                            match (then.as_literal(), els.as_literal()) {
                                // Note: NULLs from the condition should not be propagated to the result
                                // of the expression.
                                (Some(Ok(Datum::True)), _) => {
                                    // Rewritten as ((<cond> IS NOT NULL) AND (<cond>)) OR (<els>)
                                    // NULL <cond> results in: (FALSE AND NULL) OR (<els>) => (<els>)
                                    *e = cond
                                        .clone()
                                        .call_unary(UnaryFunc::IsNull(func::IsNull))
                                        .call_unary(UnaryFunc::Not(func::Not))
                                        .call_binary(cond.take(), BinaryFunc::And)
                                        .call_binary(els.take(), BinaryFunc::Or);
                                }
                                (Some(Ok(Datum::False)), _) => {
                                    // Rewritten as ((NOT <cond>) OR (<cond> IS NULL)) AND (<els>)
                                    // NULL <cond> results in: (NULL OR TRUE) AND (<els>) => TRUE AND (<els>) => (<els>)
                                    *e = cond
                                        .clone()
                                        .call_unary(UnaryFunc::Not(func::Not))
                                        .call_binary(
                                            cond.take().call_unary(UnaryFunc::IsNull(func::IsNull)),
                                            BinaryFunc::Or,
                                        )
                                        .call_binary(els.take(), BinaryFunc::And);
                                }
                                (_, Some(Ok(Datum::True))) => {
                                    // Rewritten as (NOT <cond>) OR (<cond> IS NULL) OR (<then>)
                                    // NULL <cond> results in: NULL OR TRUE OR (<then>) => TRUE
                                    *e = cond
                                        .clone()
                                        .call_unary(UnaryFunc::Not(func::Not))
                                        .call_binary(
                                            cond.take().call_unary(UnaryFunc::IsNull(func::IsNull)),
                                            BinaryFunc::Or,
                                        )
                                        .call_binary(then.take(), BinaryFunc::Or);
                                }
                                (_, Some(Ok(Datum::False))) => {
                                    // Rewritten as (<cond> IS NOT NULL) AND (<cond>) AND (<then>)
                                    // NULL <cond> results in: FALSE AND NULL AND (<then>) => FALSE
                                    *e = cond
                                        .clone()
                                        .call_unary(UnaryFunc::IsNull(func::IsNull))
                                        .call_unary(UnaryFunc::Not(func::Not))
                                        .call_binary(cond.take(), BinaryFunc::And)
                                        .call_binary(then.take(), BinaryFunc::And);
                                }
                                _ => {}
                            }
                        }
                    }
                },
            );
        }

        self.visit_mut_pre_post(
            &mut |e| {
                match e {
                    // 2) Push down not expressions
                    MirScalarExpr::CallUnary { func, expr } => {
                        if *func == UnaryFunc::Not(func::Not) {
                            match &mut **expr {
                                MirScalarExpr::CallBinary { expr1, expr2, func } => {
                                    // Transforms `NOT(a <op> b)` to `a negate(<op>) b`
                                    // if a negation exists.
                                    if let Some(negated_func) = func.negate() {
                                        *e = MirScalarExpr::CallBinary {
                                            expr1: Box::new(expr1.take()),
                                            expr2: Box::new(expr2.take()),
                                            func: negated_func,
                                        }
                                    } else {
                                        e.demorgans()
                                    }
                                }
                                // Two negates cancel each other out.
                                MirScalarExpr::CallUnary {
                                    expr: inner_expr,
                                    func: UnaryFunc::Not(func::Not),
                                } => *e = inner_expr.take(),
                                _ => {}
                            }
                        }
                    }
                    _ => {}
                };
                None
            },
            &mut |e| {
                // 3) As the second to last step, try to undistribute AND/OR
                e.undistribute_and_or();
                if let MirScalarExpr::CallBinary { func, expr1, expr2 } = e {
                    if matches!(
                        *func,
                        BinaryFunc::Eq | BinaryFunc::Or | BinaryFunc::And | BinaryFunc::NotEq
                    ) && expr2 < expr1
                    {
                        // 4) Canonically order elements so that deduplication works better.
                        ::std::mem::swap(expr1, expr2);
                    }
                }
            },
        );
    }

    /// Decompose an IsNull expression into a disjunction of
    /// simpler expressions.
    ///
    /// Assumes that `self` is the expression inside of an IsNull.
    /// Returns `Some(expressions)` if the outer IsNull is to be
    /// replaced by some other expression.
    fn decompose_is_null(&mut self) -> Option<MirScalarExpr> {
        // TODO: allow simplification of VariadicFunc and NonePure
        if let MirScalarExpr::CallBinary { func, expr1, expr2 } = self {
            // (<expr1> <op> <expr2>) IS NULL can often be simplified to
            // (<expr1> IS NULL) OR (<expr2> IS NULL).
            if func.propagates_nulls() && !func.introduces_nulls() {
                let expr1 = expr1.take().call_unary(UnaryFunc::IsNull(func::IsNull));
                let expr2 = expr2.take().call_unary(UnaryFunc::IsNull(func::IsNull));
                return Some(expr1.call_binary(expr2, BinaryFunc::Or));
            }
        } else if let MirScalarExpr::CallUnary {
            func,
            expr: inner_expr,
        } = self
        {
            if !func.introduces_nulls() {
                if func.propagates_nulls() {
                    *self = inner_expr.take();
                    return self.decompose_is_null();
                } else {
                    return Some(MirScalarExpr::literal_ok(Datum::False, ScalarType::Bool));
                }
            }
        }
        None
    }

    /// Transforms !(a && b) into !a || !b and !(a || b) into !a && !b
    fn demorgans(&mut self) {
        if let MirScalarExpr::CallUnary {
            expr: inner,
            func: UnaryFunc::Not(func::Not),
        } = self
        {
            if let MirScalarExpr::CallBinary { expr1, expr2, func } = &mut **inner {
                match func {
                    BinaryFunc::And => {
                        let inner0 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr1.take()),
                            func: UnaryFunc::Not(func::Not),
                        };
                        let inner1 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr2.take()),
                            func: UnaryFunc::Not(func::Not),
                        };
                        *self = MirScalarExpr::CallBinary {
                            expr1: Box::new(inner0),
                            expr2: Box::new(inner1),
                            func: BinaryFunc::Or,
                        }
                    }
                    BinaryFunc::Or => {
                        let inner0 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr1.take()),
                            func: UnaryFunc::Not(func::Not),
                        };
                        let inner1 = MirScalarExpr::CallUnary {
                            expr: Box::new(expr2.take()),
                            func: UnaryFunc::Not(func::Not),
                        };
                        *self = MirScalarExpr::CallBinary {
                            expr1: Box::new(inner0),
                            expr2: Box::new(inner1),
                            func: BinaryFunc::And,
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /* #region `undistribute_and` and helper functions */

    /// AND/OR undistribution to apply at each `ScalarExpr`.
    /// Transforms (a && b) || (a && c) into a && (b || c)
    /// Transforms (a || b) && (a || c) into a || (b && c)
    fn undistribute_and_or(&mut self) {
        if let MirScalarExpr::CallBinary { expr1, expr2, func } = self {
            if *func == BinaryFunc::Or || *func == BinaryFunc::And {
                // We are trying to undistribute AND when we see
                // `(a && b) || (a && c)`. Otherwise, we are trying to
                // undistribute OR`.
                let undistribute_and = *func == BinaryFunc::Or;
                let mut operands1 = Vec::new();
                expr1.harvest_operands(&mut operands1, undistribute_and);
                let operands1_len = operands1.len();

                let mut operands2 = Vec::new();
                expr2.harvest_operands(&mut operands2, undistribute_and);

                let mut intersection = operands1
                    .into_iter()
                    .filter(|e| operands2.contains(e))
                    .collect::<Vec<_>>();
                intersection.sort();
                intersection.dedup();

                // To construct the undistributed version from
                // `((a1 && ... && aN) & b) || ((a1 && ... && aN) && c)`, we
                // 1) remove all copies of operands in the intersection from
                //    `self` to get `(b || c)`
                // 2) AND one copy of the operands in the intersection back on
                //    to get (a1 && ... && aN) && (b || c)
                // (a1 & ... & aN) | ((a1 & ... & aN) & b) is equal to
                //    (a1 & ... & aN), so instead we take one of the operands in
                //    the intersection as `self` and then AND the rest on later.
                if intersection.len() == operands1_len || intersection.len() == operands2.len() {
                    *self = intersection.pop().unwrap();
                } else if !intersection.is_empty() {
                    expr1.suppress_operands(&intersection[..], undistribute_and);
                    expr2.suppress_operands(&intersection[..], undistribute_and);
                    if expr2 < expr1 {
                        ::std::mem::swap(expr1, expr2);
                    }
                }

                for term in intersection.into_iter() {
                    let (expr1, expr2) = if term < *self {
                        (term, self.take())
                    } else {
                        (self.take(), term)
                    };
                    *self = MirScalarExpr::CallBinary {
                        expr1: Box::new(expr1),
                        expr2: Box::new(expr2),
                        func: if undistribute_and {
                            BinaryFunc::And
                        } else {
                            BinaryFunc::Or
                        },
                    };
                }
            }
        }
    }

    /// Collects undistributable terms from X expressions.
    /// If `and`, X is AND. If not `and`, X is OR.
    fn harvest_operands(&mut self, operands: &mut Vec<MirScalarExpr>, and: bool) {
        if let MirScalarExpr::CallBinary { expr1, expr2, func } = self {
            let operator = if and { BinaryFunc::And } else { BinaryFunc::Or };
            if *func == operator {
                expr1.harvest_operands(operands, and);
                expr2.harvest_operands(operands, and);
                return;
            }
        }
        operands.push(self.clone())
    }

    /// Removes undistributed terms from AND expressions.
    /// If `and`, X is AND. If not `and`, X is OR.
    fn suppress_operands(&mut self, operands: &[MirScalarExpr], and: bool) {
        if let MirScalarExpr::CallBinary { expr1, expr2, func } = self {
            let operator = if and { BinaryFunc::And } else { BinaryFunc::Or };
            if *func == operator {
                // Suppress the ands in children.
                expr1.suppress_operands(operands, and);
                expr2.suppress_operands(operands, and);

                // If either argument is in our list, replace it by `true`.
                let tru = MirScalarExpr::literal_ok(Datum::True, ScalarType::Bool);
                if operands.contains(expr1) {
                    *self = std::mem::replace(expr2, tru);
                } else if operands.contains(expr2) {
                    *self = std::mem::replace(expr1, tru);
                }
            }
        }
    }

    /* #endregion */

    /// Adds any columns that *must* be non-Null for `self` to be non-Null.
    pub fn non_null_requirements(&self, columns: &mut HashSet<usize>) {
        match self {
            MirScalarExpr::Column(col) => {
                columns.insert(*col);
            }
            MirScalarExpr::Literal(..) => {}
            MirScalarExpr::CallUnmaterializable(_) => (),
            MirScalarExpr::CallUnary { func, expr } => {
                if func.propagates_nulls() {
                    expr.non_null_requirements(columns);
                }
            }
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func.propagates_nulls() {
                    expr1.non_null_requirements(columns);
                    expr2.non_null_requirements(columns);
                }
            }
            MirScalarExpr::CallVariadic { func, exprs } => {
                if func.propagates_nulls() {
                    for expr in exprs {
                        expr.non_null_requirements(columns);
                    }
                }
            }
            MirScalarExpr::If { .. } => (),
        }
    }

    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        match self {
            MirScalarExpr::Column(i) => relation_type.column_types[*i].clone(),
            MirScalarExpr::Literal(_, typ) => typ.clone(),
            MirScalarExpr::CallUnmaterializable(func) => func.output_type(),
            MirScalarExpr::CallUnary { expr, func } => func.output_type(expr.typ(relation_type)),
            MirScalarExpr::CallBinary { expr1, expr2, func } => {
                func.output_type(expr1.typ(relation_type), expr2.typ(relation_type))
            }
            MirScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(relation_type)).collect())
            }
            MirScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(relation_type);
                let else_type = els.typ(relation_type);
                then_type.union(&else_type).unwrap()
            }
        }
    }

    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Datum<'a>, EvalError> {
        match self {
            MirScalarExpr::Column(index) => Ok(datums[*index].clone()),
            MirScalarExpr::Literal(res, _column_type) => match res {
                Ok(row) => Ok(row.unpack_first()),
                Err(e) => Err(e.clone()),
            },
            // Unmaterializable functions must be transformed away before
            // evaluation. Their purpose is as a placeholder for data that is
            // not known at plan time but can be inlined before runtime.
            MirScalarExpr::CallUnmaterializable(x) => Err(EvalError::Internal(format!(
                "cannot evaluate unmaterializable function: {:?}",
                x
            ))),
            MirScalarExpr::CallUnary { func, expr } => func.eval(datums, temp_storage, expr),
            MirScalarExpr::CallBinary { func, expr1, expr2 } => {
                func.eval(datums, temp_storage, expr1, expr2)
            }
            MirScalarExpr::CallVariadic { func, exprs } => func.eval(datums, temp_storage, exprs),
            MirScalarExpr::If { cond, then, els } => match cond.eval(datums, temp_storage)? {
                Datum::True => then.eval(datums, temp_storage),
                Datum::False | Datum::Null => els.eval(datums, temp_storage),
                d => Err(EvalError::Internal(format!(
                    "if condition evaluated to non-boolean datum: {:?}",
                    d
                ))),
            },
        }
    }

    /// True iff the expression contains
    /// `UnmaterializableFunc::MzLogicalTimestamp`.
    pub fn contains_temporal(&self) -> bool {
        let mut contains = false;
        self.visit_post(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzLogicalTimestamp) = e
            {
                contains = true;
            }
        });
        contains
    }

    /// True iff the expression contains an `UnmaterializableFunc`.
    pub fn contains_unmaterializable(&self) -> bool {
        let mut contains = false;
        self.visit_post(&mut |e| {
            if let MirScalarExpr::CallUnmaterializable(_) = e {
                contains = true;
            }
        });
        contains
    }
}

impl fmt::Display for MirScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        use MirScalarExpr::*;
        match self {
            Column(i) => write!(f, "#{}", i)?,
            Literal(Ok(row), _) => write!(f, "{}", row.unpack_first())?,
            Literal(Err(e), _) => write!(f, "(err: {})", e)?,
            CallUnmaterializable(func) => write!(f, "{}()", func)?,
            CallUnary { func, expr } => {
                write!(f, "{}({})", func, expr)?;
            }
            CallBinary { func, expr1, expr2 } => {
                if func.is_infix_op() {
                    write!(f, "({} {} {})", expr1, func, expr2)?;
                } else {
                    write!(f, "{}({}, {})", func, expr1, expr2)?;
                }
            }
            CallVariadic { func, exprs } => {
                write!(f, "{}({})", func, separated(", ", exprs.clone()))?;
            }
            If { cond, then, els } => {
                write!(f, "if {} then {{{}}} else {{{}}}", cond, then, els)?;
            }
        }
        Ok(())
    }
}

impl VisitChildren for MirScalarExpr {
    fn visit_children<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr);
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    fn visit_mut_children<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr);
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    fn try_visit_children<'a, F, E>(&'a self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a Self) -> Result<(), E>,
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr)?;
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)?;
            }
        }
        Ok(())
    }

    fn try_visit_mut_children<'a, F, E>(&'a mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(&'a mut Self) -> Result<(), E>,
    {
        use MirScalarExpr::*;
        match self {
            Column(_) | Literal(_, _) | CallUnmaterializable(_) => (),
            CallUnary { expr, .. } => {
                f(expr)?;
            }
            CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)?;
            }
            CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
            }
            If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)?;
            }
        }
        Ok(())
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum DomainLimit {
    None,
    Inclusive(i64),
    Exclusive(i64),
}

impl From<&DomainLimit> for ProtoDomainLimit {
    fn from(limit: &DomainLimit) -> Self {
        use proto_domain_limit::Kind::*;
        let kind = match limit {
            DomainLimit::None => None(()),
            DomainLimit::Inclusive(v) => Inclusive(*v),
            DomainLimit::Exclusive(v) => Exclusive(*v),
        };
        ProtoDomainLimit { kind: Some(kind) }
    }
}

impl TryFrom<ProtoDomainLimit> for DomainLimit {
    type Error = TryFromProtoError;

    fn try_from(limit: ProtoDomainLimit) -> Result<Self, Self::Error> {
        use proto_domain_limit::Kind::*;
        if let Some(kind) = limit.kind {
            match kind {
                None(()) => Ok(DomainLimit::None),
                Inclusive(v) => Ok(DomainLimit::Inclusive(v)),
                Exclusive(v) => Ok(DomainLimit::Exclusive(v)),
            }
        } else {
            Err(TryFromProtoError::missing_field("`ProtoDomainLimit::kind`"))
        }
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum EvalError {
    CharacterNotValidForEncoding(i32),
    CharacterTooLargeForEncoding(i32),
    DateBinOutOfRange(String),
    DivisionByZero,
    Unsupported {
        feature: String,
        issue_no: Option<usize>,
    },
    FloatOverflow,
    FloatUnderflow,
    NumericFieldOverflow,
    Float32OutOfRange,
    Float64OutOfRange,
    Int16OutOfRange,
    Int32OutOfRange,
    Int64OutOfRange,
    OidOutOfRange,
    IntervalOutOfRange,
    TimestampOutOfRange,
    CharOutOfRange,
    InvalidBase64Equals,
    InvalidBase64Symbol(char),
    InvalidBase64EndSequence,
    InvalidTimezone(String),
    InvalidTimezoneInterval,
    InvalidTimezoneConversion,
    InvalidLayer {
        max_layer: usize,
        val: i64,
    },
    InvalidArray(InvalidArrayError),
    InvalidEncodingName(String),
    InvalidHashAlgorithm(String),
    InvalidByteSequence {
        byte_sequence: String,
        encoding_name: String,
    },
    InvalidJsonbCast {
        from: String,
        to: String,
    },
    InvalidRegex(String),
    InvalidRegexFlag(char),
    InvalidParameterValue(String),
    NegSqrt,
    NullCharacterNotPermitted,
    UnknownUnits(String),
    UnsupportedUnits(String, String),
    UnterminatedLikeEscapeSequence,
    Parse(ParseError),
    ParseHex(ParseHexError),
    Internal(String),
    InfinityOutOfDomain(String),
    NegativeOutOfDomain(String),
    ZeroOutOfDomain(String),
    OutOfDomain(DomainLimit, DomainLimit, String),
    ComplexOutOfRange(String),
    MultipleRowsFromSubquery,
    Undefined(String),
    LikePatternTooLong,
    LikeEscapeTooLong,
    StringValueTooLong {
        target_type: String,
        length: usize,
    },
    MultidimensionalArrayRemovalNotSupported,
    IncompatibleArrayDimensions {
        dims: Option<(usize, usize)>,
    },
    TypeFromOid(String),
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EvalError::CharacterNotValidForEncoding(v) => {
                write!(f, "requested character not valid for encoding: {v}")
            }
            EvalError::CharacterTooLargeForEncoding(v) => {
                write!(f, "requested character too large for encoding: {v}")
            }
            EvalError::DateBinOutOfRange(message) => f.write_str(message),
            EvalError::DivisionByZero => f.write_str("division by zero"),
            EvalError::Unsupported { feature, issue_no } => {
                write!(f, "{} not yet supported", feature)?;
                if let Some(issue_no) = issue_no {
                    write!(f, ", see https://github.com/MaterializeInc/materialize/issues/{} for more details", issue_no)?;
                }
                Ok(())
            }
            EvalError::FloatOverflow => f.write_str("value out of range: overflow"),
            EvalError::FloatUnderflow => f.write_str("value out of range: underflow"),
            EvalError::NumericFieldOverflow => f.write_str("numeric field overflow"),
            EvalError::Float32OutOfRange => f.write_str("real out of range"),
            EvalError::Float64OutOfRange => f.write_str("double precision out of range"),
            EvalError::Int16OutOfRange => f.write_str("smallint out of range"),
            EvalError::Int32OutOfRange => f.write_str("integer out of range"),
            EvalError::Int64OutOfRange => f.write_str("bigint out of range"),
            EvalError::OidOutOfRange => f.write_str("OID out of range"),
            EvalError::IntervalOutOfRange => f.write_str("interval out of range"),
            EvalError::TimestampOutOfRange => f.write_str("timestamp out of range"),
            EvalError::CharOutOfRange => f.write_str("\"char\" out of range"),
            EvalError::InvalidBase64Equals => {
                f.write_str("unexpected \"=\" while decoding base64 sequence")
            }
            EvalError::InvalidBase64Symbol(c) => write!(
                f,
                "invalid symbol \"{}\" found while decoding base64 sequence",
                c.escape_default()
            ),
            EvalError::InvalidBase64EndSequence => f.write_str("invalid base64 end sequence"),
            EvalError::InvalidJsonbCast { from, to } => {
                write!(f, "cannot cast jsonb {} to type {}", from, to)
            }
            EvalError::InvalidTimezone(tz) => write!(f, "invalid time zone '{}'", tz),
            EvalError::InvalidTimezoneInterval => {
                f.write_str("timezone interval must not contain months or years")
            }
            EvalError::InvalidTimezoneConversion => f.write_str("invalid timezone conversion"),
            EvalError::InvalidLayer { max_layer, val } => write!(
                f,
                "invalid layer: {}; must use value within [1, {}]",
                val, max_layer
            ),
            EvalError::InvalidArray(e) => e.fmt(f),
            EvalError::InvalidEncodingName(name) => write!(f, "invalid encoding name '{}'", name),
            EvalError::InvalidHashAlgorithm(alg) => write!(f, "invalid hash algorithm '{}'", alg),
            EvalError::InvalidByteSequence {
                byte_sequence,
                encoding_name,
            } => write!(
                f,
                "invalid byte sequence '{}' for encoding '{}'",
                byte_sequence, encoding_name
            ),
            EvalError::NegSqrt => f.write_str("cannot take square root of a negative number"),
            EvalError::NullCharacterNotPermitted => f.write_str("null character not permitted"),
            EvalError::InvalidRegex(e) => write!(f, "invalid regular expression: {}", e),
            EvalError::InvalidRegexFlag(c) => write!(f, "invalid regular expression flag: {}", c),
            EvalError::InvalidParameterValue(s) => f.write_str(s),
            EvalError::UnknownUnits(units) => write!(f, "unit '{}' not recognized", units),
            EvalError::UnsupportedUnits(units, typ) => {
                write!(f, "unit '{}' not supported for type {}", units, typ)
            }
            EvalError::UnterminatedLikeEscapeSequence => {
                f.write_str("unterminated escape sequence in LIKE")
            }
            EvalError::Parse(e) => e.fmt(f),
            EvalError::ParseHex(e) => e.fmt(f),
            EvalError::Internal(s) => write!(f, "internal error: {}", s),
            EvalError::InfinityOutOfDomain(s) => {
                write!(f, "function {} is only defined for finite arguments", s)
            }
            EvalError::NegativeOutOfDomain(s) => {
                write!(f, "function {} is not defined for negative numbers", s)
            }
            EvalError::ZeroOutOfDomain(s) => {
                write!(f, "function {} is not defined for zero", s)
            }
            EvalError::OutOfDomain(lower, upper, s) => {
                use DomainLimit::*;
                write!(f, "function {s} is defined for numbers ")?;
                match (lower, upper) {
                    (Inclusive(n), None) => write!(f, "greater than or equal to {n}"),
                    (Exclusive(n), None) => write!(f, "greater than {n}"),
                    (None, Inclusive(n)) => write!(f, "less than or equal to {n}"),
                    (None, Exclusive(n)) => write!(f, "less than {n}"),
                    (Inclusive(lo), Inclusive(hi)) => write!(f, "between {lo} and {hi} inclusive"),
                    (Exclusive(lo), Exclusive(hi)) => write!(f, "between {lo} and {hi} exclusive"),
                    (Inclusive(lo), Exclusive(hi)) => {
                        write!(f, "between {lo} inclusive and {hi} exclusive")
                    }
                    (Exclusive(lo), Inclusive(hi)) => {
                        write!(f, "between {lo} exclusive and {hi} inclusive")
                    }
                    (None, None) => panic!("invalid domain error"),
                }
            }
            EvalError::ComplexOutOfRange(s) => {
                write!(f, "function {} cannot return complex numbers", s)
            }
            EvalError::MultipleRowsFromSubquery => {
                write!(f, "more than one record produced in subquery")
            }
            EvalError::Undefined(s) => {
                write!(f, "{} is undefined", s)
            }
            EvalError::LikePatternTooLong => {
                write!(f, "LIKE pattern exceeds maximum length")
            }
            EvalError::LikeEscapeTooLong => {
                write!(f, "invalid escape string")
            }
            EvalError::StringValueTooLong {
                target_type,
                length,
            } => {
                write!(f, "value too long for type {}({})", target_type, length)
            }
            EvalError::MultidimensionalArrayRemovalNotSupported => {
                write!(
                    f,
                    "removing elements from multidimensional arrays is not supported"
                )
            }
            EvalError::IncompatibleArrayDimensions { dims: _ } => {
                write!(f, "cannot concatenate incompatible arrays")
            }
            EvalError::TypeFromOid(msg) => write!(f, "{msg}"),
        }
    }
}

impl EvalError {
    pub fn detail(&self) -> Option<String> {
        match self {
            EvalError::IncompatibleArrayDimensions { dims: None } => Some(
                "Arrays with differing dimensions are not compatible for concatenation."
                    .to_string(),
            ),
            EvalError::IncompatibleArrayDimensions {
                dims: Some((a_dims, b_dims)),
            } => Some(format!(
                "Arrays of {} and {} dimensions are not compatible for concatenation.",
                a_dims, b_dims
            )),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            EvalError::InvalidBase64EndSequence => Some(
                "Input data is missing padding, is truncated, or is otherwise corrupted.".into(),
            ),
            EvalError::LikeEscapeTooLong => {
                Some("Escape string must be empty or one character.".into())
            }
            _ => None,
        }
    }
}

impl std::error::Error for EvalError {}

impl From<ParseError> for EvalError {
    fn from(e: ParseError) -> EvalError {
        EvalError::Parse(e)
    }
}

impl From<ParseHexError> for EvalError {
    fn from(e: ParseHexError) -> EvalError {
        EvalError::ParseHex(e)
    }
}

impl From<InvalidArrayError> for EvalError {
    fn from(e: InvalidArrayError) -> EvalError {
        EvalError::InvalidArray(e)
    }
}

impl From<regex::Error> for EvalError {
    fn from(e: regex::Error) -> EvalError {
        EvalError::InvalidRegex(e.to_string())
    }
}

impl From<TypeFromOidError> for EvalError {
    fn from(e: TypeFromOidError) -> EvalError {
        EvalError::TypeFromOid(e.to_string())
    }
}

impl From<&EvalError> for ProtoEvalError {
    fn from(error: &EvalError) -> Self {
        use proto_eval_error::*;
        use proto_incompatible_array_dimensions::*;
        use Kind::*;
        let kind = match error {
            EvalError::CharacterNotValidForEncoding(v) => CharacterNotValidForEncoding(*v),
            EvalError::CharacterTooLargeForEncoding(v) => CharacterTooLargeForEncoding(*v),
            EvalError::DateBinOutOfRange(v) => DateBinOutOfRange(v.clone()),
            EvalError::DivisionByZero => DivisionByZero(()),
            EvalError::Unsupported { feature, issue_no } => Unsupported(ProtoUnsupported {
                feature: feature.clone(),
                issue_no: issue_no.into_proto(),
            }),
            EvalError::FloatOverflow => FloatOverflow(()),
            EvalError::FloatUnderflow => FloatUnderflow(()),
            EvalError::NumericFieldOverflow => NumericFieldOverflow(()),
            EvalError::Float32OutOfRange => Float32OutOfRange(()),
            EvalError::Float64OutOfRange => Float64OutOfRange(()),
            EvalError::Int16OutOfRange => Int16OutOfRange(()),
            EvalError::Int32OutOfRange => Int32OutOfRange(()),
            EvalError::Int64OutOfRange => Int64OutOfRange(()),
            EvalError::OidOutOfRange => OidOutOfRange(()),
            EvalError::IntervalOutOfRange => IntervalOutOfRange(()),
            EvalError::TimestampOutOfRange => TimestampOutOfRange(()),
            EvalError::CharOutOfRange => CharOutOfRange(()),
            EvalError::InvalidBase64Equals => InvalidBase64Equals(()),
            EvalError::InvalidBase64Symbol(sym) => InvalidBase64Symbol(sym.into_proto()),
            EvalError::InvalidBase64EndSequence => InvalidBase64EndSequence(()),
            EvalError::InvalidTimezone(tz) => InvalidTimezone(tz.clone()),
            EvalError::InvalidTimezoneInterval => InvalidTimezoneInterval(()),
            EvalError::InvalidTimezoneConversion => InvalidTimezoneConversion(()),
            EvalError::InvalidLayer { max_layer, val } => InvalidLayer(ProtoInvalidLayer {
                max_layer: max_layer.into_proto(),
                val: *val,
            }),
            EvalError::InvalidArray(error) => InvalidArray(error.into()),
            EvalError::InvalidEncodingName(v) => InvalidEncodingName(v.clone()),
            EvalError::InvalidHashAlgorithm(v) => InvalidHashAlgorithm(v.clone()),
            EvalError::InvalidByteSequence {
                byte_sequence,
                encoding_name,
            } => InvalidByteSequence(ProtoInvalidByteSequence {
                byte_sequence: byte_sequence.clone(),
                encoding_name: encoding_name.clone(),
            }),
            EvalError::InvalidJsonbCast { from, to } => InvalidJsonbCast(ProtoInvalidJsonbCast {
                from: from.clone(),
                to: to.clone(),
            }),
            EvalError::InvalidRegex(v) => InvalidRegex(v.clone()),
            EvalError::InvalidRegexFlag(v) => InvalidRegexFlag(v.into_proto()),
            EvalError::InvalidParameterValue(v) => InvalidParameterValue(v.clone()),
            EvalError::NegSqrt => NegSqrt(()),
            EvalError::NullCharacterNotPermitted => NullCharacterNotPermitted(()),
            EvalError::UnknownUnits(v) => UnknownUnits(v.clone()),
            EvalError::UnsupportedUnits(units, typ) => UnsupportedUnits(ProtoUnsupportedUnits {
                units: units.clone(),
                typ: typ.clone(),
            }),
            EvalError::UnterminatedLikeEscapeSequence => UnterminatedLikeEscapeSequence(()),
            EvalError::Parse(error) => Parse(error.into()),
            EvalError::ParseHex(error) => ParseHex(error.into()),
            EvalError::Internal(v) => Internal(v.clone()),
            EvalError::InfinityOutOfDomain(v) => InfinityOutOfDomain(v.clone()),
            EvalError::NegativeOutOfDomain(v) => NegativeOutOfDomain(v.clone()),
            EvalError::ZeroOutOfDomain(v) => ZeroOutOfDomain(v.clone()),
            EvalError::OutOfDomain(lower, upper, id) => OutOfDomain(ProtoOutOfDomain {
                lower: Some(lower.into()),
                upper: Some(upper.into()),
                id: id.clone(),
            }),
            EvalError::ComplexOutOfRange(v) => ComplexOutOfRange(v.clone()),
            EvalError::MultipleRowsFromSubquery => MultipleRowsFromSubquery(()),
            EvalError::Undefined(v) => Undefined(v.clone()),
            EvalError::LikePatternTooLong => LikePatternTooLong(()),
            EvalError::LikeEscapeTooLong => LikeEscapeTooLong(()),
            EvalError::StringValueTooLong {
                target_type,
                length,
            } => StringValueTooLong(ProtoStringValueTooLong {
                target_type: target_type.clone(),
                length: length.into_proto(),
            }),
            EvalError::MultidimensionalArrayRemovalNotSupported => {
                MultidimensionalArrayRemovalNotSupported(())
            }
            EvalError::IncompatibleArrayDimensions { dims } => {
                IncompatibleArrayDimensions(ProtoIncompatibleArrayDimensions {
                    dims: dims.map(|dims| ProtoDims {
                        f0: dims.0.into_proto(),
                        f1: dims.1.into_proto(),
                    }),
                })
            }
            EvalError::TypeFromOid(v) => TypeFromOid(v.clone()),
        };
        ProtoEvalError { kind: Some(kind) }
    }
}

impl TryFrom<ProtoEvalError> for EvalError {
    type Error = TryFromProtoError;

    fn try_from(error: ProtoEvalError) -> Result<Self, Self::Error> {
        use proto_eval_error::Kind::*;
        match error.kind {
            Some(kind) => match kind {
                CharacterNotValidForEncoding(v) => Ok(EvalError::CharacterNotValidForEncoding(v)),
                CharacterTooLargeForEncoding(v) => Ok(EvalError::CharacterTooLargeForEncoding(v)),
                DateBinOutOfRange(v) => Ok(EvalError::DateBinOutOfRange(v)),
                DivisionByZero(()) => Ok(EvalError::DivisionByZero),
                Unsupported(v) => Ok(EvalError::Unsupported {
                    feature: v.feature,
                    issue_no: Option::<usize>::from_proto(v.issue_no)?,
                }),
                FloatOverflow(()) => Ok(EvalError::FloatOverflow),
                FloatUnderflow(()) => Ok(EvalError::FloatUnderflow),
                NumericFieldOverflow(()) => Ok(EvalError::NumericFieldOverflow),
                Float32OutOfRange(()) => Ok(EvalError::Float32OutOfRange),
                Float64OutOfRange(()) => Ok(EvalError::Float64OutOfRange),
                Int16OutOfRange(()) => Ok(EvalError::Int16OutOfRange),
                Int32OutOfRange(()) => Ok(EvalError::Int32OutOfRange),
                Int64OutOfRange(()) => Ok(EvalError::Int64OutOfRange),
                OidOutOfRange(()) => Ok(EvalError::OidOutOfRange),
                IntervalOutOfRange(()) => Ok(EvalError::IntervalOutOfRange),
                TimestampOutOfRange(()) => Ok(EvalError::TimestampOutOfRange),
                CharOutOfRange(()) => Ok(EvalError::CharOutOfRange),
                InvalidBase64Equals(()) => Ok(EvalError::InvalidBase64Equals),
                InvalidBase64Symbol(v) => char::from_proto(v).map(EvalError::InvalidBase64Symbol),
                InvalidBase64EndSequence(()) => Ok(EvalError::InvalidBase64EndSequence),
                InvalidTimezone(v) => Ok(EvalError::InvalidTimezone(v)),
                InvalidTimezoneInterval(()) => Ok(EvalError::InvalidTimezoneInterval),
                InvalidTimezoneConversion(()) => Ok(EvalError::InvalidTimezoneConversion),
                InvalidLayer(v) => Ok(EvalError::InvalidLayer {
                    max_layer: usize::from_proto(v.max_layer)?,
                    val: v.val,
                }),
                InvalidArray(error) => Ok(EvalError::InvalidArray(error.try_into()?)),
                InvalidEncodingName(v) => Ok(EvalError::InvalidEncodingName(v)),
                InvalidHashAlgorithm(v) => Ok(EvalError::InvalidHashAlgorithm(v)),
                InvalidByteSequence(v) => Ok(EvalError::InvalidByteSequence {
                    byte_sequence: v.byte_sequence,
                    encoding_name: v.encoding_name,
                }),
                InvalidJsonbCast(v) => Ok(EvalError::InvalidJsonbCast {
                    from: v.from,
                    to: v.to,
                }),
                InvalidRegex(v) => Ok(EvalError::InvalidRegex(v)),
                InvalidRegexFlag(v) => Ok(EvalError::InvalidRegexFlag(char::from_proto(v)?)),
                InvalidParameterValue(v) => Ok(EvalError::InvalidParameterValue(v)),
                NegSqrt(()) => Ok(EvalError::NegSqrt),
                NullCharacterNotPermitted(()) => Ok(EvalError::NullCharacterNotPermitted),
                UnknownUnits(v) => Ok(EvalError::UnknownUnits(v)),
                UnsupportedUnits(v) => Ok(EvalError::UnsupportedUnits(v.units, v.typ)),
                UnterminatedLikeEscapeSequence(()) => Ok(EvalError::UnterminatedLikeEscapeSequence),
                Parse(error) => Ok(EvalError::Parse(error.try_into()?)),
                ParseHex(error) => Ok(EvalError::ParseHex(error.try_into()?)),
                Internal(v) => Ok(EvalError::Internal(v)),
                InfinityOutOfDomain(v) => Ok(EvalError::InfinityOutOfDomain(v)),
                NegativeOutOfDomain(v) => Ok(EvalError::NegativeOutOfDomain(v)),
                ZeroOutOfDomain(v) => Ok(EvalError::ZeroOutOfDomain(v)),
                OutOfDomain(v) => Ok(EvalError::OutOfDomain(
                    v.lower.try_into_if_some("`ProtoDomainLimit::lower`")?,
                    v.upper.try_into_if_some("`ProtoDomainLimit::upper`")?,
                    v.id,
                )),
                ComplexOutOfRange(v) => Ok(EvalError::ComplexOutOfRange(v)),
                MultipleRowsFromSubquery(()) => Ok(EvalError::MultipleRowsFromSubquery),
                Undefined(v) => Ok(EvalError::Undefined(v)),
                LikePatternTooLong(()) => Ok(EvalError::LikePatternTooLong),
                LikeEscapeTooLong(()) => Ok(EvalError::LikeEscapeTooLong),
                StringValueTooLong(v) => Ok(EvalError::StringValueTooLong {
                    target_type: v.target_type,
                    length: usize::from_proto(v.length)?,
                }),
                MultidimensionalArrayRemovalNotSupported(()) => {
                    Ok(EvalError::MultidimensionalArrayRemovalNotSupported)
                }
                IncompatibleArrayDimensions(v) => Ok(EvalError::IncompatibleArrayDimensions {
                    dims: v
                        .dims
                        .map::<Result<_, TryFromProtoError>, _>(|dims| {
                            let f0 = usize::from_proto(dims.f0)?;
                            let f1 = usize::from_proto(dims.f1)?;
                            Ok((f0, f1))
                        })
                        .transpose()?,
                }),
                TypeFromOid(v) => Ok(EvalError::TypeFromOid(v)),
            },
            None => Err(TryFromProtoError::missing_field("`ProtoEvalError::kind`")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    #[test]
    fn test_reduce() {
        let relation_type = RelationType::new(vec![
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(false),
        ]);
        let col = MirScalarExpr::Column;
        let err = |e| MirScalarExpr::literal(Err(e), ScalarType::Int64);
        let lit = |i| MirScalarExpr::literal_ok(Datum::Int64(i), ScalarType::Int64);
        let null = || MirScalarExpr::literal_null(ScalarType::Int64);

        struct TestCase {
            input: MirScalarExpr,
            output: MirScalarExpr,
        }

        let test_cases = vec![
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1)],
                },
                output: lit(1),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1), lit(2)],
                },
                output: lit(1),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), lit(2), null()],
                },
                output: lit(2),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![null(), col(0), null(), col(1), lit(2), lit(3)],
                },
                output: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(1), lit(2)],
                },
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(2), col(1)],
                },
                output: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), col(2)],
                },
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![lit(1), err(EvalError::DivisionByZero)],
                },
                output: lit(1),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), err(EvalError::DivisionByZero)],
                },
                output: err(EvalError::DivisionByZero),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![
                        null(),
                        err(EvalError::DivisionByZero),
                        err(EvalError::NumericFieldOverflow),
                    ],
                },
                output: err(EvalError::DivisionByZero),
            },
            TestCase {
                input: MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce,
                    exprs: vec![col(0), err(EvalError::DivisionByZero)],
                },
                output: err(EvalError::DivisionByZero),
            },
        ];

        for tc in test_cases {
            let mut actual = tc.input.clone();
            actual.reduce(&relation_type);
            assert!(
                actual == tc.output,
                "input: {}\nactual: {}\nexpected: {}",
                tc.input,
                actual,
                tc.output
            );
        }
    }

    proptest! {
        #[test]
        fn domain_limit_protobuf_roundtrip(expect in any::<DomainLimit>()) {
            let actual = protobuf_roundtrip::<_, ProtoDomainLimit>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
        #[test]
        fn eval_error_protobuf_roundtrip(expect in any::<EvalError>()) {
            let actual = protobuf_roundtrip::<_, ProtoEvalError>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
