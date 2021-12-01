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

use serde::{Deserialize, Serialize};

use lowertest::MzEnumReflect;
use ore::collections::CollectionExt;
use ore::str::separated;
use repr::adt::array::InvalidArrayError;
use repr::adt::datetime::DateTimeUnits;
use repr::adt::regex::Regex;
use repr::strconv::{ParseError, ParseHexError};
use repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};

use self::func::{BinaryFunc, NullaryFunc, UnaryFunc, VariadicFunc};
use crate::scalar::func::parse_timezone;

pub mod func;
pub mod like_pattern;

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect,
)]
pub enum MirScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Result<Row, EvalError>, ColumnType),
    /// A function call that takes no arguments.
    CallNullary(NullaryFunc),
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

    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        match self {
            MirScalarExpr::Column(_) => (),
            MirScalarExpr::Literal(_, _) => (),
            MirScalarExpr::CallNullary(_) => (),
            MirScalarExpr::CallUnary { expr, .. } => {
                f(expr);
            }
            MirScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            MirScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            MirScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    pub fn visit<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit(f));
        f(self);
    }

    pub fn visit1_mut<'a, F>(&'a mut self, mut f: F)
    where
        F: FnMut(&'a mut Self),
    {
        match self {
            MirScalarExpr::Column(_) => (),
            MirScalarExpr::Literal(_, _) => (),
            MirScalarExpr::CallNullary(_) => (),
            MirScalarExpr::CallUnary { expr, .. } => {
                f(expr);
            }
            MirScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            MirScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            MirScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    pub fn visit_mut<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut(f));
        f(self);
    }

    /// A generalization of `visit_mut`. The function `pre` runs on a
    /// `MirScalarExpr` before it runs on any of the child `MirScalarExpr`s.
    /// The function `post` runs on child `MirScalarExpr`s first before the
    /// parent. Optionally, `pre` can return which child `MirScalarExpr`s, if
    /// any, should be visited (default is to visit all children).
    pub fn visit_mut_pre_post<F1, F2>(&mut self, pre: &mut F1, post: &mut F2)
    where
        F1: FnMut(&mut Self) -> Option<Vec<&mut MirScalarExpr>>,
        F2: FnMut(&mut Self),
    {
        let to_visit = pre(self);
        if let Some(to_visit) = to_visit {
            for e in to_visit {
                e.visit_mut_pre_post(pre, post);
            }
        } else {
            self.visit1_mut(|e| e.visit_mut_pre_post(pre, post));
        }
        post(self);
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        self.visit_mut(&mut |e| {
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
        self.visit_mut(&mut |e| {
            if let MirScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit(&mut |e| {
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
    /// use expr::{BinaryFunc, MirScalarExpr};
    /// use repr::{ColumnType, Datum, RelationType, ScalarType};
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
                    | MirScalarExpr::CallNullary(_) => (),
                    MirScalarExpr::CallUnary { func, expr } => {
                        if expr.is_literal() {
                            *e = eval(e);
                        } else if let UnaryFunc::RecordGet(i) = *func {
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
                        } else if let BinaryFunc::IsLikePatternMatch { case_insensitive } = func {
                            if expr2.is_literal() {
                                // We can at least precompile the regex.
                                let pattern = expr2.as_literal_str().unwrap();
                                let flags = if *case_insensitive { "i" } else { "" };
                                *e = match like_pattern::build_regex(&pattern, flags) {
                                    Ok(regex) => expr1
                                        .take()
                                        .call_unary(UnaryFunc::IsRegexpMatch(Regex(regex))),
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
                                    Ok(regex) => expr1
                                        .take()
                                        .call_unary(UnaryFunc::IsRegexpMatch(Regex(regex))),
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(&relation_type).scalar_type,
                                    ),
                                };
                            }
                        } else if *func == BinaryFunc::DatePartInterval && expr1.is_literal() {
                            let units = expr1.as_literal_str().unwrap();
                            *e = match units.parse::<DateTimeUnits>() {
                                Ok(units) => MirScalarExpr::CallUnary {
                                    func: UnaryFunc::DatePartInterval(units),
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
                                    func: UnaryFunc::DatePartTimestamp(units),
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
                                    func: UnaryFunc::DatePartTimestampTz(units),
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
                                    func: UnaryFunc::DateTruncTimestamp(units),
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
                                    func: UnaryFunc::DateTruncTimestampTz(units),
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
                                    func: UnaryFunc::TimezoneTimestamp(tz),
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
                                    func: UnaryFunc::TimezoneTimestampTz(tz),
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
                                        func: UnaryFunc::TimezoneTime {
                                            tz,
                                            wall_time: *wall_time,
                                        },
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
                        } else if *func == VariadicFunc::RegexpMatch {
                            if exprs[1].is_literal()
                                && exprs.get(2).map_or(true, |e| e.is_literal())
                            {
                                let needle = exprs[1].as_literal_str().unwrap();
                                let flags = match exprs.len() {
                                    3 => exprs[2].as_literal_str().unwrap(),
                                    _ => "",
                                };
                                *e = match func::build_regex(needle, flags) {
                                    Ok(regex) => mem::take(exprs)
                                        .into_first()
                                        .call_unary(UnaryFunc::RegexpMatch(Regex(regex))),
                                    Err(err) => MirScalarExpr::literal(
                                        Err(err),
                                        e.typ(&relation_type).scalar_type,
                                    ),
                                };
                            }
                        }
                    }
                    MirScalarExpr::If { cond, then, els } => {
                        if let Some(literal) = cond.as_literal() {
                            match literal {
                                Ok(Datum::True) => *e = then.take(),
                                Ok(Datum::False) | Ok(Datum::Null) => *e = els.take(),
                                Err(_) => *e = cond.take(),
                                _ => unreachable!(),
                            }
                        } else if then == els {
                            *e = then.take();
                        } else if then.is_literal_ok() && els.is_literal_ok() {
                            match (then.as_literal(), els.as_literal()) {
                                (Some(Ok(Datum::True)), _) => {
                                    *e = cond.take().call_binary(els.take(), BinaryFunc::Or);
                                }
                                (Some(Ok(Datum::False)), _) => {
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
                                    *e = cond
                                        .take()
                                        .call_unary(UnaryFunc::Not(func::Not))
                                        .call_binary(then.take(), BinaryFunc::Or);
                                }
                                (_, Some(Ok(Datum::False))) => {
                                    *e = cond.take().call_binary(then.take(), BinaryFunc::And);
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
                    ) {
                        if expr2 < expr1 {
                            // 4) Canonically order elements so that deduplication works better.
                            ::std::mem::swap(expr1, expr2);
                        }
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
        // TODO: allow simplification of VariadicFunc and NullaryFunc
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
            MirScalarExpr::CallNullary(_) => (),
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
            MirScalarExpr::CallNullary(func) => func.output_type(),
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
                debug_assert!(then_type.scalar_type.base_eq(&else_type.scalar_type));
                ColumnType {
                    nullable: then_type.nullable || else_type.nullable,
                    scalar_type: then_type.scalar_type,
                }
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
            // Nullary functions must be transformed away before evaluation.
            // Their purpose is as a placeholder for data that is not known at
            // plan time but can be inlined before runtime.
            MirScalarExpr::CallNullary(x) => Err(EvalError::Internal(format!(
                "cannot evaluate nullary function: {:?}",
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

    /// True iff the expression contains `NullaryFunc::MzLogicalTimestamp`.
    pub fn contains_temporal(&self) -> bool {
        let mut contains = false;
        self.visit(&mut |e| {
            if let MirScalarExpr::CallNullary(NullaryFunc::MzLogicalTimestamp) = e {
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
            CallNullary(func) => write!(f, "{}()", func)?,
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

#[derive(
    Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzEnumReflect,
)]
pub enum EvalError {
    DivisionByZero,
    FeatureNotSupported(String),
    FloatOverflow,
    FloatUnderflow,
    NumericFieldOverflow,
    Float32OutOfRange,
    Float64OutOfRange,
    Int16OutOfRange,
    Int32OutOfRange,
    Int64OutOfRange,
    IntervalOutOfRange,
    TimestampOutOfRange,
    InvalidBase64Equals,
    InvalidBase64Symbol(char),
    InvalidBase64EndSequence,
    InvalidTimezone(String),
    InvalidTimezoneInterval,
    InvalidTimezoneConversion,
    InvalidDimension {
        max_dim: usize,
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
    UnknownUnits(String),
    UnsupportedDateTimeUnits(DateTimeUnits),
    UnterminatedLikeEscapeSequence,
    Parse(ParseError),
    ParseHex(ParseHexError),
    Internal(String),
    InfinityOutOfDomain(String),
    NegativeOutOfDomain(String),
    ZeroOutOfDomain(String),
    ComplexOutOfRange(String),
    MultipleRowsFromSubquery,
    Undefined(String),
    LikePatternTooLong,
    StringValueTooLong {
        target_type: String,
        length: usize,
    },
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EvalError::DivisionByZero => f.write_str("division by zero"),
            EvalError::FeatureNotSupported(feature) => {
                write!(f, "{}", feature)
            }
            EvalError::FloatOverflow => f.write_str("value out of range: overflow"),
            EvalError::FloatUnderflow => f.write_str("value out of range: underflow"),
            EvalError::NumericFieldOverflow => f.write_str("numeric field overflow"),
            EvalError::Float32OutOfRange => f.write_str("real out of range"),
            EvalError::Float64OutOfRange => f.write_str("double precision out of range"),
            EvalError::Int16OutOfRange => f.write_str("smallint out of range"),
            EvalError::Int32OutOfRange => f.write_str("integer out of range"),
            EvalError::Int64OutOfRange => f.write_str("bigint out of range"),
            EvalError::IntervalOutOfRange => f.write_str("interval out of range"),
            EvalError::TimestampOutOfRange => f.write_str("timestamp out of range"),
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
            EvalError::InvalidDimension { max_dim, val } => write!(
                f,
                "invalid dimension: {}; must use value within [1, {}]",
                val, max_dim
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
            EvalError::InvalidRegex(e) => write!(f, "invalid regular expression: {}", e),
            EvalError::InvalidRegexFlag(c) => write!(f, "invalid regular expression flag: {}", c),
            EvalError::InvalidParameterValue(s) => f.write_str(s),
            EvalError::UnknownUnits(units) => write!(f, "unknown units '{}'", units),
            EvalError::UnsupportedDateTimeUnits(units) => {
                write!(f, "unsupported timestamp units '{}'", units)
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
            EvalError::StringValueTooLong {
                target_type,
                length,
            } => {
                write!(f, "value too long for type {}({})", target_type, length)
            }
        }
    }
}

impl EvalError {
    pub fn detail(&self) -> Option<String> {
        None
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            EvalError::InvalidBase64EndSequence => Some(
                "Input data is missing padding, is truncated, or is otherwise corrupted.".into(),
            ),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reduce() {
        let relation_type = RelationType::new(vec![
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(true),
            ScalarType::Int64.nullable(false),
        ]);
        let col = |i| MirScalarExpr::Column(i);
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
}
