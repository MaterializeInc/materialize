// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::mem;

use chrono::{DateTime, Utc};
use repr::regex::Regex;
use repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};
use serde::{Deserialize, Serialize};

use self::func::{BinaryFunc, DateTruncTo, NullaryFunc, UnaryFunc, VariadicFunc};

pub mod func;
pub mod like_pattern;

#[serde(rename_all = "snake_case")]
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Row, ColumnType),
    /// A function call that takes no arguments.
    CallNullary(NullaryFunc),
    /// A function call that takes one expression as an argument.
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    /// A function call that takes two expressions as arguments.
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    /// A function call that takes an arbitrary number of arguments.
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn columns(is: &[usize]) -> Vec<ScalarExpr> {
        is.iter().map(|i| ScalarExpr::Column(*i)).collect()
    }

    pub fn column(column: usize) -> Self {
        ScalarExpr::Column(column)
    }

    pub fn literal(datum: Datum, typ: ColumnType) -> Self {
        let row = Row::pack(&[datum]);
        ScalarExpr::Literal(row, typ)
    }

    pub fn literal_null(typ: ColumnType) -> Self {
        ScalarExpr::literal(Datum::Null, typ)
    }

    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn if_then_else(self, t: Self, f: Self) -> Self {
        ScalarExpr::If {
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
            ScalarExpr::Column(_) => (),
            ScalarExpr::Literal(_, _) => (),
            ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { expr, .. } => {
                f(expr);
            }
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
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
            ScalarExpr::Column(_) => (),
            ScalarExpr::Literal(_, _) => (),
            ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { expr, .. } => {
                f(expr);
            }
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
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

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        self.visit_mut(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
        });
    }

    pub fn support(&self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn take(&mut self) -> Self {
        mem::replace(
            self,
            ScalarExpr::Literal(
                Row::pack(&[Datum::Null]),
                ColumnType::new(ScalarType::Unknown),
            ),
        )
    }

    pub fn as_literal(&self) -> Option<Datum> {
        if let ScalarExpr::Literal(row, _column_type) = self {
            Some(row.unpack_first())
        } else {
            None
        }
    }

    pub fn is_literal(&self) -> bool {
        if let ScalarExpr::Literal(_, _) = self {
            true
        } else {
            false
        }
    }

    pub fn as_literal_str(&self) -> Option<&str> {
        match self.as_literal() {
            Some(Datum::String(s)) => Some(s),
            _ => None,
        }
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Datum::True) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Datum::False) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Datum::Null) == self.as_literal()
    }

    /// Reduces a complex expression where possible.
    ///
    /// ```rust
    /// use expr::{BinaryFunc, EvalEnv, ScalarExpr};
    /// use repr::{ColumnType, Datum, RelationType, ScalarType};
    ///
    /// let expr_0 = ScalarExpr::Column(0);
    /// let expr_t = ScalarExpr::literal(Datum::True, ColumnType::new(ScalarType::Bool));
    /// let expr_f = ScalarExpr::literal(Datum::False, ColumnType::new(ScalarType::Bool));
    ///
    /// let mut test =
    /// expr_t
    ///     .clone()
    ///     .call_binary(expr_f.clone(), BinaryFunc::And)
    ///     .if_then_else(expr_0, expr_t.clone());
    ///
    /// let input_type = RelationType::new(vec![ColumnType::new(ScalarType::Int32)]);
    /// test.reduce(&input_type, &EvalEnv::default());
    /// assert_eq!(test, expr_t);
    /// ```
    pub fn reduce(&mut self, relation_type: &RelationType, env: &EvalEnv) {
        let temp_storage = &RowArena::new();
        let eval = |e: &ScalarExpr| {
            ScalarExpr::literal(e.eval(&[], env, temp_storage), e.typ(&relation_type))
        };
        self.visit_mut(&mut |e| match e {
            ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) => (),
            ScalarExpr::CallNullary(_) => {
                *e = eval(e);
            }
            ScalarExpr::CallUnary { expr, .. } => {
                if expr.is_literal() {
                    *e = eval(e);
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                if expr1.is_literal() && expr2.is_literal() {
                    *e = eval(e);
                } else if (expr1.is_literal_null() || expr2.is_literal_null())
                    && func.propagates_nulls()
                {
                    *e = ScalarExpr::literal_null(e.typ(relation_type));
                } else if *func == BinaryFunc::MatchLikePattern && expr2.is_literal() {
                    // We can at least precompile the regex.
                    let pattern = expr2.as_literal_str().unwrap();
                    *e = match like_pattern::build_regex(&pattern) {
                        Ok(regex) => expr1.take().call_unary(UnaryFunc::MatchRegex(Regex(regex))),
                        Err(_) => ScalarExpr::literal_null(e.typ(&relation_type)),
                    };
                } else if *func == BinaryFunc::DateTrunc && expr1.is_literal() {
                    let units = expr1.as_literal_str().unwrap();
                    *e = match units.parse::<DateTruncTo>() {
                        Ok(to) => ScalarExpr::CallUnary {
                            func: UnaryFunc::DateTrunc(to),
                            expr: Box::new(expr2.take()),
                        },
                        Err(_) => ScalarExpr::literal_null(e.typ(&relation_type)),
                    }
                } else if *func == BinaryFunc::And && (expr1.is_literal() || expr2.is_literal()) {
                    // If we are here, not both inputs are literals.
                    if expr1.is_literal_false() || expr2.is_literal_true() {
                        *e = expr1.take();
                    } else if expr2.is_literal_false() || expr1.is_literal_true() {
                        *e = expr2.take();
                    }
                } else if *func == BinaryFunc::Or && (expr1.is_literal() || expr2.is_literal()) {
                    // If we are here, not both inputs are literals.
                    if expr1.is_literal_true() || expr2.is_literal_false() {
                        *e = expr1.take();
                    } else if expr2.is_literal_true() || expr1.is_literal_false() {
                        *e = expr2.take();
                    }
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                if exprs.iter().all(|e| e.is_literal()) {
                    *e = eval(e);
                } else if func.propagates_nulls() && exprs.iter().any(|e| e.is_literal_null()) {
                    *e = ScalarExpr::literal_null(e.typ(&relation_type));
                }
            }
            ScalarExpr::If { cond, then, els } => match cond.as_literal() {
                Some(Datum::True) => *e = then.take(),
                Some(Datum::False) | Some(Datum::Null) => *e = els.take(),
                Some(_) => unreachable!(),
                None => (),
            },
        });
    }

    /// Adds any columns that *must* be non-Null for `self` to be non-Null.
    pub fn non_null_requirements(&self, columns: &mut HashSet<usize>) {
        match self {
            ScalarExpr::Column(col) => {
                columns.insert(*col);
            }
            ScalarExpr::Literal(..) => {}
            ScalarExpr::CallNullary(_) => (),
            ScalarExpr::CallUnary { func, expr } => {
                if func.propagates_nulls() {
                    expr.non_null_requirements(columns);
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func.propagates_nulls() {
                    expr1.non_null_requirements(columns);
                    expr2.non_null_requirements(columns);
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                if func.propagates_nulls() {
                    for expr in exprs {
                        expr.non_null_requirements(columns);
                    }
                }
            }
            ScalarExpr::If { .. } => (),
        }
    }

    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        match self {
            ScalarExpr::Column(i) => relation_type.column_types[*i].clone(),
            ScalarExpr::Literal(_, typ) => typ.clone(),
            ScalarExpr::CallNullary(func) => func.output_type(),
            ScalarExpr::CallUnary { expr, func } => func.output_type(expr.typ(relation_type)),
            ScalarExpr::CallBinary { expr1, expr2, func } => {
                func.output_type(expr1.typ(relation_type), expr2.typ(relation_type))
            }
            ScalarExpr::CallVariadic { exprs, func } => {
                func.output_type(exprs.iter().map(|e| e.typ(relation_type)).collect())
            }
            ScalarExpr::If { cond: _, then, els } => {
                let then_type = then.typ(relation_type);
                let else_type = els.typ(relation_type);
                let nullable = then_type.nullable || else_type.nullable;
                if then_type.scalar_type != ScalarType::Unknown {
                    then_type.nullable(nullable)
                } else {
                    else_type.nullable(nullable)
                }
            }
        }
    }

    pub fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        env: &'a EvalEnv,
        temp_storage: &'a RowArena,
    ) -> Datum<'a> {
        match self {
            ScalarExpr::Column(index) => datums[*index].clone(),
            ScalarExpr::Literal(row, _column_type) => row.unpack_first(),
            ScalarExpr::CallNullary(func) => func.eval(env, temp_storage).unwrap_or(Datum::Null),
            ScalarExpr::CallUnary { func, expr } => {
                let datum = expr.eval(datums, env, temp_storage);
                if func.propagates_nulls() && datum.is_null() {
                    Datum::Null
                } else {
                    func.eval(datum, env, temp_storage).unwrap_or(Datum::Null)
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let a = expr1.eval(datums, env, temp_storage);
                let b = expr2.eval(datums, env, temp_storage);
                if func.propagates_nulls() && (a.is_null() || b.is_null()) {
                    Datum::Null
                } else {
                    func.eval(a, b, env, temp_storage).unwrap_or(Datum::Null)
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                let datums = exprs
                    .iter()
                    .map(|e| e.eval(datums, env, temp_storage))
                    .collect::<Vec<_>>();
                if func.propagates_nulls() && datums.iter().any(|e| e.is_null()) {
                    Datum::Null
                } else {
                    func.eval(&datums, env, temp_storage).unwrap_or(Datum::Null)
                }
            }
            ScalarExpr::If { cond, then, els } => match cond.eval(datums, env, temp_storage) {
                Datum::True => then.eval(datums, env, temp_storage),
                Datum::False | Datum::Null => els.eval(datums, env, temp_storage),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
        }
    }
}

/// An evaluation environment. Stores state that controls how certain
/// expressions are evaluated.
#[derive(Default, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct EvalEnv {
    pub logical_time: Option<u64>,
    pub wall_time: Option<DateTime<Utc>>,
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum EvalError {
    DivisionByZero,
    NumericFieldOverflow,
    IntegerOutOfRange,
    InvalidEncodingName(String),
    InvalidByteSequence {
        byte_sequence: String,
        encoding_name: String,
    },
    UnknownUnits(String),
    UnterminatedLikeEscapeSequence,
}

impl std::error::Error for EvalError {}
