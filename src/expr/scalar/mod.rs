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
use pretty::{DocAllocator, DocBuilder};
use repr::regex::Regex;
use repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};
use serde::{Deserialize, Serialize};

use self::func::{BinaryFunc, DateTruncTo, NullaryFunc, UnaryFunc, VariadicFunc};
use crate::pretty::DocBuilderExt;

pub mod func;

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
    /// use repr::{ColumnType, Datum, ScalarType};
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
    /// test.reduce(&EvalEnv::default());
    /// assert_eq!(test, expr_t);
    /// ```
    pub fn reduce(&mut self, env: &EvalEnv) {
        let null = |typ| ScalarExpr::literal(Datum::Null, typ);
        let empty = RelationType::new(vec![]);
        let temp_storage = &RowArena::new();
        let eval =
            |e: &ScalarExpr| ScalarExpr::literal(e.eval(&[], env, temp_storage), e.typ(&empty));
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
                } else if *func == BinaryFunc::MatchRegex && expr2.is_literal() {
                    // we can at least precompile the regex
                    *e = match expr2.eval(&[], env, temp_storage) {
                        Datum::Null => null(expr2.typ(&empty)),
                        Datum::String(string) => {
                            match func::build_like_regex_from_string(&string) {
                                Ok(regex) => {
                                    expr1.take().call_unary(UnaryFunc::MatchRegex(Regex(regex)))
                                }
                                Err(_) => null(expr2.typ(&empty)),
                            }
                        }
                        _ => unreachable!(),
                    };
                } else if *func == BinaryFunc::DateTrunc && expr1.is_literal() {
                    *e = match expr1.eval(&[], env, &temp_storage) {
                        Datum::Null => null(expr1.typ(&empty)),
                        Datum::String(s) => match s.parse::<DateTruncTo>() {
                            Ok(to) => ScalarExpr::CallUnary {
                                func: UnaryFunc::DateTrunc(to),
                                expr: Box::new(expr2.take()),
                            },
                            Err(_) => null(expr1.typ(&empty)),
                        },
                        _ => unreachable!(),
                    }
                } else if *func == BinaryFunc::And && (expr1.is_literal() || expr2.is_literal()) {
                    // If we are here, not both inputs are literals.
                    if expr1.is_literal_false() || expr2.is_literal_true() {
                        *e = (**expr1).clone();
                    } else if expr2.is_literal_false() || expr1.is_literal_true() {
                        *e = (**expr2).clone();
                    }
                } else if *func == BinaryFunc::Or && (expr1.is_literal() || expr2.is_literal()) {
                    // If we are here, not both inputs are literals.
                    if expr1.is_literal_true() || expr2.is_literal_false() {
                        *e = (**expr1).clone();
                    } else if expr2.is_literal_true() || expr1.is_literal_false() {
                        *e = (**expr2).clone();
                    }
                }
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                if exprs.iter().all(|e| e.is_literal()) {
                    *e = eval(e);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                if cond.is_literal() {
                    match cond.eval(&[], env, &temp_storage) {
                        Datum::True if then.is_literal() => *e = eval(then),
                        Datum::False | Datum::Null if els.is_literal() => *e = eval(els),
                        Datum::True | Datum::False | Datum::Null => (),
                        _ => unreachable!(),
                    }
                }
            }
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
            ScalarExpr::CallNullary(func) => func.eval(env, temp_storage),
            ScalarExpr::CallUnary { func, expr } => {
                let datum = expr.eval(datums, env, temp_storage);
                if func.propagates_nulls() && datum.is_null() {
                    Datum::Null
                } else {
                    func.eval(datum, env, temp_storage)
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let a = expr1.eval(datums, env, temp_storage);
                let b = expr2.eval(datums, env, temp_storage);
                if func.propagates_nulls() && (a.is_null() || b.is_null()) {
                    Datum::Null
                } else {
                    func.eval(a, b, env, temp_storage)
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
                    func.eval(&datums, env, temp_storage)
                }
            }
            ScalarExpr::If { cond, then, els } => match cond.eval(datums, env, temp_storage) {
                Datum::True => then.eval(datums, env, temp_storage),
                Datum::False | Datum::Null => els.eval(datums, env, temp_storage),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
        }
    }

    /// Converts this [`ScalarExpr`] to a document for pretty printing. See
    /// [`RelationExpr::to_doc`](crate::RelationExpr::to_doc) for details on the
    /// approach.
    pub fn to_doc<'a, A>(&'a self, alloc: &'a A) -> DocBuilder<'a, A>
    where
        A: DocAllocator<'a>,
        A::Doc: Clone,
    {
        use ScalarExpr::*;

        let needs_wrap = |expr: &ScalarExpr| match expr {
            CallUnary { func, .. } => match func {
                // `UnaryFunc::MatchRegex` renders as a binary function that
                // matches the embedded regex.
                UnaryFunc::MatchRegex(_) => true,
                _ => false,
            },
            CallBinary { .. } | If { .. } => true,
            Column(_) | Literal(_, _) | CallVariadic { .. } | CallNullary(_) => false,
        };

        let maybe_wrap = |expr| {
            if needs_wrap(expr) {
                expr.to_doc(alloc).tightly_embrace("(", ")")
            } else {
                expr.to_doc(alloc)
            }
        };

        match self {
            Column(n) => alloc.text("#").append(n.to_string()),
            Literal(..) => alloc.text(self.as_literal().unwrap().to_string()),
            CallNullary(func) => alloc.text(func.to_string()),
            CallUnary { func, expr } => {
                let mut doc = alloc.text(func.to_string());
                if !func.display_is_symbolic() && !needs_wrap(expr) {
                    doc = doc.append(" ");
                }
                doc.append(maybe_wrap(expr))
            }
            CallBinary { func, expr1, expr2 } => maybe_wrap(expr1)
                .group()
                .append(alloc.line())
                .append(func.to_string())
                .append(alloc.line())
                .append(maybe_wrap(expr2).group()),
            CallVariadic { func, exprs } => alloc.text(func.to_string()).append(
                alloc
                    .intersperse(
                        exprs.iter().map(|e| e.to_doc(alloc)),
                        alloc.text(",").append(alloc.line()),
                    )
                    .tightly_embrace("(", ")"),
            ),
            If { cond, then, els } => alloc
                .text("if")
                .append(alloc.line().append(cond.to_doc(alloc)).nest(2))
                .append(alloc.line())
                .append("then")
                .append(alloc.line().append(then.to_doc(alloc)).nest(2))
                .append(alloc.line())
                .append("else")
                .append(alloc.line().append(els.to_doc(alloc)).nest(2)),
        }
        .group()
    }
}

/// An evaluation environment. Stores state that controls how certain
/// expressions are evaluated.
#[derive(Default, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct EvalEnv {
    pub logical_time: Option<u64>,
    pub wall_time: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use pretty::RcDoc;

    use super::*;

    impl ScalarExpr {
        fn doc(&self) -> RcDoc {
            self.to_doc(&pretty::RcAllocator).into_doc()
        }
    }

    #[test]
    fn test_pretty_scalar_expr() {
        use ScalarType::*;
        let col_type = |st| ColumnType::new(st);
        let int64_lit = |n| ScalarExpr::literal(Datum::Int64(n), col_type(Int64));

        let plus_expr = int64_lit(1).call_binary(int64_lit(2), BinaryFunc::AddInt64);
        assert_eq!(plus_expr.doc().pretty(72).to_string(), "1 + 2");

        let regex_expr = ScalarExpr::literal(Datum::String("foo"), col_type(String)).call_binary(
            ScalarExpr::literal(Datum::String("f?oo"), col_type(String)),
            BinaryFunc::MatchRegex,
        );
        assert_eq!(regex_expr.doc().pretty(72).to_string(), r#""foo" ~ "f?oo""#);

        let neg_expr = int64_lit(1).call_unary(UnaryFunc::NegInt64);
        assert_eq!(neg_expr.doc().pretty(72).to_string(), "-1");

        let bool_expr = ScalarExpr::literal(Datum::True, col_type(Bool))
            .call_binary(
                ScalarExpr::literal(Datum::False, col_type(Bool)),
                BinaryFunc::And,
            )
            .call_unary(UnaryFunc::Not);
        assert_eq!(bool_expr.doc().pretty(72).to_string(), "!(true && false)");

        let cond_expr = ScalarExpr::if_then_else(
            ScalarExpr::literal(Datum::True, col_type(Bool)),
            neg_expr.clone(),
            plus_expr.clone(),
        );
        assert_eq!(
            cond_expr.doc().pretty(72).to_string(),
            "if true then -1 else 1 + 2"
        );

        let variadic_expr = ScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![ScalarExpr::Column(7), plus_expr, neg_expr.clone()],
        };
        assert_eq!(
            variadic_expr.doc().pretty(72).to_string(),
            "coalesce(#7, 1 + 2, -1)"
        );

        let mega_expr = ScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![cond_expr],
        }
        .call_binary(neg_expr, BinaryFunc::ModInt64)
        .call_unary(UnaryFunc::IsNull)
        .call_unary(UnaryFunc::Not)
        .call_binary(bool_expr, BinaryFunc::Or);
        assert_eq!(
            mega_expr.doc().pretty(72).to_string(),
            "!isnull(coalesce(if true then -1 else 1 + 2) % -1) || !(true && false)"
        );
        println!("{}", mega_expr.doc().pretty(64).to_string());
        assert_eq!(
            mega_expr.doc().pretty(64).to_string(),
            "!isnull(coalesce(if true then -1 else 1 + 2) % -1)
||
!(true && false)"
        );
        assert_eq!(
            mega_expr.doc().pretty(16).to_string(),
            "!isnull(
  coalesce(
    if
      true
    then
      -1
    else
      1 + 2
  )
  %
  -1
)
||
!(true && false)"
        );
    }
}
