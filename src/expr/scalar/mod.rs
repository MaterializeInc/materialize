// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub mod func;

use pretty::{BoxDoc, Doc};
use repr::regex::Regex;
use repr::{ColumnType, Datum, RelationType, Row, ScalarType};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::mem;

use self::func::{BinaryFunc, UnaryFunc, VariadicFunc};
use crate::pretty_pretty::to_tightly_braced_doc;

#[serde(rename_all = "snake_case")]
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// (Stored as a row, because we can't own a Datum)
    Literal(Row, ColumnType),
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
    /// A call to BinaryFunc::MatchRegex for which we have precompiled the regex
    MatchCachedRegex { expr: Box<ScalarExpr>, regex: Regex },
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
            ScalarExpr::MatchCachedRegex { expr, .. } => {
                f(expr);
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
            ScalarExpr::MatchCachedRegex { expr, .. } => {
                f(expr);
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

    pub fn support(&mut self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit_mut(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn take(&mut self) -> Self {
        mem::replace(
            self,
            ScalarExpr::Literal(Row::pack(&[Datum::Null]), ColumnType::new(ScalarType::Null)),
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
    /// use expr::{BinaryFunc, ScalarExpr};
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
    /// test.reduce();
    /// assert_eq!(test, expr_t);
    /// ```
    pub fn reduce(&mut self) {
        let empty = RelationType::new(vec![]);
        let eval = |e: &ScalarExpr| ScalarExpr::literal(e.eval(&[]), e.typ(&empty));
        self.visit_mut(&mut |e| match e {
            ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) => (),
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
                    *e = match expr2.eval(&[]) {
                        Datum::Null => {
                            ScalarExpr::literal(Datum::Null, ColumnType::new(ScalarType::Null))
                        }
                        Datum::String(string) => {
                            match func::build_like_regex_from_string(&string) {
                                Ok(regex) => ScalarExpr::MatchCachedRegex {
                                    expr: Box::new(expr1.take()),
                                    regex: Regex(regex),
                                },
                                Err(_) => ScalarExpr::literal(
                                    Datum::Null,
                                    ColumnType::new(ScalarType::Null),
                                ),
                            }
                        }
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
                    match cond.eval(&[]) {
                        Datum::True if then.is_literal() => *e = eval(then),
                        Datum::False | Datum::Null if els.is_literal() => *e = eval(els),
                        Datum::True | Datum::False | Datum::Null => (),
                        _ => unreachable!(),
                    }
                }
            }
            ScalarExpr::MatchCachedRegex { expr, .. } => {
                if expr.is_literal() {
                    *e = eval(e);
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
            ScalarExpr::CallUnary { func, expr } => {
                if func != &UnaryFunc::IsNull {
                    expr.non_null_requirements(columns);
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                if func != &BinaryFunc::Or {
                    expr1.non_null_requirements(columns);
                    expr2.non_null_requirements(columns);
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                if func != &VariadicFunc::Coalesce {
                    for expr in exprs {
                        expr.non_null_requirements(columns);
                    }
                }
            }
            ScalarExpr::If {
                cond,
                then: _,
                els: _,
            } => {
                cond.non_null_requirements(columns);
            }
            ScalarExpr::MatchCachedRegex { expr, .. } => {
                expr.non_null_requirements(columns);
            }
        }
    }

    pub fn typ(&self, relation_type: &RelationType) -> ColumnType {
        match self {
            ScalarExpr::Column(i) => relation_type.column_types[*i],
            ScalarExpr::Literal(_, typ) => *typ,
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
                if then_type.scalar_type != ScalarType::Null {
                    then_type.nullable(nullable)
                } else {
                    else_type.nullable(nullable)
                }
            }
            ScalarExpr::MatchCachedRegex { expr, .. } => {
                let nullable = expr.typ(relation_type).nullable;
                ColumnType::new(ScalarType::Bool).nullable(nullable)
            }
        }
    }

    pub fn eval<'a>(&'a self, datums: &[Datum<'a>]) -> Datum<'a> {
        match self {
            ScalarExpr::Column(index) => datums[*index].clone(),
            ScalarExpr::Literal(row, _column_type) => row.unpack_first(),
            ScalarExpr::CallUnary { func, expr } => {
                let eval = expr.eval(datums);
                if func.propagates_nulls() && eval.is_null() {
                    Datum::Null
                } else {
                    (func.func())(eval)
                }
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let eval1 = expr1.eval(datums);
                let eval2 = expr2.eval(datums);
                if func.propagates_nulls() && (eval1.is_null() || eval2.is_null()) {
                    Datum::Null
                } else {
                    (func.func())(eval1, eval2)
                }
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                let evals = exprs.iter().map(|e| e.eval(datums)).collect::<Vec<_>>();
                if func.propagates_nulls() && evals.iter().any(|e| e.is_null()) {
                    Datum::Null
                } else {
                    (func.func())(&evals)
                }
            }
            ScalarExpr::If { cond, then, els } => match cond.eval(datums) {
                Datum::True => then.eval(datums),
                Datum::False | Datum::Null => els.eval(datums),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
            ScalarExpr::MatchCachedRegex { expr, regex } => {
                let eval = expr.eval(datums);
                func::match_cached_regex(eval, regex)
            }
        }
    }

    /// Converts this [`ScalarExpr`] to a [`Doc`] or document for pretty
    /// printing. See [`RelationExpr::to_doc`](crate::RelationExpr::to_doc)
    /// for details on the approach.
    pub fn to_doc(&self) -> Doc<BoxDoc<()>> {
        use ScalarExpr::*;

        fn needs_wrap(expr: &ScalarExpr) -> bool {
            match expr {
                Column(_) | Literal(_, _) | CallUnary { .. } | CallVariadic { .. } => false,
                CallBinary { .. } | If { .. } | MatchCachedRegex { .. } => true,
            }
        }

        fn maybe_wrap(expr: &ScalarExpr) -> Doc<BoxDoc<()>> {
            if needs_wrap(expr) {
                to_tightly_braced_doc("(", expr, ")")
            } else {
                expr.to_doc()
            }
        };

        match self {
            Column(n) => to_doc!("#", n.to_string()),
            Literal(..) => self.as_literal().unwrap().into(),
            CallUnary { func, expr } => {
                let mut doc = Doc::from(func);
                if !func.display_is_symbolic() && !needs_wrap(expr) {
                    doc = doc.append(" ");
                }
                doc.append(maybe_wrap(expr))
            }
            CallBinary { func, expr1, expr2 } => to_doc!(
                maybe_wrap(expr1).group(),
                Doc::space(),
                func,
                Doc::space(),
                maybe_wrap(expr2).group()
            ),
            CallVariadic { func, exprs } => to_doc!(
                func,
                to_tightly_braced_doc(
                    "(",
                    Doc::intersperse(exprs, to_doc!(",", Doc::space())),
                    ")"
                )
            ),
            If { cond, then, els } => to_doc!(
                "if",
                to_doc!(Doc::space(), cond.to_doc()).nest(2),
                Doc::space(),
                "then",
                to_doc!(Doc::space(), then.to_doc()).nest(2),
                Doc::space(),
                "else",
                to_doc!(Doc::space(), els.to_doc()).nest(2)
            ),
            ScalarExpr::MatchCachedRegex { expr, regex } => to_doc!(
                maybe_wrap(expr).group(),
                Doc::space(),
                &BinaryFunc::MatchRegex,
                Doc::space(),
                regex.as_str()
            ),
        }
        .group()
    }
}

impl<'a> From<&'a ScalarExpr> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(s: &'a ScalarExpr) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        s.to_doc()
    }
}

impl<'a> From<&'a Box<ScalarExpr>> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(s: &'a Box<ScalarExpr>) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        s.to_doc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pretty_scalar_expr() {
        use ScalarType::*;
        let col_type = |st| ColumnType::new(st);
        let int64_lit = |n| ScalarExpr::literal(Datum::Int64(n), col_type(Int64));

        let plus_expr = int64_lit(1).call_binary(int64_lit(2), BinaryFunc::AddInt64);
        assert_eq!(plus_expr.to_doc().pretty(72).to_string(), "1 + 2");

        let regex_expr = ScalarExpr::literal(Datum::cow_from_str("foo"), col_type(String))
            .call_binary(
                ScalarExpr::literal(Datum::cow_from_str("f?oo"), col_type(String)),
                BinaryFunc::MatchRegex,
            );
        assert_eq!(
            regex_expr.to_doc().pretty(72).to_string(),
            r#""foo" ~ "f?oo""#
        );

        let neg_expr = int64_lit(1).call_unary(UnaryFunc::NegInt64);
        assert_eq!(neg_expr.to_doc().pretty(72).to_string(), "-1");

        let bool_expr = ScalarExpr::literal(Datum::True, col_type(Bool))
            .call_binary(
                ScalarExpr::literal(Datum::False, col_type(Bool)),
                BinaryFunc::And,
            )
            .call_unary(UnaryFunc::Not);
        assert_eq!(
            bool_expr.to_doc().pretty(72).to_string(),
            "!(true && false)"
        );

        let cond_expr = ScalarExpr::if_then_else(
            ScalarExpr::literal(Datum::True, col_type(Bool)),
            neg_expr.clone(),
            plus_expr.clone(),
        );
        assert_eq!(
            cond_expr.to_doc().pretty(72).to_string(),
            "if true then -1 else 1 + 2"
        );

        let variadic_expr = ScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce,
            exprs: vec![ScalarExpr::Column(7), plus_expr, neg_expr.clone()],
        };
        assert_eq!(
            variadic_expr.to_doc().pretty(72).to_string(),
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
            mega_expr.to_doc().pretty(72).to_string(),
            "!isnull(coalesce(if true then -1 else 1 + 2) % -1) || !(true && false)"
        );
        println!("{}", mega_expr.to_doc().pretty(64).to_string());
        assert_eq!(
            mega_expr.to_doc().pretty(64).to_string(),
            "!isnull(coalesce(if true then -1 else 1 + 2) % -1)
||
!(true && false)"
        );
        assert_eq!(
            mega_expr.to_doc().pretty(16).to_string(),
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
        )
    }
}
