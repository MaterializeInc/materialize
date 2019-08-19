// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

pub mod func;

use repr::Datum;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

use self::func::{BinaryFunc, UnaryFunc, VariadicFunc};

#[serde(rename_all = "snake_case")]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    Literal(Datum),
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

    pub fn literal(datum: Datum) -> Self {
        ScalarExpr::Literal(datum)
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
            ScalarExpr::Literal(_) => (),
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
            ScalarExpr::Literal(_) => (),
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

    #[allow(dead_code)]
    fn permute(&mut self, permutation: &HashMap<usize, usize>) {
        self.visit_mut(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
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

    fn is_literal(&self) -> bool {
        if let ScalarExpr::Literal(_) = self {
            true
        } else {
            false
        }
    }

    /// Reduces a complex expression where possible.
    ///
    /// ```rust
    /// use expr::{BinaryFunc, ScalarExpr};
    /// use repr::Datum;
    ///
    /// let expr_0 = ScalarExpr::Column(0);
    /// let expr_t = ScalarExpr::Literal(Datum::True);
    /// let expr_f = ScalarExpr::Literal(Datum::False);
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
        self.visit_mut(&mut |e| {
            let should_eval = match e {
                ScalarExpr::CallUnary { expr, .. } => expr.is_literal(),
                ScalarExpr::CallBinary { expr1, expr2, .. } => {
                    expr1.is_literal() && expr2.is_literal()
                }
                ScalarExpr::CallVariadic { exprs, .. } => exprs.iter().all(|e| e.is_literal()),
                ScalarExpr::If { cond, then, els } => {
                    if cond.is_literal() {
                        let eval = cond.eval(&[]);
                        if eval == Datum::True {
                            then.is_literal()
                        } else if eval == Datum::False || eval == Datum::Null {
                            els.is_literal()
                        } else {
                            unreachable!()
                        }
                    } else {
                        false
                    }
                }
                _ => false,
            };

            if should_eval {
                *e = ScalarExpr::Literal(e.eval(&[]));
            }
        });
    }

    pub fn eval(&self, data: &[Datum]) -> Datum {
        match self {
            ScalarExpr::Column(index) => data[*index].clone(),
            ScalarExpr::Literal(datum) => datum.clone(),
            ScalarExpr::CallUnary { func, expr } => {
                let eval = expr.eval(data);
                (func.func())(eval)
            }
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                let eval1 = expr1.eval(data);
                let eval2 = expr2.eval(data);
                (func.func())(eval1, eval2)
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                let evals = exprs.iter().map(|e| e.eval(data)).collect();
                (func.func())(evals)
            }
            ScalarExpr::If { cond, then, els } => match cond.eval(data) {
                Datum::True => then.eval(data),
                Datum::False | Datum::Null => els.eval(data),
                d => panic!("IF condition evaluated to non-boolean datum {:?}", d),
            },
        }
    }
}
