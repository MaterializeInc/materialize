// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::collections::{HashMap, HashSet};

use crate::repr::*;

use super::types::*;

impl ScalarExpr {
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
    /// use materialize::dataflow::ScalarExpr;
    /// use materialize::dataflow::func::BinaryFunc;
    /// use materialize::repr::Datum;
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
}

impl RelationExpr {
    pub fn visit1<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value);
                f(body);
            }
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::OrDefault { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Distinct { input } => f(input),
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
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
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => (),
            RelationExpr::Let { value, body, .. } => {
                f(value);
                f(body);
            }
            RelationExpr::Project { input, .. } => {
                f(input);
            }
            RelationExpr::Map { input, .. } => {
                f(input);
            }
            RelationExpr::Filter { input, .. } => {
                f(input);
            }
            RelationExpr::Join { inputs, .. } => {
                for input in inputs {
                    f(input);
                }
            }
            RelationExpr::Reduce { input, .. } => {
                f(input);
            }
            RelationExpr::TopK { input, .. } => {
                f(input);
            }
            RelationExpr::OrDefault { input, .. } => {
                f(input);
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Distinct { input } => f(input),
            RelationExpr::Union { left, right } => {
                f(left);
                f(right);
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

    pub fn visit_mut_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_pre(f));
    }
}
