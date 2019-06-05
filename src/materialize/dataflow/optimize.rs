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

    fn visit_inner<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit_inner(f));
        f(self);
    }

    pub fn visit<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        self.visit_inner(&mut f);
    }

    pub fn visit1_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
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

    fn visit_mut_inner<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut_inner(f));
        f(self);
    }

    pub fn visit_mut<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        self.visit_mut_inner(&mut f);
    }

    fn permute(&mut self, permutation: &HashMap<usize, usize>) {
        self.visit_mut(|e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&mut self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit_mut(|e| {
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
        self.visit_mut(|e| {
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

    fn visit_inner<'a, F>(&'a self, f: &mut F)
    where
        F: FnMut(&'a Self),
    {
        self.visit1(|e| e.visit_inner(f));
        f(self);
    }

    pub fn visit<'a, F>(&'a self, mut f: F)
    where
        F: FnMut(&'a Self),
    {
        self.visit_inner(&mut f)
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

    #[allow(dead_code)]
    pub fn visit_mut_inner<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1_mut(|e| e.visit_mut_inner(f));
        f(self)
    }

    #[allow(dead_code)]
    pub fn visit_mut_inner_pre<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        f(self);
        self.visit1_mut(|e| e.visit_mut_inner_pre(f));
    }

    #[allow(dead_code)]
    fn push_down_projects_with(self, outputs: &[usize]) -> Self {
        match self {
            RelationExpr::Constant { mut rows, typ } => {
                for row in rows.iter_mut() {
                    *row = outputs.iter().map(|&i| row[i].clone()).collect();
                }
                let column_types = outputs
                    .iter()
                    .map(|&i| typ.column_types[i].clone())
                    .collect();
                RelationExpr::Constant {
                    rows,
                    typ: RelationType { column_types },
                }
            }
            get @ RelationExpr::Get { .. } => {
                let input = Box::new(get.push_down_projects());
                RelationExpr::Project {
                    input,
                    outputs: outputs.to_vec(),
                }
            }
            RelationExpr::Let { name, value, body } => {
                let value = Box::new(value.push_down_projects());
                let body = Box::new(body.push_down_projects_with(outputs));
                RelationExpr::Let { name, value, body }
            }
            RelationExpr::Project {
                input,
                outputs: inner_outputs,
            } => {
                let outputs = outputs
                    .iter()
                    .map(|&i| inner_outputs[i])
                    .collect::<Vec<_>>();
                input.push_down_projects_with(&outputs)
            }
            RelationExpr::Map { input, scalars } => {
                // TODO check for support of scalars - have to keep columns they need and wrap in a new Project
                let arity = input.arity();
                let inner_outputs = outputs
                    .iter()
                    .cloned()
                    .filter(|&i| i < arity)
                    .collect::<Vec<_>>();
                let input = Box::new(input.push_down_projects_with(&inner_outputs));
                let permutation = inner_outputs
                    .into_iter()
                    .enumerate()
                    .map(|(new_i, old_i)| (old_i, new_i))
                    .collect::<HashMap<_, _>>();
                let scalars = outputs
                    .iter()
                    .filter(|&&i| i >= arity)
                    .map(|&i| {
                        let (mut scalar, typ) = scalars[i - arity].clone();
                        scalar.permute(&permutation);
                        (scalar, typ)
                    })
                    .collect();
                RelationExpr::Map { input, scalars }
            }
            _ => unimplemented!(),
        }
    }

    fn push_down_projects(self) -> Self {
        match self {
            RelationExpr::Project { input, outputs } => input.push_down_projects_with(&outputs),
            mut other => {
                other.visit1_mut(|e| {
                    let owned = std::mem::replace(
                        e,
                        // dummy value
                        RelationExpr::Constant {
                            rows: vec![],
                            typ: RelationType {
                                column_types: vec![],
                            },
                        },
                    );
                    *e = owned.push_down_projects()
                });
                other
            }
        }
    }
}
