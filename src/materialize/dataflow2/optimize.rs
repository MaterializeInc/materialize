use std::collections::{HashMap, HashSet};

use crate::repr::*;

use super::types::*;

impl ScalarExpr {
    fn visit1<F>(&mut self, f: &mut F)
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

    pub fn visit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1(&mut |e| e.visit(f));
        f(self);
    }

    fn permute(&mut self, permutation: &HashMap<usize, usize>) {
        self.visit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    fn support(&mut self) -> HashSet<usize> {
        let mut support = HashSet::new();
        self.visit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }
}

impl RelationExpr {
    fn visit1<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
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

    fn visit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit1(&mut |e| e.visit(f));
        f(self)
    }

    fn push_down_projects_with(self, outputs: &[usize]) -> Self {
        match self {
            RelationExpr::Constant { mut rows, typ } => {
                for row in rows.iter_mut() {
                    *row = outputs.iter().map(|&i| row[i].clone()).collect();
                }
                let typ = outputs.iter().map(|&i| typ[i].clone()).collect();
                RelationExpr::Constant { rows, typ }
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
                other.visit1(&mut |e| {
                    let owned = std::mem::replace(
                        e,
                        // dummy value
                        RelationExpr::Constant {
                            rows: vec![],
                            typ: vec![],
                        },
                    );
                    *e = owned.push_down_projects()
                });
                other
            }
        }
    }
}
