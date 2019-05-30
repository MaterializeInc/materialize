use std::collections::HashMap;

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

    fn visit<F>(&mut self, f: &mut F)
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
}

impl RelationExpr {
    fn identity_permutation(&self) -> HashMap<usize, usize> {
        (0..self.arity()).map(|i| (i, i)).collect()
    }

    fn visit1<F>(&mut self, f: &mut F) -> HashMap<usize, usize>
    where
        F: FnMut(&mut Self) -> HashMap<usize, usize>,
    {
        match self {
            RelationExpr::Constant { .. } | RelationExpr::Get { .. } => self.identity_permutation(),
            RelationExpr::Let { value, body, .. } => {
                let old_value_arity = value.arity();
                let value_permutation = f(value);
                // value might be used in multiple places, so let's not allow rewrites to change it's output
                for old_i in 0..old_value_arity {
                    assert_eq!(old_i, value_permutation[&old_i]);
                }
                let body_permutation = f(body);
                body_permutation
            }
            RelationExpr::Project { input, outputs } => {
                let input_permutation = f(input);
                for old_i in outputs {
                    *old_i = input_permutation[old_i]
                }
                self.identity_permutation()
            }
            RelationExpr::Map { input, scalars } => {
                let old_arity = input.arity();
                let input_permutation = f(input);
                let new_arity = input.arity();
                for (scalar, _) in scalars.iter_mut() {
                    scalar.permute(&input_permutation);
                }
                let mut permutation = input_permutation;
                for i in 0..scalars.len() {
                    permutation.insert(old_arity + i, new_arity + i);
                }
                permutation
            }
            RelationExpr::Filter { input, predicates } => {
                let input_permutation = f(input);
                for predicate in predicates {
                    predicate.permute(&input_permutation);
                }
                input_permutation
            }
            RelationExpr::Join { inputs, variables } => {
                let old_arities = inputs.iter().map(|input| input.arity()).collect::<Vec<_>>();
                let inputs_permutation =
                    inputs.iter_mut().map(|input| f(input)).collect::<Vec<_>>();
                for variable in variables {
                    *variable = variable
                        .iter()
                        .map(|(input_ix, column_ix)| {
                            (*input_ix, inputs_permutation[*input_ix][column_ix])
                        })
                        .collect()
                }
                let mut total_old_arity = 0;
                let mut total_new_arity = 0;
                let mut permutation = HashMap::new();
                for ((input, input_permutation), old_arity) in inputs
                    .iter()
                    .zip(inputs_permutation.iter())
                    .zip(old_arities)
                {
                    for old_i in 0..old_arity {
                        let new_i = input_permutation[&old_i];
                        permutation.insert(total_old_arity + old_i, total_new_arity + new_i);
                    }
                    total_old_arity += old_arity;
                    total_new_arity += input.arity();
                }
                permutation
            }
            RelationExpr::Reduce {
                input, group_key, ..
            } => {
                let input_permutation = f(input);
                for old_i in group_key {
                    *old_i = input_permutation[&old_i];
                }
                self.identity_permutation()
            }
            RelationExpr::OrDefault { input, default } => {
                let old_arity = input.arity();
                let input_permutation = f(input);
                let mut new_default = (0..input.arity()).map(|_| Datum::Null).collect::<Vec<_>>();
                for old_i in 0..old_arity {
                    let new_i = input_permutation[&old_i];
                    new_default[new_i] = default[old_i].clone();
                }
                *default = new_default;
                input_permutation
            }
            RelationExpr::Negate { input } => f(input),
            RelationExpr::Distinct { input } => f(input),
            RelationExpr::Union { left, right } => {
                let old_left_arity = left.arity();
                let left_permutation = f(left);
                let right_permutation = f(right);
                let new_left_arity = left.arity();
                let mut permutation = left_permutation;
                for (old_i, new_i) in right_permutation {
                    permutation.insert(old_left_arity + old_i, new_left_arity + new_i);
                }
                permutation
            }
        }
    }

    fn visit<F>(&mut self, f: &mut F) -> HashMap<usize, usize>
    where
        F: FnMut(&mut Self) -> HashMap<usize, usize>,
    {
        let old_arity = self.arity();
        let permutation1 = self.visit1(f);
        let permutation2 = f(self);
        let mut permutation = HashMap::new();
        for old_i in 0..old_arity {
            permutation.insert(old_i, permutation2[&permutation1[&old_i]]);
        }
        permutation
    }
}
