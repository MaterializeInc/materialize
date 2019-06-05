// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use super::RelationExpr;
use crate::repr::RelationType;

pub trait Transform {
    /// Transform a relation into a functionally equivalent relation.
    ///
    /// Arguably the metadata *shouldn't* change, but we're new here.
    fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType);
}

pub use join_order::JoinOrder;
pub use predicate_pushdown::PredicatePushdown;

pub mod join_order {

    use crate::dataflow::RelationExpr;
    use crate::repr::RelationType;

    /// Re-order relations in a join to process them in an order that makes sense.
    ///
    /// ```rust
    /// use materialize::dataflow::RelationExpr;
    /// use materialize::repr::{ColumnType, RelationType, ScalarType};
    /// use materialize::dataflow::transform::JoinOrder;
    ///
    /// let input1 = RelationExpr::constant(vec![], RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    /// ]));
    /// let input2 = RelationExpr::constant(vec![], RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    /// ]));
    /// let input3 = RelationExpr::constant(vec![], RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    /// ]));
    /// let mut expr = RelationExpr::join(
    ///     vec![input1, input2, input3],
    ///     vec![vec![(0, 0), (2, 0)]],
    /// );
    /// let typ = RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    ///     ColumnType::new(ScalarType::Bool),
    ///     ColumnType::new(ScalarType::Bool),
    /// ]);
    ///
    /// JoinOrder.transform(&mut expr, &typ);
    ///
    /// if let RelationExpr::Project { input, outputs } = expr {
    ///     assert_eq!(outputs, vec![0, 2, 1]);
    /// }
    /// ```
    pub struct JoinOrder;

    impl super::Transform for JoinOrder {
        fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
            self.transform(relation, metadata)
        }
    }

    impl JoinOrder {
        pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            relation.visit_mut_inner(&mut |e| {
                self.action(e, &e.typ());
            });
        }
        pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            if let RelationExpr::Join { inputs, variables } = relation {
                let arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

                // Step 1: determine a relation_expr order starting from `inputs[0]`.
                let mut relation_expr_order = vec![0];
                while relation_expr_order.len() < inputs.len() {
                    let mut candidates = (0..inputs.len())
                        .filter(|i| !relation_expr_order.contains(i))
                        .map(|i| {
                            (
                                variables
                                    .iter()
                                    .filter(|vars| {
                                        vars.iter().any(|(idx, _)| &i == idx)
                                            && vars
                                                .iter()
                                                .any(|(idx, _)| relation_expr_order.contains(idx))
                                    })
                                    .count(),
                                i,
                            )
                        })
                        .collect::<Vec<_>>();

                    candidates.sort();
                    relation_expr_order.push(candidates.pop().expect("Candidate expected").1);
                }

                // Step 2: rewrite `variables`.
                let mut positions = vec![0; relation_expr_order.len()];
                for (index, input) in relation_expr_order.iter().enumerate() {
                    positions[*input] = index;
                }

                let mut new_variables = Vec::new();
                for variable in variables.iter() {
                    let mut new_set = Vec::new();
                    for (rel, col) in variable.iter() {
                        new_set.push((positions[*rel], *col));
                    }
                    new_variables.push(new_set);
                }

                // Step 3: prepare `Project`.
                // We want to present as if in the order we promised, so we need to permute.
                // In particular, for each (rel, col) in order, we want to figure out where
                // it lives in our weird local order, and build an expr that picks it out.
                let mut offset = 0;
                let mut offsets = vec![0; relation_expr_order.len()];
                for input in relation_expr_order.iter() {
                    offsets[*input] = offset;
                    offset += arities[*input];
                }

                let mut projection = Vec::new();
                for rel in 0..inputs.len() {
                    for col in 0..arities[rel] {
                        let position = offsets[rel] + col;
                        projection.push(position);
                    }
                }

                // Step 4: prepare output
                let mut new_inputs = Vec::new();
                for rel in relation_expr_order.into_iter() {
                    new_inputs.push(inputs[rel].clone()); // TODO: Extract from `inputs`.
                }

                let join = RelationExpr::Join {
                    inputs: new_inputs,
                    variables: new_variables,
                };

                // Output projection
                *relation = join.project(projection);
                // (output, metadata)
            }
            // else {
            //     (relation, metadata)
            // }
        }
    }
}

pub mod predicate_pushdown {

    /// Re-order relations in a join to process them in an order that makes sense.
    ///
    /// ```rust
    /// # use pretty_assertions::assert_eq;
    ///
    /// use materialize::dataflow::{RelationExpr, ScalarExpr};
    /// use materialize::dataflow::func::BinaryFunc;
    /// use materialize::dataflow::transform::PredicatePushdown;
    /// use materialize::repr::{ColumnType, Datum, RelationType, ScalarType};
    ///
    /// let input1 = RelationExpr::constant(vec![], RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    /// ]));
    /// let input2 = RelationExpr::constant(vec![], RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    /// ]));
    /// let input3 = RelationExpr::constant(vec![], RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    /// ]));
    /// let join = RelationExpr::join(
    ///     vec![input1.clone(), input2.clone(), input3.clone()],
    ///     vec![vec![(0, 0), (2, 0)].into_iter().collect()],
    /// );
    ///
    /// let predicate0 = ScalarExpr::column(0);
    /// let predicate1 = ScalarExpr::column(1);
    /// let predicate01 = ScalarExpr::column(0).call_binary(ScalarExpr::column(2), BinaryFunc::AddInt64);
    /// let predicate012 = ScalarExpr::literal(Datum::False);
    ///
    /// let mut expr = join.filter(
    ///    vec![
    ///        predicate0.clone(),
    ///        predicate1.clone(),
    ///        predicate01.clone(),
    ///        predicate012.clone(),
    ///    ]);
    ///
    /// let typ = RelationType::new(vec![
    ///     ColumnType::new(ScalarType::Bool),
    ///     ColumnType::new(ScalarType::Bool),
    ///     ColumnType::new(ScalarType::Bool),
    /// ]);
    ///
    /// PredicatePushdown.transform(&mut expr, &typ);
    ///
    /// let expected = RelationExpr::join(
    ///     vec![
    ///         input1.filter(vec![predicate0.clone(), predicate012.clone()]),
    ///         input2.filter(vec![predicate0.clone(), predicate012.clone()]),
    ///         input3.filter(vec![predicate012]),
    ///     ],
    ///     vec![vec![(0, 0), (2, 0)]],
    /// ).filter(vec![predicate01]);
    ///
    /// assert_eq!(expr, expected);
    /// ```
    use crate::dataflow::types::{RelationExpr, ScalarExpr};
    use crate::repr::RelationType;

    pub struct PredicatePushdown;

    impl PredicatePushdown {
        pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            relation.visit_mut_inner_pre(&mut |e| {
                self.action(e, &e.typ());
            });
        }
        pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
            if let RelationExpr::Filter { input, predicates } = relation {
                if let RelationExpr::Join { inputs, variables } = &mut **input {
                    // We want to scan `predicates` for any that can apply
                    // to individual elements of `inputs`.

                    let input_arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

                    let mut offset = 0;
                    let mut prior_arities = Vec::new();
                    for input in 0..inputs.len() {
                        prior_arities.push(offset);
                        offset += input_arities[input];
                    }

                    let input_relation = input_arities
                        .iter()
                        .enumerate()
                        .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                        .collect::<Vec<_>>();

                    // Predicates to push at each input, and to retain.
                    let mut push_downs = vec![Vec::new(); inputs.len()];
                    let mut retain = Vec::new();

                    for mut predicate in predicates.drain(..) {
                        // Determine the relation support of each predicate.
                        let mut support = Vec::new();
                        predicate.visit(|e| {
                            if let ScalarExpr::Column(i) = e {
                                support.push(input_relation[*i]);
                            }
                        });
                        support.sort();
                        support.dedup();

                        match support.len() {
                            0 => {
                                for push_down in push_downs.iter_mut() {
                                    // no support, so nothing to rewrite.
                                    push_down.push(predicate.clone());
                                }
                            }
                            1 => {
                                let relation = support[0];
                                predicate.visit_mut(|e| {
                                    // subtract
                                    if let ScalarExpr::Column(i) = e {
                                        *i -= prior_arities[relation];
                                    }
                                });
                                push_downs[relation].push(predicate);
                            }
                            _ => {
                                use crate::dataflow::func::BinaryFunc;
                                if let ScalarExpr::CallBinary {
                                    func: BinaryFunc::Eq,
                                    expr1,
                                    expr2,
                                } = &predicate
                                {
                                    if let (ScalarExpr::Column(c1), ScalarExpr::Column(c2)) =
                                        (&**expr1, &**expr2)
                                    {
                                        let relation1 = input_relation[*c1];
                                        let relation2 = input_relation[*c2];
                                        assert!(relation1 != relation2);
                                        let key1 = (relation1, *c1 - prior_arities[relation1]);
                                        let key2 = (relation2, *c2 - prior_arities[relation2]);
                                        let pos1 = variables.iter().position(|l| l.contains(&key1));
                                        let pos2 = variables.iter().position(|l| l.contains(&key2));
                                        match (pos1, pos2) {
                                            (None, None) => {
                                                variables.push(vec![key1, key2]);
                                            }
                                            (Some(idx1), None) => {
                                                variables[idx1].push(key2);
                                            }
                                            (None, Some(idx2)) => {
                                                variables[idx2].push(key1);
                                            }
                                            (Some(idx1), Some(idx2)) => {
                                                // assert!(idx1 != idx2);
                                                if idx1 != idx2 {
                                                    let temp = variables[idx2].clone();
                                                    variables[idx1].extend(temp);
                                                    variables[idx1].sort();
                                                    variables[idx1].dedup();
                                                    variables.remove(idx2);
                                                }
                                            }
                                        }
                                    } else {
                                        retain.push(predicate);
                                    }
                                } else {
                                    retain.push(predicate);
                                }
                            }
                        }
                    }

                    // promote same-relation constraints.
                    for variable in variables.iter_mut() {
                        variable.sort();
                        variable.dedup(); // <-- not obviously necessary.

                        let mut pos = 0;
                        while pos + 1 < variable.len() {
                            if variable[pos].0 == variable[pos + 1].0 {
                                use crate::dataflow::func::BinaryFunc;
                                push_downs[variable[pos].0].push(ScalarExpr::CallBinary {
                                    func: BinaryFunc::Eq,
                                    expr1: Box::new(ScalarExpr::Column(variable[pos].1)),
                                    expr2: Box::new(ScalarExpr::Column(variable[pos + 1].1)),
                                });
                                variable.remove(pos + 1);
                            } else {
                                pos += 1;
                            }
                        }
                    }

                    let new_inputs = inputs
                        .drain(..)
                        .zip(push_downs)
                        .enumerate()
                        .map(|(_index, (input, push_down))| {
                            if !push_down.is_empty() {
                                input.filter(push_down)
                            } else {
                                input
                            }
                        })
                        .collect();

                    *inputs = new_inputs;
                    *predicates = retain;
                }
            }
        }
    }
}

pub mod fusion {

    pub mod filter {

        /// Re-order relations in a join to process them in an order that makes sense.
        ///
        /// ```rust
        /// use materialize::dataflow::{RelationExpr, ScalarExpr};
        /// use materialize::dataflow::transform::fusion::filter::Filter;
        /// use materialize::repr::{ColumnType, Datum, RelationType, ScalarType};
        ///
        /// let input = RelationExpr::constant(vec![], RelationType::new(vec![
        ///     ColumnType::new(ScalarType::Bool),
        /// ]));
        ///
        /// let predicate0 = ScalarExpr::Column(0);
        /// let predicate1 = ScalarExpr::Column(0);
        /// let predicate2 = ScalarExpr::Column(0);
        ///
        /// let mut expr =
        /// input
        ///     .clone()
        ///     .filter(vec![predicate0.clone()])
        ///     .filter(vec![predicate1.clone()])
        ///     .filter(vec![predicate2.clone()]);
        ///
        /// let typ = RelationType::new(vec![
        ///     ColumnType::new(ScalarType::Bool),
        /// ]);
        ///
        /// Filter.transform(&mut expr, &typ);
        ///
        /// let correct = input.filter(vec![predicate0, predicate1, predicate2]);
        ///
        /// assert_eq!(expr, correct);
        /// ```
        use crate::dataflow::RelationExpr;
        use crate::repr::RelationType;

        pub struct Filter;

        impl crate::dataflow::transform::Transform for Filter {
            fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
                self.transform(relation, metadata)
            }
        }

        impl Filter {
            pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
                relation.visit_mut_inner_pre(&mut |e| {
                    self.action(e, &e.typ());
                });
            }
            pub fn action(&self, relation: &mut RelationExpr, metadata: &RelationType) {
                if let RelationExpr::Filter { input, predicates } = relation {
                    // consolidate nested filters.
                    while let RelationExpr::Filter {
                        input: inner,
                        predicates: p2,
                    } = &mut **input
                    {
                        predicates.extend(p2.drain(..));
                        let empty = Box::new(RelationExpr::Constant {
                            rows: vec![],
                            typ: metadata.to_owned(),
                        });
                        *input = std::mem::replace(inner, empty);
                    }

                    // remove the Filter stage if empty.
                    if predicates.is_empty() {
                        let empty = RelationExpr::Constant {
                            rows: vec![],
                            typ: metadata.to_owned(),
                        };
                        *relation = std::mem::replace(input, empty);
                    }
                }
            }
        }
    }

    pub mod join {

        use crate::dataflow::RelationExpr;
        use crate::repr::RelationType;

        pub struct Join;

        impl crate::dataflow::transform::Transform for Join {
            fn transform(&self, relation: &mut RelationExpr, metadata: &RelationType) {
                self.transform(relation, metadata)
            }
        }

        impl Join {
            pub fn transform(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
                relation.visit_mut_inner(&mut |e| {
                    self.action(e, &e.typ());
                });
            }

            pub fn action(&self, relation: &mut RelationExpr, _metadata: &RelationType) {
                if let RelationExpr::Join { inputs, variables } = relation {
                    let mut new_inputs = Vec::new();
                    let mut new_variables = Vec::new();

                    let mut new_relation = Vec::new();

                    for input in inputs.drain(..) {
                        let mut columns = Vec::new();
                        if let RelationExpr::Join {
                            mut inputs,
                            mut variables,
                        } = input
                        {
                            // Update and push all of the variables.
                            for mut variable in variables.drain(..) {
                                for (rel, _col) in variable.iter_mut() {
                                    *rel += new_inputs.len();
                                }
                                new_variables.push(variable);
                            }
                            // Add all of the inputs.
                            for input in inputs.drain(..) {
                                let new_inputs_len = new_inputs.len();
                                let arity = input.arity();
                                columns.extend((0..arity).map(|c| (new_inputs_len, c)));
                                new_inputs.push(input);
                            }
                        } else {
                            // Retain the input.
                            let new_inputs_len = new_inputs.len();
                            let arity = input.arity();
                            columns.extend((0..arity).map(|c| (new_inputs_len, c)));
                            new_inputs.push(input);
                        }
                        new_relation.push(columns);
                    }

                    for mut variable in variables.drain(..) {
                        for (rel, col) in variable.iter_mut() {
                            let (rel2, col2) = new_relation[*rel][*col];
                            *rel = rel2;
                            *col = col2;
                        }
                        new_variables.push(variable);
                    }

                    *inputs = new_inputs;
                    *variables = new_variables;
                }
            }
        }
    }
}
