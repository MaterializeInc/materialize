use crate::dataflow2::types::{RelationExpr, RelationType};

pub trait Transform {
    /// Transform a relation into a functionally equivalent relation.
    ///
    /// Arguably the metadata *shouldn't* change, but we're new here.
    fn transform(
        &self,
        relation: RelationExpr,
        metadata: RelationType,
    ) -> (RelationExpr, RelationType);
}

pub use join_order::JoinOrder;
pub use predicate_pushdown::PredicatePushdown;

pub mod join_order {

    use crate::dataflow2::types::{RelationExpr, RelationType};

    /// Re-order relations in a join to process them in an order that makes sense.
    ///
    /// ```rust
    /// use materialize::dataflow2::RelationExpr;
    /// use materialize::dataflow2::ColumnType;
    /// use materialize::repr::FType;
    ///
    /// let input1 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
    /// let input2 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
    /// let input3 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
    /// let join = RelationExpr::Join {
    ///     inputs: vec![input1, input2, input3],
    ///     variables: vec![vec![(0,0),(2,0)].into_iter().collect()],
    /// };
    /// let typ = vec![
    ///     ColumnType { typ: FType::Bool, is_nullable: false },
    ///     ColumnType { typ: FType::Bool, is_nullable: false },
    ///     ColumnType { typ: FType::Bool, is_nullable: false },
    /// ];
    ///
    /// let join_order = materialize::dataflow2::transform::JoinOrder;
    /// let (opt_rel, opt_typ) = join_order.transform(join, typ);
    ///
    /// if let RelationExpr::Project { input, outputs } = opt_rel {
    ///     assert_eq!(outputs, vec![0, 2, 1]);
    /// }
    /// ```
    pub struct JoinOrder;

    impl super::Transform for JoinOrder {
        fn transform(
            &self,
            relation: RelationExpr,
            metadata: RelationType,
        ) -> (RelationExpr, RelationType) {
            self.transform(relation, metadata)
        }
    }

    impl JoinOrder {
        pub fn transform(
            &self,
            relation: RelationExpr,
            metadata: RelationType,
        ) -> (RelationExpr, RelationType) {
            if let RelationExpr::Join { inputs, variables } = relation {
                let arities = inputs.iter().map(|i| i.arity()).collect::<Vec<_>>();

                // Step 1: determine a plan order starting from `inputs[0]`.
                let mut plan_order = vec![0];
                while plan_order.len() < inputs.len() {
                    let mut candidates = (0..inputs.len())
                        .filter(|i| !plan_order.contains(i))
                        .map(|i| {
                            (
                                variables
                                    .iter()
                                    .filter(|vars| {
                                        vars.iter().any(|(idx, _)| &i == idx)
                                            && vars.iter().any(|(idx, _)| plan_order.contains(idx))
                                    })
                                    .count(),
                                i,
                            )
                        })
                        .collect::<Vec<_>>();

                    candidates.sort();
                    plan_order.push(candidates.pop().expect("Candidate expected").1);
                }

                // Step 2: rewrite `variables`.
                let mut positions = vec![0; plan_order.len()];
                for (index, input) in plan_order.iter().enumerate() {
                    positions[*input] = index;
                }

                let mut new_variables = Vec::new();
                for variable in variables.iter() {
                    let mut new_set = std::collections::HashSet::new();
                    for (rel, col) in variable.iter() {
                        new_set.insert((positions[*rel], *col));
                    }
                    new_variables.push(new_set);
                }

                // Step 3: prepare `Project`.
                // We want to present as if in the order we promised, so we need to permute.
                // In particular, for each (rel, col) in order, we want to figure out where
                // it lives in our weird local order, and build an expr that picks it out.
                let mut offset = 0;
                let mut offsets = vec![0; plan_order.len()];
                for input in plan_order.iter() {
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
                for rel in plan_order.into_iter() {
                    new_inputs.push(inputs[rel].clone()); // TODO: Extract from `inputs`.
                }

                let join = RelationExpr::Join {
                    inputs: new_inputs,
                    variables: new_variables,
                };

                // Output projection
                let output = join.project(projection);
                (output, metadata)
            } else {
                (relation, metadata)
            }
        }
    }
}

pub mod predicate_pushdown {

    /// Re-order relations in a join to process them in an order that makes sense.
    ///
    /// ```rust
    /// use materialize::dataflow2::{RelationExpr, ScalarExpr};
    /// use materialize::dataflow2::ColumnType;
    /// use materialize::repr::{Datum, FType};
    ///
    /// let input1 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
    /// let input2 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
    /// let input3 = RelationExpr::Constant { rows: vec![], typ: vec![ColumnType { typ: FType::Bool, is_nullable: false }] };
    /// let join = RelationExpr::Join {
    ///     inputs: vec![input1.clone(), input2.clone(), input3.clone()],
    ///     variables: vec![vec![(0,0),(2,0)].into_iter().collect()],
    /// };
    ///
    /// let predicate0 = ScalarExpr::Column(0);
    /// let predicate1 = ScalarExpr::Column(1);
    /// let predicate01 = ScalarExpr::CallBinary {
    ///     func: materialize::dataflow::func::BinaryFunc::Eq,
    ///     expr1: Box::new(ScalarExpr::Column(0)),
    ///     expr2: Box::new(ScalarExpr::Column(1)),
    /// };
    /// let predicate012 = ScalarExpr::Literal(Datum::False);
    ///
    /// let filter = join.filter(
    ///    vec![
    ///        predicate0.clone(),
    ///        predicate1.clone(),
    ///        predicate01.clone(),
    ///        predicate012.clone(),
    ///    ]);
    ///
    /// let typ = vec![
    ///     ColumnType { typ: FType::Bool, is_nullable: false },
    ///     ColumnType { typ: FType::Bool, is_nullable: false },
    ///     ColumnType { typ: FType::Bool, is_nullable: false },
    /// ];
    ///
    /// let pushdown = materialize::dataflow2::transform::PredicatePushdown;
    /// let (opt_rel, opt_typ) = pushdown.transform(filter, typ);
    ///
    /// let join = RelationExpr::Join {
    ///     inputs: vec![
    ///         input1.filter(vec![predicate0.clone(), predicate012.clone()]),
    ///         input2.filter(vec![predicate0.clone(), predicate012.clone()]),
    ///         input3.filter(vec![predicate012]),
    ///     ],
    ///     variables: vec![vec![(0,0),(2,0)].into_iter().collect()],
    /// };
    ///
    /// assert_eq!(opt_rel, join.filter(vec![predicate01]));
    /// ```
    use crate::dataflow2::types::{RelationExpr, RelationType, ScalarExpr};

    pub struct PredicatePushdown;

    impl PredicatePushdown {
        pub fn transform(
            &self,
            relation: RelationExpr,
            metadata: RelationType,
        ) -> (RelationExpr, RelationType) {
            if let RelationExpr::Filter { input, predicates } = relation {
                match *input {
                    RelationExpr::Join {
                        mut inputs,
                        variables,
                    } => {
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

                        for mut predicate in predicates.into_iter() {
                            // Determine the relation support of each predicate.
                            let mut support = Vec::new();
                            predicate.visit(&mut |e| {
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
                                    predicate.visit(&mut |e| {
                                        // subtract
                                        if let ScalarExpr::Column(i) = e {
                                            *i -= prior_arities[relation];
                                        }
                                    });
                                    push_downs[relation].push(predicate);
                                }
                                _ => {
                                    retain.push(predicate);
                                }
                            }
                        }

                        let inputs = inputs
                            .into_iter()
                            .zip(push_downs)
                            .enumerate()
                            .map(|(index, (input, push_down))| {
                                if !push_down.is_empty() {
                                    input.filter(push_down)
                                } else {
                                    input
                                }
                            })
                            .collect();

                        let mut result = RelationExpr::Join { inputs, variables };

                        if !retain.is_empty() {
                            result = result.filter(retain);
                        }

                        (result, metadata)
                    }
                    // fail out to reforming the filter
                    input => (input.filter(predicates), metadata),
                }
            } else {
                (relation, metadata)
            }
        }
    }

}
