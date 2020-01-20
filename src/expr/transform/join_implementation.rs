// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Clippy's cognitive complexity is easy to reach.
//#![allow(clippy::cognitive_complexity)]

//! Transformations that allow join to make use of indexes.
//!
//! This is mostly a proof-of-concept that indexes work. The transformations in this module
//! may or may not belong together. Also, the transformations are subject to change as indexes
//! become more advanced.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, RelationExpr, ScalarExpr};

/// Determines the join implementation for join operators.
///
/// This includes determining the type of join (e.g. differential linear, or delta queries),
/// determining the orders of collections, lifting predicates if useful arrangements exist,
/// and identifying opportunities to use indexes to replace filters.
#[derive(Debug)]
pub struct JoinImplementation;

impl super::Transform for JoinImplementation {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation, indexes);
    }
}

impl JoinImplementation {
    pub fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) {
        relation.visit_mut(&mut |e| {
            self.action(e, indexes);
        });
    }

    pub fn action(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) {
        if let RelationExpr::Join { inputs, .. } = relation {
            // Common information of broad utility.
            let types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
            let arities = types
                .iter()
                .map(|t| t.column_types.len())
                .collect::<Vec<_>>();

            let mut offset = 0;
            let mut prior_arities = Vec::new();
            for input in 0..inputs.len() {
                prior_arities.push(offset);
                offset += arities[input];
            }

            // The first fundamental question is whether we should employ a delta query or not.
            //
            // Here we conservatively use the rule that if sufficient arrangements exist we will
            // use a delta query. An arrangement is considered available if it is a global get
            // with columns present in `indexes`, if it is an `ArrangeBy` with the columns present,
            // or a filter wrapped around either of these.

            let unique_keys = types.iter().map(|t| t.keys.clone()).collect::<Vec<_>>();
            let mut available_arrangements = vec![Vec::new(); inputs.len()];
            for index in 0..inputs.len() {
                // We can work around filters, as we can lift the predicates into the join execution.
                let mut input = &mut inputs[index];
                while let RelationExpr::Filter {
                    input: inner,
                    predicates: _,
                } = input
                {
                    input = inner;
                }
                // Get and ArrangeBy expressions contribute arrangements.
                match input {
                    RelationExpr::Get { id, typ: _ } => {
                        // If an external reference, we collect its keys from the `indexes` input.
                        if let crate::id::Id::Global(id) = id {
                            if let Some(keys) = indexes.get(id) {
                                available_arrangements[index].extend(keys.clone());
                            }
                        }
                    }
                    RelationExpr::ArrangeBy { input, keys } => {
                        // We may use any presented arrangement keys.
                        available_arrangements[index].extend(keys.clone());
                        if let RelationExpr::Get { id, typ: _ } = &**input {
                            if let crate::id::Id::Global(id) = id {
                                if let Some(keys) = indexes.get(id) {
                                    available_arrangements[index].extend(keys.clone());
                                }
                            }
                        }
                    }
                    RelationExpr::Reduce { group_key, .. } => {
                        // The first `keys.len()` columns form an arrangement key.
                        available_arrangements[index].push(
                            (0..group_key.len())
                                .map(|c| ScalarExpr::Column(c))
                                .collect(),
                        );
                    }
                    _ => {}
                }
            }

            // Determine if we can perform delta queries with the existing arrangements.
            // We could defer the execution if we are sure we know we want one input,
            // but we could imagine wanting the best from each and then comparing the two.
            let delta_query_plan = delta_queries::plan(
                relation,
                &prior_arities,
                &available_arrangements,
                &unique_keys,
            );
            let differential_plan = differential::plan(
                relation,
                &prior_arities,
                &available_arrangements,
                &unique_keys,
            );

            *relation = delta_query_plan
                .or(differential_plan)
                .expect("Failed to produce a join plan");
        }
    }
}

mod delta_queries {

    use crate::{relation::JoinImplementation, RelationExpr, ScalarExpr};

    /// Creates a delta query plan, and any predicates that need to be lifted.
    ///
    /// The method returns `None` if it fails to find a sufficiently pleasing plan.
    pub fn plan(
        join: &RelationExpr,
        prior_arities: &[usize],
        available: &[Vec<Vec<ScalarExpr>>],
        unique_keys: &[Vec<Vec<usize>>],
    ) -> Option<RelationExpr> {
        let mut new_join = join.clone();

        if let RelationExpr::Join {
            inputs,
            variables,
            demand,
            implementation,
        } = &mut new_join
        {
            // Determine a viable order for each relation, or return `None` if none found.
            let orders = super::optimize_orders(inputs.len(), variables, available, unique_keys);
            if !orders.iter().all(|o| o.iter().all(|(c, _, _)| c.arranged)) {
                return None;
            }

            // Convert the order information into specific (input, keys) information.
            let orders = orders
                .into_iter()
                .map(|o| o.into_iter().map(|(_c, k, r)| (r, k)).collect::<Vec<_>>())
                .collect::<Vec<_>>();

            // Implement arrangements in each of the inputs.
            let mut lifted = Vec::new();
            super::implement_arrangements(
                inputs,
                available,
                prior_arities,
                orders.iter().flatten(),
                &mut lifted,
            );
            *implementation = JoinImplementation::DeltaQuery(orders);
            if !lifted.is_empty() {
                *demand = None;
                new_join = new_join.filter(lifted);
            }

            // Hooray done!
            Some(new_join)
        } else {
            panic!("delta_queries::plan call on non-join expression.")
        }
    }
}

mod differential {

    use crate::{relation::JoinImplementation, RelationExpr, ScalarExpr};

    /// Creates a linear differential plan, and any predicates that need to be lifted.
    /// Creates a delta query plan, and any predicates that need to be lifted.
    ///
    /// The method returns `None` if it fails to find a sufficiently pleasing plan.
    pub fn plan(
        join: &RelationExpr,
        prior_arities: &[usize],
        available: &[Vec<Vec<ScalarExpr>>],
        unique_keys: &[Vec<Vec<usize>>],
    ) -> Option<RelationExpr> {
        let mut new_join = join.clone();

        if let RelationExpr::Join {
            inputs,
            variables,
            demand,
            implementation,
        } = &mut new_join
        {
            // We prefer a starting point based on the characteristics of the other input arrangements.
            // We could change this preference at any point, but the list of orders should still inform.
            let mut orders =
                super::optimize_orders(inputs.len(), variables, available, unique_keys);
            orders.sort_by_key(|x| x.iter().map(|(c, _, _)| c.clone()).min().unwrap());
            let mut order = orders
                .pop()?
                .into_iter()
                .map(|(_c, k, r)| (r, k))
                .collect::<Vec<_>>();
            let (start, _keys) = order.remove(0);

            // Implement arrangements in each of the inputs.
            let mut lifted = Vec::new();
            super::implement_arrangements(
                inputs,
                available,
                prior_arities,
                order.iter(),
                &mut lifted,
            );
            *implementation = JoinImplementation::Differential(start, order);
            if !lifted.is_empty() {
                *demand = None;
                new_join = new_join.filter(lifted);
            }

            // Hooray done!
            Some(new_join)
        } else {
            panic!("differential::plan call on non-join expression.")
        }
    }
}

fn optimize_orders(
    inputs: usize,
    variables: &[Vec<(usize, usize)>],
    available: &[Vec<Vec<ScalarExpr>>],
    unique_keys: &[Vec<Vec<usize>>],
) -> Vec<Vec<(CandidateCharacteristics, Vec<ScalarExpr>, usize)>> {
    (0..inputs)
        .map(|start| {
            // Rule-out `start` but claim that it does a pretty good job.
            // Worth understanding based on join implementation, but for
            // the moment neither implementation uses a start arrangement.
            let mut order = vec![(
                CandidateCharacteristics::new(true, true, true),
                Vec::new(),
                start,
            )];
            while order.len() < inputs {
                let ordered = order.iter().map(|(_c, _k, r)| *r).collect::<Vec<_>>();
                let candidate =
                    optimize_candidates(inputs, &ordered, variables, available, unique_keys);
                order.push(candidate.expect("Failed to find candidate in optimize_orders"));
            }
            order
        })
        .collect::<Vec<_>>()
}

/// Identifiers the next most appealing candidate and keys to use.
///
/// This method restricts its search to collections with arrangements
/// that can be used (i.e. have as keys expressions over columns bound
/// in `order`), and orders candidates by 1. whether their key is unique,
/// and 2. whether the key exactly matches the constrained columns of
/// the collection, and 3. whether an arrangement exists. This ordering
/// is intended to minimize the volume of intermediate records; one can
/// use a different order to prefer minimizing memory use.
///
/// These rules are subject to change if it turns out that they are silly.
/// For example, these rules prioritize non-arranged unique keys over any
/// arranged non-unique keys; that sounds conservatively smart, but there
/// are reasonable justifications to swap that around.
fn optimize_candidates(
    relations: usize,
    order: &[usize],
    variables: &[Vec<(usize, usize)>],
    arrange_keys: &[Vec<Vec<ScalarExpr>>],
    unique_keys: &[Vec<Vec<usize>>],
) -> Option<(CandidateCharacteristics, Vec<ScalarExpr>, usize)> {
    (0..relations)
        .filter(|i| !order.contains(i))
        .flat_map(|i| {
            let constrained = constrained_columns(i, &order, variables);
            arrange_keys[i]
                .iter()
                // For a key to be viable, we must be able to form the value needed to look up the key.
                .filter(|key| {
                    key.iter()
                        .all(|k| k.support().iter().all(|col| constrained.contains(col)))
                })
                .map(|key| {
                    let key_unique = unique_keys[i].iter().any(|uniq| {
                        uniq.iter()
                            .all(|col| key.contains(&ScalarExpr::Column(*col)))
                    });
                    let key_equal = key.len() == constrained.len();
                    (
                        CandidateCharacteristics::new(key_unique, key_equal, true),
                        key.clone(),
                        i,
                    )
                })
                // We can always consider a new arrangement, based on `constrained`.
                .chain(Some({
                    let unique = unique_keys[i]
                        .iter()
                        .any(|uniq| uniq.iter().all(|col| constrained.contains(col)));
                    (
                        CandidateCharacteristics::new(unique, true, false),
                        constrained.iter().map(|c| ScalarExpr::Column(*c)).collect(),
                        i,
                    )
                }))
                .max()
        })
        .max()
}

/// Lists the columns of collection `index` constrained to be equal to columns present in `order`.
fn constrained_columns(
    index: usize,
    order: &[usize],
    variables: &[Vec<(usize, usize)>],
) -> std::collections::HashSet<usize> {
    let mut results = std::collections::HashSet::new();
    for variable in variables.iter() {
        if variable.iter().any(|(rel, _col)| order.contains(rel)) {
            for (rel, col) in variable.iter() {
                if rel == &index {
                    results.insert(*col);
                }
            }
        }
    }
    results
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone)]
pub struct CandidateCharacteristics {
    unique_key: bool,
    exact_key: bool,
    arranged: bool,
}

impl CandidateCharacteristics {
    fn new(unique_key: bool, exact_key: bool, arranged: bool) -> Self {
        Self {
            unique_key,
            exact_key,
            arranged,
        }
    }
}

/// Modify `inputs` to ensure specified arrangements are available.
///
/// Lift filter predicates when all needed arrangements are otherwise available.
fn implement_arrangements<'a>(
    inputs: &mut [RelationExpr],
    available_arrangements: &[Vec<Vec<ScalarExpr>>],
    prior_arities: &[usize],
    needed_arrangements: impl Iterator<Item = &'a (usize, Vec<ScalarExpr>)>,
    lifted_predicates: &mut Vec<ScalarExpr>,
) {
    // Collect needed arrangements by source index.
    let mut needed = vec![Vec::new(); inputs.len()];
    for (index, key) in needed_arrangements {
        needed[*index].push(key.clone());
    }

    // Transform inputs[index] based on needed and available arrangements.
    // Specifically, lift intervening predicates if all arrangements exist.
    for (index, needed) in needed.iter_mut().enumerate() {
        needed.sort();
        needed.dedup();
        // We should lift any predicates, iff all arrangements are otherwise available.
        if needed
            .iter()
            .all(|key| available_arrangements[index].contains(key))
        {
            while let RelationExpr::Filter {
                input: inner,
                predicates,
            } = &mut inputs[index]
            {
                lifted_predicates.extend(predicates.drain(..).map(|mut expr| {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c += prior_arities[index]
                        }
                    });
                    expr
                }));
                inputs[index] = inner.take_dangerous();
            }
        }
        // Clean up existing arrangements, and install one with the needed keys.
        while let RelationExpr::ArrangeBy { input: inner, .. } = &mut inputs[index] {
            inputs[index] = inner.take_dangerous();
        }
        if !needed.is_empty() {
            inputs[index] = RelationExpr::arrange_by(inputs[index].take_dangerous(), needed);
        }
    }
}
