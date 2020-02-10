// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Clippy's cognitive complexity is easy to reach.
//#![allow(clippy::cognitive_complexity)]

//! Transformations that allow join to make use of indexes.
//!
//! This is mostly a proof-of-concept that indexes work. The transformations in this module
//! may or may not belong together. Also, the transformations are subject to change as indexes
//! become more advanced.

use std::collections::HashMap;

use repr::RelationType;

use crate::{BinaryFunc, EvalEnv, GlobalId, Id, RelationExpr, ScalarExpr};

/// Replaces filters of the form ScalarExpr::Column(i) == ScalarExpr::Literal, where i is a column for
/// which an index exists, with a
/// Join{
///   variables: [(0, i), (1,0)],
///   ArrangeBy{input, keys: [ScalarExpr::Column(i)]},
///   <constant>
/// }
/// TODO (wangandi): materialize#616 consider a general case when there exists in an index on an
/// expression of column i
#[derive(Debug)]
pub struct FilterEqualLiteral;

impl super::Transform for FilterEqualLiteral {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation, indexes);
    }
}

impl FilterEqualLiteral {
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
        if let RelationExpr::Filter { input, predicates } = relation {
            if let RelationExpr::Get {
                id: Id::Global(id), ..
            } = &mut **input
            {
                // gather predicates of the form CallBinary{Binaryfunc::Eq, Column, Literal}
                let (columns, predinfo): (Vec<_>, Vec<_>) = predicates
                    .iter()
                    .enumerate()
                    .filter_map(|(i, p)| {
                        if let ScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = p
                        {
                            match (&**expr1, &**expr2) {
                                (ScalarExpr::Literal(litrow, littyp), ScalarExpr::Column(c)) => {
                                    Some((*c, (litrow.clone(), littyp.clone(), i)))
                                }
                                (ScalarExpr::Column(c), ScalarExpr::Literal(litrow, littyp)) => {
                                    Some((*c, (litrow.clone(), littyp.clone(), i)))
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    })
                    .unzip();
                if !columns.is_empty() {
                    let key_set = &indexes[id];
                    // find set of keys of the largest size that is a subset of columns
                    let best_index = key_set
                        .iter()
                        .filter(|ks| {
                            ks.iter().all(|k| match k {
                                ScalarExpr::Column(c) => columns.contains(c),
                                _ => false,
                            })
                        })
                        .max_by_key(|ks| ks.len());
                    if let Some(keys) = best_index {
                        let column_order = keys
                            .iter()
                            .map(|k| match k {
                                ScalarExpr::Column(c) => {
                                    columns.iter().position(|d| c == d).unwrap()
                                }
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>();
                        let mut constant_row = Vec::new();
                        let mut constant_col_types = Vec::new();
                        let mut variables = Vec::new();
                        for (new_idx, old_idx) in column_order.into_iter().enumerate() {
                            variables.push(vec![(0, columns[old_idx]), (1, new_idx)]);
                            constant_row.extend(predinfo[old_idx].0.unpack());
                            constant_col_types.push(predinfo[old_idx].1.clone());
                        }
                        let mut constant_type = RelationType::new(constant_col_types);
                        for i in 0..keys.len() {
                            constant_type = constant_type.add_keys(vec![i]);
                        }
                        let arity = input.arity();
                        let converted_join = RelationExpr::join(
                            vec![
                                input.take_dangerous().arrange_by(&[keys.clone()]),
                                RelationExpr::constant(vec![constant_row], constant_type),
                            ],
                            variables,
                        )
                        .project((0..arity).collect::<Vec<_>>());
                        *input = Box::new(converted_join);
                    }
                }
            }
        }
    }
}

/// Reorders join equivalence classes in order to make use of indexes and replaces Get statements with
/// ArrangeBy statements so we can see that the index is being used. Pushes filter statements up
/// if it allows usage of indexes.
/// TODO (wangandi): fuse this with JoinOrder so that index information can be used
#[derive(Debug)]
pub struct FilterLifting;

impl super::Transform for FilterLifting {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation, indexes);
    }
}

impl FilterLifting {
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
        if let RelationExpr::Join {
            inputs, variables, ..
        } = relation
        {
            // find the keys being joined on for each input
            let mut join_keys_by_input = vec![Vec::new(); inputs.len()];
            for equivalence in variables.iter_mut() {
                equivalence.sort();
                assert_ne!(equivalence[0].0, equivalence[1].0);
                // We assume the equivalence classes are sorted, and that each relation is included
                // only once in an equivalence class
                for (input_num, column) in equivalence.iter().skip(1) {
                    join_keys_by_input[*input_num].push(*column)
                }
            }
            // for each input, find if there is an index corresponding to keys being joined on
            let mut matching_index_by_input = Vec::new();
            for (input_num, (join_keys, join_input)) in join_keys_by_input
                .into_iter()
                .zip(inputs.iter())
                .enumerate()
            {
                if let RelationExpr::Filter { input, predicates } = join_input {
                    // if none of the predicates refer to join keys, it can be lifted
                    if predicates.iter().all(|p| {
                        let support = p.support();
                        !join_keys.iter().any(|k| support.contains(k))
                    }) {
                        add_matching_index_by_input(
                            &**input,
                            indexes,
                            input_num,
                            join_keys,
                            &mut matching_index_by_input,
                        );
                    }
                } else {
                    add_matching_index_by_input(
                        join_input,
                        indexes,
                        input_num,
                        join_keys,
                        &mut matching_index_by_input,
                    );
                }
            }
            if !matching_index_by_input.is_empty() {
                // do a topological sort of the variables. There exists a directed edge from
                // equivalence class x to class y if there exist a variable z in x and
                // a variable b in y such that there exists an index where z and b are keys,
                // and z comes right before b.
                let mut incoming_edges = std::iter::repeat(vec![])
                    .take(variables.len())
                    .collect::<Vec<_>>();
                for (input_num, index) in matching_index_by_input.iter() {
                    for k in 1..index.len() {
                        if let ScalarExpr::Column(c_from) = index[k - 1] {
                            if let ScalarExpr::Column(c_to) = index[k] {
                                let from_class = variables
                                    .iter()
                                    .position(|eq| eq.contains(&(*input_num, c_from)));
                                let to_class = variables
                                    .iter()
                                    .position(|eq| eq.contains(&(*input_num, c_to)));
                                if let Some(from_position) = from_class {
                                    if let Some(to_position) = to_class {
                                        if !incoming_edges[to_position].contains(&from_position) {
                                            incoming_edges[to_position].push(from_position)
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                let mut new_variables = Vec::new();
                while let Some(position) = incoming_edges.iter().position(|edges| edges.is_empty())
                {
                    new_variables.push(variables.remove(position));
                    incoming_edges.remove(position);
                    for edges in incoming_edges.iter_mut() {
                        *edges = edges
                            .iter()
                            .filter_map(|e| match e {
                                e if *e == position => None,
                                e if *e > position => Some(e - 1),
                                _ => Some(*e),
                            })
                            .collect();
                    }
                }
                // if there is a cycle in the topological sort, don't bother sorting
                //the rest of the variables
                new_variables.append(variables);
                *variables = new_variables;
                // TODO(wangandi): considering turning the filters into wrappers instead of lifting them
                // reorder the variables to match the key + convert input to arrangement if the variable
                // rearrangement is a success
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                let input_arities = input_types
                    .iter()
                    .map(|i| i.column_types.len())
                    .collect::<Vec<_>>();
                let mut offset = 0;
                let mut prior_arities = Vec::new();
                for input in 0..inputs.len() {
                    prior_arities.push(offset);
                    offset += input_arities[input];
                }
                let mut lifted_predicates = Vec::new();
                for (input_num, index) in matching_index_by_input {
                    // if there was a cycle in the topological sort, test that the index can be used
                    // in this variable ordering.
                    if !incoming_edges.is_empty() {
                        let mut index_iter = index.iter();
                        let mut current_key = index_iter.next();
                        for variable in variables.iter() {
                            if let Some(key) = current_key {
                                if let ScalarExpr::Column(c) = key {
                                    if variable.contains(&(input_num, *c)) {
                                        current_key = index_iter.next();
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                        if current_key.is_some() {
                            continue;
                        }
                    }
                    if let RelationExpr::Filter { input, predicates } = &mut inputs[input_num] {
                        for predicate in predicates {
                            let mut predicate = predicate.clone();
                            predicate.permute(
                                &(prior_arities[input_num]
                                    ..(prior_arities[input_num] + input_arities[input_num]))
                                    .collect::<Vec<_>>(),
                            );
                            lifted_predicates.push(predicate);
                        }
                        inputs[input_num] = input.take_dangerous();
                    }
                    inputs[input_num] = inputs[input_num].clone().arrange_by(&[index]);
                }
                // put an outer filter around the join if any filters were lifted.
                if !lifted_predicates.is_empty() {
                    *relation = relation.take_dangerous().filter(lifted_predicates);
                }
            }
        }
    }
}

fn add_matching_index_by_input(
    join_input: &RelationExpr,
    indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    input_num: usize,
    join_keys: Vec<usize>,
    matching_index_by_input: &mut Vec<(usize, Vec<ScalarExpr>)>,
) {
    if let RelationExpr::Get {
        id: Id::Global(id), ..
    } = join_input
    {
        let index_keys = indexes.get(id).and_then(|indexes| {
            indexes.iter().find(|ik| {
                ik.len() == join_keys.len()
                    && join_keys
                        .iter()
                        .all(|k| ik.contains(&ScalarExpr::Column(*k)))
            })
        });
        if let Some(index_keys) = index_keys {
            matching_index_by_input.push((input_num, index_keys.clone()));
        }
    }
}
