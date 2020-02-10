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

use crate::{EvalEnv, GlobalId, Id, RelationExpr, ScalarExpr};

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
        let mut arranged = HashMap::new();
        for (k, v) in indexes {
            arranged.insert(Id::Global(*k), v.clone());
        }
        self.action_recursive(relation, &mut arranged);
    }

    /// Pre-order visitor for each `RelationExpr`.
    pub fn action_recursive(
        &self,
        relation: &mut RelationExpr,
        arranged: &mut HashMap<Id, Vec<Vec<ScalarExpr>>>,
    ) {
        if let RelationExpr::Let { id, value, body } = relation {
            self.action_recursive(value, arranged);
            match &**value {
                RelationExpr::ArrangeBy { keys, .. } => {
                    arranged.insert(Id::Local(*id), keys.clone());
                }
                RelationExpr::Reduce { group_key, .. } => {
                    arranged.insert(
                        Id::Local(*id),
                        vec![(0..group_key.len())
                            .map(|c| ScalarExpr::Column(c))
                            .collect()],
                    );
                }
                _ => {}
            }
            self.action_recursive(body, arranged);
            arranged.remove(&Id::Local(*id));
        } else {
            relation.visit1_mut(|e| self.action_recursive(e, arranged));
            self.action(relation, arranged);
        }
    }

    pub fn action(&self, relation: &mut RelationExpr, indexes: &HashMap<Id, Vec<Vec<ScalarExpr>>>) {
        if let RelationExpr::Join { inputs, .. } = relation {
            // Common information of broad utility.
            // TODO: Figure out how to package this up for everyone who uses it.
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
                        if let Some(keys) = indexes.get(id) {
                            available_arrangements[index].extend(keys.clone());
                        }
                    }
                    RelationExpr::ArrangeBy { input, keys } => {
                        // We may use any presented arrangement keys.
                        available_arrangements[index].extend(keys.clone());
                        if let RelationExpr::Get { id, typ: _ } = &**input {
                            if let Some(keys) = indexes.get(id) {
                                available_arrangements[index].extend(keys.clone());
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
                available_arrangements[index].sort();
                available_arrangements[index].dedup();
                available_arrangements[index].retain(|key| {
                    key.iter().all(|k| {
                        if let ScalarExpr::Column(_) = k {
                            true
                        } else {
                            false
                        }
                    })
                });
            }

            // Determine if we can perform delta queries with the existing arrangements.
            // We could defer the execution if we are sure we know we want one input,
            // but we could imagine wanting the best from each and then comparing the two.
            let delta_query_plan = delta_queries::plan(
                relation,
                &arities,
                &prior_arities,
                &available_arrangements,
                &unique_keys,
            );
            let differential_plan = differential::plan(
                relation,
                &arities,
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
        arities: &[usize],
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
            let orders = super::optimize_orders(variables, available, unique_keys);
            if !orders.iter().all(|o| o.iter().all(|(c, _, _)| c.arranged)) {
                return None;
            }

            // Convert the order information into specific (input, keys) information.
            let orders = orders
                .into_iter()
                .map(|o| {
                    o.into_iter()
                        .skip(1)
                        .map(|(_c, k, r)| (r, k))
                        .collect::<Vec<_>>()
                })
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

            if !lifted.is_empty() {
                // We must add the support of expression in `lifted` to the `demand`
                // member to ensure they are correctly populated.
                if let Some(demand) = demand {
                    let mut rel_col = Vec::new();
                    for (input, arity) in arities.iter().enumerate() {
                        for _ in 0..*arity {
                            rel_col.push(input);
                        }
                    }
                    for expr in lifted.iter() {
                        for column in expr.support() {
                            let rel = rel_col[column];
                            demand[rel].push(column - prior_arities[rel]);
                        }
                    }
                    for list in demand.iter_mut() {
                        list.sort();
                        list.dedup();
                    }
                }
            }

            *implementation = JoinImplementation::DeltaQuery(orders);

            if !lifted.is_empty() {
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
    pub fn plan(
        join: &RelationExpr,
        arities: &[usize],
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
            for variable in variables.iter_mut() {
                variable.sort();
            }
            variables.sort();

            // We prefer a starting point based on the characteristics of the other input arrangements.
            // We could change this preference at any point, but the list of orders should still inform.
            // Important, we should choose something stable under re-ordering, to converge under fixed
            // point iteration; we choose to start with the first input optimizing our criteria, which
            // should remain stable even when promoted to the first position.
            let orders = super::optimize_orders(variables, available, unique_keys);
            let max_min_characteristics = orders
                .iter()
                .flat_map(|order| order.iter().map(|(c, _, _)| c.clone()).min())
                .max()
                .unwrap();
            let mut order = orders
                .into_iter()
                .find(|o| o.iter().map(|(c, _, _)| c).min().unwrap() == &max_min_characteristics)?
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

            if !lifted.is_empty() {
                // We must add the support of expression in `lifted` to the `demand`
                // member to ensure they are correctly populated.
                if let Some(demand) = demand {
                    let mut rel_col = Vec::new();
                    for (input, arity) in arities.iter().enumerate() {
                        for _ in 0..*arity {
                            rel_col.push(input);
                        }
                    }
                    for expr in lifted.iter() {
                        for column in expr.support() {
                            let rel = rel_col[column];
                            demand[rel].push(column - prior_arities[rel]);
                        }
                    }
                    for list in demand.iter_mut() {
                        list.sort();
                        list.dedup();
                    }
                }
            }

            // Install the implementation.
            *implementation = JoinImplementation::Differential(start, order);

            if !lifted.is_empty() {
                new_join = new_join.filter(lifted);
            }

            // Hooray done!
            Some(new_join)
        } else {
            panic!("differential::plan call on non-join expression.")
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
        if !needed.is_empty()
            && needed
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

fn optimize_orders(
    variables: &[Vec<(usize, usize)>],
    available: &[Vec<Vec<ScalarExpr>>],
    unique_keys: &[Vec<Vec<usize>>],
) -> Vec<Vec<(Characteristics, Vec<ScalarExpr>, usize)>> {
    let mut orderer = Orderer::new(variables, available, unique_keys);
    (0..available.len())
        .map(move |i| orderer.optimize_order_for(i))
        .collect::<Vec<_>>()
}

/// Characteristics of a join order candidate collection.
///
/// A candidate is described by a collection and a key, and may have various liabilities.
/// Primarily, the candidate may risk substantial inflation of records, which is something
/// that concerns us greatly. Additionally the candidate may be unarranged, and we would
/// prefer candidates that do not require additional memory. Finally, we prefer lower id
/// collections in the interest of consistent tie-breaking.
#[derive(Eq, PartialEq, Ord, PartialOrd, Debug, Clone)]
pub struct Characteristics {
    // An excellent indication that record count will not increase.
    unique_key: bool,
    // A weaker signal that record count will not increase.
    key_length: usize,
    // Indicates that there will be no additional in-memory footprint.
    arranged: bool,
    // We want to prefer input earlier in the input list, for stability of ordering.
    input: std::cmp::Reverse<usize>,
}

impl Characteristics {
    fn new(unique_key: bool, key_length: usize, arranged: bool, input: usize) -> Self {
        Self {
            unique_key,
            key_length,
            arranged,
            input: std::cmp::Reverse(input),
        }
    }
}

struct Orderer<'a> {
    inputs: usize,
    variables: &'a [Vec<(usize, usize)>],
    arrangements: &'a [Vec<Vec<ScalarExpr>>],
    unique_keys: &'a [Vec<Vec<usize>>],
    reverse_variables: Vec<Vec<usize>>,
    unique_arrangement: Vec<Vec<bool>>,

    order: Vec<(Characteristics, Vec<ScalarExpr>, usize)>,
    placed: Vec<bool>,
    bound: Vec<Vec<usize>>,
    variables_active: Vec<bool>,
    arrangement_active: Vec<Vec<usize>>,
    priority_queue: std::collections::BinaryHeap<(Characteristics, Vec<ScalarExpr>, usize)>,
}

impl<'a> Orderer<'a> {
    fn new(
        variables: &'a [Vec<(usize, usize)>],
        arrangements: &'a [Vec<Vec<ScalarExpr>>],
        unique_keys: &'a [Vec<Vec<usize>>],
    ) -> Self {
        let inputs = arrangements.len();
        // A map from inputs to the variables in which they are contained.
        let mut reverse_variables = vec![Vec::new(); inputs];
        for (index, variable) in variables.iter().enumerate() {
            for (rel, _col) in variable.iter() {
                reverse_variables[*rel].push(index);
            }
        }
        // Per-arrangement information about uniqueness of the arrangement key.
        let mut unique_arrangement = vec![Vec::new(); inputs];
        for (input, keys) in arrangements.iter().enumerate() {
            for key in keys.iter() {
                unique_arrangement[input].push(
                    unique_keys[input]
                        .iter()
                        .any(|cols| cols.iter().all(|c| key.contains(&ScalarExpr::Column(*c)))),
                );
            }
        }

        let order = Vec::with_capacity(inputs);
        let placed = vec![false; inputs];
        let bound = vec![Vec::new(); inputs];
        let variables_active = vec![false; variables.len()];
        let arrangement_active = vec![Vec::new(); inputs];
        let priority_queue = std::collections::BinaryHeap::new();
        Self {
            inputs,
            variables,
            arrangements,
            unique_keys,
            reverse_variables,
            unique_arrangement,
            order,
            placed,
            bound,
            variables_active,
            arrangement_active,
            priority_queue,
        }
    }

    fn optimize_order_for(
        &mut self,
        start: usize,
    ) -> Vec<(Characteristics, Vec<ScalarExpr>, usize)> {
        self.order.clear();
        self.priority_queue.clear();
        for input in 0..self.inputs {
            self.placed[input] = false;
            self.bound[input].clear();
            self.arrangement_active[input].clear();
        }
        for index in 0..self.variables.len() {
            self.variables_active[index] = false;
        }

        // Introduce cross joins as a possibility.
        for input in 0..self.inputs {
            let is_unique = self.unique_keys[input].iter().any(|cols| cols.is_empty());
            if let Some(pos) = self.arrangements[input]
                .iter()
                .position(|key| key.is_empty())
            {
                self.arrangement_active[input].push(pos);
                self.priority_queue.push((
                    Characteristics::new(is_unique, 0, true, input),
                    vec![],
                    input,
                ));
            } else {
                self.priority_queue.push((
                    Characteristics::new(is_unique, 0, false, input),
                    vec![],
                    input,
                ));
            }
        }

        self.order.push((
            Characteristics::new(true, usize::max_value(), true, start),
            vec![],
            start,
        ));
        self.order_input(start);
        while self.order.len() < self.inputs {
            let (characteristics, key, input) = self.priority_queue.pop().unwrap();
            if !self.placed[input] {
                self.order.push((characteristics, key, input));
                self.order_input(input);
            }
        }

        std::mem::replace(&mut self.order, Vec::new())
    }

    /// Introduces a specific input and keys to the order, along with its characteristics.
    ///
    /// This method places a next element in the order, and updates the associated state
    /// about other candidates, including which columns are now bound and which potential
    /// keys are available to consider (both arranged, and unarranged).
    fn order_input(&mut self, input: usize) {
        self.placed[input] = true;
        for variable in self.reverse_variables[input].iter() {
            if !self.variables_active[*variable] {
                self.variables_active[*variable] = true;
                for (rel, col) in self.variables[*variable].iter() {
                    // Update bound columns.
                    self.bound[*rel].push(*col);
                    self.bound[*rel].sort();
                    // Reconsider all available arrangements.
                    for (pos, keys) in self.arrangements[*rel].iter().enumerate() {
                        if !self.arrangement_active[*rel].contains(&pos) {
                            // Determine if the arrangement is viable, which happens when the
                            // support of its keys are all bound.
                            if keys
                                .iter()
                                .all(|k| k.support().iter().all(|c| self.bound[*rel].contains(c)))
                            {
                                self.arrangement_active[*rel].push(pos);
                                // TODO: This could be pre-computed, as it is independent of the order.
                                let is_unique = self.unique_arrangement[*rel][pos];
                                self.priority_queue.push((
                                    Characteristics::new(is_unique, keys.len(), true, *rel),
                                    keys.clone(),
                                    *rel,
                                ));
                            }
                        }
                    }
                    let is_unique = self.unique_keys[*rel]
                        .iter()
                        .any(|cols| cols.iter().all(|c| self.bound[*rel].contains(c)));
                    self.priority_queue.push((
                        Characteristics::new(is_unique, self.bound[*rel].len(), false, *rel),
                        self.bound[*rel]
                            .iter()
                            .map(|c| ScalarExpr::Column(*c))
                            .collect::<Vec<_>>(),
                        *rel,
                    ));
                }
            }
        }
    }
}
