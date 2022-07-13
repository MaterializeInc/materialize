// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Clippy's cognitive complexity is easy to reach.
//#![allow(clippy::cognitive_complexity)]

//! Determines the join implementation for join operators.
//!
//! This includes determining the type of join (e.g. differential linear, or delta queries),
//! determining the orders of collections, lifting predicates if useful arrangements exist,
//! and identifying opportunities to use indexes to replace filters.

use std::collections::HashMap;

use mz_expr::visit::{Visit, VisitChildren};
use mz_expr::{JoinInputMapper, MapFilterProject, MirRelationExpr, MirScalarExpr, RECURSION_LIMIT};
use mz_ore::stack::{CheckedRecursion, RecursionGuard};

use self::index_map::IndexMap;
use crate::{TransformArgs, TransformError};

/// Determines the join implementation for join operators.
#[derive(Debug)]
pub struct JoinImplementation {
    recursion_guard: RecursionGuard,
}

impl Default for JoinImplementation {
    /// Construct a new [`JoinImplementation`] where `recursion_guard`
    /// is initialized with [`RECURSION_LIMIT`] as limit.
    fn default() -> JoinImplementation {
        JoinImplementation {
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }
}

impl CheckedRecursion for JoinImplementation {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl crate::Transform for JoinImplementation {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action_recursive(relation, &mut IndexMap::new(args.indexes))
    }
}

impl JoinImplementation {
    /// Pre-order visitor for each `MirRelationExpr` to find join operators.
    ///
    /// This method accumulates state about let-bound arrangements, so that
    /// join operators can more accurately assess their available arrangements.
    pub fn action_recursive(
        &self,
        relation: &mut MirRelationExpr,
        indexes: &mut IndexMap,
    ) -> Result<(), crate::TransformError> {
        if let MirRelationExpr::Let { id, value, body } = relation {
            self.action_recursive(value, indexes)?;
            match &**value {
                MirRelationExpr::ArrangeBy { keys, .. } => {
                    for key in keys {
                        indexes.add_local(*id, key.clone());
                    }
                }
                MirRelationExpr::Reduce { group_key, .. } => {
                    indexes.add_local(
                        *id,
                        (0..group_key.len()).map(MirScalarExpr::Column).collect(),
                    );
                }
                _ => {}
            }
            self.action_recursive(body, indexes)?;
            indexes.remove_local(*id);
            Ok(())
        } else {
            relation.try_visit_mut_children(|e| self.action_recursive(e, indexes))?;
            self.action(relation, indexes);
            Ok(())
        }
    }

    /// Determines the join implementation for join operators.
    pub fn action(&self, relation: &mut MirRelationExpr, indexes: &IndexMap) {
        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } = relation
        {
            let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();

            // Canonicalize the equivalence classes
            mz_expr::canonicalize::canonicalize_equivalences(equivalences, &input_types);

            // Common information of broad utility.
            let input_mapper = JoinInputMapper::new_from_input_types(&input_types);

            // The first fundamental question is whether we should employ a delta query or not.
            //
            // Here we conservatively use the rule that if sufficient arrangements exist we will
            // use a delta query. An arrangement is considered available if it is a global get
            // with columns present in `indexes`, if it is an `ArrangeBy` with the columns present,
            // or a filter wrapped around either of these.

            let unique_keys = input_types
                .into_iter()
                .map(|typ| typ.keys)
                .collect::<Vec<_>>();
            let mut available_arrangements = vec![Vec::new(); inputs.len()];
            for index in 0..inputs.len() {
                // We can work around mfps, as we can lift the mfps into the join execution.
                let (mfp, input) = MapFilterProject::extract_non_errors_from_expr(&inputs[index]);
                let (_, _, project) = mfp.as_map_filter_project();
                // Get and ArrangeBy expressions contribute arrangements.
                match input {
                    MirRelationExpr::Get { id, typ: _ } => {
                        available_arrangements[index]
                            .extend(indexes.get(*id).map(|key| key.to_vec()));
                    }
                    MirRelationExpr::ArrangeBy { input, keys } => {
                        // We may use any presented arrangement keys.
                        available_arrangements[index].extend(keys.clone());
                        if let MirRelationExpr::Get { id, typ: _ } = &**input {
                            available_arrangements[index]
                                .extend(indexes.get(*id).map(|key| key.to_vec()));
                        }
                    }
                    MirRelationExpr::Reduce { group_key, .. } => {
                        // The first `keys.len()` columns form an arrangement key.
                        available_arrangements[index]
                            .push((0..group_key.len()).map(MirScalarExpr::Column).collect());
                    }
                    _ => {}
                }
                available_arrangements[index].sort();
                available_arrangements[index].dedup();
                let reverse_project = project
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| (c, i))
                    .collect::<HashMap<_, _>>();
                // Eliminate arrangements referring to columns that have been
                // projected away by surrounding MFPs.
                available_arrangements[index].retain(|key| {
                    key.iter()
                        .all(|k| k.support().iter().all(|c| reverse_project.contains_key(c)))
                });
                // Permute arrangements so columns reference what is after the MFP.
                for key_set in available_arrangements[index].iter_mut() {
                    for key in key_set.iter_mut() {
                        key.permute_map(&reverse_project);
                    }
                }
                // Currently we only support using arrangements all of whose
                // keys can be found in some equivalence.
                // Note: because `order_input` currently only finds arrangements
                // with exact key matches, the code below can be removed with no
                // change in behavior, but this is being kept for a future
                // TODO: expand `order_input`
                available_arrangements[index].retain(|key| {
                    key.iter().all(|k| {
                        let k = input_mapper.map_expr_to_global(k.clone(), index);
                        equivalences
                            .iter()
                            .any(|equivalence| equivalence.contains(&k))
                    })
                });
            }

            // Determine if we can perform delta queries with the existing arrangements.
            // We could defer the execution if we are sure we know we want one input,
            // but we could imagine wanting the best from each and then comparing the two.
            let delta_query_plan = delta_queries::plan(
                relation,
                &input_mapper,
                &available_arrangements,
                &unique_keys,
            );
            let differential_plan = differential::plan(
                relation,
                &input_mapper,
                &available_arrangements,
                &unique_keys,
            );

            *relation = delta_query_plan
                .or(differential_plan)
                .expect("Failed to produce a join plan");
        }
    }
}

mod index_map {
    use std::collections::HashMap;

    use mz_expr::{Id, LocalId, MirScalarExpr};

    use crate::IndexOracle;

    /// Keeps track of local and global indexes available while descending
    /// a `MirRelationExpr`.
    #[derive(Debug)]
    pub struct IndexMap<'a> {
        local: HashMap<LocalId, Vec<Vec<MirScalarExpr>>>,
        global: &'a dyn IndexOracle,
    }

    impl IndexMap<'_> {
        /// Creates a new index map with knowledge of the provided global
        /// indexes.
        pub fn new(global: &dyn IndexOracle) -> IndexMap {
            IndexMap {
                local: HashMap::new(),
                global,
            }
        }

        /// Adds a local index on the specified collection with the specified
        /// key.
        pub fn add_local(&mut self, id: LocalId, key: Vec<MirScalarExpr>) {
            self.local.entry(id).or_default().push(key)
        }

        /// Removes all local indexes on the specified collection.
        pub fn remove_local(&mut self, id: LocalId) {
            self.local.remove(&id);
        }

        pub fn get(&self, id: Id) -> Box<dyn Iterator<Item = &[MirScalarExpr]> + '_> {
            match id {
                Id::Global(id) => self.global.indexes_on(id),
                Id::Local(id) => Box::new(
                    self.local
                        .get(&id)
                        .into_iter()
                        .flatten()
                        .map(|x| x.as_slice()),
                ),
            }
        }
    }
}

mod delta_queries {

    use mz_expr::{JoinImplementation, JoinInputMapper, MirRelationExpr, MirScalarExpr};

    use crate::TransformError;

    /// Creates a delta query plan, and any predicates that need to be lifted.
    ///
    /// The method returns `Err` if it fails to find a sufficiently pleasing plan or
    /// if any errors occur during planning.
    pub fn plan(
        join: &MirRelationExpr,
        input_mapper: &JoinInputMapper,
        available: &[Vec<Vec<MirScalarExpr>>],
        unique_keys: &[Vec<Vec<usize>>],
    ) -> Result<MirRelationExpr, TransformError> {
        let mut new_join = join.clone();

        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            implementation,
        } = &mut new_join
        {
            if inputs.len() < 2 {
                // Single input joins are filters and should be planned as
                // differential plans instead of delta queries. Because a
                // a filter gets converted into a single input join only when
                // there are existing arrangements, without this early return,
                // filters will always be planned as delta queries.
                return Err(TransformError::Internal(String::from(
                    "should be planned as differential plan",
                )));
            }

            // Determine a viable order for each relation, or return `Err` if none found.
            let orders = super::optimize_orders(equivalences, available, unique_keys, input_mapper);

            // A viable delta query requires that, for every order,
            // there is an arrangement for every input except for
            // the starting one.
            if !orders
                .iter()
                .all(|o| o.iter().skip(1).all(|(c, _, _)| c.arranged))
            {
                return Err(TransformError::Internal(String::from(
                    "delta plan not viable",
                )));
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
            let lifted_mfp =
                super::implement_arrangements(inputs, available, orders.iter().flatten());

            *implementation = JoinImplementation::DeltaQuery(orders);

            super::install_lifted_mfp(&mut new_join, lifted_mfp)?;

            // Hooray done!
            Ok(new_join)
        } else {
            Err(TransformError::Internal(String::from(
                "delta_queries::plan call on non-join expression",
            )))
        }
    }
}

mod differential {

    use mz_expr::{JoinImplementation, JoinInputMapper, MirRelationExpr, MirScalarExpr};

    use crate::TransformError;

    /// Creates a linear differential plan, and any predicates that need to be lifted.
    pub fn plan(
        join: &MirRelationExpr,
        input_mapper: &JoinInputMapper,
        available: &[Vec<Vec<MirScalarExpr>>],
        unique_keys: &[Vec<Vec<usize>>],
    ) -> Result<MirRelationExpr, TransformError> {
        let mut new_join = join.clone();

        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            implementation,
        } = &mut new_join
        {
            // We prefer a starting point based on the characteristics of the other input arrangements.
            // We could change this preference at any point, but the list of orders should still inform.
            // Important, we should choose something stable under re-ordering, to converge under fixed
            // point iteration; we choose to start with the first input optimizing our criteria, which
            // should remain stable even when promoted to the first position.
            let mut orders =
                super::optimize_orders(equivalences, available, unique_keys, input_mapper);

            // For differential join, it is not as important for the starting
            // input to have good characteristics because the other ones
            // determine whether intermediate results blow up. Thus, we do not
            // include the starting input when max-minning.
            let max_min_characteristics = orders
                .iter()
                .flat_map(|order| order.iter().skip(1).map(|(c, _, _)| c.clone()).min())
                .max();
            let mut order = if let Some(max_min_characteristics) = max_min_characteristics {
                orders
                    .into_iter()
                    .find(|o| {
                        o.iter().skip(1).map(|(c, _, _)| c).min().unwrap()
                            == &max_min_characteristics
                    })
                    .ok_or_else(|| {
                        TransformError::Internal(String::from(
                            "could not find max-min characteristics",
                        ))
                    })?
                    .into_iter()
                    .map(|(_c, k, r)| (r, k))
                    .collect::<Vec<_>>()
            } else {
                // if max_min_characteristics is None, then there must only be
                // one input and thus only one order in orders
                orders
                    .remove(0)
                    .into_iter()
                    .map(|(_c, k, r)| (r, k))
                    .collect::<Vec<_>>()
            };

            let (start, start_keys) = &order[0];
            let start = *start;
            let start_keys = if available[start].contains(&start_keys) {
                Some(start_keys.clone())
            } else {
                // if there is not already a pre-existing arrangement
                // for the start input, do not implement one
                order.remove(0);
                None
            };

            // Implement arrangements in each of the inputs.
            let lifted_mfp = super::implement_arrangements(inputs, available, order.iter());

            if start_keys.is_some() {
                // now that the starting arrangement has been implemented,
                // remove it from `order` so `order` only contains information
                // about the other inputs
                order.remove(0);
            }

            // Install the implementation.
            *implementation = JoinImplementation::Differential((start, start_keys), order);

            super::install_lifted_mfp(&mut new_join, lifted_mfp)?;

            // Hooray done!
            Ok(new_join)
        } else {
            Err(TransformError::Internal(String::from(
                "differential::plan call on non-join expression.",
            )))
        }
    }
}

/// Modify `inputs` to ensure specified arrangements are available.
///
/// Lift filter predicates when all needed arrangements are otherwise available.
fn implement_arrangements<'a>(
    inputs: &mut [MirRelationExpr],
    available_arrangements: &[Vec<Vec<MirScalarExpr>>],
    needed_arrangements: impl Iterator<Item = &'a (usize, Vec<MirScalarExpr>)>,
) -> MapFilterProject {
    // Collect needed arrangements by source index.
    let mut needed = vec![Vec::new(); inputs.len()];
    for (index, key) in needed_arrangements {
        needed[*index].push(key.clone());
    }

    let mut lifted_mfps = vec![None; inputs.len()];

    // Transform inputs[index] based on needed and available arrangements.
    // Specifically, lift intervening mfps if all arrangements exist.
    for (index, needed) in needed.iter_mut().enumerate() {
        needed.sort();
        needed.dedup();
        // We should lift any mfps, iff all arrangements are otherwise available.
        if !needed.is_empty()
            && needed
                .iter()
                .all(|key| available_arrangements[index].contains(key))
        {
            lifted_mfps[index] = Some(MapFilterProject::extract_non_errors_from_expr_mut(
                &mut inputs[index],
            ));
        }
        // Clean up existing arrangements, and install one with the needed keys.
        while let MirRelationExpr::ArrangeBy { input: inner, .. } = &mut inputs[index] {
            inputs[index] = inner.take_dangerous();
        }
        if !needed.is_empty() {
            // If a mfp was lifted in order to install the arrangement, permute
            // the arrangement.
            if let Some(lifted_mfp) = &lifted_mfps[index] {
                let (_, _, project) = lifted_mfp.as_map_filter_project();
                for arr in needed.iter_mut() {
                    for key in arr.iter_mut() {
                        key.permute(&project);
                    }
                }
            }
            inputs[index] = MirRelationExpr::arrange_by(inputs[index].take_dangerous(), needed);
        }
    }

    // Combined lifted mfps into one.
    let new_join_mapper = JoinInputMapper::new(inputs);
    let mut arity = new_join_mapper.total_columns();
    let combined_mfp = MapFilterProject::new(arity);
    let mut combined_filter = Vec::new();
    let mut combined_map = Vec::new();
    let mut combined_project = Vec::new();
    for (index, lifted_mfp) in lifted_mfps.into_iter().enumerate() {
        if let Some(mut lifted_mfp) = lifted_mfp {
            lifted_mfp.permute(
                // globalize all input column references
                new_join_mapper
                    .local_columns(index)
                    .zip(new_join_mapper.global_columns(index))
                    .collect(),
                // shift the position of scalars to be after the last input
                // column
                arity,
            );
            let (mut map, mut filter, mut project) = lifted_mfp.as_map_filter_project();
            arity += map.len();
            combined_map.append(&mut map);
            combined_filter.append(&mut filter);
            combined_project.append(&mut project);
        } else {
            combined_project.extend(new_join_mapper.global_columns(index));
        }
    }
    combined_mfp
        .map(combined_map)
        .filter(combined_filter)
        .project(combined_project)
}

fn install_lifted_mfp(
    new_join: &mut MirRelationExpr,
    mfp: MapFilterProject,
) -> Result<(), TransformError> {
    if !mfp.is_identity() {
        let (map, filter, project) = mfp.as_map_filter_project();
        if let MirRelationExpr::Join { equivalences, .. } = new_join {
            for equivalence in equivalences.iter_mut() {
                for expr in equivalence.iter_mut() {
                    // permute `equivalences` in light of the project being lifted
                    expr.permute(&project);
                    // if column references refer to mapped expressions that have been
                    // lifted, replace the column reference with the mapped expression.
                    expr.visit_mut_pre_post(
                        &mut |e| {
                            if let MirScalarExpr::Column(c) = e {
                                if *c >= mfp.input_arity {
                                    *e = map[*c - mfp.input_arity].clone();
                                }
                            }
                            None
                        },
                        &mut |_| {},
                    )?;
                }
            }
        }
        *new_join = new_join.clone().map(map).filter(filter).project(project);
    }
    Ok(())
}

fn optimize_orders(
    equivalences: &[Vec<MirScalarExpr>],
    available: &[Vec<Vec<MirScalarExpr>>],
    unique_keys: &[Vec<Vec<usize>>],
    input_mapper: &JoinInputMapper,
) -> Vec<Vec<(Characteristics, Vec<MirScalarExpr>, usize)>> {
    let mut orderer = Orderer::new(equivalences, available, unique_keys, input_mapper);
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
    equivalences: &'a [Vec<MirScalarExpr>],
    arrangements: &'a [Vec<Vec<MirScalarExpr>>],
    unique_keys: &'a [Vec<Vec<usize>>],
    input_mapper: &'a JoinInputMapper,
    reverse_equivalences: Vec<Vec<(usize, usize)>>,
    unique_arrangement: Vec<Vec<bool>>,

    order: Vec<(Characteristics, Vec<MirScalarExpr>, usize)>,
    placed: Vec<bool>,
    bound: Vec<Vec<MirScalarExpr>>,
    equivalences_active: Vec<bool>,
    arrangement_active: Vec<Vec<usize>>,
    priority_queue: std::collections::BinaryHeap<(Characteristics, Vec<MirScalarExpr>, usize)>,
}

impl<'a> Orderer<'a> {
    fn new(
        equivalences: &'a [Vec<MirScalarExpr>],
        arrangements: &'a [Vec<Vec<MirScalarExpr>>],
        unique_keys: &'a [Vec<Vec<usize>>],
        input_mapper: &'a JoinInputMapper,
    ) -> Self {
        let inputs = arrangements.len();
        // A map from inputs to the equivalence classes in which they are referenced.
        let mut reverse_equivalences = vec![Vec::new(); inputs];
        for (index, equivalence) in equivalences.iter().enumerate() {
            for (index2, expr) in equivalence.iter().enumerate() {
                for input in input_mapper.lookup_inputs(expr) {
                    reverse_equivalences[input].push((index, index2));
                }
            }
        }
        // Per-arrangement information about uniqueness of the arrangement key.
        let mut unique_arrangement = vec![Vec::new(); inputs];
        for (input, keys) in arrangements.iter().enumerate() {
            for key in keys.iter() {
                unique_arrangement[input].push(unique_keys[input].iter().any(|cols| {
                    cols.iter()
                        .all(|c| key.contains(&MirScalarExpr::Column(*c)))
                }));
            }
        }

        let order = Vec::with_capacity(inputs);
        let placed = vec![false; inputs];
        let bound = vec![Vec::new(); inputs];
        let equivalences_active = vec![false; equivalences.len()];
        let arrangement_active = vec![Vec::new(); inputs];
        let priority_queue = std::collections::BinaryHeap::new();
        Self {
            inputs,
            equivalences,
            arrangements,
            unique_keys,
            input_mapper,
            reverse_equivalences,
            unique_arrangement,
            order,
            placed,
            bound,
            equivalences_active,
            arrangement_active,
            priority_queue,
        }
    }

    fn optimize_order_for(
        &mut self,
        start: usize,
    ) -> Vec<(Characteristics, Vec<MirScalarExpr>, usize)> {
        self.order.clear();
        self.priority_queue.clear();
        for input in 0..self.inputs {
            self.placed[input] = false;
            self.bound[input].clear();
            self.arrangement_active[input].clear();
        }
        for index in 0..self.equivalences.len() {
            self.equivalences_active[index] = false;
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

        if self.inputs > 1 {
            self.order_input(start);
            while self.order.len() < self.inputs - 1 {
                let (characteristics, key, input) = self.priority_queue.pop().unwrap();
                // put the tuple into `self.order` unless the tuple with the same
                // input is already in `self.order`. For all inputs other than
                // start, `self.placed[input]` is an indication of whether a
                // corresponding tuple is already in `self.order`.
                if !self.placed[input] {
                    // non-starting inputs are ordered in decreasing priority
                    self.order.push((characteristics, key, input));
                    self.order_input(input);
                }
            }
        }

        // calculate characteristics of an arrangement, if any on the starting input
        // by default, there is no arrangement on the starting input
        let mut start_tuple = (Characteristics::new(false, 0, false, start), vec![], start);
        // use an arrangement if there exists one that lines up with the keys of
        // the second input
        if let Some((_, key, second)) = self.order.get(0) {
            // for each key of the second input, try to find the corresponding key in
            // the starting input
            let candidate_start_key = key
                .iter()
                .filter_map(|k| {
                    let k = self.input_mapper.map_expr_to_global(k.clone(), *second);
                    self.input_mapper
                        .find_bound_expr(&k, &[start], self.equivalences)
                        .map(|bound_key| self.input_mapper.map_expr_to_local(bound_key))
                })
                .collect::<Vec<_>>();
            if candidate_start_key.len() == key.len() {
                if let Some(pos) = self.arrangements[start]
                    .iter()
                    .position(|k| k == &candidate_start_key)
                {
                    let is_unique = self.unique_arrangement[start][pos];
                    start_tuple = (
                        Characteristics::new(is_unique, candidate_start_key.len(), true, start),
                        candidate_start_key,
                        start,
                    );
                }
            }
        }
        self.order.insert(0, start_tuple);

        std::mem::replace(&mut self.order, Vec::new())
    }

    /// Introduces a specific input and keys to the order, along with its characteristics.
    ///
    /// This method places a next element in the order, and updates the associated state
    /// about other candidates, including which columns are now bound and which potential
    /// keys are available to consider (both arranged, and unarranged).
    fn order_input(&mut self, input: usize) {
        self.placed[input] = true;
        for (equivalence, expr_index) in self.reverse_equivalences[input].iter() {
            if !self.equivalences_active[*equivalence] {
                // Placing `input` *may* activate the equivalence. Each of its columns
                // come in to scope, which may result in an expression in `equivalence`
                // becoming fully defined (when its support is contained in placed inputs)
                let fully_supported = self
                    .input_mapper
                    .lookup_inputs(&self.equivalences[*equivalence][*expr_index])
                    .all(|i| self.placed[i]);
                if fully_supported {
                    self.equivalences_active[*equivalence] = true;
                    for expr in self.equivalences[*equivalence].iter() {
                        // find the relations that columns in the expression belong to
                        let mut rels = self.input_mapper.lookup_inputs(&expr);
                        // Skip the expression if
                        // * the expression is a literal -> this would translate
                        //   to `rels` being empty
                        // * the expression has columns belonging to more than
                        //   one relation -> TODO: see how we can plan better in
                        //   this case. Arguably, if this happens, it would
                        //   not be unreasonable to ask the user to write the
                        //   query better.
                        if let Some(rel) = rels.next() {
                            if rels.next().is_none() {
                                let expr = self.input_mapper.map_expr_to_local(expr.clone());

                                // Update bound columns.
                                self.bound[rel].push(expr);
                                self.bound[rel].sort();

                                // Reconsider all available arrangements.
                                for (pos, keys) in self.arrangements[rel].iter().enumerate() {
                                    if !self.arrangement_active[rel].contains(&pos) {
                                        // TODO: support the restoration of the
                                        // following original lines, which have been
                                        // commented out because Materialize may
                                        // panic otherwise. The original line and comments
                                        // here are:
                                        // Determine if the arrangement is viable, which happens when the
                                        // support of its keys are all bound.
                                        // if keys.iter().all(|k| k.support().iter().all(|c| self.bound[*rel].contains(&ScalarExpr::Column(*c))) {

                                        // Determine if the arrangement is viable,
                                        // which happens when all its keys are bound.
                                        if keys.iter().all(|k| self.bound[rel].contains(k)) {
                                            self.arrangement_active[rel].push(pos);
                                            // TODO: This could be pre-computed, as it is independent of the order.
                                            let is_unique = self.unique_arrangement[rel][pos];
                                            self.priority_queue.push((
                                                Characteristics::new(
                                                    is_unique,
                                                    keys.len(),
                                                    true,
                                                    rel,
                                                ),
                                                keys.clone(),
                                                rel,
                                            ));
                                        }
                                    }
                                }
                                let is_unique = self.unique_keys[rel].iter().any(|cols| {
                                    cols.iter().all(|c| {
                                        self.bound[rel].contains(&MirScalarExpr::Column(*c))
                                    })
                                });
                                self.priority_queue.push((
                                    Characteristics::new(
                                        is_unique,
                                        self.bound[rel].len(),
                                        false,
                                        rel,
                                    ),
                                    self.bound[rel].clone(),
                                    rel,
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
}
