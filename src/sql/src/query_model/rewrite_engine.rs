// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::{
    BoxId, BoxScalarExpr, BoxType, ColumnReference, DistinctOperation, Model, QuantifierId,
    QuantifierSet, QuantifierType, Select,
};
use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Rule type
#[allow(dead_code)]
#[derive(Debug)]
pub enum RuleType {
    TopBoxOnly,
    PreOrder,
    PostOrder,
}

/// Trait for rewrite rules
pub trait Rule {
    fn name(&self) -> &'static str;

    fn rule_type(&self) -> RuleType;

    /// Whether the action should be fired for the given box.
    /// This method is not allowed to modify the model in any way.
    fn condition(&mut self, model: &Model, box_id: BoxId) -> bool;

    /// Invoked immediately after `condition` if it returned true.
    fn action(&mut self, model: &mut Model, box_id: BoxId);
}

/// Entry-point of the normalization stage.
pub fn rewrite_model(model: &mut Model) {
    let mut rules: Vec<Box<dyn Rule>> = vec![
        Box::new(ColumnRemoval::new()),
        Box::new(SelectMerge::new()),
        Box::new(ConstantLifting::new()),
        Box::new(WindowingToSelect::new()),
    ];

    apply_rules_to_model(model, &mut rules);

    model.garbage_collect();

    let mut decorrelation_rules: Vec<Box<dyn Rule>> = vec![Box::new(Decorrelation::new())];

    apply_rules_to_model(model, &mut decorrelation_rules);

    model.garbage_collect();

    model.update_ids();
}

/// Transform the model by applying a list of rewrite rules.
pub fn apply_rules_to_model(model: &mut Model, rules: &mut Vec<Box<dyn Rule>>) {
    for rule in rules
        .iter_mut()
        .filter(|r| matches!(r.rule_type(), RuleType::TopBoxOnly))
    {
        apply_rule(&mut **rule, model, model.top_box);
    }

    deep_apply_rules(rules, model, model.top_box, &mut HashSet::new());

    for rule in rules
        .iter_mut()
        .filter(|r| matches!(r.rule_type(), RuleType::TopBoxOnly))
    {
        apply_rule(&mut **rule, model, model.top_box);
    }
}

/// Apply a rewrite rule to a given box within the Model if it matches the condition.
fn apply_rule(rule: &mut dyn Rule, model: &mut Model, box_id: BoxId) {
    if rule.condition(model, box_id) {
        rule.action(model, box_id);
        println!(
            "{}",
            crate::query_model::dot::DotGenerator::new()
                .generate(&model, rule.name())
                .unwrap()
        );
    }
}

/// Descend and apply recursively the given list of rewrite rules to all boxes within
/// the subgraph starting in the given box. `visited_boxes` keeps track of all the
/// visited boxes so far, to avoid visiting them again.
fn deep_apply_rules(
    rules: &mut Vec<Box<dyn Rule>>,
    model: &mut Model,
    box_id: BoxId,
    visited_boxes: &mut HashSet<BoxId>,
) {
    if visited_boxes.insert(box_id) {
        for rule in rules
            .iter_mut()
            .filter(|r| matches!(r.rule_type(), RuleType::PreOrder))
        {
            apply_rule(&mut **rule, model, box_id);
        }

        let quantifiers = model.get_box(box_id).quantifiers.clone();
        for q_id in quantifiers {
            let input_box = model.get_quantifier(q_id).input_box;
            deep_apply_rules(rules, model, input_box, visited_boxes);
        }

        for rule in rules
            .iter_mut()
            .filter(|r| matches!(r.rule_type(), RuleType::PostOrder))
        {
            apply_rule(&mut **rule, model, box_id);
        }
    }
}

/// Merges nested select boxes.
struct SelectMerge {
    to_merge: QuantifierSet,
}

impl SelectMerge {
    fn new() -> Self {
        Self {
            /// Set of quantifiers to be removed from the current box
            to_merge: BTreeSet::new(),
        }
    }
}

impl Rule for SelectMerge {
    fn name(&self) -> &'static str {
        "SelectMerge"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::PostOrder
    }

    fn condition(&mut self, model: &Model, box_id: BoxId) -> bool {
        self.to_merge.clear();
        let outer_box = model.get_box(box_id);
        if let BoxType::Select(_outer_select) = &outer_box.box_type {
            for q_id in outer_box.quantifiers.iter() {
                let q = model.get_quantifier(*q_id);

                // Only Select boxes under Foreach quantifiers can be merged
                // into the parent Select box.
                if let QuantifierType::Foreach = q.quantifier_type {
                    let input_box = model.get_box(q.input_box);

                    // TODO(asenac) clone shared boxes
                    if input_box.ranging_quantifiers.len() == 1 {
                        if let BoxType::Select(inner_select) = &input_box.box_type {
                            if input_box.distinct != DistinctOperation::Enforce
                                && inner_select.order_key.is_none()
                                && inner_select.limit.is_none()
                            {
                                self.to_merge.insert(*q_id);
                            }
                        }
                    }
                }
            }
        }
        !self.to_merge.is_empty()
    }

    fn action(&mut self, model: &mut Model, box_id: BoxId) {
        // Dereference all the expressions in the sub-graph referencing the quantifiers
        // that are about to be squashed into the current box.
        let _ = model.visit_pre_boxes_in_subgraph_mut(
            &mut |mut b| -> Result<(), ()> {
                b.visit_expressions_mut(&mut |expr: &mut BoxScalarExpr| -> Result<(), ()> {
                    expr.visit_mut(&mut |expr| {
                        if let BoxScalarExpr::ColumnReference(c) = expr {
                            if self.to_merge.contains(&c.quantifier_id) {
                                let inner_box = model.get_quantifier(c.quantifier_id).input_box;
                                let inner_box = model.get_box(inner_box);

                                *expr = inner_box.columns[c.position].expr.clone();
                            }
                        }
                    });
                    Ok(())
                })?;
                Ok(())
            },
            box_id,
        );

        // Add all the quantifiers in the input boxes of the quantifiers to be
        // merged into the current box
        let mut outer_box = model.get_mut_box(box_id);
        for q_id in self.to_merge.iter() {
            outer_box.quantifiers.remove(q_id);

            let input_box_id = model.get_mut_quantifier(*q_id).input_box;
            let input_box = model.get_box(input_box_id);
            for child_q in input_box.quantifiers.iter() {
                model.get_mut_quantifier(*child_q).parent_box = box_id;
                outer_box.quantifiers.insert(*child_q);
            }
            if let Some(predicates) = input_box.get_predicates() {
                for p in predicates.iter() {
                    outer_box.add_predicate(p.clone());
                }
            }
        }
    }
}

/// Replaces any column reference pointing to a constant that can be lifted
/// with the constant value pointed.
///
/// Constants can only be lifted from Foreach quantifiers.
///
/// TODO(asenac) For unions, we can only lift a constant if all the branches
/// project the same constant in the same position.
struct ConstantLifting {}

impl ConstantLifting {
    fn new() -> Self {
        Self {}
    }
}

impl Rule for ConstantLifting {
    fn name(&self) -> &'static str {
        "ConstantLifting"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::PostOrder
    }

    fn condition(&mut self, model: &Model, box_id: BoxId) -> bool {
        // No need to handle outer joins here since, once they are
        // normalized, their preserving quantifier is in a Select box.
        // TODO(asenac) grouping and unions
        model.get_box(box_id).is_select()
    }

    fn action(&mut self, model: &mut Model, box_id: BoxId) {
        let mut the_box = model.get_mut_box(box_id);

        // Dereference all column references and check whether the referenced
        // expression is constant within the context of the box it belongs to.
        let _ = the_box.visit_expressions_mut(&mut |e| -> Result<(), ()> {
            e.visit_mut(&mut |e| {
                if let BoxScalarExpr::ColumnReference(c) = e {
                    let q = model.get_quantifier(c.quantifier_id);
                    if let QuantifierType::Foreach = q.quantifier_type {
                        let input_box = model.get_box(q.input_box);
                        if !input_box.is_data_source()
                            && input_box.columns[c.position]
                                .expr
                                .is_constant_within_context(&input_box.quantifiers)
                        {
                            *e = input_box.columns[c.position].expr.clone();
                        }
                    }
                }
            });
            Ok(())
        });
    }
}

struct ColumnRemoval {
    remap: HashMap<BoxId, HashMap<usize, usize>>,
}

impl ColumnRemoval {
    fn new() -> Self {
        Self {
            remap: HashMap::new(),
        }
    }
}

impl Rule for ColumnRemoval {
    fn name(&self) -> &'static str {
        "ColumnRemoval"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::TopBoxOnly
    }

    fn condition(&mut self, model: &Model, top_box: BoxId) -> bool {
        self.remap.clear();
        // used columns per box
        let mut used_columns = HashMap::new();
        for (_, b) in model.boxes.iter() {
            let _ = b
                .borrow()
                .visit_expressions(&mut |expr: &BoxScalarExpr| -> Result<(), ()> {
                    // TODO(asenac) Unions
                    expr.visit(&mut |expr: &BoxScalarExpr| {
                        if let BoxScalarExpr::ColumnReference(c) = expr {
                            let box_id = model.get_quantifier(c.quantifier_id).input_box;
                            used_columns
                                .entry(box_id)
                                .or_insert_with(BTreeSet::new)
                                .insert(c.position);
                        }
                    });
                    Ok(())
                });
        }
        for (box_id, b) in model.boxes.iter().filter(|(box_id, _)| **box_id != top_box) {
            let b = b.borrow();
            // All columns projected by a DISTINCT box are used implictly
            if b.distinct != DistinctOperation::Enforce {
                let columns = used_columns.entry(*box_id).or_insert_with(BTreeSet::new);
                if columns.len() != b.columns.len() {
                    // Not all columns are used, re-map
                    let mut remap = HashMap::new();
                    for (new_position, position) in columns.iter().enumerate() {
                        remap.insert(*position, new_position);
                    }
                    self.remap.insert(*box_id, remap);
                }
            }
        }
        !self.remap.is_empty()
    }

    fn action(&mut self, model: &mut Model, top_box: BoxId) {
        loop {
            for (box_id, b) in model.boxes.iter() {
                if let Some(remap) = self.remap.get(box_id) {
                    // Remove the unused columns projected by this box
                    let mut b = b.borrow_mut();
                    b.columns = b
                        .columns
                        .drain(..)
                        .enumerate()
                        .filter_map(|(position, c)| {
                            if let Some(_) = remap.get(&position) {
                                Some(c)
                            } else {
                                None
                            }
                        })
                        .collect();
                }

                let _ = b.borrow_mut().visit_expressions_mut(
                    &mut |expr: &mut BoxScalarExpr| -> Result<(), ()> {
                        expr.visit_mut(&mut |expr: &mut BoxScalarExpr| {
                            if let BoxScalarExpr::ColumnReference(c) = expr {
                                let box_id = model.get_quantifier(c.quantifier_id).input_box;
                                if let Some(remap) = self.remap.get(&box_id) {
                                    c.position = remap[&c.position];
                                }
                            }
                        });
                        Ok(())
                    },
                );
            }
            // Removing some column references may result in other columns
            // to become unused
            if !self.condition(model, top_box) {
                break;
            }
        }
    }
}

/// Implements query decorrelation as a transformation rule.
///
/// A correlated box is a box where at least one of its quantifiers points
/// to a sub-graph that references columns from other quantifiers in the
/// correlated box.
///
/// Only join boxes (Select and OuterJoin boxes) can possibly correlated,
/// but not full outer joins (OuterJoin box without a preserving quantifier).
///
/// This transformation decorrelates all correlated boxes in the model
/// during a post order traversal, by apply the correlated quantifier to
/// an outer relation producing all the distinct combinations of the columns
/// the correlated quantifier depends on. The decorrelated box projects
/// all the columns from the outer relation as a suffix. The decorrelated
/// quantifier must be then joined with the columns from quantifiers in the
/// current join it used to be correlated with.
///
/// Note: a quantifier cannot be correlated with itself, that is called
/// recursion instead.
struct Decorrelation {
    /// The correlation information of the quantifiers in the current box
    /// being evaluated.
    correlation_info: BTreeMap<QuantifierId, HashSet<ColumnReference>>,
}

type CacheEntry = (
    // Join box Id, usedd as the outer relation for decorrelation
    BoxId,
    // Column map
    HashMap<ColumnReference, usize>,
    // CTE cache
    HashMap<BoxId, BoxId>,
);

impl Decorrelation {
    fn new() -> Self {
        Self {
            correlation_info: BTreeMap::new(),
        }
    }

    /// Returns a decorrelated version of the input relation, ie. that is not correlated with
    /// any of the columns in the `column_map`.
    ///
    /// The resulting box projects the formerly correlated columns at the end of its projection.
    fn decorrelate_internal(
        &mut self,
        model: &mut Model,
        outer_relation: BoxId,
        column_map: &HashMap<ColumnReference, usize>,
        input_relation: BoxId,
        decorrelated_boxes: &mut HashMap<BoxId, BoxId>,
    ) -> BoxId {
        let mut clone_box_and_decorrelate_inputs =
            |model: &mut Model, box_id: BoxId| -> (BoxId, Vec<(QuantifierId, usize)>) {
                let new_box = model.clone_box(box_id);
                let input_quantifiers = model.get_box(new_box).quantifiers.clone();
                let quantifiers_and_offsets = input_quantifiers
                    .iter()
                    .map(|q_id| {
                        let input_box = model.get_quantifier(*q_id).input_box;
                        let old_arity = model.get_box(input_box).columns.len();
                        let decorrelated_box = self.decorrelate(
                            model,
                            outer_relation,
                            column_map,
                            input_box,
                            decorrelated_boxes,
                        );
                        model.update_input_box(*q_id, decorrelated_box);
                        (*q_id, old_arity)
                    })
                    .collect_vec();
                (new_box, quantifiers_and_offsets)
            };

        let project_outer_columns =
            |model: &Model,
             box_id: BoxId,
             input_quantifier: QuantifierId,
             first_outer_column: usize| {
                let mut the_box = model.get_mut_box(box_id);
                for i in 0..column_map.len() {
                    let col_ref = BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: input_quantifier,
                        position: first_outer_column + i,
                    });
                    the_box.add_column(col_ref);
                }
            };

        // TODO(asenac) avoid this cloning
        let box_type = model.get_box(input_relation).box_type.clone();
        let (new_box, input_quantifier_and_first_column) = match box_type {
            BoxType::Select(_) => {
                let (new_select_box, quantifiers_and_offsets) =
                    clone_box_and_decorrelate_inputs(model, input_relation);

                let outer_columns = column_map.len();
                // join the input quantifiers by their outer columns
                let mut it = quantifiers_and_offsets.iter();
                if let Some((first_quantifier, first_offset)) = it.next() {
                    let mut select_box = model.get_mut_box(new_select_box);
                    for (other_quantifier, other_offset) in it {
                        for i in 0..outer_columns {
                            let left = BoxScalarExpr::ColumnReference(ColumnReference {
                                quantifier_id: *first_quantifier,
                                position: *first_offset + i,
                            });
                            let right = BoxScalarExpr::ColumnReference(ColumnReference {
                                quantifier_id: *other_quantifier,
                                position: *other_offset + i,
                            });
                            let cmp = BoxScalarExpr::CallBinary {
                                // @todo BinaryFunc::ValueEq
                                func: expr::BinaryFunc::Eq,
                                expr1: Box::new(left),
                                expr2: Box::new(right),
                            };
                            select_box.add_predicate(cmp);
                        }
                    }
                    drop(select_box);

                    // Extend the projection of the select box to include the outer columns
                    // at the end
                    project_outer_columns(model, new_select_box, *first_quantifier, *first_offset);
                }

                (
                    new_select_box,
                    quantifiers_and_offsets.into_iter().find(|(q_id, _)| {
                        model.get_quantifier(*q_id).quantifier_type == QuantifierType::Foreach
                    }),
                )
            }
            BoxType::Grouping(_) => {
                let (new_grouping_box, quantifiers_and_offsets) =
                    clone_box_and_decorrelate_inputs(model, input_relation);

                if let Some((first_quantifier, first_offset)) =
                    quantifiers_and_offsets.iter().next()
                {
                    // Extend the projection of the box to include the outer columns at the end
                    project_outer_columns(
                        model,
                        new_grouping_box,
                        *first_quantifier,
                        *first_offset,
                    );
                }

                // Add the outer columns to the grouping key. They will be rewritten later
                // in terms of the input quantifier
                if let BoxType::Grouping(grouping) =
                    &mut model.get_mut_box(new_grouping_box).box_type
                {
                    grouping.key.extend(
                        column_map
                            .iter()
                            .sorted_by_key(|(_, position)| **position)
                            .map(|(col_ref, _)| BoxScalarExpr::ColumnReference(col_ref.clone())),
                    );
                }

                (new_grouping_box, quantifiers_and_offsets.into_iter().next())
            }
            BoxType::Windowing => {
                let (new_windowing_box, quantifiers_and_offsets) =
                    clone_box_and_decorrelate_inputs(model, input_relation);

                if let Some((first_quantifier, first_offset)) =
                    quantifiers_and_offsets.iter().next()
                {
                    // Extend the projection of the box to include the outer columns at the end
                    project_outer_columns(
                        model,
                        new_windowing_box,
                        *first_quantifier,
                        *first_offset,
                    );
                }

                // Add the outer columns to the partition key of all window functions.
                let mut the_box = model.get_mut_box(new_windowing_box);
                let key_suffix = column_map
                    .iter()
                    .sorted_by_key(|(_, position)| **position)
                    .map(|(col_ref, _)| BoxScalarExpr::ColumnReference(col_ref.clone()))
                    .collect_vec();

                for c in the_box.columns.iter_mut() {
                    if let BoxScalarExpr::Windowing(expr) = &mut c.expr {
                        expr.partition.extend(key_suffix.clone());
                    }
                }

                (
                    new_windowing_box,
                    quantifiers_and_offsets.into_iter().next(),
                )
            }
            BoxType::Union | BoxType::Except | BoxType::Intersect => {
                let (new_box, quantifiers_and_offsets) =
                    clone_box_and_decorrelate_inputs(model, input_relation);

                if let Some((first_quantifier, first_offset)) =
                    quantifiers_and_offsets.iter().next()
                {
                    // Extend the projection of the box to include the outer columns at the end
                    project_outer_columns(model, new_box, *first_quantifier, *first_offset);
                }

                (new_box, quantifiers_and_offsets.into_iter().next())
            }
            BoxType::Get(_) => {
                let join_box = model.make_select_box();
                model.make_quantifier(QuantifierType::Foreach, input_relation, join_box);
                model.make_quantifier(QuantifierType::Foreach, outer_relation, join_box);
                model.get_mut_box(join_box).add_all_input_columns(model);
                (join_box, None)
            }
            _ => panic!("Cannot decorrelated this type of box"),
        };

        // Rewrite any expression within the box that references correlated columns.
        // Replace them with column references to an input quantifier projecting the
        // decorrelated columns at some offset.
        if let Some((input_quantifier, first_column)) = input_quantifier_and_first_column {
            let _ =
                model
                    .get_mut_box(new_box)
                    .visit_expressions_mut(&mut |expr| -> Result<(), ()> {
                        expr.visit_mut(&mut |expr| {
                            if let BoxScalarExpr::ColumnReference(col_ref) = expr {
                                if let Some(position) = column_map.get(col_ref) {
                                    col_ref.quantifier_id = input_quantifier;
                                    col_ref.position = first_column + *position;
                                }
                            }
                        });
                        Ok(())
                    });

            // Update correlated references among quantifiers in the current box
            // TODO(asenac) this can be removed when full decorrelation is supported, since applying this
            // rule in PostOrder means that the joins being decorrelated are not correlated internally.
            let quantifier_map = model
                .get_box(input_relation)
                .quantifiers
                .iter()
                .zip(model.get_box(new_box).quantifiers.iter())
                .map(|(old, new)| (*old, *new))
                .collect::<HashMap<_, _>>();
            if !quantifier_map.is_empty() {
                let _ = model.visit_pre_boxes_in_subgraph_mut(
                    &mut |mut b| -> Result<(), ()> {
                        b.remap_column_references(&quantifier_map);
                        Ok(())
                    },
                    new_box,
                );
            }
        }

        new_box
    }

    /// Returns the decorrelated version of `input_relation`. It avoids decorrelating
    /// the same box several times (for example, when decorrelating CTEs) by using
    /// `decorrelated_boxes` as a cache.
    fn decorrelate(
        &mut self,
        model: &mut Model,
        outer_relation: BoxId,
        column_map: &HashMap<ColumnReference, usize>,
        input_relation: BoxId,
        decorrelated_boxes: &mut HashMap<BoxId, BoxId>,
    ) -> BoxId {
        if let Some(decorrelated_box) = decorrelated_boxes.get(&input_relation) {
            *decorrelated_box
        } else {
            let decorrelated_box = self.decorrelate_internal(
                model,
                outer_relation,
                column_map,
                input_relation,
                decorrelated_boxes,
            );
            decorrelated_boxes.insert(input_relation, decorrelated_box);
            decorrelated_box
        }
    }

    /// Builds a distinct sub-join with the quantifiers referenced in `dependencies` that
    /// can be used as the outer relation for the decorrelation of a quantifier with these
    /// dependencies.
    ///
    /// A cache is used to avoid creating the sub-graph when two or more quantifiers from
    /// the same `join_box` have the same set of dependencies.
    fn build_outer_relation_from_dependencies<'a>(
        model: &mut Model,
        _join_box: BoxId,
        dependencies: HashSet<ColumnReference>,
        cache: &'a mut HashMap<Vec<ColumnReference>, CacheEntry>,
    ) -> &'a mut CacheEntry {
        let dependencies = dependencies.into_iter().sorted().collect_vec();
        cache.entry(dependencies.clone()).or_insert_with(move || {
            // Build a join among the dependencies of the current quantifier
            let outer_relation = model.make_select_box();
            // Maps correlated column references with column positions in the outer_relation
            let mut column_map = HashMap::new();
            for (dependent_quantifier, col_refs) in &dependencies
                .into_iter()
                .sorted()
                .group_by(|c| c.quantifier_id)
            {
                // Clone the quantifier in the new join box (outer_relation)
                let (quantifier_type, input_box) = {
                    let q = model.get_quantifier(dependent_quantifier);
                    (q.quantifier_type.clone(), q.input_box)
                };
                let new_quantifier =
                    model.make_quantifier(quantifier_type, input_box, outer_relation);

                // Make `outer_relation` project all the columns from this quantifier
                // the caller depends on
                let mut outer_box = model.get_mut_box(outer_relation);
                for col_ref in col_refs {
                    let new_position = outer_box.columns.len();
                    outer_box.add_column(BoxScalarExpr::ColumnReference(ColumnReference {
                        quantifier_id: new_quantifier,
                        position: col_ref.position,
                    }));

                    column_map.insert(col_ref.clone(), new_position);
                }
            }

            model.get_mut_box(outer_relation).distinct = DistinctOperation::Enforce;

            // @todo add all predicates from `join_box` that only reference
            // dependent quantifiers

            (outer_relation, column_map, HashMap::new())
        })
    }
}

impl Rule for Decorrelation {
    fn name(&self) -> &'static str {
        "Decorrelation"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::PostOrder
    }

    fn condition(&mut self, model: &Model, box_id: BoxId) -> bool {
        let b = model.get_box(box_id);
        if b.is_select() {
            self.correlation_info = b.correlation_info(model);
            // @todo For now we only decorrelate Foreach quantifiers. Add
            // support for the remaining type of quantifiers
            !self.correlation_info.is_empty()
                && self.correlation_info.iter().all(|(q_id, _)| {
                    model.get_quantifier(*q_id).quantifier_type == QuantifierType::Foreach
                })
        } else {
            false
        }
    }

    fn action(&mut self, model: &mut Model, box_id: BoxId) {
        let mut non_correlated_quantifires = model
            .get_box(box_id)
            .quantifiers
            .iter()
            .filter(|q| !self.correlation_info.contains_key(q))
            .cloned()
            .collect::<QuantifierSet>();

        // Cache with the `outer_relation` used for decorrelation.
        let mut outer_relation_cache = HashMap::new();

        let mut remaining_quantifiers = BTreeMap::new();
        std::mem::swap(&mut self.correlation_info, &mut remaining_quantifiers);
        let mut remaining_quantifiers = remaining_quantifiers.into_iter().collect_vec();

        // Find the first correlated operand whose dependencies are satisfied
        while let Some(position) = remaining_quantifiers.iter().position(|(_, dependencies)| {
            dependencies
                .iter()
                .all(|c| non_correlated_quantifires.contains(&c.quantifier_id))
        }) {
            let (quantifier_id, dependencies) = remaining_quantifiers.remove(position);

            // Build the outer relation joining the dependencies of the current quantifier
            let (outer_relation, column_map, cte_cache) =
                Self::build_outer_relation_from_dependencies(
                    model,
                    box_id,
                    dependencies,
                    &mut outer_relation_cache,
                );

            let correlated_box = model.get_quantifier(quantifier_id).input_box;
            let old_arity = model.get_box(correlated_box).columns.len();
            let decorrelated_box = self.decorrelate(
                model,
                *outer_relation,
                column_map,
                correlated_box,
                cte_cache,
            );
            // Make the correlated quantifier point to the new correlated box, so it becomes
            // a decorrelated quantifier.
            model.update_input_box(quantifier_id, decorrelated_box);

            // Join the decorrelated quantifier with the columns from the current select
            // box it used to reference
            for (col_ref, position) in column_map.iter().sorted_by_key(|(_, position)| **position) {
                let right = BoxScalarExpr::ColumnReference(ColumnReference {
                    quantifier_id,
                    position: old_arity + position,
                });
                let cmp = BoxScalarExpr::CallBinary {
                    // @todo BinaryFunc::ValueEq
                    func: expr::BinaryFunc::Eq,
                    expr1: Box::new(BoxScalarExpr::ColumnReference(col_ref.clone())),
                    expr2: Box::new(right),
                };
                model.get_mut_box(box_id).add_predicate(cmp);
            }

            non_correlated_quantifires.insert(quantifier_id);
        }

        // Remove any non-longer referenced objects from the model. This helps at catching bugs
        // in the decorrelation logic.
        model.garbage_collect();

        // Simplify the decorrelated box by fusing any redundant `Select` boxes, for example,
        // those added when the decorrelation logic is applied to a `Get` operator.
        let mut rules: Vec<Box<dyn Rule>> = vec![Box::new(SelectMerge::new())];
        deep_apply_rules(&mut rules, model, box_id, &mut HashSet::new());
    }
}

/// Any windowing box not projecting the result of any windowing function can be converted
/// into a regular Select box.
struct WindowingToSelect {}

impl WindowingToSelect {
    fn new() -> Self {
        Self {}
    }
}

impl Rule for WindowingToSelect {
    fn name(&self) -> &'static str {
        "WindowingToSelect"
    }

    fn rule_type(&self) -> RuleType {
        RuleType::PostOrder
    }

    fn condition(&mut self, model: &Model, box_id: BoxId) -> bool {
        let b = model.get_box(box_id);
        if let BoxType::Windowing = &b.box_type {
            !b.columns
                .iter()
                .any(|c| matches!(c.expr, BoxScalarExpr::Windowing(..)))
        } else {
            false
        }
    }

    fn action(&mut self, model: &mut Model, box_id: BoxId) {
        let mut b = model.get_mut_box(box_id);
        b.box_type = BoxType::Select(Select::new());
    }
}
