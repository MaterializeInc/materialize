// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::{
    BoxId, BoxScalarExpr, BoxType, DistinctOperation, Model, QuantifierSet, QuantifierType,
};
use std::collections::{BTreeSet, HashMap, HashSet};

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
    ];

    apply_rules_to_model(model, &mut rules);

    model.garbage_collect();
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
