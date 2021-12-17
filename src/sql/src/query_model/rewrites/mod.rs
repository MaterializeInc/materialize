// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transform a Query Graph Model into a normalized, decorrelated form.

use crate::query_model::{BoxId, Model};
use std::collections::HashSet;

/// Trait that all rewrite rules must implement.
pub(crate) trait Rule {
    fn name(&self) -> &'static str;

    fn rule_type() -> RuleType
    where
        Self: Sized;

    /// Checks whether the given box should be rewritten.
    ///
    /// This method is not allowed to modify the model in any way.
    ///
    /// If this method should return true, the rule instance should modified to
    /// store information about how the box should be rewritten to avoid
    /// duplicating work.
    ///
    /// If this method returns false, the rewrite rule instance will be
    /// destroyed immediately.
    fn check(&mut self, model: &Model, box_id: BoxId) -> bool;

    /// Rewrite the box according to the rule.
    ///
    /// Invoked immediately after [Rule::check] if it returned true.
    ///
    /// The rewrite rule instance will be destroyed immediately after this
    /// call.
    fn rewrite(&mut self, model: &mut Model, box_id: BoxId);
}

/// Classifies the rule by which boxes it should be applied to and in what
/// order.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum RuleType {
    /// The rule should be applied to all boxes.
    /// Order of the boxes does not matter.
    AllBoxes,
    /// The rule should be applied to all boxes in pre-order.
    PreOrder,
    /// The rule should be applied to all boxes in post-order.
    PostOrder,
    /// The rule should only be applied to the top box.
    TopBoxOnly,
}

/// Generates blank instances of a rule.
#[allow(missing_debug_implementations)]
struct RuleGenerator {
    /// The type of the rule.
    pub typ: RuleType,
    /// Function that creates a blank instance of the rule.
    pub generate: Box<dyn Fn() -> Box<dyn Rule>>,
}

/// Create a RuleGenerator that generates blank instances of `R`.
#[allow(dead_code)]
fn make_generator<R: 'static + Rule + Default>() -> RuleGenerator {
    RuleGenerator {
        typ: R::rule_type(),
        generate: Box::new(|| Box::new(R::default())),
    }
}

/// Entry-point of the normalization stage.
pub fn rewrite_model(model: &mut Model) {
    // TODO: populate this list.
    let mut rules: Vec<RuleGenerator> = vec![];
    apply_rules_to_model(model, &mut rules);
    model.garbage_collect();

    // At the end of the process, update box and quantifier ids to make it
    // easier to compare the graph before and after optimization.
    model.update_ids();
}

/// Transform the model by applying a list of rewrite rules.
fn apply_rules_to_model(model: &mut Model, rules: &mut Vec<RuleGenerator>) {
    let mut rewritten = true;

    while rewritten {
        rewritten = false;

        for rule in rules
            .iter_mut()
            .filter(|r| matches!(r.typ, RuleType::TopBoxOnly))
        {
            rewritten |= apply_rule(&rule, model, model.top_box);
        }

        rewritten |= recursively_apply_rules(rules, model, model.top_box, &mut HashSet::new());

        for rule in rules
            .iter_mut()
            .filter(|r| matches!(r.typ, RuleType::TopBoxOnly))
        {
            rewritten |= apply_rule(&rule, model, model.top_box);
        }
    }
}

/// Applies the rewrite rule corresponding to the `rule_generator` to a box.
///
/// Returns whether the box was rewritten.
fn apply_rule(rule_generator: &RuleGenerator, model: &mut Model, box_id: BoxId) -> bool {
    // Create a fresh instance of the rule.
    let mut rule = (*rule_generator.generate)();
    let rewrite = rule.check(model, box_id);
    if rewrite {
        rule.rewrite(model, box_id);
    }
    rewrite
}

/// Apply the given list of rewrite rules to the given box and
/// all descendent boxes.
///
/// `visited_boxes` keeps track of all the visited boxes so far
/// to avoid visiting them again.
///
/// Returns whether any box was rewritten.
fn recursively_apply_rules(
    rules: &mut Vec<RuleGenerator>,
    model: &mut Model,
    box_id: BoxId,
    visited_boxes: &mut HashSet<BoxId>,
) -> bool {
    let mut rewritten = false;

    if visited_boxes.insert(box_id) {
        for rule in rules
            .iter_mut()
            .filter(|r| matches!(r.typ, RuleType::PreOrder))
        {
            rewritten |= apply_rule(&rule, model, box_id);
        }

        let quantifiers = model.get_box(box_id).quantifiers.clone();
        for q_id in quantifiers {
            let input_box = model.get_quantifier(q_id).input_box;
            rewritten |= recursively_apply_rules(rules, model, input_box, visited_boxes);
        }

        for rule in rules
            .iter_mut()
            .filter(|r| matches!(r.typ, RuleType::PostOrder | RuleType::AllBoxes))
        {
            rewritten |= apply_rule(&rule, model, box_id);
        }
    }
    rewritten
}
