// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apply rewrites to the Query Graph Model.

use crate::query_model::{BoxId, Model};
use std::collections::HashSet;

/// Trait that all rewrite rules must implement.
pub(crate) trait Rule {
    fn name(&self) -> &'static str;

    fn rule_type() -> ApplyStrategy
    where
        Self: Sized;

    /// Checks whether the given box should be rewritten.
    ///
    /// This method is not allowed to modify the model in any way.
    ///
    /// If this method should return true, the rule instance should be modified
    /// to store information about how the box model should be rewritten to avoid
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

/// Where and how a rule should be applied to boxes in the model.
#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum ApplyStrategy {
    // Apply only to the top box.
    TopBox,
    // Apply to all boxes. Customize with the order in which the boxes should be
    // visited.
    AllBoxes(VisitOrder),
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum VisitOrder {
    Pre,
    Post,
}

impl Default for VisitOrder {
    /// Call if a rule does not care about the order in which it is applied.
    fn default() -> Self {
        VisitOrder::Post
    }
}

/// Generates blank instances of a rule.
#[allow(missing_debug_implementations)]
struct RuleGenerator {
    /// The type of the rule.
    pub typ: ApplyStrategy,
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

/// Apply all available rewrite rules to the model.
pub fn rewrite_model(model: &mut Model) {
    let rules: Vec<RuleGenerator> = vec![];
    apply_rules_to_model(model, rules);
    model.garbage_collect();

    // At the end of the process, update box and quantifier ids to make it
    // easier to compare the graph before and after optimization.
    model.update_ids();
}

/// Transform the model by applying a list of rewrite rules.
fn apply_rules_to_model(model: &mut Model, rules: Vec<RuleGenerator>) {
    let (top_box, other): (Vec<RuleGenerator>, Vec<RuleGenerator>) = rules
        .into_iter()
        .partition(|r| matches!(r.typ, ApplyStrategy::TopBox));
    let (pre, post): (Vec<RuleGenerator>, Vec<RuleGenerator>) = other
        .into_iter()
        .partition(|r| matches!(r.typ, ApplyStrategy::AllBoxes(VisitOrder::Pre)));

    for rule in &top_box {
        apply_rule(&rule, model, model.top_box);
    }

    let mut rewritten = true;
    while rewritten {
        rewritten = false;

        rewritten |= apply_dft_rules(&pre, &post, model);

        for rule in &top_box {
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

/// Traverse the model depth-first, applying rules to each box.
///
/// To avoid the possibility of stack overflow, the implementation does not use
/// recursion. Instead, it keeps track of boxes we have entered using a `Vec`.
/// It also keeps track of boxes we have exited so that if multiple boxes are
/// parents of the same child box, the child will only be visited once.
///
/// Rules in `pre` are applied at enter-time, and rules in `post` are applied at
/// exit-time.
///
/// Returns whether any box was rewritten.
fn apply_dft_rules(pre: &Vec<RuleGenerator>, post: &Vec<RuleGenerator>, model: &mut Model) -> bool {
    let mut rewritten = false;

    // All nodes that have been entered but not exited. Last node in the vec is
    // the node that we most recently entered.
    let mut entered = Vec::new();
    // All nodes that have been exited.
    let mut exited = HashSet::new();

    fn find_next_child_to_enter(
        model: &mut Model,
        entered: &mut Vec<(BoxId, usize)>,
        exited: &mut HashSet<BoxId>,
    ) -> Option<BoxId> {
        let (box_id, traversed_quantifiers) = entered.last_mut().unwrap();
        let b = model.get_box(*box_id);
        for q_id in b.quantifiers.iter().skip(*traversed_quantifiers) {
            let q = model.get_quantifier(*q_id);
            *traversed_quantifiers += 1;
            if !exited.contains(&q.input_box) {
                return Some(q.input_box);
            }
        }
        return None;
    }

    // Enter the top box.
    for rule in pre {
        rewritten |= apply_rule(&rule, model, model.top_box);
    }
    entered.push((model.top_box, 0));
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(model, &mut entered, &mut exited) {
            for rule in pre {
                rewritten |= apply_rule(&rule, model, to_enter);
            }
            entered.push((to_enter, 0));
        } else {
            // If we can't descend any more, exit the current node we are in.
            let (box_id, _) = entered.pop().unwrap();
            for rule in post {
                rewritten |= apply_rule(&rule, model, box_id);
            }
            exited.insert(box_id);
        }
    }

    rewritten
}
