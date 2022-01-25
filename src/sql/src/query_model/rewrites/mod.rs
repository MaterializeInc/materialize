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

    fn strategy(&self) -> ApplyStrategy;

    /// Determines how to rewrite the box.
    ///
    /// Returns None if the box does not need to be rewritten.
    fn check(&self, model: &Model, box_id: BoxId) -> Option<Box<dyn Rewrite>>;
}

/// Trait for something that modifies a Query Graph Model.
pub(crate) trait Rewrite {
    fn rewrite(&mut self, model: &mut Model);
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

/// Apply all available rewrite rules to the model.
pub fn rewrite_model(model: &mut Model) {
    let rules: Vec<Box<dyn Rule>> = vec![];
    apply_rules_to_model(model, rules);
    model.garbage_collect();

    // At the end of the process, update box and quantifier ids to make it
    // easier to compare the graph before and after optimization.
    model.update_ids();
}

/// Transform the model by applying a list of rewrite rules.
fn apply_rules_to_model(model: &mut Model, rules: Vec<Box<dyn Rule>>) {
    let (top_box, other): (Vec<Box<dyn Rule>>, Vec<Box<dyn Rule>>) = rules
        .into_iter()
        .partition(|r| matches!(r.strategy(), ApplyStrategy::TopBox));
    let (pre, post): (Vec<Box<dyn Rule>>, Vec<Box<dyn Rule>>) = other
        .into_iter()
        .partition(|r| matches!(r.strategy(), ApplyStrategy::AllBoxes(VisitOrder::Pre)));

    for rule in &top_box {
        apply_rule(rule.as_ref(), model, model.top_box);
    }

    let mut rewritten = true;
    while rewritten {
        rewritten = false;

        rewritten |= apply_dft_rules(&pre, &post, model);

        for rule in &top_box {
            rewritten |= apply_rule(rule.as_ref(), model, model.top_box);
        }
    }
}

/// Applies the rewrite rule corresponding to the `rule` to a box.
///
/// Returns whether the box was rewritten.
fn apply_rule(rule: &dyn Rule, model: &mut Model, box_id: BoxId) -> bool {
    let rewrite = rule.check(model, box_id);
    if let Some(mut rewrite) = rewrite {
        rewrite.rewrite(model);
        true
    } else {
        false
    }
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
fn apply_dft_rules(pre: &Vec<Box<dyn Rule>>, post: &Vec<Box<dyn Rule>>, model: &mut Model) -> bool {
    let mut rewritten = false;

    // All nodes that have been entered but not exited. Last node in the vec is
    // the node that we most recently entered.
    let mut entered = Vec::new();
    // All nodes that have been exited.
    let mut exited = HashSet::new();

    // In our current node, find the next child box, if any, that we have not entered.
    fn find_next_child_to_enter(
        model: &Model,
        entered: &mut Vec<(BoxId, usize)>,
        exited: &HashSet<BoxId>,
    ) -> Option<BoxId> {
        let (box_id, traversed_quantifiers) = entered.last_mut().unwrap();
        let b = model.get_box(*box_id);
        for q in b.input_quantifiers().skip(*traversed_quantifiers) {
            *traversed_quantifiers += 1;
            if !exited.contains(&q.input_box) {
                return Some(q.input_box);
            }
        }
        return None;
    }

    // Pseudocode for the recursive version of this function would look like:
    // ```
    // apply_preorder_rules()
    // foreach quantifier:
    //    recursive_call(quantifier.input_box)
    // apply_postorder_rules()
    // ```
    // In this non-recursive implementation, you can think of the call stack as
    // been replaced by `entered`. Every time an object is pushed into `entered`
    // would have been a time you would have pushed a recursive call onto the
    // call stack. Likewise, times an object is popped from `entered` would have
    // been times when recursive calls leave the stack.

    // Start from the top box.
    entered.push((model.top_box, 0));
    for rule in pre {
        rewritten |= apply_rule(rule.as_ref(), model, model.top_box);
    }
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(model, &mut entered, &exited) {
            entered.push((to_enter, 0));
            for rule in pre {
                rewritten |= apply_rule(rule.as_ref(), model, to_enter);
            }
        } else {
            // If this box has no more children to descend into,
            // run PostOrder rules and exit the current box.
            let (box_id, _) = entered.last().unwrap();
            for rule in post {
                rewritten |= apply_rule(rule.as_ref(), model, *box_id);
            }
            exited.insert(*box_id);
            entered.pop();
        }
    }

    rewritten
}
