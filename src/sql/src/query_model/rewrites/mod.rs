// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apply rewrites to [`Model`] instances.

use crate::query_model::{BoxId, Model};
use std::collections::HashSet;

/// Trait that all rewrite rules must implement.
pub(crate) trait Rule {
    /// The type of matches associated with this [`Rule`].
    type Match;

    /// The name of the rule, used for debugging and tracing purposes.
    fn name(&self) -> &'static str;

    /// The [`ApplyStrategy`] for the rule.
    fn strategy(&self) -> ApplyStrategy;

    /// Determines how to rewrite the box.
    ///
    /// Returns None if the box does not need to be rewritten.
    fn check(&self, model: &Model, box_id: BoxId) -> Option<Self::Match>;

    /// Rewrites the [`Model`] based on the parameters of [`Self::Match`]
    /// determined by a previous [`Rule::check`] call.
    fn rewrite(&self, model: &mut Model, mat: Self::Match);
}

/// A trait with a blanket implementation that abstracts over the
/// associated [`Rule::Match`] and its use sites [`Rule::check`] and [`Rule::rewrite`]
/// behind [`ApplyRule::apply`] method.
///
/// Allows for holding a vector of rules in a `Vec<dyn ApplyRule>` instances.
pub(crate) trait ApplyRule {
    /// The name of the rule, used for debugging and tracing purposes.
    fn name(&self) -> &'static str;

    /// The [`ApplyStrategy`] for the rule.
    fn strategy(&self) -> ApplyStrategy;

    /// Attempts to apply the rewrite to a subgraph rooted at the given `box_id`.
    /// Returns whether the attempt was successful.
    fn apply(&self, model: &mut Model, box_id: BoxId) -> bool;
}

/// Blanket implementation for [`ApplyRule`] for all [`Rule`] types.
impl<U: Rule> ApplyRule for U {
    #[inline(always)]
    fn name(&self) -> &'static str {
        self.name()
    }

    #[inline(always)]
    fn strategy(&self) -> ApplyStrategy {
        self.strategy()
    }

    /// Apply the [`Rule::rewrite`] and return `true` if [`Rule::check`] returns
    /// a [`Rule::Match`], or return `false` otherwise.
    fn apply(&self, model: &mut Model, box_id: BoxId) -> bool {
        if let Some(mat) = self.check(model, box_id) {
            self.rewrite(model, mat);
            true
        } else {
            false
        }
    }
}

/// Where and how a rule should be applied to boxes in the model.
#[allow(dead_code)]
#[derive(Debug, Copy, Clone)]
pub(crate) enum ApplyStrategy {
    // Apply only to the top box.
    TopBox,
    // Apply to all boxes. Customize with the order in which the boxes should be
    // visited.
    AllBoxes(VisitOrder),
}

#[allow(dead_code)]
#[derive(Debug, Copy, Clone)]
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
    let rules: Vec<Box<dyn ApplyRule>> = vec![];
    apply_rules_to_model(model, rules);
    model.garbage_collect();

    // At the end of the process, update box and quantifier ids to make it
    // easier to compare the graph before and after optimization.
    model.update_ids();
}

/// Transform the model by applying a list of rewrite rules.
fn apply_rules_to_model(model: &mut Model, rules: Vec<Box<dyn ApplyRule>>) {
    let (top_box, other): (Vec<Box<dyn ApplyRule>>, Vec<Box<dyn ApplyRule>>) = rules
        .into_iter()
        .partition(|r| matches!(r.strategy(), ApplyStrategy::TopBox));
    let (pre, post): (Vec<Box<dyn ApplyRule>>, Vec<Box<dyn ApplyRule>>) = other
        .into_iter()
        .partition(|r| matches!(r.strategy(), ApplyStrategy::AllBoxes(VisitOrder::Pre)));

    for rule in &top_box {
        rule.apply(model, model.top_box);
    }

    let mut rewritten = true;
    while rewritten {
        rewritten = false;

        rewritten |= apply_dft_rules(&pre, &post, model);

        for rule in &top_box {
            rewritten |= rule.apply(model, model.top_box);
        }
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
fn apply_dft_rules(
    pre: &Vec<Box<dyn ApplyRule>>,
    post: &Vec<Box<dyn ApplyRule>>,
    model: &mut Model,
) -> bool {
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
        rewritten |= rule.apply(model, model.top_box);
    }
    while !entered.is_empty() {
        if let Some(to_enter) = find_next_child_to_enter(model, &mut entered, &exited) {
            entered.push((to_enter, 0));
            for rule in pre {
                rewritten |= rule.apply(model, to_enter);
            }
        } else {
            // If this box has no more children to descend into,
            // run PostOrder rules and exit the current box.
            let (box_id, _) = entered.last().unwrap();
            for rule in post {
                rewritten |= rule.apply(model, *box_id);
            }
            exited.insert(*box_id);
            entered.pop();
        }
    }

    rewritten
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_model::*;

    #[test]
    fn it_applies_a_simple_rule() {
        let mut model = test_model();
        let src_id = model.make_box(get_user_id(5).into());

        let rules: Vec<Box<dyn ApplyRule>> = vec![
            // create E(src_id, curr_id) quantifiers in pre-visit
            Box::new(ConnectAll::new(src_id, VisitOrder::Pre)),
            // create A(src_id, curr_id) quantifiers in post-visit
            Box::new(ConnectAll::new(src_id, VisitOrder::Post)),
        ];

        apply_rules_to_model(&mut model, rules);

        let act_result = model
            .get_box(src_id)
            .ranging_quantifiers()
            .map(|q| (q.parent_box, q.quantifier_type))
            .collect::<Vec<_>>();

        let exp_result = vec![
            (BoxId(4), QuantifierType::Existential),
            (BoxId(1), QuantifierType::Existential),
            (BoxId(0), QuantifierType::Existential),
            (BoxId(0), QuantifierType::All),
            (BoxId(1), QuantifierType::All),
            (BoxId(2), QuantifierType::Existential),
            (BoxId(2), QuantifierType::All),
            (BoxId(3), QuantifierType::Existential),
            (BoxId(3), QuantifierType::All),
            (BoxId(4), QuantifierType::All),
        ];

        assert_eq!(act_result, exp_result);
    }

    /// Create the following model of select boxes:
    ///
    /// ```
    ///              --------------
    ///             /              \
    /// (b0) --- (b1) --- (b3) --- (b4)
    ///    \              /         /
    ///     ---- (b2) ----         /
    ///            \              /
    ///             --------------
    /// ```
    fn test_model() -> Model {
        let mut model = Model::new();

        // vertices
        let b0 = model.make_box(Select::default().into());
        let b1 = model.make_box(Select::default().into());
        let b2 = model.make_box(Select::default().into());
        let b3 = model.make_box(Select::default().into());
        let b4 = model.make_box(Select::default().into());

        // edges
        model.make_quantifier(QuantifierType::Foreach, b0, b1);
        model.make_quantifier(QuantifierType::Foreach, b0, b2);
        model.make_quantifier(QuantifierType::Foreach, b1, b3);
        model.make_quantifier(QuantifierType::Foreach, b2, b3);
        model.make_quantifier(QuantifierType::Foreach, b1, b4);
        model.make_quantifier(QuantifierType::Foreach, b1, b4);
        model.make_quantifier(QuantifierType::Foreach, b2, b4);
        model.make_quantifier(QuantifierType::Foreach, b3, b4);

        // top box
        model.top_box = b4;

        model
    }

    fn get_user_id(id: u64) -> Get {
        Get {
            id: expr::GlobalId::User(id),
        }
    }

    /// A test [`Rule`] that creates quantifiers between `src_id`
    /// and all other nodes in the given [`VisitOrder`].
    struct ConnectAll {
        src_id: BoxId,
        visit_order: VisitOrder,
    }

    impl ConnectAll {
        fn new(src_id: BoxId, visit_order: VisitOrder) -> ConnectAll {
            ConnectAll {
                src_id,
                visit_order,
            }
        }

        fn quantifier_type(&self) -> QuantifierType {
            match self.visit_order {
                VisitOrder::Pre => QuantifierType::Existential,
                VisitOrder::Post => QuantifierType::All,
            }
        }

        fn matches_quantifer(&self, quantifier_type: QuantifierType) -> bool {
            match self.visit_order {
                VisitOrder::Pre => matches!(quantifier_type, QuantifierType::Existential),
                VisitOrder::Post => matches!(quantifier_type, QuantifierType::All),
            }
        }

        fn connected(&self, model: &Model, tgt_id: BoxId) -> bool {
            model.quantifiers.values().any(|q| {
                let q = q.borrow();
                self.matches_quantifer(q.quantifier_type)
                    && (q.input_box == self.src_id && q.parent_box == tgt_id)
            })
        }
    }

    /// A [`Rule::Match`] for the [`ConnectAll`] rule.
    struct Connect {
        src_id: BoxId,
        tgt_id: BoxId,
        quantifier_type: QuantifierType,
    }

    impl Rule for ConnectAll {
        type Match = Connect;

        fn name(&self) -> &'static str {
            "connect-all"
        }

        fn strategy(&self) -> ApplyStrategy {
            ApplyStrategy::AllBoxes(self.visit_order)
        }

        fn check(&self, model: &Model, box_id: BoxId) -> Option<Connect> {
            if box_id != self.src_id && !self.connected(model, box_id) {
                Some(Connect {
                    src_id: self.src_id,
                    tgt_id: box_id,
                    quantifier_type: self.quantifier_type(),
                })
            } else {
                None
            }
        }

        fn rewrite(&self, model: &mut Model, mat: Connect) {
            // println!("{}:{}⇒{}", self.quantifier_type, self.src_id, self.tgt_id);
            model.make_quantifier(mat.quantifier_type, mat.src_id, mat.tgt_id);
        }
    }
}
