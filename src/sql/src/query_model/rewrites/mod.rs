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
    let mut rewritten_in_pre = false;
    let mut rewritten_in_post = false;

    let _ = model.visit_mut_pre_post(
        &mut |m, box_id| -> Result<(), ()> {
            for rule in pre {
                rewritten_in_pre |= apply_rule(rule.as_ref(), m, *box_id);
            }
            Ok(())
        },
        &mut |m, box_id| -> Result<(), ()> {
            for rule in post {
                rewritten_in_post |= apply_rule(rule.as_ref(), m, *box_id);
            }
            Ok(())
        },
    );

    rewritten_in_pre | rewritten_in_post
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_model::*;

    #[test]
    fn it_applies_a_simple_rule() {
        let mut model = test_model();
        let src_id = model.make_box(get_user_id(5).into());

        let rules: Vec<Box<dyn Rule>> = vec![
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

    impl Rule for ConnectAll {
        fn name(&self) -> &'static str {
            "connect-all"
        }

        fn strategy(&self) -> ApplyStrategy {
            ApplyStrategy::AllBoxes(self.visit_order)
        }

        fn check(&self, model: &Model, box_id: BoxId) -> Option<Box<dyn Rewrite>> {
            if box_id != self.src_id && !self.connected(model, box_id) {
                Some(Box::new(Connect {
                    src_id: self.src_id,
                    tgt_id: box_id,
                    quantifier_type: self.quantifier_type(),
                }))
            } else {
                None
            }
        }
    }

    /// A test [`Rewrite`] that creates the specified quantifier.
    struct Connect {
        src_id: BoxId,
        tgt_id: BoxId,
        quantifier_type: QuantifierType,
    }

    impl Rewrite for Connect {
        fn rewrite(&mut self, model: &mut Model) {
            // println!("{}:{}⇒{}", self.quantifier_type, self.src_id, self.tgt_id);
            model.make_quantifier(self.quantifier_type, self.src_id, self.tgt_id);
        }
    }
}
