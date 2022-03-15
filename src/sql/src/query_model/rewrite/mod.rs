// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apply rewrites to [`Model`] instances.
//!
//! The public interface consists of the [`Model::optimize`] method.

mod rule;

use std::collections::HashSet;

use super::attribute::core::{Attribute, RequiredAttributes};
use super::model::{BoxId, Model};

impl Model {
    pub fn optimize(&mut self) {
        rewrite_model(self);
    }
}

/// Trait that all rewrite rules must implement.
pub(crate) trait Rule {
    /// The type of matches associated with this [`Rule`].
    type Match;

    /// The name of the rule, used for debugging and tracing purposes.
    fn name(&self) -> &'static str;

    /// The [`ApplyStrategy`] for the rule.
    fn strategy(&self) -> ApplyStrategy;

    /// Derived attributes required by this [`Rule`].
    fn required_attributes(&self) -> HashSet<Box<dyn Attribute>>;

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

    /// Derived attributes required by this [`ApplyRule`].
    fn required_attributes(&self) -> HashSet<Box<dyn Attribute>>;

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

    #[inline(always)]
    fn required_attributes(&self) -> HashSet<Box<dyn Attribute>> {
        self.required_attributes()
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
    let rules: Vec<Box<dyn ApplyRule>> = vec![
        // simplify outer joins first
        Box::new(rule::simplify_outer_joins::SimplifyOuterJoins),
    ];
    apply_rules_to_model(model, rules);
    model.garbage_collect();

    // At the end of the process, update box and quantifier ids to make it
    // easier to compare the graph before and after optimization.
    model.update_ids();
}

/// Transform the model by applying a list of rewrite rules.
fn apply_rules_to_model(model: &mut Model, rules: Vec<Box<dyn ApplyRule>>) {
    // collect a set of attributes required by the given rules
    let attributes = rules
        .iter()
        .flat_map(|r| r.required_attributes())
        .collect::<HashSet<_>>();
    let attributes = RequiredAttributes::from(attributes);
    // derived initial values of the required attributes
    attributes.derive(model, model.top_box);

    let (top_box, other): (Vec<Box<dyn ApplyRule>>, Vec<Box<dyn ApplyRule>>) = rules
        .into_iter()
        .partition(|r| matches!(r.strategy(), ApplyStrategy::TopBox));
    let (pre, post): (Vec<Box<dyn ApplyRule>>, Vec<Box<dyn ApplyRule>>) = other
        .into_iter()
        .partition(|r| matches!(r.strategy(), ApplyStrategy::AllBoxes(VisitOrder::Pre)));

    let mut rewritten = true;
    while rewritten {
        let mut rewritten_in_top = false;
        let mut rewritten_in_pre = false;
        let mut rewritten_in_post = false;

        for rule in &top_box {
            if rule.apply(model, model.top_box) {
                rewritten_in_top |= true;
                attributes.derive(model, model.top_box);
            }
        }

        let _ = model.try_visit_mut_pre_post(
            &mut |model, box_id| -> Result<(), ()> {
                for rule in pre.iter() {
                    if rule.apply(model, *box_id) {
                        rewritten_in_pre |= true;
                        attributes.derive(model, model.top_box);
                    }
                }
                Ok(())
            },
            &mut |model, box_id| -> Result<(), ()> {
                for rule in post.iter() {
                    if rule.apply(model, *box_id) {
                        rewritten_in_post |= true;
                        attributes.derive(model, model.top_box);
                    }
                }
                Ok(())
            },
        );

        rewritten = rewritten_in_top | rewritten_in_pre | rewritten_in_post;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_model::model::*;
    use crate::query_model::test::util::*;

    #[test]
    fn it_applies_a_simple_rule() {
        let mut model = test_model();
        let src_id = model.make_box(qgm::get(5).into());

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
    ///             --------------
    fn test_model() -> Model {
        let mut model = Model::default();

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
            model.quantifiers().any(|q| {
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

        fn required_attributes(&self) -> HashSet<Box<dyn Attribute>> {
            HashSet::new()
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
