// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements outerjoin simplification as a variant of Algorithm A in the seminal
//! paper by Rosenthal and Galindo-Legaria[^1].
//!
//! [^1]: [Galindo-Legaria, Cesar, and Arnon Rosenthal.
//! "Outerjoin simplification and reordering for query optimization."
//! ACM Transactions on Database Systems (TODS) 22.1 (1997): 43-74.
//! ](https://www.academia.edu/26160408/Outerjoin_simplification_and_reordering_for_query_optimization)

use std::cell::RefCell;
use std::collections::HashSet;

use crate::query_model::attribute::core::Attribute;
use crate::query_model::attribute::propagated_nulls::PropagatedNulls;
use crate::query_model::attribute::rejected_nulls::RejectedNulls;
use crate::query_model::model::BoxType;
use crate::query_model::rewrite::ApplyStrategy;
use crate::query_model::rewrite::Rule;
use crate::query_model::rewrite::VisitOrder;
use crate::query_model::BoxId;
use crate::query_model::QuantifierId;

pub(crate) struct SimplifyOuterJoins();

/// A struct that indicates an outer join simplification action.
pub(crate) enum Action {
    /// Simplify a full outer join to a left/right outer join by
    /// changing the type of [`QuantifierId`] from `PreservedForeach`
    /// to `Foreach`.
    OuterToOuter(BoxId, QuantifierId),
    /// Simplify a full outer join to an inner join by
    /// changing all quantifier types from `PreservedForeach`
    /// to `Foreach` and the box type from `OuterJoin` to `Select`.
    OuterToInner(BoxId),
}

impl Rule for SimplifyOuterJoins {
    /// A (non-empty) sequence of simplification actions to be performed.
    type Match = Vec<Action>;

    fn name(&self) -> &'static str {
        "SimplifyOuterJoins"
    }

    fn strategy(&self) -> ApplyStrategy {
        ApplyStrategy::AllBoxes(VisitOrder::Pre)
    }

    fn required_attributes(&self) -> std::collections::HashSet<Box<dyn Attribute>> {
        HashSet::from([
            Box::new(PropagatedNulls) as Box<dyn Attribute>,
            Box::new(RejectedNulls) as Box<dyn Attribute>,
        ])
    }

    fn check(
        &self,
        model: &crate::query_model::Model,
        box_id: crate::query_model::BoxId,
    ) -> Option<Self::Match> {
        // get the rejected nulls at the root
        let r#box = model.get_box(box_id);
        let rej_nulls = r#box.attributes.get::<RejectedNulls>();

        // we can return immediately if there are no rejected nulls at this node
        if rej_nulls.is_empty() {
            return None;
        }

        // An optional match found at the current box.
        let mut mat = vec![] as Self::Match;

        // A stack that keeps track of the number of randing quantifiers
        // associated with each box along the recursion trace.
        let quantifier_counts = RefCell::new(vec![] as Vec<usize>);

        // A stack of the rejected nulls for the predicates associated
        // with the current box (identified by box_id) expressed in
        // terms of the input quantifiers of the current box
        // initialize with the nulls at the root.
        let rej_nulls = RefCell::new(vec![rej_nulls.clone()]);

        let _: Result<(), ()> = model.try_visit_pre_post_descendants(
            &mut |model, box_id| {
                // mutably borrow stack variables for the duration of this closure
                let mut rej_nulls = rej_nulls.borrow_mut();
                let mut quantifier_counts = quantifier_counts.borrow_mut();

                let r#box = model.get_box(*box_id);

                // Push the ranging quantifiers count the current node.
                quantifier_counts.push(r#box.ranging_quantifiers().count());

                // At the moment, we don't want to do any work:
                // (1) if a match is already found, or
                // (2) if we are in a in subtree with a non-unique ancestor chain.
                // In the future, we might remove constraint (2).
                // See [this comment](https://github.com/MaterializeInc/materialize/issues/10239#issuecomment-1030123237)
                // and the preceding discussion for more details.
                if quantifier_counts.iter().all(|count| count == &1) {
                    // Get the ID of the quantifer that connects this box with its parent.
                    let quantifier_id = r#box.ranging_quantifiers().nth(0).unwrap().id;
                    // Get the propagated nulls for the output attributes of this box.
                    let propagated_nulls = r#box.attributes.get::<PropagatedNulls>();

                    // Take the rej_nulls expressed in terms of the parent's quantifiers.
                    let rej_nulls_parent = rej_nulls.last().unwrap();

                    // And use it to compute the rej_nulls expressed in terms of this child's quantifiers.
                    let rej_nulls_child = rej_nulls_parent
                        .iter()
                        // Filter out columns that are not from this child
                        .filter(|c| c.quantifier_id != quantifier_id)
                        // and replace the rest with the set of nulls that
                        // the corresponding column propagates from its input.
                        .flat_map(|c| match propagated_nulls.get(c.position) {
                            Some(set) => set.clone(),
                            None => HashSet::new(),
                        })
                        .collect::<HashSet<_>>();

                    // If this is an outer join box, we might have a potential match
                    if let BoxType::OuterJoin(..) = r#box.box_type {
                        assert_eq!(r#box.input_quantifiers().count(), 2);

                        let lhs = r#box.input_quantifiers().nth(0).unwrap();
                        let rhs = r#box.input_quantifiers().nth(0).unwrap();

                        let rej_lhs = rej_nulls_child.iter().any(|c| c.quantifier_id == lhs.id);
                        let rej_rhs = rej_nulls_child.iter().any(|c| c.quantifier_id == rhs.id);

                        // TODO: this classification incomplete and probably incorrect
                        match (rej_lhs, rej_rhs) {
                            // If both sides are rejected, return a match that
                            // indicates that this box should be converted to
                            // an inner join.
                            (true, true) => mat.push(Action::OuterToInner(r#box.id)),
                            // If only one side is rejected, we need to check
                            // which side it is.
                            (true, false) => mat.push(Action::OuterToOuter(r#box.id, lhs.id)),
                            (false, true) => mat.push(Action::OuterToOuter(r#box.id, rhs.id)),
                            // If no side is rejected, do nothing.
                            (false, false) => (),
                        }
                    };

                    // Push rej_nulls_child to the stack.
                    rej_nulls.push(rej_nulls_child);
                }

                Ok(())
            },
            &mut |_model, _box_id| {
                // mutably borrow stack variables for the duration of this closure
                let mut rej_nulls = rej_nulls.borrow_mut();
                let mut quantifier_counts = quantifier_counts.borrow_mut();

                if quantifier_counts.iter().all(|count| count == &1) {
                    // Pop the rejected nulls for the current node.
                    rej_nulls.pop();
                }

                // Pop the quantifier count of the current node.
                quantifier_counts.pop();

                Ok(())
            },
            box_id,
        );

        if mat.len() > 0 {
            Some(mat)
        } else {
            None
        }
    }

    fn rewrite(&self, _model: &mut crate::query_model::Model, _mat: Self::Match) {
        todo!()
    }
}
