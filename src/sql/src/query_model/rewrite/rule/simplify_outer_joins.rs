// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implements outerjoin simplification.
//!
//! For each outer join box, check to see if any ancestor rejects nulls
//! on its output.

use std::cell::RefCell;
use std::collections::HashSet;

use crate::query_model::attribute::core::Attribute;
use crate::query_model::attribute::propagated_nulls::PropagatedNulls;
use crate::query_model::attribute::rejected_nulls::RejectedNulls;
use crate::query_model::model::{
    BoundRef, BoxType, ColumnReference, QuantifierType, QueryBox, Select,
};
use crate::query_model::rewrite::ApplyStrategy;
use crate::query_model::rewrite::Rule;
use crate::query_model::rewrite::VisitOrder;
use crate::query_model::{BoxId, Model, QuantifierId};

pub(crate) struct SimplifyOuterJoins;

impl Rule for SimplifyOuterJoins {
    /// A (non-empty) sequence of ids corresponding to quantifiers whose type
    /// should be changed from [QuantifierType::PreservedForeach] to
    /// [QuantifierType::Foreach], and the box that the quantifiers belong to.
    type Match = (BoxId, Vec<QuantifierId>);

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
        box_id_to_check: crate::query_model::BoxId,
    ) -> Option<Self::Match> {
        let mut quantifiers_to_change = vec![];

        let box_to_check = model.get_box(box_id_to_check);
        if let BoxType::OuterJoin(..) = box_to_check.box_type {
            assert_eq!(box_to_check.input_quantifiers().count(), 2);

            // We currently only apply the rule to an outer join if a unique
            // ancestor chain can be found going from the top box to the box to
            // check. We might remove this constraint in the future.
            // See [this comment](https://github.com/MaterializeInc/materialize/issues/10239#issuecomment-1030123237)
            // and the preceding discussion for more details.

            let unique_ancestor_chain = unique_ancestor_chain(model, box_id_to_check);

            if let Some(ancestor_chain) = unique_ancestor_chain {
                // Collect all the rejected nulls from all the ancestors in the chain.
                let mut ancestral_rejected_nulls = HashSet::new();

                // As we move down the chain, going from parent to child,
                // We want to translate the rejected nulls for each parent box
                // to be in terms of the child box. Then we want to add the
                // nulls that the child box rejects.
                for ancestor_id in ancestor_chain {
                    let ancestor_box = model.get_box(ancestor_id);
                    map_nulls_to_inputs(&mut ancestral_rejected_nulls, &ancestor_box);
                    ancestral_rejected_nulls.extend(
                        ancestor_box
                            .attributes
                            .get::<RejectedNulls>()
                            .into_iter()
                            .cloned(),
                    );
                }
                map_nulls_to_inputs(&mut ancestral_rejected_nulls, &box_to_check);

                let mut quantifiers = box_to_check.input_quantifiers();
                let lhs = quantifiers.next().unwrap();
                let rhs = quantifiers.next().unwrap();

                let rej_lhs = ancestral_rejected_nulls
                    .iter()
                    .any(|c| c.quantifier_id == lhs.id);
                let rej_rhs = ancestral_rejected_nulls
                    .iter()
                    .any(|c| c.quantifier_id == rhs.id);

                // If null rows are rejected from LHS, and RHS is a
                // PreservedForeach quantifier, change the RHS to a Foreach
                // quantifier.
                if rej_lhs && rhs.quantifier_type == QuantifierType::PreservedForeach {
                    quantifiers_to_change.push(rhs.id);
                }
                // And vice versa.
                if rej_rhs && lhs.quantifier_type == QuantifierType::PreservedForeach {
                    quantifiers_to_change.push(lhs.id);
                }
            }
        }

        if quantifiers_to_change.len() > 0 {
            Some((box_id_to_check, quantifiers_to_change))
        } else {
            // If there are no quantifiers to change, return None.
            None
        }
    }

    fn rewrite(&self, model: &mut Model, mat: Self::Match) {
        let (box_id, q_ids) = (mat.0, mat.1);

        // Change the specified quantifiers to type Foreach.
        for q_id in q_ids {
            let mut q = model.get_mut_quantifier(q_id);
            q.quantifier_type = QuantifierType::Foreach;
        }

        // If all the quantifiers in the box are type Foreach,
        // convert the box to type Select.
        let mut r#box = model.get_mut_box(box_id);
        if r#box
            .input_quantifiers()
            .all(|q| q.quantifier_type == QuantifierType::Foreach)
        {
            r#box.box_type = match &mut r#box.box_type {
                BoxType::OuterJoin(outer_join) => {
                    Select::new(outer_join.predicates.split_off(0)).into()
                }
                _ => Select::default().into(),
            };
        }
    }
}

/// Find the unique ancestor chain from the top box of `model` to `target` box.
/// The ancestor chain does not contain `target`.
fn unique_ancestor_chain(model: &Model, target: BoxId) -> Option<Vec<BoxId>> {
    // Until target is reached, this is the stack of boxes we have entered.
    // This does not change after target is reached.
    let ancestor_chain = RefCell::new(vec![]);

    let unique_ancestor_chain_found = RefCell::new(false);

    // Traverse the graph starting from the top until `target` is reached.
    let _: Result<(), ()> = mz_ore::graph::try_nonrecursive_dft(
        model,
        model.top_box,
        &mut |model, box_id| {
            let r#box = model.get_box(*box_id);
            if *unique_ancestor_chain_found.borrow() {
                // If target has been reached, don't do anything.
                Ok(vec![])
            } else {
                // Register that we have visited this node.
                ancestor_chain.borrow_mut().push(*box_id);

                if r#box.ranging_quantifiers().count() > 1 {
                    // If a box with more than one parent is found:
                    // * Do not go deeper.
                    // * Do not check if the box is the target.
                    Ok(vec![])
                } else if *box_id == target {
                    *unique_ancestor_chain_found.borrow_mut() = true;
                    Ok(vec![])
                } else {
                    Ok(r#box.input_quantifiers().map(|q| q.input_box).collect())
                }
            }
        },
        &mut |_, _| {
            if !*unique_ancestor_chain_found.borrow() {
                ancestor_chain.borrow_mut().pop();
            }
            Ok(())
        },
    );
    if *unique_ancestor_chain_found.borrow() {
        let mut ancestor_chain = ancestor_chain.take();
        ancestor_chain.pop();
        Some(ancestor_chain)
    } else {
        None
    }
}

/// Map (rejected nulls from any ancestor of `box`, expressed as column
/// references from an input of the parent of `box`) ->
/// (column references from an input of `box`)
fn map_nulls_to_inputs(
    ancestral_rejected_nulls: &mut HashSet<ColumnReference>,
    r#box: &BoundRef<'_, QueryBox>,
) {
    if !ancestral_rejected_nulls.is_empty() {
        // Get the ID of the quantifer that connects this box with its parent.
        let quantifier_id = r#box.ranging_quantifiers().nth(0).unwrap().id;
        // Retain only columns from the child box.
        ancestral_rejected_nulls.retain(|c| c.quantifier_id == quantifier_id);
        // Replace each column with the set of nulls that
        // the column propagates from its input.
        let propagated_nulls = r#box.attributes.get::<PropagatedNulls>();
        *ancestral_rejected_nulls = ancestral_rejected_nulls
            .iter()
            .flat_map(|c| match propagated_nulls.get(c.position) {
                Some(set) => set.clone(),
                None => HashSet::new(),
            })
            .collect();
    }
}
