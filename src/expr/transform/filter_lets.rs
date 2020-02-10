// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use crate::{EvalEnv, GlobalId, Id, LocalId, RelationExpr, ScalarExpr};

/// Pushes common filter predicates on gets into the let binding.
///
/// For each `Let` expression, this transform collects the subset
/// of predicates that can be found in filter statements immediately
/// preceding all `Get` expressions for the name. These collected
/// predicates are then introduced into the bound `value` and removed
/// from filter statements preceding the `Get` expressions in the body.
#[derive(Debug)]
pub struct FilterLets;

impl super::Transform for FilterLets {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        _: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
        _: &EvalEnv,
    ) {
        self.transform(relation)
    }
}

impl FilterLets {
    pub fn transform(&self, relation: &mut RelationExpr) {
        relation.visit_mut_pre(&mut |e| {
            self.action(e);
        });
    }

    pub fn action(&self, relation: &mut RelationExpr) {
        if let RelationExpr::Let { id, value, body } = relation {
            let mut common = None;
            common_constraints(body, *id, &mut common);
            if let Some(constraints) = common {
                if !constraints.is_empty() {
                    delete_constraints(body, *id, &constraints[..]);
                    **value = value.take_dangerous().filter(constraints);
                }
            }
        }
    }
}

/// Accumulate predicate `ScalarExpr`s common to all filters immediately
/// preceding a `Get` referencing `bound_name`. A `None` value of `constraints`
/// indicates that no such `Get` has yet been encountered, and the list otherwise
/// contains all common predicates (and may be empty if there are no common
/// predicates). In particular, if a `Get` occurs with no immediately preceding
/// filter, the list is immediately set to the empty list.
fn common_constraints(
    expr: &RelationExpr,
    bound_id: LocalId,
    constraints: &mut Option<Vec<ScalarExpr>>,
) {
    match expr {
        RelationExpr::Get { id, .. } if *id == Id::Local(bound_id) => {
            // No filter found, and so no possible common constraints exist.
            *constraints = Some(Vec::new())
        }
        RelationExpr::Filter { input, predicates } => {
            if let RelationExpr::Get { id, .. } = &**input {
                if *id == Id::Local(bound_id) {
                    if let Some(constraints) = constraints {
                        // If we have existing constraints, restrict them.
                        constraints.retain(|p| predicates.contains(p));
                    } else {
                        // If this is our first encounter, install predicates.
                        *constraints = Some(predicates.clone());
                    }
                }
            } else {
                expr.visit1(|e| common_constraints(e, bound_id, constraints))
            }
        }
        _ => expr.visit1(|e| common_constraints(e, bound_id, constraints)),
    }
}

/// Delete each constraint in `constraints` from any filter immediately preceding a get for `bound_name`.
fn delete_constraints(expr: &mut RelationExpr, bound_id: LocalId, constraints: &[ScalarExpr]) {
    match expr {
        RelationExpr::Filter { input, predicates } => {
            if let RelationExpr::Get { id, .. } = &**input {
                if *id == Id::Local(bound_id) {
                    predicates.retain(|p| !constraints.contains(p));
                    if predicates.is_empty() {
                        *expr = input.take_dangerous();
                    }
                }
            } else {
                expr.visit1_mut(|e| delete_constraints(e, bound_id, constraints))
            }
        }
        _ => expr.visit1_mut(|e| delete_constraints(e, bound_id, constraints)),
    }
}
