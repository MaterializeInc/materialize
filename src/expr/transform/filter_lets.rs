// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::{RelationExpr, ScalarExpr};

/// Pushes common filter predicates on gets into the let binding.
#[derive(Debug)]
pub struct FilterLets;

impl super::Transform for FilterLets {
    fn transform(&self, relation: &mut RelationExpr) {
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
        if let RelationExpr::Let { name, value, body } = relation {
            let mut common = None;
            common_constraints(body, name, &mut common);
            if let Some(constraints) = common {
                if !constraints.is_empty() {
                    delete_constraints(body, name, &constraints[..]);
                    **value = value.take_dangerous().filter(constraints);
                }
            }
        }
    }
}

fn common_constraints(
    expr: &RelationExpr,
    bound_name: &str,
    constraints: &mut Option<Vec<ScalarExpr>>,
) {
    match expr {
        RelationExpr::Get { name, .. } if name == bound_name => *constraints = Some(Vec::new()),
        RelationExpr::Filter { input, predicates } => {
            if let RelationExpr::Get { name, .. } = &**input {
                if name == bound_name {
                    if let Some(constraints) = constraints {
                        constraints.retain(|p| predicates.contains(p));
                    } else {
                        *constraints = Some(predicates.clone());
                    }
                }
            } else {
                expr.visit1(|e| common_constraints(e, bound_name, constraints))
            }
        }
        _ => expr.visit1(|e| common_constraints(e, bound_name, constraints)),
    }
}

fn delete_constraints(expr: &mut RelationExpr, bound_name: &str, constraints: &[ScalarExpr]) {
    match expr {
        RelationExpr::Filter { input, predicates } => {
            if let RelationExpr::Get { name, .. } = &**input {
                if name == bound_name {
                    predicates.retain(|p| !constraints.contains(p));
                }
            } else {
                expr.visit1_mut(|e| delete_constraints(e, bound_name, constraints))
            }
        }
        _ => expr.visit1_mut(|e| delete_constraints(e, bound_name, constraints)),
    }
}
