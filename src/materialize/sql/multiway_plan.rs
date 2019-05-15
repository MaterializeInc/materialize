// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use sqlparser::sqlast;
use sqlparser::sqlast::ASTNode;

use crate::dataflow::Plan::MultiwayJoin;
use crate::sql::plan::SQLPlan;

#[allow(dead_code)]
pub fn plan_multiple_joins(
    plans: &[SQLPlan],
    joins: &[sqlast::Join],
    selection: Option<ASTNode>,
) -> Result<(SQLPlan, Option<ASTNode>), failure::Error> {
    assert!(plans.len() == joins.len() + 1);

    let mut constraints = Vec::new(); // Remaining constraints
    if let Some(constraint) = &selection {
        constraints.push(constraint);
    }

    for join in joins.iter() {
        // Extract any join-specific constraint.
        let constraint = match &join.join_operator {
            sqlast::JoinOperator::Inner(constraint) => Some(constraint),
            sqlast::JoinOperator::LeftOuter(_constraint) => {
                bail!("Outer joins not supported");
                // Some(constraint)
            }
            sqlast::JoinOperator::RightOuter(_constraint) => {
                bail!("Outer joins not supported");
                // Some(constraint)
            }
            sqlast::JoinOperator::FullOuter(_constraint) => {
                bail!("Outer joins not supported");
                // Some(constraint)
            }
            sqlast::JoinOperator::Implicit => None,
            sqlast::JoinOperator::Cross => None,
        };

        // Introduce constraint to `constraints`.
        if let Some(constraint) = constraint {
            match constraint {
                sqlast::JoinConstraint::On(constraint) => constraints.push(constraint),
                sqlast::JoinConstraint::Using(_) => {
                    bail!("Using statement unsupported (schema altering)");
                }
                sqlast::JoinConstraint::Natural => {
                    bail!("Natural join unsupported (schema altering)");
                }
            }
        }
    }

    let mut predicates = Vec::new(); // Residual predicates
    let mut equalities = Vec::new(); // Equijoin constraints

    // Extract equijoin constraints as such.
    while let Some(mut constraint) = constraints.pop() {
        while let ASTNode::SQLNested(expr) = constraint {
            constraint = expr;
        }

        // Taken from `plan_implicit_join`.
        match constraint {
            // TODO: Should we do the following, or is it important to
            //       only unnest at the root?
            // ASTNode::SQLNested(expr) => { constraints.push(expr); },
            ASTNode::SQLBinaryExpr { left, op, right } => match op {
                // And statements should each be processed independently.
                sqlast::SQLOperator::And => {
                    constraints.push(left);
                    constraints.push(right);
                }
                // Equality statements may be digestible.
                sqlast::SQLOperator::Eq => {
                    let l_location = resolve_locations(left, &plans[..]);
                    let r_location = resolve_locations(right, &plans[..]);

                    if let (Ok(l_location), Ok(r_location)) = (l_location, r_location) {
                        if l_location.len() != 1 {
                            bail!("Name not uniquely found");
                        }
                        if r_location.len() != 1 {
                            bail!("Name not uniquely found");
                        }

                        if l_location[0].0 != r_location[0].0 {
                            equalities.push((l_location[0], r_location[0]));
                        } else {
                            // Intra-relation constraint; odd, but ok.
                            predicates.push(constraint);
                        }
                    } else {
                        predicates.push(constraint);
                    }
                }
                _ => predicates.push(constraint),
            },
            _ => predicates.push(constraint),
        }
    }

    let mut arities = Vec::new();
    let mut columns = Vec::new();
    for plan in plans.iter() {
        arities.push(plan.columns().len());
        columns.extend(plan.columns().iter().cloned());
    }

    let plans = plans.iter().map(|x| x.plan().clone()).collect::<Vec<_>>();

    let plan = SQLPlan::from_plan_columns(
        MultiwayJoin {
            plans,
            arities,
            equalities,
        },
        columns,
    );

    let mut predicates_iter = predicates.into_iter();
    let predicates = predicates_iter.next().map(|expr| {
        predicates_iter.fold(expr.clone(), |e1, e2| ASTNode::SQLBinaryExpr {
            left: Box::new(e1.clone()),
            op: sqlast::SQLOperator::And,
            right: Box::new(e2.clone()),
        })
    });

    Ok((plan, predicates))
}

fn resolve_locations(
    ast_node: &ASTNode,
    plans: &[SQLPlan],
) -> Result<Vec<(usize, usize)>, failure::Error> {
    match ast_node {
        ASTNode::SQLIdentifier(column_name) => Ok(plans
            .iter()
            .enumerate()
            .flat_map(|(i, p)| p.resolve_column(column_name).map(|c| (i, c.0)))
            .collect::<Vec<_>>()),
        ASTNode::SQLCompoundIdentifier(l_names) => {
            if l_names.len() != 2 {
                bail!("Compound names mandatory");
            }
            Ok(plans
                .iter()
                .enumerate()
                .flat_map(|(i, p)| {
                    p.resolve_table_column(&l_names[0], &l_names[1])
                        .map(|c| (i, c.0))
                })
                .collect::<Vec<_>>())
        }
        _ => {
            bail!("Unsupported column description: {:?}", ast_node);
        }
    }
}
