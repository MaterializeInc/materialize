// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Re-order relations in a join to process them in an order that makes sense.
//!
//! ```rust
//! use expr::{RelationExpr, ScalarExpr};
//! use expr::transform::fusion::filter::Filter;
//! use repr::{ColumnType, Datum, RelationType, ScalarType};
//!
//! let input = RelationExpr::constant(vec![], RelationType::new(vec![
//!     ColumnType::new(ScalarType::Bool),
//! ]));
//!
//! let predicate0 = ScalarExpr::Column(0);
//! let predicate1 = ScalarExpr::Column(0);
//! let predicate2 = ScalarExpr::Column(0);
//!
//! let mut expr =
//! input
//!     .clone()
//!     .filter(vec![predicate0.clone()])
//!     .filter(vec![predicate1.clone()])
//!     .filter(vec![predicate2.clone()]);
//!
//! // .transform() will deduplicate any predicates
//! Filter.transform(&mut expr);
//!
//! let correct = input.filter(vec![predicate0]);
//!
//! assert_eq!(expr, correct);
//! ```

use std::collections::HashSet;
use crate::{RelationExpr, ScalarExpr};
use crate::{UnaryFunc, BinaryFunc, VariadicFunc};

#[derive(Debug)]
pub struct NewIsNull;

impl crate::transform::Transform for NewIsNull {
    fn transform(&self, relation: &mut RelationExpr) {
        self.transform(relation)
    }
}

impl NewIsNull {
    pub fn transform(&self, relation: &mut RelationExpr) {
        self.action(relation, HashSet::new());
    }
    /// Columns that must be non-null.
    pub fn action(&self, relation: &mut RelationExpr, mut columns: HashSet<usize>) {
        match relation {
            RelationExpr::Constant { rows, .. } => {
                rows.retain(|(row, _)| columns.iter().all(|c| row[*c] != repr::Datum::Null))
            }
            RelationExpr::Get { .. } => { }
            RelationExpr::Let { body, .. } => {
                self.action(body, columns);
            }
            RelationExpr::Project { input, outputs } => {
                self.action(input, columns.into_iter().map(|c| outputs[c]).collect());
            }
            RelationExpr::Map { input, scalars } => {
                let arity = input.arity();
                let mut new_columns = HashSet::new();
                for column in columns {
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if column < arity {
                        new_columns.insert(column);
                    }
                    else {
                        must_be_non_null(&scalars[column - arity], &mut new_columns);
                    }
                }
                self.action(input, new_columns);
            }
            RelationExpr::Filter { input, predicates } => {
                for predicate in predicates {
                    must_be_non_null(predicate, &mut columns);
                    // TODO: Equality constraints should smear around requirements!
                    // TODO: Not(IsNull) should add a constraint!
                }
                self.action(input, columns);
            }
            RelationExpr::Join { inputs, variables } => {

                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
                let input_arities = input_types
                    .iter()
                    .map(|i| i.column_types.len())
                    .collect::<Vec<_>>();

                let mut offset = 0;
                let mut prior_arities = Vec::new();
                for input in 0..inputs.len() {
                    prior_arities.push(offset);
                    offset += input_arities[input];
                }

                let input_relation = input_arities
                    .iter()
                    .enumerate()
                    .flat_map(|(r, a)| std::iter::repeat(r).take(*a))
                    .collect::<Vec<_>>();

                let mut new_columns = vec![HashSet::new(); inputs.len()];
                for column in columns {
                    let input = input_relation[column];
                    new_columns[input].insert(column - prior_arities[input]);
                }

                // `variable` smears constraints around.
                for variable in variables {
                    if variable.iter().any(|(r,c)| new_columns[*r].contains(c)) {
                        for (r,c) in variable {
                            new_columns[*r].insert(*c);
                        }
                    }
                }

                for (input, columns) in inputs.iter_mut().zip(new_columns) {
                    self.action(input, columns);
                }
            }
            RelationExpr::Reduce { input, group_key, .. } => {
                let mut new_columns = HashSet::new();
                for column in columns {
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if column < group_key.len() {
                        new_columns.insert(group_key[column]);
                    }
                }
                self.action(input, new_columns);
            }
            RelationExpr::TopK { input, ..} => {
                self.action(input, columns);
            }
            RelationExpr::Negate { input } => {
                self.action(input, columns);
            }
            RelationExpr::Threshold { input } => {
                self.action(input, columns);
            }
            RelationExpr::Union { left, right } => {
                self.action(left, columns.clone());
                self.action(right, columns);
            }
        }
    }
}

/// Columns that must be non-null for the predicate to be true.
fn must_be_non_null(predicate: &ScalarExpr, columns: &mut HashSet<usize>) {
    match predicate {
        ScalarExpr::Column(col) => {
            columns.insert(*col);
        }
        ScalarExpr::Literal(..) => { }
        ScalarExpr::CallUnary { func, expr } => {
            if func != &UnaryFunc::IsNull {
                must_be_non_null(expr, columns);
            }
        }
        ScalarExpr::CallBinary { func, expr1, expr2 } => {
            if func != &BinaryFunc::Or {
                must_be_non_null(expr1, columns);
                must_be_non_null(expr2, columns);
            }
        }
        ScalarExpr::CallVariadic { func, exprs } => {
            if func != &VariadicFunc::Coalesce {
                for expr in exprs {
                    must_be_non_null(expr, columns);
                }
            }
        }
        ScalarExpr::If { cond, then: _, els: _ } => {
            must_be_non_null(cond, columns);
        }
    }
}