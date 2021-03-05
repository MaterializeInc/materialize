// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Push non-null requirements toward sources.
//!
//! This analysis derives NonNull requirements on the arguments to predicates.
//! These requirements exist because most functions with Null arguments are
//! themselves Null, and a predicate that evaluates to Null will not pass.
//!
//! These requirements are not here introduced as constraints, but rather flow
//! to sources of data and restrict any constant collections to those rows that
//! satisfy the constraint. The main consequence is when Null values are added
//! in support of outer-joins and subqueries, we can occasionally remove that
//! branch when we observe that Null values would be subjected to predicates.
//!
//! This analysis relies on a careful understanding of `ScalarExpr` and the
//! semantics of various functions, *some of which may be non-Null even with
//! Null arguments*.
use std::collections::{HashMap, HashSet};

use crate::TransformArgs;
use expr::{Id, JoinInputMapper, MirRelationExpr, MirScalarExpr};

/// Push non-null requirements toward sources.
#[derive(Debug)]
pub struct NonNullRequirements;

impl crate::Transform for NonNullRequirements {
    fn transform(
        &self,
        relation: &mut MirRelationExpr,
        _: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.action(relation, HashSet::new(), &mut HashMap::new());
        Ok(())
    }
}

impl NonNullRequirements {
    /// Push non-null requirements toward sources.
    pub fn action(
        &self,
        relation: &mut MirRelationExpr,
        mut columns: HashSet<usize>,
        gets: &mut HashMap<Id, Vec<HashSet<usize>>>,
    ) {
        match relation {
            MirRelationExpr::Constant { rows, .. } => {
                if let Ok(rows) = rows {
                    rows.retain(|(row, _)| {
                        let datums = row.unpack();
                        columns.iter().all(|c| datums[*c] != repr::Datum::Null)
                    })
                }
            }
            MirRelationExpr::Get { id, .. } => {
                gets.entry(*id).or_insert_with(Vec::new).push(columns);
            }
            MirRelationExpr::Let { id, value, body } => {
                // Let harvests any non-null requirements from its body,
                // and acts on the intersection of the requirements for
                // each corresponding Get, pushing them at its value.
                let id = Id::Local(*id);
                let prior = gets.insert(id, Vec::new());
                self.action(body, columns, gets);
                let mut needs = gets.remove(&id).unwrap();
                if let Some(prior) = prior {
                    gets.insert(id, prior);
                }
                if let Some(mut need) = needs.pop() {
                    while let Some(x) = needs.pop() {
                        need.retain(|col| x.contains(col))
                    }
                    self.action(value, need, gets);
                }
            }
            MirRelationExpr::Project { input, outputs } => {
                self.action(
                    input,
                    columns.into_iter().map(|c| outputs[c]).collect(),
                    gets,
                );
            }
            MirRelationExpr::Map { input, scalars } => {
                let arity = input.arity();
                if columns
                    .iter()
                    .any(|c| *c >= arity && scalars[*c - arity].is_literal_null())
                {
                    // A null value was introduced in a marked column;
                    // the entire expression can be zeroed out.
                    relation.take_safely();
                } else {
                    // For each column, if it must be non-null, extract the expression's
                    // non-null requirements and include them too. We go in reverse order
                    // to ensure we squeegee down all requirements even for references to
                    // other columns produced in this operator.
                    for column in (arity..(arity + scalars.len())).rev() {
                        if columns.contains(&column) {
                            scalars[column - arity].non_null_requirements(&mut columns);
                        }
                        columns.remove(&column);
                    }
                    self.action(input, columns, gets);
                }
            }
            MirRelationExpr::FlatMap {
                input,
                func,
                exprs,
                demand: _,
            } => {
                // Columns whose number is smaller than arity refer to
                // columns of `input`. Columns whose number is
                // greater than or equal to the arity refer to columns created
                // by the FlatMap. The latter group of columns cannot be
                // propagated down.
                let arity = input.arity();
                columns.retain(|c| *c < arity);

                if func.empty_on_null_input() {
                    // we can safely disregard rows where any of the exprs
                    // evaluate to null
                    for expr in exprs {
                        expr.non_null_requirements(&mut columns);
                    }
                }

                // TODO: if `!func.empty_on_null_input()` and there are members
                // of `columns` that refer to columns created by the FlatMap, we
                // may be able to propagate some non-null requirements based on
                // which columns created by the FlatMap cannot be null. However,
                // as there are no TableFuncs for which
                // `!func.empty_on_null_input()`, it is yet unknown what should
                // be done in this case.

                self.action(input, columns, gets);
            }
            MirRelationExpr::Filter { input, predicates } => {
                for predicate in predicates {
                    predicate.non_null_requirements(&mut columns);
                    // TODO: Not(IsNull) should add a constraint!
                }
                self.action(input, columns, gets);
            }
            MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } => {
                let input_types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();

                let input_mapper = JoinInputMapper::new_from_input_types(&input_types);

                let mut new_columns = input_mapper.split_column_set_by_input(&columns);

                // `variable` smears constraints around.
                // Also, any non-nullable columns impose constraints on their equivalence class.
                for equivalence in equivalences {
                    let exists_constraint = equivalence.iter().any(|expr| {
                        if let MirScalarExpr::Column(c) = expr {
                            let (col, rel) = input_mapper.map_column_to_local(*c);
                            new_columns[rel].contains(&col)
                                || !input_types[rel].column_types[col].nullable
                        } else {
                            false
                        }
                    });

                    if exists_constraint {
                        for expr in equivalence.iter() {
                            if let MirScalarExpr::Column(c) = expr {
                                let (col, rel) = input_mapper.map_column_to_local(*c);
                                new_columns[rel].insert(col);
                            }
                        }
                    }
                }

                for (input, columns) in inputs.iter_mut().zip(new_columns) {
                    self.action(input, columns, gets);
                }
            }
            MirRelationExpr::Reduce {
                input,
                group_key,
                aggregates,
                monotonic: _,
                expected_group_size: _,
            } => {
                let mut new_columns = HashSet::new();
                for column in columns {
                    // No obvious requirements on aggregate columns.
                    // A "non-empty" requirement, I guess?
                    if column < group_key.len() {
                        group_key[column].non_null_requirements(&mut new_columns);
                    }
                    if column == group_key.len() && aggregates.len() == 1 {
                        aggregates[0].expr.non_null_requirements(&mut new_columns);
                    }
                }
                self.action(input, new_columns, gets);
            }
            MirRelationExpr::TopK {
                input, group_key, ..
            } => {
                // We can only allow rows to be discarded if their key columns are
                // NULL, as discarding rows based on other columns can change the
                // result set, based on how NULL is ordered.
                columns.retain(|c| group_key.contains(c));
                // TODO(mcsherry): bind NULL ordering and apply the tranformation
                // to all columns if the correct ASC/DESC ordering is observed
                // (with some care about orderings on multiple columns).
                self.action(input, columns, gets);
            }
            MirRelationExpr::Negate { input } => {
                self.action(input, columns, gets);
            }
            MirRelationExpr::Threshold { input } => {
                self.action(input, columns, gets);
            }
            MirRelationExpr::DeclareKeys { input, .. } => {
                self.action(input, columns, gets);
            }
            MirRelationExpr::Union { base, inputs } => {
                self.action(base, columns.clone(), gets);
                for input in inputs {
                    self.action(input, columns.clone(), gets);
                }
            }
            MirRelationExpr::ArrangeBy { input, .. } => {
                self.action(input, columns, gets);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Transform;
    use expr::{BinaryFunc, GlobalId, IdGen, MirScalarExpr, TableFunc};
    use repr::{ColumnType, RelationType, ScalarType};

    #[test]
    fn issue5520_regression_map_test() {
        let mut test_expr = MirRelationExpr::Filter {
            input: Box::new(MirRelationExpr::FlatMap {
                input: Box::new(MirRelationExpr::Map {
                    input: Box::new(MirRelationExpr::Get {
                        id: Id::Global(GlobalId::User(0)),
                        typ: RelationType::new(vec![
                            ColumnType {
                                nullable: true,
                                scalar_type: ScalarType::Int32,
                            },
                            ColumnType {
                                nullable: true,
                                scalar_type: ScalarType::Int64,
                            },
                        ]),
                    }),
                    scalars: vec![MirScalarExpr::literal_null(ScalarType::Int32)],
                }),
                func: TableFunc::GenerateSeriesInt32,
                exprs: vec![MirScalarExpr::Column(1)],
                demand: None,
            }),
            predicates: vec![MirScalarExpr::CallBinary {
                func: BinaryFunc::Eq,
                expr1: Box::new(MirScalarExpr::Column(0)),
                expr2: Box::new(MirScalarExpr::Column(3)),
            }],
        };
        let expected = test_expr.clone();
        let transform = NonNullRequirements;
        assert!(transform
            .transform(
                &mut test_expr,
                TransformArgs {
                    id_gen: &mut IdGen::default(),
                    indexes: &mut HashMap::new()
                }
            )
            .is_ok());
        assert_eq!(test_expr, expected);
    }
}
