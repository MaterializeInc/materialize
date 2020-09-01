// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Clippy's cognitive complexity is easy to reach.
//#![allow(clippy::cognitive_complexity)]

//! Transformations that allow join to make use of indexes.
//!
//! This is mostly a proof-of-concept that indexes work. The transformations in this module
//! may or may not belong together. Also, the transformations are subject to change as indexes
//! become more advanced.

use std::collections::HashMap;

use repr::RelationType;

use crate::TransformArgs;
use expr::{BinaryFunc, GlobalId, Id, RelationExpr, ScalarExpr};

/// Replaces filters of the form ScalarExpr::Column(i) == ScalarExpr::Literal, where i is a column for
/// which an index exists, with a
/// Join{
///   equivalences: [(0, i), (1,0)],
///   ArrangeBy{input, keys: [ScalarExpr::Column(i)]},
///   <constant>
/// }
/// TODO (wangandi): materialize#616 consider a general case when there exists in an index on an
/// expression of column i
#[derive(Debug)]
pub struct FilterEqualLiteral;

impl crate::Transform for FilterEqualLiteral {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        let mut indexes = HashMap::new();
        for (on_id, idxs) in args.indexes {
            let keys = idxs.iter().map(|(_id, keys)| keys.clone()).collect();
            indexes.insert(*on_id, keys);
        }
        self.transform(relation, &indexes);
        Ok(())
    }
}

impl FilterEqualLiteral {
    pub fn transform(&self, relation: &mut RelationExpr, indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,) {
        relation.visit_mut(&mut |e| {
            self.action(e, indexes);
        });
    }

    fn action(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) {
        if let RelationExpr::Filter { input, predicates } = relation {
            if let RelationExpr::Get {
                id: Id::Global(id), ..
            } = &mut **input
            {
                // gather predicates of the form CallBinary{Binaryfunc::Eq, Column, Literal}
                let (columns, predinfo): (Vec<_>, Vec<_>) = predicates
                    .iter()
                    .enumerate()
                    .filter_map(|(i, p)| {
                        if let ScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = p
                        {
                            match (&**expr1, &**expr2) {
                                (ScalarExpr::Literal(litrow, littyp), ScalarExpr::Column(c)) => {
                                    Some((*c, (litrow.clone(), littyp.clone(), i)))
                                }
                                (ScalarExpr::Column(c), ScalarExpr::Literal(litrow, littyp)) => {
                                    Some((*c, (litrow.clone(), littyp.clone(), i)))
                                }
                                _ => None,
                            }
                        } else {
                            None
                        }
                    })
                    .unzip();
                if !columns.is_empty() {
                    let key_set = &indexes[id];
                    // find set of keys of the largest size that is a subset of columns
                    let best_index = key_set
                        .iter()
                        .filter(|ks| {
                            ks.iter().all(|k| match k {
                                ScalarExpr::Column(c) => columns.contains(c),
                                _ => false,
                            })
                        })
                        .max_by_key(|ks| ks.len());
                    if let Some(keys) = best_index {
                        let column_order = keys
                            .iter()
                            .map(|k| match k {
                                ScalarExpr::Column(c) => {
                                    columns.iter().position(|d| c == d).unwrap()
                                }
                                _ => unreachable!(),
                            })
                            .collect::<Vec<_>>();
                        let mut constant_row = Vec::new();
                        let mut constant_col_types = Vec::new();
                        let mut variables = Vec::new();
                        for (new_idx, old_idx) in column_order.into_iter().enumerate() {
                            variables.push(vec![(0, columns[old_idx]), (1, new_idx)]);
                            constant_row.extend(predinfo[old_idx].0.unpack());
                            constant_col_types.push(predinfo[old_idx].1.clone());
                        }
                        let mut constant_type = RelationType::new(constant_col_types);
                        for i in 0..keys.len() {
                            constant_type = constant_type.add_keys(vec![i]);
                        }
                        let arity = input.arity();
                        let converted_join = RelationExpr::join(
                            vec![
                                input.take_dangerous().arrange_by(&[keys.clone()]),
                                RelationExpr::constant(vec![constant_row], constant_type),
                            ],
                            variables,
                        )
                        .project((0..arity).collect::<Vec<_>>());
                        *input = Box::new(converted_join);
                    }
                }
            }
        }
    }
}
