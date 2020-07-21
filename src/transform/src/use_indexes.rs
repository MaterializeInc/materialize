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

use crate::TransformArgs;
use expr::{BinaryFunc, GlobalId, Id, RelationExpr, ScalarExpr};

/// Replaces filters of the form ScalarExpr::Column(i) == ScalarExpr::Literal, where i is a column for
/// which an index exists, with a
/// Join{
///   equivalences: [(0, i), (1,0)],
///   ArrangeBy{input, keys: [ScalarExpr::Column(i)]},
///   <constant>
/// }
#[derive(Debug)]
pub struct FilterEqualLiteral;

impl crate::Transform for FilterEqualLiteral {
    fn transform(
        &self,
        relation: &mut RelationExpr,
        args: TransformArgs,
    ) -> Result<(), crate::TransformError> {
        self.transform(relation, args);
        Ok(())
    }
}

impl FilterEqualLiteral {
    /// Replaces filters of the form ScalarExpr::Column(i) == ScalarExpr::Literal, where i is a column for
    /// which an index exists, with a
    /// Join{
    ///   equivalences: [(0, i), (1,0)],
    ///   ArrangeBy{input, keys: [ScalarExpr::Column(i)]},
    ///   <constant>
    /// }
    pub fn transform(&self, relation: &mut RelationExpr, args: TransformArgs) {
        relation.visit_mut(&mut |e| {
            self.action(e, args.indexes);
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
                if indexes.contains_key(id) {
                    let mut predicates_by_column: HashMap<ScalarExpr, (ScalarExpr, usize)> =
                        HashMap::new();
                    // gather predicates of the form CallBinary{Binaryfunc::Eq,
                    // Column, Literal}
                    // TODO (wangandi): materialize#616 relax the requirement
                    // `Column` to be any arbitrary ScalarExpr
                    for (predicate_num, predicate) in predicates.iter().enumerate() {
                        if let ScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = predicate
                        {
                            match (&**expr1, &**expr2) {
                                (ScalarExpr::Literal(_, _), ScalarExpr::Column(_)) => {
                                    predicates_by_column.insert(
                                        (**expr2).clone(),
                                        ((**expr1).clone(), predicate_num),
                                    );
                                }
                                (ScalarExpr::Column(_), ScalarExpr::Literal(_, _)) => {
                                    predicates_by_column.insert(
                                        (**expr1).clone(),
                                        ((**expr2).clone(), predicate_num),
                                    );
                                }
                                _ => {}
                            }
                        }
                    }
                    if !predicates_by_column.is_empty() {
                        let key_set = &indexes[id];
                        // find set of keys of the largest size that is a subset of columns
                        let best_index = key_set
                            .iter()
                            .filter(|ks| ks.iter().all(|k| predicates_by_column.contains_key(k)))
                            .max_by_key(|ks| ks.len());
                        if let Some(keys) = best_index {
                            let mut equivalences = Vec::new();
                            for key in keys {
                                equivalences.push(vec![
                                    key.clone(),
                                    predicates_by_column.remove(key).unwrap().0,
                                ]);
                            }
                            let converted_join = RelationExpr::join_scalars(
                                vec![input.take_dangerous().arrange_by(&[keys.clone()])],
                                equivalences,
                            );
                            *input = Box::new(converted_join);
                        }
                    }
                }
            }
        }
    }
}
