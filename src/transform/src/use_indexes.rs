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

/// Suppose you have
/// %0 =
/// | Get <some input>
/// | Filter (<Literal> = <Column>)
/// and an index on <some input>(<Column>).
///
/// Then this transform will turn the above plan into
/// %0 =
/// | Get <some input>
/// | ArrangeBy (<Column>)
///
/// %1 =
/// | Join %0 (<Column> <Literal>)
/// | | implementation = Unimplemented
/// | | demand = None
/// | Filter (<Literal> = <Column>)
///
/// Note that it is the responsibility of ColumnKnowledge (PredicateKnowledge
/// in the future) to clean up the Filter after the Join. It is the responsibility
/// of JoinImplementation to determine an implementation for the Join.
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
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) {
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
                if indexes.contains_key(id) {
                    // A map from column to the literal it is supposed to be
                    // equivalent to. It is possible that there is a badly written
                    // query where there are 2 filters `<column>=x` and `<column>=y`
                    // If x == y, then the two filters are the same, and it is
                    // ok to store just one copy in the matter.
                    // If x != y, because it doesn't matter what this transform
                    // does because `relation` will just become an empty Constant.
                    let mut expr_to_equivalent_literal: HashMap<ScalarExpr, ScalarExpr> =
                        HashMap::new();
                    // gather predicates of the form CallBinary{Binaryfunc::Eq,
                    // Column, Literal}
                    // TODO (wangandi): relax the requirement `Column` to be any
                    // arbitrary `ScalarExpr`. Currently blocked by
                    // `ColumnKnowledge` not having been extended to
                    // `PredicateKnowledge` yet. (See PR #3044)
                    for predicate in predicates.iter() {
                        if let ScalarExpr::CallBinary {
                            func: BinaryFunc::Eq,
                            expr1,
                            expr2,
                        } = predicate
                        {
                            match (&**expr1, &**expr2) {
                                (ScalarExpr::Literal(_, _), ScalarExpr::Column(_)) => {
                                    expr_to_equivalent_literal
                                        .insert((**expr2).clone(), (**expr1).clone());
                                }
                                (ScalarExpr::Column(_), ScalarExpr::Literal(_, _)) => {
                                    expr_to_equivalent_literal
                                        .insert((**expr1).clone(), (**expr2).clone());
                                }
                                _ => {}
                            }
                        }
                    }
                    if !expr_to_equivalent_literal.is_empty() {
                        let key_set = &indexes[id];
                        // find all sets of keys for which every column
                        // corresponds to an equality to a literal
                        // The JoinImplementation transform will decide which
                        // set of key is used
                        let spanning_key_set = key_set
                            .iter()
                            .filter_map(|ks| {
                                if ks
                                    .iter()
                                    .all(|k| expr_to_equivalent_literal.contains_key(k))
                                {
                                    Some(ks.clone())
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();
                        if !spanning_key_set.is_empty() {
                            let mut equivalences = Vec::new();
                            for spanning_keys in spanning_key_set.iter() {
                                for key in spanning_keys {
                                    let equivalent_literal =
                                        expr_to_equivalent_literal.remove(&key);
                                    if equivalent_literal.is_some() {
                                        equivalences
                                            .push(vec![key.clone(), equivalent_literal.unwrap()])
                                    }
                                }
                            }
                            let converted_join = RelationExpr::join_scalars(
                                vec![input.take_dangerous().arrange_by(&spanning_key_set)],
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
