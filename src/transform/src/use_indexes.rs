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

<<<<<<< HEAD
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
=======
/// Replaces filters of the form ScalarExpr::Column(i) == ScalarExpr::Literal, where i is a column for
/// which an index exists, with a
/// Join{
///   equivalences: [(0, i), (1,0)],
///   ArrangeBy{input, keys: [ScalarExpr::Column(i)]},
///   <constant>
/// }
>>>>>>> Do a semijoin when there is a filter on an index.
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
<<<<<<< HEAD
    fn transform(
        &self,
        relation: &mut RelationExpr,
        indexes: &HashMap<GlobalId, Vec<Vec<ScalarExpr>>>,
    ) {
=======
    /// Replaces filters of the form ScalarExpr::Column(i) == ScalarExpr::Literal, where i is a column for
    /// which an index exists, with a
    /// Join{
    ///   equivalences: [(0, i), (1,0)],
    ///   ArrangeBy{input, keys: [ScalarExpr::Column(i)]},
    ///   <constant>
    /// }
    pub fn transform(&self, relation: &mut RelationExpr, args: TransformArgs) {
>>>>>>> Do a semijoin when there is a filter on an index.
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
                    // TODO (wangandi): materialize#616 relax the requirement
                    // `Column` to be any arbitrary ScalarExpr
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
                }
            }
        }
    }
}
