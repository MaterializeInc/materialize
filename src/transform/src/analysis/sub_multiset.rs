// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Proves that an expression is a sub-multiset of a target collection.

use std::collections::BTreeSet;

use mz_expr::{Id, MirRelationExpr};
use mz_repr::GlobalId;
use mz_repr::optimize::OptimizerFeatures;

use crate::analysis::{Analysis, Arity, Derived, DerivedBuilder, NonNegative};

/// Analysis that determines whether an expression is a sub-multiset of the
/// collection `target`: at every time, every row's accumulated count in the
/// expression is at most its accumulated count in `target`.
///
/// `true` is a proof and `false` means "not proven". Every shape the analysis
/// does not recognize is `false`, which makes the analysis sound only as long
/// as each rule below is. A false `true` lets a caller retract rows a
/// collection does not hold.
///
/// The analysis trusts its parameters: `target` and every member of `known`
/// must be non-negative collections, and every member of `known` must be a
/// sub-multiset of `target` at every time. Like [`NonNegative`], which it uses,
/// it assumes all `Id::Global` collections are non-negative.
#[derive(Debug)]
pub struct SubMultiset {
    target: GlobalId,
    known: BTreeSet<GlobalId>,
}

impl SubMultiset {
    /// An analysis proving containment in `target`, given that every collection
    /// in `known` is already a sub-multiset of `target`.
    pub fn new(target: GlobalId, known: BTreeSet<GlobalId>) -> Self {
        Self { target, known }
    }

    /// Whether `expr` is proven to be a sub-multiset of the target.
    pub fn holds(self, expr: &MirRelationExpr, features: &OptimizerFeatures) -> bool {
        let mut builder = DerivedBuilder::new(features);
        builder.require(self);
        let derived = builder.visit(expr);
        derived
            .as_view()
            .value::<SubMultiset>()
            .copied()
            .unwrap_or(false)
    }
}

impl Analysis for SubMultiset {
    type Value = bool;

    fn announce_dependencies(builder: &mut DerivedBuilder) {
        builder.require(NonNegative);
        builder.require(Arity);
    }

    fn derive(
        &self,
        expr: &MirRelationExpr,
        index: usize,
        results: &[Self::Value],
        depends: &Derived,
    ) -> Self::Value {
        let non_negative = depends.results::<NonNegative>();
        let arity = depends.results::<Arity>();
        match expr {
            MirRelationExpr::Get {
                id: Id::Global(id), ..
            } => *id == self.target || self.known.contains(id),
            // `Let` substitutes its value, so a binding inherits the value's
            // result. A `LetRec` binding can be read before its value is
            // derived, which `get` reports as not proven, and the `LetRec` arm
            // below keeps any result inside one from escaping.
            MirRelationExpr::Get {
                id: Id::Local(id), ..
            } => depends
                .bindings()
                .get(id)
                .and_then(|offset| results.get(*offset))
                .copied()
                .unwrap_or(false),
            // The body is the last child.
            MirRelationExpr::Let { .. } => results[index - 1],
            MirRelationExpr::LetRec { .. } => false,
            MirRelationExpr::Filter { .. } | MirRelationExpr::ArrangeBy { .. } => {
                results[index - 1]
            }
            // With a non-negative input, a row's count in the output is at
            // most its count in the input.
            MirRelationExpr::TopK { .. } => results[index - 1] && non_negative[index - 1],
            MirRelationExpr::Project { input, outputs } => {
                // An identity projection, or one that drops exactly the
                // columns a `Map` appended, yields the rows of the `Map`'s
                // input.
                if is_identity(outputs, arity[index - 1]) {
                    results[index - 1]
                } else if let MirRelationExpr::Map { .. } = &**input {
                    is_identity(outputs, arity[index - 2]) && results[index - 2]
                } else {
                    false
                }
            }
            MirRelationExpr::Threshold { input } => {
                // `max(0, c)` is at most `c` wherever `c` is positive, and the
                // target is non-negative.
                if results[index - 1] {
                    return true;
                }
                // `max(0, p - b1 - b2 ...)` is at most `max(0, p)` when every
                // `bi` is non-negative.
                let MirRelationExpr::Union { .. } = &**input else {
                    return false;
                };
                let children = children(input, index - 1, depends);
                let mut proven = 0;
                for (child, offset) in children {
                    match child {
                        MirRelationExpr::Negate { .. } if non_negative[offset - 1] => {}
                        _ if results[offset] => proven += 1,
                        _ => return false,
                    }
                }
                proven == 1
            }
            MirRelationExpr::Union { .. } => is_proven_intersection(expr, index, results, depends),
            _ => false,
        }
    }
}

/// Whether `outputs` lists the columns `0..arity` in order.
fn is_identity(outputs: &[usize], arity: usize) -> bool {
    outputs.len() == arity && outputs.iter().enumerate().all(|(i, c)| i == *c)
}

/// The children of `expr`, at post-order `index`, in order, with their
/// post-order indexes.
fn children<'a>(
    expr: &'a MirRelationExpr,
    index: usize,
    depends: &Derived,
) -> Vec<(&'a MirRelationExpr, usize)> {
    let mut offsets: Vec<_> = depends
        .children_of_rev(index, expr.children().count())
        .collect();
    offsets.reverse();
    expr.children().zip(offsets).collect()
}

/// Recognizes `A INTERSECT ALL B` as planned,
/// `Union(A, Negate(Threshold(Union(A, Negate(B)))))` with either union's
/// inputs in either order, and proves it if `A` or `B` is proven.
///
/// The expression accumulates to `a - max(0, a - b)`, which is `min(a, b)`
/// for every `a` and `b`, so it is at most whichever input is proven. This
/// relies on both occurrences of `A` being structurally equal, and so
/// evaluating to the same collection.
fn is_proven_intersection(
    expr: &MirRelationExpr,
    index: usize,
    results: &[bool],
    depends: &Derived,
) -> bool {
    let outer = children(expr, index, depends);
    let [first, second] = outer.as_slice() else {
        return false;
    };
    for ((a, a_offset), (negated, negated_offset)) in [(first, second), (second, first)] {
        let MirRelationExpr::Negate { input } = negated else {
            continue;
        };
        let MirRelationExpr::Threshold { input } = &**input else {
            continue;
        };
        let MirRelationExpr::Union { .. } = &**input else {
            continue;
        };
        // `Negate` and `Threshold` are unary, so the inner union sits two
        // positions below the negation.
        let inner = children(input, negated_offset - 2, depends);
        let [inner_first, inner_second] = inner.as_slice() else {
            continue;
        };
        for ((a_copy, _), (negated_b, negated_b_offset)) in
            [(inner_first, inner_second), (inner_second, inner_first)]
        {
            let MirRelationExpr::Negate { .. } = negated_b else {
                continue;
            };
            if a_copy == a && (results[*a_offset] || results[negated_b_offset - 1]) {
                return true;
            }
        }
    }
    false
}

#[cfg(test)]
mod tests;
