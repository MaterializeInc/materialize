// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Index-directed extraction of literal constraints from a filter predicate.
//!
//! The question this module answers is always asked about a specific list of key
//! expressions: "given that this predicate holds, which values can these expressions
//! take?" Everything in the predicate that says nothing about those expressions is
//! invisible to the answer, and costs a single visit of the node.
//!
//! The answer is a [`KeyBounds`]: a disjunction of conjunctive boxes, where a box bounds
//! each key field independently. Two shapes motivate the representation.
//!
//! * `a IN (1, 2) AND b IN (3, 4)` is one box, `{a: {1,2}, b: {3,4}}`. The four key values
//!   are the cross product, formed over datums rather than over expression nodes.
//! * `(a, b) IN ((1, 3), (2, 4))` is two boxes, `{a: {1}, b: {3}}` and `{a: {2}, b: {4}}`.
//!   Collapsing it to one box would admit `(1, 4)`, which the predicate rejects.
//!
//! `AND` intersects boxes pairwise and `OR` concatenates them, so the box count is bounded
//! by the number of distinct key tuples the predicate admits. It does not grow with
//! disjunctions over columns that the index does not cover.
//!
//! NOTE: A `KeyBounds` is only ever a *sound* bound. Over-approximating is always safe for
//! choosing lookup values, because the residual filter still runs and the constant
//! collection the lookups become has distinct rows, so the semi-join cannot duplicate.
//! Removing a constraint from the filter is a different claim, and requires
//! [`KeyBounds::exact`].

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::MirScalarExpr;
use mz_expr::VariadicFunc;
use mz_expr::func::variadic::{And, Or};
use mz_repr::Row;

/// Largest number of boxes we will carry. Beyond this we widen, trading exactness for a
/// bound on our own work. The cap is on the disjunction width, not on the number of key
/// values a box denotes, so a wide `IN` list over a single column costs one box.
const MAX_BOXES: usize = 1024;

/// Largest number of key values we will ask an index to look up. Above this a full scan
/// plus filter is the better plan, and the constant collection would itself be a burden.
const MAX_LOOKUP_VALUES: usize = 100_000;

/// The values a single key field may take. `None` means the predicate does not bound it.
///
/// `Some` is never empty: a field bounded to no values makes its whole box unsatisfiable,
/// and such boxes are dropped rather than stored.
type FieldBound = Option<BTreeSet<Row>>;

/// One conjunctive bound on all key fields, entry `i` bounding key field `i`.
type KeyBox = Vec<FieldBound>;

/// What a predicate implies about a list of key expressions.
#[derive(Clone, Debug)]
pub struct KeyBounds {
    /// The key can only take a value that falls inside at least one of these boxes.
    ///
    /// An empty list therefore means the predicate is never satisfied.
    boxes: Vec<KeyBox>,
    /// Whether `boxes` characterizes the predicate exactly, so that the predicate is
    /// equivalent to "the key falls in one of these boxes".
    ///
    /// False either because the predicate constrains something besides the key fields, or
    /// because we widened to stay inside [`MAX_BOXES`]. Only an exact bound may be removed
    /// from the filter.
    exact: bool,
    /// Whether matching a key field required inverting a cast on it. Reported so that the
    /// caller can prefer an index whose key needs no inversion.
    pub inv_cast: bool,
    /// The number of key fields, which is the width of every box.
    arity: usize,
}

impl KeyBounds {
    /// The bound of a predicate we cannot read: every key value is admissible, and the
    /// predicate is not equivalent to that, so it must stay in the filter.
    fn top(arity: usize) -> Self {
        KeyBounds {
            boxes: vec![vec![None; arity]],
            exact: false,
            inv_cast: false,
            arity,
        }
    }

    /// The bound of a predicate that is always satisfied, which is the identity of `and`.
    ///
    /// NOTE: This differs from [`KeyBounds::top`] only in `exact`, and the difference
    /// matters. `top` stands for "we could not read this predicate", so it must not be
    /// removed from the filter. `unit` stands for "there is nothing here to read".
    fn unit(arity: usize) -> Self {
        KeyBounds {
            boxes: vec![vec![None; arity]],
            exact: true,
            inv_cast: false,
            arity,
        }
    }

    /// The bound of a predicate that is never satisfied.
    fn bottom(arity: usize) -> Self {
        KeyBounds {
            boxes: Vec::new(),
            exact: true,
            inv_cast: false,
            arity,
        }
    }

    /// Extracts what a conjunction of predicates jointly says about `key`.
    pub fn conjunction<'a>(
        predicates: impl IntoIterator<Item = &'a MirScalarExpr>,
        key: &[MirScalarExpr],
    ) -> Self {
        Self::conjunction_of(
            predicates.into_iter().map(|p| Self::extract(p, key)),
            key.len(),
        )
    }

    /// The bound implied by all of `args` holding.
    fn conjunction_of(args: impl IntoIterator<Item = Self>, arity: usize) -> Self {
        args.into_iter().fold(Self::unit(arity), Self::and)
    }

    /// Extracts what `predicate` says about `key`.
    ///
    /// Linear in the size of `predicate`, apart from the box arithmetic, which is bounded
    /// by [`MAX_BOXES`].
    pub fn extract(predicate: &MirScalarExpr, key: &[MirScalarExpr]) -> Self {
        mz_ore::stack::maybe_grow(|| match predicate {
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::And(And),
                exprs,
            } => Self::conjunction_of(exprs.iter().map(|e| Self::extract(e, key)), key.len()),
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::Or(Or),
                exprs,
            } => Self::disjunction(exprs.iter().map(|e| Self::extract(e, key)), key.len()),
            _ => Self::leaf(predicate, key),
        })
    }

    /// Extracts what a predicate with no `AND`/`OR` at its root says about `key`.
    fn leaf(predicate: &MirScalarExpr, key: &[MirScalarExpr]) -> Self {
        // NOTE: `null` counts as never satisfied because these are filter predicates, where
        // a row that evaluates to `null` is dropped just as a `false` one is. A literal
        // *error* is not: that row errors out rather than being filtered away, so it stays
        // opaque.
        if predicate.is_literal_false() || predicate.is_literal_null() {
            return Self::bottom(key.len());
        }
        // A literal equality whose cast cannot be inverted without erroring is never true.
        if predicate.impossible_literal_equality_because_types() {
            return Self::bottom(key.len());
        }
        let mut result = Self::top(key.len());
        // A single leaf can pin more than one key field, if the key holds both an
        // expression and a cast of it. Recording all of them is sound and no less precise.
        for (i, key_field) in key.iter().enumerate() {
            if let Some((literal, inv_cast)) = predicate.expr_eq_literal(key_field) {
                result.boxes[0][i] = Some(BTreeSet::from([literal]));
                result.exact = true;
                result.inv_cast |= inv_cast;
            }
        }
        result
    }

    /// The bound implied by both `self` and `other` holding.
    fn and(self, other: Self) -> Self {
        debug_assert_eq!(self.arity, other.arity);
        let arity = self.arity;
        let inv_cast = self.inv_cast || other.inv_cast;

        // Widen before multiplying, so the product stays inside the budget. Only the wider
        // operand is widened, because widening both would discard structure that
        // `MAX_BOXES` can still afford to keep.
        let (left, right, exact) = if self.boxes.len() * other.boxes.len() > MAX_BOXES {
            if self.boxes.len() >= other.boxes.len() {
                (Self::widen(&self.boxes, arity), other.boxes, false)
            } else {
                (self.boxes, Self::widen(&other.boxes, arity), false)
            }
        } else {
            (self.boxes, other.boxes, self.exact && other.exact)
        };

        Self {
            boxes: Self::product(&left, &right),
            exact,
            inv_cast,
            arity,
        }
    }

    /// The bound implied by any one of `args` holding.
    ///
    /// NOTE: Taken n-ary rather than folded pairwise. Folding would normalize the
    /// accumulator once per argument, which is quadratic in the width of an `IN` list, and
    /// an `IN` list is the case that matters most here.
    fn disjunction(args: impl IntoIterator<Item = Self>, arity: usize) -> Self {
        let mut boxes = Vec::new();
        // A disjunction with no arguments is `false`, which `bottom` already describes.
        let mut result = Self::bottom(arity);
        for arg in args {
            debug_assert_eq!(arg.arity, arity);
            result.exact &= arg.exact;
            result.inv_cast |= arg.inv_cast;
            boxes.extend(arg.boxes);
        }
        result.boxes = Self::normalize(boxes, arity);
        if result.boxes.len() > MAX_BOXES {
            result.boxes = Self::widen(&result.boxes, arity);
            result.exact = false;
        }
        result
    }

    /// Pairwise intersection of two box lists, dropping boxes that come out unsatisfiable.
    fn product(left: &[KeyBox], right: &[KeyBox]) -> Vec<KeyBox> {
        let mut out = Vec::new();
        for l in left {
            for r in right {
                if let Some(b) = Self::intersect(l, r) {
                    out.push(b);
                }
            }
        }
        Self::normalize(out, left.first().map_or(0, |b| b.len()))
    }

    /// Deduplicates a disjunction of boxes, and merges any two that differ in a single
    /// field by unioning that field.
    ///
    /// The merge is what keeps `a IN (<n values>)` to one box instead of `n` of them, which
    /// matters for both cost and exactness: `n` boxes would blow the [`MAX_BOXES`] budget
    /// and force a widening, when the single merged box is an exact answer.
    fn normalize(mut boxes: Vec<KeyBox>, arity: usize) -> Vec<KeyBox> {
        boxes.sort();
        boxes.dedup();
        for i in 0..arity {
            // Group by every field but `i`, then union field `i` within each group.
            let mut groups: BTreeMap<KeyBox, FieldBound> = BTreeMap::new();
            for mut b in boxes {
                let field = b[i].take();
                match groups.entry(b) {
                    Entry::Vacant(e) => {
                        e.insert(field);
                    }
                    Entry::Occupied(mut e) => {
                        // An unbounded field stays unbounded in the union.
                        let merged = match (e.get_mut().take(), field) {
                            (Some(mut l), Some(r)) => {
                                l.extend(r);
                                Some(l)
                            }
                            _ => None,
                        };
                        *e.get_mut() = merged;
                    }
                }
            }
            boxes = groups
                .into_iter()
                .map(|(mut b, field)| {
                    b[i] = field;
                    b
                })
                .collect();
        }
        boxes
    }

    /// Intersects two boxes, returning `None` if no key value satisfies both.
    fn intersect(left: &KeyBox, right: &KeyBox) -> Option<KeyBox> {
        left.iter()
            .zip_eq(right.iter())
            .map(|(l, r)| match (l, r) {
                (None, None) => Some(None),
                (None, Some(s)) | (Some(s), None) => Some(Some(s.clone())),
                (Some(l), Some(r)) => {
                    let both: BTreeSet<Row> = l.intersection(r).cloned().collect();
                    // An empty field bound makes the whole box unsatisfiable.
                    (!both.is_empty()).then_some(Some(both))
                }
            })
            .collect()
    }

    /// Collapses a disjunction of boxes into the single box that contains all of them.
    ///
    /// Sound but lossy: the result admits key values that no input box did, which is why
    /// every caller clears `exact`.
    fn widen(boxes: &[KeyBox], arity: usize) -> Vec<KeyBox> {
        if boxes.is_empty() {
            // "Never satisfied" needs no widening, and widening it would be wrong.
            return Vec::new();
        }
        let widened = (0..arity)
            .map(|i| {
                // A field left unconstrained by any one box is unconstrained in the union.
                boxes
                    .iter()
                    .map(|b| b[i].as_ref())
                    .fold_options(BTreeSet::new(), |mut acc, s| {
                        acc.extend(s.iter().cloned());
                        acc
                    })
            })
            .collect();
        vec![widened]
    }

    /// The key values to look up.
    ///
    /// An empty result means the predicate is never satisfied. `None` means the value count
    /// exceeds [`MAX_LOOKUP_VALUES`], for which there is no useful advice to give: a full
    /// scan really is the better plan.
    ///
    /// Callers must establish that every key field is bounded, via
    /// [`KeyBounds::bounds_every_field`], before calling this.
    pub fn lookup_values(&self) -> Option<Vec<Row>> {
        assert!(self.bounds_every_field(), "unbounded key field");
        let mut values = BTreeSet::new();
        for b in &self.boxes {
            let sets = b
                .iter()
                .map(|f| f.as_ref().expect("checked by bounds_every_field"))
                .collect_vec();
            for combination in sets.into_iter().multi_cartesian_product() {
                values.insert(Row::pack(combination.iter().map(|r| r.unpack_first())));
                if values.len() > MAX_LOOKUP_VALUES {
                    return None;
                }
            }
        }
        Some(values.into_iter().collect())
    }

    /// Whether every key field is bounded in every box, which is what makes an index
    /// usable at all.
    pub fn bounds_every_field(&self) -> bool {
        self.arity > 0 && self.boxes.iter().all(|b| b.iter().all(|f| f.is_some()))
    }

    /// The key fields that every box bounds.
    ///
    /// When this is a strict, non-empty subset of the key, an index on just these fields
    /// would have been usable, which is what the "index too wide" notice reports.
    pub fn bounded_fields(&self) -> Vec<usize> {
        (0..self.arity)
            .filter(|i| self.boxes.iter().all(|b| b[*i].is_some()))
            .collect()
    }

    /// Whether no key value at all satisfies the predicate, which makes the whole relation
    /// empty.
    ///
    /// Sound in the same way the lookup values are: every rule here only ever widens the
    /// set of admissible key values, so an empty result really means empty.
    pub fn is_unsatisfiable(&self) -> bool {
        self.boxes.is_empty()
    }

    /// Whether the bound characterizes the predicate exactly, so that replacing the
    /// predicate with the corresponding key lookup preserves meaning.
    pub fn exact(&self) -> bool {
        self.exact
    }
}

/// Replaces every subexpression of `predicates` that can never be satisfied with `false`.
///
/// `predicates` is an implicit conjunction, as an MFP's predicate list is.
///
/// This is what turns `a IN (1, 2) AND a IN (2, 3, 4)` into `a = 2`, and it applies whether
/// or not any index is involved, so `key` should be the full list of literal-pinned
/// expressions from [`literal_constrained_exprs`] rather than an index key.
///
/// Bottom-up and single-pass, carrying each node's bounds back up so that no subtree is
/// analyzed twice. A pruned child reports itself unsatisfiable, so a parent whose every
/// disjunct died is pruned in the same pass. The leftover `false` arguments are for
/// `MirScalarExpr::reduce` to clean up.
/// Returns whether anything was pruned, and whether the conjunction is unsatisfiable as a
/// whole. The latter covers contradictions that span two predicates, such as `c IN (1, 2)`
/// alongside `c IN (3, 4)`, which no amount of pruning inside either one would reveal.
pub fn prune_unsatisfiable<'a>(
    predicates: impl IntoIterator<Item = &'a mut MirScalarExpr>,
    key: &[MirScalarExpr],
) -> (bool, bool) {
    let mut changed = false;
    let bounds = predicates
        .into_iter()
        .map(|p| prune_inner(p, key, &mut changed))
        .collect_vec();
    let empty = KeyBounds::conjunction_of(bounds, key.len()).is_unsatisfiable();
    (changed, empty)
}

fn prune_inner(
    predicate: &mut MirScalarExpr,
    key: &[MirScalarExpr],
    changed: &mut bool,
) -> KeyBounds {
    mz_ore::stack::maybe_grow(|| {
        let bounds = match predicate {
            MirScalarExpr::CallVariadic {
                func: func @ (VariadicFunc::And(And) | VariadicFunc::Or(Or)),
                exprs,
            } => {
                let is_and = matches!(func, VariadicFunc::And(_));
                let child_bounds = exprs
                    .iter_mut()
                    .map(|e| prune_inner(e, key, changed))
                    .collect_vec();
                if is_and {
                    KeyBounds::conjunction_of(child_bounds, key.len())
                } else {
                    KeyBounds::disjunction(child_bounds, key.len())
                }
            }
            _ => KeyBounds::leaf(predicate, key),
        };
        if bounds.is_unsatisfiable() && !predicate.is_literal_false() {
            *predicate = MirScalarExpr::literal_false();
            *changed = true;
        }
        bounds
    })
}

/// Every expression that the predicate constrains to literal values somewhere.
///
/// Index-blind, and used for two things: cheaply rejecting an index whose key mentions an
/// expression the predicate never pins, and recommending a key to a user whose index was
/// too wide.
pub fn literal_constrained_exprs<'a>(
    predicates: impl IntoIterator<Item = &'a MirScalarExpr>,
) -> Vec<MirScalarExpr> {
    let mut found = BTreeSet::new();
    for predicate in predicates {
        predicate.visit_pre(|e| {
            if let Some(expr) = e.any_expr_eq_literal() {
                found.insert(expr);
            }
        });
    }
    found.into_iter().collect()
}
