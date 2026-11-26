// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! What a filter predicate implies about the values of an index key.
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
//! The analysis is exact: it never approximates, and a predicate it cannot read leaves the
//! key unbounded rather than guessed at. Its one limit is on the number of key values it
//! will enumerate for a lookup, which is a limit on the size of the constant collection a
//! plan may carry, the same one constant folding observes.
//!
//! NOTE: Literal values are compared as `Row`s, and `RowRef`'s `Ord` orders by the packed
//! byte representation rather than by `Datum::cmp`. Packing canonicalizes numerics, but
//! writes a float's raw bits, so `f = 0.0 AND f = '-0'::float8` intersects to the empty set
//! even though the two literals are equal in SQL (SQL-452). Byte identity is therefore the
//! definition of literal equality for everything in this module.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::MirScalarExpr;
use mz_expr::VariadicFunc;
use mz_expr::func::variadic::{And, Or};
use mz_repr::Row;

use crate::FOLD_CONSTANTS_LIMIT;

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
    /// An empty list means the predicate is never satisfied.
    boxes: Vec<KeyBox>,
    /// Whether matching a key field required inverting a cast on it. Reported so that the
    /// caller can prefer an index whose key needs no inversion.
    pub inv_cast: bool,
    /// The number of key fields, which is the width of every box.
    arity: usize,
}

impl KeyBounds {
    /// The bound of a predicate that says nothing about the key: every key value is
    /// admissible. It is the identity of `and`.
    fn top(arity: usize) -> Self {
        KeyBounds {
            boxes: vec![vec![None; arity]],
            inv_cast: false,
            arity,
        }
    }

    /// The bound of a predicate that is never satisfied.
    fn bottom(arity: usize) -> Self {
        KeyBounds {
            boxes: Vec::new(),
            inv_cast: false,
            arity,
        }
    }

    /// Extracts what a conjunction of predicates jointly says about `key`.
    pub fn conjunction<'a>(
        predicates: impl IntoIterator<Item = &'a MirScalarExpr>,
        key: &[MirScalarExpr],
    ) -> Self {
        predicates
            .into_iter()
            .map(|p| Self::extract(p, key))
            .fold(Self::top(key.len()), Self::and)
    }

    /// Extracts what `predicate` says about `key`.
    ///
    /// Linear in the size of `predicate`, apart from the box arithmetic, which is bounded by
    /// the number of distinct key tuples the predicate admits.
    pub fn extract(predicate: &MirScalarExpr, key: &[MirScalarExpr]) -> Self {
        mz_ore::stack::maybe_grow(|| match predicate {
            MirScalarExpr::CallVariadic {
                func: VariadicFunc::And(And),
                exprs,
            } => exprs
                .iter()
                .map(|e| Self::extract(e, key))
                .fold(Self::top(key.len()), Self::and),
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
                result.inv_cast |= inv_cast;
            }
        }
        result
    }

    /// The bound implied by both `self` and `other` holding: the pairwise intersection of
    /// their boxes.
    fn and(self, other: Self) -> Self {
        debug_assert_eq!(self.arity, other.arity);
        Self {
            boxes: Self::product(&self.boxes, &other.boxes, self.arity),
            inv_cast: self.inv_cast || other.inv_cast,
            arity: self.arity,
        }
    }

    /// The bound implied by any one of `args` holding: the union of their boxes.
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
            result.inv_cast |= arg.inv_cast;
            boxes.extend(arg.boxes);
        }
        result.boxes = Self::normalize(boxes, arity);
        result
    }

    /// Pairwise intersection of two box lists, dropping boxes that come out unsatisfiable.
    fn product(left: &[KeyBox], right: &[KeyBox], arity: usize) -> Vec<KeyBox> {
        let mut out = Vec::new();
        for l in left {
            for r in right {
                if let Some(b) = Self::intersect(l, r) {
                    out.push(b);
                }
            }
        }
        Self::normalize(out, arity)
    }

    /// Deduplicates a disjunction of boxes, and merges any two that differ in a single
    /// field by unioning that field.
    ///
    /// The merge is what keeps `a IN (<n values>)` to one box instead of `n` of them.
    fn normalize(mut boxes: Vec<KeyBox>, arity: usize) -> Vec<KeyBox> {
        boxes.sort();
        boxes.dedup();
        if boxes.len() < 2 {
            return boxes;
        }
        for i in 0..arity {
            // Merging on a field that every box agrees on is a no-op: two boxes sharing a
            // group would then agree on every field and have been deduplicated already.
            // Skipping those keeps the cost proportional to the fields that actually vary,
            // which is what makes a wide key affordable when most of it is pinned to single
            // values.
            if boxes.iter().all(|b| b[i] == boxes[0][i]) {
                continue;
            }
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

    /// The key values to look up.
    ///
    /// An empty result means the predicate is never satisfied. `None` means there is nothing
    /// to look up: either a key field is unbounded, or enumerating the values would exceed
    /// [`FOLD_CONSTANTS_LIMIT`], the size of constant collection a plan may carry. Callers
    /// that need to tell those apart should consult [`KeyBounds::bounds_every_field`] first.
    pub fn lookup_values(&self) -> Option<Vec<Row>> {
        if !self.bounds_every_field() {
            return None;
        }
        let mut values = BTreeSet::new();
        for b in &self.boxes {
            let sets = b.iter().map(|f| f.as_ref()).collect::<Option<Vec<_>>>()?;
            for combination in sets.into_iter().multi_cartesian_product() {
                values.insert(Row::pack(combination.iter().map(|r| r.unpack_first())));
                if values.len() > FOLD_CONSTANTS_LIMIT {
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
}

#[cfg(test)]
mod tests {
    use mz_expr::func;
    use mz_repr::{Datum, ReprScalarType};

    use super::*;

    fn lit(v: i32) -> MirScalarExpr {
        MirScalarExpr::literal_ok(Datum::Int32(v), ReprScalarType::Int32)
    }

    fn col_eq(c: usize, v: i32) -> MirScalarExpr {
        MirScalarExpr::column(c).call_binary(lit(v), func::BinaryFunc::Eq(func::Eq))
    }

    fn and(args: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::call_variadic(VariadicFunc::And(And), args)
    }

    fn or(args: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::call_variadic(VariadicFunc::Or(Or), args)
    }

    /// `(#0, #1) IN ((f(i), g(i)) for i in 0..n)`.
    fn pair_list(n: i32, f: impl Fn(i32) -> i32, g: impl Fn(i32) -> i32) -> MirScalarExpr {
        or((0..n)
            .map(|i| and(vec![col_eq(0, f(i)), col_eq(1, g(i))]))
            .collect())
    }

    fn key() -> Vec<MirScalarExpr> {
        vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]
    }

    fn values(bounds: &KeyBounds) -> Vec<(i32, i32)> {
        bounds
            .lookup_values()
            .expect("every field bounded")
            .iter()
            .map(|row| {
                let mut it = row.iter();
                (
                    it.next().unwrap().unwrap_int32(),
                    it.next().unwrap().unwrap_int32(),
                )
            })
            .collect()
    }

    #[mz_ore::test]
    fn independent_lists_form_one_box_and_a_product_of_values() {
        let p = and(vec![
            or(vec![col_eq(0, 1), col_eq(0, 2)]),
            or(vec![col_eq(1, 3), col_eq(1, 4)]),
        ]);
        let bounds = KeyBounds::extract(&p, &key());
        assert_eq!(bounds.boxes.len(), 1);
        assert_eq!(values(&bounds), vec![(1, 3), (1, 4), (2, 3), (2, 4)]);
    }

    #[mz_ore::test]
    fn pair_lists_keep_their_pairs() {
        let bounds = KeyBounds::extract(&pair_list(3, |i| i, |i| i + 10), &key());
        assert_eq!(values(&bounds), vec![(0, 10), (1, 11), (2, 12)]);
    }

    #[mz_ore::test]
    fn conjunctions_of_pair_lists_intersect_exactly() {
        // The two lists share exactly the pairs (i, i) for odd i in 0..40.
        let evens_and_all = pair_list(40, |i| i, |i| i);
        let shifted = pair_list(40, |i| i, |i| if i % 2 == 1 { i } else { i + 1 });
        let bounds = KeyBounds::conjunction([&evens_and_all, &shifted], &key());
        let expected: Vec<(i32, i32)> = (0..40).filter(|i| i % 2 == 1).map(|i| (i, i)).collect();
        assert_eq!(values(&bounds), expected);
    }

    #[mz_ore::test]
    fn disjoint_pair_lists_are_unsatisfiable() {
        let a = pair_list(40, |i| i, |i| i);
        let b = pair_list(40, |i| i, |i| i + 1);
        let bounds = KeyBounds::conjunction([&a, &b], &key());
        assert!(bounds.bounds_every_field());
        assert_eq!(bounds.lookup_values(), Some(Vec::new()));
    }

    #[mz_ore::test]
    fn an_unread_predicate_leaves_the_key_unbounded() {
        let opaque = MirScalarExpr::column(2).call_is_null();
        let p = and(vec![col_eq(0, 1), opaque]);
        let bounds = KeyBounds::extract(&p, &key());
        assert!(!bounds.bounds_every_field());
        assert_eq!(bounds.bounded_fields(), vec![0]);
        assert_eq!(bounds.lookup_values(), None);
    }

    #[mz_ore::test]
    fn a_disjunct_that_says_nothing_about_the_key_unbounds_it() {
        let p = or(vec![col_eq(0, 1), MirScalarExpr::column(2).call_is_null()]);
        let bounds = KeyBounds::extract(&p, &[MirScalarExpr::column(0)]);
        assert!(!bounds.bounds_every_field());
    }

    #[mz_ore::test]
    fn null_and_false_disjuncts_contribute_no_values() {
        let p = or(vec![
            and(vec![
                MirScalarExpr::literal_null(ReprScalarType::Bool),
                col_eq(1, 7),
            ]),
            and(vec![col_eq(0, 2), col_eq(1, 5)]),
        ]);
        let bounds = KeyBounds::extract(&p, &key());
        assert_eq!(values(&bounds), vec![(2, 5)]);
    }

    #[mz_ore::test]
    fn too_many_values_are_not_enumerated() {
        let wide = |c: usize| or((0..200).map(|v| col_eq(c, v)).collect());
        let p = and(vec![wide(0), wide(1)]);
        let bounds = KeyBounds::extract(&p, &key());
        assert!(bounds.bounds_every_field());
        assert_eq!(
            bounds.lookup_values(),
            None,
            "40,000 values exceed the limit"
        );
    }
}
