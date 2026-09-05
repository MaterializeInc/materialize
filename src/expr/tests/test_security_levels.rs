// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Property tests for the ordering guarantee security levels carry.
//!
//! The guarantee is stated here once, declaratively, as [`is_admissible`], and
//! checked against `MapFilterProject` on arbitrary predicate lists. It is
//! written from the intended semantics rather than derived from
//! `sort_predicates`, so the two are independent and can disagree.
//!
//! See `doc/developer/design/20260828_security_barrier_views.md`.

use mz_expr::{MapFilterProject, MirScalarExpr, Predicate};
use proptest::prelude::*;

/// The ordering requirement, stated independently of how it is implemented.
///
/// A predicate may be evaluated only after every predicate at a lower level.
/// `MapFilterProject` evaluates its predicates in list order and stops at the
/// first that fails, so list order is evaluation order, and admissibility is a
/// property of the list.
fn is_admissible<E>(predicates: &[(usize, Predicate<E>)]) -> bool {
    predicates
        .windows(2)
        .all(|pair| pair[0].1.level() <= pair[1].1.level())
}

/// Levels present in a predicate list, as a sorted multiset.
fn levels<E>(predicates: &[(usize, Predicate<E>)]) -> Vec<u8> {
    let mut levels: Vec<_> = predicates.iter().map(|(_, p)| p.level()).collect();
    levels.sort();
    levels
}

const ARITY: usize = 8;

/// Predicates as `(level, column)`. Varying the column varies the position the
/// predicate would sort to on its own, which is what makes the level's priority
/// over position observable.
fn predicate_specs() -> impl Strategy<Value = Vec<(u8, usize)>> {
    prop::collection::vec((0u8..4, 0usize..ARITY), 0..12)
}

fn build(specs: &[(u8, usize)]) -> MapFilterProject {
    MapFilterProject::new(ARITY).filter_leveled(
        specs
            .iter()
            .map(|(level, col)| Predicate::at_level(MirScalarExpr::column(*col), *level)),
    )
}

proptest! {
    /// Adding predicates yields an admissible order, whatever order they arrive
    /// in and whichever columns they reference.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn filter_leveled_is_admissible(specs in predicate_specs()) {
        let mfp = build(&specs);
        prop_assert!(is_admissible(&mfp.predicates), "not admissible: {:?}", levels(&mfp.predicates));
    }

    /// Sorting reorders and never invents, drops, or relabels a predicate.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn filter_leveled_preserves_levels(specs in predicate_specs()) {
        let mfp = build(&specs);
        let mut expected: Vec<u8> = specs.iter().map(|(level, _)| *level).collect();
        expected.sort();
        prop_assert_eq!(levels(&mfp.predicates), expected);
    }

    /// `optimize` rebuilds the predicate list, so it has to re-establish the
    /// order rather than inherit it. This is the property that a future
    /// transform which rebuilds an MFP would break.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn optimize_preserves_admissibility(specs in predicate_specs()) {
        let mut mfp = build(&specs);
        mfp.optimize();
        prop_assert!(is_admissible(&mfp.predicates), "not admissible: {:?}", levels(&mfp.predicates));
    }

    /// `optimize` may drop a predicate, by deduplicating or by folding it away,
    /// but it may never lower one: every level it produces was already present.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn optimize_never_lowers_a_level(specs in predicate_specs()) {
        let before = build(&specs);
        let expected = levels(&before.predicates);
        let mut after = before;
        after.optimize();
        for level in levels(&after.predicates) {
            prop_assert!(expected.contains(&level), "invented level {level}");
        }
    }
}

/// The ordering constraint outranks position, which is the whole point: a
/// constrained predicate over an early column must still wait for an
/// unconstrained predicate over a later one.
#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn level_outranks_position() {
    let mfp = MapFilterProject::new(ARITY).filter_leveled([
        Predicate::at_level(MirScalarExpr::column(0), 1),
        Predicate::at_level(MirScalarExpr::column(7), 0),
    ]);
    let order: Vec<_> = mfp.predicates.iter().map(|(_, p)| p.level()).collect();
    assert_eq!(order, vec![0, 1], "level must outrank position");
}
