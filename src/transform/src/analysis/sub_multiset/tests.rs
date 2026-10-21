// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_expr::{LocalId, MirRelationExpr, MirScalarExpr};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, GlobalId, ReprRelationType, ReprScalarType};

use super::SubMultiset;

const TARGET: GlobalId = GlobalId::User(1);
const KNOWN: GlobalId = GlobalId::User(2);
const OTHER: GlobalId = GlobalId::User(3);

fn typ() -> ReprRelationType {
    ReprRelationType::new(vec![
        ReprScalarType::Int64.nullable(false),
        ReprScalarType::Int64.nullable(false),
    ])
}

fn get(id: GlobalId) -> MirRelationExpr {
    MirRelationExpr::global_get(id, typ())
}

fn local(id: u64) -> MirRelationExpr {
    MirRelationExpr::local_get(LocalId::new(id), typ())
}

fn proven(expr: &MirRelationExpr) -> bool {
    SubMultiset::new(TARGET, BTreeSet::from([KNOWN])).holds(expr, &OptimizerFeatures::default())
}

/// `Negate` without the double-negation elimination of [`MirRelationExpr::negate`].
fn raw_negate(input: MirRelationExpr) -> MirRelationExpr {
    MirRelationExpr::Negate {
        input: Box::new(input),
    }
}

fn union(inputs: Vec<MirRelationExpr>) -> MirRelationExpr {
    MirRelationExpr::union_many(inputs, typ())
}

fn intersect_all(a: MirRelationExpr, b: MirRelationExpr) -> MirRelationExpr {
    union(vec![
        a.clone(),
        union(vec![a, b.negate()]).threshold().negate(),
    ])
}

fn predicate() -> MirScalarExpr {
    MirScalarExpr::column(0).call_is_null().not()
}

#[mz_ore::test]
fn gets() {
    assert!(proven(&get(TARGET)));
    assert!(proven(&get(KNOWN)));
    assert!(!proven(&get(OTHER)));
}

#[mz_ore::test]
fn filters_and_arrangements_pass_through() {
    assert!(proven(&get(TARGET).filter([predicate()])));
    assert!(proven(
        &get(TARGET).arrange_by(&[vec![MirScalarExpr::column(0)]])
    ));
    assert!(!proven(&get(OTHER).filter([predicate()])));
}

#[mz_ore::test]
fn union_all_of_target_is_not_contained() {
    assert!(!proven(&union(vec![get(TARGET), get(TARGET)])));
    assert!(!proven(&union(vec![
        get(TARGET).filter([predicate()]),
        get(TARGET).filter([predicate().not()]),
    ])));
}

#[mz_ore::test]
fn join_is_not_contained() {
    let join = MirRelationExpr::join(vec![get(TARGET), get(OTHER)], vec![]);
    assert!(!proven(&join.project(vec![0, 1])));
}

#[mz_ore::test]
fn except_all() {
    assert!(proven(
        &union(vec![get(TARGET), get(OTHER).negate()]).threshold()
    ));
    assert!(proven(
        &union(vec![get(OTHER).negate(), get(TARGET)]).threshold()
    ));
    assert!(proven(
        &union(vec![get(KNOWN), get(OTHER).negate(), get(OTHER).negate()]).threshold()
    ));
    assert!(proven(&get(TARGET).threshold()));
    // The optimizer pushes `Negate` below `Map` and `Project`.
    let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
    let pushed = get(OTHER).project(vec![0]).negate().map(vec![one.clone()]);
    assert!(proven(&union(vec![get(TARGET), pushed]).threshold()));
    let positive = get(OTHER).project(vec![0]).map(vec![one]);
    assert!(!proven(&union(vec![get(TARGET), positive]).threshold()));
    // A doubly negated input adds rows.
    assert!(!proven(
        &union(vec![get(TARGET), raw_negate(raw_negate(get(OTHER)))]).threshold()
    ));
    // Two proven inputs can exceed the target.
    assert!(!proven(
        &union(vec![get(TARGET), get(KNOWN), get(OTHER).negate()]).threshold()
    ));
    assert!(!proven(
        &union(vec![get(TARGET), get(OTHER), get(OTHER).negate()]).threshold()
    ));
    assert!(!proven(
        &union(vec![get(OTHER), get(TARGET).negate()]).threshold()
    ));
    // Without the threshold, nothing is proven.
    assert!(!proven(&union(vec![get(TARGET), get(OTHER).negate()])));
}

#[mz_ore::test]
fn intersect_all_is_contained_in_either_input() {
    assert!(proven(&intersect_all(get(TARGET), get(OTHER))));
    assert!(proven(&intersect_all(get(OTHER), get(TARGET))));
    assert!(!proven(&intersect_all(get(OTHER), get(OTHER))));

    // Inputs of both unions in the other order.
    let swapped = union(vec![
        union(vec![get(OTHER).negate(), get(TARGET)])
            .threshold()
            .negate(),
        get(TARGET),
    ]);
    assert!(proven(&swapped));

    // With `A` proven, the subtracted input can be anything.
    let any_y = union(vec![
        get(TARGET),
        union(vec![get(TARGET), get(OTHER)]).threshold().negate(),
    ]);
    assert!(proven(&any_y));
    // With only `B` proven, it has to be subtracted.
    let added_b = union(vec![
        get(OTHER),
        union(vec![get(OTHER), get(TARGET)]).threshold().negate(),
    ]);
    assert!(!proven(&added_b));

    // The two copies of `A` must be the same expression.
    let mismatched = union(vec![
        get(OTHER),
        union(vec![get(TARGET), get(TARGET).negate()])
            .threshold()
            .negate(),
    ]);
    assert!(!proven(&mismatched));
}

#[mz_ore::test]
fn projections() {
    assert!(proven(&get(TARGET).project(vec![0, 1])));
    assert!(!proven(&get(TARGET).project(vec![1, 0])));
    assert!(!proven(&get(TARGET).project(vec![0])));
    assert!(!proven(&get(TARGET).project(vec![0, 0])));

    let one = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
    let mapped = get(TARGET).map(vec![one]);
    assert!(!proven(&mapped));
    assert!(proven(&mapped.clone().project(vec![0, 1])));
    assert!(!proven(&mapped.clone().project(vec![0, 2])));
    assert!(!proven(&mapped.project(vec![2, 1])));
}

#[mz_ore::test]
fn other_operators_are_not_proven() {
    assert!(!proven(&get(TARGET).negate()));
    assert!(!proven(&get(TARGET).distinct()));
    assert!(!proven(&MirRelationExpr::constant(vec![], typ())));
    assert!(proven(&get(TARGET).top_k(vec![], vec![], None, 1, None)));
    assert!(!proven(
        &union(vec![get(TARGET), get(OTHER).negate()]).top_k(vec![], vec![], None, 1, None)
    ));
}

#[mz_ore::test]
fn let_bindings() {
    let let_target = MirRelationExpr::Let {
        id: LocalId::new(1),
        value: Box::new(get(TARGET)),
        body: Box::new(intersect_all(get(OTHER), local(1))),
    };
    assert!(proven(&let_target));

    let let_other = MirRelationExpr::Let {
        id: LocalId::new(1),
        value: Box::new(get(OTHER)),
        body: Box::new(local(1).filter([predicate()])),
    };
    assert!(!proven(&let_other));

    let letrec = MirRelationExpr::LetRec {
        ids: vec![LocalId::new(1)],
        values: vec![get(TARGET)],
        limits: vec![None],
        body: Box::new(local(1)),
    };
    assert!(!proven(&letrec));
}
