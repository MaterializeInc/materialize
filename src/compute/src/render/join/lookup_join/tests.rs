// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::input::Input;
use mz_compute_types::dyncfgs::all_dyncfgs;
use mz_compute_types::plan::join::JoinClosure;
use mz_compute_types::plan::join::delta_join::DeltaStagePlan;
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_compute_types::plan::{ArrangementStrategy, AvailableCollections};
use mz_expr::{MapFilterProject, permutation_for_arrangement};
use mz_repr::{Datum, Timestamp};
use timely::dataflow::operators::Capture;
use timely::dataflow::operators::capture::Extract;
use timely::progress::Antichain;

use super::*;
use crate::render::columnar::vec_to_columnar;

fn identity_closure(arity: usize) -> JoinClosure {
    JoinClosure {
        ready_equivalences: Vec::new(),
        before: MapFilterProject::<LirScalarExpr>::new(arity)
            .into_plan()
            .unwrap()
            .into_nontemporal()
            .unwrap(),
    }
}

fn row(datums: &[Datum]) -> Row {
    Row::pack_slice(datums)
}

/// Joins a source `(k)` with a lookup relation `(k, v)` on `k`, producing `(k, v)`.
///
/// The source retracts its first update at time 2 and, at time 3, holds a `+1` and a `-1` of the
/// same row that arrive on different inputs, the way parts of a persist snapshot can.
#[mz_ore::test]
fn lookup_join_responds_to_positive_source_updates_only() {
    let key = vec![LirScalarExpr::column(0)];
    let (stream_permutation, stream_thinning) = permutation_for_arrangement(&key, 1);
    assert!(stream_permutation == vec![0] && stream_thinning.is_empty());
    let (lookup_permutation, lookup_thinning) = permutation_for_arrangement(&key, 2);
    let plan = LookupJoinPlan {
        source_relation: 0,
        initial_closure: identity_closure(1),
        stage_plans: vec![DeltaStagePlan {
            lookup_relation: 1,
            stream_key: key.clone(),
            stream_thinning,
            lookup_key: key.clone(),
            // The key datum, then the lookup value `v`.
            closure: identity_closure(2),
        }],
        final_closure: None,
    };

    let captured = timely::execute_directly(move |worker| {
        worker.dataflow::<Timestamp, _, _>(|scope| {
            let config_set = Rc::new(all_dyncfgs(ConfigSet::default()));
            let (mut source_a, source_a_rows) = scope.new_collection();
            let (mut source_b, source_b_rows) = scope.new_collection();
            let (mut lookup, lookup_rows) = scope.new_collection();
            let (_source_errs_input, source_errs) = scope.new_collection();
            let (_lookup_errs_input, lookup_errs) = scope.new_collection();

            let source = CollectionBundle::from_edge(
                vec_to_columnar(source_a_rows.concat(source_b_rows)),
                source_errs,
            );
            let lookup_bundle =
                CollectionBundle::from_edge(vec_to_columnar(lookup_rows), lookup_errs)
                    .ensure_collections(
                        AvailableCollections {
                            raw: false,
                            arranged: vec![(key, lookup_permutation, lookup_thinning)],
                        },
                        None,
                        MapFilterProject::<LirScalarExpr>::new(2)
                            .into_plan()
                            .unwrap(),
                        Antichain::from_elem(Timestamp::MIN),
                        Antichain::new(),
                        &config_set,
                        ArrangementStrategy::Direct,
                        ErrorScope::Row,
                    );

            let mut errs = Vec::new();
            let oks = build_lookup_join(
                &[source, lookup_bundle],
                plan,
                &config_set,
                ErrorScope::Row,
                &mut errs,
            );
            let captured = oks.inner.capture();

            let one = Datum::Int32(1);
            let two = Datum::Int32(2);
            lookup.update_at(
                row(&[one, Datum::String("a")]),
                Timestamp::from(0),
                Diff::ONE,
            );
            lookup.update_at(
                row(&[two, Datum::String("x")]),
                Timestamp::from(0),
                Diff::ONE,
            );
            // Visible to the source update at the same time.
            lookup.update_at(
                row(&[one, Datum::String("b")]),
                Timestamp::from(1),
                Diff::ONE,
            );
            // Only visible to source updates at later times.
            lookup.update_at(
                row(&[one, Datum::String("c")]),
                Timestamp::from(2),
                Diff::ONE,
            );
            source_a.update_at(row(&[one]), Timestamp::from(1), Diff::ONE);
            source_a.update_at(row(&[one]), Timestamp::from(2), -Diff::ONE);
            source_a.update_at(row(&[two]), Timestamp::from(3), Diff::ONE);
            source_b.update_at(row(&[two]), Timestamp::from(3), -Diff::ONE);
            source_a.update_at(row(&[one]), Timestamp::from(4), Diff::ONE);
            for input in [&mut source_a, &mut source_b, &mut lookup] {
                input.advance_to(Timestamp::from(5));
                input.flush();
            }
            captured
        })
    });

    let mut updates: Vec<_> = captured
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    updates.sort();
    let one = Datum::Int32(1);
    let expected = vec![
        (
            row(&[one, Datum::String("a")]),
            Timestamp::from(1),
            Diff::ONE,
        ),
        (
            row(&[one, Datum::String("a")]),
            Timestamp::from(4),
            Diff::ONE,
        ),
        (
            row(&[one, Datum::String("b")]),
            Timestamp::from(1),
            Diff::ONE,
        ),
        (
            row(&[one, Datum::String("b")]),
            Timestamp::from(4),
            Diff::ONE,
        ),
        (
            row(&[one, Datum::String("c")]),
            Timestamp::from(4),
            Diff::ONE,
        ),
    ];
    assert_eq!(updates, expected);
}
