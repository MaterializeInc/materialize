// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Cost-model-native join commit for eqsat extraction.
//!
//! Turns a left-deep [`JoinOrder`] chosen by the cost model into a committed
//! `MirRelationExpr::Join` with `JoinImplementation::Differential`, reusing
//! `JoinImplementation`'s mechanical lowering helpers
//! (`implement_arrangements` / `permute_order` / `install_lifted_mfp`). It does
//! not call JI's `optimize_orders` planner, `differential::plan`, or
//! `canonicalize_equivalences`: the order and keys come entirely from the cost
//! model, and the equivalences are left as extraction spelled them.
//!
//! The module also commits a `DeltaQuery` (via [`commit_delta_query`]) when the
//! cost model yields a fully-keyed delta plan that needs no new arrangements
//! beyond what differential would (delta is free), the same reuse-aware rule as
//! production's eager delta path.

use mz_expr::JoinImplementation::IndexedFilter;
use mz_expr::{
    Columns, FilterCharacteristics, JoinImplementation, JoinInputCharacteristics, JoinInputMapper,
    MapFilterProject, MirRelationExpr, MirScalarExpr,
};

use crate::eqsat::cost::{JoinOrder, JoinStep};

/// Build the `JoinInputCharacteristics` for one order step (start or lookup)
/// from the structurally-known fields. `available` must be the ORIGINAL per-input
/// arrangement keys (before `implement_arrangements` wraps inputs in ArrangeBy),
/// matching `JoinImplementation`'s `arranged` computation. `filters` is the
/// caller-derived filter characteristics for this step, computed per-input by
/// [`input_filter_characteristics`]. `cardinality` is left neutral; the
/// cardinality and selectivity axes are future work.
fn step_characteristics(
    input: usize,
    key: &[MirScalarExpr],
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    filters: FilterCharacteristics,
    enable_join_prioritize_arranged: bool,
) -> JoinInputCharacteristics {
    let arranged = available[input].iter().any(|k| k.as_slice() == key);
    // A unique key qualifies iff every one of its columns appears in `key`
    // (mirrors join_implementation.rs:1184). Non-column key members never match a
    // unique-key column, so they simply do not contribute.
    let keys = inputs[input].typ().keys;
    let unique_key = keys.iter().any(|cols| {
        cols.iter()
            .all(|c| key.contains(&MirScalarExpr::column(*c)))
    });
    JoinInputCharacteristics::new(
        unique_key,
        key.len(),
        arranged,
        None,
        filters,
        input,
        enable_join_prioritize_arranged,
    )
}

/// Derive the `FilterCharacteristics` that a single raised join input contributes,
/// mirroring the per-input half of `JoinImplementation`
/// (join_implementation.rs:233-296): the filter at the input's top MFP, plus the
/// literal-equality flag for an `IndexedFilter` input, plus the same two sources
/// behind a user-provided `ArrangeBy`.
///
/// JI additionally folds in predicates that could be pushed down from *above* the
/// join (`mfp_above`). That source is not reachable here: the eqsat raise commits
/// a join bottom-up with no visibility of the expression above it, so only the
/// per-input predicates carried on `input` contribute.
fn input_filter_characteristics(
    input: &MirRelationExpr,
) -> Result<FilterCharacteristics, mz_ore::stack::RecursionLimitError> {
    let (mfp, inner) = MapFilterProject::extract_non_errors_from_expr(input);
    let (_, filter, _) = mfp.as_map_filter_project();
    let mut characteristics = FilterCharacteristics::filter_characteristics(&filter)?;
    if matches!(
        inner,
        MirRelationExpr::Join {
            implementation: IndexedFilter(..),
            ..
        }
    ) {
        characteristics.add_literal_equality();
    }
    if let MirRelationExpr::ArrangeBy {
        input: arrange_by_input,
        ..
    } = inner
    {
        let (mfp, inner) = MapFilterProject::extract_non_errors_from_expr(arrange_by_input);
        let (_, filter, _) = mfp.as_map_filter_project();
        characteristics |= FilterCharacteristics::filter_characteristics(&filter)?;
        if matches!(
            inner,
            MirRelationExpr::Join {
                implementation: IndexedFilter(..),
                ..
            }
        ) {
            characteristics.add_literal_equality();
        }
    }
    Ok(characteristics)
}

/// Commit `join` (a bare, `Unimplemented` `Join`) to a `Differential` plan that
/// follows `order`. `available` gives each input's existing arrangement keys
/// (before `implement_arrangements` wraps them). `inputs` are the raised join
/// inputs, used to compute `JoinInputCharacteristics` for EXPLAIN. Returns `None`
/// (caller keeps the bare join) if `join` is not a `Join`, the order is empty,
/// or a valid start arrangement key cannot be formed.
pub(crate) fn commit_differential(
    mut join: MirRelationExpr,
    order: JoinOrder,
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    prioritize_arranged: bool,
) -> Option<MirRelationExpr> {
    if order.steps.is_empty() {
        return None;
    }
    // The join's inputs (for the `JoinInputMapper`) and equivalences (for the
    // start-key derivation), captured before the mutable borrow below.
    let (input_mapper, equivalences) = match &join {
        MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } => (JoinInputMapper::new(inputs), equivalences.clone()),
        _ => return None,
    };

    // Build the (input, local_key, characteristics) order; element 0 is the
    // start. Characteristics are populated here against the original `available`
    // (before implement_arrangements), and the start element is recomputed after
    // key alignment to reflect the aligned key.
    let mut order_tuples: Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)> =
        order
            .steps
            .iter()
            .map(|s| {
                let key: Vec<MirScalarExpr> = s
                    .key_cols
                    .iter()
                    .map(|&c| MirScalarExpr::column(c))
                    .collect();
                // Differential commit is out of scope for filter markers: pass
                // neutral filters so its EXPLAIN markers stay unchanged.
                let chars = step_characteristics(
                    s.input,
                    &key,
                    available,
                    inputs,
                    FilterCharacteristics::none(),
                    prioritize_arranged,
                );
                (s.input, key, Some(chars))
            })
            .collect();

    // Fix the START arrangement key. The cost model's per-step keys are correct
    // for the LOOKUP inputs (the renderer re-keys the stream side to match them),
    // but the start is the stream side of the *first* binary join: its
    // arrangement key is used verbatim and must line up component-for-component
    // with the first lookup's key (equal length, matching order). The cost
    // model's start key (all columns equated to any input) is over-wide when the
    // start is a join-graph hub, which makes the first join stage silently
    // produce no rows. Derive it exactly as `JoinImplementation` does
    // (join_implementation.rs:1305-1318): for each component of the first
    // lookup's key, find the equated expression bound in the start input.
    if order_tuples.len() >= 2 {
        let start = order_tuples[0].0;
        let second = order_tuples[1].0;
        let second_key = order_tuples[1].1.clone();
        let aligned: Vec<MirScalarExpr> = second_key
            .iter()
            .filter_map(|k| {
                let k = input_mapper.map_expr_to_global(k.clone(), second);
                input_mapper
                    .find_bound_expr(&k, &[start], &equivalences)
                    .map(|bound| input_mapper.map_expr_to_local(bound))
            })
            .collect();
        if aligned.len() != second_key.len() {
            // No start key aligned with the first lookup: fall back to the bare
            // Unimplemented join rather than emit a wrong plan.
            return None;
        }
        order_tuples[0].1 = aligned.clone();
        // Recompute the start's characteristics on the aligned key so the
        // EXPLAIN markers reflect the actual arrangement key used.
        order_tuples[0].2 = Some(step_characteristics(
            start,
            &aligned,
            available,
            inputs,
            FilterCharacteristics::none(),
            prioritize_arranged,
        ));
    }

    let MirRelationExpr::Join {
        inputs,
        implementation,
        ..
    } = &mut join
    else {
        return None;
    };

    let (start, mut start_key, start_characteristics) = order_tuples[0].clone();

    // Mechanical lowering: wrap inputs in ArrangeBy / lift MFPs for reuse.
    let (lifted_mfp, lifted_projections) =
        crate::join_implementation::implement_arrangements(inputs, available, order_tuples.iter());

    // Compensate keys for any projections lifted by `implement_arrangements`.
    if let Some(proj) = &lifted_projections[start] {
        start_key.iter_mut().for_each(|k| k.permute(proj));
    }
    crate::join_implementation::permute_order(&mut order_tuples, &lifted_projections);

    // The start arrangement is recorded separately; drop it from the remainder.
    order_tuples.remove(0);

    *implementation = JoinImplementation::Differential(
        (start, Some(start_key), start_characteristics),
        order_tuples,
    );

    crate::join_implementation::install_lifted_mfp(&mut join, lifted_mfp);
    Some(join)
}

/// Count the distinct lookup arrangements a delta plan needs that are not already
/// in `available`, deduplicated across all paths. Matches
/// `delta_queries::plan` (join_implementation.rs:727-739).
pub(crate) fn delta_new_arrangements(
    paths: &[Vec<JoinStep>],
    available: &[Vec<Vec<MirScalarExpr>>],
) -> usize {
    let mut missing: std::collections::BTreeSet<(usize, Vec<MirScalarExpr>)> =
        std::collections::BTreeSet::new();
    for path in paths {
        for step in path {
            let key: Vec<MirScalarExpr> = step
                .key_cols
                .iter()
                .map(|&c| MirScalarExpr::column(c))
                .collect();
            let arranged = available[step.input]
                .iter()
                .any(|k| k.as_slice() == key.as_slice());
            if !arranged {
                missing.insert((step.input, key));
            }
        }
    }
    missing.len()
}

/// Commit `join` (a bare `Unimplemented` `Join`) to a `DeltaQuery` following
/// `paths` (one lookup sequence per driver, driver excluded). `available` gives
/// each input's existing arrangement keys. Returns `None` if `join` is not a
/// `Join`. Reuses `JoinImplementation`'s mechanical lowering helpers.
///
/// NOTE: No start key or start element is stored for delta — the renderer
/// derives the driver's source key and each stream key. This avoids the
/// C1-style start-key alignment issue that differential requires.
pub(crate) fn commit_delta_query(
    mut join: MirRelationExpr,
    paths: Vec<Vec<JoinStep>>,
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
    prioritize_arranged: bool,
) -> Option<MirRelationExpr> {
    // Per-input filter characteristics, derived from the raised inputs the same
    // way `JoinImplementation` derives them (minus the unreachable push-down from
    // above the join, see `input_filter_characteristics`). On a recursion-limit
    // error, fall back to the bare join.
    let input_filters: Vec<FilterCharacteristics> = inputs
        .iter()
        .map(input_filter_characteristics)
        .collect::<Result<_, _>>()
        .ok()?;

    // Build the (input, key, characteristics) lookup tuples per path (lookups only,
    // no start element; the driver is excluded from each path by the cost model).
    // `paths` is indexed by driver position (cost.rs:1097-1104), so `paths[driver]`
    // is the delta chain when `driver` receives an update.
    //
    // Unlike DIFFERENTIAL's left-to-right cumulative OR (join_implementation.rs:
    // 843-854), a delta plan runs one differential dataflow per driver, and each
    // lookup arrangement is keyed on its own input alone. So each lookup's
    // characteristics carry only that input's own filters, never a driver's or an
    // earlier lookup's, matching `delta_queries::plan` (join_implementation.rs:
    // 698-773).
    let mut orders: Vec<Vec<(usize, Vec<MirScalarExpr>, Option<JoinInputCharacteristics>)>> = paths
        .iter()
        .map(|path| {
            path.iter()
                .map(|step| {
                    let key: Vec<MirScalarExpr> = step
                        .key_cols
                        .iter()
                        .map(|&c| MirScalarExpr::column(c))
                        .collect();
                    let chars = step_characteristics(
                        step.input,
                        &key,
                        available,
                        inputs,
                        input_filters[step.input].clone(),
                        prioritize_arranged,
                    );
                    (step.input, key, Some(chars))
                })
                .collect()
        })
        .collect();

    let MirRelationExpr::Join {
        inputs: join_inputs,
        implementation,
        ..
    } = &mut join
    else {
        return None;
    };

    // Mechanical lowering: wrap inputs in ArrangeBy / lift MFPs for reuse.
    let (lifted_mfp, lifted_projections) = crate::join_implementation::implement_arrangements(
        join_inputs,
        available,
        orders.iter().flatten(),
    );
    // Compensate keys for any projections lifted by implement_arrangements.
    orders
        .iter_mut()
        .for_each(|order| crate::join_implementation::permute_order(order, &lifted_projections));

    *implementation = JoinImplementation::DeltaQuery(orders);
    crate::join_implementation::install_lifted_mfp(&mut join, lifted_mfp);
    Some(join)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{Id, JoinImplementation, LocalId, MirRelationExpr};
    use mz_repr::{ReprRelationType, ReprScalarType};

    #[mz_ore::test]
    fn step_characteristics_reports_arranged_unique_and_len() {
        use mz_repr::{ReprRelationType, ReprScalarType};
        // input 0: arity 2, unique key {0}; input 1: arity 2, no key.
        let typ0 = ReprRelationType::new(vec![
            ReprScalarType::Int32.nullable(true),
            ReprScalarType::Int32.nullable(true),
        ])
        .with_keys(vec![vec![0]]);
        let in0 = MirRelationExpr::Get {
            id: mz_expr::Id::Local(mz_expr::LocalId::new(0)),
            typ: typ0,
            access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
        };
        let in1 = get(1, 2);
        let inputs = vec![in0, in1];
        // input 0 is arranged by [#0]; input 1 has no arrangements.
        let available = vec![vec![vec![MirScalarExpr::column(0)]], Vec::new()];

        // Lookup on input 0, key [#0]: arranged + unique + len 1.
        let c0 = step_characteristics(
            0,
            &[MirScalarExpr::column(0)],
            &available,
            &inputs,
            FilterCharacteristics::none(),
            false,
        );
        assert!(
            format!("{c0:?}").contains("key_length: 1"),
            "expected key_length 1, got {c0:?}"
        );
        assert!(c0.arranged(), "input 0 is arranged by [#0]");
        assert!(
            format!("{c0:?}").contains("unique_key: true"),
            "input 0 key [#0] covers the unique key {{0}}, got {c0:?}"
        );
        // Lookup on input 1, key [#0]: not arranged, not unique.
        let c1 = step_characteristics(
            1,
            &[MirScalarExpr::column(0)],
            &available,
            &inputs,
            FilterCharacteristics::none(),
            false,
        );
        assert!(!c1.arranged(), "input 1 has no arrangement");
    }

    use crate::eqsat::cost::{JoinOrder, JoinStep};

    #[mz_ore::test]
    fn delta_new_arrangements_counts_distinct_missing() {
        // 3 inputs. Lookups land on inputs 0, 1, and 2 (all keyed on [#0]); input 1
        // is already arranged by [#0], inputs 0 and 2 are not. Distinct missing =
        // {(0,[#0]), (2,[#0])} = 2.
        let available = vec![
            Vec::new(),
            vec![vec![MirScalarExpr::column(0)]], // input 1 arranged by [#0]
            Vec::new(),
        ];
        let paths = vec![
            vec![step(1, &[0]), step(2, &[0])], // driver 0
            vec![step(0, &[0]), step(2, &[0])], // driver 1 (input 0 lookup on [#0], not avail)
            vec![step(1, &[0]), step(0, &[0])], // driver 2
        ];
        // Missing distinct (input,key): (2,[#0]) and (0,[#0]) => 2.
        assert_eq!(delta_new_arrangements(&paths, &available), 2);
    }

    #[mz_ore::test]
    fn commit_delta_query_builds_deltaquery_shape() {
        let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs.clone(),
            vec![
                vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
                vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            ],
        );
        // One path per driver, lookups only.
        let paths = vec![
            vec![step(1, &[0]), step(2, &[0])],
            vec![step(0, &[0]), step(2, &[0])],
            vec![step(1, &[0]), step(0, &[1])],
        ];
        let available = vec![Vec::new(); 3];
        let out = commit_delta_query(join, paths, &available, &inputs, false)
            .expect("commit must succeed");
        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join");
        };
        match implementation {
            JoinImplementation::DeltaQuery(orders) => {
                assert_eq!(orders.len(), 3, "one path per driver");
                for o in orders {
                    assert_eq!(o.len(), 2, "two lookups per path");
                    assert!(o.iter().all(|(_, _, c)| c.is_some()), "chars populated");
                }
            }
            other => panic!("expected DeltaQuery, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn commit_delta_query_per_input_filter_markers() {
        use mz_expr::{BinaryFunc, func};
        use mz_repr::Datum;

        // input 0 carries a literal-equality filter `#0 = 5`; inputs 1 and 2 are
        // bare. JI-native DELTA assigns each lookup its own input's filter
        // characteristics (join_implementation.rs:698-773, `delta_queries::plan`),
        // unlike DIFFERENTIAL's cumulative left-to-right OR
        // (join_implementation.rs:843-854). So the lookup for input 0 must carry
        // the `e` (literal-equality) marker in every chain and position, while
        // lookups for inputs 1 and 2 must never carry it, even when they
        // immediately follow input 0's lookup in the same chain.
        let filtered0 = MirRelationExpr::Filter {
            input: Box::new(get(0, 2)),
            predicates: vec![MirScalarExpr::column(0).call_binary(
                MirScalarExpr::literal_ok(Datum::Int32(5), ReprScalarType::Int32),
                BinaryFunc::Eq(func::Eq),
            )],
        };
        let inputs = vec![filtered0, get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs.clone(),
            vec![
                vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
                vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            ],
        );
        // paths[driver]: driver 0 filtered, drivers 1 and 2 bare. Input 0's
        // lookup sits mid-chain for drivers 1 and 2, so any cumulative
        // propagation to the lookup that follows it would show up here.
        let paths = vec![
            vec![step(1, &[0]), step(2, &[0])], // driver 0 (filtered): bare, bare
            vec![step(0, &[0]), step(2, &[0])], // driver 1: filtered, then bare
            vec![step(0, &[0]), step(1, &[0])], // driver 2: filtered, then bare
        ];
        let available = vec![Vec::new(); 3];
        let out = commit_delta_query(join, paths, &available, &inputs, false)
            .expect("commit must succeed");
        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join");
        };
        let JoinImplementation::DeltaQuery(orders) = implementation else {
            panic!("expected DeltaQuery, got {implementation:?}");
        };
        let marks = |c: &Option<JoinInputCharacteristics>| c.as_ref().unwrap().explain();
        // Driver 0's chain never touches input 0: neither lookup (on inputs 1
        // and 2) carries `e`.
        assert!(!marks(&orders[0][0].2).contains('e'), "chain 0 lookup on 1");
        assert!(!marks(&orders[0][1].2).contains('e'), "chain 0 lookup on 2");
        // Driver 1's chain: the lookup on input 0 carries its own `e`, but the
        // following lookup on input 2 does NOT inherit it.
        assert!(marks(&orders[1][0].2).contains('e'), "chain 1 lookup on 0");
        assert!(
            !marks(&orders[1][1].2).contains('e'),
            "chain 1 lookup on 2 must not inherit input 0's filter"
        );
        // Driver 2's chain: same per-input isolation, with a different bare
        // input following the filtered one.
        assert!(marks(&orders[2][0].2).contains('e'), "chain 2 lookup on 0");
        assert!(
            !marks(&orders[2][1].2).contains('e'),
            "chain 2 lookup on 1 must not inherit input 0's filter"
        );
    }

    #[mz_ore::test]
    fn commit_delta_query_no_filters_no_markers() {
        // Sanity: with no per-input filters, no filter letters appear (regression
        // guard that we did not start fabricating markers).
        let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs.clone(),
            vec![
                vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
                vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
            ],
        );
        let paths = vec![
            vec![step(1, &[0]), step(2, &[0])],
            vec![step(0, &[0]), step(2, &[0])],
            vec![step(0, &[0]), step(1, &[0])],
        ];
        let available = vec![Vec::new(); 3];
        let out = commit_delta_query(join, paths, &available, &inputs, false)
            .expect("commit must succeed");
        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join");
        };
        let JoinImplementation::DeltaQuery(orders) = implementation else {
            panic!("expected DeltaQuery");
        };
        for order in orders {
            for (_, _, c) in order {
                let e = c.as_ref().unwrap().explain();
                assert!(
                    !e.contains('e') && !e.contains('l') && !e.contains('n') && !e.contains('f'),
                    "no filter markers expected, got {e:?}"
                );
            }
        }
    }

    fn get(local: u64, arity: usize) -> MirRelationExpr {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int32.nullable(true))
                .collect(),
        );
        MirRelationExpr::Get {
            id: Id::Local(LocalId::new(local)),
            typ,
            access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
        }
    }

    fn step(input: usize, key_cols: &[usize]) -> JoinStep {
        JoinStep {
            input,
            key_cols: key_cols.iter().copied().collect(),
        }
    }

    #[mz_ore::test]
    fn commit_differential_builds_expected_shape() {
        // 3-input join, no available arrangements.
        let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs.clone(),
            vec![
                vec![
                    mz_expr::MirScalarExpr::column(0),
                    mz_expr::MirScalarExpr::column(2),
                ],
                vec![
                    mz_expr::MirScalarExpr::column(2),
                    mz_expr::MirScalarExpr::column(4),
                ],
            ],
        );
        // Order: start at input 1 (key on local col 0), then input 0 (key local
        // col 0), then input 2 (key local col 0).
        let order = JoinOrder {
            steps: vec![step(1, &[0]), step(0, &[0]), step(2, &[0])],
        };
        let available = vec![Vec::new(); 3];

        let out = commit_differential(join, order, &available, &inputs, false)
            .expect("commit must succeed on a 3-input join");

        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join, got {out:?}");
        };
        match implementation {
            JoinImplementation::Differential((start, start_key, _), rest) => {
                assert_eq!(*start, 1, "start input");
                assert_eq!(
                    start_key.as_ref().map(|k| k.len()),
                    Some(1),
                    "start key len"
                );
                assert_eq!(rest.len(), 2, "two remaining inputs");
                assert_eq!(rest[0].0, 0);
                assert_eq!(rest[1].0, 2);
            }
            other => panic!("expected Differential, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn commit_differential_aligns_hub_start_key_to_first_lookup() {
        // Star join: input 0 is a hub connected to input 1 (on col 0) AND input 2
        // (on col 1). A naive start key (all hub cols equated to any input) would
        // be 2 columns wide, but the start must be arranged by exactly the first
        // lookup's key (1 column), aligned. Pass a deliberately over-wide start
        // key and assert commit_differential narrows it to match the first lookup.
        let inputs = vec![get(0, 2), get(1, 2), get(2, 2)];
        let join = MirRelationExpr::join_scalars(
            inputs.clone(),
            vec![
                // #0 (hub col 0) = #2 (input 1 col 0)
                vec![MirScalarExpr::column(0), MirScalarExpr::column(2)],
                // #1 (hub col 1) = #4 (input 2 col 0)
                vec![MirScalarExpr::column(1), MirScalarExpr::column(4)],
            ],
        );
        // Start = hub (0) with an OVER-WIDE key [0, 1]; first lookup = input 1
        // keyed on its local col 0 (== hub col 0); then input 2 on its local col 0.
        let order = JoinOrder {
            steps: vec![step(0, &[0, 1]), step(1, &[0]), step(2, &[0])],
        };
        let available = vec![Vec::new(); 3];

        let out = commit_differential(join, order, &available, &inputs, false)
            .expect("commit must succeed on a 3-input star join");

        let MirRelationExpr::Join { implementation, .. } = &out else {
            panic!("expected a Join, got {out:?}");
        };
        match implementation {
            JoinImplementation::Differential((start, start_key, _), rest) => {
                assert_eq!(*start, 0, "start is the hub");
                let start_key = start_key.as_ref().expect("start key present");
                // The fix: start key aligns with the FIRST lookup (1 column),
                // not the naive 2-column union — and equals the hub column the
                // first lookup keys against (local col 0 == #0).
                assert_eq!(
                    start_key,
                    &vec![MirScalarExpr::column(0)],
                    "start key must align 1:1 with the first lookup, not be over-wide"
                );
                assert_eq!(rest[0].0, 1, "first lookup is input 1");
                assert_eq!(
                    rest[0].1.len(),
                    start_key.len(),
                    "start/lookup key lengths match"
                );
            }
            other => panic!("expected Differential, got {other:?}"),
        }
    }
}
