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

use mz_expr::{
    Columns, FilterCharacteristics, JoinImplementation, JoinInputCharacteristics, JoinInputMapper,
    MirRelationExpr, MirScalarExpr,
};

use crate::eqsat::cost::JoinOrder;

/// Build the `JoinInputCharacteristics` for one order step (start or lookup)
/// from the structurally-known fields. `available` must be the ORIGINAL per-input
/// arrangement keys (before `implement_arrangements` wraps inputs in ArrangeBy),
/// matching `JoinImplementation`'s `arranged` computation. `cardinality` and
/// `filters` are left at their neutral values; the cardinality and selectivity
/// axes are future work.
fn step_characteristics(
    input: usize,
    key: &[MirScalarExpr],
    available: &[Vec<Vec<MirScalarExpr>>],
    inputs: &[MirRelationExpr],
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
        FilterCharacteristics::none(),
        input,
        enable_join_prioritize_arranged,
    )
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
                let chars =
                    step_characteristics(s.input, &key, available, inputs, prioritize_arranged);
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
        let c0 = step_characteristics(0, &[MirScalarExpr::column(0)], &available, &inputs, false);
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
        let c1 = step_characteristics(1, &[MirScalarExpr::column(0)], &available, &inputs, false);
        assert!(!c1.arranged(), "input 1 has no arrangement");
    }

    use crate::eqsat::cost::{JoinOrder, JoinStep};

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
