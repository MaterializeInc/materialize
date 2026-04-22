// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Property tests comparing [`FactBatch`] against a reference oracle.
//!
//! The oracle is a simple `BTreeMap<K, BTreeMap<V, Vec<(T, R)>>>` built from the
//! same sorted input. For any generated input, the FactBatch cursor must produce
//! identical traversal output.

use std::collections::BTreeMap;

use differential_dataflow::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
use proptest::prelude::*;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use super::KVUpdates;
use super::batch::{FactBatch, FactBuilder};
use super::column::FactColumn;

/// Build a FactBatch from sorted data with a given description.
fn build_fact_batch(
    data: &[(u64, u64, u64, i64)],
    lower: u64,
    upper: u64,
) -> FactBatch<u64, u64, u64, i64> {
    let mut chunk = FactColumn::Typed(KVUpdates::<u64, u64, u64, i64>::form(
        data.iter().map(|(k, v, t, d)| (k, v, (t, d))),
    ));
    let mut builder = FactBuilder::with_capacity(0, 0, 0);
    builder.push(&mut chunk);
    builder.done(Description::new(
        Antichain::from_elem(lower),
        Antichain::from_elem(upper),
        Antichain::from_elem(0u64),
    ))
}

/// Collect cursor output as `Vec<(K, V, Vec<(T, R)>)>`.
fn collect_cursor(batch: &FactBatch<u64, u64, u64, i64>) -> Vec<(u64, u64, Vec<(u64, i64)>)> {
    let mut result = Vec::new();
    let mut cursor = batch.cursor();
    while cursor.key_valid(batch) {
        while cursor.val_valid(batch) {
            let k = *cursor.key(batch);
            let v = *cursor.val(batch);
            let mut times = Vec::new();
            cursor.map_times(batch, |t, d| times.push((*t, *d)));
            result.push((k, v, times));
            cursor.step_val(batch);
        }
        cursor.step_key(batch);
    }
    result
}

/// Build oracle output from sorted data.
fn oracle_from_sorted(data: &[(u64, u64, u64, i64)]) -> Vec<(u64, u64, Vec<(u64, i64)>)> {
    let mut map: BTreeMap<u64, BTreeMap<u64, Vec<(u64, i64)>>> = BTreeMap::new();
    for &(k, v, t, d) in data {
        map.entry(k).or_default().entry(v).or_default().push((t, d));
    }
    let mut result = Vec::new();
    for (k, vals) in map {
        for (v, times) in vals {
            result.push((k, v, times));
        }
    }
    result
}

/// Sort and deduplicate input to produce valid sorted data for form().
/// Note: does NOT consolidate (sum diffs) — just sorts.
fn sort_input(data: &mut Vec<(u64, u64, u64, i64)>) {
    data.sort_by(|a, b| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2)));
}

/// Oracle for merge: combine two sorted datasets, apply compaction, consolidate.
fn oracle_merge(
    data1: &[(u64, u64, u64, i64)],
    data2: &[(u64, u64, u64, i64)],
    compaction_time: Option<u64>,
) -> Vec<(u64, u64, Vec<(u64, i64)>)> {
    let mut combined: Vec<(u64, u64, u64, i64)> = Vec::new();
    combined.extend_from_slice(data1);
    combined.extend_from_slice(data2);

    // Apply compaction: advance all times to max(time, compaction_time).
    if let Some(ct) = compaction_time {
        for (_, _, t, _) in &mut combined {
            *t = std::cmp::max(*t, ct);
        }
    }

    // Group by (k, v, t) and consolidate.
    let mut map: BTreeMap<(u64, u64, u64), i64> = BTreeMap::new();
    for (k, v, t, d) in combined {
        *map.entry((k, v, t)).or_default() += d;
    }

    // Remove zeros, build output.
    let mut result_map: BTreeMap<u64, BTreeMap<u64, Vec<(u64, i64)>>> = BTreeMap::new();
    for ((k, v, t), d) in map {
        if d != 0 {
            result_map
                .entry(k)
                .or_default()
                .entry(v)
                .or_default()
                .push((t, d));
        }
    }

    let mut result = Vec::new();
    for (k, vals) in result_map {
        for (v, times) in vals {
            result.push((k, v, times));
        }
    }
    result
}

proptest! {
    /// Cursor traversal of a FactBatch matches the oracle for any sorted input.
    #[test]
    fn cursor_matches_oracle(
        mut data in prop::collection::vec(
            (0..50u64, 0..30u64, 0..20u64, -3..3i64),
            0..200
        )
    ) {
        sort_input(&mut data);
        let batch = build_fact_batch(&data, 0, 1000);
        let fact_result = collect_cursor(&batch);
        let oracle_result = oracle_from_sorted(&data);
        prop_assert_eq!(fact_result, oracle_result);
    }

    /// seek_key lands on the correct key (first key >= target).
    #[test]
    fn seek_key_correctness(
        mut data in prop::collection::vec(
            (0..100u64, 0..50u64, 0..20u64, 1..5i64),
            1..100
        ),
        seek_targets in prop::collection::vec(0..120u64, 1..20),
    ) {
        sort_input(&mut data);
        let batch = build_fact_batch(&data, 0, 1000);

        for target in seek_targets {
            let mut cursor = batch.cursor();
            cursor.seek_key(&batch, &target);
            if cursor.key_valid(&batch) {
                // Cursor should point to first key >= target.
                prop_assert!(*cursor.key(&batch) >= target,
                    "seek_key({}) landed on {}", target, *cursor.key(&batch));
            }
            // All keys before cursor position should be < target.
            let mut check = batch.cursor();
            while check.key_valid(&batch) && *check.key(&batch) < target {
                check.step_key(&batch);
            }
            // check and cursor should be at the same position.
            if cursor.key_valid(&batch) {
                prop_assert!(check.key_valid(&batch));
                prop_assert_eq!(*check.key(&batch), *cursor.key(&batch));
            } else {
                prop_assert!(!check.key_valid(&batch));
            }
        }
    }

    /// seek_val lands on the correct val (first val >= target).
    #[test]
    fn seek_val_correctness(
        mut data in prop::collection::vec(
            (0..5u64, 0..50u64, 0..10u64, 1..3i64),
            1..100
        ),
        seek_targets in prop::collection::vec(0..60u64, 1..10),
    ) {
        sort_input(&mut data);
        let batch = build_fact_batch(&data, 0, 1000);

        let mut cursor = batch.cursor();
        if !cursor.key_valid(&batch) { return Ok(()); }

        for target in seek_targets {
            cursor.rewind_vals(&batch);
            cursor.seek_val(&batch, &target);
            if cursor.val_valid(&batch) {
                prop_assert!(*cursor.val(&batch) >= target,
                    "seek_val({}) landed on {}", target, *cursor.val(&batch));
            }
        }
    }

    /// Merge of two batches produces the same output as the oracle merge.
    #[test]
    fn merge_matches_oracle(
        mut data1 in prop::collection::vec(
            (0..50u64, 0..30u64, 0..10u64, -3..3i64),
            0..100
        ),
        mut data2 in prop::collection::vec(
            (0..50u64, 0..30u64, 10..20u64, -3..3i64),
            0..100
        ),
    ) {
        sort_input(&mut data1);
        sort_input(&mut data2);

        let batch1 = build_fact_batch(&data1, 0, 500);
        let batch2 = build_fact_batch(&data2, 500, 1000);

        let mut merger = batch1.begin_merge(&batch2, AntichainRef::new(&[]));
        merger.work(&batch1, &batch2, &mut 1_000_000);
        let merged = merger.done();

        let fact_result = collect_cursor(&merged);
        let oracle_result = oracle_merge(&data1, &data2, None);

        prop_assert_eq!(fact_result, oracle_result);
    }

    /// Merge with time compaction matches oracle.
    #[test]
    fn merge_compaction_matches_oracle(
        mut data1 in prop::collection::vec(
            (0..30u64, 0..20u64, 0..10u64, -2..2i64),
            0..80
        ),
        mut data2 in prop::collection::vec(
            (0..30u64, 0..20u64, 10..20u64, -2..2i64),
            0..80
        ),
        compaction_time in 0..25u64,
    ) {
        sort_input(&mut data1);
        sort_input(&mut data2);

        let batch1 = build_fact_batch(&data1, 0, 500);
        let batch2 = build_fact_batch(&data2, 500, 1000);

        let frontier = Antichain::from_elem(compaction_time);
        let mut merger = batch1.begin_merge(&batch2, frontier.borrow());
        merger.work(&batch1, &batch2, &mut 1_000_000);
        let merged = merger.done();

        let fact_result = collect_cursor(&merged);
        let oracle_result = oracle_merge(&data1, &data2, Some(compaction_time));

        prop_assert_eq!(fact_result, oracle_result);
    }

    /// Builder produces the same output as direct form() construction.
    #[test]
    fn builder_matches_form(
        mut data in prop::collection::vec(
            (0..50u64, 0..30u64, 0..20u64, -3..3i64),
            0..200
        )
    ) {
        sort_input(&mut data);

        // Build via Builder.
        let builder_batch = build_fact_batch(&data, 0, 1000);
        let builder_result = collect_cursor(&builder_batch);

        // Build via form() directly.
        let refs: Vec<_> = data.iter().map(|(k, v, t, d)| (k, v, (t, d))).collect();
        let storage = super::KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());
        let form_batch = FactBatch {
            storage,
            description: Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(1000u64),
                Antichain::from_elem(0u64),
            ),
            updates: data.len(),
        };
        let form_result = collect_cursor(&form_batch);

        prop_assert_eq!(builder_result, form_result);
    }

    /// Builder must dedup keys and vals across chunk boundaries.
    ///
    /// Splits sorted input at every possible point, pushes each half as a
    /// separate `FactColumn::Typed` chunk through the same `FactBuilder`, and
    /// verifies the resulting batch matches a one-shot `form()` build. This
    /// is the property the reverted attempt broke — a byte-bounded chunker
    /// can place the same key's vals in two adjacent chunks, so raw
    /// concatenation duplicates the key (yielding inflated counts, e.g.
    /// aoc_1204.slt returning 20696 vs expected 978).
    #[test]
    fn builder_dedups_across_chunks(
        mut data in prop::collection::vec(
            (0..10u64, 0..8u64, 0..5u64, 1..3i64),
            2..50
        ),
        split_points in prop::collection::vec(any::<usize>(), 0..5),
    ) {
        sort_input(&mut data);
        if data.is_empty() { return Ok(()); }

        // Reference: one-shot form().
        let refs: Vec<_> = data.iter().map(|(k, v, t, d)| (k, v, (t, d))).collect();
        let storage = super::KVUpdates::<u64, u64, u64, i64>::form(refs.into_iter());
        let form_batch = FactBatch {
            storage,
            description: Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(1000u64),
                Antichain::from_elem(0u64),
            ),
            updates: data.len(),
        };
        let form_result = collect_cursor(&form_batch);

        // Build via Builder with multiple chunks split at arbitrary points.
        let mut splits: Vec<usize> = split_points
            .into_iter()
            .map(|p| p % (data.len() + 1))
            .collect();
        splits.push(0);
        splits.push(data.len());
        splits.sort();
        splits.dedup();

        let mut builder = FactBuilder::with_capacity(0, 0, 0);
        for window in splits.windows(2) {
            let slice = &data[window[0]..window[1]];
            if slice.is_empty() { continue; }
            let mut chunk = FactColumn::Typed(
                super::KVUpdates::<u64, u64, u64, i64>::form(
                    slice.iter().map(|(k, v, t, d)| (k, v, (t, d))),
                ),
            );
            builder.push(&mut chunk);
        }
        let builder_batch = builder.done(Description::new(
            Antichain::from_elem(0u64),
            Antichain::from_elem(1000u64),
            Antichain::from_elem(0u64),
        ));
        let builder_result = collect_cursor(&builder_batch);

        prop_assert_eq!(builder_result, form_result);
    }
}
