// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::iter;
use std::sync::LazyLock;

use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{BatchReader, Cursor, Navigable};
use itertools::EitherOrBoth;
use maplit::btreemap;
use mz_ore::cast::CastFrom;
use mz_repr::{
    CatalogItemId, ColumnName, Datum, Diff, Row, RowPacker, SqlColumnType, SqlScalarType,
};
use timely::progress::Antichain;

use crate::avro::DiffPair;

/// Walks `batch` and invokes `on_diff_pair` for each `DiffPair` at each
/// `(key, timestamp)`.
///
/// Thin wrapper around `iter_diff_pairs`.
pub fn for_each_diff_pair<B, C, F>(batch: &B, mut on_diff_pair: F)
where
    B: BatchReader<Time = C::Time> + Navigable<Cursor = C>,
    C: Cursor<Storage = B, Diff = Diff>,
    C::Time: Copy,
    C::ValOwn: 'static,
    F: FnMut(&<C::KeyContainer as BatchContainer>::Owned, C::Time, DiffPair<C::ValOwn>),
{
    for (key, timed_pairs) in iter_diff_pairs(batch, None, None) {
        for (time, pair) in timed_pairs {
            on_diff_pair(&key, time, pair);
        }
    }
}

/// Walks `batch` and emits, for each key, an iterator of the `DiffPair`s at
/// each timestamp.
///
/// Ignores updates outside the specified time range. Inclusive 'lower', exclusive 'upper'.
///
/// Within a key, diffs are partitioned by sign into retractions (befores) and
/// insertions (afters), sorted by timestamp, and zipped into `DiffPair`s via a
/// merge-join. Pairs are emitted in ascending timestamp order for a given key;
/// no ordering is guaranteed across keys. Callers are responsible for tracking
/// `(key, timestamp)` boundaries themselves if they need to detect groups
/// with more than one pair (e.g., for primary-key violation checks).
///
/// The per-key iterator owns its data, so it can be held or consumed after the
/// outer iterator has advanced. An update with multiplicity `n` fans out into
/// `n` pairs lazily, cloning the value as pairs are consumed. Memory held per
/// key is proportional to the number of distinct updates, not the fan-out.
pub fn iter_diff_pairs<B, C>(
    batch: &B,
    lower: Option<Antichain<C::Time>>,
    upper: Option<Antichain<C::Time>>,
) -> impl Iterator<
    Item = (
        <C::KeyContainer as BatchContainer>::Owned,
        impl Iterator<Item = (C::Time, DiffPair<C::ValOwn>)>,
    ),
>
where
    B: BatchReader<Time = C::Time> + Navigable<Cursor = C>,
    C: Cursor<Storage = B, Diff = Diff>,
    C::Time: Copy,
    C::ValOwn: 'static,
{
    let mut cursor = batch.cursor();
    iter::from_fn(move || {
        while cursor.key_valid(batch) {
            let k = cursor.key(batch);

            // Partition updates at this key into retractions (befores) and
            // insertions (afters). The buffers move into the yielded iterator,
            // so they are per key rather than reused across keys.
            let mut befores: Vec<(C::Time, C::ValOwn, usize)> = vec![];
            let mut afters: Vec<(C::Time, C::ValOwn, usize)> = vec![];
            while cursor.val_valid(batch) {
                let v = cursor.val(batch);
                cursor.map_times(batch, |t, diff| {
                    let t = C::owned_time(t);
                    if lower.as_ref().is_some_and(|lower| !lower.less_equal(&t))
                        || upper.as_ref().is_some_and(|upper| upper.less_equal(&t))
                    {
                        // This record is outside the specified time interval. Ignore it.
                        return;
                    }
                    let diff = C::owned_diff(diff);
                    let update = (t, C::owned_val(v), usize::cast_from(diff.unsigned_abs()));
                    if diff < Diff::ZERO {
                        befores.push(update);
                    } else {
                        afters.push(update);
                    }
                });
                cursor.step_val(batch);
            }

            if befores.is_empty() && afters.is_empty() {
                // No records at this key. Try the next one.
                cursor.step_key(batch);
                continue;
            }

            befores.sort_by_key(|(t, _v, _diff)| *t);
            afters.sort_by_key(|(t, _v, _diff)| *t);

            // The use of `repeat_n()` here is intentional.
            // Typically, cnt = 1, and `repeat_n((t, v), cnt)` will return the original `(t, v)`.
            // `iter::repeat((t, v)).take(cnt)` would clone `v` `cnt` times even when `cnt = 1`.
            let fan_out = |(t, v, cnt): (C::Time, C::ValOwn, usize)| iter::repeat_n((t, v), cnt);
            let befores_iter = befores.into_iter().flat_map(fan_out);
            let afters_iter = afters.into_iter().flat_map(fan_out);

            let key_owned = <C::KeyContainer as BatchContainer>::into_owned(k);

            let pairs =
                itertools::merge_join_by(befores_iter, afters_iter, |(t1, _v1), (t2, _v2)| {
                    t1.cmp(t2)
                })
                .map(|pair| {
                    let (t, before, after) = match pair {
                        EitherOrBoth::Both((t, before), (_t, after)) => {
                            (t, Some(before), Some(after))
                        }
                        EitherOrBoth::Left((t, before)) => (t, Some(before), None),
                        EitherOrBoth::Right((t, after)) => (t, None, Some(after)),
                    };
                    (t, DiffPair { before, after })
                });

            cursor.step_key(batch);
            return Some((key_owned, pairs));
        }
        None
    })
}

// NOTE(benesch): statically allocating transient IDs for the
// transaction and row types is a bit of a hack to allow us to attach
// custom names to these types in the generated Avro schema. In the
// future, these types should be real types that get created in the
// catalog with userspace IDs when the user creates the sink, and their
// names and IDs should be plumbed in from the catalog at the moment
// the sink is created.
pub(crate) const TRANSACTION_TYPE_ID: CatalogItemId = CatalogItemId::Transient(1);
pub(crate) const DBZ_ROW_TYPE_ID: CatalogItemId = CatalogItemId::Transient(2);

pub static ENVELOPE_CUSTOM_NAMES: LazyLock<BTreeMap<CatalogItemId, String>> = LazyLock::new(|| {
    btreemap! {
        TRANSACTION_TYPE_ID => "transaction".into(),
        DBZ_ROW_TYPE_ID => "row".into(),
    }
});

pub(crate) fn dbz_envelope(
    names_and_types: Vec<(ColumnName, SqlColumnType)>,
) -> Vec<(ColumnName, SqlColumnType)> {
    let row = SqlColumnType {
        nullable: true,
        scalar_type: SqlScalarType::Record {
            fields: names_and_types.into(),
            custom_id: Some(DBZ_ROW_TYPE_ID),
        },
    };
    vec![("before".into(), row.clone()), ("after".into(), row)]
}

pub fn dbz_format(rp: &mut RowPacker, dp: DiffPair<Row>) {
    if let Some(before) = dp.before {
        rp.push_list_with(|rp| rp.extend_by_row(&before));
    } else {
        rp.push(Datum::Null);
    }
    if let Some(after) = dp.after {
        rp.push_list_with(|rp| rp.extend_by_row(&after));
    } else {
        rp.push(Datum::Null);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use differential_dataflow::trace::implementations::chunker::ContainerChunker;
    use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder};
    use differential_dataflow::trace::{Batcher, Builder};
    use timely::container::{ContainerBuilder, PushInto};
    use timely::progress::Antichain;

    use super::*;

    /// Seals a single batch from an unordered list of `((key, val), time, diff)`
    /// tuples upper-bounded at `upper`.
    fn batch_from_tuples_with<V: Ord + Clone + 'static>(
        mut tuples: Vec<((String, V), u64, Diff)>,
        upper: u64,
    ) -> <ValBuilder<String, V, u64, Diff> as Builder>::Output {
        // The batcher consumes already-chunked input via `PushInto`; chunking
        // is the caller's responsibility.
        let mut batcher = ValBatcher::<String, V, u64, Diff>::new(None, 0);
        let mut chunker = ContainerChunker::<Vec<((String, V), u64, Diff)>>::default();
        chunker.push_into(&mut tuples);
        while let Some(chunk) = chunker.extract() {
            batcher.push_into(std::mem::take(chunk));
        }
        while let Some(chunk) = chunker.finish() {
            batcher.push_into(std::mem::take(chunk));
        }
        let (mut chain, description) = batcher.seal(Antichain::from_elem(upper));
        ValBuilder::<String, V, u64, Diff>::seal(&mut chain, description)
    }

    /// `batch_from_tuples_with` pinned to `String` values, so call sites can
    /// build values with `.into()`.
    fn batch_from_tuples(
        tuples: Vec<((String, String), u64, Diff)>,
        upper: u64,
    ) -> <ValBuilder<String, String, u64, Diff> as Builder>::Output {
        batch_from_tuples_with(tuples, upper)
    }

    /// Collects `for_each_diff_pair` invocations into a flat, deterministically
    /// sorted list for easy assertion.
    fn collect_diff_pairs<B, C>(batch: &B) -> Vec<(String, u64, Option<String>, Option<String>)>
    where
        B: BatchReader<Time = C::Time> + Navigable<Cursor = C>,
        C: Cursor<Storage = B, Diff = Diff>,
        C::Time: Copy + Into<u64>,
        C::ValOwn: 'static + Into<String>,
        <C::KeyContainer as BatchContainer>::Owned: Into<String> + Clone,
    {
        let mut out = vec![];
        for_each_diff_pair(batch, |k, t, dp| {
            out.push((
                k.clone().into(),
                t.into(),
                dp.before.map(Into::into),
                dp.after.map(Into::into),
            ));
        });
        out.sort();
        out
    }

    /// Collects `iter_diff_pairs` output into a per-key list, sorted by key
    /// for deterministic assertion. Pair order within a key is preserved.
    fn collect_bounded_diff_pairs<B, C>(
        batch: &B,
        lower: Option<u64>,
        upper: Option<u64>,
    ) -> Vec<(String, Vec<(u64, Option<String>, Option<String>)>)>
    where
        B: BatchReader<Time = u64> + Navigable<Cursor = C>,
        C: Cursor<Storage = B, Diff = Diff, Time = u64>,
        C::ValOwn: 'static + Into<String>,
        <C::KeyContainer as BatchContainer>::Owned: Into<String>,
    {
        let mut out: Vec<_> = iter_diff_pairs(
            batch,
            lower.map(Antichain::from_elem),
            upper.map(Antichain::from_elem),
        )
        .map(|(k, pairs)| {
            let pairs = pairs
                .into_iter()
                .map(|(t, dp)| (t, dp.before.map(Into::into), dp.after.map(Into::into)))
                .collect();
            (k.into(), pairs)
        })
        .collect();
        out.sort();
        out
    }

    #[mz_ore::test]
    fn single_insertion() {
        let batch = batch_from_tuples(vec![(("k1".into(), "v1".into()), 5, Diff::ONE)], 6);
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(pairs, vec![("k1".into(), 5, None, Some("v1".into()))]);
    }

    #[mz_ore::test]
    fn single_retraction() {
        let batch = batch_from_tuples(vec![(("k1".into(), "v1".into()), 5, -Diff::ONE)], 6);
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(pairs, vec![("k1".into(), 5, Some("v1".into()), None)]);
    }

    #[mz_ore::test]
    fn update_at_same_timestamp() {
        // Retract v1 and insert v2 at the same timestamp → paired into a single
        // DiffPair with both before and after populated.
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, -Diff::ONE),
                (("k1".into(), "v2".into()), 5, Diff::ONE),
            ],
            6,
        );
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(
            pairs,
            vec![("k1".into(), 5, Some("v1".into()), Some("v2".into()))]
        );
    }

    #[mz_ore::test]
    fn update_across_timestamps() {
        // Insert v1 at t=5, then replace with v2 at t=10.
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, Diff::ONE),
                (("k1".into(), "v1".into()), 10, -Diff::ONE),
                (("k1".into(), "v2".into()), 10, Diff::ONE),
            ],
            11,
        );
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(
            pairs,
            vec![
                ("k1".into(), 5, None, Some("v1".into())),
                ("k1".into(), 10, Some("v1".into()), Some("v2".into())),
            ]
        );
    }

    #[mz_ore::test]
    fn diff_greater_than_one_fans_out() {
        // Diff=3 becomes three independent `DiffPair`s at the same timestamp.
        let batch = batch_from_tuples(vec![(("k1".into(), "v1".into()), 5, Diff::from(3))], 6);
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(
            pairs,
            vec![
                ("k1".into(), 5, None, Some("v1".into())),
                ("k1".into(), 5, None, Some("v1".into())),
                ("k1".into(), 5, None, Some("v1".into())),
            ]
        );
    }

    #[mz_ore::test]
    fn multiple_keys_are_independent() {
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, Diff::ONE),
                (("k2".into(), "v2".into()), 5, Diff::ONE),
            ],
            6,
        );
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(
            pairs,
            vec![
                ("k1".into(), 5, None, Some("v1".into())),
                ("k2".into(), 5, None, Some("v2".into())),
            ]
        );
    }

    #[mz_ore::test]
    fn unpaired_before_and_after_at_different_timestamps() {
        // Retraction at t=5, insertion at t=10 — they do NOT pair because they
        // live at different timestamps.
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, -Diff::ONE),
                (("k1".into(), "v2".into()), 10, Diff::ONE),
            ],
            11,
        );
        let pairs = collect_diff_pairs(&batch);
        assert_eq!(
            pairs,
            vec![
                ("k1".into(), 5, Some("v1".into()), None),
                ("k1".into(), 10, None, Some("v2".into())),
            ]
        );
    }

    #[mz_ore::test]
    fn pairs_are_grouped_by_key() {
        // One item per key, pairs within a key in ascending timestamp order.
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, Diff::ONE),
                (("k1".into(), "v1".into()), 10, -Diff::ONE),
                (("k1".into(), "v2".into()), 10, Diff::ONE),
                (("k2".into(), "v3".into()), 7, Diff::ONE),
            ],
            11,
        );
        let items = collect_bounded_diff_pairs(&batch, None, None);
        assert_eq!(
            items,
            vec![
                (
                    "k1".into(),
                    vec![
                        (5, None, Some("v1".into())),
                        (10, Some("v1".into()), Some("v2".into())),
                    ]
                ),
                ("k2".into(), vec![(7, None, Some("v3".into()))]),
            ]
        );
    }

    #[mz_ore::test]
    fn lower_bound_is_inclusive() {
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 4, Diff::ONE),
                (("k1".into(), "v2".into()), 5, Diff::ONE),
            ],
            6,
        );
        let items = collect_bounded_diff_pairs(&batch, Some(5), None);
        assert_eq!(
            items,
            vec![("k1".into(), vec![(5, None, Some("v2".into()))])]
        );
    }

    #[mz_ore::test]
    fn upper_bound_is_exclusive() {
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, Diff::ONE),
                (("k1".into(), "v2".into()), 6, Diff::ONE),
            ],
            7,
        );
        let items = collect_bounded_diff_pairs(&batch, None, Some(6));
        assert_eq!(
            items,
            vec![("k1".into(), vec![(5, None, Some("v1".into()))])]
        );
    }

    #[mz_ore::test]
    fn bounds_filter_within_a_key() {
        // The window [6, 11) drops the t=5 insertion but keeps the t=10
        // retraction/insertion, which still pair with each other.
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 5, Diff::ONE),
                (("k1".into(), "v1".into()), 10, -Diff::ONE),
                (("k1".into(), "v2".into()), 10, Diff::ONE),
            ],
            11,
        );
        let items = collect_bounded_diff_pairs(&batch, Some(6), Some(11));
        assert_eq!(
            items,
            vec![(
                "k1".into(),
                vec![(10, Some("v1".into()), Some("v2".into()))]
            )]
        );
    }

    #[mz_ore::test]
    fn fully_filtered_key_emits_no_item() {
        // All of k1's updates fall outside the bounds, so k1 must not appear
        // at all, not even as a key with an empty pair list.
        let batch = batch_from_tuples(
            vec![
                (("k1".into(), "v1".into()), 3, Diff::ONE),
                (("k2".into(), "v2".into()), 5, Diff::ONE),
            ],
            6,
        );
        let items = collect_bounded_diff_pairs(&batch, Some(5), None);
        assert_eq!(
            items,
            vec![("k2".into(), vec![(5, None, Some("v2".into()))])]
        );
    }

    #[mz_ore::test]
    fn bounds_filter_everything() {
        let batch = batch_from_tuples(vec![(("k1".into(), "v1".into()), 5, Diff::ONE)], 6);
        let items = collect_bounded_diff_pairs(&batch, Some(6), None);
        assert_eq!(items, vec![]);
    }

    thread_local! {
        static TRACKED_LIVE: Cell<usize> = const { Cell::new(0) };
        static TRACKED_PEAK: Cell<usize> = const { Cell::new(0) };
    }

    /// Value type that records the peak number of simultaneously live
    /// instances on this thread, to observe cloning behavior.
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Tracked(String);

    impl Tracked {
        fn new(s: &str) -> Self {
            Self::record_live();
            Tracked(s.into())
        }

        fn record_live() {
            let live = TRACKED_LIVE.with(|l| {
                l.set(l.get() + 1);
                l.get()
            });
            TRACKED_PEAK.with(|p| p.set(p.get().max(live)));
        }

        /// Resets the peak to the current live count and returns that count.
        fn reset_peak() -> usize {
            let live = TRACKED_LIVE.with(Cell::get);
            TRACKED_PEAK.with(|p| p.set(live));
            live
        }

        fn peak() -> usize {
            TRACKED_PEAK.with(Cell::get)
        }
    }

    impl Clone for Tracked {
        fn clone(&self) -> Self {
            Self::record_live();
            Tracked(self.0.clone())
        }
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            TRACKED_LIVE.with(|l| l.set(l.get() - 1));
        }
    }

    #[mz_ore::test]
    fn fan_out_clones_lazily() {
        // A hot key: one consolidated update whose diff fans out into many
        // pairs. Consuming the pairs one at a time must not materialize the
        // fan-out; only the buffered update and the pair in flight may hold a
        // clone of the value. An implementation that collects the fan-out
        // (e.g. into a per-key Vec) peaks at FAN_OUT live clones instead and
        // regresses sink memory in proportion to the hottest key.
        const FAN_OUT: i64 = 1000;
        let batch = batch_from_tuples_with(
            vec![(("k1".into(), Tracked::new("v1")), 5, Diff::from(FAN_OUT))],
            6,
        );

        let baseline = Tracked::reset_peak();
        let mut pair_count = 0i64;
        for (_key, pairs) in iter_diff_pairs(&batch, None, None) {
            for (_time, pair) in pairs {
                pair_count += 1;
                drop(pair);
            }
        }
        assert_eq!(pair_count, FAN_OUT);

        let peak_extra = Tracked::peak() - baseline;
        assert!(
            peak_extra <= 3,
            "walking the fan-out held {peak_extra} extra live values at peak, \
             expected O(1) rather than O(FAN_OUT)"
        );
    }
}
