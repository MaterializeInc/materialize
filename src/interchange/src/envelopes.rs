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
use differential_dataflow::trace::{BatchReader, Cursor};
use itertools::{Either, EitherOrBoth};
use maplit::btreemap;
use mz_ore::cast::CastFrom;
use mz_repr::{
    CatalogItemId, ColumnName, Datum, Diff, Row, RowPacker, SqlColumnType, SqlScalarType,
};

use crate::avro::DiffPair;

/// Walks `batch` and invokes `on_diff_pair` for each `DiffPair` at each
/// `(key, timestamp)`.
///
/// Within a key, diffs are partitioned by sign into retractions (befores) and
/// insertions (afters), sorted by timestamp, and zipped into `DiffPair`s via a
/// merge-join. Pairs are emitted in ascending timestamp order for a given key;
/// no ordering is guaranteed across keys. Callers are responsible for tracking
/// `(key, timestamp)` boundaries themselves if they need to detect groups
/// with more than one pair (e.g., for primary-key violation checks).
pub fn for_each_diff_pair<B, F>(batch: &B, mut on_diff_pair: F)
where
    B: BatchReader<Diff = Diff>,
    B::Time: Copy,
    B::ValOwn: 'static,
    F: FnMut(&<B::KeyContainer as BatchContainer>::Owned, B::Time, DiffPair<B::ValOwn>),
{
    let mut befores: Vec<(B::Time, B::ValOwn, usize)> = vec![];
    let mut afters: Vec<(B::Time, B::ValOwn, usize)> = vec![];

    let mut cursor = batch.cursor();
    while cursor.key_valid(batch) {
        let k = cursor.key(batch);

        // Partition updates at this key into retractions (befores) and
        // insertions (afters).
        while cursor.val_valid(batch) {
            let v = cursor.val(batch);
            cursor.map_times(batch, |t, diff| {
                let diff = B::owned_diff(diff);
                let update = (
                    B::owned_time(t),
                    B::owned_val(v),
                    usize::cast_from(diff.unsigned_abs()),
                );
                if diff < Diff::ZERO {
                    befores.push(update);
                } else {
                    afters.push(update);
                }
            });
            cursor.step_val(batch);
        }

        befores.sort_by_key(|(t, _v, _diff)| *t);
        afters.sort_by_key(|(t, _v, _diff)| *t);

        // Fan `(time, val, count)` out to `count` copies of `(time, val)`.
        // `iter::repeat(x).take(n)` clones on every `next()` — including the
        // first — so using it with `count == 1` (the snapshot common case)
        // does a gratuitous clone. `iter::once` moves the value; we only
        // fall back to `iter::repeat` when fan-out is actually needed.
        let fan_out = |(t, v, cnt): (B::Time, B::ValOwn, usize)| {
            if cnt == 1 {
                Either::Left(iter::once((t, v)))
            } else {
                Either::Right(iter::repeat((t, v)).take(cnt))
            }
        };
        let befores_iter = befores.drain(..).flat_map(fan_out);
        let afters_iter = afters.drain(..).flat_map(fan_out);

        let key_owned = <B::KeyContainer as BatchContainer>::into_owned(k);

        for pair in
            itertools::merge_join_by(befores_iter, afters_iter, |(t1, _v1), (t2, _v2)| t1.cmp(t2))
        {
            let (t, before, after) = match pair {
                EitherOrBoth::Both((t, before), (_t, after)) => (t, Some(before), Some(after)),
                EitherOrBoth::Left((t, before)) => (t, Some(before), None),
                EitherOrBoth::Right((t, after)) => (t, None, Some(after)),
            };
            on_diff_pair(&key_owned, t, DiffPair { before, after });
        }

        cursor.step_key(batch);
    }
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
    use differential_dataflow::trace::Batcher;
    use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder};
    use timely::progress::Antichain;

    use super::*;

    /// Seals a single batch from an unordered list of `((key, val), time, diff)`
    /// tuples upper-bounded at `upper`.
    fn batch_from_tuples(
        mut tuples: Vec<((String, String), u64, Diff)>,
        upper: u64,
    ) -> <ValBuilder<String, String, u64, Diff> as differential_dataflow::trace::Builder>::Output
    {
        let mut batcher = ValBatcher::<String, String, u64, Diff>::new(None, 0);
        batcher.push_container(&mut tuples);
        batcher.seal::<ValBuilder<String, String, u64, Diff>>(Antichain::from_elem(upper))
    }

    /// Collects `for_each_diff_pair` invocations into a flat, deterministically
    /// sorted list for easy assertion.
    fn collect_diff_pairs<B>(batch: &B) -> Vec<(String, u64, Option<String>, Option<String>)>
    where
        B: BatchReader<Diff = Diff>,
        B::Time: Copy + Into<u64>,
        B::ValOwn: 'static + Into<String>,
        <B::KeyContainer as BatchContainer>::Owned: Into<String> + Clone,
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
}
