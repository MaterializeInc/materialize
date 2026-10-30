// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Draining sealed stash chunks against the persist feedback, one function per
//! stash flavor.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::trace::{Cursor, TraceReader};
use mz_repr::{Diff, GlobalId};
use mz_row_spine::FundedValRowSpine;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::body::ColumnBody;
use mz_timely_util::columnar::unload::UnloadBatch;
use timely::dataflow::operators::Capability;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::{Antichain, Timestamp};

use super::UpsertOutputHandle;
use super::feedback::{FeedbackSpine, FeedbackUpdate, decode_upsert_value, upsert_value_byte_len};
use super::stash::{TimeMajorUpdate, UpsertDiff, UpsertUpdate};
use crate::upsert::{UpsertKey, UpsertValue};

/// Where a stashed entry's time falls relative to the feedback frontier.
enum TimeClass {
    /// `ts < persist_upper`: already persisted, dropped by the drain.
    AlreadyPersisted,
    /// `ts == persist_upper`: processed now.
    Eligible,
    /// `ts > persist_upper`: re-stashed until persist catches up.
    Ineligible,
}

/// Classify `ts` against `persist_upper`: the single spelling of the drain's
/// eligibility test. The chunked drain's probe-collection pass and its
/// classification pass, and the paged drain's cursor walk, all call this, so
/// the probe set and the classification cannot disagree. Under the
/// operator's total order, `Eligible` means `ts` equals the frontier's one
/// element.
fn classify_time<T: PartialOrder>(persist_upper: &Antichain<T>, ts: &T) -> TimeClass {
    if !persist_upper.less_equal(ts) {
        TimeClass::AlreadyPersisted
    } else if persist_upper.less_than(ts) {
        TimeClass::Ineligible
    } else {
        TimeClass::Eligible
    }
}

/// Counts from a single call to [`drain_sealed_input_chunked`] or
/// [`drain_sealed_input_paged`], used to update metrics.
pub(super) struct DrainStats {
    /// Number of eligible entries probed against the feedback trace.
    pub(super) eligible: u64,
    /// Number of probed entries for which a prior value was found.
    pub(super) result_count: u64,
    /// New value written with no prior value (insert).
    pub(super) inserts: u64,
    /// New value written over an existing value (update).
    pub(super) updates: u64,
    /// Tombstone (None) applied to an existing value (delete).
    pub(super) deletes: u64,
    /// Total output records emitted (retractions + insertions).
    pub(super) output_count: u64,
}

/// Process sealed chunks from the batcher, classifying each entry by its
/// timestamp relative to `persist_upper`:
///
///   * `ts == persist_upper`: eligible for processing now (bulk probe of the
///     feedback trace + output).
///   * `ts >  persist_upper`: not yet processable. Returned in `ineligible`
///     for re-stashing until the feedback frontier catches up to it.
///   * `ts <  persist_upper`: already persisted by some writer and not
///     relevant anymore, so DROPPED. The downstream persist_sink would filter
///     such updates out anyway since the shard upper is further ahead, and
///     our state is already up-to-date to `persist_upper` so we could not
///     emit correct retractions for it. Re-stashing it would strand the data
///     forever (`persist_upper` only advances, so `ts == persist_upper` can
///     never again hold) and pin the operator's output frontier below the
///     shard upper. This mirrors v1's `relevant = persist_upper.less_equal(ts)`.
///
/// The sealed chunks are already sorted and consolidated by the merge
/// batcher, so each chunk's eligible keys form a sorted, deduplicated probe
/// set, and the prior state comes back through one bulk `extract_into` pass
/// per trace batch. Resident fence metadata selects the touched trace
/// chunks, and only those bodies are read back, copy-out, per chunk probed.
/// Sealed chunks are pulled from the iterator one at a time and dropped
/// before the next is requested, so at most one loaded stash chunk (plus the
/// probe hits for its keys) is resident regardless of drain size. Only the
/// re-stashed ineligible set is materialized.
pub(super) async fn drain_sealed_input_chunked<T, O>(
    sealed: impl Iterator<Item = ColumnBody<TimeMajorUpdate<T, O>>>,
    ineligible: &mut Vec<UpsertUpdate<T, O>>,
    output_handle: &UpsertOutputHandle<T>,
    output_cap: &Capability<T>,
    persist_upper: &Antichain<T>,
    trace: &mut TraceAgent<FeedbackSpine<T>>,
    worker_id: usize,
    source_id: GlobalId,
) -> DrainStats
where
    T: Timestamp + TotalOrder + Lattice + Sync,
    T: columnation::Columnation + columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: columnar::Columnar,
{
    let mut eligible_count: u64 = 0;
    let mut result_count: u64 = 0;
    let mut output_count: u64 = 0;
    let mut inserts: u64 = 0;
    let mut updates: u64 = 0;
    let mut deletes: u64 = 0;

    // The batches this drain reads against. `Rc` clones of the trace's
    // sealed chunk batches: chunk bodies stay spilled and are read back
    // copy-out, per probed chunk, inside `extract_into`.
    let batches = trace
        .batches_through(Antichain::new().borrow())
        .expect("complete batch set for the feedback trace; is it closed?");

    // Eligible keys are probed against the trace in windows of this many
    // distinct keys, bounding what one pass stages resident: probe hits carry
    // full values, so an unwindowed pass over a byte-graded chunk of small
    // records could stage tens of thousands of values at once.
    const PROBE_WINDOW: usize = 1024;

    for chunk in sealed {
        use columnar::{Index, Len};
        let view = chunk.borrow();
        let total = view.len();
        let mut start = 0;
        while start < total {
            // Pass 1: this window's sorted, deduplicated probe keys. The
            // chunk is sorted by (time, key) and eligible entries share one
            // time, so they are contiguous and key-sorted: a window is a
            // contiguous record range, probes come out sorted, and dedup is
            // a neighbor test. The window closes where its PROBE_WINDOW + 1st
            // distinct eligible key would begin.
            let mut probe_col = <UpsertKey as columnar::Columnar>::Container::default();
            let mut probe_count = 0usize;
            let mut end = total;
            {
                use columnar::Push;
                let mut last_probe: Option<&UpsertKey> = None;
                for index in start..total {
                    let ((_, key), ts, _diff) = view.get(index);
                    let ts = <T as columnar::Columnar>::into_owned(ts);
                    if matches!(classify_time(persist_upper, &ts), TimeClass::Eligible) {
                        if last_probe != Some(key) {
                            if probe_count == PROBE_WINDOW {
                                end = index;
                                break;
                            }
                            probe_col.push(key);
                            probe_count += 1;
                            last_probe = Some(key);
                        }
                    }
                }
            }

            // Pass 2: bulk-probe the trace's batches through the chunk
            // batches' `UnloadChunk` surface and consolidate the hits into
            // the prior value per key. Hits arrive per batch, so equal
            // `(key, val)` pairs from different batches are non-adjacent.
            // Sort before folding.
            let mut old_values: std::collections::BTreeMap<UpsertKey, UpsertValue> =
                std::collections::BTreeMap::new();
            if probe_count > 0 {
                use columnar::Borrow;
                let mut staging = <FeedbackUpdate<T> as columnar::Columnar>::Container::default();
                for batch in &batches {
                    batch.extract_into(probe_col.borrow(), &mut staging);
                }
                let staged = staging.borrow();
                let mut hits: Vec<_> = (0..staged.len())
                    .map(|i| {
                        let ((key, val), _time, diff) = staged.get(i);
                        (key, val, <Diff as columnar::Columnar>::into_owned(diff))
                    })
                    .collect();
                hits.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
                let mut i = 0;
                while i < hits.len() {
                    let (key, val, _) = hits[i];
                    let mut count = Diff::ZERO;
                    let mut j = i;
                    while j < hits.len() && hits[j].0 == key && hits[j].1 == val {
                        count += hits[j].2;
                        j += 1;
                    }
                    if count.is_positive() {
                        assert!(
                            count == 1.into(),
                            "unexpected multiple entries for the same key in persist trace"
                        );
                        let prev = old_values.insert(*key, decode_upsert_value(val.iter()));
                        assert!(
                            prev.is_none(),
                            "unexpected multiple values for the same key in persist trace"
                        );
                    }
                    i = j;
                }
            }

            // Pass 3: classify and emit this window's records.
            for index in start..end {
                let ((_, key), ts, diff) = view.get(index);
                let ts = <T as columnar::Columnar>::into_owned(ts);
                match classify_time(persist_upper, &ts) {
                    TimeClass::AlreadyPersisted => continue,
                    TimeClass::Ineligible => {
                        // Re-stash for later (owned).
                        ineligible.push((
                            *key,
                            ts,
                            <UpsertDiff<O> as columnar::Columnar>::into_owned(diff),
                        ));
                        continue;
                    }
                    TimeClass::Eligible => {}
                }

                // ts == persist_upper: eligible. The chunk holds one entry per
                // (key, time) and eligibility pins the time, so this key appears
                // at most once and its prior value can move out of the map.
                eligible_count += 1;
                let old_value = old_values.remove(key);

                if old_value.is_some() {
                    result_count += 1;
                }

                match diff.value {
                    Some(row) => {
                        if let Some(old_val) = old_value {
                            let size = upsert_value_byte_len(&old_val);
                            output_handle
                                .give_fueled(
                                    output_cap,
                                    (old_val, ts.clone(), Diff::MINUS_ONE),
                                    size,
                                )
                                .await;
                            output_count += 1;
                            updates += 1;
                        } else {
                            inserts += 1;
                        }
                        let new_val = decode_upsert_value(row.iter());
                        let size = upsert_value_byte_len(&new_val);
                        output_handle
                            .give_fueled(output_cap, (new_val, ts, Diff::ONE), size)
                            .await;
                        output_count += 1;
                    }
                    None => {
                        if let Some(old_val) = old_value {
                            let size = upsert_value_byte_len(&old_val);
                            output_handle
                                .give_fueled(output_cap, (old_val, ts, Diff::MINUS_ONE), size)
                                .await;
                            output_count += 1;
                            deletes += 1;
                        }
                    }
                }
            }
            start = end;
        }
    }

    tracing::debug!(
        worker_id = %worker_id,
        source_id = %source_id,
        ineligible = ineligible.len(),
        eligible = eligible_count,
        "drained stash",
    );

    DrainStats {
        eligible: eligible_count,
        result_count,
        inserts,
        updates,
        deletes,
        output_count,
    }
}

/// [`drain_sealed_input_chunked`]'s counterpart for the paged flavor,
/// classifying entries the same way but reading prior state through a trace
/// cursor.
///
/// The sealed chunks are already sorted and consolidated by the merge
/// batcher, so the trace cursor walks forward through keys in order and
/// seeks amortize. Entries are walked by reference rather than collecting
/// the eligible set into an owned Vec, and eligible values are emitted
/// straight from the column's `RowRef` with no owned `UpsertDiff` copy. Only
/// the re-stashed ineligible set is materialized.
pub(super) async fn drain_sealed_input_paged<T, O>(
    sealed: Vec<Column<UpsertUpdate<T, O>>>,
    ineligible: &mut Vec<UpsertUpdate<T, O>>,
    output_handle: &UpsertOutputHandle<T>,
    output_cap: &Capability<T>,
    persist_upper: &Antichain<T>,
    trace: &mut TraceAgent<FundedValRowSpine<UpsertKey, T, Diff>>,
    worker_id: usize,
    source_id: GlobalId,
) -> DrainStats
where
    T: Timestamp + TotalOrder + Lattice + Sync,
    T: columnation::Columnation + columnar::Columnar,
    O: columnar::Columnar,
{
    use columnar::Index as _;

    // Classify each entry by its timestamp relative to `persist_upper`:
    //
    //   * `ts == persist_upper`: eligible for processing now.
    //   * `ts >  persist_upper`: not yet processable; re-stashed (ineligible)
    //     until the feedback frontier catches up to it.
    //   * `ts <  persist_upper`: already persisted by some writer and not
    //     relevant anymore. We DROP it. The downstream persist_sink would
    //     filter such updates out anyway since the shard upper is further
    //     ahead, and our state is already up-to-date to `persist_upper` so we
    //     could not emit correct retractions for it. Re-stashing it would
    //     strand the data forever (`persist_upper` only advances, so
    //     `ts == persist_upper` can never again hold) and pin the operator's
    //     output frontier below the shard upper. This mirrors v1's
    //     `relevant = persist_upper.less_equal(ts)`.
    let mut eligible_count: u64 = 0;
    let mut result_count: u64 = 0;
    let mut output_count: u64 = 0;
    let mut inserts: u64 = 0;
    let mut updates: u64 = 0;
    let mut deletes: u64 = 0;

    let (mut cursor, storage) = trace.cursor();

    for chunk in &sealed {
        for (key, ts, diff) in chunk.borrow().into_index_iter() {
            let ts = <T as columnar::Columnar>::into_owned(ts);
            match classify_time(persist_upper, &ts) {
                TimeClass::AlreadyPersisted => continue,
                TimeClass::Ineligible => {
                    // Re-stash for later (owned).
                    ineligible.push((
                        *key,
                        ts,
                        <UpsertDiff<O> as columnar::Columnar>::into_owned(diff),
                    ));
                    continue;
                }
                TimeClass::Eligible => {}
            }

            // ts == persist_upper: eligible. Look up the prior value for this
            // key in the persist trace and emit the retraction / insertion. The
            // spine stores keys in a columnation arena, so we seek by the
            // column's borrowed `&UpsertKey` directly.
            eligible_count += 1;
            cursor.seek_key(&storage, key);
            let old_value = match cursor.get_key(&storage) {
                Some(found) if found == key => {
                    let mut result = None;
                    while let Some(val) = cursor.get_val(&storage) {
                        let mut count = Diff::ZERO;
                        cursor.map_times(&storage, |_time, d| {
                            count += d.clone();
                        });
                        if count.is_positive() {
                            assert!(
                                count == 1.into(),
                                "unexpected multiple entries for the same key in persist trace"
                            );
                            assert!(
                                result.is_none(),
                                "unexpected multiple values for the same key in persist trace"
                            );
                            result = Some(decode_upsert_value(val));
                        }
                        cursor.step_val(&storage);
                    }
                    result
                }
                _ => None,
            };

            if old_value.is_some() {
                result_count += 1;
            }

            match diff.value {
                Some(row) => {
                    if let Some(old_val) = old_value {
                        let size = upsert_value_byte_len(&old_val);
                        output_handle
                            .give_fueled(output_cap, (old_val, ts.clone(), Diff::MINUS_ONE), size)
                            .await;
                        output_count += 1;
                        updates += 1;
                    } else {
                        inserts += 1;
                    }
                    let new_val = decode_upsert_value(row.iter());
                    let size = upsert_value_byte_len(&new_val);
                    output_handle
                        .give_fueled(output_cap, (new_val, ts, Diff::ONE), size)
                        .await;
                    output_count += 1;
                }
                None => {
                    if let Some(old_val) = old_value {
                        let size = upsert_value_byte_len(&old_val);
                        output_handle
                            .give_fueled(output_cap, (old_val, ts, Diff::MINUS_ONE), size)
                            .await;
                        output_count += 1;
                        deletes += 1;
                    }
                }
            }
        }
    }

    tracing::debug!(
        worker_id = %worker_id,
        source_id = %source_id,
        ineligible = ineligible.len(),
        eligible = eligible_count,
        "drained stash",
    );

    DrainStats {
        eligible: eligible_count,
        result_count,
        inserts,
        updates,
        deletes,
        output_count,
    }
}
