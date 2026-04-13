// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of the feedback UPSERT operator.
//!
//! # Architecture
//!
//! The operator converts a stream of upsert commands `(key, Option<value>)` into
//! a differential collection of `(key, value)` pairs, using a feedback loop
//! through persist to maintain the "previous value" state needed for computing
//! retractions.
//!
//! ## Dataflow topology
//!
//! ```text
//!   Source input ──► ┌──────────┐ ──► Output ──► Persist
//!                    │  Upsert  │
//!   Persist read ──► └──────────┘
//!       ▲                                           │
//!       └───────────── feedback ────────────────────┘
//! ```
//!
//! ## Operator loop (each iteration)
//!
//! 1. **Ingest source data.** Read upsert commands from the source input,
//!    wrap each in an [`UpsertDiff`] (carrying `from_time` for dedup), and
//!    push into the [`MergeBatcher`]. The batcher consolidates entries for the
//!    same `(key, time)` using the `UpsertDiff` Semigroup, which keeps the
//!    update with the highest `FromTime` (latest Kafka offset). This happens
//!    via amortized geometric merging as data is pushed in, bounding memory
//!    to O(unique key-time pairs) even during large Kafka snapshots.
//!
//! 2. **Read persist frontier.** Check the probe on the persist arrangement
//!    to learn which times have been committed. When the persist frontier
//!    reaches the resume upper, rehydration is complete.
//!
//! 3. **Seal & drain.** Call `batcher.seal(input_upper)` to extract all
//!    source-finalized entries as sorted, consolidated chunks. Each entry is
//!    classified:
//!    - **Eligible** (at the persist frontier): the persist trace has the
//!      correct "before" state for this time. Look up the old value via a
//!      cursor, emit a retraction if present, and emit the new value.
//!    - **Ineligible** (between persist and input frontiers): persist hasn't
//!      caught up yet. Push back into the batcher for the next iteration.
//!
//! 4. **Capability management.** Downgrade the output capability to the
//!    minimum time of any remaining buffered data (in the batcher or pushed
//!    back as ineligible). Drop the capability entirely when the batcher is
//!    empty.
//!
//! ## Eligibility condition (total order)
//!
//! For a total-order timestamp with `input_upper = {i}` and
//! `persist_upper = {p}`, an entry at time `ts` is eligible when
//! `ts == p < i` — the source has finalized it and persist is exactly at
//! that time, so the trace cursor returns the correct prior state.

use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::implementations::merge_batcher::{
    MergeBatcher, container::VecMerger,
};
use differential_dataflow::trace::{Batcher, Builder, Cursor, Description, TraceReader};
use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Diff, GlobalId, Row};
use mz_storage_types::errors::{DataflowError, EnvelopeError};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use std::convert::Infallible;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertValue;

// ── Source stash diff type ───────────────────────────────────────────────────
// The source stash uses a custom diff type that carries the upsert payload.
// The Semigroup implementation does "max FromTime wins" — when two updates for
// the same (key, time) are consolidated, the one with the higher FromTime
// (latest Kafka offset) is kept.

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct UpsertDiff<O> {
    from_time: O,
    value: Option<UpsertValue>,
}

impl<O> IsZero for UpsertDiff<O> {
    fn is_zero(&self) -> bool {
        false
    }
}

impl<O: Ord + Clone> Semigroup for UpsertDiff<O> {
    fn plus_equals(&mut self, rhs: &Self) {
        if rhs.from_time > self.from_time {
            *self = rhs.clone();
        }
    }
}

// ── MergeBatcher type alias ──────────────────────────────────────────────────
// The source stash uses DD's MergeBatcher for amortized consolidation.
// Data is pushed in unsorted; the batcher maintains geometrically-sized sorted
// chains and consolidates via the UpsertDiff Semigroup automatically.

type UpsertBatcher<T, FromTime> = MergeBatcher<
    Vec<(UpsertKey, T, UpsertDiff<FromTime>)>,
    ContainerChunker<Vec<(UpsertKey, T, UpsertDiff<FromTime>)>>,
    VecMerger<UpsertKey, T, UpsertDiff<FromTime>>,
>;

/// A minimal [`Builder`] that captures sealed chains without copying.
///
/// Used with [`MergeBatcher::seal`] to extract sorted, consolidated chunks
/// directly as `Vec<Vec<...>>`.
struct CapturingBuilder<D, T>(D, PhantomData<T>);

impl<D, T: Timestamp> Builder for CapturingBuilder<D, T> {
    type Input = D;
    type Time = T;
    type Output = Vec<D>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        unimplemented!()
    }

    fn push(&mut self, _chunk: &mut Self::Input) {
        unimplemented!()
    }

    fn done(self, _description: Description<Self::Time>) -> Self::Output {
        unimplemented!()
    }

    #[inline]
    fn seal(chain: &mut Vec<Self::Input>, _description: Description<Self::Time>) -> Self::Output {
        std::mem::take(chain)
    }
}

/// Transforms a stream of upserts (key-value updates) into a differential
/// collection.
///
/// Persist feedback is arranged into a differential trace (DD manages the
/// spine lifecycle). Source input is stashed with a custom `UpsertDiff`
/// Semigroup that deduplicates by keeping the highest FromTime per (key, time).
///
/// Has two inputs:
///   1. **Source input** — upsert commands from the external source.
///   2. **Persist input** — feedback of the operator's own output, read back
///      from persist.  Arranged into a trace for cursor-based lookups.
pub fn upsert_inner<'scope, T, FromTime>(
    input: VecCollection<'scope, T, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<T>,
    persist_input: VecCollection<'scope, T, Result<Row, DataflowError>, Diff>,
    persist_token: Option<Vec<PressOnDropButton>>,
    upsert_metrics: UpsertMetrics,
    source_config: crate::source::SourceExportCreationConfig,
) -> (
    VecCollection<'scope, T, Result<Row, DataflowError>, Diff>,
    StreamVec<'scope, T, (Option<GlobalId>, HealthStatusUpdate)>,
    StreamVec<'scope, T, Infallible>,
    PressOnDropButton,
)
where
    T: Timestamp + TotalOrder + Sync,
    T: Refines<mz_repr::Timestamp> + TotalOrder + differential_dataflow::lattice::Lattice + Sync,
    FromTime: Debug + timely::ExchangeData + Clone + Ord + Sync,
{
    // ── Arrange persist feedback ────────────────────────────────────────
    // Extract (UpsertKey, UpsertValue) from the persist feedback collection
    // and arrange it. DD manages the spine, batching, and compaction.
    let persist_keyed = persist_input.flat_map(move |result| {
        let value = match result {
            Ok(ok) => Ok(ok),
            Err(DataflowError::EnvelopeError(err)) => match *err {
                EnvelopeError::Upsert(err) => Err(Box::new(err)),
                EnvelopeError::Flat(_) => return None,
            },
            Err(_) => return None,
        };
        let value_ref = match value {
            Ok(ref row) => Ok(row),
            Err(ref err) => Err(&**err),
        };
        Some((UpsertKey::from_value(value_ref, &key_indices), value))
    });
    let persist_arranged = persist_keyed.arrange_by_key();
    let mut persist_trace = persist_arranged.trace.clone();

    // Probe the persist arrangement's stream for frontier tracking.
    // This replaces receiving the batch stream as an input — we just
    // read the probe frontier to know when persist has caught up.
    use timely::dataflow::operators::Probe;
    let (persist_probe, _persist_probe_stream) = persist_arranged.stream.probe();

    // ── Build the async processing operator ─────────────────────────────
    let mut builder = AsyncOperatorBuilder::new("Upsert".to_string(), input.scope());

    let (output_handle, output) = builder.new_output::<CapacityContainerBuilder<_>>();
    let (_snapshot_handle, snapshot_stream) =
        builder.new_output::<CapacityContainerBuilder<Vec<Infallible>>>();
    let (_health_output, health_stream) = builder
        .new_output::<CapacityContainerBuilder<Vec<(Option<GlobalId>, HealthStatusUpdate)>>>();

    let mut input = builder.new_input_for(
        input.inner,
        Exchange::new(move |((key, _, _), _, _)| UpsertKey::hashed(key)),
        &output_handle,
    );

    // We still need the persist stream as an input so the operator wakes
    // when the persist arrangement produces batches (frontier advances).
    // We read the actual frontier from the probe though.
    let mut persist_wakeup = builder.new_disconnected_input(_persist_probe_stream, Pipeline);

    let upsert_shared_metrics = Arc::clone(&upsert_metrics.shared);
    let _ = upsert_shared_metrics;

    let shutdown_button = builder.build(move |caps| async move {
        let _persist_token = persist_token;

        let [output_cap, snapshot_cap, _health_cap]: [_; 3] = caps.try_into().unwrap();
        drop(output_cap);
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        let mut hydrating = true;

        // Source stash backed by DD's MergeBatcher. The batcher maintains
        // geometrically-sized sorted chains and consolidates via the
        // UpsertDiff Semigroup as data is pushed in, bounding memory to
        // O(unique key-time pairs) even during large initial snapshots.
        let mut batcher: UpsertBatcher<T, FromTime> = Batcher::new(None, 0);
        // Scratch buffer for accumulating source events before flushing to
        // the batcher. Drained on each iteration via `push_container`.
        let mut push_buffer: Vec<(UpsertKey, T, UpsertDiff<FromTime>)> = Vec::new();
        // Capability held at the minimum time of any buffered data. When
        // Some, the operator may still produce output; when None, the
        // batcher is empty.
        let mut stash_cap: Option<Capability<T>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        let mut output_updates = vec![];
        let snapshot_start = std::time::Instant::now();
        let mut prev_persist_upper = Antichain::from_elem(Timestamp::minimum());

        // Accumulators for rehydration metrics, set as gauges when rehydration completes.
        let mut rehydration_total: u64 = 0;
        let mut rehydration_updates: u64 = 0;

        // ──────────────────────────────────────────────────────────────────
        // Main operator loop. Each iteration performs four steps:
        //   Step 1: Ingest source data into the batcher.
        //   Step 2: Read the persist frontier and update rehydration state.
        //   Step 3: Seal the batcher, drain eligible entries, push back the rest.
        //   Step 4: Manage the output capability.
        // ──────────────────────────────────────────────────────────────────
        loop {
            // Block until woken by source input or a persist frontier advance.
            tokio::select! {
                _ = input.ready() => {}
                _ = persist_wakeup.ready() => {
                    while persist_wakeup.next_sync().is_some() {}
                }
            }

            // ── Step 1: Ingest source data ────────────────────────────────
            // Read all available source events, wrap each value in an
            // UpsertDiff (carrying FromTime for dedup), and buffer them.
            // Events before the resume_upper are dropped (already persisted).
            while let Some(event) = input.next_sync() {
                match event {
                    AsyncEvent::Data(cap, data) => {
                        let mut pushed_any = false;
                        for ((key, value, from_time), ts, diff) in data {
                            assert!(diff.is_positive(), "invalid upsert input");
                            if PartialOrder::less_equal(&input_upper, &resume_upper)
                                && !resume_upper.less_equal(&ts)
                            {
                                continue;
                            }
                            push_buffer.push((key, ts, UpsertDiff { from_time, value }));
                            pushed_any = true;
                        }
                        // Track the minimum capability across all buffered data
                        // so we can emit output at the correct times.
                        if pushed_any {
                            stash_cap = Some(match stash_cap {
                                Some(prev) if cap.time() < prev.time() => cap,
                                Some(prev) => prev,
                                None => cap,
                            });
                        }
                    }
                    AsyncEvent::Progress(upper) => {
                        if PartialOrder::less_than(&upper, &resume_upper) {
                            continue;
                        }
                        input_upper = upper;
                    }
                }
            }

            // Flush buffered events into the batcher. This triggers the
            // chunker + geometric chain merging, which consolidates entries
            // for the same (key, time) via the UpsertDiff Semigroup.
            if !push_buffer.is_empty() {
                batcher.push_container(&mut push_buffer);
            }

            // ── Step 2: Read persist frontier ─────────────────────────────
            // The persist probe tells us which output times have been
            // committed back through the feedback loop. This determines:
            //   - Whether rehydration is complete (persist >= resume_upper).
            //   - Which source entries are eligible for processing (their
            //     time must equal persist_upper so the trace cursor returns
            //     the correct prior state).
            //   - How far to compact the persist trace.
            let persist_upper = persist_probe.with_frontier(|f| f.to_owned());

            if persist_upper != prev_persist_upper {
                let last_rehydration_chunk =
                    hydrating && PartialOrder::less_equal(&resume_upper, &persist_upper);

                if last_rehydration_chunk {
                    hydrating = false;
                    upsert_metrics
                        .rehydration_latency
                        .set(snapshot_start.elapsed().as_secs_f64());
                    upsert_metrics
                        .rehydration_total
                        .set(rehydration_total);
                    upsert_metrics
                        .rehydration_updates
                        .set(rehydration_updates);
                    tracing::info!(
                        worker_id = %source_config.worker_id,
                        source_id = %source_config.id,
                        "upsert finished rehydration",
                    );
                    snapshot_cap.downgrade(&[]);
                }

                let _ = snapshot_cap.try_downgrade(persist_upper.iter());

                // Compact the trace so the spine can merge old batches.
                persist_trace.set_logical_compaction(persist_upper.borrow());
                persist_trace.set_physical_compaction(persist_upper.borrow());

                prev_persist_upper = persist_upper.clone();
            }

            // ── Step 3: Seal & drain ──────────────────────────────────────
            // Seal the batcher at input_upper to extract all source-finalized
            // entries as sorted, consolidated chunks. The seal merges all
            // internal chains (O(N) linear merge of sorted data) and splits
            // by time: entries at ts < input_upper are extracted, the rest
            // stay in the batcher.
            //
            // Extracted entries are partitioned into:
            //   - Eligible (ts == persist_upper): processed now via cursor
            //     lookup on the persist trace.
            //   - Ineligible (persist_upper < ts < input_upper): persist
            //     hasn't caught up yet; pushed back into the batcher.
            if stash_cap.is_some() {
                let cap = stash_cap.as_mut().unwrap();

                let sealed = batcher.seal::<CapturingBuilder<_, _>>(input_upper.clone());
                // Frontier of data remaining in the batcher (ts >= input_upper).
                let remaining_frontier = batcher.frontier().to_owned();

                let mut ineligible = Vec::new();
                let drain_stats = drain_sealed_input(
                    sealed,
                    &mut ineligible,
                    &mut output_updates,
                    &persist_upper,
                    &mut persist_trace,
                    &source_config,
                );

                upsert_metrics.multi_get_size.inc_by(drain_stats.eligible);
                upsert_metrics
                    .multi_get_result_count
                    .inc_by(drain_stats.result_count);
                upsert_metrics.multi_put_size.inc_by(drain_stats.output_count);
                upsert_metrics.upsert_inserts.inc_by(drain_stats.inserts);
                upsert_metrics.upsert_updates.inc_by(drain_stats.updates);
                upsert_metrics.upsert_deletes.inc_by(drain_stats.deletes);

                source_config
                    .source_statistics
                    .update_bytes_indexed_by(drain_stats.size_diff);
                source_config
                    .source_statistics
                    .update_records_indexed_by(
                        drain_stats.inserts as i64 - drain_stats.deletes as i64,
                    );

                if hydrating {
                    rehydration_total += drain_stats.inserts;
                    rehydration_updates += drain_stats.eligible;
                }

                // Emit output: retractions of old values and insertions of
                // new values, all at the eligible timestamp.
                for (update, ts, diff) in output_updates.drain(..) {
                    output_handle.give(cap, (update, ts, diff));
                }

                // ── Step 4: Capability management ─────────────────────────
                // Downgrade the output capability to the minimum time of any
                // remaining data: either entries still in the batcher (above
                // input_upper) or ineligible entries being pushed back.
                let min_ineligible_ts = ineligible.iter().map(|(_, ts, _)| ts).min().cloned();
                if !ineligible.is_empty() {
                    batcher.push_container(&mut ineligible);
                }

                let has_remaining = !remaining_frontier.is_empty() || min_ineligible_ts.is_some();
                if has_remaining {
                    let min_ts = match (
                        remaining_frontier.elements().first(),
                        min_ineligible_ts.as_ref(),
                    ) {
                        (Some(a), Some(b)) => std::cmp::min(a, b).clone(),
                        (Some(a), None) => a.clone(),
                        (None, Some(b)) => b.clone(),
                        (None, None) => unreachable!(),
                    };
                    cap.downgrade(&min_ts);
                } else {
                    // Batcher is completely empty — drop the capability so
                    // downstream operators can make progress.
                    stash_cap = None;
                }
            }

            if input_upper.is_empty() {
                break;
            }
        }
    });

    (
        output
            .as_collection()
            .map(|result: UpsertValue| match result {
                Ok(ok) => Ok(ok),
                Err(err) => Err(DataflowError::from(EnvelopeError::Upsert(*err))),
            }),
        health_stream,
        snapshot_stream,
        shutdown_button.press_on_drop(),
    )
}

/// Counts from a single call to [`drain_sealed_input`], used to update metrics.
struct DrainStats {
    /// Number of entries looked up in the persist trace (cursor seeks).
    eligible: u64,
    /// Number of cursor lookups that found an existing value.
    result_count: u64,
    /// New value written with no prior value (insert).
    inserts: u64,
    /// New value written over an existing value (update).
    updates: u64,
    /// Tombstone (None) applied to an existing value (delete).
    deletes: u64,
    /// Total output records emitted (retractions + insertions).
    output_count: u64,
    /// Net byte change to indexed state: positive on inserts, negative on deletes.
    size_diff: i64,
}

/// Returns the indexed byte size of a [`UpsertValue`].
fn upsert_value_bytes(val: &UpsertValue) -> i64 {
    match val {
        Ok(row) => i64::try_from(row.byte_len()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

/// Process sealed chunks from the batcher. Entries at the persist frontier are
/// eligible for processing (cursor lookup + output); all others are returned
/// in `ineligible` for re-stashing.
///
/// The sealed chunks are already sorted and consolidated by the MergeBatcher.
fn drain_sealed_input<T, FromTime>(
    sealed: Vec<Vec<(UpsertKey, T, UpsertDiff<FromTime>)>>,
    ineligible: &mut Vec<(UpsertKey, T, UpsertDiff<FromTime>)>,
    output: &mut Vec<(UpsertValue, T, Diff)>,
    persist_upper: &Antichain<T>,
    trace: &mut TraceAgent<ValSpine<UpsertKey, UpsertValue, T, Diff>>,
    source_config: &crate::source::SourceExportCreationConfig,
) -> DrainStats
where
    T: TotalOrder + Lattice + timely::ExchangeData + Timestamp + Clone + Debug + Ord + Sync,
    FromTime: timely::ExchangeData + Clone + Ord + Sync,
{
    // Separate eligible (at persist frontier) from ineligible.
    let mut eligible = Vec::new();
    for chunk in sealed {
        for entry in chunk {
            let (_, ref ts, _) = entry;
            if !persist_upper.less_than(ts) && persist_upper.less_equal(ts) {
                eligible.push(entry);
            } else {
                ineligible.push(entry);
            }
        }
    }

    tracing::debug!(
        worker_id = %source_config.worker_id,
        source_id = %source_config.id,
        ineligible = ineligible.len(),
        eligible = eligible.len(),
        "draining stash",
    );

    let eligible_count = u64::try_from(eligible.len()).expect("eligible count overflows u64");

    if eligible.is_empty() {
        return DrainStats {
            eligible: 0,
            result_count: 0,
            inserts: 0,
            updates: 0,
            deletes: 0,
            output_count: 0,
            size_diff: 0,
        };
    }

    let output_before = output.len();
    let mut result_count: u64 = 0;
    let mut inserts: u64 = 0;
    let mut updates: u64 = 0;
    let mut deletes: u64 = 0;
    let mut size_diff: i64 = 0;

    // Eligible entries are sorted by (key, time) from the batcher.
    // The trace cursor moves forward through keys, matching this order.
    let (mut cursor, storage) = trace.cursor();

    for (key, ts, upsert_diff) in eligible {
        // Look up the current value for this key in the persist trace.
        // For ValSpine with Vector layout, Key<'a> = &'a UpsertKey.
        cursor.seek_key(&storage, &key);
        let old_value = if cursor.get_key(&storage) == Some(&key) {
            let mut result = None;
            while let Some(val) = cursor.get_val(&storage) {
                let mut count = Diff::ZERO;
                cursor.map_times(&storage, |_time, diff| {
                    count += diff.clone();
                });
                if count.is_positive() {
                    assert!(
                        count == 1.into(),
                        "unexpected multiple entries for the same key in persist trace"
                    );
                    result = Some(val.clone());
                }
                cursor.step_val(&storage);
            }
            result
        } else {
            None
        };

        if old_value.is_some() {
            result_count += 1;
        }

        match upsert_diff.value {
            Some(new_val) => {
                size_diff += upsert_value_bytes(&new_val);
                if let Some(old_val) = old_value {
                    size_diff -= upsert_value_bytes(&old_val);
                    output.push((old_val, ts.clone(), Diff::MINUS_ONE));
                    updates += 1;
                } else {
                    inserts += 1;
                }
                output.push((new_val, ts, Diff::ONE));
            }
            None => {
                if let Some(old_val) = old_value {
                    size_diff -= upsert_value_bytes(&old_val);
                    output.push((old_val, ts, Diff::MINUS_ONE));
                    deletes += 1;
                }
            }
        }
    }

    let output_count =
        u64::try_from(output.len() - output_before).expect("output count overflows u64");

    DrainStats {
        eligible: eligible_count,
        result_count,
        inserts,
        updates,
        deletes,
        output_count,
        size_diff,
    }
}

#[cfg(test)]
mod test {
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_types::ShardId;
    use mz_repr::{Datum, Timestamp as MzTimestamp};
    use mz_storage_operators::persist_source::Subtime;
    use mz_storage_types::sources::SourceEnvelope;
    use mz_storage_types::sources::envelope::{KeyEnvelope, UpsertEnvelope, UpsertStyle};
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Input};
    use timely::progress::Timestamp;

    use crate::metrics::StorageMetrics;
    use crate::metrics::upsert::UpsertMetricDefs;
    use crate::source::SourceExportCreationConfig;
    use crate::statistics::{SourceStatistics, SourceStatisticsMetricDefs};

    use super::*;

    type Ts = (MzTimestamp, Subtime);

    fn new_ts(ts: u64) -> Ts {
        (MzTimestamp::new(ts), Subtime::minimum())
    }

    fn key(k: i64) -> UpsertKey {
        UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(k)])))
    }

    fn row(k: i64, v: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(k), Datum::Int64(v)])
    }

    macro_rules! upsert_test {
        (|$input:ident, $persist:ident, $worker:ident| $body:block) => {{
            let output_handle = timely::execute_directly(move |$worker| {
                let (mut $input, mut $persist, output_handle) = $worker
                    .dataflow::<MzTimestamp, _, _>(|scope| {
                        scope.scoped::<Ts, _, _>("upsert", |scope| {
                            let (input_handle, input) = scope.new_input();
                            let (persist_handle, persist_input) = scope.new_input();
                            let source_id = GlobalId::User(0);

                            let reg = MetricsRegistry::new();
                            let upsert_defs = UpsertMetricDefs::register_with(&reg);
                            let upsert_metrics =
                                UpsertMetrics::new(&upsert_defs, source_id, 0, None);

                            let reg2 = MetricsRegistry::new();
                            let storage_metrics = StorageMetrics::register_with(&reg2);

                            let reg3 = MetricsRegistry::new();
                            let stats_defs =
                                SourceStatisticsMetricDefs::register_with(&reg3);
                            let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
                                source_arity: 2,
                                style: UpsertStyle::Default(KeyEnvelope::Flattened),
                                key_indices: vec![0],
                            });
                            let source_statistics = SourceStatistics::new(
                                source_id, 0, &stats_defs, source_id, &ShardId::new(),
                                envelope, Antichain::from_elem(Timestamp::minimum()),
                            );
                            let source_config = SourceExportCreationConfig {
                                id: source_id,
                                worker_id: 0,
                                metrics: storage_metrics,
                                source_statistics,
                            };

                            let (output, _, _, button) = upsert_inner(
                                input.as_collection(),
                                vec![0],
                                Antichain::from_elem(Timestamp::minimum()),
                                persist_input.as_collection(),
                                None,
                                upsert_metrics,
                                source_config,
                            );
                            std::mem::forget(button);
                            (input_handle, persist_handle, output.inner.capture())
                        })
                    });

                $body

                output_handle
            });

            let mut actual: Vec<_> = output_handle
                .extract()
                .into_iter()
                .flat_map(|(_cap, container)| container)
                .collect();
            differential_dataflow::consolidation::consolidate_updates(&mut actual);
            actual
        }};
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn gh_9160_repro() {
        let actual = upsert_test!(|input, persist, worker| {
            let key0 = key(0);
            let key1 = key(1);
            let value1 = row(0, 0);
            let value3 = row(0, 1);
            let value4 = row(0, 2);

            input.send(((key0, Some(Ok(value1.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();

            persist.send((Ok(value1), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send_batch(&mut vec![
                ((key1, None, 2), new_ts(2), Diff::ONE),
                ((key0, Some(Ok(value3)), 3), new_ts(3), Diff::ONE),
            ]);
            input.advance_to(new_ts(3));
            input.send_batch(&mut vec![(
                (key0, Some(Ok(value4)), 4),
                new_ts(3),
                Diff::ONE,
            )]);
            input.advance_to(new_ts(4));
            worker.step();

            persist.advance_to(new_ts(3));
            worker.step();
        });

        let value1 = row(0, 0);
        let value4 = row(0, 2);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(value1.clone()), new_ts(0), Diff::ONE),
            (Ok(value1), new_ts(3), Diff::MINUS_ONE),
            (Ok(value4), new_ts(3), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn out_of_order_keys_across_timestamps() {
        let actual = upsert_test!(|input, persist, worker| {
            let key_high = key(99);
            let key_low = key(1);
            let val_a = row(99, 1);
            let val_b = row(1, 2);

            input.send(((key_high, Some(Ok(val_a.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            persist.send((Ok(val_a.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((key_low, Some(Ok(val_b.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.send((Ok(val_b.clone()), new_ts(1), Diff::ONE));
            persist.advance_to(new_ts(2));
            worker.step();

            let val_a2 = row(99, 10);
            let val_b2 = row(1, 20);
            input.send_batch(&mut vec![
                (
                    (key_high, Some(Ok(val_a2.clone())), 3),
                    new_ts(2),
                    Diff::ONE,
                ),
                ((key_low, Some(Ok(val_b2.clone())), 4), new_ts(2), Diff::ONE),
            ]);
            input.advance_to(new_ts(3));
            worker.step();
            persist.advance_to(new_ts(3));
            worker.step();
        });

        let val_a = row(99, 1);
        let val_b = row(1, 2);
        let val_a2 = row(99, 10);
        let val_b2 = row(1, 20);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val_b.clone()), new_ts(1), Diff::ONE),
            (Ok(val_b), new_ts(2), Diff::MINUS_ONE),
            (Ok(val_b2), new_ts(2), Diff::ONE),
            (Ok(val_a.clone()), new_ts(0), Diff::ONE),
            (Ok(val_a), new_ts(2), Diff::MINUS_ONE),
            (Ok(val_a2), new_ts(2), Diff::ONE),
        ];
        let mut actual_sorted = actual;
        let mut expected_sorted = expected;
        actual_sorted.sort();
        expected_sorted.sort();
        assert_eq!(actual_sorted, expected_sorted);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn rehydration_then_update() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(42);
            let old_val = row(42, 100);
            let new_val = row(42, 200);

            persist.send((Ok(old_val), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, Some(Ok(new_val)), 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        let old_val = row(42, 100);
        let new_val = row(42, 200);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(old_val), new_ts(1), Diff::MINUS_ONE),
            (Ok(new_val), new_ts(1), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn delete_existing_key() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(7);
            let val = row(7, 77);

            input.send(((k, Some(Ok(val.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            persist.send((Ok(val), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        let val = row(7, 77);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val.clone()), new_ts(0), Diff::ONE),
            (Ok(val), new_ts(1), Diff::MINUS_ONE),
        ];
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn multi_batch_rehydration() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(5);
            let old_val = row(5, 10);
            let new_val = row(5, 20);
            let updated_val = row(5, 30);

            persist.send((Ok(old_val.clone()), new_ts(0), Diff::ONE));
            persist.send((Ok(old_val), new_ts(0), Diff::MINUS_ONE));
            persist.send((Ok(new_val), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, Some(Ok(updated_val)), 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        let new_val = row(5, 20);
        let updated_val = row(5, 30);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(new_val), new_ts(1), Diff::MINUS_ONE),
            (Ok(updated_val), new_ts(1), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn delete_nonexistent_key() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(99);

            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 1), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        assert!(actual.is_empty(), "expected empty output, got: {actual:?}");
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn reinsert_after_delete() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(3);
            let val_a = row(3, 10);
            let val_b = row(3, 20);

            input.send(((k, Some(Ok(val_a.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            persist.send((Ok(val_a.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.send((Ok(val_a), new_ts(1), Diff::MINUS_ONE));
            persist.advance_to(new_ts(2));
            worker.step();

            input.send(((k, Some(Ok(val_b.clone())), 3), new_ts(2), Diff::ONE));
            input.advance_to(new_ts(3));
            worker.step();
            persist.advance_to(new_ts(3));
            worker.step();
        });

        let val_a = row(3, 10);
        let val_b = row(3, 20);
        let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(val_a.clone()), new_ts(0), Diff::ONE),
            (Ok(val_a), new_ts(1), Diff::MINUS_ONE),
            (Ok(val_b), new_ts(2), Diff::ONE),
        ];
        expected.sort();
        let mut actual = actual;
        actual.sort();
        assert_eq!(actual, expected);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn idempotent_update() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(11);
            let val = row(11, 50);

            input.send(((k, Some(Ok(val.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            persist.send((Ok(val.clone()), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, Some(Ok(val.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        let val = row(11, 50);
        let expected: Vec<(Result<Row, DataflowError>, _, _)> =
            vec![(Ok(val), new_ts(0), Diff::ONE)];
        assert_eq!(actual, expected);
    }
}
