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
//!    wrap each in an `UpsertDiff` (carrying a columnar order key projected
//!    from `FromTime` via [`UpsertSourceTime`] for dedup), and push into the
//!    source-stash batcher. The batcher is a paged columnar merge batcher: it
//!    consolidates entries for the same `(key, time)` via the `UpsertDiff`
//!    Semigroup — keeping the update with the highest order key (latest source
//!    offset) — through amortized geometric merging as data is pushed in, and
//!    pages cold chains out of RSS through the pager. This bounds resident
//!    memory to O(unique key-time pairs) even during large source snapshots.
//!
//! 2. **Read persist frontier.** Check the probe on the persist arrangement
//!    to learn which times have been committed. When the persist frontier
//!    reaches the resume upper, rehydration is complete.
//!
//! 3. **Seal & drain.** Call `batcher.seal(input_upper)` to extract all
//!    source-finalized entries as sorted, consolidated `Column` chunks. Each
//!    entry is classified:
//!    - **Eligible** (at the persist frontier): the persist trace has the
//!      correct "before" state for this time. Look up the old value via a
//!      cursor, emit a retraction if present, and emit the new value.
//!    - **Ineligible** (between persist and input frontiers): persist hasn't
//!      caught up yet. Push back into the batcher for the next iteration.
//!    - **Already persisted** (below the persist frontier): some writer has
//!      already advanced the shard past this time, so it is dropped. See
//!      `drain_sealed_input` for why re-stashing it would strand the data
//!      and pin the output frontier below the shard upper.
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
//! that time, so the trace cursor returns the correct prior state. An entry
//! with `p < ts` is ineligible (persist hasn't caught up), and one with
//! `ts < p` is already persisted and dropped.

use std::fmt::Debug;

use columnar::Index as _;
use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::{Batcher, Cursor, Description, TraceReader};
use differential_dataflow::{AsCollection, VecCollection};
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_row_spine::{DatumSeq, ValRowColPagedBuilder, ValRowSpine};
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::merge_batcher::ColumnMergeBatcher;
use mz_timely_util::columnar::{Col2ValPagedBatcher, Column};
use mz_timely_util::containers::stack::FueledBuilder;
use std::convert::Infallible;
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::StreamVec;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Capability, CapabilitySet, Exchange as _};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::AntichainRef;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertSourceTime;
use crate::upsert::UpsertValue;

/// The persist-feedback arrangement's batcher, wrapping [`Col2ValPagedBatcher`]
/// only to capture the storage upsert-stash pager at construction.
///
/// `arrange_core` builds its batcher via [`Batcher::new`], which has no pager
/// hook, so a plain `Col2ValPagedBatcher` falls back to the process-global
/// (compute) pager — meaning the feedback arrangement's spill would be gated by
/// compute's `enable_column_paged_batcher_spill` rather than storage's
/// `enable_upsert_paged_spill`. Injecting `upsert_stash_pager::pager()` in `new`
/// puts the feedback arrangement under the same flag as the source stash. Every
/// other method delegates to the inner batcher unchanged.
struct UpsertFeedbackBatcher<T: columnar::Columnar>(Col2ValPagedBatcher<UpsertKey, Row, T, Diff>);

impl<T> Batcher for UpsertFeedbackBatcher<T>
where
    T: Timestamp + columnar::Columnar + Default + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    type Output = Column<((UpsertKey, Row), T, Diff)>;
    type Time = T;

    fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        let mut batcher =
            <Col2ValPagedBatcher<UpsertKey, Row, T, Diff> as Batcher>::new(logger, operator_id);
        batcher.set_pager(crate::upsert::upsert_stash_pager::pager());
        Self(batcher)
    }

    fn seal(&mut self, upper: Antichain<T>) -> (Vec<Self::Output>, Description<T>) {
        self.0.seal(upper)
    }

    fn frontier(&mut self) -> AntichainRef<'_, T> {
        self.0.frontier()
    }
}

impl<T> PushInto<Column<((UpsertKey, Row), T, Diff)>> for UpsertFeedbackBatcher<T>
where
    T: Timestamp + columnar::Columnar + Default + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    fn push_into(&mut self, chunk: Column<((UpsertKey, Row), T, Diff)>) {
        self.0.push_into(chunk)
    }
}

// The source stash carries the upsert payload in a custom diff type so the
// merge batcher consolidates by (key, time), keeping the update with the
// highest `FromTime` (latest source offset) per group. The diff is `Columnar`
// so the paged merge batcher can store it in a `Column` and page it out of RSS.
//
// The value is a tag-encoded `Row` (see `upsert_value_to_row`) rather than an
// `UpsertValue`: folding both the `Ok` and `Err` arms into one `Row` lets the
// value share a single columnar byte container, and `Row` already implements
// `Columnar`. `None` is a deletion tombstone.

// Derive ordering on the generated `UpsertDiffReference` too: the paged merge
// batcher requires `Ref: Ord` to sort the `(key, time, diff)` columns it
// consolidates. The derived order (by `from_time`, then `value`) is fine —
// "max FromTime wins" can tie only between equal `from_time`s, and a source
// never emits two distinct values for the same `(key, time, from_time)`, so
// the consolidated result doesn't depend on the fold order of equal
// `(key, time)` runs.
#[derive(Clone, Debug, Default, columnar::Columnar)]
#[columnar(derive(PartialEq, Eq, PartialOrd, Ord))]
struct UpsertDiff<O> {
    from_time: O,
    value: Option<Row>,
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

// Accumulate a borrowed columnar reference: the paged merge batcher consolidates
// `Column`-resident diffs through this path on every fold of an equal
// `(key, time)` run. Materialize only the order key to decide the "max FromTime
// wins" comparison — copying the value `Row` out of the column solely when `rhs`
// wins. Losing folds (the common case for a repeatedly-updated key) then pay no
// `Row` copy at all.
impl<'a, O> Semigroup<columnar::Ref<'a, UpsertDiff<O>>> for UpsertDiff<O>
where
    O: columnar::Columnar + Ord + Clone,
{
    fn plus_equals(&mut self, rhs: &columnar::Ref<'a, UpsertDiff<O>>) {
        let rhs_from_time = <O as columnar::Columnar>::into_owned(rhs.from_time);
        if rhs_from_time > self.from_time {
            self.from_time = rhs_from_time;
            self.value = <Option<Row> as columnar::Columnar>::into_owned(rhs.value);
        }
    }
}

/// Consolidate `updates` through `chunker` into `Column` chunks and push them
/// into `batcher`, emptying `updates` (keeping its capacity). The chunker
/// readies a fully-consolidated chunk per `push_into`, so the `extract` loop
/// drains everything it produced.
fn flush_to_batcher<T, O>(
    updates: &mut Vec<UpsertUpdate<T, O>>,
    chunker: &mut UpsertChunker<T, O>,
    batcher: &mut UpsertBatcher<T, O>,
) where
    T: columnar::Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: columnar::Columnar + Default + Ord + Clone,
    for<'a> columnar::Ref<'a, O>: Ord,
{
    use timely::container::{ContainerBuilder as _, PushInto as _};
    if updates.is_empty() {
        return;
    }
    let mut raw: Column<UpsertUpdate<T, O>> = Default::default();
    for update in updates.drain(..) {
        raw.push_into(&update);
    }
    chunker.push_into(&mut raw);
    while let Some(chunk) = chunker.extract() {
        batcher.push_into(std::mem::take(chunk));
    }
}

// The source stash uses the paged columnar merge batcher. Data is pushed in
// unsorted; the batcher maintains geometrically-sized sorted chains and
// consolidates via the UpsertDiff Semigroup automatically. Unlike DD's
// in-memory `VecMerger`, this batcher stores each chain entry as a `Column`
// routed through the process-global pager, so the not-yet-eligible backlog
// (the snapshot / persist-lag window) pages out of RSS instead of growing it.

/// One source-stash update: a key, its dataflow time, and the payload diff.
/// `O` is the columnar order key projected from the source `FromTime` (see
/// [`UpsertSourceTime`]).
type UpsertUpdate<T, O> = (UpsertKey, T, UpsertDiff<O>);

type UpsertBatcher<T, O> = ColumnMergeBatcher<UpsertKey, T, UpsertDiff<O>>;

/// The chunker that sorts and consolidates raw input into the `Column` chunks
/// [`UpsertBatcher`] consumes.
type UpsertChunker<T, O> = ColumnChunker<UpsertUpdate<T, O>>;

/// The operator's data-output handle. A fueled `Vec` builder so the drain can
/// `give_fueled` each emitted update and yield to timely under large snapshot
/// drains instead of monopolizing the worker.
type UpsertOutputHandle<T> =
    AsyncOutputHandle<T, FueledBuilder<CapacityContainerBuilder<Vec<(UpsertValue, T, Diff)>>>>;

// The persist-feedback arrangement uses a `ValRowSpine<UpsertKey, _, _>`: keys
// land in a columnation arena (`UpsertKey` is `[u8; 32]` + `Copy`, so it uses
// `CopyRegion`), and values are stored as packed `Row` bytes in a
// `DatumContainer`. `UpsertValue` is `Result<Row, Box<UpsertError>>`, so we
// still need to fold both arms into a single `Row` with a leading tag column
// so they share the value container.

/// Encode an [`UpsertValue`] as a `Row` with a leading tag column so both `Ok`
/// and `Err` payloads round-trip through `Row` byte storage.
///
/// Exposed (hidden from docs) only so the storage fuzz crate can exercise the
/// value encoding; not a stable public API.
#[doc(hidden)]
pub fn upsert_value_to_row(value: &UpsertValue) -> Row {
    let mut row = Row::default();
    let mut packer = row.packer();
    match value {
        Ok(ok) => {
            packer.push(Datum::UInt8(0));
            packer.extend(ok.iter());
        }
        Err(err) => {
            packer.push(Datum::UInt8(1));
            let bytes =
                bincode::serialize(err.as_ref()).expect("UpsertError is serializable via bincode");
            packer.push(Datum::Bytes(&bytes));
        }
    }
    row
}

/// Heap-size estimate for an emitted [`UpsertValue`], used to drive
/// `give_fueled` yielding on the output edge.
fn upsert_value_byte_len(value: &UpsertValue) -> usize {
    match value {
        Ok(row) => row.byte_len(),
        Err(err) => std::mem::size_of_val(err.as_ref()),
    }
}

/// Decode an [`UpsertValue`] produced by [`upsert_value_to_row`] back from the
/// `DatumSeq` view returned by a `ValRowSpine` cursor.
///
/// Exposed (hidden from docs) only so the storage fuzz crate can exercise the
/// value encoding; not a stable public API.
#[doc(hidden)]
pub fn datum_seq_to_upsert_value(seq: DatumSeq<'_>) -> UpsertValue {
    decode_upsert_value(seq)
}

/// Decode an [`UpsertValue`] produced by [`upsert_value_to_row`] from any datum
/// iterator — a `ValRowSpine` cursor's `DatumSeq` or a stashed `Row`'s `iter`.
fn decode_upsert_value<'a>(mut iter: impl Iterator<Item = Datum<'a>>) -> UpsertValue {
    let tag = match iter.next() {
        Some(Datum::UInt8(tag)) => tag,
        other => panic!("upsert value missing UInt8 tag, got {:?}", other),
    };
    match tag {
        0 => {
            let mut row = Row::default();
            row.packer().extend(iter);
            Ok(row)
        }
        1 => {
            let bytes = match iter.next() {
                Some(Datum::Bytes(b)) => b,
                other => panic!("upsert error tag missing Bytes payload, got {:?}", other),
            };
            let err: UpsertError =
                bincode::deserialize(bytes).expect("UpsertError bincode round-trip");
            Err(Box::new(err))
        }
        tag => panic!("unknown upsert value tag {tag}"),
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
#[allow(clippy::disallowed_methods)]
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
    T: Refines<mz_repr::Timestamp> + differential_dataflow::lattice::Lattice,
    T: columnation::Columnation,
    T: columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    FromTime: Debug + timely::ExchangeData + Clone + Ord + Sync,
    FromTime: UpsertSourceTime,
{
    // Arrange persist feedback.
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
    let persist_keyed = persist_keyed
        .inner
        // The arrangement already implicitly exchanges by key, so this is redundant, but we want to
        // do it earlier so that we can inspect the stream properly for source statistics.
        .exchange(move |((key, _), _, _)| UpsertKey::hashed(key))
        .as_collection()
        .inspect(move |((_, row), _, diff)| {
            source_config
                .source_statistics
                .update_records_indexed_by(diff.into_inner());
            source_config.source_statistics.update_bytes_indexed_by(
                row.as_ref().map_or(0, |r| r.byte_len().try_into().unwrap()) * diff.into_inner(),
            );
        });
    // Encode (UpsertKey, UpsertValue) → (UpsertKey, Row) into `Column`
    // containers so the feedback arrangement uses the paged columnar path: the
    // batcher routes its spine input through the process-global pager, paging
    // cold feedback chains out of RSS, while `ValRowSpine` keeps keys in a
    // columnation arena (UpsertKey is fixed-size [u8; 32]) and values as packed
    // `Row` bytes in a `DatumContainer`. Built with `Pipeline` so we keep the
    // locality established by the `UpsertKey::hashed` exchange above.
    let encoded = persist_keyed
        .inner
        .unary::<ColumnBuilder<((UpsertKey, Row), T, Diff)>, _, _, _>(
            Pipeline,
            "Persist feedback encode",
            |_, _| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for ((key, value), ts, diff) in data.drain(..) {
                            let row = upsert_value_to_row(&value);
                            session.give(((&key, &row), &ts, &diff));
                        }
                    });
                }
            },
        );
    let persist_arranged = arrange_core::<
        _,
        _,
        ColumnChunker<((UpsertKey, Row), T, Diff)>,
        UpsertFeedbackBatcher<T>,
        ValRowColPagedBuilder<UpsertKey, T, Diff>,
        ValRowSpine<UpsertKey, T, Diff>,
    >(encoded, Pipeline, "Persist feedback");
    let mut persist_trace = persist_arranged.trace.clone();

    // Probe the persist arrangement's stream for frontier tracking.
    // This replaces receiving the batch stream as an input — we just
    // read the probe frontier to know when persist has caught up.
    use timely::dataflow::operators::Probe;
    let (persist_probe, _persist_probe_stream) = persist_arranged.stream.probe();

    // Build the async processing operator.
    let mut builder = AsyncOperatorBuilder::new("Upsert V2".to_string(), input.scope());

    let (output_handle, output) = builder
        .new_output::<FueledBuilder<CapacityContainerBuilder<Vec<(UpsertValue, T, Diff)>>>>();
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

    let shutdown_button = builder.build(move |caps| async move {
        // Hold the persist source tokens for the operator's lifetime so the
        // feedback shard stays open until shutdown.
        let _persist_token = persist_token;

        let [output_cap, snapshot_cap, _health_cap]: [_; 3] = caps.try_into().unwrap();
        drop(output_cap);
        let mut snapshot_cap = CapabilitySet::from_elem(snapshot_cap);

        let mut hydrating = true;

        // Source stash backed by the paged columnar merge batcher. The batcher
        // maintains geometrically-sized sorted chains and consolidates via the
        // UpsertDiff Semigroup as data is pushed in, bounding memory to
        // O(unique key-time pairs) even during large initial snapshots, and
        // pages cold chains out of RSS through the pager.
        //
        // The pager is storage-owned (configured by `UpdateConfiguration` from
        // storage's own dyncfgs), distinct from the compute column-paged
        // batcher's process-global pager. Captured once here: backend / budget /
        // codec tunes take effect live (the policy is reconfigured in place),
        // but flipping the enable flag takes effect on dataflows created after
        // the change. While disabled, the pager keeps every chunk resident.
        let mut batcher: UpsertBatcher<T, FromTime::Order> = Batcher::new(None, 0);
        batcher.set_pager(crate::upsert::upsert_stash_pager::pager());
        // The chunker sorts and consolidates raw input into the `Column` chunks
        // the batcher consumes.
        let mut chunker: UpsertChunker<T, FromTime::Order> = Default::default();
        // Scratch buffer for accumulating source events before flushing to
        // the batcher. Drained on each iteration via the chunker.
        let mut push_buffer: Vec<UpsertUpdate<T, FromTime::Order>> = Vec::new();

        // Capability held at the minimum time of any buffered data. When
        // Some, the operator may still produce output; when None, the
        // batcher is empty.
        let mut stash_cap: Option<Capability<T>> = None;
        let mut input_upper = Antichain::from_elem(Timestamp::minimum());

        let snapshot_start = std::time::Instant::now();
        let mut prev_persist_upper = Antichain::from_elem(Timestamp::minimum());

        // Accumulators for rehydration metrics, set as gauges when rehydration completes.
        let mut rehydration_total: u64 = 0;
        let mut rehydration_updates: u64 = 0;

        // Main operator loop. Each iteration performs four steps:
        //   Step 1: Ingest source data into the batcher.
        //   Step 2: Read the persist frontier and update rehydration state.
        //   Step 3: Seal the batcher, drain eligible entries, push back the rest.
        //   Step 4: Manage the output capability.
        loop {
            // Block until woken by source input or a persist frontier advance.
            tokio::select! {
                _ = input.ready() => {}
                _ = persist_wakeup.ready() => {
                    while persist_wakeup.next_sync().is_some() {}
                }
            }

            // Step 1: Ingest source data.
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
                            let value = value.as_ref().map(upsert_value_to_row);
                            let from_time = from_time.upsert_order();
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

            // Flush buffered events through the chunker into the batcher. This
            // triggers the chunker + geometric chain merging, which consolidates
            // entries for the same (key, time) via the UpsertDiff Semigroup.
            flush_to_batcher(&mut push_buffer, &mut chunker, &mut batcher);

            // Step 2: Read persist frontier.
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
                    upsert_metrics.rehydration_total.set(rehydration_total);
                    upsert_metrics.rehydration_updates.set(rehydration_updates);
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

            // Step 3: Seal & drain.
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
            //
            // We skip the seal entirely unless an eligible entry is at all
            // possible. `seal` performs an O(N) merge of all chains
            // regardless of how much it extracts, so calling it when nothing
            // can be processed makes the operator quadratic in the number of
            // wakeups (a real pathology during upstream snapshots and during
            // rehydration when the source races ahead of persist).
            //
            // For an entry at `ts` to be eligible we need
            // `ts == persist_upper && ts < input_upper`. The necessary
            // preconditions, expressible without scanning the batcher:
            //   1. `cap.time() <= persist_upper`. Since `cap.time()` is
            //      maintained as a lower bound on `min(ts in batcher)`, if
            //      `cap.time() > persist_upper` then every buffered ts is
            //      strictly above persist_upper and none can equal it.
            //   2. `persist_upper < input_upper`. Otherwise no `ts` that
            //      satisfies `ts == persist_upper` can also satisfy
            //      `ts < input_upper`.
            //
            // This naturally covers both the post-hydration source-snapshot
            // case (cap == persist == input → condition 2 fails) and the
            // rehydration-with-source-ahead case (cap > persist → condition
            // 1 fails). It also no-ops correctly when persist has shut down
            // (empty persist_upper makes condition 2 vacuously false).
            if let Some(cap) = stash_cap.as_mut()
                && !persist_upper.less_than(cap.time())
                && PartialOrder::less_than(&persist_upper, &input_upper)
            {
                // Step 1 already consolidated `push_buffer` through the chunker
                // (which readies a complete chunk per `push_into`), so the
                // chunker holds nothing pending here and we can seal directly.
                let (sealed, _description) = batcher.seal(input_upper.clone());
                // Frontier of data remaining in the batcher (ts >= input_upper).
                let remaining_frontier = batcher.frontier().to_owned();

                let mut ineligible = Vec::new();
                // `drain_sealed_input` emits eligible output directly through
                // `output_handle` (fueled), so there is no intermediate output
                // buffer to drain afterward.
                let drain_stats = drain_sealed_input(
                    sealed,
                    &mut ineligible,
                    &output_handle,
                    &*cap,
                    &persist_upper,
                    &mut persist_trace,
                    &source_config.worker_id,
                    &source_config.id,
                )
                .await;

                upsert_metrics.multi_get_size.inc_by(drain_stats.eligible);
                upsert_metrics
                    .multi_get_result_count
                    .inc_by(drain_stats.result_count);
                upsert_metrics
                    .multi_put_size
                    .inc_by(drain_stats.output_count);
                upsert_metrics.upsert_inserts.inc_by(drain_stats.inserts);
                upsert_metrics.upsert_updates.inc_by(drain_stats.updates);
                upsert_metrics.upsert_deletes.inc_by(drain_stats.deletes);

                if hydrating {
                    rehydration_total += drain_stats.inserts;
                    rehydration_updates += drain_stats.eligible;
                }

                // Step 4: Capability management.
                // Downgrade the output capability to the minimum time of any
                // remaining data: either entries still in the batcher (above
                // input_upper) or ineligible entries being pushed back.
                let min_ineligible_ts = ineligible.iter().map(|(_, ts, _)| ts).min().cloned();
                flush_to_batcher(&mut ineligible, &mut chunker, &mut batcher);

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
}

/// Process sealed chunks from the batcher, classifying each entry by its
/// timestamp relative to `persist_upper`: entries at the frontier are eligible
/// for processing now (cursor lookup + output), entries above it are returned
/// in `ineligible` for re-stashing, and entries below it are already persisted
/// and dropped (see the body for why).
///
/// The sealed chunks are already sorted and consolidated by the MergeBatcher,
/// so the trace cursor walks forward through keys in order — seeks amortize.
async fn drain_sealed_input<T, O>(
    sealed: Vec<Column<UpsertUpdate<T, O>>>,
    ineligible: &mut Vec<UpsertUpdate<T, O>>,
    output_handle: &UpsertOutputHandle<T>,
    output_cap: &Capability<T>,
    persist_upper: &Antichain<T>,
    trace: &mut TraceAgent<ValRowSpine<UpsertKey, T, Diff>>,
    worker_id: &usize,
    source_id: &GlobalId,
) -> DrainStats
where
    T: TotalOrder + Lattice + timely::ExchangeData + Timestamp + Clone + Debug + Ord + Sync,
    T: columnation::Columnation + columnar::Columnar,
    O: columnar::Columnar,
{
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
    // Walk the sealed chunks by reference rather than collecting the eligible
    // set into an owned Vec. The chunks are globally sorted (the seal merges
    // all chains into one run), so the cursor seeks still walk forward and
    // amortize, and eligible values are emitted straight from the column's
    // `RowRef` with no owned `UpsertDiff` copy. Only the re-stashed ineligible
    // set is materialized.
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
            if !persist_upper.less_equal(&ts) {
                // ts < persist_upper: drop.
                continue;
            }
            if persist_upper.less_than(&ts) {
                // ts > persist_upper: re-stash for later (owned).
                ineligible.push((
                    *key,
                    ts,
                    <UpsertDiff<O> as columnar::Columnar>::into_owned(diff),
                ));
                continue;
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

    // The tests drive the operator with a plain integer `FromTime` standing in
    // for a Kafka offset; project it to itself so dedup orders by it directly.
    impl UpsertSourceTime for i32 {
        type Order = i32;
        fn upsert_order(&self) -> i32 {
            *self
        }
    }

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

    /// Operator-level repro of the 0dt read-only-handoff stranding bug.
    ///
    /// Models a lagging replacement generation: the external (old) writer has
    /// already advanced the shard — and therefore the feedback `persist_upper`
    /// — to `T = 10`, while the operator itself has emitted nothing. The
    /// lagging replacement now produces source data at timestamps BELOW that
    /// upper (`ts = 5, 7`), i.e. data the external writer has already persisted.
    ///
    /// `drain_sealed_input` DROPS such already-persisted data (it satisfies
    /// neither `ts == persist_upper` nor `ts > persist_upper`), mirroring v1's
    /// `relevant = persist_upper.less_equal(ts)`. Were it instead re-stashed,
    /// the data would be stranded forever — `persist_upper` only advances, so
    /// `ts == persist_upper` could never again hold — and `min_ineligible_ts`
    /// would pin the operator's output capability at `ts = 5`, BELOW the shard
    /// upper, where it would stay for good. Dropping it lets the frontier
    /// advance freely.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn lagging_replacement_below_upper_strands_data() {
        let (frontier, emitted) = run_below_upper_scenario_v2();

        // The below-upper data is discarded (no output) and the output frontier
        // is not pinned below the shard upper (10); it advances to the input
        // upper (11), matching v1's behavior.
        assert!(
            emitted.is_empty(),
            "below-upper data should be dropped, not emitted; got {emitted:?}"
        );
        assert_eq!(
            frontier,
            vec![new_ts(11)],
            "v2 output frontier should advance to the input upper, not pin below \
             persist_upper"
        );
        assert!(
            frontier[0] >= new_ts(10),
            "v2 output frontier {frontier:?} should reach at least persist_upper (10)"
        );
    }

    /// Shared driver for the lagging-replacement scenario against v2. Returns
    /// `(output_frontier, consolidated_emitted_updates)`.
    fn run_below_upper_scenario_v2() -> (Vec<Ts>, Vec<(Result<Row, DataflowError>, Ts, Diff)>) {
        use timely::dataflow::operators::Probe;

        let (frontier, capture) = timely::execute_directly(move |worker| {
            let (mut input, mut persist, probe, capture) =
                worker.dataflow::<MzTimestamp, _, _>(|scope| {
                    scope.scoped::<Ts, _, _>("upsert", |scope| {
                        let (input_handle, input) = scope.new_input();
                        let (persist_handle, persist_input) = scope.new_input();
                        let source_id = GlobalId::User(0);

                        let reg = MetricsRegistry::new();
                        let upsert_defs = UpsertMetricDefs::register_with(&reg);
                        let upsert_metrics = UpsertMetrics::new(&upsert_defs, source_id, 0, None);

                        let reg2 = MetricsRegistry::new();
                        let storage_metrics = StorageMetrics::register_with(&reg2);

                        let reg3 = MetricsRegistry::new();
                        let stats_defs = SourceStatisticsMetricDefs::register_with(&reg3);
                        let envelope = SourceEnvelope::Upsert(UpsertEnvelope {
                            source_arity: 2,
                            style: UpsertStyle::Default(KeyEnvelope::Flattened),
                            key_indices: vec![0],
                        });
                        let source_statistics = SourceStatistics::new(
                            source_id,
                            0,
                            &stats_defs,
                            source_id,
                            &ShardId::new(),
                            envelope,
                            Antichain::from_elem(Timestamp::minimum()),
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
                        let (probe, stream) = output.inner.probe();
                        (input_handle, persist_handle, probe, stream.capture())
                    })
                });

            // The external writer has advanced the shard (feedback persist_upper)
            // to T = 10 WITHOUT the operator emitting anything itself.
            persist.advance_to(new_ts(10));
            for _ in 0..20 {
                worker.step();
            }

            // The lagging replacement produces source data at ts BELOW the
            // current persist_upper (5 and 7 while persist_upper = 10).
            input.send(((key(0), Some(Ok(row(0, 1))), 1), new_ts(5), Diff::ONE));
            input.send(((key(1), Some(Ok(row(1, 2))), 2), new_ts(7), Diff::ONE));
            input.advance_to(new_ts(11));
            for _ in 0..20 {
                worker.step();
            }

            (probe.with_frontier(|f| f.to_vec()), capture)
        });

        let mut emitted: Vec<_> = capture
            .extract()
            .into_iter()
            .flat_map(|(_cap, c)| c)
            .collect();
        differential_dataflow::consolidation::consolidate_updates(&mut emitted);
        (frontier, emitted)
    }
}
