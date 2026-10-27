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
//!    source-stash batcher. The batcher consolidates entries for the same
//!    `(key, time)` via the `UpsertDiff` Semigroup, keeping the update with
//!    the highest order key (latest source offset), through amortized
//!    geometric merging as data is pushed in, and cold stash state leaves RSS
//!    through the flavor's spill path (see below). This bounds resident
//!    memory even during large source snapshots.
//!
//! 2. **Read persist frontier.** Check the probe on the persist arrangement
//!    to learn which times have been committed. When the persist frontier
//!    reaches the resume upper, rehydration is complete.
//!
//! 3. **Seal & drain.** Seal the batcher just past the persist frontier
//!    (capped at the input frontier, see `seal_upper`) to extract
//!    source-finalized entries as sorted, consolidated chunks. Each entry is
//!    classified:
//!    - **Eligible** (at the persist frontier): the persist trace has the
//!      correct "before" state for this time. Look up the old value in the
//!      feedback arrangement, emit a retraction if present, and emit the new
//!      value.
//!    - **Ineligible** (above the persist frontier, below the seal upper):
//!      persist hasn't caught up yet. Push back into the batcher for the next
//!      iteration.
//!    - **Already persisted** (below the persist frontier): some writer has
//!      already advanced the shard past this time, so it is dropped. See the
//!      drain functions for why re-stashing it would strand the data and pin
//!      the output frontier below the shard upper.
//!
//! 4. **Capability management.** Downgrade the output capability to the
//!    minimum time of any remaining buffered data (in the batcher or pushed
//!    back as ineligible). Drop the capability entirely when the batcher is
//!    empty.
//!
//! ## Stash flavors
//!
//! [`UpsertStashFlavor`], resolved from `enable_upsert_chunked_stash` at
//! operator construction, selects between two instantiations of the same
//! loop:
//!
//! * **Chunked**: the stash is differential's chunk merge batcher over
//!   time-major `ColumnChunk`s and the feedback arrangement is a spine of
//!   chunk batches. Committed chunk bodies spill to the process buffer pool,
//!   the drain loads sealed chunks one at a time, and prior state comes back
//!   through bulk probes of the trace's batches.
//! * **Paged**: the stash is the paged columnar merge batcher and the
//!   feedback arrangement is a `ValRowSpine`. Cold chains page out of RSS
//!   through the storage-owned column pager, and prior state comes back
//!   through a trace cursor.
//!
//! Both flavors' spill paths are gated by `enable_upsert_paged_spill`.
//!
//! ## Eligibility condition (total order)
//!
//! For a total-order timestamp with `input_upper = {i}` and
//! `persist_upper = {p}`, an entry at time `ts` is eligible when
//! `ts == p < i` — the source has finalized it and persist is exactly at
//! that time, so the feedback trace holds the correct prior state. An entry
//! with `p < ts` is ineligible (persist hasn't caught up), and one with
//! `ts < p` is already persisted and dropped.

use std::fmt::Debug;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::operators::arrange::arrangement::{Arranged, arrange_core};
use differential_dataflow::trace::chunk::{ChunkBatcher, ChunkBuilder};
use differential_dataflow::trace::{BatchReader, Batcher, TraceReader};
use differential_dataflow::{AsCollection, VecCollection};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_repr::{Diff, GlobalId, Row};
use mz_row_spine::{FundedValRowSpine, ValRowColPagedBuilder};
use mz_storage_types::dyncfgs::ENABLE_UPSERT_CHUNKED_STASH;
use mz_storage_types::errors::{DataflowError, EnvelopeError};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use mz_timely_util::columnar::chunk::{ChunkChunker, StorageSpill};
use mz_timely_util::columnar::merge_batcher::PagedChunker;
use mz_timely_util::containers::stack::FueledBuilder;
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
use crate::upsert::UpsertSourceTime;
use crate::upsert::UpsertValue;

use self::feedback::{FeedbackChunk, FeedbackSpine, UpsertFeedbackBatcher, encode_feedback};
use self::stash::{ChunkedArm, PagedArm, UpsertDiff, UpsertStashArm, UpsertUpdate, seal_upper};

#[cfg(feature = "fuzzing")]
pub use self::feedback::datum_seq_to_upsert_value;
pub use self::feedback::upsert_value_to_row;

mod drain;
mod feedback;
mod stash;
#[cfg(test)]
mod tests;

/// Which stash and feedback-arrangement representation the upsert-v2
/// operator instantiates. The two flavors run the same operator loop; they
/// differ in the batcher, the feedback trace, and how the drain reads prior
/// state. See the module docs for the comparison.
#[derive(Clone, Copy, Debug)]
pub enum UpsertStashFlavor {
    /// Paged columnar merge batcher stash, `ValRowSpine` feedback
    /// arrangement, cursor-based drain. Spills through the storage-owned
    /// column pager.
    Paged,
    /// Chunk merge batcher stash, chunk-spine feedback arrangement,
    /// bulk-probe drain. Spills through the process buffer pool.
    Chunked,
}

impl UpsertStashFlavor {
    /// Resolve the flavor from the replica's config set. Call this once per
    /// source at operator construction time, so a dataflow keeps one flavor
    /// for its whole life even if the flag flips underneath it.
    pub fn from_config(config: &ConfigSet) -> Self {
        if ENABLE_UPSERT_CHUNKED_STASH.get(config) {
            Self::Chunked
        } else {
            Self::Paged
        }
    }
}

/// The operator's data-output handle. A fueled `Vec` builder so the drain can
/// `give_fueled` each emitted update and yield to timely under large snapshot
/// drains instead of monopolizing the worker.
type UpsertOutputHandle<T> =
    AsyncOutputHandle<T, FueledBuilder<CapacityContainerBuilder<Vec<(UpsertValue, T, Diff)>>>>;

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
///      from persist.  Arranged into a trace for prior-value lookups.
///
/// `flavor` selects the stash and feedback-arrangement representation; see
/// the module docs.
#[allow(clippy::disallowed_methods)]
pub fn upsert_inner<'scope, T, FromTime>(
    flavor: UpsertStashFlavor,
    input: VecCollection<'scope, T, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    key_indices: Vec<usize>,
    resume_upper: Antichain<T>,
    persist_ok: VecCollection<'scope, T, Row, Diff>,
    persist_err: VecCollection<'scope, T, DataflowError, Diff>,
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
    // The feedback keying and encoding are flavor-independent; the
    // arrangement they feed is not, so each arm names its own arrange types
    // and hands the arrangement to the shared operator loop.
    let encoded = encode_feedback(
        persist_ok,
        persist_err,
        key_indices,
        source_config.source_statistics.clone(),
    );
    match flavor {
        UpsertStashFlavor::Chunked => {
            // Chains and sealed batches alike are `FeedbackChunk`s whose
            // bodies spill to the buffer pool, behind the same storage spill
            // gate as the source stash.
            let persist_arranged = arrange_core::<
                _,
                _,
                ChunkChunker<(UpsertKey, Row), T, Diff, StorageSpill>,
                ChunkBatcher<FeedbackChunk<T>>,
                ChunkBuilder<FeedbackChunk<T>>,
                FeedbackSpine<T>,
            >(encoded, Pipeline, "Persist feedback");
            build_upsert_operator::<ChunkedArm, _, _>(
                input,
                resume_upper,
                persist_arranged,
                persist_token,
                upsert_metrics,
                source_config,
            )
        }
        UpsertStashFlavor::Paged => {
            // The batcher routes its spine input through the storage-owned
            // pager, paging cold feedback chains out of RSS, while
            // `ValRowSpine` keeps keys in a columnation arena (`UpsertKey`
            // is fixed-size `[u8; 32]`) and values as packed `Row` bytes in
            // a `DatumContainer`.
            let persist_arranged = arrange_core::<
                _,
                _,
                PagedChunker<((UpsertKey, Row), T, Diff)>,
                UpsertFeedbackBatcher<T>,
                ValRowColPagedBuilder<UpsertKey, T, Diff>,
                FundedValRowSpine<UpsertKey, T, Diff>,
            >(encoded, Pipeline, "Persist feedback");
            build_upsert_operator::<PagedArm, _, _>(
                input,
                resume_upper,
                persist_arranged,
                persist_token,
                upsert_metrics,
                source_config,
            )
        }
    }
}

/// The flavor-independent upsert-v2 operator loop, generic over an
/// [`UpsertStashArm`].
///
/// Consumes the source input and the arranged persist feedback (constructed
/// per flavor by [`upsert_inner`]) and drives the ingest / seal / drain /
/// capability loop described in the module docs, calling through the arm at
/// the few points where the flavors diverge.
#[allow(clippy::disallowed_methods)]
fn build_upsert_operator<'scope, A, T, FromTime>(
    input: VecCollection<'scope, T, (UpsertKey, Option<UpsertValue>, FromTime), Diff>,
    resume_upper: Antichain<T>,
    persist_arranged: Arranged<'scope, TraceAgent<A::Spine>>,
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
    A: UpsertStashArm<T, FromTime::Order>,
    T: Timestamp + TotalOrder + Sync,
    T: Refines<mz_repr::Timestamp> + differential_dataflow::lattice::Lattice,
    T: columnation::Columnation,
    T: columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    FromTime: Debug + timely::ExchangeData + Clone + Ord + Sync,
    FromTime: UpsertSourceTime,
{
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

        // Source stash. The batcher maintains geometrically-sized sorted
        // chains and consolidates via the UpsertDiff Semigroup as data is
        // pushed in, bounding memory to O(unique key-time pairs) even during
        // large initial snapshots. How cold stash state leaves RSS is
        // flavor-specific; see the arm impls.
        let mut batcher = A::new_batcher();
        // The chunker sorts and consolidates raw input into the chunks the
        // batcher consumes.
        let mut chunker = A::Chunker::default();
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
                    while let Some(event) = persist_wakeup.next_sync() {
                        if let AsyncEvent::Data(_, batches) = event {
                            for batch in batches {
                                mz_timely_util::columnar::chunk::metrics::record_batch(batch.len());
                            }
                        }
                    }
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
            A::flush(&mut push_buffer, &mut chunker, &mut batcher);

            // Step 2: Read persist frontier.
            // The persist probe tells us which output times have been
            // committed back through the feedback loop. This determines:
            //   - Whether rehydration is complete (persist >= resume_upper).
            //   - Which source entries are eligible for processing (their
            //     time must equal persist_upper so the feedback trace holds
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
            // Seal the batcher at `seal_upper`, just past persist_upper and at
            // most input_upper, to extract source-finalized entries as sorted,
            // consolidated chunks. The seal merges all internal chains and
            // splits by time: entries below the seal upper are extracted, the
            // rest stay in the batcher.
            //
            // Extracted entries are partitioned into:
            //   - Eligible (ts == persist_upper): processed now via a
            //     prior-value lookup in the persist trace.
            //   - Ineligible (persist_upper < ts < seal upper): persist
            //     hasn't caught up yet, so pushed back into the batcher.
            //
            // We skip the seal entirely unless an eligible entry is at all
            // possible. `seal` merges all chains regardless of how much it
            // extracts, so calling it when nothing can be processed makes
            // the operator quadratic in the number of wakeups (a real
            // pathology during upstream snapshots and during rehydration
            // when the source races ahead of persist).
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
                let (sealed, _description) = batcher.seal(seal_upper(&persist_upper, &input_upper));
                // Frontier of data remaining in the batcher (ts >= seal upper).
                let remaining_frontier = batcher.frontier().to_owned();

                let mut ineligible = Vec::new();
                // The drain emits eligible output directly through
                // `output_handle` (fueled), so there is no intermediate
                // output buffer to drain afterward.
                let drain_stats = A::drain(
                    sealed,
                    &mut ineligible,
                    &output_handle,
                    &*cap,
                    &persist_upper,
                    &mut persist_trace,
                    source_config.worker_id,
                    source_config.id,
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
                upsert_metrics
                    .restashed
                    .inc_by(u64::cast_from(ineligible.len()));

                if hydrating {
                    rehydration_total += drain_stats.inserts;
                    rehydration_updates += drain_stats.eligible;
                }

                // Step 4: Capability management.
                // Downgrade the output capability to the minimum time of any
                // remaining data: either entries still in the batcher (at or
                // above the seal upper) or ineligible entries being pushed back.
                let min_ineligible_ts = ineligible.iter().map(|(_, ts, _)| ts).min().cloned();
                A::flush(&mut ineligible, &mut chunker, &mut batcher);

                // `Option::min` alone would be wrong here, `None` sorts low.
                // Chain the candidates and take the min over present ones.
                let min_ts = remaining_frontier
                    .elements()
                    .first()
                    .into_iter()
                    .chain(min_ineligible_ts.as_ref())
                    .min();
                match min_ts {
                    Some(min_ts) => cap.downgrade(min_ts),
                    // Batcher is completely empty. Drop the capability so
                    // downstream operators can make progress.
                    None => stash_cap = None,
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
