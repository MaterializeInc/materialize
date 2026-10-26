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

use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::operators::arrange::arrangement::{Arranged, arrange_core};
use differential_dataflow::trace::chunk::{ChunkBatch, ChunkBatcher, ChunkBuilder};
use differential_dataflow::trace::{BatchReader, Batcher, Cursor, Description, TraceReader};
use differential_dataflow::{AsCollection, VecCollection};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_repr::{Datum, Diff, GlobalId, Row};
// Only the fuzzing-gated `datum_seq_to_upsert_value` takes a `DatumSeq`.
#[cfg(feature = "fuzzing")]
use mz_row_spine::DatumSeq;
use mz_row_spine::{FundedValRowSpine, ValRowColPagedBuilder};
use mz_storage_types::dyncfgs::ENABLE_UPSERT_CHUNKED_STASH;
use mz_storage_types::errors::{DataflowError, EnvelopeError, UpsertError};
use mz_timely_util::builder_async::{
    AsyncOutputHandle, Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder,
    PressOnDropButton,
};
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::body::ColumnBody;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::chunk::{ChunkChunker, ColumnChunk, StorageSpill};
use mz_timely_util::columnar::merge_batcher::{ColumnMergeBatcher, PagedChunker};
use mz_timely_util::columnar::unload::UnloadBatch;
use mz_timely_util::columnar::{Col2ValPagedBatcher, Column};
use mz_timely_util::containers::stack::FueledBuilder;
use std::convert::Infallible;
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::operators::{Capability, CapabilitySet, Exchange as _};
use timely::dataflow::{Stream, StreamVec};
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::AntichainRef;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::healthcheck::HealthStatusUpdate;
use crate::metrics::upsert::UpsertMetrics;
use crate::statistics::SourceStatistics;
use crate::upsert::UpsertKey;
use crate::upsert::UpsertSourceTime;
use crate::upsert::UpsertValue;

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

/// The paged flavor's persist-feedback batcher, wrapping
/// [`Col2ValPagedBatcher`] only to capture the storage upsert-stash pager at
/// construction.
///
/// `arrange_core` builds its batcher via [`Batcher::new`], which has no pager
/// hook, so a plain `Col2ValPagedBatcher` falls back to the process-global
/// (compute) pager, meaning the feedback arrangement's spill would be gated
/// by compute's `enable_column_paged_batcher_spill` rather than storage's
/// `enable_upsert_paged_spill`. Injecting `upsert_stash_pager::pager()` in
/// `new` puts the feedback arrangement under the same flag as the source
/// stash. Every other method delegates to the inner batcher unchanged.
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

/// One persist-feedback update: a key, its current value row, the time, and
/// an additive count.
type FeedbackUpdate<T> = ((UpsertKey, Row), T, Diff);

/// One feedback-arrangement chunk: a sorted, consolidated run of updates,
/// resident or spilled to the buffer pool. Both batcher chains and sealed
/// spine batches are sequences of these, so the arrangement's state pages out
/// of RSS under the pool's budget, and the drain reads it back through the
/// bulk [`UnloadChunk`](mz_timely_util::columnar::unload::UnloadChunk)
/// surface: copy-out probes, no cursor borrows.
type FeedbackChunk<T> = ColumnChunk<(UpsertKey, Row), T, Diff, StorageSpill>;

/// The feedback arrangement's trace: a funded spine of `Rc`-shared chunk
/// batches. See [`mz_timely_util::funded_spine`].
type FeedbackSpine<T> =
    mz_timely_util::funded_spine::Spine<std::rc::Rc<ChunkBatch<FeedbackChunk<T>>>>;

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

/// One source-stash update: a key, its dataflow time, and the payload diff.
/// `O` is the columnar order key projected from the source `FromTime` (see
/// [`UpsertSourceTime`]).
type UpsertUpdate<T, O> = (UpsertKey, T, UpsertDiff<O>);

/// One chunked-flavor stash update: [`UpsertUpdate`] with the time copied
/// into the data, so the batcher sorts by `(time, key)`.
///
/// A seal at an upper then cuts a prefix of the batcher's chains. Its extract
/// passes chunks wholly on either side of the upper through by their time
/// bounds, so it rewrites only the chunk the upper splits. Its merges pass
/// chunks through when their data ranges are disjoint, which holds between
/// chains at different times, so data at a later time does not rewrite the
/// stashed backlog; data at the same time merges as in any batcher (see
/// [`ChunkedArm`] for what a pass-through still costs). Sorted by key, every
/// chunk spans the stash's whole time range, so each seal would load and
/// rewrite every chunk, which makes catching up on a tail of `n` timestamps
/// quadratic in the tail. Within one time, entries stay sorted by key, which
/// the drain's monotone probes rely on.
type TimeMajorUpdate<T, O> = ((T, UpsertKey), T, UpsertDiff<O>);

/// One chunked-flavor stash chunk: a sorted, consolidated run of time-major
/// updates, resident or spilled to the buffer pool.
type UpsertChunk<T, O> = ColumnChunk<(T, UpsertKey), T, UpsertDiff<O>, StorageSpill>;

/// The chunked flavor's stash: differential's chunk merge batcher over
/// `ColumnChunk`s. Data is pushed in unsorted. The batcher maintains
/// geometrically-sized sorted chains and consolidates via the UpsertDiff
/// Semigroup automatically. Committed chunks spill their bodies to the
/// process buffer pool (see `mz_timely_util::columnar::chunk`), so the
/// not-yet-eligible backlog (the snapshot / persist-lag window) pages out of
/// RSS instead of growing it.
type UpsertChunkBatcher<T, O> = ChunkBatcher<UpsertChunk<T, O>>;

/// The paged flavor's stash: the paged columnar merge batcher, consolidating
/// like [`UpsertChunkBatcher`] but storing each chain entry as a `Column`
/// routed through the storage-owned pager, which pages cold chains out of
/// RSS.
type UpsertPagedBatcher<T, O> = ColumnMergeBatcher<UpsertKey, T, UpsertDiff<O>>;

/// The chunker that sorts and consolidates raw input into the `Column` chunks
/// the paged stash batcher consumes.
type UpsertChunker<T, O> = ColumnChunker<UpsertUpdate<T, O>>;

/// [`UpsertChunker`]'s time-major counterpart, for the chunked stash.
type TimeMajorChunker<T, O> = ColumnChunker<TimeMajorUpdate<T, O>>;

/// The operator's data-output handle. A fueled `Vec` builder so the drain can
/// `give_fueled` each emitted update and yield to timely under large snapshot
/// drains instead of monopolizing the worker.
type UpsertOutputHandle<T> =
    AsyncOutputHandle<T, FueledBuilder<CapacityContainerBuilder<Vec<(UpsertValue, T, Diff)>>>>;

// The persist-feedback arrangement stores `(UpsertKey, Row)` pairs in columnar
// chunk batches ([`FeedbackSpine`]). `UpsertValue` is
// `Result<Row, Box<UpsertError>>`, so we fold both arms into a single `Row`
// with a leading tag column so they share the value column.

/// Encode an [`UpsertValue`] as a `Row` with a leading tag column so both `Ok`
/// and `Err` payloads round-trip through `Row` byte storage.
///
/// Used on the render path. `pub` only so [`crate::fuzz_exports`] can re-export
/// it under the `fuzzing` feature for the storage fuzz crate. The enclosing
/// module is crate-private, so it is not otherwise reachable. Not a stable
/// public API.
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
/// Exists only for the storage fuzz crate, so it is gated behind the `fuzzing`
/// feature. Not a stable public API.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub fn datum_seq_to_upsert_value(seq: DatumSeq<'_>) -> UpsertValue {
    decode_upsert_value(seq)
}

/// Decode an [`UpsertValue`] produced by [`upsert_value_to_row`] from any
/// datum iterator, whether over a columnar `Row` reference or an owned `Row`.
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

/// Key the persist feedback by [`UpsertKey`], record source statistics, and
/// encode `(UpsertKey, UpsertValue)` as `(UpsertKey, Row)` `Column` chunks,
/// the input both flavors' feedback arrangements consume. Built with
/// `Pipeline` downstream of an `UpsertKey::hashed` exchange, so the
/// arrangement keeps that locality.
fn encode_feedback<'scope, T>(
    persist_ok: VecCollection<'scope, T, Row, Diff>,
    persist_err: VecCollection<'scope, T, DataflowError, Diff>,
    key_indices: Vec<usize>,
    source_statistics: SourceStatistics,
) -> Stream<'scope, T, Column<((UpsertKey, Row), T, Diff)>>
where
    T: Timestamp + TotalOrder + Sync,
    T: Refines<mz_repr::Timestamp> + differential_dataflow::lattice::Lattice,
    T: columnation::Columnation,
    T: columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
{
    let persist_keyed = crate::upsert::key_persist_feedback(persist_ok, persist_err, key_indices);
    let persist_keyed = persist_keyed
        .inner
        // The arrangement already implicitly exchanges by key, so this is redundant, but we want to
        // do it earlier so that we can inspect the stream properly for source statistics.
        .exchange(move |((key, _), _, _)| UpsertKey::hashed(key))
        .as_collection()
        .inspect(move |((_, row), _, diff)| {
            source_statistics.update_records_indexed_by(diff.into_inner());
            source_statistics.update_bytes_indexed_by(
                row.as_ref().map_or(0, |r| r.byte_len().try_into().unwrap()) * diff.into_inner(),
            );
        });
    persist_keyed
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
        )
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

/// The start of the `mz_repr::Timestamp` after `persist_upper`'s, capped at
/// `input_upper` so no unfinalized time is extracted.
///
/// Only entries at `persist_upper` are eligible in a round, and the drain
/// returns every extracted entry past `persist_upper` as a resident `Vec`,
/// outside the buffer pool budget, to be re-pushed through the chunker.
/// Sealing at `input_upper` instead would do that for the whole backlog
/// between the two frontiers once per persist round trip, which is the entire
/// source tail while the operator catches up from behind.
fn seal_upper<T>(persist_upper: &Antichain<T>, input_upper: &Antichain<T>) -> Antichain<T>
where
    T: Timestamp + TotalOrder + Refines<mz_repr::Timestamp>,
{
    match persist_upper.as_option().and_then(next_outer) {
        Some(next) if !input_upper.less_equal(&next) => Antichain::from_elem(next),
        _ => input_upper.clone(),
    }
}

/// The least time past every time sharing `ts`'s `mz_repr::Timestamp`, or
/// `None` when that timestamp is the maximum.
fn next_outer<T: Refines<mz_repr::Timestamp>>(ts: &T) -> Option<T> {
    ts.clone().to_outer().try_step_forward().map(T::to_inner)
}

/// The flavor-specific pieces of the upsert-v2 operator: the stash batcher,
/// the feedback arrangement's spine, and how the drain reads prior state.
/// [`build_upsert_operator`] holds the flavor-independent operator loop and
/// calls through this trait at the few points where the flavors diverge.
trait UpsertStashArm<T, O>
where
    T: Timestamp + Lattice + columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: columnar::Columnar + Default + Ord + Clone + Send + Sync + 'static,
    for<'a> columnar::Ref<'a, O>: Ord + Copy,
{
    /// The feedback arrangement's spine. `'static` because the operator
    /// future owns a trace agent for it.
    type Spine: TraceReader<Time = T> + 'static;
    /// The source-stash batcher. `'static` because the operator future owns
    /// it.
    type Batcher: Batcher<Time = T> + 'static;

    /// Sorts and consolidates raw updates into the batcher's chunks.
    type Chunker: Default + 'static;

    /// A new stash batcher for one source dataflow.
    fn new_batcher() -> Self::Batcher;

    /// Consolidate `updates` through `chunker` into chunks and push them into
    /// `batcher`, emptying `updates` (keeping its capacity). The chunker
    /// readies a fully-consolidated chunk per `push_into`, so nothing stays
    /// pending in it between calls.
    fn flush(
        updates: &mut Vec<UpsertUpdate<T, O>>,
        chunker: &mut Self::Chunker,
        batcher: &mut Self::Batcher,
    );

    /// Classify one sealed stash against `persist_upper` and emit eligible
    /// output; see [`DrainStats`].
    async fn drain(
        sealed: Vec<<Self::Batcher as Batcher>::Output>,
        ineligible: &mut Vec<UpsertUpdate<T, O>>,
        output_handle: &UpsertOutputHandle<T>,
        output_cap: &Capability<T>,
        persist_upper: &Antichain<T>,
        trace: &mut TraceAgent<Self::Spine>,
        worker_id: usize,
        source_id: GlobalId,
    ) -> DrainStats;
}

/// The shared body of the arms' `flush`: writes each update into a raw
/// column through `record`, consolidates it through `chunker`, and hands each
/// readied chunk to `push`.
fn flush_through<T, O, U, C>(
    updates: &mut Vec<UpsertUpdate<T, O>>,
    chunker: &mut C,
    mut record: impl FnMut(UpsertUpdate<T, O>, &mut Column<U>),
    mut push: impl FnMut(C::Container),
) where
    U: columnar::Columnar,
    C: ContainerBuilder + for<'a> PushInto<&'a mut Column<U>>,
    C::Container: Default,
{
    if updates.is_empty() {
        return;
    }
    let mut raw: Column<U> = Default::default();
    for update in updates.drain(..) {
        record(update, &mut raw);
    }
    chunker.push_into(&mut raw);
    while let Some(chunk) = chunker.extract() {
        push(std::mem::take(chunk));
    }
}

/// The chunked flavor: time-major [`UpsertChunkBatcher`] stash,
/// [`FeedbackSpine`] feedback arrangement, bulk-probe drain.
///
/// NOTE: the seal's chain merge loads the bodies of the chunks it actually
/// merges. Chunks whose data ranges are disjoint pass through, and the
/// partition against the upper passes chunks wholly on one side of it
/// through, so only chunks the upper splits are rewritten (see
/// [`TimeMajorUpdate`]). Two costs remain per seal. A spilled body that ages
/// to `compress_min_depth` through a pass-through is loaded once and
/// re-spilled compressed. A resident chunk's time bounds are recomputed by
/// scanning its time column, so with spilling off a seal still reads every
/// stashed time. The drain then loads sealed chunks one at a time, so a large
/// drain (a frontier advance releasing a snapshot's worth of stash at once)
/// holds at most one chunk resident rather than the whole backlog.
struct ChunkedArm;

impl<T, O> UpsertStashArm<T, O> for ChunkedArm
where
    T: Timestamp + TotalOrder + Lattice + Sync,
    T: columnation::Columnation + columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: columnar::Columnar + Default + Ord + Clone + Send + Sync + 'static,
    for<'a> columnar::Ref<'a, O>: Ord + Copy,
{
    type Spine = FeedbackSpine<T>;
    type Batcher = UpsertChunkBatcher<T, O>;
    type Chunker = TimeMajorChunker<T, O>;

    fn new_batcher() -> Self::Batcher {
        Batcher::new(None, 0)
    }

    fn flush(
        updates: &mut Vec<UpsertUpdate<T, O>>,
        chunker: &mut Self::Chunker,
        batcher: &mut Self::Batcher,
    ) {
        flush_through(
            updates,
            chunker,
            |(key, ts, diff), raw| raw.push_into(&((ts.clone(), key), ts, diff)),
            |chunk| batcher.push_into(ColumnChunk::from_body(chunk)),
        );
    }

    async fn drain(
        sealed: Vec<UpsertChunk<T, O>>,
        ineligible: &mut Vec<UpsertUpdate<T, O>>,
        output_handle: &UpsertOutputHandle<T>,
        output_cap: &Capability<T>,
        persist_upper: &Antichain<T>,
        trace: &mut TraceAgent<Self::Spine>,
        worker_id: usize,
        source_id: GlobalId,
    ) -> DrainStats {
        drain_sealed_input_chunked(
            sealed.into_iter().map(ColumnChunk::into_body),
            ineligible,
            output_handle,
            output_cap,
            persist_upper,
            trace,
            worker_id,
            source_id,
        )
        .await
    }
}

/// The paged flavor: [`UpsertPagedBatcher`] stash, `ValRowSpine` feedback
/// arrangement, cursor drain. Cold chains page out of RSS through the
/// storage-owned column pager, captured once per batcher at construction.
struct PagedArm;

impl<T, O> UpsertStashArm<T, O> for PagedArm
where
    T: Timestamp + TotalOrder + Lattice + Sync,
    T: columnation::Columnation + columnar::Columnar + Default,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    O: columnar::Columnar + Default + Ord + Clone + Send + Sync + 'static,
    for<'a> columnar::Ref<'a, O>: Ord + Copy,
{
    type Spine = FundedValRowSpine<UpsertKey, T, Diff>;
    type Batcher = UpsertPagedBatcher<T, O>;
    type Chunker = UpsertChunker<T, O>;

    fn new_batcher() -> Self::Batcher {
        let mut batcher: UpsertPagedBatcher<T, O> = Batcher::new(None, 0);
        batcher.set_pager(crate::upsert::upsert_stash_pager::pager());
        batcher
    }

    fn flush(
        updates: &mut Vec<UpsertUpdate<T, O>>,
        chunker: &mut Self::Chunker,
        batcher: &mut Self::Batcher,
    ) {
        flush_through(
            updates,
            chunker,
            |update, raw| raw.push_into(&update),
            // The pager's chains are edge containers, so the body goes back
            // onto one.
            |chunk| batcher.push_into(Column::from(chunk)),
        );
    }

    async fn drain(
        sealed: Vec<Column<UpsertUpdate<T, O>>>,
        ineligible: &mut Vec<UpsertUpdate<T, O>>,
        output_handle: &UpsertOutputHandle<T>,
        output_cap: &Capability<T>,
        persist_upper: &Antichain<T>,
        trace: &mut TraceAgent<Self::Spine>,
        worker_id: usize,
        source_id: GlobalId,
    ) -> DrainStats {
        drain_sealed_input_paged(
            sealed,
            ineligible,
            output_handle,
            output_cap,
            persist_upper,
            trace,
            worker_id,
            source_id,
        )
        .await
    }
}

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
struct DrainStats {
    /// Number of eligible entries probed against the feedback trace.
    eligible: u64,
    /// Number of probed entries for which a prior value was found.
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
async fn drain_sealed_input_chunked<T, O>(
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
async fn drain_sealed_input_paged<T, O>(
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

#[cfg(test)]
mod test {
    //! No test drives errors through the persist feedback, so every harness below closes
    //! the error input by dropping its handle.

    use mz_ore::cast::CastLossy;
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

    /// `ts` refined to its `sub`-th subtime.
    fn sub_ts(ts: u64, sub: u64) -> Ts {
        use timely::progress::PathSummary;
        let sub = (0..sub).fold(Subtime::minimum(), |acc, _| {
            Subtime::least_summary()
                .results_in(&acc)
                .expect("no overflow")
        });
        (MzTimestamp::new(ts), sub)
    }

    fn key(k: i64) -> UpsertKey {
        UpsertKey::from_key(Ok(&Row::pack_slice(&[Datum::Int64(k)])))
    }

    fn row(k: i64, v: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(k), Datum::Int64(v)])
    }

    // Runs the test body once per stash flavor and asserts the two flavors
    // produce identical (consolidated) output, so every scenario covers both
    // operator arms. Returns one flavor's output for the caller's own
    // expected-value assertion.
    macro_rules! upsert_test {
        (|$input:ident, $persist:ident, $worker:ident| $body:block) => {
            upsert_test!(|$input, $persist, $worker, _registry| $body)
        };
        // `$registry` holds the operator's upsert metrics, for `$body` to read.
        (|$input:ident, $persist:ident, $worker:ident, $registry:ident| $body:block) => {{
            let run = |flavor: UpsertStashFlavor| {
                let $registry = MetricsRegistry::new();
                let output_handle = timely::execute_directly(move |$worker| {
                    let (mut $input, mut $persist, output_handle) = $worker
                        .dataflow::<MzTimestamp, _, _>(|scope| {
                            scope.scoped::<Ts, _, _>("upsert", |scope| {
                                let (input_handle, input) = scope.new_input();
                                let (persist_handle, persist_input) = scope.new_input();
                                let (_persist_err_handle, persist_err_input) = scope.new_input();
                                let source_id = GlobalId::User(0);

                                let upsert_defs = UpsertMetricDefs::register_with(&$registry);
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
                                    flavor,
                                    input.as_collection(),
                                    vec![0],
                                    Antichain::from_elem(Timestamp::minimum()),
                                    persist_input.as_collection(),
                                    persist_err_input.as_collection(),
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
            };

            let paged = run(UpsertStashFlavor::Paged);
            let chunked = run(UpsertStashFlavor::Chunked);
            assert_eq!(paged, chunked, "stash flavors must produce equal output");
            chunked
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

            persist.send((value1, new_ts(0), Diff::ONE));
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
            persist.send((val_a.clone(), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((key_low, Some(Ok(val_b.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.send((val_b.clone(), new_ts(1), Diff::ONE));
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

    /// A source tail spanning several timestamps ahead of persist drains one
    /// timestamp per persist advance, each against the prior round's output.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn source_tail_ahead_of_persist() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(7);
            let (v0, v1, v2) = (row(7, 0), row(7, 1), row(7, 2));

            input.send_batch(&mut vec![
                ((k, Some(Ok(v0.clone())), 1), new_ts(0), Diff::ONE),
                ((k, Some(Ok(v1.clone())), 2), new_ts(1), Diff::ONE),
                ((k, Some(Ok(v2.clone())), 3), new_ts(2), Diff::ONE),
            ]);
            input.advance_to(new_ts(3));
            worker.step();

            // The operator reads the persist frontier when input progress
            // wakes it, so each round advances both.
            persist.send((v0.clone(), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();
            input.advance_to(new_ts(4));
            worker.step();

            persist.send((v0, new_ts(1), Diff::MINUS_ONE));
            persist.send((v1.clone(), new_ts(1), Diff::ONE));
            persist.advance_to(new_ts(2));
            worker.step();
            input.advance_to(new_ts(5));
            worker.step();

            persist.send((v1, new_ts(2), Diff::MINUS_ONE));
            persist.send((v2, new_ts(2), Diff::ONE));
            persist.advance_to(new_ts(3));
            worker.step();
            input.advance_to(new_ts(6));
            worker.step();
        });

        let (v0, v1, v2) = (row(7, 0), row(7, 1), row(7, 2));
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(v0.clone()), new_ts(0), Diff::ONE),
            (Ok(v0), new_ts(1), Diff::MINUS_ONE),
            (Ok(v1.clone()), new_ts(1), Diff::ONE),
            (Ok(v1), new_ts(2), Diff::MINUS_ONE),
            (Ok(v2), new_ts(2), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// A tail that arrives over several input frontier advances, so over
    /// several batcher chains, drains in time order per key.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn source_tail_across_rounds() {
        let actual = upsert_test!(|input, persist, worker| {
            let (ka, kb) = (key(1), key(2));
            let (a0, a2, b1, b2) = (row(1, 0), row(1, 2), row(2, 1), row(2, 2));

            input.send(((ka, Some(Ok(a0.clone())), 1), new_ts(0), Diff::ONE));
            input.advance_to(new_ts(1));
            worker.step();
            input.send(((kb, Some(Ok(b1.clone())), 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            input.send_batch(&mut vec![
                ((ka, Some(Ok(a2.clone())), 3), new_ts(2), Diff::ONE),
                ((kb, Some(Ok(b2.clone())), 4), new_ts(2), Diff::ONE),
            ]);
            input.advance_to(new_ts(3));
            worker.step();

            // The operator reads the persist frontier when input progress
            // wakes it, so each round advances both.
            persist.send((a0.clone(), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();
            input.advance_to(new_ts(4));
            worker.step();

            persist.send((b1.clone(), new_ts(1), Diff::ONE));
            persist.advance_to(new_ts(2));
            worker.step();
            input.advance_to(new_ts(5));
            worker.step();

            persist.send((a0, new_ts(2), Diff::MINUS_ONE));
            persist.send((a2, new_ts(2), Diff::ONE));
            persist.send((b1, new_ts(2), Diff::MINUS_ONE));
            persist.send((b2, new_ts(2), Diff::ONE));
            persist.advance_to(new_ts(3));
            worker.step();
            input.advance_to(new_ts(6));
            worker.step();
        });

        let (a0, a2, b1, b2) = (row(1, 0), row(1, 2), row(2, 1), row(2, 2));
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(a0.clone()), new_ts(0), Diff::ONE),
            (Ok(a0), new_ts(2), Diff::MINUS_ONE),
            (Ok(a2), new_ts(2), Diff::ONE),
            (Ok(b1.clone()), new_ts(1), Diff::ONE),
            (Ok(b1), new_ts(2), Diff::MINUS_ONE),
            (Ok(b2), new_ts(2), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// Two updates for one key at a time still open at the input frontier,
    /// arriving in separate operator rounds, consolidate to the later one.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn open_time_across_rounds_consolidates() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(1);
            // `send` buffers until `flush` or `advance_to`, and the operator
            // takes several steps to react to each change.
            input.advance_to(new_ts(1));
            input.send(((k, Some(Ok(row(1, 1))), 1), new_ts(1), Diff::ONE));
            input.flush();
            for _ in 0..5 {
                worker.step();
            }
            input.send(((k, Some(Ok(row(1, 2))), 2), new_ts(1), Diff::ONE));
            input.flush();
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(2));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(new_ts(1));
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(3));
            for _ in 0..5 {
                worker.step();
            }
        });
        let expected: Vec<(Result<Row, DataflowError>, _, _)> =
            vec![(Ok(row(1, 2)), new_ts(1), Diff::ONE)];
        assert_eq!(actual, expected);
    }

    /// Two updates for one key at a time the source sent ahead of the input
    /// frontier, arriving in separate operator rounds, consolidate to the
    /// later one.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn time_ahead_of_frontier_across_rounds_consolidates() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(1);
            input.send(((k, Some(Ok(row(1, 1))), 1), new_ts(1), Diff::ONE));
            input.flush();
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(1));
            for _ in 0..5 {
                worker.step();
            }
            input.send(((k, Some(Ok(row(1, 2))), 2), new_ts(1), Diff::ONE));
            input.flush();
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(2));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(new_ts(1));
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(3));
            for _ in 0..5 {
                worker.step();
            }
        });
        let expected: Vec<(Result<Row, DataflowError>, _, _)> =
            vec![(Ok(row(1, 2)), new_ts(1), Diff::ONE)];
        assert_eq!(actual, expected);
    }

    /// An input frontier inside a timestamp finalizes its earlier subtimes,
    /// which drain one persist advance at a time.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn refined_input_frontier_drains_finalized_subtimes() {
        let actual = upsert_test!(|input, persist, worker| {
            let (a, b) = (row(1, 1), row(2, 1));
            input.send(((key(1), Some(Ok(a.clone())), 1), sub_ts(1, 0), Diff::ONE));
            input.send(((key(2), Some(Ok(b)), 2), sub_ts(1, 1), Diff::ONE));
            input.advance_to(sub_ts(1, 2));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(sub_ts(1, 0));
            for _ in 0..5 {
                worker.step();
            }
            persist.send((a, sub_ts(1, 0), Diff::ONE));
            persist.advance_to(sub_ts(1, 1));
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(3));
            for _ in 0..5 {
                worker.step();
            }
        });
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(row(1, 1)), sub_ts(1, 0), Diff::ONE),
            (Ok(row(2, 1)), sub_ts(1, 1), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// Data far ahead of the frontier stays stashed through earlier rounds
    /// and drains once persist reaches it.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn far_ahead_data_drains() {
        let actual = upsert_test!(|input, persist, worker| {
            input.send(((key(1), Some(Ok(row(1, 1))), 1), new_ts(2), Diff::ONE));
            input.send(((key(2), Some(Ok(row(2, 1))), 2), new_ts(5000), Diff::ONE));
            input.advance_to(new_ts(5001));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(new_ts(2));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(new_ts(5000));
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(5002));
            for _ in 0..5 {
                worker.step();
            }
        });
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(row(1, 1)), new_ts(2), Diff::ONE),
            (Ok(row(2, 1)), new_ts(5000), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// An update at a later subtime of the persist frontier's timestamp is
    /// extracted with the eligible one, pushed back, and drained once persist
    /// reaches it.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn later_subtime_is_pushed_back_and_drained() {
        let actual = upsert_test!(|input, persist, worker, registry| {
            let (k, a, b) = (key(1), row(1, 1), row(1, 2));
            input.send(((k, Some(Ok(a.clone())), 1), sub_ts(1, 0), Diff::ONE));
            input.send(((k, Some(Ok(b)), 2), sub_ts(1, 1), Diff::ONE));
            input.advance_to(new_ts(2));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(sub_ts(1, 0));
            for _ in 0..5 {
                worker.step();
            }
            persist.send((a, sub_ts(1, 0), Diff::ONE));
            persist.advance_to(sub_ts(1, 1));
            for _ in 0..5 {
                worker.step();
            }
            input.advance_to(new_ts(3));
            for _ in 0..5 {
                worker.step();
            }

            let restashed: u64 = registry
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
                .flat_map(|family| family.get_metric())
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum();
            assert_eq!(restashed, 1, "the later subtime is pushed back once");
        });

        let (a, b) = (row(1, 1), row(1, 2));
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = vec![
            (Ok(a.clone()), sub_ts(1, 0), Diff::ONE),
            (Ok(a), sub_ts(1, 1), Diff::MINUS_ONE),
            (Ok(b), sub_ts(1, 1), Diff::ONE),
        ];
        assert_eq!(actual, expected);
    }

    /// A source tail far ahead of persist, drained one timestamp per persist
    /// advance, extracts only the timestamp being drained. Extracting the
    /// whole tail each round would push it back every time, making catch-up
    /// quadratic in the tail.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn tail_catch_up_restashes_nothing() {
        const TIMESTAMPS: u64 = 20;
        const PER_TS: i64 = 10;
        let actual = upsert_test!(|input, persist, worker, registry| {
            let mut offset = 0;
            for t in 0..TIMESTAMPS {
                let mut batch = Vec::new();
                for k in 0..PER_TS {
                    offset += 1;
                    let v = row(k, i64::from(offset));
                    batch.push(((key(k), Some(Ok(v)), offset), new_ts(t), Diff::ONE));
                }
                input.send_batch(&mut batch);
                input.advance_to(new_ts(t + 1));
                worker.step();
            }

            // The operator reads the persist frontier when input progress
            // wakes it, so each round advances both.
            for t in 1..=TIMESTAMPS {
                persist.advance_to(new_ts(t));
                worker.step();
                input.advance_to(new_ts(TIMESTAMPS + t));
                worker.step();
            }

            let restashed: u64 = registry
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
                .flat_map(|family| family.get_metric())
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum();
            assert_eq!(restashed, 0, "updates pushed back during catch-up");
        });

        // No feedback reaches persist, so every update drains as an insert.
        let inserts = actual.iter().filter(|(_, _, diff)| *diff == Diff::ONE);
        assert_eq!(
            u64::cast_from(inserts.count()),
            TIMESTAMPS * PER_TS.unsigned_abs()
        );
    }

    /// A persist frontier that jumps into the middle of a stashed tail drops
    /// the entries it passed, drains the one it lands on, and keeps the rest
    /// in the stash for later rounds without extracting them.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn persist_jump_into_tail() {
        let actual = upsert_test!(|input, persist, worker, registry| {
            for t in 2..=8 {
                let k = i64::try_from(t).expect("small");
                input.send(((key(k), Some(Ok(row(k, k))), 1), new_ts(t), Diff::ONE));
            }
            input.advance_to(new_ts(10));
            for _ in 0..5 {
                worker.step();
            }
            persist.advance_to(new_ts(5));
            for _ in 0..5 {
                worker.step();
            }
            for t in 6..=9 {
                persist.advance_to(new_ts(t));
                input.advance_to(new_ts(10 + t));
                for _ in 0..5 {
                    worker.step();
                }
            }

            let restashed: u64 = registry
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
                .flat_map(|family| family.get_metric())
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum();
            assert_eq!(restashed, 0, "the kept tail was extracted and pushed back");
        });
        let expected: Vec<(Result<Row, DataflowError>, _, _)> = (5..=8)
            .map(|t| {
                let k = i64::try_from(t).expect("small");
                (Ok(row(k, k)), new_ts(t), Diff::ONE)
            })
            .collect();
        assert_eq!(actual, expected);
    }

    /// A tail of several timestamps, each larger than one committed chunk,
    /// drains correctly when the chunked stash spills, and each seal passes
    /// the spilled chunks wholly past its upper through. The paged flavor
    /// stays resident and is the reference.
    ///
    /// Each timestamp's 25k updates fill one to two committed chunks. Loading
    /// commits each chunk once and ages it into a compressed body once, and
    /// a seal re-commits at most the two pieces of the chunk it splits, so
    /// the pool sees about 3 inserts per timestamp (34 here). A seal that
    /// rewrote every remaining chunk would add about one per remaining
    /// timestamp per round, roughly 100 more over 12 timestamps.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn spilled_tail_drains_across_split_chunks() {
        use mz_ore::pool::Pool;
        use mz_timely_util::columnar::chunk::set_spill_override;

        let pool = Pool::new().expect("pool creation");
        set_spill_override(Some(pool.clone()));

        const TIMESTAMPS: u64 = 12;
        const KEYS: i64 = 25_000;
        let actual = upsert_test!(|input, persist, worker, registry| {
            let mut batch = Vec::new();
            let mut offset = 0;
            for t in 0..TIMESTAMPS {
                for k in 0..KEYS {
                    offset += 1;
                    let v = row(k, i64::from(offset));
                    batch.push(((key(k), Some(Ok(v)), offset), new_ts(t), Diff::ONE));
                }
            }
            input.send_batch(&mut batch);
            input.advance_to(new_ts(TIMESTAMPS));
            for _ in 0..5 {
                worker.step();
            }
            for t in 1..=TIMESTAMPS {
                persist.advance_to(new_ts(t));
                input.advance_to(new_ts(TIMESTAMPS + t));
                for _ in 0..5 {
                    worker.step();
                }
            }

            let restashed: u64 = registry
                .gather()
                .iter()
                .filter(|family| family.name() == "mz_storage_upsert_restashed_total")
                .flat_map(|family| family.get_metric())
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum();
            assert_eq!(restashed, 0, "updates pushed back during catch-up");
        });

        set_spill_override(None);
        let inserts = pool.stats().inserts;
        assert!(inserts > 0, "chunks should have spilled through the pool");
        assert!(
            inserts <= 5 * TIMESTAMPS,
            "{inserts} pool inserts: seals rewrote chunks past their upper"
        );

        // No feedback reaches persist, so every update drains as an insert.
        let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = Vec::new();
        let mut offset = 0;
        for t in 0..TIMESTAMPS {
            for k in 0..KEYS {
                offset += 1;
                expected.push((Ok(row(k, i64::from(offset))), new_ts(t), Diff::ONE));
            }
        }
        let mut actual = actual;
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
    }

    /// Drives an arm's stash batcher through a pseudo-random sequence of
    /// pushes, seals and push-backs, checking it against a model that maps
    /// each `(time, key)` to the greatest order pushed for it, which is what
    /// consolidation keeps. Every seal must extract exactly the model's
    /// entries below the upper, and the least time held, the batcher's
    /// frontier plus the pushed-back entries, must match the model's.
    fn stash_matches_model<A>(
        decode: impl Fn(Vec<<A::Batcher as Batcher>::Output>) -> Vec<(UpsertKey, Ts, i32)>,
    ) where
        A: UpsertStashArm<Ts, i32>,
    {
        use std::collections::BTreeMap;

        let mut batcher = A::new_batcher();
        let mut chunker = A::Chunker::default();
        let mut model: BTreeMap<(Ts, UpsertKey), i32> = BTreeMap::new();
        let mut state: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = move |n: u64| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            (state >> 33) % n
        };
        let mut upper = sub_ts(0, 0);
        let mut order = 0;
        for _ in 0..2000 {
            match next(3) {
                0 | 1 => {
                    // Source input arrives at or past the seal upper.
                    let mut updates = Vec::new();
                    for _ in 0..next(20) {
                        let outer = u64::from(upper.0) + next(40);
                        let ts = std::cmp::max(sub_ts(outer, next(3)), upper);
                        let k = key(i64::try_from(next(8)).expect("small"));
                        order += 1;
                        let kept = model.entry((ts, k)).or_insert(order);
                        *kept = std::cmp::max(*kept, order);
                        let diff = UpsertDiff {
                            from_time: order,
                            value: None,
                        };
                        updates.push((k, ts, diff));
                    }
                    A::flush(&mut updates, &mut chunker, &mut batcher);
                }
                _ => {
                    let outer = u64::from(upper.0) + next(5);
                    upper = std::cmp::max(sub_ts(outer, next(3)), upper);
                    let (sealed, _description) = batcher.seal(Antichain::from_elem(upper));
                    let mut least = batcher.frontier().to_owned();
                    let mut extracted: Vec<_> = decode(sealed)
                        .into_iter()
                        .map(|(k, ts, order)| (ts, k, order))
                        .collect();
                    extracted.sort();
                    let mut expected = Vec::new();
                    while let Some(entry) = model.first_entry()
                        && entry.key().0 < upper
                    {
                        let ((ts, k), order) = entry.remove_entry();
                        expected.push((ts, k, order));
                    }
                    assert_eq!(extracted, expected, "seal at {upper:?}");
                    // Push some back, as the drain does with ineligible updates.
                    let mut pushed_back = Vec::new();
                    for (ts, k, order) in extracted {
                        if next(3) == 0 {
                            model.insert((ts, k), order);
                            let diff = UpsertDiff {
                                from_time: order,
                                value: None,
                            };
                            pushed_back.push((k, ts, diff));
                            least.insert(ts);
                        }
                    }
                    A::flush(&mut pushed_back, &mut chunker, &mut batcher);
                    assert_eq!(
                        least.as_option(),
                        model.keys().next().map(|(ts, _)| ts),
                        "least time held"
                    );
                }
            }
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn paged_stash_matches_model() {
        stash_matches_model::<PagedArm>(|sealed| {
            use columnar::Index;
            let mut out = Vec::new();
            for column in &sealed {
                for (k, ts, diff) in column.borrow().into_index_iter() {
                    let diff = <UpsertDiff<i32> as columnar::Columnar>::into_owned(diff);
                    out.push((
                        *k,
                        <Ts as columnar::Columnar>::into_owned(ts),
                        diff.from_time,
                    ));
                }
            }
            out
        });
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn chunked_stash_matches_model() {
        stash_matches_model::<ChunkedArm>(|sealed| {
            use columnar::{Index, Len};
            let mut out = Vec::new();
            for chunk in sealed {
                let body = chunk.into_body();
                let view = body.borrow();
                for index in 0..view.len() {
                    let ((_, k), ts, diff) = view.get(index);
                    let diff = <UpsertDiff<i32> as columnar::Columnar>::into_owned(diff);
                    out.push((
                        *k,
                        <Ts as columnar::Columnar>::into_owned(ts),
                        diff.from_time,
                    ));
                }
            }
            assert!(
                out.is_sorted_by_key(|(k, ts, _)| (*ts, *k)),
                "sealed chunks are time-major"
            );
            out
        });
    }

    #[mz_ore::test]
    fn seal_upper_stops_past_persist_upper() {
        use timely::progress::PathSummary;
        let at = |t: u64, s: u64| {
            let sub = (0..s).fold(Subtime::minimum(), |acc, _| {
                Subtime::least_summary()
                    .results_in(&acc)
                    .expect("no overflow")
            });
            Antichain::from_elem((MzTimestamp::new(t), sub))
        };
        // The next outer timestamp, when the input frontier is past it.
        assert_eq!(seal_upper(&at(5, 3), &at(9, 0)), at(6, 0));
        // Capped at the input frontier, which may sit inside the outer timestamp.
        assert_eq!(seal_upper(&at(5, 0), &at(5, 7)), at(5, 7));
        assert_eq!(seal_upper(&at(5, 0), &at(6, 0)), at(6, 0));
        // An empty input frontier caps nothing. Times past the next outer
        // timestamp stay stashed, as they cannot be eligible this round.
        assert_eq!(seal_upper(&at(5, 0), &Antichain::new()), at(6, 0));
        // At the maximum outer timestamp there is no next one to stop at.
        assert_eq!(
            seal_upper(&at(u64::MAX, 2), &at(u64::MAX, 7)),
            at(u64::MAX, 7)
        );
        assert_eq!(
            seal_upper(&at(u64::MAX, 2), &Antichain::new()),
            Antichain::new()
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn rehydration_then_update() {
        let actual = upsert_test!(|input, persist, worker| {
            let k = key(42);
            let old_val = row(42, 100);
            let new_val = row(42, 200);

            persist.send((old_val, new_ts(0), Diff::ONE));
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
    fn drain_crosses_probe_window() {
        // More distinct keys than one drain probe window holds, all eligible
        // at the same timestamp. The records fit one sealed chunk (the ship
        // threshold is ~2 MiB), so the drain must close a probe window
        // mid-chunk and carry keys correctly across the boundary.
        const KEYS: i64 = 1500;
        let actual = upsert_test!(|input, persist, worker| {
            for k in 0..KEYS {
                persist.send((row(k, k), new_ts(0), Diff::ONE));
            }
            persist.advance_to(new_ts(1));
            worker.step();

            for k in 0..KEYS {
                input.send(((key(k), Some(Ok(row(k, k + 1))), 1), new_ts(1), Diff::ONE));
            }
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = Vec::new();
        for k in 0..KEYS {
            expected.push((Ok(row(k, k)), new_ts(1), Diff::MINUS_ONE));
            expected.push((Ok(row(k, k + 1)), new_ts(1), Diff::ONE));
        }
        let mut actual_sorted = actual;
        actual_sorted.sort();
        expected.sort();
        assert_eq!(actual_sorted, expected);
    }

    /// The probe-window scenario with committed chunks forced through a
    /// private buffer pool, so the chunked flavor's seal and drain read
    /// spilled bodies back through the pool codec rather than resident
    /// memory. The override is thread-scoped and `execute_directly` runs the
    /// worker on this thread, so it reaches the operator's batchers. The
    /// paged flavor (which the harness also runs) routes through the column
    /// pager rather than the chunk override, so it stays resident and serves
    /// as the reference.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn drain_reads_spilled_chunks() {
        use mz_ore::pool::Pool;
        use mz_timely_util::columnar::chunk::set_spill_override;

        let pool = Pool::new().expect("pool creation");
        set_spill_override(Some(pool.clone()));

        const KEYS: i64 = 1500;
        let actual = upsert_test!(|input, persist, worker| {
            for k in 0..KEYS {
                persist.send((row(k, k), new_ts(0), Diff::ONE));
            }
            persist.advance_to(new_ts(1));
            worker.step();

            for k in 0..KEYS {
                input.send(((key(k), Some(Ok(row(k, k + 1))), 1), new_ts(1), Diff::ONE));
            }
            input.advance_to(new_ts(2));
            worker.step();
            persist.advance_to(new_ts(2));
            worker.step();
        });

        set_spill_override(None);
        assert!(
            pool.stats().inserts > 0,
            "chunks should have spilled through the pool"
        );

        let mut expected: Vec<(Result<Row, DataflowError>, _, _)> = Vec::new();
        for k in 0..KEYS {
            expected.push((Ok(row(k, k)), new_ts(1), Diff::MINUS_ONE));
            expected.push((Ok(row(k, k + 1)), new_ts(1), Diff::ONE));
        }
        let mut actual_sorted = actual;
        actual_sorted.sort();
        expected.sort();
        assert_eq!(actual_sorted, expected);
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
            persist.send((val, new_ts(0), Diff::ONE));
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

            persist.send((old_val.clone(), new_ts(0), Diff::ONE));
            persist.send((old_val, new_ts(0), Diff::MINUS_ONE));
            persist.send((new_val, new_ts(0), Diff::ONE));
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
            persist.send((val_a.clone(), new_ts(0), Diff::ONE));
            persist.advance_to(new_ts(1));
            worker.step();

            input.send(((k, None, 2), new_ts(1), Diff::ONE));
            input.advance_to(new_ts(2));
            worker.step();
            persist.send((val_a, new_ts(1), Diff::MINUS_ONE));
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
            persist.send((val.clone(), new_ts(0), Diff::ONE));
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
    /// The drain DROPS such already-persisted data (it satisfies
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
        for flavor in [UpsertStashFlavor::Paged, UpsertStashFlavor::Chunked] {
            let (frontier, emitted) = run_below_upper_scenario_v2(flavor);

            // The below-upper data is discarded (no output) and the output
            // frontier is not pinned below the shard upper (10); it advances
            // to the input upper (11), matching v1's behavior.
            assert!(
                emitted.is_empty(),
                "below-upper data should be dropped, not emitted; got {emitted:?} ({flavor:?})"
            );
            assert_eq!(
                frontier,
                vec![new_ts(11)],
                "v2 output frontier should advance to the input upper, not pin below \
                 persist_upper ({flavor:?})"
            );
            assert!(
                frontier[0] >= new_ts(10),
                "v2 output frontier {frontier:?} should reach at least persist_upper (10) \
                 ({flavor:?})"
            );
        }
    }

    /// Shared driver for the lagging-replacement scenario against v2. Returns
    /// `(output_frontier, consolidated_emitted_updates)`.
    fn run_below_upper_scenario_v2(
        flavor: UpsertStashFlavor,
    ) -> (Vec<Ts>, Vec<(Result<Row, DataflowError>, Ts, Diff)>) {
        use timely::dataflow::operators::Probe;

        let (frontier, capture) = timely::execute_directly(move |worker| {
            let (mut input, mut persist, probe, capture) =
                worker.dataflow::<MzTimestamp, _, _>(|scope| {
                    scope.scoped::<Ts, _, _>("upsert", |scope| {
                        let (input_handle, input) = scope.new_input();
                        let (persist_handle, persist_input) = scope.new_input();
                        let (_persist_err_handle, persist_err_input) = scope.new_input();
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
                            flavor,
                            input.as_collection(),
                            vec![0],
                            Antichain::from_elem(Timestamp::minimum()),
                            persist_input.as_collection(),
                            persist_err_input.as_collection(),
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
