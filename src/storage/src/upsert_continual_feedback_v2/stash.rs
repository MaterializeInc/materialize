// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The upsert-v2 source stash: the consolidating diff, the stash batcher
//! types, and the per-flavor [`UpsertStashArm`]s.

use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::agent::TraceAgent;
use differential_dataflow::trace::chunk::ChunkBatcher;
use differential_dataflow::trace::{Batcher, TraceReader};
use mz_repr::{Diff, GlobalId, Row};
use mz_row_spine::FundedValRowSpine;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::chunk::{ColumnChunk, StorageSpill};
use mz_timely_util::columnar::merge_batcher::ColumnMergeBatcher;
use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::operators::Capability;
use timely::order::TotalOrder;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use super::UpsertOutputHandle;
use super::drain::{DrainStats, drain_sealed_input_chunked, drain_sealed_input_paged};
use super::feedback::FeedbackSpine;
use crate::upsert::UpsertKey;

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
pub(super) struct UpsertDiff<O> {
    pub(super) from_time: O,
    pub(super) value: Option<Row>,
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
/// [`UpsertSourceTime`](crate::upsert::UpsertSourceTime)).
pub(super) type UpsertUpdate<T, O> = (UpsertKey, T, UpsertDiff<O>);

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
pub(super) type TimeMajorUpdate<T, O> = ((T, UpsertKey), T, UpsertDiff<O>);

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

/// The start of the `mz_repr::Timestamp` after `persist_upper`'s, capped at
/// `input_upper` so no unfinalized time is extracted.
///
/// Only entries at `persist_upper` are eligible in a round, and the drain
/// returns every extracted entry past `persist_upper` as a resident `Vec`,
/// outside the buffer pool budget, to be re-pushed through the chunker.
/// Sealing at `input_upper` instead would do that for the whole backlog
/// between the two frontiers once per persist round trip, which is the entire
/// source tail while the operator catches up from behind.
pub(super) fn seal_upper<T>(
    persist_upper: &Antichain<T>,
    input_upper: &Antichain<T>,
) -> Antichain<T>
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
/// [`build_upsert_operator`](super::build_upsert_operator) holds the
/// flavor-independent operator loop and calls through this trait at the few
/// points where the flavors diverge.
pub(super) trait UpsertStashArm<T, O>
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
pub(super) struct ChunkedArm;

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
pub(super) struct PagedArm;

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
