// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See LICENSE in this directory.

//! Sorted, consolidated runs of updates, and operators over sequences of them.
//!
//! A [`Chunk`] is a consolidated, sorted run of `(data, time, diff)` updates.
//! A sequence of chunks is also expected to be consolidated and sorted.
//!
//! The [`Chunk`] trait exposes whole-chunk operations, so that the implementor can internally divert to their best implementations, with amortized overhead.
//! Each operation is invoked as if "streaming", providing input and output queues.
//! An implementor is expected to drain as much as possible of the inputs, and any chunk written to the output is "committed" and likely to be shipped onward.
//!
//! # Wiring a `Chunk` into an arrangement
//!
//! Implementing [`Chunk`] for a type `C` is the only bespoke code needed; three aliases then expand into a full trace:
//!
//! * [`ChunkBatcher<Chu, C>`](ChunkBatcher): the merge batcher.
//! * [`ChunkBuilder<C>`](ChunkBuilder): the batch builder.
//! * [`ChunkSpine<C>`](ChunkSpine): the trace, a spine of `Rc`-shared batches.
//!
//! These aliases use [`ChunkBatch`], [`ChunkMerger`], [`ChunkBatchMerger`], and [`ChunkBatchBuilder`] for batch formation and trace maintenance.
//!
//! In this vendored module, the enclosing Materialize adapter supplies the trace
//! interfaces. Differential's arrangement wiring, reference chunk backend, and
//! cursor implementations are not included here.
//!
//! # Bounded footprint
//!
//! There is a `TARGET` associated constant that signals the intended chunk size.
//! The constant should be chosen large enough to amortize overheads, but small enough that per-chunk work does not "stall" the system when invoked.
//! The implementor is trusted to make a reasonable choice here.
//!
//! The [`Chunk::settle`] method "settles" sequences of chunks, and is called as chunks are no longer expected to be needed in the near future.
//! The implementor should ensure the chunks are "graded", in that the sequence of chunks are all at most `TARGET` in size, any two in order sum to strictly more than `TARGET`.
//! This is also an opportunity to compress data, or spill to disk or cloud storage.
//!
//! Producers settle their committed output as they go, keeping the active, unsettled chunk set small.
//! Implementors must keep [`len`](Chunk::len) cheap even when a chunk's body is paged out.

use std::collections::VecDeque;

use super::spine::SpineBatch;
use differential_dataflow::lattice::Lattice;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

/// A non-empty, bounded, consolidated, sorted sequence of `(data, time, diff)`.
///
/// An implementor gains access to types and trait implementations that provide
/// batch formation and trace maintenance with no additional effort.
///
/// The necessary implementations are either "data" or "metadata" operations.
/// The "data" operations transform lists of chunks, are expected to do roughly
/// "one chunk's worth" of work at a time; they can afford to compress and page.
/// The "metadata" operations provide chunk information, and should be lightweight.
///
/// The trait exposes only time, which trace maintenance needs.
/// Reading a chunk's contents is separate from trace maintenance.
pub trait Chunk: Sized + Clone {
    /// Retained I/O state for one chunk operation. It must not borrow the trace.
    type Pending: Default;

    /// Prepare operation state from resident metadata before reading any input.
    fn pending_for(_left: &[Self], _right: &[Self]) -> Self::Pending {
        Self::Pending::default()
    }

    /// The timestamp type of the chunk's updates.
    ///
    /// Trace maintenance uses time to describe intervals and to advance and compact updates.
    type Time: Lattice + timely::progress::Timestamp;

    /// The intended maximum chunk size.
    const TARGET: usize;

    /// The number of updates in the chunk.
    fn len(&self) -> usize;

    /// Merge the fronts of two input deques through their shared horizon.
    ///
    /// Both deques are non-empty (the caller guarantees it). The two queues are both
    /// the heads of lists of chunks, and the implementor should only merge through the
    /// least last `(key, val, time)` update, or risk emitting an unconsolidated
    /// output chunk.
    ///
    /// When a chunk cannot be completely retired, perhaps it had the larger last update,
    /// it should be rewritten as a new chunk and pushed back to the front of the queue.
    /// The invocation is expected to consume at least one of its inputs, and the harness
    /// may continually re-invoke if this doesn't happen.
    ///
    /// A merge concludes when the harness sees that either input is now empty, at which
    /// point it appends the queue to the output without the method's assistance.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>);

    /// Partition `input` updates into `keep` (greater or equal `frontier`) or not (`ship`).
    ///
    /// An implementation should yield with some frequency to allow the output to "settle".
    /// The harness may guard against this, but it prefers to provide as much context as it
    /// can in order to allow broader chunk fusion where needed.
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<Self::Time>,
        residual: &mut Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    );

    /// Advance times by `frontier` producing consolidated chunks.
    ///
    /// An output for `(key, val)` should generally not be produced until a later pair
    /// is observed, or `done` is set, to ensure the output chunks are consolidated.
    /// Incomplete work can be pushed back to the front of `input`.
    ///
    /// On `done` a single `(key, val)` group may span the whole input; advancing and
    /// consolidating it should cost time linear in its size, not quadratic.
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<Self::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    );

    /// Reshape `input` to a sequence that maintains the "grading" structural invariant.
    ///
    /// Specifically, the chunks in `output` should have a maximum size of `TARGET` and
    /// each adjacent pair should have lengths that sum to more than `TARGET`.
    /// This is also a good moment to consider compression or paging out the contents.
    /// When `done` is set the input must be moved to the output.
    ///
    /// This method may be called on already settled data, and should be efficient then.
    ///
    /// Implementors that want the standard maximal packing can delegate to the
    /// [`pack`] helper, supplying their layout's coalesce / split / commit closures.
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>);

    /// Poll a merge tick, retaining progress and registering a wakeup when blocked.
    fn poll_merge(
        _state: &mut Self::Pending,
        _cx: &mut std::task::Context<'_>,
        in1: &mut VecDeque<Self>,
        in2: &mut VecDeque<Self>,
        out: &mut VecDeque<Self>,
    ) -> std::task::Poll<()> {
        Self::merge(in1, in2, out);
        std::task::Poll::Ready(())
    }
    /// Poll timestamp advancement. `done` and `frontier` stay fixed while pending.
    fn poll_advance(
        _state: &mut Self::Pending,
        _cx: &mut std::task::Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<Self::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) -> std::task::Poll<()> {
        Self::advance(input, frontier, done, out);
        std::task::Poll::Ready(())
    }
    /// Poll extraction. The frontier and output queues stay fixed while pending.
    fn poll_extract(
        _state: &mut Self::Pending,
        _cx: &mut std::task::Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<Self::Time>,
        residual: &mut Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) -> std::task::Poll<()> {
        Self::extract(input, frontier, residual, keep, ship);
        std::task::Poll::Ready(())
    }
    /// Poll settlement, preserving both carry and output across suspension.
    fn poll_settle(
        _state: &mut Self::Pending,
        _cx: &mut std::task::Context<'_>,
        input: &mut VecDeque<Self>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) -> std::task::Poll<()> {
        Self::settle(input, done, out);
        std::task::Poll::Ready(())
    }
}

/// Maximal-packing driver an implementor's [`Chunk::settle`] may delegate to.
///
/// Holds a `carry` chunk under construction, grown by `combine` until it reaches
/// `TARGET` (then emitted) and emitted early when the next chunk can't be absorbed
/// without exceeding `TARGET`; over-sized chunks are peeled with `split`. Each
/// committed chunk is passed through `seal` (the compress / spill hook — use the
/// identity closure when there's nothing to do). The closures are the only
/// layout-specific pieces:
///
/// * `combine(&mut acc, next)` — append `next` onto `acc` (caller guarantees their
///   lengths sum to at most `TARGET`, and `next` follows `acc` in one sorted,
///   consolidated chain), so packing a run of small chunks stays linear.
/// * `split(chunk, n)` — the first `n` updates and the remaining `len - n`.
/// * `seal(chunk)` — commit a chunk (e.g. compress or spill); identity to keep it.
pub fn pack<C: Chunk>(
    input: &mut VecDeque<C>,
    done: bool,
    out: &mut VecDeque<C>,
    mut combine: impl FnMut(&mut C, C),
    mut split: impl FnMut(C, usize) -> (C, C),
    mut seal: impl FnMut(C) -> C,
) {
    let mut carry: Option<C> = None;
    while let Some(chunk) = input.pop_front() {
        match carry.take() {
            None => pack_absorb(chunk, &mut carry, out, &mut split, &mut seal),
            Some(mut c) if c.len() + chunk.len() <= C::TARGET => {
                // Combines into one legal chunk; coalesce in place.
                combine(&mut c, chunk);
                if c.len() == C::TARGET {
                    out.push_back(seal(c));
                } else {
                    carry = Some(c);
                }
            }
            Some(c) => {
                // `c` is maximal against this neighbour; emit it and absorb afresh.
                out.push_back(seal(c));
                pack_absorb(chunk, &mut carry, out, &mut split, &mut seal);
            }
        }
    }
    if let Some(c) = carry {
        if done {
            out.push_back(seal(c));
        } else {
            input.push_front(c);
        }
    }
}

/// Absorb `chunk` into an empty `carry` (a [`pack`] helper): pass a `TARGET` chunk
/// straight through (sealed), hold a smaller one as the new carry, or peel
/// `TARGET`-sized pieces off a larger one and carry the remainder.
fn pack_absorb<C, S, L>(
    chunk: C,
    carry: &mut Option<C>,
    out: &mut VecDeque<C>,
    split: &mut S,
    seal: &mut L,
) where
    C: Chunk,
    S: FnMut(C, usize) -> (C, C),
    L: FnMut(C) -> C,
{
    match chunk.len().cmp(&C::TARGET) {
        std::cmp::Ordering::Equal => out.push_back(seal(chunk)),
        std::cmp::Ordering::Less => *carry = Some(chunk),
        std::cmp::Ordering::Greater => {
            let mut rest = chunk;
            loop {
                let (head, tail) = split(rest, C::TARGET);
                out.push_back(seal(head));
                if tail.len() >= C::TARGET {
                    rest = tail;
                } else {
                    if tail.len() > 0 {
                        *carry = Some(tail);
                    }
                    break;
                }
            }
        }
    }
}

/// A batch: an ordered [`Chunk`] sequence whose concatenation is its updates.
pub struct ChunkBatch<C: Chunk> {
    /// Ordered, consolidated chunks; their concatenation is the batch.
    pub chunks: Vec<C>,
}

impl<C: Chunk> ChunkBatch<C> {
    /// Assemble a batch from ordered chunks.
    pub fn new(chunks: Vec<C>) -> Self {
        for chunk in &chunks {
            assert!(chunk.len() > 0, "ChunkBatch chunks must be non-empty");
        }
        ChunkBatch { chunks }
    }
}

impl<C: Chunk + 'static> SpineBatch for ChunkBatch<C>
where
    C::Time: timely::progress::Timestamp + Lattice + Ord,
{
    type Time = C::Time;
    type Merger = ChunkBatchMerger<C>;
    fn len(&self) -> usize {
        self.chunks.iter().map(C::len).sum()
    }
}

/// A merge-batcher [`Merger`](super::batcher::Merger)
/// over chains of [`Chunk`]s.
///
/// `merge` runs the whole-chain binary merger; `extract` splits by the seal frontier
/// using [`Chunk::extract`]. The batcher consolidates equal `(data, time)` updates
/// but does *not* advance times — time advancement is advance's job, handled later in
/// the trace. Both settle their output, since the batcher's chains want to be graded.
pub type ChunkBatcher<Chu, C> = super::batcher::MergeBatcher<Chu, ChunkMerger<C>, ChunkBuilder<C>>;

/// A spine of `Rc`-shared [`ChunkBatch`]s of type `C`: the trace type for `arrange`.
pub type ChunkSpine<C> = super::spine::Spine<std::rc::Rc<ChunkBatch<C>>>;

/// A [`ChunkBatch`] builder over chunks of type `C`, emitting `Rc`-shared updates.
pub type ChunkBuilder<C> = ChunkBatchBuilder<C>;

/// A merge-batcher [`Merger`](super::batcher::Merger)
/// over chains of [`Chunk`]s.
///
/// `merge` runs the whole-chain binary merger; `extract` splits by the seal frontier
/// using [`Chunk::extract`]. The batcher consolidates equal `(data, time)` updates
/// but does *not* advance times — time advancement is advance's job, handled later in
/// the trace. Both settle their output, since the batcher's chains want to be graded.
pub struct ChunkMerger<C: Chunk> {
    merge: Option<ChainMerge<C>>,
    extract: Option<ChainExtract<C>>,
}

enum ChainPhase {
    Choose,
    Merge,
    Settle(bool),
}
struct ChainMerge<C: Chunk> {
    a: VecDeque<C>,
    b: VecDeque<C>,
    staged: VecDeque<C>,
    settled: VecDeque<C>,
    io: C::Pending,
    phase: ChainPhase,
}
enum ExtractPhase {
    Choose,
    Extract,
    Keep(bool),
    Ship(bool),
}
struct ChainExtract<C: Chunk> {
    input: VecDeque<C>,
    keep: VecDeque<C>,
    ship: VecDeque<C>,
    kept: VecDeque<C>,
    shipped: VecDeque<C>,
    io: C::Pending,
    phase: ExtractPhase,
}
impl<C: Chunk> Default for ChunkMerger<C> {
    fn default() -> Self {
        Self {
            merge: None,
            extract: None,
        }
    }
}

impl<C> super::batcher::Merger for ChunkMerger<C>
where
    C: Chunk + Default + 'static,
    C::Time: Clone + timely::PartialOrder + 'static,
{
    type Chunk = C;
    type Time = C::Time;

    fn merge(&mut self, list1: Vec<C>, list2: Vec<C>, output: &mut Vec<C>, _stash: &mut Vec<C>) {
        // Settle the output after each merge, to maintain bounded active chunks.
        let mut in1: VecDeque<C> = list1.into();
        let mut in2: VecDeque<C> = list2.into();
        let (mut staged, mut settled) = (VecDeque::new(), VecDeque::new());
        while !in1.is_empty() && !in2.is_empty() {
            C::merge(&mut in1, &mut in2, &mut staged);
            C::settle(&mut staged, false, &mut settled);
        }
        // Append the non-empty tail from either input, settle as we go.
        for tail in in1.drain(..).chain(in2.drain(..)) {
            staged.push_back(tail);
            C::settle(&mut staged, false, &mut settled);
        }
        C::settle(&mut staged, true, &mut settled);
        output.extend(settled);
    }

    fn extract(
        &mut self,
        merged: Vec<C>,
        upper: AntichainRef<C::Time>,
        frontier: &mut Antichain<C::Time>,
        ship: &mut Vec<C>,
        kept: &mut Vec<C>,
        _stash: &mut Vec<C>,
    ) {
        // `extract` keeps updates greater-or-equal `upper` and ships the rest, folding
        // the lower envelope of kept times into `frontier`. Drive it a bounded amount
        // per call (≈ one input chunk) and `settle` each side as it accumulates, so
        // neither `keep` (retained across yields) nor `ship` (handed to the builder)
        // builds up unsettled in core. `settle` may withhold a sub-`TARGET` carry
        // between calls; the final `settle(done)` flushes it.
        let mut input: VecDeque<C> = merged.into();
        let (mut keep, mut shipped) = (VecDeque::new(), VecDeque::new());
        let (mut kept_q, mut shipped_q) = (VecDeque::new(), VecDeque::new());
        while !input.is_empty() {
            C::extract(&mut input, upper, frontier, &mut keep, &mut shipped);
            C::settle(&mut keep, false, &mut kept_q);
            C::settle(&mut shipped, false, &mut shipped_q);
        }
        C::settle(&mut keep, true, &mut kept_q);
        C::settle(&mut shipped, true, &mut shipped_q);
        kept.extend(kept_q);
        ship.extend(shipped_q);
    }

    fn poll_merge(
        &mut self,
        a: &mut Vec<C>,
        b: &mut Vec<C>,
        output: &mut Vec<C>,
        _stash: &mut Vec<C>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        use std::task::{Poll, ready};
        let work = self.merge.get_or_insert_with(|| ChainMerge {
            io: C::pending_for(a, b),
            a: std::mem::take(a).into(),
            b: std::mem::take(b).into(),
            staged: VecDeque::new(),
            settled: VecDeque::new(),
            phase: ChainPhase::Choose,
        });
        loop {
            match work.phase {
                ChainPhase::Choose => {
                    work.phase = if !work.a.is_empty() && !work.b.is_empty() {
                        ChainPhase::Merge
                    } else if let Some(tail) = work.a.pop_front().or_else(|| work.b.pop_front()) {
                        work.staged.push_back(tail);
                        ChainPhase::Settle(false)
                    } else {
                        ChainPhase::Settle(true)
                    };
                }
                ChainPhase::Merge => {
                    ready!(C::poll_merge(
                        &mut work.io,
                        cx,
                        &mut work.a,
                        &mut work.b,
                        &mut work.staged
                    ));
                    work.phase = ChainPhase::Settle(false);
                }
                ChainPhase::Settle(done) => {
                    ready!(C::poll_settle(
                        &mut work.io,
                        cx,
                        &mut work.staged,
                        done,
                        &mut work.settled
                    ));
                    if done {
                        output.extend(self.merge.take().unwrap().settled);
                        return Poll::Ready(());
                    }
                    work.phase = ChainPhase::Choose;
                }
            }
        }
    }

    fn poll_extract(
        &mut self,
        input: &mut Vec<C>,
        upper: AntichainRef<C::Time>,
        frontier: &mut Antichain<C::Time>,
        ship: &mut Vec<C>,
        kept: &mut Vec<C>,
        _stash: &mut Vec<C>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        use std::task::{Poll, ready};
        let work = self.extract.get_or_insert_with(|| ChainExtract {
            io: C::pending_for(input, &[]),
            input: std::mem::take(input).into(),
            keep: VecDeque::new(),
            ship: VecDeque::new(),
            kept: VecDeque::new(),
            shipped: VecDeque::new(),
            phase: ExtractPhase::Choose,
        });
        loop {
            match work.phase {
                ExtractPhase::Choose => {
                    work.phase = if work.input.is_empty() {
                        ExtractPhase::Keep(true)
                    } else {
                        ExtractPhase::Extract
                    };
                }
                ExtractPhase::Extract => {
                    ready!(C::poll_extract(
                        &mut work.io,
                        cx,
                        &mut work.input,
                        upper,
                        frontier,
                        &mut work.keep,
                        &mut work.ship
                    ));
                    work.phase = ExtractPhase::Keep(false);
                }
                ExtractPhase::Keep(done) => {
                    ready!(C::poll_settle(
                        &mut work.io,
                        cx,
                        &mut work.keep,
                        done,
                        &mut work.kept
                    ));
                    work.phase = ExtractPhase::Ship(done);
                }
                ExtractPhase::Ship(done) => {
                    ready!(C::poll_settle(
                        &mut work.io,
                        cx,
                        &mut work.ship,
                        done,
                        &mut work.shipped
                    ));
                    if done {
                        let work = self.extract.take().unwrap();
                        ship.extend(work.shipped);
                        kept.extend(work.kept);
                        return Poll::Ready(());
                    }
                    work.phase = ExtractPhase::Choose;
                }
            }
        }
    }

    fn len(chunk: &C) -> usize {
        chunk.len()
    }
}

enum BatchMergePhase {
    Choose,
    Merge,
    Account,
    Advance,
    Settle,
}

/// The resumable [`SpineBatch::Merger`] for [`ChunkBatch`]: merges two updates and advances
/// their times to the compaction frontier, a fuel-bounded step at a time.
///
/// Each step pipelines [`merge`](Chunk::merge) → [`advance`](Chunk::advance) →
/// [`settle`](Chunk::settle) and settles its output, so a suspended merge holds only
/// graded chunks. The sources are read by cloning (a cheap refcount bump) and must be
/// supplied unchanged on every call.
pub struct ChunkBatchMerger<C: Chunk> {
    /// Compaction frontier supplied at construction.
    frontier: Antichain<C::Time>,
    /// Input deques, refilled from the sources (clones) head-of-list at a time.
    in1: VecDeque<C>,
    in2: VecDeque<C>,
    /// Next source chunk to clone into `in1` / `in2`.
    idx1: usize,
    idx2: usize,
    /// `advance`'s input: the merge output plus advance's withheld carry at the front.
    merged: VecDeque<C>,
    /// `advance`'s output and `settle`'s input: merged-and-advanced chunks, with
    /// settle's withheld sub-`TARGET` carry at the front.
    advanced: VecDeque<C>,
    /// `settle`'s output: the committed, graded result, grown by `work`. Graded at
    /// every yield, so a suspended merge holds well-formed (spillable) chunk state.
    settled: VecDeque<C>,
    /// Set once both sources are drained and advance's and settle's final flushes ran.
    complete: bool,
    io: C::Pending,
    phase: BatchMergePhase,
    work: usize,
    produced: VecDeque<C>,
}

impl<C> super::spine::Merger<ChunkBatch<C>> for ChunkBatchMerger<C>
where
    C: Chunk + 'static,
    C::Time: timely::progress::Timestamp + Lattice + Ord + 'static,
{
    fn new(
        source1: &ChunkBatch<C>,
        source2: &ChunkBatch<C>,
        frontier: AntichainRef<C::Time>,
    ) -> Self {
        Self {
            frontier: frontier.to_owned(),
            in1: VecDeque::new(),
            in2: VecDeque::new(),
            idx1: 0,
            idx2: 0,
            merged: VecDeque::new(),
            advanced: VecDeque::new(),
            settled: VecDeque::new(),
            complete: false,
            io: C::pending_for(&source1.chunks, &source2.chunks),
            phase: BatchMergePhase::Choose,
            work: 0,
            produced: VecDeque::new(),
        }
    }

    fn work(&mut self, source1: &ChunkBatch<C>, source2: &ChunkBatch<C>, fuel: &mut isize) {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            self.poll_work(source1, source2, &mut cx, fuel).is_ready(),
            "pending chunk merger requires poll_work"
        );
    }

    fn poll_work(
        &mut self,
        source1: &ChunkBatch<C>,
        source2: &ChunkBatch<C>,
        cx: &mut std::task::Context<'_>,
        fuel: &mut isize,
    ) -> std::task::Poll<super::spine::MergeStatus> {
        use super::spine::MergeStatus;
        use std::task::{Poll, ready};
        loop {
            match self.phase {
                BatchMergePhase::Choose => {
                    if self.complete {
                        return Poll::Ready(MergeStatus::Complete);
                    }
                    if *fuel <= 0 {
                        return Poll::Ready(MergeStatus::InProgress);
                    }
                    const BURST: usize = 8;
                    while self.in1.len() < BURST && self.idx1 < source1.chunks.len() {
                        self.in1.push_back(source1.chunks[self.idx1].clone());
                        self.idx1 += 1;
                    }
                    while self.in2.len() < BURST && self.idx2 < source2.chunks.len() {
                        self.in2.push_back(source2.chunks[self.idx2].clone());
                        self.idx2 += 1;
                    }
                    if !self.in1.is_empty() && !self.in2.is_empty() {
                        self.phase = BatchMergePhase::Merge;
                    } else {
                        if let Some(chunk) = self.in1.pop_front().or_else(|| self.in2.pop_front()) {
                            self.produced.push_back(chunk);
                        } else {
                            self.complete = true;
                        }
                        self.phase = BatchMergePhase::Account;
                    }
                }
                BatchMergePhase::Merge => {
                    ready!(C::poll_merge(
                        &mut self.io,
                        cx,
                        &mut self.in1,
                        &mut self.in2,
                        &mut self.produced
                    ));
                    self.phase = BatchMergePhase::Account;
                }
                BatchMergePhase::Account => {
                    self.work = self.produced.iter().map(C::len).sum();
                    self.merged.append(&mut self.produced);
                    self.phase = BatchMergePhase::Advance;
                }
                BatchMergePhase::Advance => {
                    ready!(C::poll_advance(
                        &mut self.io,
                        cx,
                        &mut self.merged,
                        self.frontier.borrow(),
                        self.complete,
                        &mut self.advanced
                    ));
                    self.phase = BatchMergePhase::Settle;
                }
                BatchMergePhase::Settle => {
                    ready!(C::poll_settle(
                        &mut self.io,
                        cx,
                        &mut self.advanced,
                        self.complete,
                        &mut self.settled
                    ));
                    *fuel -= self.work.cast_signed();
                    self.phase = BatchMergePhase::Choose;
                }
            }
        }
    }

    fn done(self) -> Option<ChunkBatch<C>> {
        debug_assert!(self.merged.is_empty() && self.advanced.is_empty());
        wrap(self.settled.into())
    }
}

/// A builder that collects a chunk sequence into a [`ChunkBatch`].
pub struct ChunkBatchBuilder<C: Chunk> {
    /// Pushed chunks awaiting settling; holds settle's sub-`TARGET` carry at the front.
    input: VecDeque<C>,
    /// The graded chunks emitted so far.
    output: VecDeque<C>,
}

impl<C: Chunk> Default for ChunkBatchBuilder<C> {
    fn default() -> Self {
        Self {
            input: VecDeque::new(),
            output: VecDeque::new(),
        }
    }
}

impl<C> ChunkBatchBuilder<C>
where
    C: Chunk + Default + 'static,
    C::Time: timely::progress::Timestamp,
{
    pub fn push(&mut self, chunk: &mut C) {
        let chunk = std::mem::take(chunk);
        if chunk.len() > 0 {
            self.input.push_back(chunk);
            C::settle(&mut self.input, false, &mut self.output);
        }
    }

    pub fn done(self) -> Option<ChunkBatch<C>> {
        let ChunkBatchBuilder {
            mut input,
            mut output,
        } = self;
        C::settle(&mut input, true, &mut output);
        let chunks: Vec<C> = output.into();
        wrap(chunks)
    }
}

impl<C> super::batcher::Sealer<C> for ChunkBatchBuilder<C>
where
    C: Chunk + Default + 'static,
    C::Time: timely::progress::Timestamp,
{
    type Output = ChunkBatch<C>;

    fn seal(chain: &mut Vec<C>) -> Option<Self::Output> {
        // We settle the chain because we are not guaranteed to received pre-settled data.
        // This should be efficient on pre-settled data.
        wrap(settle_all(std::mem::take(chain)))
    }
}

/// Wraps settled chunks as a batch, absent when there are no chunks.
fn wrap<C: Chunk>(chunks: Vec<C>) -> Option<ChunkBatch<C>> {
    (!chunks.is_empty()).then(|| ChunkBatch::new(chunks))
}

/// Whether `chunks` satisfy the [`Chunk::TARGET`] grading invariant: every chunk
/// at most `TARGET`, and every adjacent pair summing to more than `TARGET` (so no
/// two neighbours could be combined into one legal chunk — a *maximal packing*).
///
/// This is the post-[`settle`](Chunk::settle) shape; useful as a test/debug check.
pub fn is_graded<C: Chunk>(chunks: &[C]) -> bool {
    chunks.iter().all(|c| c.len() <= C::TARGET)
        && chunks
            .windows(2)
            .all(|w| w[0].len() + w[1].len() > C::TARGET)
}

/// Settle `input` to completion into a fresh graded `Vec` (see [`Chunk::settle`]).
///
/// A convenience for the one-shot callers (batch sealing, the batcher's merge and
/// extract) that have a whole sequence in hand and want it graded; the streaming
/// callers drive [`Chunk::settle`] directly across ticks.
pub fn settle_all<C: Chunk>(input: impl IntoIterator<Item = C>) -> Vec<C> {
    let mut input: VecDeque<C> = input.into_iter().collect();
    let mut out = VecDeque::new();
    C::settle(&mut input, true, &mut out);
    debug_assert!(input.is_empty());
    out.into()
}

/// Merge two full chains of chunks into one, to completion, appending to `out`.
///
/// The plain whole-chain driver: ticks [`Chunk::merge`] until one deque empties, then
/// appends the other's remainder (the verbatim tail). Output is near-graded, not
/// settled. The batcher's `merge` runs the same loop but settles after each push (the
/// bounded-footprint discipline) and so does not use this; it stays as the simplest way
/// to drive [`Chunk::merge`] to completion.
pub fn merge_chains<C: Chunk>(chain1: Vec<C>, chain2: Vec<C>, out: &mut VecDeque<C>) {
    let mut in1: VecDeque<C> = chain1.into();
    let mut in2: VecDeque<C> = chain2.into();
    while !in1.is_empty() && !in2.is_empty() {
        C::merge(&mut in1, &mut in2, out);
    }
    // One deque is empty; the other's remainder is all greater than everything merged.
    out.extend(in1.drain(..));
    out.extend(in2.drain(..));
}
