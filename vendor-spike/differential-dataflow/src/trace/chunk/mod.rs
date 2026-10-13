//! Sorted, consolidated runs of updates, and operators over sequences of them.
//!
//! A [`Chunk`] is a consolidated, sorted run of `(data, time, diff)` updates.
//! A sequence of chunks is also expected to be consolidated and sorted.
//!
//! The [`Chunk`] trait exposes whole-chunk operations, so that the implementor
//! can internally divert to their best implementations, with amortized overhead.
//! Each operation is invoked as if "streaming", providing input and output queues.
//! An implementor is expected to drain as much as possible of the inputs, and any
//! chunk written to the output is "committed" and likely to be shipped onward.
//!
//! # Wiring a `Chunk` into an arrangement
//!
//! Implementing [`Chunk`] for a type `C` is the only bespoke code needed; three
//! aliases then expand into a full trace:
//!
//! * [`ChunkBatcher<C>`](ChunkBatcher) — the merge batcher.
//! * [`ChunkBuilder<C>`](ChunkBuilder) — the batch builder.
//! * [`ChunkSpine<C>`](ChunkSpine) — the trace, a spine of `Rc`-shared batches.
//!
//! These are the `Batcher` / `Builder` / `Spine` to hand to
//! [`arrange_core`](crate::operators::arrange::arrangement::arrange_core), along with a
//! chunker that forms `C` from the input stream — typically
//! [`ContainerChunker<C>`](crate::trace::implementations::chunker::ContainerChunker).
//! Trace *maintenance* needs only [`Chunk`]; cursor-driven *consumption* of the
//! arrangement additionally asks `C` for the [`NavigableChunk`] capability.
//! Everything else here ([`ChunkBatch`], [`ChunkMerger`], [`ChunkBatchMerger`],
//! [`ChunkBatchCursor`], [`ChunkBatchBuilder`]) is machinery those aliases expand to and is
//! not named directly. The [`vec`](mod@vec) module is a worked `Chunk`
//! that re-exports the three aliases specialized to its layout, and the `chunks` example
//! stands one up.
//!
//! # Bounded footprint
//!
//! There is a `TARGET` associated constant that signals the intended chunk size.
//! The constant should be chosen large enough to amortize overheads, but small
//! enough that per-chunk work does not "stall" the system when invoked.
//! The implementor is trusted to make a reasonable choice here.
//!
//! The [`Chunk::settle`] method "settles" sequences of chunks, and is called as
//! chunks are no longer expected to be needed in the near future. The implementor
//! should ensure the chunks are "graded", in that the sequence of chunks are all
//! at most `TARGET` in size, any two in order sum to strictly more than `TARGET`.
//! This is also an opportunity to compress data, or spill to disk or cloud storage.
//!
//! The active (un-settled) chunk set is kept small from both sides. Every producer
//! settles its committed output as it goes (see [`Chunk::settle`]), rather than
//! building a whole sequence and settling at the end. And every walk over a whole
//! chunk sequence reads only resident metadata — [`len`](Chunk::len) and
//! [`bounds`](NavigableChunk::bounds) — never a chunk body: the straddle cursor
//! seeks by galloping the chunks' bounds from a hint and opens only the chunk(s) a
//! query touches. Implementors must therefore keep `len` and `bounds` cheap even
//! when a chunk's body is paged out.

use std::collections::VecDeque;

use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;
use crate::lattice::Lattice;
use crate::trace::{Batch, BatchReader, Description, Navigable};
use crate::trace::cursor::Cursor;
use crate::trace::implementations::BatchContainer;

pub mod vec;

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
/// The trait has no opinion about keys, vals, or diffs — only time, which trace
/// maintenance needs. Reading a chunk's contents is a separate, optional
/// capability: see [`NavigableChunk`].
pub trait Chunk: Sized + Clone {
    /// The timestamp type of the chunk's updates.
    ///
    /// Key/val/diff opinions live on the optional [`NavigableChunk`] capability; the chunk itself
    /// only needs time, to bound its interval and participate in advancement and compaction.
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

}

/// The navigation capability: a [`Chunk`] whose contents can be read by cursor.
///
/// This is optional. Batch formation and trace maintenance need only [`Chunk`];
/// implementing this trait additionally lets [`ChunkBatch`] offer the straddle
/// cursor ([`ChunkBatchCursor`]), which is how cursor-driven operator paths read
/// an arrangement. Chunks consumed only by whole-chunk logic (tactics) can skip it.
///
/// `bounds` must stay cheap even when a chunk's body is paged out: the straddle
/// cursor consults chunk bounds throughout navigation — seeks binary-search them,
/// boundary crossings compare against them — and opens a chunk's body only when a
/// query touches it.
pub trait NavigableChunk: Chunk + Navigable<Cursor: Cursor<Time = <Self as Chunk>::Time>> {
    /// The first and last `(key, val, time)` triples in the chunk.
    fn bounds(&self) -> (
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
    );
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
                if c.len() == C::TARGET { out.push_back(seal(c)); } else { carry = Some(c); }
            }
            Some(c) => {
                // `c` is maximal against this neighbour; emit it and absorb afresh.
                out.push_back(seal(c));
                pack_absorb(chunk, &mut carry, out, &mut split, &mut seal);
            }
        }
    }
    if let Some(c) = carry {
        if done { out.push_back(seal(c)); } else { input.push_front(c); }
    }
}

/// Absorb `chunk` into an empty `carry` (a [`pack`] helper): pass a `TARGET` chunk
/// straight through (sealed), hold a smaller one as the new carry, or peel
/// `TARGET`-sized pieces off a larger one and carry the remainder.
fn pack_absorb<C, S, L>(chunk: C, carry: &mut Option<C>, out: &mut VecDeque<C>, split: &mut S, seal: &mut L)
where
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
                if tail.len() >= C::TARGET { rest = tail; }
                else { if tail.len() > 0 { *carry = Some(tail); } break; }
            }
        }
    }
}

type KeyCon<C> = <<C as Navigable>::Cursor as Cursor>::KeyContainer;
type ValCon<C> = <<C as Navigable>::Cursor as Cursor>::ValContainer;

/// A batch is a [`Chunk`] sequence plus a [`Description`].
pub struct ChunkBatch<C: Chunk> {
    /// Ordered, consolidated chunks; their concatenation is the batch.
    pub chunks: Vec<C>,
    /// The lower, upper, and since frontiers of the batch.
    pub description: Description<C::Time>,
}

impl<C: Chunk> ChunkBatch<C> {
    /// Assemble a batch from ordered chunks.
    pub fn new(chunks: Vec<C>, description: Description<C::Time>) -> Self {
        for chunk in &chunks {
            assert!(chunk.len() > 0, "ChunkBatch chunks must be non-empty");
        }
        ChunkBatch { chunks, description }
    }
}

impl<C: NavigableChunk> crate::trace::Navigable for ChunkBatch<C> {
    type Cursor = ChunkBatchCursor<C>;
    fn cursor(&self) -> Self::Cursor {
        ChunkBatchCursor { key_chunk: 0, chunk: 0, inner: self.chunks.first().map(C::cursor) }
    }
}

impl<C: Chunk> BatchReader for ChunkBatch<C> {
    type Time = C::Time;
    fn len(&self) -> usize { self.chunks.iter().map(C::len).sum() }
    fn description(&self) -> &Description<Self::Time> { &self.description }
}

impl<C: Chunk + Default + 'static> Batch for ChunkBatch<C>
where
    C::Time: timely::progress::Timestamp + Lattice + Ord,
{
    type Merger = ChunkBatchMerger<C>;

    fn empty(lower: Antichain<Self::Time>, upper: Antichain<Self::Time>) -> Self {
        use timely::progress::Timestamp;
        let since = Antichain::from_elem(Self::Time::minimum());
        ChunkBatch::new(Vec::new(), Description::new(lower, upper, since))
    }
}

/// A merge-batcher [`Merger`](crate::trace::implementations::merge_batcher::Merger)
/// over chains of [`Chunk`]s.
///
/// `merge` runs the whole-chain binary merger; `extract` splits by the seal frontier
/// using [`Chunk::extract`]. The batcher consolidates equal `(data, time)` updates
/// but does *not* advance times — time advancement is advance's job, handled later in
/// the trace. Both settle their output, since the batcher's chains want to be graded.
pub type ChunkBatcher<C> = crate::trace::implementations::merge_batcher::MergeBatcher<ChunkMerger<C>>;

/// A spine of `Rc`-shared [`ChunkBatch`]es of type `C`: the trace type for `arrange`.
pub type ChunkSpine<C> = crate::trace::implementations::spine_fueled::Spine<std::rc::Rc<ChunkBatch<C>>>;

/// A reference-counted [`ChunkBatch`] builder over chunks of type `C`.
pub type ChunkBuilder<C> = crate::trace::rc_blanket_impls::RcBuilder<ChunkBatchBuilder<C>>;

/// A cursor over a [`ChunkBatch`], merging the per-chunk cursors.
///
/// Chunk breakpoints are unconstrained, so a single key — or `(key, val)` — may
/// straddle consecutive chunks. But the chunks are one globally-sorted sequence
/// merely cut at arbitrary points, so the operation is *concatenation*, never a
/// merge: across a boundary a key's vals concatenate and a `(key, val)`'s times
/// concatenate. The cursor exploits this. It holds the chunk currently being read
/// and a cursor into it; it seeks by galloping the chunks' resident
/// [`bounds`](NavigableChunk::bounds) from a remembered hint (the current key's first
/// chunk), and at boundaries it *continues* into the next chunk rather than merging —
/// consulting the two neighbouring chunks' bounds to detect when a key or `(key, val)`
/// spills forward, without touching chunk contents. No state is materialized up front:
/// a monotone seek sweep costs `O(log Δ)` bounds reads per seek and a sequential pass two
/// per boundary, so cursor construction is free.
pub struct ChunkBatchCursor<C: NavigableChunk> {
    /// First chunk of the current key's run; where `rewind_vals` returns to.
    key_chunk: usize,
    /// Chunk currently being read; `>= key_chunk`, within the current key's span.
    chunk: usize,
    /// Cursor into `chunk`; `None` once `chunk` is past the last chunk.
    inner: Option<C::Cursor>,
}

impl<C: NavigableChunk> ChunkBatchCursor<C> {
    /// Move the active chunk to `c`, opening a fresh inner cursor at its start.
    fn goto(&mut self, c: usize, storage: &ChunkBatch<C>) {
        self.chunk = c;
        self.inner = storage.chunks.get(c).map(C::cursor);
    }

    /// Does key `k` span the boundary between chunks `c` and `c + 1` — chunk `c`
    /// ends with it and chunk `c + 1` begins with it?
    ///
    /// Two resident [`bounds`](NavigableChunk::bounds) reads; the `reborrow`s
    /// unify the (invariant) item lifetimes with `k`'s.
    fn key_spills(s: &ChunkBatch<C>, c: usize, k: <C::Cursor as Cursor>::Key<'_>) -> bool {
        <KeyCon<C> as BatchContainer>::reborrow(s.chunks[c].bounds().1.0) == <KeyCon<C> as BatchContainer>::reborrow(k)
            && <KeyCon<C> as BatchContainer>::reborrow(s.chunks[c + 1].bounds().0.0) == <KeyCon<C> as BatchContainer>::reborrow(k)
    }

    /// Does `(k, v)` span the boundary between chunks `c` and `c + 1`?
    fn val_spills(s: &ChunkBatch<C>, c: usize, k: <C::Cursor as Cursor>::Key<'_>, v: <C::Cursor as Cursor>::Val<'_>) -> bool {
        Self::key_spills(s, c, k)
            && <ValCon<C> as BatchContainer>::reborrow(s.chunks[c].bounds().1.1) == <ValCon<C> as BatchContainer>::reborrow(v)
            && <ValCon<C> as BatchContainer>::reborrow(s.chunks[c + 1].bounds().0.1) == <ValCon<C> as BatchContainer>::reborrow(v)
    }

    /// The first chunk, at or after `hint`, whose last key is `>= key`: where `key`'s run
    /// begins. `hint` — the current key's first chunk (`key_chunk`) — is a valid lower bound
    /// for a forward seek; a backward seek is detected and served by a full search from the
    /// front. Galloping keeps a monotone seek sweep at `O(log Δ)` bounds reads per seek rather
    /// than `O(log chunks)`; only resident [`bounds`](NavigableChunk::bounds) are read.
    fn locate_key(s: &ChunkBatch<C>, hint: usize, key: <C::Cursor as Cursor>::Key<'_>) -> usize {
        let n = s.chunks.len();
        // `last_key(i) < key`, from chunk `i`'s resident bounds.
        let lt = |i: usize| <KeyCon<C> as BatchContainer>::reborrow(s.chunks[i].bounds().1.0)
            .lt(&<KeyCon<C> as BatchContainer>::reborrow(key));
        let hint = hint.min(n);
        // The hint can skip the answer only on a backward seek; then search from the front.
        let lo = if hint == 0 || lt(hint - 1) { hint } else { 0 };
        if lo >= n || !lt(lo) { return lo; }
        // Exponential search from `lo`, then binary within the final bracket.
        let (mut prev, mut step) = (lo, 1usize);
        while prev + step < n && lt(prev + step) { prev += step; step <<= 1; }
        let (mut a, mut b) = (prev + 1, (prev + step).min(n));
        while a < b { let m = a + (b - a) / 2; if lt(m) { a = m + 1; } else { b = m; } }
        a
    }
}

impl<C: NavigableChunk> Cursor for ChunkBatchCursor<C> {
    type Storage = ChunkBatch<C>;

    type KeyContainer = <C::Cursor as Cursor>::KeyContainer;
    type Key<'a> = <C::Cursor as Cursor>::Key<'a>;
    type ValContainer = <C::Cursor as Cursor>::ValContainer;
    type Val<'a> = <C::Cursor as Cursor>::Val<'a>;
    type ValOwn = <C::Cursor as Cursor>::ValOwn;
    type TimeContainer = <C::Cursor as Cursor>::TimeContainer;
    type TimeGat<'a> = <C::Cursor as Cursor>::TimeGat<'a>;
    type Time = <C::Cursor as Cursor>::Time;
    type DiffContainer = <C::Cursor as Cursor>::DiffContainer;
    type DiffGat<'a> = <C::Cursor as Cursor>::DiffGat<'a>;
    type Diff = <C::Cursor as Cursor>::Diff;

    fn key_valid(&self, s: &Self::Storage) -> bool { self.chunk < s.chunks.len() && self.inner.as_ref().is_some_and(|i| i.key_valid(&s.chunks[self.chunk])) }
    fn val_valid(&self, s: &Self::Storage) -> bool { self.chunk < s.chunks.len() && self.inner.as_ref().is_some_and(|i| i.val_valid(&s.chunks[self.chunk])) }
    fn key<'a>(&self, s: &'a Self::Storage) -> Self::Key<'a> { self.inner.as_ref().unwrap().key(&s.chunks[self.chunk]) }
    fn val<'a>(&self, s: &'a Self::Storage) -> Self::Val<'a> { self.inner.as_ref().unwrap().val(&s.chunks[self.chunk]) }
    fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<Self::Key<'a>> { if self.key_valid(s) { Some(self.key(s)) } else { None } }
    fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<Self::Val<'a>> { if self.val_valid(s) { Some(self.val(s)) } else { None } }

    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, s: &Self::Storage, mut logic: L) {
        if !self.val_valid(s) { return; }
        let (k, v) = (self.key(s), self.val(s));
        self.inner.as_mut().unwrap().map_times(&s.chunks[self.chunk], &mut logic);
        // Follow the (key, val) forward across boundaries while it spills.
        let mut c = self.chunk;
        while c + 1 < s.chunks.len() && Self::val_spills(s, c, k, v) {
            c += 1;
            s.chunks[c].cursor().map_times(&s.chunks[c], &mut logic);
        }
    }

    fn step_key(&mut self, s: &Self::Storage) {
        if !self.key_valid(s) { return; }
        let n = s.chunks.len();
        let k = self.key(s);
        // Advance to the last chunk the key spans.
        while self.chunk + 1 < n && Self::key_spills(s, self.chunk, k) {
            self.goto(self.chunk + 1, s);
        }
        // Step past the key within its last chunk.
        {
            let inner = self.inner.as_mut().unwrap();
            inner.seek_key(&s.chunks[self.chunk], k);
            inner.step_key(&s.chunks[self.chunk]);
        }
        // If that exhausted the chunk, the next key (if any) starts the next chunk.
        if !self.inner.as_ref().unwrap().key_valid(&s.chunks[self.chunk]) && self.chunk + 1 < n {
            self.goto(self.chunk + 1, s);
        }
        self.key_chunk = self.chunk;
    }

    fn seek_key(&mut self, s: &Self::Storage, key: Self::Key<'_>) {
        let n = s.chunks.len();
        // First chunk whose last key is `>= key`: where `key`'s run begins. Gallop from the
        // current key's first chunk (`key_chunk`), a lower bound for forward seeks.
        let lo = Self::locate_key(s, self.key_chunk, key);
        self.goto(lo, s);
        self.key_chunk = lo;
        if lo < n { self.inner.as_mut().unwrap().seek_key(&s.chunks[lo], key); }
    }

    fn step_val(&mut self, s: &Self::Storage) {
        if !self.val_valid(s) { return; }
        let n = s.chunks.len();
        let (k, v) = (self.key(s), self.val(s));
        // Advance to the last chunk the (key, val) spans.
        while self.chunk + 1 < n && Self::val_spills(s, self.chunk, k, v) {
            self.goto(self.chunk + 1, s);
        }
        // Step past the (key, val) within that chunk.
        self.inner.as_mut().unwrap().step_val(&s.chunks[self.chunk]);
        // If the key's vals are exhausted here but the key spills, roll forward.
        if !self.inner.as_ref().unwrap().val_valid(&s.chunks[self.chunk])
            && self.chunk + 1 < n && Self::key_spills(s, self.chunk, k)
        {
            self.goto(self.chunk + 1, s);
            self.inner.as_mut().unwrap().seek_key(&s.chunks[self.chunk], k);
        }
    }

    fn seek_val(&mut self, s: &Self::Storage, val: Self::Val<'_>) {
        if !self.key_valid(s) { return; }
        let n = s.chunks.len();
        let k = self.key(s);
        loop {
            self.inner.as_mut().unwrap().seek_val(&s.chunks[self.chunk], val);
            if self.inner.as_ref().unwrap().val_valid(&s.chunks[self.chunk]) { return; }
            // Key's vals exhausted in this chunk; if the key spills, retry in the next.
            if self.chunk + 1 < n && Self::key_spills(s, self.chunk, k) {
                self.goto(self.chunk + 1, s);
                self.inner.as_mut().unwrap().seek_key(&s.chunks[self.chunk], k);
            } else {
                return;
            }
        }
    }

    fn rewind_keys(&mut self, s: &Self::Storage) {
        self.key_chunk = 0;
        self.goto(0, s);
    }

    fn rewind_vals(&mut self, s: &Self::Storage) {
        if !self.key_valid(s) { return; }
        let k = self.key(s);
        let kc = self.key_chunk;
        self.goto(kc, s);
        self.inner.as_mut().unwrap().seek_key(&s.chunks[kc], k);
    }
}

/// A merge-batcher [`Merger`](crate::trace::implementations::merge_batcher::Merger)
/// over chains of [`Chunk`]s.
///
/// `merge` runs the whole-chain binary merger; `extract` splits by the seal frontier
/// using [`Chunk::extract`]. The batcher consolidates equal `(data, time)` updates
/// but does *not* advance times — time advancement is advance's job, handled later in
/// the trace. Both settle their output, since the batcher's chains want to be graded.
pub struct ChunkMerger<C> {
    _marker: std::marker::PhantomData<C>,
}

impl<C> Default for ChunkMerger<C> {
    fn default() -> Self { Self { _marker: std::marker::PhantomData } }
}

impl<C> crate::trace::implementations::merge_batcher::Merger for ChunkMerger<C>
where
    C: Chunk + Default + 'static,
    C::Time: Clone + timely::PartialOrder + 'static,
{
    type Chunk = C;
    type Time = C::Time;

    fn merge(
        &mut self,
        list1: Vec<C>,
        list2: Vec<C>,
        output: &mut Vec<C>,
        _stash: &mut Vec<C>,
    ) {
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

    fn len(chunk: &C) -> usize { chunk.len() }
}

/// The resumable [`Batch::Merger`] for [`ChunkBatch`]: merges two batches and advances
/// their times to the compaction frontier, a fuel-bounded step at a time.
///
/// Each step pipelines [`merge`](Chunk::merge) → [`advance`](Chunk::advance) →
/// [`settle`](Chunk::settle) and settles its output, so a suspended merge holds only
/// graded chunks. The sources are read by cloning (a cheap refcount bump) and must be
/// supplied unchanged on every call.
pub struct ChunkBatchMerger<C: Chunk> {
    /// Compaction frontier supplied at construction.
    frontier: Antichain<C::Time>,
    /// Result frontiers, retained for the output description.
    lower: Antichain<C::Time>,
    upper: Antichain<C::Time>,
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
}

impl<C> crate::trace::Merger<ChunkBatch<C>> for ChunkBatchMerger<C>
where
    C: Chunk + Default + 'static,
    C::Time: timely::progress::Timestamp + Lattice + Ord + 'static,
{
    fn new(source1: &ChunkBatch<C>, source2: &ChunkBatch<C>, frontier: AntichainRef<C::Time>) -> Self {
        let lower = source1.description.lower().meet(source2.description.lower());
        let upper = source1.description.upper().join(source2.description.upper());
        Self {
            frontier: frontier.to_owned(),
            lower,
            upper,
            in1: VecDeque::new(),
            in2: VecDeque::new(),
            idx1: 0,
            idx2: 0,
            merged: VecDeque::new(),
            advanced: VecDeque::new(),
            settled: VecDeque::new(),
            complete: false,
        }
    }

    fn work(&mut self, source1: &ChunkBatch<C>, source2: &ChunkBatch<C>, fuel: &mut isize) {

        // TODO: The logic is a bit tortured here, and should be improved.

        if self.complete { return; }

        while *fuel > 0 {
            // Refill each input deque up to a burst of source chunks (clones).
            // The constant trades away fuel precision for overhead amortization.
            const BURST: usize = 8;
            while self.in1.len() < BURST && self.idx1 < source1.chunks.len() {
                self.in1.push_back(source1.chunks[self.idx1].clone());
                self.idx1 += 1;
            }
            while self.in2.len() < BURST && self.idx2 < source2.chunks.len() {
                self.in2.push_back(source2.chunks[self.idx2].clone());
                self.idx2 += 1;
            }

            // Merge's per-tick output (a burst's worth, or one tail chunk), measured
            // for fuel before it joins the carry already in `merged`.
            let mut produced = VecDeque::new();
            if !self.in1.is_empty() && !self.in2.is_empty() {
                // Both sides have data: drain the loaded burst.
                C::merge(&mut self.in1, &mut self.in2, &mut produced);
            } else if let Some(chunk) = self.in1.pop_front().or_else(|| self.in2.pop_front()) {
                // Exactly one side has data: flush its verbatim tail, one chunk a step.
                produced.push_back(chunk);
            } else {
                // Both sources drained: final flush of advance's and settle's carries.
                C::advance(&mut self.merged, self.frontier.borrow(), true, &mut self.advanced);
                C::settle(&mut self.advanced, true, &mut self.settled);
                self.complete = true;
                break;
            }

            let work: usize = produced.iter().map(C::len).sum();
            self.merged.extend(produced);
            C::advance(&mut self.merged, self.frontier.borrow(), false, &mut self.advanced);
            // Maintain grading at the yield boundary: this step may exhaust `fuel` and
            // suspend with `advanced` held, and held chunk state must be graded.
            C::settle(&mut self.advanced, false, &mut self.settled);
            *fuel -= work as isize;
        }
    }

    fn done(self) -> ChunkBatch<C> {
        debug_assert!(self.merged.is_empty() && self.advanced.is_empty());
        let description = Description::new(self.lower, self.upper, self.frontier);
        ChunkBatch::new(self.settled.into(), description)
    }
}

/// A [`Builder`](crate::trace::Builder) that collects a chunk sequence into a [`ChunkBatch`].
pub struct ChunkBatchBuilder<C: Chunk> {
    /// Pushed chunks awaiting settling; holds settle's sub-`TARGET` carry at the front.
    input: VecDeque<C>,
    /// The graded chunks emitted so far.
    output: VecDeque<C>,
}

impl<C> crate::trace::Builder for ChunkBatchBuilder<C>
where
    C: Chunk + Default + 'static,
    C::Time: timely::progress::Timestamp,
{
    type Input = C;
    type Time = C::Time;
    type Output = ChunkBatch<C>;

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        Self { input: VecDeque::new(), output: VecDeque::new() }
    }

    fn push(&mut self, chunk: &mut C) {
        let chunk = std::mem::take(chunk);
        if chunk.len() > 0 {
            self.input.push_back(chunk);
            C::settle(&mut self.input, false, &mut self.output);
        }
    }

    fn done(self, description: Description<C::Time>) -> ChunkBatch<C> {
        let ChunkBatchBuilder { mut input, mut output } = self;
        C::settle(&mut input, true, &mut output);
        ChunkBatch::new(output.into(), description)
    }

    fn seal(chain: &mut Vec<C>, description: Description<C::Time>) -> ChunkBatch<C> {
        // We settle the chain because we are not guaranteed to received pre-settled data.
        // This should be efficient on pre-settled data.
        ChunkBatch::new(settle_all(std::mem::take(chain)), description)
    }
}

/// Whether `chunks` satisfy the [`Chunk::TARGET`] grading invariant: every chunk
/// at most `TARGET`, and every adjacent pair summing to more than `TARGET` (so no
/// two neighbours could be combined into one legal chunk — a *maximal packing*).
///
/// This is the post-[`settle`](Chunk::settle) shape; useful as a test/debug check.
pub fn is_graded<C: Chunk>(chunks: &[C]) -> bool {
    chunks.iter().all(|c| c.len() <= C::TARGET)
        && chunks.windows(2).all(|w| w[0].len() + w[1].len() > C::TARGET)
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
pub fn merge_chains<C: Chunk>(
    chain1: Vec<C>,
    chain2: Vec<C>,
    out: &mut VecDeque<C>,
) {
    let mut in1: VecDeque<C> = chain1.into();
    let mut in2: VecDeque<C> = chain2.into();
    while !in1.is_empty() && !in2.is_empty() {
        C::merge(&mut in1, &mut in2, out);
    }
    // One deque is empty; the other's remainder is all greater than everything merged.
    out.extend(in1.drain(..));
    out.extend(in2.drain(..));
}
