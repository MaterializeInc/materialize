//! A worked [`Chunk`]: the columnar `UpdatesTyped<U>` trie, resident or paged.
//!
//! Where [`vec`](crate::trace::chunk::vec) backs a chunk with a flat `Vec<((K,V),T,R)>`, this
//! backs it with the column-oriented trie from [`crate::columnar::updates`] —
//! deduplicated keys, per-key val runs, per-val `(time, diff)` runs. It is a
//! *retargeting* of the columnar trace pile at the [`Chunk`] abstraction: the
//! storage (`UpdatesTyped`) and the trie-native merge (`trie_merger`) are reused
//! verbatim, and the four transducers delegate to them. The harness
//! ([`ChunkBatch`](crate::trace::chunk::ChunkBatch), the straddle cursor, the batcher/builder/
//! spine aliases) is shared with `vec`.
//!
//! This makes columnar trace merges trie-native (the old `OrdValBatch`-backed
//! trace ran them through ord_neu's row-oriented merger).
//!
//! # Resident vs paged
//!
//! A [`ColChunk`] is either [`Resident`](ColChunk::Resident) (the trie in memory)
//! or [`Paged`](ColChunk::Paged) (resident bounds + a byte handle). [`Chunk::settle`]
//! is the spill point: it pages committed chunks out via
//! [`spill`](self::spill) when a worker has installed a spiller. Reads
//! fetch a paged chunk's trie back, caching it in a [`OnceCell`] so repeated
//! cursor access pays the fetch once; [`len`](Chunk::len) and [`bounds`](Chunk::bounds)
//! read the resident metadata and never fetch.
//!
//! [`Chunk::advance`] is trie-native: it melds its input in place and rewrites
//! only the time column (keys/vals/diffs stay columnar refs), then re-consolidates
//! the advanced time runs (a global pass a future per-`(key,val)` streaming
//! consolidation could narrow).

use std::cell::OnceCell;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::rc::Rc;

use columnar::{Borrow, Columnar, Container, ContainerOf, Index, Len, Push};
use timely::Accountable;
use timely::container::{PushInto, SizableContainer};
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use crate::consolidation::Consolidate;
use crate::lattice::Lattice;
use crate::trace::Navigable;
use crate::trace::cursor::Cursor;
use crate::trace::implementations::{BatchContainer, Layout, WithLayout};

use crate::columnar::layout::{ColumnarLayout, ColumnarUpdate, Coltainer};
use crate::columnar::updates::{child_range, UpdatesBuilder, UpdatesTyped};
use crate::columnar::trie_merger;

use super::spill::{self, BytesSource};

use crate::trace::chunk::Chunk;

/// The chunk size: the [`Chunk::TARGET`] grading value.
///
/// Smaller than `crate::columnar::LINK_TARGET` so multi-chunk batches — and the
/// straddle cursor — are exercised; the trie operations are happy at any size.
const TARGET: usize = 8192;

/// Resident bounds for a paged chunk: the first and last `(key, val, time)` as
/// single-element columnar containers (so [`Chunk::bounds`] returns refs without
/// fetching), plus the record count.
pub struct ChunkMeta<U: ColumnarUpdate> {
    fk: ContainerOf<U::Key>, fv: ContainerOf<U::Val>, ft: ContainerOf<U::Time>,
    lk: ContainerOf<U::Key>, lv: ContainerOf<U::Val>, lt: ContainerOf<U::Time>,
    len: usize,
}

/// A paged chunk: resident `meta`, a byte handle to fetch the trie, and a cache
/// populated on first read. Opaque — held inside [`ColChunk::Paged`].
pub struct PagedChunk<U: ColumnarUpdate> {
    meta: ChunkMeta<U>,
    source: Box<dyn BytesSource>,
    cache: OnceCell<Rc<UpdatesTyped<U>>>,
}

/// A single-element container holding `borrowed[i]`.
fn singleton<C: Container + Default>(borrowed: <C as Borrow>::Borrowed<'_>, i: usize) -> C {
    let mut out = C::default();
    out.push(borrowed.get(i));
    out
}

/// Snapshot a trie's first/last `(key, val, time)` and length as resident metadata.
fn meta_of<U: ColumnarUpdate>(t: &UpdatesTyped<U>) -> ChunkMeta<U> {
    let v = t.view();
    let (nk, nv, nt) = (v.keys.values.len(), v.vals.values.len(), v.times.values.len());
    ChunkMeta {
        fk: singleton(v.keys.values, 0),  lk: singleton(v.keys.values, nk - 1),
        fv: singleton(v.vals.values, 0),  lv: singleton(v.vals.values, nv - 1),
        ft: singleton(v.times.values, 0), lt: singleton(v.times.values, nt - 1),
        len: t.len(),
    }
}

/// A sorted, consolidated columnar trie of `((key, val), time, diff)`: resident, or
/// paged to backing storage.
pub enum ColChunk<U: ColumnarUpdate> {
    /// The trie in memory, shared via `Rc`.
    Resident(Rc<UpdatesTyped<U>>),
    /// Spilled out: resident bounds + a fetch handle (and a fill-once cache).
    Paged(Rc<PagedChunk<U>>),
}

impl<U: ColumnarUpdate> Clone for ColChunk<U> {
    fn clone(&self) -> Self {
        match self {
            ColChunk::Resident(rc) => ColChunk::Resident(Rc::clone(rc)),
            ColChunk::Paged(p) => ColChunk::Paged(Rc::clone(p)),
        }
    }
}
impl<U: ColumnarUpdate> Default for ColChunk<U> {
    fn default() -> Self { ColChunk::Resident(Rc::new(UpdatesTyped::default())) }
}

impl<U: ColumnarUpdate> ColChunk<U> {
    /// Wrap an already sorted, consolidated trie as a resident chunk. The chunker
    /// uses this to hand its melded `UpdatesTyped` blobs to the `Chunk` harness.
    pub fn from_trie(updates: UpdatesTyped<U>) -> Self { ColChunk::Resident(Rc::new(updates)) }

    /// Borrow the chunk's trie, fetching (and caching) it if paged. Reads tie to
    /// `&self`, so cursor refs remain valid for the borrow.
    fn trie(&self) -> &UpdatesTyped<U> {
        match self {
            ColChunk::Resident(rc) => &**rc,
            ColChunk::Paged(p) => &**p.cache.get_or_init(|| {
                spill::note_fetched();
                Rc::new(spill::decode::<U>(&*p.source))
            }),
        }
    }

    /// Mutable access to the backing trie (materializing if paged, cloning if the
    /// `Rc` is shared), for builder closures that populate a chunk in place — e.g.
    /// `reduce_abelian`.
    pub fn updates_mut(&mut self) -> &mut UpdatesTyped<U> {
        if let ColChunk::Paged(_) = self {
            *self = ColChunk::Resident(Rc::new(into_trie(std::mem::take(self))));
        }
        match self {
            ColChunk::Resident(rc) => Rc::make_mut(rc),
            ColChunk::Paged(_) => unreachable!(),
        }
    }
}

/// Take a chunk's trie by value, fetching it if paged (and notifying the spiller
/// for budget accounting).
fn into_trie<U: ColumnarUpdate>(chunk: ColChunk<U>) -> UpdatesTyped<U> {
    match chunk {
        ColChunk::Resident(rc) => Rc::try_unwrap(rc).unwrap_or_else(|rc| (*rc).clone()),
        ColChunk::Paged(p) => match p.cache.get() {
            Some(rc) => (**rc).clone(),
            None => { spill::note_fetched(); spill::decode::<U>(&*p.source) }
        },
    }
}

// --- Container traits (batcher side, via `ContainerChunker<ColChunk>`) ---

impl<U: ColumnarUpdate> Accountable for ColChunk<U> {
    fn record_count(&self) -> i64 { Chunk::len(self) as i64 }
}

impl<U: ColumnarUpdate> SizableContainer for ColChunk<U> {
    // Absorb at `TARGET`, the grading size, so the chunker emits pre-graded chunks.
    fn at_capacity(&self) -> bool { Chunk::len(self) >= TARGET }
    // The trie grows as updates are pushed; nothing to pre-size.
    fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
}

impl<U: ColumnarUpdate> Consolidate for ColChunk<U> {
    fn len(&self) -> usize { Chunk::len(self) }
    fn clear(&mut self) { *self.updates_mut() = UpdatesTyped::default(); }
    fn consolidate_into(&mut self, target: &mut Self) {
        let taken = std::mem::take(self.updates_mut());
        *target.updates_mut() = taken.consolidate();
    }
}

impl<U: ColumnarUpdate> PushInto<((U::Key, U::Val), U::Time, U::Diff)> for ColChunk<U> {
    fn push_into(&mut self, item: ((U::Key, U::Val), U::Time, U::Diff)) {
        self.updates_mut().push_into(item);
    }
}

// --- Cursor (trace side), navigating the trie directly (cf. `OrdValCursor`) ---

/// A cursor over a [`ColChunk`], tracking the current key and value as absolute
/// indices into the trie's flat `keys.values` / `vals.values` columns.
pub struct ColChunkCursor<U: ColumnarUpdate> {
    key_cursor: usize,
    val_cursor: usize,
    phantom: PhantomData<U>,
}

impl<U: ColumnarUpdate> WithLayout for ColChunk<U> {
    type Layout = ColumnarLayout<U>;
}
impl<U: ColumnarUpdate> WithLayout for ColChunkCursor<U> {
    type Layout = ColumnarLayout<U>;
}

impl<U: ColumnarUpdate> Cursor for ColChunkCursor<U> {
    type Storage = ColChunk<U>;

    type KeyContainer = <ColumnarLayout<U> as Layout>::KeyContainer;
    type Key<'a> = <<ColumnarLayout<U> as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
    type ValContainer = <ColumnarLayout<U> as Layout>::ValContainer;
    type Val<'a> = <<ColumnarLayout<U> as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
    type ValOwn = <<ColumnarLayout<U> as Layout>::ValContainer as BatchContainer>::Owned;
    type TimeContainer = <ColumnarLayout<U> as Layout>::TimeContainer;
    type TimeGat<'a> = <<ColumnarLayout<U> as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
    type Time = <<ColumnarLayout<U> as Layout>::TimeContainer as BatchContainer>::Owned;
    type DiffContainer = <ColumnarLayout<U> as Layout>::DiffContainer;
    type DiffGat<'a> = <<ColumnarLayout<U> as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;
    type Diff = <<ColumnarLayout<U> as Layout>::DiffContainer as BatchContainer>::Owned;

    fn key_valid(&self, s: &Self::Storage) -> bool { self.key_cursor < s.trie().view().keys.values.len() }
    fn val_valid(&self, s: &Self::Storage) -> bool {
        let view = s.trie().view();
        self.key_cursor < view.keys.values.len()
            && self.val_cursor < child_range(view.vals.bounds, self.key_cursor).end
    }
    fn key<'a>(&self, s: &'a Self::Storage) -> Self::Key<'a> { s.trie().view().keys.values.get(self.key_cursor) }
    fn val<'a>(&self, s: &'a Self::Storage) -> Self::Val<'a> { s.trie().view().vals.values.get(self.val_cursor) }
    fn get_key<'a>(&self, s: &'a Self::Storage) -> Option<Self::Key<'a>> {
        if self.key_valid(s) { Some(self.key(s)) } else { None }
    }
    fn get_val<'a>(&self, s: &'a Self::Storage) -> Option<Self::Val<'a>> {
        if self.val_valid(s) { Some(self.val(s)) } else { None }
    }
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, s: &Self::Storage, mut logic: L) {
        if !self.val_valid(s) { return; }
        let view = s.trie().view();
        for t in child_range(view.times.bounds, self.val_cursor) {
            let time = view.times.values.get(t);
            for d in child_range(view.diffs.bounds, t) {
                logic(time, view.diffs.values.get(d));
            }
        }
    }
    fn step_key(&mut self, s: &Self::Storage) {
        self.key_cursor += 1;
        if self.key_valid(s) { self.rewind_vals(s); }
        else { self.key_cursor = s.trie().view().keys.values.len(); }
    }
    fn seek_key(&mut self, s: &Self::Storage, key: Self::Key<'_>) {
        let view = s.trie().view();
        let n = view.keys.values.len();
        let mut lo = self.key_cursor;
        trie_merger::gallop(view.keys.values, &mut lo, n, |x|
            <Coltainer<U::Key> as BatchContainer>::reborrow(x).lt(&<Coltainer<U::Key> as BatchContainer>::reborrow(key)));
        self.key_cursor = lo;
        if self.key_valid(s) { self.rewind_vals(s); }
    }
    fn step_val(&mut self, s: &Self::Storage) {
        self.val_cursor += 1;
        if !self.val_valid(s) {
            self.val_cursor = child_range(s.trie().view().vals.bounds, self.key_cursor).end;
        }
    }
    fn seek_val(&mut self, s: &Self::Storage, val: Self::Val<'_>) {
        if !self.key_valid(s) { return; }
        let view = s.trie().view();
        let upper = child_range(view.vals.bounds, self.key_cursor).end;
        let mut lo = self.val_cursor;
        trie_merger::gallop(view.vals.values, &mut lo, upper, |x|
            <Coltainer<U::Val> as BatchContainer>::reborrow(x).lt(&<Coltainer<U::Val> as BatchContainer>::reborrow(val)));
        self.val_cursor = lo;
    }
    fn rewind_keys(&mut self, s: &Self::Storage) { self.key_cursor = 0; self.rewind_vals(s); }
    fn rewind_vals(&mut self, s: &Self::Storage) {
        if self.key_valid(s) {
            self.val_cursor = child_range(s.trie().view().vals.bounds, self.key_cursor).start;
        }
    }
}

/// Wrap a non-empty resident trie as a chunk and append it to `out`.
fn emit<U: ColumnarUpdate>(updates: UpdatesTyped<U>, out: &mut VecDeque<ColChunk<U>>) {
    if updates.len() > 0 { out.push_back(ColChunk::Resident(Rc::new(updates))); }
}

/// Drop the empty diff lists `merge_pair`'s `write_diffs` leaves where updates
/// cancel — but only when some actually cancelled. With no cancellation every
/// time keeps its singleton diff, so `diffs.values.len() == times.values.len()`
/// and we skip [`UpdatesTyped::filter_zero`]'s rebuild entirely.
fn consolidated<U: ColumnarUpdate>(merged: UpdatesTyped<U>) -> UpdatesTyped<U> {
    if merged.diffs.values.len() == merged.times.values.len() { merged } else { merged.filter_zero() }
}

impl<U: ColumnarUpdate> Navigable for ColChunk<U>
where U::Time: 'static {
    type Cursor = ColChunkCursor<U>;

    fn cursor(&self) -> Self::Cursor {
        ColChunkCursor { key_cursor: 0, val_cursor: 0, phantom: PhantomData }
    }
}

impl<U: ColumnarUpdate> crate::trace::chunk::NavigableChunk for ColChunk<U>
where U::Time: 'static {
    fn bounds(&self) -> (
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
        (<Self::Cursor as Cursor>::Key<'_>, <Self::Cursor as Cursor>::Val<'_>, <Self::Cursor as Cursor>::TimeGat<'_>),
    ) {
        match self {
            ColChunk::Resident(rc) => {
                let view = rc.view();
                let (nk, nv, nt) = (view.keys.values.len(), view.vals.values.len(), view.times.values.len());
                // Sorted trie: the first/last of each flat column are the bounding triples.
                ((view.keys.values.get(0), view.vals.values.get(0), view.times.values.get(0)),
                 (view.keys.values.get(nk - 1), view.vals.values.get(nv - 1), view.times.values.get(nt - 1)))
            }
            ColChunk::Paged(p) => {
                // Read the resident metadata — no fetch.
                let m = &p.meta;
                ((m.fk.borrow().get(0), m.fv.borrow().get(0), m.ft.borrow().get(0)),
                 (m.lk.borrow().get(0), m.lv.borrow().get(0), m.lt.borrow().get(0)))
            }
        }
    }
}

impl<U: ColumnarUpdate> Chunk for ColChunk<U>
where U::Time: 'static {
    type Time = <<ColumnarLayout<U> as Layout>::TimeContainer as BatchContainer>::Owned;

    const TARGET: usize = TARGET;

    fn len(&self) -> usize {
        match self {
            ColChunk::Resident(rc) => rc.len(),
            ColChunk::Paged(p) => p.meta.len,
        }
    }

    /// Trie-native binary merge of the two deques' front chunks through their
    /// shared horizon, via [`trie_merger::merge_pair`]: it merges one input fully
    /// and a prefix of the other (the survey/`write_layer` bulk range-copy),
    /// leaving the survivor's suffix as a cursor we materialize with
    /// [`trie_merger::suffix_chunk`] and push back. The harness re-invokes.
    ///
    /// One pair per call (not a resumable drain): `merge_pair` surveys each batch
    /// from index 0, so re-passing a large survivor would re-survey its whole body
    /// every call (quadratic). Re-materializing the suffix as a standalone chunk
    /// keeps each survey bounded by the shrinking remainder instead.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let mut cursor1 = Some(((0, 0, 0), into_trie(in1.pop_front().unwrap())));
        let mut cursor2 = Some(((0, 0, 0), into_trie(in2.pop_front().unwrap())));
        emit(consolidated(trie_merger::merge_pair(&mut cursor1, &mut cursor2)), out);
        // Push the survivor's unconsumed suffix back to the front of its deque.
        if let Some((cursor, batch)) = cursor1 {
            let suffix = trie_merger::suffix_chunk(cursor, &batch);
            if suffix.len() > 0 { in1.push_front(ColChunk::Resident(Rc::new(suffix))); }
        }
        if let Some((cursor, batch)) = cursor2 {
            let suffix = trie_merger::suffix_chunk(cursor, &batch);
            if suffix.len() > 0 { in2.push_front(ColChunk::Resident(Rc::new(suffix))); }
        }
    }

    /// Partition the front chunk by `frontier` (keep `>=`, ship `<`), folding kept
    /// times into `residual`, via [`trie_merger::extract`]. One chunk per call.
    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<U::Time>,
        residual: &mut Antichain<U::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(chunk) = input.pop_front() else { return };
        trie_merger::extract(
            std::iter::once(into_trie(chunk)),
            frontier,
            residual,
            |c| emit(c, ship),
            |c| emit(c, keep),
        );
    }

    /// Advance times by `frontier`, consolidating each complete `(key, val)` group
    /// and withholding the last unless `done`.
    ///
    /// Streams the input one chunk at a time, holding at most a single chunk plus the
    /// withheld trailing `(key, val)` group (the `carry`) — it never pulls the whole
    /// chain into core. Cross-chunk consolidation happens *only* where a `(key, val)`
    /// straddles a boundary: the (un-advanced) carry is melded onto the next chunk's
    /// head, a sorted-chain meld that just stitches that one group. [`advance_trie`]
    /// then advances and consolidates the time runs *within* each chunk (keys / vals /
    /// diffs stay columnar refs, no global re-sort). Grading the emitted chunks is
    /// left to [`Chunk::settle`].
    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<U::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        // The withheld trailing `(key, val)` group, kept un-advanced so it melds
        // cleanly onto the next chunk's head if the group straddles the boundary.
        let mut carry: Option<UpdatesTyped<U>> = None;
        while let Some(chunk) = input.pop_front() {
            // Stitch the carry onto this chunk via a sorted-chain meld (touches only
            // the straddling group); otherwise take the chunk as-is.
            let combined = match carry.take() {
                None => into_trie(chunk),
                Some(c) => {
                    let mut builder = UpdatesBuilder::new_from(c);
                    builder.meld(&into_trie(chunk));
                    builder.done()
                }
            };
            if combined.len() == 0 { continue; }

            // Withhold the trailing `(key, val)` group (it may continue in the next
            // chunk). Its size is read from the resident bounds, no body scan.
            let tail = {
                let v = combined.view();
                let last_val = child_range(v.vals.bounds, v.keys.values.len() - 1).end - 1;
                let times = child_range(v.times.bounds, last_val);
                child_range(v.diffs.bounds, times.end - 1).end
                    - child_range(v.diffs.bounds, times.start).start
            };
            if tail == combined.len() {
                // A single `(key, val)` spans the chunk; hold it all as the carry.
                carry = Some(combined);
                continue;
            }
            let split = combined.len() - tail;
            let (keep, rest) = trie_merger::split_at(combined, split);
            carry = Some(rest);
            // Advance the now-complete groups and emit one chunk; `settle` grades.
            emit(advance_trie(keep, frontier), out);
        }
        // Flush the final carry: advance and emit if `done`, else hold for next call.
        if let Some(c) = carry {
            if done { emit(advance_trie(c, frontier), out); }
            else { input.push_front(ColChunk::Resident(Rc::new(c))); }
        }
    }

    /// Maximal packing via the harness [`pack`](crate::trace::chunk::pack): coalesce by melding
    /// the next trie onto the carry (adjacent chunks of a sorted, consolidated
    /// chain, so meld's "strictly greater first triple" precondition holds), split
    /// with [`trie_merger::split_at`], and seal through [`seal_chunk`] (the spill
    /// point — pages a committed chunk when a spiller is installed).
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        crate::trace::chunk::pack(
            input, done, out,
            |acc, next| {
                let mut build = UpdatesBuilder::new_from(into_trie(std::mem::take(acc)));
                build.meld(&into_trie(next));
                *acc = ColChunk::Resident(Rc::new(build.done()));
            },
            |chunk, n| {
                let (first, rest) = trie_merger::split_at(into_trie(chunk), n);
                (ColChunk::Resident(Rc::new(first)), ColChunk::Resident(Rc::new(rest)))
            },
            seal_chunk,
        );
    }
}

/// The columnar spill point: when a spiller is installed and over the high-water
/// mark, page a committed chunk out (serialize via [`spill::try_page`], keep
/// resident bounds + a fetch handle). Otherwise keep it resident.
fn seal_chunk<U: ColumnarUpdate>(chunk: ColChunk<U>) -> ColChunk<U> {
    let ColChunk::Resident(rc) = chunk else { return chunk };
    if !spill::active() { return ColChunk::Resident(rc); }
    let updates = Rc::try_unwrap(rc).unwrap_or_else(|rc| (*rc).clone());
    let meta = meta_of(&updates);
    match spill::try_page(updates) {
        Ok(source) => ColChunk::Paged(Rc::new(PagedChunk { meta, source, cache: OnceCell::new() })),
        Err(updates) => ColChunk::Resident(Rc::new(updates)),
    }
}

/// Advance the times in `keep` by `frontier` and rebuild the trie, consolidating
/// each `(key, val)`'s time run *in isolation*. Advancing preserves `(key, val)`
/// order, so only the per-group time runs can reorder — they are re-sorted and
/// merged locally (a no-op sort when advancing is monotone, e.g. total-order
/// times), never the whole trie. Keys / vals / diffs are copied by columnar ref;
/// a `(key, val)` whose diffs fully cancel is dropped.
fn advance_trie<U: ColumnarUpdate>(
    keep: UpdatesTyped<U>,
    frontier: AntichainRef<U::Time>,
) -> UpdatesTyped<U> {
    use crate::difference::{IsZero, Semigroup};
    let view = keep.view();
    let mut out = UpdatesTyped::<U>::default();
    let mut run: Vec<(U::Time, U::Diff)> = Vec::new();
    let mut time = U::Time::default();
    let mut any_key = false;

    for key_idx in 0..view.keys.values.len() {
        let mut key_emitted = false;
        for val_idx in child_range(view.vals.bounds, key_idx) {
            // Gather (advanced time, summed diff) for this `(key, val)`.
            run.clear();
            for time_idx in child_range(view.times.bounds, val_idx) {
                <U::Time as Columnar>::copy_from(&mut time, view.times.values.get(time_idx));
                time.advance_by(frontier);
                let mut diff = U::Diff::default();
                for diff_idx in child_range(view.diffs.bounds, time_idx) {
                    diff.plus_equals(&<U::Diff as Columnar>::into_owned(view.diffs.values.get(diff_idx)));
                }
                run.push((time.clone(), diff));
            }
            // Re-sort the run by advanced time and merge equal times, dropping zeros.
            run.sort_by(|a, b| a.0.cmp(&b.0));
            let (mut w, mut r) = (0, 0);
            while r < run.len() {
                let t = run[r].0.clone();
                let mut d = run[r].1.clone();
                r += 1;
                while r < run.len() && run[r].0 == t { d.plus_equals(&run[r].1); r += 1; }
                if !d.is_zero() { run[w] = (t, d); w += 1; }
            }
            run.truncate(w);
            if run.is_empty() { continue; }

            // Emit the surviving group, sealing bounds level by level (cf. `form`).
            if !key_emitted {
                out.keys.values.push(view.keys.values.get(key_idx));
                key_emitted = true;
            }
            out.vals.values.push(view.vals.values.get(val_idx));
            for (t, d) in &run {
                out.times.values.push(t);
                out.diffs.values.push(d);
                out.diffs.bounds.push(out.diffs.values.len() as u64);
            }
            out.times.bounds.push(out.times.values.len() as u64);
        }
        if key_emitted {
            out.vals.bounds.push(out.vals.values.len() as u64);
            any_key = true;
        }
    }
    if any_key { out.keys.bounds.push(out.keys.values.len() as u64); }
    out
}

#[cfg(test)]
mod test {
    use std::collections::VecDeque;
    use columnar::Push;
    use super::{ColChunk, Chunk};
    use crate::columnar::updates::UpdatesTyped;
    use crate::trace::chunk::merge_chains;
    use crate::trace::Navigable;

    type Upd = (u64, u64, u64, i64);

    // A sorted, consolidated columnar chunk from raw updates.
    fn chunk(updates: Vec<Upd>) -> ColChunk<Upd> {
        let mut u = UpdatesTyped::<Upd>::default();
        for (k, v, t, d) in updates { u.push((&k, &v, &t, &d)); }
        ColChunk::from_trie(u.consolidate())
    }

    // Flatten a chunk sequence back to its update stream.
    fn flat<I: IntoIterator<Item = ColChunk<Upd>>>(chunks: I) -> Vec<Upd> {
        chunks.into_iter().flat_map(|c| c.trie().iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect::<Vec<_>>()).collect()
    }

    // Cut a consolidated set into a chain of small chunks, so groups straddle boundaries.
    fn chain(updates: &[Upd], sz: usize) -> Vec<ColChunk<Upd>> {
        updates.chunks(sz).map(|c| chunk(c.to_vec())).collect()
    }

    // Property test: merging two multi-chunk chains (driven through `merge` by
    // `merge_chains`) reproduces the union of all updates, consolidated. Tiny
    // chunks force `(key, val)` groups to straddle chunk boundaries on both
    // sides, exercising the survey/`merge_pair` horizon and suffix push-back.
    #[test]
    fn merge_matches_reference() {
        use crate::consolidation::consolidate_updates;

        let mut seed = 0x2545F4914F6CDD1Du64;
        let mut rng = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

        fn gen(rng: &mut impl FnMut() -> u64, n: usize) -> Vec<Upd> {
            let mut v: Vec<Upd> = (0..n).map(|_| {
                let k = rng() % 20; let val = rng() % 3; let t = rng() % 8;
                let d = if rng() % 4 == 0 { -1 } else { 1 };
                (k, val, t, d)
            }).collect();
            let mut rows: Vec<((u64, u64), u64, i64)> = v.drain(..).map(|(k, val, t, d)| ((k, val), t, d)).collect();
            consolidate_updates(&mut rows);
            rows.into_iter().map(|((k, val), t, d)| (k, val, t, d)).collect()
        }

        for _ in 0..300 {
            let (n1, n2) = ((rng() as usize % 60) + 1, (rng() as usize % 60) + 1);
            let u1 = gen(&mut rng, n1);
            let u2 = gen(&mut rng, n2);
            if u1.is_empty() || u2.is_empty() { continue; }
            let sz = (rng() as usize % 5) + 1;

            let mut out = VecDeque::new();
            merge_chains(chain(&u1, sz), chain(&u2, sz), &mut out);
            let merged = flat(out);

            let mut reference: Vec<((u64, u64), u64, i64)> =
                u1.iter().chain(u2.iter()).map(|&(k, v, t, d)| ((k, v), t, d)).collect();
            consolidate_updates(&mut reference);
            let reference: Vec<Upd> = reference.into_iter().map(|((k, v), t, d)| (k, v, t, d)).collect();

            assert_eq!(merged, reference, "chunk size {sz}\n  u1={u1:?}\n  u2={u2:?}");
        }
    }

    // `settle` produces a maximal packing: chunks `<= TARGET`, adjacent pairs
    // summing past `TARGET`, contents preserved exactly.
    #[test]
    fn settle_maximal_packing() {
        use super::TARGET;
        use crate::trace::chunk::is_graded;

        let t = TARGET;
        let sizes = [t / 3, t / 3, t / 3, t, t / 2, t / 2, t, 1, t - 1];
        let total: usize = sizes.iter().sum();
        let mut key = 0u64;
        let mut input = VecDeque::new();
        let mut output = VecDeque::new();
        for &s in &sizes {
            let updates: Vec<Upd> = (0..s).map(|_| { let k = key; key += 1; (k, 0, 0, 1) }).collect();
            input.push_back(chunk(updates));
            ColChunk::settle(&mut input, false, &mut output);
        }
        ColChunk::settle(&mut input, true, &mut output);
        let chunks: Vec<_> = output.into();

        assert!(is_graded(&chunks), "not graded: {:?}", chunks.iter().map(Chunk::len).collect::<Vec<_>>());
        let got = flat(chunks);
        assert_eq!(got.len(), total);
        assert!(got.windows(2).all(|w| w[0].0 < w[1].0));
    }

    // The straddle-aware `ChunkBatch` cursor reconstructs the same grouped
    // updates as a flat reference, even when a key — and a `(key, val)`'s times —
    // span a chunk boundary.
    #[test]
    fn cursor_handles_straddle() {
        use crate::trace::cursor::Cursor;
        use crate::trace::Description;
        use crate::trace::chunk::ChunkBatch;
        use timely::progress::Antichain;

        let chunks = vec![
            chunk(vec![(0, 0, 0, 1), (1, 0, 0, 1), (1, 1, 0, 1)]),
            chunk(vec![(1, 1, 1, 1), (1, 2, 0, 1)]),
            chunk(vec![(2, 0, 0, 1)]),
        ];
        let desc = Description::new(
            Antichain::from_elem(0u64), Antichain::from_elem(2u64), Antichain::from_elem(0u64));
        let batch = ChunkBatch::new(chunks, desc);

        let mut cursor = batch.cursor();
        let got = cursor.to_vec(&batch, |k| *k, |v| *v);
        let want = vec![
            ((0u64, 0u64), vec![(0u64, 1i64)]),
            ((1, 0), vec![(0, 1)]),
            ((1, 1), vec![(0, 1), (1, 1)]),
            ((1, 2), vec![(0, 1)]),
            ((2, 0), vec![(0, 1)]),
        ];
        assert_eq!(got, want);
    }

    // Driving `ChunkBatchMerger` to completion with tiny `fuel` (so it suspends
    // and settles on nearly every tick) yields the same advanced-and-consolidated
    // batch as a one-shot reference, and that batch is graded. Exercises the
    // resumable merge -> advance -> settle pipeline end to end.
    #[test]
    fn batch_merger_resumable_matches_reference() {
        use crate::trace::{Description, Merger};
        use crate::trace::chunk::{ChunkBatch, ChunkBatchMerger, is_graded};
        use crate::trace::cursor::Cursor;
        use crate::consolidation::consolidate_updates;
        use timely::progress::Antichain;

        let mut seed = 0x9E3779B97F4A7C15u64;
        let mut rng = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

        fn gen(rng: &mut impl FnMut() -> u64) -> Vec<Upd> {
            let n = rng() as usize % 40 + 1;
            let mut rows: Vec<((u64, u64), u64, i64)> = (0..n).map(|_| {
                let k = rng() % 10; let val = rng() % 3; let t = rng() % 6;
                let d = if rng() % 4 == 0 { -1 } else { 1 };
                ((k, val), t, d)
            }).collect();
            consolidate_updates(&mut rows);
            rows.into_iter().map(|((k, v), t, d)| (k, v, t, d)).collect()
        }
        fn batch(updates: &[Upd], sz: usize) -> ChunkBatch<ColChunk<Upd>> {
            let chunks: Vec<_> = updates.chunks(sz).map(|c| chunk(c.to_vec())).collect();
            let desc = Description::new(
                Antichain::from_elem(0u64), Antichain::from_elem(10u64), Antichain::from_elem(0u64));
            ChunkBatch::new(chunks, desc)
        }
        fn read(b: &ChunkBatch<ColChunk<Upd>>) -> Vec<Upd> {
            let mut out = Vec::new();
            let mut c = b.cursor();
            while c.key_valid(b) {
                let k = *c.key(b);
                while c.val_valid(b) {
                    let v = *c.val(b);
                    c.map_times(b, |t, d| out.push(((k, v), *t, *d)));
                    c.step_val(b);
                }
                c.step_key(b);
            }
            consolidate_updates(&mut out);
            out.into_iter().map(|((k, v), t, d)| (k, v, t, d)).collect()
        }

        for _ in 0..200 {
            let u1 = gen(&mut rng);
            let u2 = gen(&mut rng);
            if u1.is_empty() || u2.is_empty() { continue; }
            let sz = (rng() as usize % 4) + 1;
            let f = rng() % 6;
            let (s1, s2) = (batch(&u1, sz), batch(&u2, sz));
            let frontier = Antichain::from_elem(f);

            let mut merger = ChunkBatchMerger::new(&s1, &s2, frontier.borrow());
            loop {
                let mut fuel = 1isize;
                merger.work(&s1, &s2, &mut fuel);
                if fuel > 0 { break; }
            }
            let result = merger.done();

            assert!(is_graded(&result.chunks), "ungraded result: {:?}",
                result.chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            let got = read(&result);
            let mut want: Vec<((u64, u64), u64, i64)> =
                u1.iter().chain(u2.iter()).map(|&(k, v, t, d)| ((k, v), t.max(f), d)).collect();
            consolidate_updates(&mut want);
            let want: Vec<Upd> = want.into_iter().map(|((k, v), t, d)| (k, v, t, d)).collect();
            assert_eq!(got, want, "fuel-driven merge mismatch\n  u1={u1:?}\n  u2={u2:?}\n  f={f}");
        }
    }

    // With a spiller installed and a tiny budget, `settle` pages its committed
    // chunks out; reads must transparently fetch them back and reproduce the
    // exact contents (exercises the trie byte codec + the OnceCell materialization).
    #[test]
    fn settle_pages_and_round_trips() {
        use std::cell::RefCell;
        use std::rc::Rc;
        use std::sync::Arc;
        use std::sync::atomic::Ordering::Relaxed;
        use crate::columnar::trace::spill::{self, BytesSource, BytesStore, SpillStats};

        // In-memory backing store: an arena of byte blobs.
        struct MemStore(Rc<RefCell<Vec<Vec<u8>>>>);
        struct MemSource(Rc<RefCell<Vec<Vec<u8>>>>, usize);
        impl BytesStore for MemStore {
            fn store(&mut self, bytes: &[u8]) -> Box<dyn BytesSource> {
                let mut a = self.0.borrow_mut();
                let id = a.len();
                a.push(bytes.to_vec());
                Box::new(MemSource(self.0.clone(), id))
            }
        }
        impl BytesSource for MemSource { fn load(&self) -> Vec<u8> { self.0.borrow()[self.1].clone() } }

        let arena = Rc::new(RefCell::new(Vec::new()));
        let stats = Arc::new(SpillStats::default());
        spill::install(1, Box::new(MemStore(arena)), stats.clone()); // budget 1 record → page everything

        // Many single-update chunks with distinct keys; settle coalesces and pages.
        let n = 5 * super::TARGET as u64;
        let mut input: VecDeque<_> = (0..n).map(|k| chunk(vec![(k, 0, 0, 1)])).collect();
        let mut out = VecDeque::new();
        ColChunk::settle(&mut input, true, &mut out);

        assert!(stats.spilled_chunks.load(Relaxed) > 0, "nothing was paged");
        assert!(out.iter().any(|c| matches!(c, ColChunk::Paged(_))), "no paged chunk in output");
        // Contents survive the disk round-trip exactly.
        let got = flat(out);
        let want: Vec<Upd> = (0..n).map(|k| (k, 0, 0, 1)).collect();
        assert_eq!(got, want);
        assert!(stats.fetched_chunks.load(Relaxed) > 0, "nothing was fetched back");

        spill::uninstall();
    }

    // A single `(key, val)` spanning every pushed chunk: `advance` makes no
    // progress until `done`, accumulating in the carry, and must still produce
    // the right advanced-and-consolidated result.
    #[test]
    fn advance_single_key_spanning_pushes() {
        use timely::progress::Antichain;
        let frontier = Antichain::from_elem(100u64);
        let n = 50u64;
        let mut q = VecDeque::new();
        let mut out = VecDeque::new();
        for t in 0..n {
            q.push_back(chunk(vec![(7, 0, t, 1)]));
            ColChunk::advance(&mut q, frontier.borrow(), false, &mut out);
        }
        ColChunk::advance(&mut q, frontier.borrow(), true, &mut out);
        assert_eq!(flat(out), vec![(7, 0, 100, n as i64)]);
    }

    // `advance` advances and consolidates complete `(key, val)` groups eagerly,
    // withholding the (possibly-growing) last group as the carry when not `done`.
    #[test]
    fn advance_emits_complete_groups_eagerly() {
        use timely::progress::Antichain;
        let frontier = Antichain::from_elem(5u64);
        // Group (0,0) is complete within this chunk; group (1,0) might still grow.
        let mut q = VecDeque::from([chunk(vec![(0, 0, 0, 1), (0, 0, 1, 1), (1, 0, 0, 1)])]);
        let mut out = VecDeque::new();
        ColChunk::advance(&mut q, frontier.borrow(), false, &mut out);
        // The trailing group (1,0) is withheld as the carry at the front of `input`.
        assert_eq!(q.len(), 1);
        assert_eq!(Chunk::len(&q[0]), 1);
        // Group (0,0)'s times {0,1} advanced to 5 and consolidated, emitted now.
        assert_eq!(flat(out), vec![(0, 0, 5, 2)]);
    }

    // Streaming the input one chunk at a time must yield exactly what a single
    // all-at-once flush does — the resumable path is the one-shot path cut at
    // group boundaries.
    #[test]
    fn advance_resumable_matches_oneshot() {
        use timely::progress::Antichain;
        let frontier = Antichain::from_elem(3u64);
        // Groups span chunk boundaries and carry several times each.
        let input = || vec![
            chunk(vec![(0, 0, 0, 1), (0, 0, 1, 1), (1, 0, 0, 1)]),
            chunk(vec![(1, 0, 5, 1), (1, 1, 0, 1), (2, 0, 0, 1)]),
            chunk(vec![(2, 0, 2, 1), (2, 0, 9, 1)]),
        ];
        let oneshot = {
            let mut q: VecDeque<_> = input().into();
            let mut out = VecDeque::new();
            ColChunk::advance(&mut q, frontier.borrow(), false, &mut out);
            ColChunk::advance(&mut q, frontier.borrow(), true, &mut out);
            flat(out)
        };
        let incremental = {
            let mut q = VecDeque::new();
            let mut out = VecDeque::new();
            for c in input() { q.push_back(c); ColChunk::advance(&mut q, frontier.borrow(), false, &mut out); }
            ColChunk::advance(&mut q, frontier.borrow(), true, &mut out);
            flat(out)
        };
        assert_eq!(oneshot, incremental);
        for u in &oneshot { assert!(u.2 >= 3); }
    }

    // Property test: driving `advance` resumably over many tiny chunks must match a
    // row oracle (advance each time to `max(t, frontier)`, then consolidate). Tiny
    // chunks force `(key, val)` groups — with several times each — to straddle
    // boundaries, exercising the meld / withhold / split path.
    #[test]
    fn advance_matches_row_reference() {
        use timely::progress::Antichain;
        use crate::consolidation::consolidate_updates;

        let mut seed = 0x2545F4914F6CDD1Du64;
        let mut rng = move || { seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17; seed };

        for _ in 0..200 {
            // A sorted, consolidated set over a small space, so groups collide and
            // a `(key, val)` carries several times.
            let n = rng() as usize % 60 + 1;
            let mut rows: Vec<((u64, u64), u64, i64)> = (0..n).map(|_| {
                let k = rng() % 8; let v = rng() % 3; let t = rng() % 6;
                let d = if rng() % 4 == 0 { -1 } else { 1 };
                ((k, v), t, d)
            }).collect();
            consolidate_updates(&mut rows);
            if rows.is_empty() { continue; }
            let f = rng() % 6;
            let frontier = Antichain::from_elem(f);
            let sz = rng() as usize % 5 + 1; // tiny chunks → heavy straddling

            // Drive resumably: push one chunk, advance(false), … then advance(true).
            let mut q = VecDeque::new();
            let mut out = VecDeque::new();
            for c in rows.chunks(sz) {
                q.push_back(chunk(c.iter().map(|&((k, v), t, d)| (k, v, t, d)).collect()));
                ColChunk::advance(&mut q, frontier.borrow(), false, &mut out);
            }
            ColChunk::advance(&mut q, frontier.borrow(), true, &mut out);
            let got = flat(out);

            // Oracle: advance each time (u64 total order → `max(t, f)`), consolidate.
            let mut want: Vec<((u64, u64), u64, i64)> =
                rows.iter().map(|&((k, v), t, d)| ((k, v), t.max(f), d)).collect();
            consolidate_updates(&mut want);
            let want: Vec<Upd> = want.into_iter().map(|((k, v), t, d)| (k, v, t, d)).collect();

            assert_eq!(got, want, "frontier {f}, chunk size {sz}, rows {rows:?}");
        }
    }
}
