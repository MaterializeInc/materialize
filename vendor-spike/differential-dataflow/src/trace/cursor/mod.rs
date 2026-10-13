//! Traits and types for navigating order sequences of update tuples.
//!
//! The `Cursor` trait contains several methods for efficiently navigating ordered collections
//! of tuples of the form `(key, val, time, diff)`. The cursor is different from an iterator
//! both because it allows navigation on multiple levels (key and val), but also because it
//! supports efficient seeking (via the `seek_key` and `seek_val` methods).

pub mod cursor_list;

pub use self::cursor_list::CursorList;

use timely::progress::Timestamp;
use crate::lattice::Lattice;
use crate::difference::Semigroup;
use crate::trace::implementations::containers::BatchContainer;

/// A batch type that has a cursor for navigation.
///
/// This is the entry point for accessing batch data through cursors, and the place that opinions
/// about keys and values are introduced (via the `Cursor` associated type). Cut-and-merge assembly
/// is the trace's concern: [`TraceReader::batches_through`](crate::trace::TraceReader::batches_through)
/// selects the batches and the defaulted
/// [`TraceReader::cursor_through`](crate::trace::TraceReader::cursor_through) builds a [`CursorList`]
/// over their per-batch cursors.
pub trait Navigable {

    /// The cursor type.
    ///
    /// The cursor carries the layout opinions (keys, values, containers); a `Navigable` type only
    /// promises that it can produce one.
    type Cursor: Cursor<Storage = Self>;

    /// Acquire a cursor suitable for the instance.
    fn cursor(&self) -> Self::Cursor;
}

/// The cursor type for a trace's batches.
pub type BatchCursor<Tr> = <<Tr as crate::trace::TraceReader>::Batch as Navigable>::Cursor;

/// The borrowed key type of a trace's batch cursor.
pub type BatchKey<'a, Tr> = <BatchCursor<Tr> as Cursor>::Key<'a>;
/// The borrowed val type of a trace's batch cursor.
pub type BatchVal<'a, Tr> = <BatchCursor<Tr> as Cursor>::Val<'a>;
/// The owned val type of a trace's batch cursor.
pub type BatchValOwn<Tr> = <BatchCursor<Tr> as Cursor>::ValOwn;
/// The borrowed diff type of a trace's batch cursor.
pub type BatchDiffGat<'a, Tr> = <BatchCursor<Tr> as Cursor>::DiffGat<'a>;
/// The owned diff type of a trace's batch cursor.
pub type BatchDiff<Tr> = <BatchCursor<Tr> as Cursor>::Diff;
/// The borrowed time type of a trace's batch cursor.
pub type BatchTimeGat<'a, Tr> = <BatchCursor<Tr> as Cursor>::TimeGat<'a>;

/// Assembles a merged cursor over a sequence of batches.
///
/// The batches become the cursor's storage and are returned alongside the cursor; they must be kept
/// alive and handed to the cursor's navigation methods. This is the shared assembly behind
/// `TraceReader::cursor_through` and the per-round input cursors in `reduce` / `count` / `threshold`.
pub fn cursor_list<B: crate::trace::BatchReader + Navigable>(batches: Vec<B>) -> (CursorList<B::Cursor>, Vec<B>) {
    let cursors = batches.iter().map(|batch| batch.cursor()).collect::<Vec<_>>();
    let cursor = CursorList::new(cursors, &batches);
    (cursor, batches)
}

/// A cursor for navigating ordered `(key, val, time, diff)` updates.
pub trait Cursor {

    /// Storage required by the cursor.
    type Storage;

    /// Alias for a borrowed key.
    type Key<'a>: Copy + Ord;
    /// Alias for an owned val.
    type ValOwn: Clone + Ord;
    /// Alias for a borrowed val.
    type Val<'a>: Copy + Ord;
    /// Alias for an owned time.
    type Time: Lattice + Timestamp;
    /// Alias for a borrowed time.
    type TimeGat<'a>: Copy + Ord;
    /// Alias for an owned diff.
    type Diff: Semigroup + 'static;
    /// Alias for a borrowed diff.
    type DiffGat<'a>: Copy + Ord;

    /// Container for update keys.
    type KeyContainer: for<'a> BatchContainer<ReadItem<'a> = Self::Key<'a>>;
    /// Container for update vals.
    type ValContainer: for<'a> BatchContainer<ReadItem<'a> = Self::Val<'a>, Owned = Self::ValOwn>;
    /// Container for times.
    type TimeContainer: for<'a> BatchContainer<ReadItem<'a> = Self::TimeGat<'a>, Owned = Self::Time>;
    /// Container for diffs.
    type DiffContainer: for<'a> BatchContainer<ReadItem<'a> = Self::DiffGat<'a>, Owned = Self::Diff>;

    /// Construct an owned val from a reference.
    #[inline(always)] fn owned_val(val: Self::Val<'_>) -> Self::ValOwn { <Self::ValContainer as BatchContainer>::into_owned(val) }
    /// Construct an owned time from a reference.
    #[inline(always)] fn owned_time(time: Self::TimeGat<'_>) -> Self::Time { <Self::TimeContainer as BatchContainer>::into_owned(time) }
    /// Construct an owned diff from a reference.
    #[inline(always)] fn owned_diff(diff: Self::DiffGat<'_>) -> Self::Diff { <Self::DiffContainer as BatchContainer>::into_owned(diff) }
    /// Clones a reference time onto an owned time.
    #[inline(always)] fn clone_time_onto(time: Self::TimeGat<'_>, onto: &mut Self::Time) { <Self::TimeContainer as BatchContainer>::clone_onto(time, onto) }

    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self, storage: &Self::Storage) -> bool;
    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values for this key.
    fn val_valid(&self, storage: &Self::Storage) -> bool;

    /// A reference to the current key. Asserts if invalid.
    fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a>;
    /// A reference to the current value. Asserts if invalid.
    fn val<'a>(&self, storage: &'a Self::Storage) -> Self::Val<'a>;

    /// Returns a reference to the current key, if valid.
    fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Key<'a>>;
    /// Returns a reference to the current value, if valid.
    fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<Self::Val<'a>>;

    /// Applies `logic` to each pair of time and difference. Intended for mutation of the
    /// closure's scope.
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, logic: L);

    /// Advances the cursor to the next key.
    fn step_key(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified key.
    fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>);

    /// Advances the cursor to the next value.
    fn step_val(&mut self, storage: &Self::Storage);
    /// Advances the cursor to the specified value.
    fn seek_val(&mut self, storage: &Self::Storage, val: Self::Val<'_>);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self, storage: &Self::Storage);
    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self, storage: &Self::Storage);

    /// Loads `target` with all updates associated with the supplied `key`.
    ///
    /// First `target` is cleared, and then if we find `key` we populated it with each of its `(val, time, diff)` updates.
    /// If `meet` is supplied, the time is joined with each `time` in the updates, to advance the times before consolidation.
    fn populate_key<'a>(&mut self, storage: &'a Self::Storage, key: Self::Key<'a>, meet: Option<&Self::Time>, target: &mut crate::operators::EditList<Self::Val<'a>, Self::Time, Self::Diff>) {
        target.clear();
        self.seek_key(storage, key);
        if self.get_key(storage) == Some(key) {
            self.rewind_vals(storage);
            while let Some(val) = self.get_val(storage) {
                self.map_times(storage, |time, diff| {
                    use crate::lattice::Lattice;
                    let mut time = Self::owned_time(time);
                    if let Some(meet) = meet { time.join_assign(meet); }
                    target.push(time, Self::owned_diff(diff))
                });
                target.seal(val);
                self.step_val(storage);
            }
        }
    }

    /// Rewinds the cursor and outputs its contents to a Vec
    fn to_vec<K, IK, V, IV>(&mut self, storage: &Self::Storage, into_key: IK, into_val: IV) -> Vec<((K, V), Vec<(Self::Time, Self::Diff)>)>
    where
        IK: for<'a> Fn(Self::Key<'a>) -> K,
        IV: for<'a> Fn(Self::Val<'a>) -> V,
    {
        let mut out = Vec::new();
        self.rewind_keys(storage);
        while let Some(key) = self.get_key(storage) {
            self.rewind_vals(storage);
            while let Some(val) = self.get_val(storage) {
                let mut kv_out = Vec::new();
                self.map_times(storage, |ts, r| {
                    kv_out.push((Self::owned_time(ts), Self::owned_diff(r)));
                });
                out.push(((into_key(key), into_val(val)), kv_out));
                self.step_val(storage);
            }
            self.step_key(storage);
        }
        out
    }
}
