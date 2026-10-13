//! A generic cursor implementation merging multiple cursors.

use super::Cursor;

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and the the indices of cursors with
/// the minimum key and minimum value. It performs no clever management of these sets otherwise.
#[derive(Debug)]
pub struct CursorList<C> {
    cursors: Vec<C>,
    min_key: Vec<usize>,
    min_val: Vec<usize>,
}

impl<C: Cursor> CursorList<C> {
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(cursors: Vec<C>, storage: &[C::Storage]) -> Self  {
        let mut result = CursorList {
            cursors,
            min_key: Vec::new(),
            min_val: Vec::new(),
        };

        result.minimize_keys(storage);
        result
    }

    /// Initialize min_key with the indices of cursors with the minimum key.
    ///
    /// This method scans the current keys of each cursor, and tracks the indices
    /// of cursors whose key equals the minimum valid key seen so far. As it goes,
    /// if it observes an improved key it clears the current list, updates the
    /// minimum key, and continues.
    ///
    /// Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    /// in a consistent state as well.
    fn minimize_keys(&mut self, storage: &[C::Storage]) {

        self.min_key.clear();

        // We'll visit each non-`None` key, maintaining the indexes of the least keys in `self.min_key`.
        // An explicit min-tracking loop rather than `flat_map`: the adapter's inner `next` fails to
        // inline in hot callers (e.g. reduce's per-key replay) and surfaces as a hot `FlattenCompat::next`.
        let mut min_key = None;
        for (idx, cursor) in self.cursors.iter().enumerate() {
            if let Some(key) = cursor.get_key(&storage[idx]) {
                match min_key {
                    None => {
                        self.min_key.push(idx);
                        min_key = Some(key);
                    }
                    Some(min) => match key.cmp(&min) {
                        std::cmp::Ordering::Less => {
                            self.min_key.clear();
                            self.min_key.push(idx);
                            min_key = Some(key);
                        }
                        std::cmp::Ordering::Equal => {
                            self.min_key.push(idx);
                        }
                        std::cmp::Ordering::Greater => { }
                    }
                }
            }
        }

        self.minimize_vals(storage);
    }

    /// Initialize min_val with the indices of minimum key cursors with the minimum value.
    ///
    /// This method scans the current values of cursor with minimum keys, and tracks the
    /// indices of cursors whose value equals the minimum valid value seen so far. As it
    /// goes, if it observes an improved value it clears the current list, updates the minimum
    /// value, and continues.
    fn minimize_vals(&mut self, storage: &[C::Storage]) {

        self.min_val.clear();

        // We'll visit each non-`None` value, maintaining the indexes of the least values in `self.min_val`.
        // Explicit loop rather than `flat_map`, for the inlining reason noted in `minimize_keys`.
        let mut min_val = None;
        for idx in self.min_key.iter().cloned() {
            if let Some(val) = self.cursors[idx].get_val(&storage[idx]) {
                match min_val {
                    None => {
                        self.min_val.push(idx);
                        min_val = Some(val);
                    }
                    Some(min) => match val.cmp(&min) {
                        std::cmp::Ordering::Less => {
                            self.min_val.clear();
                            self.min_val.push(idx);
                            min_val = Some(val);
                        }
                        std::cmp::Ordering::Equal => {
                            self.min_val.push(idx);
                        }
                        std::cmp::Ordering::Greater => { }
                    }
                }
            }
        }
    }
}

impl<C: Cursor> Cursor for CursorList<C> {

    type Storage = Vec<C::Storage>;

    type Key<'a> = C::Key<'a>;
    type ValOwn = C::ValOwn;
    type Val<'a> = C::Val<'a>;
    type Time = C::Time;
    type TimeGat<'a> = C::TimeGat<'a>;
    type Diff = C::Diff;
    type DiffGat<'a> = C::DiffGat<'a>;
    type KeyContainer = C::KeyContainer;
    type ValContainer = C::ValContainer;
    type TimeContainer = C::TimeContainer;
    type DiffContainer = C::DiffContainer;

    // validation methods
    #[inline]
    fn key_valid(&self, _storage: &Vec<C::Storage>) -> bool { !self.min_key.is_empty() }
    #[inline]
    fn val_valid(&self, _storage: &Vec<C::Storage>) -> bool { !self.min_val.is_empty() }

    // accessors
    #[inline]
    fn key<'a>(&self, storage: &'a Vec<C::Storage>) -> Self::Key<'a> {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.cursors[self.min_key[0]].key_valid(&storage[self.min_key[0]]));
        self.cursors[self.min_key[0]].key(&storage[self.min_key[0]])
    }
    #[inline]
    fn val<'a>(&self, storage: &'a Vec<C::Storage>) -> Self::Val<'a> {
        debug_assert!(self.key_valid(storage));
        debug_assert!(self.val_valid(storage));
        debug_assert!(self.cursors[self.min_val[0]].val_valid(&storage[self.min_val[0]]));
        self.cursors[self.min_val[0]].val(&storage[self.min_val[0]])
    }
    #[inline]
    fn get_key<'a>(&self, storage: &'a Vec<C::Storage>) -> Option<Self::Key<'a>> {
        self.min_key.get(0).map(|idx| self.cursors[*idx].key(&storage[*idx]))
    }
    #[inline]
    fn get_val<'a>(&self, storage: &'a Vec<C::Storage>) -> Option<Self::Val<'a>> {
        self.min_val.get(0).map(|idx| self.cursors[*idx].val(&storage[*idx]))
    }

    #[inline]
    fn map_times<L: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Vec<C::Storage>, mut logic: L) {
        for &index in self.min_val.iter() {
            self.cursors[index].map_times(&storage[index], |t,d| logic(t,d));
        }
    }

    // key methods
    #[inline]
    fn step_key(&mut self, storage: &Vec<C::Storage>) {
        for &index in self.min_key.iter() {
            self.cursors[index].step_key(&storage[index]);
        }
        self.minimize_keys(storage);
    }
    #[inline]
    fn seek_key(&mut self, storage: &Vec<C::Storage>, key: Self::Key<'_>) {
        for (cursor, storage) in self.cursors.iter_mut().zip(storage) {
            cursor.seek_key(storage, key);
        }
        self.minimize_keys(storage);
    }

    // value methods
    #[inline]
    fn step_val(&mut self, storage: &Vec<C::Storage>) {
        for &index in self.min_val.iter() {
            self.cursors[index].step_val(&storage[index]);
        }
        self.minimize_vals(storage);
    }
    #[inline]
    fn seek_val(&mut self, storage: &Vec<C::Storage>, val: Self::Val<'_>) {
        for (cursor, storage) in self.cursors.iter_mut().zip(storage) {
            cursor.seek_val(storage, val);
        }
        self.minimize_vals(storage);
    }

    // rewinding methods
    #[inline]
    fn rewind_keys(&mut self, storage: &Vec<C::Storage>) {
        for (cursor, storage) in self.cursors.iter_mut().zip(storage) {
            cursor.rewind_keys(storage);
        }
        self.minimize_keys(storage);
    }
    #[inline]
    fn rewind_vals(&mut self, storage: &Vec<C::Storage>) {
        for &index in self.min_key.iter() {
            self.cursors[index].rewind_vals(&storage[index]);
        }
        self.minimize_vals(storage);
    }
}
