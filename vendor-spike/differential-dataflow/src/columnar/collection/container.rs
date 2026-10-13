//! `RecordedUpdates<U>`: the columnar stream container.
//!
//! This is the CONTAINER tier of the columnar chunk: the type that flows on
//! dataflow edges (it is `Accountable` and `ContainerBytes`), built input-side
//! by the collection [`Builder`](super::Builder), shuffled by [`Pact`](super::Pact),
//! and melded into [`ColChunk`](crate::columnar::trace::ColChunk) batches by the
//! trace face's [`Chunker`](crate::columnar::trace::Chunker). It wraps the
//! columnar [`Updates`](crate::columnar::updates::Updates) trie with the
//! pre-consolidation record count timely's exchange accounting needs.

use crate::columnar::{layout, updates};

/// A thin wrapper around `Updates` that tracks the pre-consolidation record count
/// for timely's exchange accounting. This wrapper is the stream container type;
/// the `TrieChunker` strips it, passing bare `UpdatesTyped` into the merge batcher.
pub struct RecordedUpdates<U: layout::ColumnarUpdate> {
    /// The trie of `(key, val, time, diff)` updates.
    pub updates: updates::Updates<U>,
    /// Number of records in `updates` before consolidation.
    pub records: usize,
    /// Whether `updates` is known to be sorted and consolidated
    /// (no duplicate (key, val, time) triples, no zero diffs).
    pub consolidated: bool,
}

impl<U: layout::ColumnarUpdate> Default for RecordedUpdates<U> {
    fn default() -> Self { Self { updates: Default::default(), records: 0, consolidated: true } }
}

impl<U: layout::ColumnarUpdate> Clone for RecordedUpdates<U> {
    fn clone(&self) -> Self { Self { updates: self.updates.clone(), records: self.records, consolidated: self.consolidated } }
}

impl<U: layout::ColumnarUpdate> timely::Accountable for RecordedUpdates<U> {
    #[inline] fn record_count(&self) -> i64 { self.records as i64 }
}

impl<U: layout::ColumnarUpdate> timely::dataflow::channels::ContainerBytes for RecordedUpdates<U> {
    // Wire format: a 16-byte prefix (`records`, `consolidated`) then the bare
    // four-column `Updates` codec — so the wire and spill paths share one encoder
    // ([`Updates::write_to`](crate::columnar::updates::Updates::write_to)).
    fn from_bytes(mut bytes: timely::bytes::arc::Bytes) -> Self {
        let prefix = bytes.extract_to(16);
        let records      = u64::from_le_bytes(prefix[0..8].try_into().unwrap()) as usize;
        let consolidated = u64::from_le_bytes(prefix[8..16].try_into().unwrap()) != 0;
        RecordedUpdates { updates: updates::Updates::read_from(bytes), records, consolidated }
    }

    fn length_in_bytes(&self) -> usize {
        16 + self.updates.length_in_bytes()
    }

    fn into_bytes<W: std::io::Write>(&self, writer: &mut W) {
        writer.write_all(&(self.records as u64).to_le_bytes()).unwrap();
        writer.write_all(&(self.consolidated as u64).to_le_bytes()).unwrap();
        self.updates.write_to(writer);
    }
}

// Container trait impls for RecordedUpdates, enabling iterative scopes.
mod container_impls {
    use columnar::{Columnar, Index, Len, Push};
    use timely::progress::{Timestamp, timestamp::Refines};
    use crate::difference::Abelian;
    use crate::collection::containers::{Negate, Enter, Leave, ResultsIn};

    use crate::columnar::layout::ColumnarUpdate as Update;
    use crate::columnar::updates::{self, UpdatesTyped};
    use super::RecordedUpdates;

    impl<U: Update<Diff: Abelian>> Negate for RecordedUpdates<U> {
        fn negate(self) -> Self {
            use columnar::Container;
            let RecordedUpdates { mut updates, records, consolidated } = self;
            let view = updates.view();
            let old_diffs = view.diffs.values;
            let mut new_diffs = <<U::Diff as Columnar>::Container as Container>::with_capacity_for([old_diffs].into_iter());
            let mut owned = U::Diff::default();
            for i in 0..old_diffs.len() {
                columnar::Columnar::copy_from(&mut owned, old_diffs.get(i));
                owned.negate();
                new_diffs.push(&owned);
            }
            // TODO: avoid make_typed() call as we are overwriting.
            updates.diffs.make_typed().values = new_diffs;
            RecordedUpdates { updates, records, consolidated }
        }
    }

    impl<K, V, T1, T2, R> Enter<T1, T2> for RecordedUpdates<(K, V, T1, R)>
    where
        (K, V, T1, R): Update<Key=K, Val=V, Time=T1, Diff=R>,
        (K, V, T2, R): Update<Key=K, Val=V, Time=T2, Diff=R>,
        T1: Timestamp + Columnar + Default + Clone,
        T2: Refines<T1> + Columnar + Default + Clone,
        K: Columnar, V: Columnar, R: Columnar,
    {
        type InnerContainer = RecordedUpdates<(K, V, T2, R)>;
        fn enter(self) -> Self::InnerContainer {
            // Rebuild the time column from a borrowed view; keys/vals/diffs
            // move untouched, preserving any Stash::Bytes backing.
            use columnar::bytes::stash::Stash;
            let RecordedUpdates { updates, records, consolidated } = self;
            let times = updates.times.borrow();
            let times_values = times.values;
            let mut new_times = <<T2 as Columnar>::Container as Default>::default();
            let mut t1_owned = T1::default();
            for i in 0..times_values.len() {
                Columnar::copy_from(&mut t1_owned, times_values.get(i));
                let t2 = T2::to_inner(t1_owned.clone());
                new_times.push(&t2);
            }
            // TODO: Assumes Enter (to_inner) is order-preserving on times.
            // Deconstruct `updates` to reform with same parts but different time type.
            let updates::Updates { keys, vals, mut times, diffs } = updates;
            // TODO: Avoid make_typed() call, as we are overwriting.
            times.make_typed();
            let Stash::Typed(times_lists) = times else { unreachable!() };
            let times = Stash::Typed(updates::Lists {
                values: new_times,
                bounds: times_lists.bounds,
            });
            RecordedUpdates {
                updates: updates::Updates { keys, vals, times, diffs },
                records,
                consolidated,
            }
        }
    }

    impl<K, V, T1, T2, R> Leave<T1, T2> for RecordedUpdates<(K, V, T1, R)>
    where
        (K, V, T1, R): Update<Key=K, Val=V, Time=T1, Diff=R>,
        (K, V, T2, R): Update<Key=K, Val=V, Time=T2, Diff=R>,
        T1: Refines<T2> + Columnar + Default + Clone,
        T2: Timestamp + Columnar + Default + Clone,
        K: Columnar, V: Columnar, R: Columnar,
    {
        type OuterContainer = RecordedUpdates<(K, V, T2, R)>;
        fn leave(self) -> Self::OuterContainer {
            // Rebuild the time column from a borrowed view; keys/vals/diffs
            // move untouched. Distinct T1 times can collapse to the same T2
            // time, so the result is consolidated.
            use columnar::bytes::stash::Stash;
            let RecordedUpdates { updates, records, consolidated: _ } = self;
            let times = updates.times.borrow();
            let times_values = times.values;
            let mut new_times = <<T2 as Columnar>::Container as Default>::default();
            let mut t1_owned = T1::default();
            for i in 0..times_values.len() {
                Columnar::copy_from(&mut t1_owned, times_values.get(i));
                let t2: T2 = t1_owned.clone().to_outer();
                new_times.push(&t2);
            }
            let updates::Updates { keys, vals, mut times, diffs } = updates;
            // Extract `times` bounds via make_typed (one-column copy if Bytes-backed).
            times.make_typed();
            let Stash::Typed(times_lists) = times else { unreachable!() };
            let times = Stash::Typed(updates::Lists {
                values: new_times,
                bounds: times_lists.bounds,
            });
            let mid = updates::Updates { keys, vals, times, diffs };
            // Collapse adjacent (k,v,t2) duplicates created by `to_outer`.
            RecordedUpdates {
                updates: mid.into_typed().consolidate().into(),
                records,
                consolidated: true,
            }
        }
    }

    impl<U: Update> ResultsIn<<U::Time as Timestamp>::Summary> for RecordedUpdates<U> {
        fn results_in(self, step: &<U::Time as Timestamp>::Summary) -> Self {
            use timely::progress::PathSummary;
            // Apply results_in to each time; drop updates whose time maps to None.
            // This must rebuild the trie since some entries may be removed.
            let mut output = UpdatesTyped::<U>::default();
            let mut time_owned = U::Time::default();
            // TODO: Build all times first, and if no `None` outputs, can re-use k, v, d.
            for (k, v, t, d) in self.updates.view().iter() {
                Columnar::copy_from(&mut time_owned, t);
                if let Some(new_time) = step.results_in(&time_owned) {
                    output.push((k, v, &new_time, d));
                }
            }
            // TODO: Time advancement may not be order preserving, but .. it could be.
            // TODO: Before this is consolidated the above would need to be `form`ed.
            RecordedUpdates { updates: output.into(), records: self.records, consolidated: false }
        }
    }
}

#[cfg(test)]
mod test {
    use columnar::Push;
    use timely::dataflow::channels::ContainerBytes;
    use crate::columnar::updates::UpdatesTyped;
    use super::RecordedUpdates;

    type Upd = (u64, u64, u64, i64);

    // The wire codec must round-trip: `into_bytes` then `from_bytes` reproduces
    // the record count, the consolidated flag, and the four-column contents. The
    // body delegates to `Updates::write_to`/`read_from` (shared with spill); this
    // covers the 16-byte `records` / `consolidated` prefix the wire format adds,
    // and asserts `length_in_bytes` agrees with what `into_bytes` emits.
    #[test]
    fn recorded_updates_bytes_round_trip() {
        let rows: Vec<Upd> = vec![
            (0, 0, 0, 1), (0, 1, 0, 1), (1, 0, 0, 2), (1, 0, 3, -1), (2, 5, 7, 4),
        ];
        let mut trie = UpdatesTyped::<Upd>::default();
        for (k, v, t, d) in &rows { trie.push((k, v, t, d)); }
        let trie = trie.consolidate();
        let records = trie.len();
        let original = RecordedUpdates::<Upd> { updates: trie.into(), records, consolidated: true };
        let want: Vec<Upd> = original.updates.view().iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect();

        let mut buf = Vec::new();
        original.into_bytes(&mut buf);
        assert_eq!(buf.len(), original.length_in_bytes(), "length_in_bytes disagrees with into_bytes");

        let bytes = timely::bytes::arc::BytesMut::from(buf).freeze();
        let decoded = RecordedUpdates::<Upd>::from_bytes(bytes);

        assert_eq!(decoded.records, records);
        assert!(decoded.consolidated);
        let got: Vec<Upd> = decoded.updates.view().iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect();
        assert_eq!(got, want);
    }
}
