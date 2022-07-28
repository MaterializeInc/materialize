// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::ops::{ControlFlow, ControlFlow::Break, ControlFlow::Continue};
use std::time::Duration;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::now::EpochMillis;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::{Determinacy, InvalidUsage};
use crate::r#impl::paths::PartialBlobKey;
use crate::r#impl::trace::{FueledMergeReq, FueledMergeRes, Trace};
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::{PersistConfig, ShardId};

include!(concat!(
    env!("OUT_DIR"),
    "/mz_persist_client.r#impl.state.rs"
));

#[derive(Clone, Debug, PartialEq)]
pub struct ReaderState<T> {
    pub seqno: SeqNo,
    pub since: Antichain<T>,
    /// UNIX_EPOCH timestamp (in millis) of this reader's most recent heartbeat
    pub last_heartbeat_timestamp_ms: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriterState {
    /// UNIX_EPOCH timestamp (in millis) of this writer's most recent heartbeat
    pub last_heartbeat_timestamp_ms: u64,
}

/// A [Batch] but with the updates themselves stored externally.
///
/// [Batch]: differential_dataflow::trace::BatchReader
#[derive(Clone, Debug, PartialEq)]
pub struct HollowBatch<T> {
    /// Describes the times of the updates in the batch.
    pub desc: Description<T>,
    /// Pointers usable to retrieve the updates.
    pub keys: Vec<PartialBlobKey>,
    /// The number of updates in the batch.
    pub len: usize,
}

// TODO: Document invariants.
#[derive(Debug, Clone)]
pub struct StateCollections<T> {
    pub(crate) readers: HashMap<ReaderId, ReaderState<T>>,
    pub(crate) writers: HashMap<WriterId, WriterState>,

    pub(crate) trace: Trace<T>,
}

impl<T> StateCollections<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub fn register_reader(
        &mut self,
        reader_id: &ReaderId,
        seqno: SeqNo,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<Infallible, (Upper<T>, ReaderState<T>)> {
        // TODO: Handle if the reader or writer already exist (probably with a
        // retry).
        let read_cap = ReaderState {
            seqno,
            since: self.trace.since().clone(),
            last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
        };
        self.readers.insert(reader_id.clone(), read_cap.clone());
        Continue((Upper(self.trace.upper().clone()), read_cap))
    }

    pub fn register_writer(
        &mut self,
        writer_id: &WriterId,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<Infallible, (Upper<T>, WriterState)> {
        let writer_state = WriterState {
            last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
        };
        self.writers.insert(writer_id.clone(), writer_state.clone());
        Continue((Upper(self.trace.upper().clone()), writer_state))
    }

    pub fn clone_reader(
        &mut self,
        new_reader_id: &ReaderId,
        seqno: SeqNo,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<Infallible, ReaderState<T>> {
        // TODO: Handle if the reader already exists (probably with a retry).
        let read_cap = ReaderState {
            seqno,
            since: self.trace.since().clone(),
            last_heartbeat_timestamp_ms: heartbeat_timestamp_ms,
        };
        self.readers.insert(new_reader_id.clone(), read_cap.clone());
        Continue(read_cap)
    }

    pub fn compare_and_append(
        &mut self,
        batch: &HollowBatch<T>,
        writer_id: &WriterId,
    ) -> ControlFlow<Result<Upper<T>, InvalidUsage<T>>, Vec<FueledMergeReq<T>>> {
        if !self.writers.contains_key(writer_id) {
            return Break(Err(InvalidUsage::UnknownWriter(writer_id.clone())));
        }

        if PartialOrder::less_than(batch.desc.upper(), batch.desc.lower()) {
            return Break(Err(InvalidUsage::InvalidBounds {
                lower: batch.desc.lower().clone(),
                upper: batch.desc.upper().clone(),
            }));
        }

        // If the time interval is empty, the list of updates must also be
        // empty.
        if batch.desc.upper() == batch.desc.lower() && !batch.keys.is_empty() {
            return Break(Err(InvalidUsage::InvalidEmptyTimeInterval {
                lower: batch.desc.lower().clone(),
                upper: batch.desc.upper().clone(),
                keys: batch.keys.to_vec(),
            }));
        }

        let shard_upper = self.trace.upper();
        if shard_upper != batch.desc.lower() {
            return Break(Ok(Upper(shard_upper.clone())));
        }

        if batch.desc.upper() != batch.desc.lower() {
            self.trace.push_batch(batch.clone());
        }
        debug_assert_eq!(self.trace.upper(), batch.desc.upper());

        Continue(self.trace.take_merge_reqs())
    }

    pub fn apply_merge_res(&mut self, res: &FueledMergeRes<T>) -> ControlFlow<Infallible, bool> {
        let applied = self.trace.apply_merge_res(res);
        Continue(applied)
    }

    pub fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        seqno: SeqNo,
        new_since: &Antichain<T>,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<Infallible, Since<T>> {
        let reader_state = self.reader(reader_id);

        // Also use this as an opportunity to heartbeat the reader and downgrade
        // the seqno capability.
        reader_state.last_heartbeat_timestamp_ms = heartbeat_timestamp_ms;
        reader_state.seqno = seqno;

        let reader_current_since = if PartialOrder::less_than(&reader_state.since, new_since) {
            reader_state.since.clone_from(new_since);
            self.update_since();
            new_since.clone()
        } else {
            // No-op, but still commit the state change so that this gets
            // linearized.
            reader_state.since.clone()
        };

        Continue(Since(reader_current_since))
    }

    pub fn heartbeat_reader(
        &mut self,
        reader_id: &ReaderId,
        heartbeat_timestamp_ms: u64,
    ) -> ControlFlow<Infallible, ()> {
        let reader_state = self.reader(reader_id);
        reader_state.last_heartbeat_timestamp_ms = heartbeat_timestamp_ms;
        Continue(())
    }

    pub fn expire_reader(&mut self, reader_id: &ReaderId) -> ControlFlow<Infallible, bool> {
        let existed = self.readers.remove(reader_id).is_some();
        if existed {
            self.update_since();
        }
        // No-op if existed is false, but still commit the state change so that
        // this gets linearized.
        Continue(existed)
    }

    pub fn expire_writer(&mut self, writer_id: &WriterId) -> ControlFlow<Infallible, bool> {
        let existed = self.writers.remove(writer_id).is_some();
        // No-op if existed is false, but still commit the state change so that
        // this gets linearized.
        Continue(existed)
    }

    fn reader(&mut self, id: &ReaderId) -> &mut ReaderState<T> {
        self.readers
            .get_mut(id)
            // The only (tm) ways to hit this are (1) inventing a ReaderId
            // instead of getting it from Register or (2) if a lease expired.
            // (1) is a gross mis-use and (2) isn't implemented yet, so it feels
            // okay to leave this for followup work.
            .expect("TODO: Implement automatic lease renewals")
    }

    fn update_since(&mut self) {
        let mut readers = self.readers.values();
        let mut since = match readers.next() {
            Some(reader) => reader.since.clone(),
            None => {
                // If there are no current readers, leave `since` unchanged so
                // it doesn't regress.
                return;
            }
        };
        while let Some(reader) = readers.next() {
            since.meet_assign(&reader.since);
        }
        self.trace.downgrade_since(since);
    }
}

// TODO: Document invariants.
#[derive(Debug)]
pub struct State<K, V, T, D> {
    pub(crate) shard_id: ShardId,

    pub(crate) seqno: SeqNo,
    pub(crate) collections: StateCollections<T>,

    // According to the docs, PhantomData is to "mark things that act like they
    // own a T". State doesn't actually own K, V, or D, just the ability to
    // produce them. Using the `fn() -> T` pattern gets us the same variance as
    // T [1], but also allows State to correctly derive Send+Sync.
    //
    // [1]:
    //     https://doc.rust-lang.org/nomicon/phantom-data.html#table-of-phantomdata-patterns
    pub(crate) _phantom: PhantomData<fn() -> (K, V, D)>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for State<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            shard_id: self.shard_id.clone(),
            seqno: self.seqno.clone(),
            collections: self.collections.clone(),
            _phantom: self._phantom.clone(),
        }
    }
}

impl<K, V, T, D> State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    pub fn new(shard_id: ShardId) -> Self {
        State {
            shard_id,
            seqno: SeqNo::minimum(),
            collections: StateCollections {
                readers: HashMap::new(),
                writers: HashMap::new(),
                trace: Trace::default(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    pub fn upper(&self) -> Antichain<T> {
        self.collections.trace.upper().clone()
    }

    pub fn seqno_since(&self) -> SeqNo {
        let mut seqno_since = self.seqno;
        for cap in self.collections.readers.values() {
            seqno_since = std::cmp::min(seqno_since, cap.seqno);
        }
        seqno_since
    }

    pub fn clone_apply<R, E, WorkFn>(&self, work_fn: &mut WorkFn) -> ControlFlow<E, (R, Self)>
    where
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> ControlFlow<E, R>,
    {
        let mut new_state = State {
            shard_id: self.shard_id,
            seqno: self.seqno.next(),
            collections: self.collections.clone(),
            _phantom: PhantomData,
        };
        let work_ret = work_fn(new_state.seqno, &mut new_state.collections)?;
        Continue((work_ret, new_state))
    }

    /// Returns the batches that contain updates up to (and including) the given `as_of`. The
    /// result `Vec` contains blob keys, along with a [`Description`] of what updates in the
    /// referenced parts are valid to read.
    pub fn snapshot(
        &self,
        as_of: &Antichain<T>,
    ) -> Result<Result<Vec<HollowBatch<T>>, Upper<T>>, Since<T>> {
        if PartialOrder::less_than(as_of, self.collections.trace.since()) {
            return Err(Since(self.collections.trace.since().clone()));
        }
        let upper = self.collections.trace.upper();
        if PartialOrder::less_equal(upper, as_of) {
            return Ok(Err(Upper(upper.clone())));
        }

        let mut batches = Vec::new();
        self.collections.trace.map_batches(|b| {
            if PartialOrder::less_than(as_of, b.desc.lower()) {
                return;
            }
            batches.push(b.clone());
        });
        Ok(Ok(batches))
    }

    // NB: Unlike the other methods here, this one is read-only.
    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<Result<(), Upper<T>>, Since<T>> {
        if PartialOrder::less_than(as_of, self.collections.trace.since()) {
            return Err(Since(self.collections.trace.since().clone()));
        }
        let upper = self.collections.trace.upper();
        if PartialOrder::less_equal(upper, as_of) {
            return Ok(Err(Upper(upper.clone())));
        }
        Ok(Ok(()))
    }

    pub fn next_listen_batch(&self, frontier: &Antichain<T>) -> Option<HollowBatch<T>> {
        // TODO: Avoid the O(n^2) here: `next_listen_batch` is called once per
        // batch and this iterates through all batches to find the next one.
        let mut ret = None;
        self.collections.trace.map_batches(|b| {
            if ret.is_some() {
                return;
            }
            if PartialOrder::less_equal(b.desc.lower(), frontier)
                && PartialOrder::less_than(frontier, b.desc.upper())
            {
                ret = Some(b.clone());
            }
        });
        ret
    }

    pub fn readers_needing_expiration(&self, now_ms: EpochMillis) -> Vec<ReaderId> {
        // TODO: Give lots of extra leeway in this temporary hack version of
        // automatic expiry.
        let read_lease_duration = PersistConfig::FAKE_READ_LEASE_DURATION * 15;

        let mut ret = Vec::new();
        for (reader, state) in self.collections.readers.iter() {
            // TODO: We likely want to store the lease duration in state, so the
            // answer to which readers are considered expired doesn't depend on
            // the version of the code running.
            let time_since_last_heartbeat_ms =
                now_ms.saturating_sub(state.last_heartbeat_timestamp_ms);
            if Duration::from_millis(time_since_last_heartbeat_ms) > read_lease_duration {
                ret.push(reader.clone());
            }
        }
        ret
    }
}

/// Wrapper for Antichain that represents a Since
#[derive(Debug, PartialEq)]
pub struct Since<T>(pub Antichain<T>);

// When used as an error, Since is determinate.
impl<T> Determinacy for Since<T> {
    const DETERMINANT: bool = true;
}

/// Wrapper for Antichain that represents an Upper
#[derive(Debug, PartialEq)]
pub struct Upper<T>(pub Antichain<T>);

// When used as an error, Upper is determinate.
impl<T> Determinacy for Upper<T> {
    const DETERMINANT: bool = true;
}

#[cfg(test)]
mod tests {
    use mz_ore::now::SYSTEM_TIME;

    use super::*;

    use crate::InvalidUsage::{InvalidBounds, InvalidEmptyTimeInterval};

    fn hollow<T: Timestamp>(lower: T, upper: T, keys: &[&str], len: usize) -> HollowBatch<T> {
        HollowBatch {
            desc: Description::new(
                Antichain::from_elem(lower),
                Antichain::from_elem(upper),
                Antichain::from_elem(T::minimum()),
            ),
            keys: keys
                .iter()
                .map(|x| PartialBlobKey((*x).to_owned()))
                .collect(),
            len,
        }
    }

    #[test]
    fn downgrade_since() {
        let mut state = State::<(), (), u64, i64>::new(ShardId::new());
        let reader = ReaderId::new();
        let seqno = SeqNo::minimum();
        let now = SYSTEM_TIME.clone();
        let _ = state.collections.register_reader(&reader, seqno, now());

        // The shard global since == 0 initially.
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(0));

        // Greater
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, seqno, &Antichain::from_elem(2), now()),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Equal (no-op)
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, seqno, &Antichain::from_elem(2), now()),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Less (no-op)
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, seqno, &Antichain::from_elem(1), now()),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));

        // Create a second reader.
        let reader2 = ReaderId::new();
        let _ = state.collections.register_reader(&reader2, seqno, now());

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader2, seqno, &Antichain::from_elem(3), now()),
            Continue(Since(Antichain::from_elem(3)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Shard since == 3 when all readers have since >= 3.
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, seqno, &Antichain::from_elem(5), now()),
            Continue(Since(Antichain::from_elem(5)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since unaffected readers with since > shard since expiring.
        assert_eq!(state.collections.expire_reader(&reader), Continue(true));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Create a third reader.
        let reader3 = ReaderId::new();
        let _ = state.collections.register_reader(&reader3, seqno, now());

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader3, seqno, &Antichain::from_elem(10), now()),
            Continue(Since(Antichain::from_elem(10)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(3));

        // Shard since advances when reader with the minimal since expires.
        assert_eq!(state.collections.expire_reader(&reader2), Continue(true));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(10));

        // Shard since unaffected when all readers are expired.
        assert_eq!(state.collections.expire_reader(&reader3), Continue(true));
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(10));
    }

    #[test]
    fn compare_and_append() {
        mz_ore::test::init_logging();
        let mut state = State::<String, String, u64, i64>::new(ShardId::new()).collections;

        let writer_id = WriterId::new();
        let _ = state.register_writer(&writer_id, 0);

        // State is initially empty.
        assert_eq!(state.trace.num_spine_batches(), 0);
        assert_eq!(state.trace.num_hollow_batches(), 0);
        assert_eq!(state.trace.num_updates(), 0);

        // Cannot insert a batch with a lower != current shard upper.
        assert_eq!(
            state.compare_and_append(&hollow(1, 2, &["key1"], 1), &writer_id),
            Break(Ok(Upper(Antichain::from_elem(0))))
        );

        // Insert an empty batch with an upper > lower..
        assert!(state
            .compare_and_append(&hollow(0, 5, &[], 0), &writer_id)
            .is_continue());

        // Cannot insert a batch with a upper less than the lower.
        assert_eq!(
            state.compare_and_append(&hollow(5, 4, &["key1"], 1), &writer_id),
            Break(Err(InvalidBounds {
                lower: Antichain::from_elem(5),
                upper: Antichain::from_elem(4)
            }))
        );

        // Cannot insert a nonempty batch with an upper equal to lower.
        assert_eq!(
            state.compare_and_append(&hollow(5, 5, &["key1"], 1), &writer_id),
            Break(Err(InvalidEmptyTimeInterval {
                lower: Antichain::from_elem(5),
                upper: Antichain::from_elem(5),
                keys: vec![PartialBlobKey("key1".to_owned())],
            }))
        );

        // Can insert an empty batch with an upper equal to lower.
        assert!(state
            .compare_and_append(&hollow(5, 5, &[], 0), &writer_id)
            .is_continue());
    }

    #[test]
    fn snapshot() {
        mz_ore::test::init_logging();
        let now = SYSTEM_TIME.clone();

        let mut state = State::<String, String, u64, i64>::new(ShardId::new());
        // Cannot take a snapshot with as_of == shard upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Ok(Err(Upper(Antichain::from_elem(0))))
        );

        // Cannot take a snapshot with as_of > shard upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(5)),
            Ok(Err(Upper(Antichain::from_elem(0))))
        );

        let writer_id = WriterId::new();
        let _ = state.collections.register_writer(&writer_id, 0);

        // Advance upper to 5.
        assert!(state
            .collections
            .compare_and_append(&hollow(0, 5, &["key1"], 1), &writer_id)
            .is_continue());

        // Can take a snapshot with as_of < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1)]))
        );

        // Can take a snapshot with as_of >= shard since, as long as as_of < shard_upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(4)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1)]))
        );

        // Cannot take a snapshot with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(5)),
            Ok(Err(Upper(Antichain::from_elem(5))))
        );
        assert_eq!(
            state.snapshot(&Antichain::from_elem(6)),
            Ok(Err(Upper(Antichain::from_elem(5))))
        );

        let reader = ReaderId::new();
        // Advance the since to 2.
        let _ = state
            .collections
            .register_reader(&reader, SeqNo::minimum(), now());
        assert_eq!(
            state.collections.downgrade_since(
                &reader,
                SeqNo::minimum(),
                &Antichain::from_elem(2),
                now()
            ),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.trace.since(), &Antichain::from_elem(2));
        // Cannot take a snapshot with as_of < shard_since.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(1)),
            Err(Since(Antichain::from_elem(2)))
        );

        // Advance the upper to 10 via an empty batch.
        assert!(state
            .collections
            .compare_and_append(&hollow(5, 10, &[], 0), &writer_id)
            .is_continue());

        // Can still take snapshots at times < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(7)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1), hollow(5, 10, &[], 0)]))
        );

        // Cannot take snapshots with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(Err(Upper(Antichain::from_elem(10))))
        );

        // Advance upper to 15.
        assert!(state
            .collections
            .compare_and_append(&hollow(10, 15, &["key2"], 1), &writer_id)
            .is_continue());

        // Filter out batches whose lowers are less than the requested as of (the
        // batches that are too far in the future for the requested as_of).
        assert_eq!(
            state.snapshot(&Antichain::from_elem(9)),
            Ok(Ok(vec![hollow(0, 5, &["key1"], 1), hollow(5, 10, &[], 0)]))
        );

        // Don't filter out batches whose lowers are <= the requested as_of.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(Ok(vec![
                hollow(0, 5, &["key1"], 1),
                hollow(5, 10, &[], 0),
                hollow(10, 15, &["key2"], 1)
            ]))
        );

        assert_eq!(
            state.snapshot(&Antichain::from_elem(11)),
            Ok(Ok(vec![
                hollow(0, 5, &["key1"], 1),
                hollow(5, 10, &[], 0),
                hollow(10, 15, &["key2"], 1)
            ]))
        );
    }

    #[test]
    fn next_listen_batch() {
        mz_ore::test::init_logging();

        let mut state = State::<String, String, u64, i64>::new(ShardId::new());

        // Empty collection never has any batches to listen for, regardless of the
        // current frontier.
        assert_eq!(state.next_listen_batch(&Antichain::from_elem(0)), None);
        assert_eq!(state.next_listen_batch(&Antichain::new()), None);

        let writer_id = WriterId::new();
        let _ = state.collections.register_writer(&writer_id, 0);

        // Add two batches of data, one from [0, 5) and then another from [5, 10).
        assert!(state
            .collections
            .compare_and_append(&hollow(0, 5, &["key1"], 1), &writer_id)
            .is_continue());
        assert!(state
            .collections
            .compare_and_append(&hollow(5, 10, &["key2"], 1), &writer_id)
            .is_continue());

        // All frontiers in [0, 5) return the first batch.
        for t in 0..=4 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Some(hollow(0, 5, &["key1"], 1))
            );
        }

        // All frontiers in [5, 10) return the second batch.
        for t in 5..=9 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Some(hollow(5, 10, &["key2"], 1))
            );
        }

        // There is no batch currently available for t = 10.
        assert_eq!(state.next_listen_batch(&Antichain::from_elem(10)), None);

        // By definition, there is no frontier ever at the empty antichain which
        // is the time after all possible times.
        assert_eq!(state.next_listen_batch(&Antichain::new()), None);
    }

    #[test]
    fn expire_writer() {
        mz_ore::test::init_logging();

        let mut state = State::<String, String, u64, i64>::new(ShardId::new());

        let writer_id_one = WriterId::new();

        // Writer has not been registered and should be unable to write
        assert_eq!(
            state
                .collections
                .compare_and_append(&hollow(0, 2, &["key1"], 1), &writer_id_one),
            Break(Err(InvalidUsage::UnknownWriter(writer_id_one.clone())))
        );

        assert!(state
            .collections
            .register_writer(&writer_id_one, 0)
            .is_continue());

        let writer_id_two = WriterId::new();
        assert!(state
            .collections
            .register_writer(&writer_id_two, 0)
            .is_continue());

        // Writer is registered and is now eligible to write
        assert!(state
            .collections
            .compare_and_append(&hollow(0, 2, &["key1"], 1), &writer_id_one)
            .is_continue());

        assert!(state
            .collections
            .expire_writer(&writer_id_one)
            .is_continue());

        // Writer has been expired and should be fenced off from further writes
        assert_eq!(
            state
                .collections
                .compare_and_append(&hollow(2, 5, &["key2"], 1), &writer_id_one),
            Break(Err(InvalidUsage::UnknownWriter(writer_id_one.clone())))
        );

        // But other writers should still be able to write
        assert!(state
            .collections
            .compare_and_append(&hollow(2, 5, &["key2"], 1), &writer_id_two)
            .is_continue());
    }
}
