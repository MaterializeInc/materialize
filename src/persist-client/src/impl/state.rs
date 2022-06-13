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

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::{Determinacy, InvalidUsage};
use crate::read::ReaderId;
use crate::ShardId;

#[derive(Clone, Debug, PartialEq)]
pub struct ReadCapability<T> {
    pub seqno: SeqNo,
    pub since: Antichain<T>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HollowBatch<T> {
    pub(crate) desc: Description<T>,
    pub(crate) keys: Vec<String>,
    pub(crate) len: usize,
}

// TODO: Document invariants.
#[derive(Debug, Clone)]
pub struct StateCollections<T> {
    readers: HashMap<ReaderId, ReadCapability<T>>,

    since: Antichain<T>,
    trace: Vec<(Vec<String>, Description<T>)>,
}

impl<T> StateCollections<T>
where
    T: Timestamp + Lattice + Codec64,
{
    pub fn register(
        &mut self,
        seqno: SeqNo,
        reader_id: &ReaderId,
    ) -> ControlFlow<Infallible, (Upper<T>, ReadCapability<T>)> {
        // TODO: Handle if the reader or writer already exist (probably with a
        // retry).
        let read_cap = ReadCapability {
            seqno,
            since: self.since.clone(),
        };
        self.readers.insert(reader_id.clone(), read_cap.clone());
        Continue((Upper(self.upper()), read_cap))
    }

    pub fn clone_reader(
        &mut self,
        seqno: SeqNo,
        new_reader_id: &ReaderId,
    ) -> ControlFlow<Infallible, ReadCapability<T>> {
        // TODO: Handle if the reader already exists (probably with a retry).
        let read_cap = ReadCapability {
            seqno,
            since: self.since.clone(),
        };
        self.readers.insert(new_reader_id.clone(), read_cap.clone());
        Continue(read_cap)
    }

    pub fn compare_and_append(
        &mut self,
        keys: &[String],
        desc: &Description<T>,
    ) -> ControlFlow<Result<Upper<T>, InvalidUsage<T>>, ()> {
        if PartialOrder::less_than(desc.upper(), desc.lower()) {
            return Break(Err(InvalidUsage::InvalidBounds {
                lower: desc.lower().clone(),
                upper: desc.upper().clone(),
            }));
        }

        // If the time iterval is empty, the list of updates must also be empty.
        if desc.upper() == desc.lower() && !keys.is_empty() {
            return Break(Err(InvalidUsage::InvalidEmptyTimeInterval {
                lower: desc.lower().clone(),
                upper: desc.upper().clone(),
                keys: keys.to_vec(),
            }));
        }

        let shard_upper = self.upper();
        if &shard_upper != desc.lower() {
            return Break(Ok(Upper(shard_upper)));
        }

        self.push_batch(keys, desc);
        debug_assert_eq!(&self.upper(), desc.upper());

        Continue(())
    }

    pub fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> ControlFlow<Infallible, Since<T>> {
        let read_cap = self.reader(reader_id);
        let reader_current_since = if PartialOrder::less_than(&read_cap.since, new_since) {
            read_cap.since.clone_from(new_since);
            self.update_since();
            new_since.clone()
        } else {
            // No-op, but still commit the state change so that this gets
            // linearized.
            read_cap.since.clone()
        };
        Continue(Since(reader_current_since))
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

    fn upper(&self) -> Antichain<T> {
        self.trace.last().map_or_else(
            || Antichain::from_elem(T::minimum()),
            |(_, desc)| desc.upper().clone(),
        )
    }

    fn reader(&mut self, id: &ReaderId) -> &mut ReadCapability<T> {
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
        self.since = since;
    }

    fn push_batch(&mut self, keys: &[String], desc: &Description<T>) {
        // Sneaky optimization! If there are no keys in this batch, we can
        // simply extend the description of the last one (assuming there is
        // one). This will be an absurdly common case in actual usage because
        // empty batches are used to communicate progress and most things will
        // be low-traffic.
        //
        // To make things easier to reason about, we avoid mutating any batch
        // that actually has data, and only apply the optimization if the most
        // recent batch _also_ has no keys. We call these batches with no keys
        // "padding batches".
        if let Some((last_batch_keys, last_desc)) = self.trace.last_mut() {
            if keys.is_empty() && last_batch_keys.is_empty() {
                *last_desc = Description::new(
                    last_desc.lower().clone(),
                    desc.upper().clone(),
                    last_desc.since().clone(),
                );
                return;
            }
        }
        self.trace.push((keys.to_owned(), desc.clone()));
    }
}

// TODO: Document invariants.
#[derive(Debug)]
pub struct State<K, V, T, D> {
    shard_id: ShardId,

    seqno: SeqNo,
    collections: StateCollections<T>,

    // According to the docs, PhantomData is to "mark things that act like they
    // own a T". State doesn't actually own K, V, or D, just the ability to
    // produce them. Using the `fn() -> T` pattern gets us the same variance as
    // T [1], but also allows State to correctly derive Send+Sync.
    //
    // [1]:
    //     https://doc.rust-lang.org/nomicon/phantom-data.html#table-of-phantomdata-patterns
    _phantom: PhantomData<fn() -> (K, V, D)>,
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
                since: Antichain::from_elem(T::minimum()),
                trace: Vec::new(),
            },
            _phantom: PhantomData,
        }
    }

    pub fn decode(buf: &[u8]) -> Result<Self, InvalidUsage<T>> {
        let state: StateRollupMeta = bincode::deserialize(buf)
            .map_err(|err| format!("unable to decode state: {}", err))
            // We received a State that we couldn't decode. This could happen if
            // persist messes up backward/forward compatibility, if the durable
            // data was corrupted, or if operations messes up deployment. In any
            // case, fail loudly.
            .expect("internal error: invalid encoded state");
        Self::try_from(&state)
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    pub fn upper(&self) -> Antichain<T> {
        self.collections.upper()
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
    ) -> Result<Result<Vec<(String, Description<T>)>, Upper<T>>, Since<T>> {
        if PartialOrder::less_than(as_of, &self.collections.since) {
            return Err(Since(self.collections.since.clone()));
        }
        let upper = self.collections.upper();
        if PartialOrder::less_equal(&upper, as_of) {
            return Ok(Err(Upper(upper)));
        }
        let batches = self
            .collections
            .trace
            .iter()
            .filter(|(_keys, desc)| !PartialOrder::less_than(as_of, desc.lower()))
            .flat_map(|(keys, desc)| keys.iter().map(|key| (key.clone(), desc.clone())))
            .collect();

        Ok(Ok(batches))
    }

    // NB: Unlike the other methods here, this one is read-only.
    pub fn verify_listen(&self, as_of: &Antichain<T>) -> Result<Result<(), Upper<T>>, Since<T>> {
        if PartialOrder::less_than(as_of, &self.collections.since) {
            return Err(Since(self.collections.since.clone()));
        }
        let upper = self.collections.upper();
        if PartialOrder::less_equal(&upper, as_of) {
            return Ok(Err(Upper(upper)));
        }
        Ok(Ok(()))
    }

    pub fn next_listen_batch(
        &self,
        frontier: &Antichain<T>,
    ) -> Option<(&[String], &Description<T>)> {
        // TODO: Avoid the O(n^2) here: `next_listen_batch` is called once per
        // batch and this iterates through all batches to find the next one.
        for (keys, desc) in self.collections.trace.iter() {
            if PartialOrder::less_equal(desc.lower(), frontier)
                && PartialOrder::less_than(frontier, desc.upper())
            {
                return Some((keys.as_slice(), desc));
            }
        }
        return None;
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

#[derive(Debug, Serialize, Deserialize)]
struct AntichainMeta(Vec<[u8; 8]>);

#[derive(Debug, Serialize, Deserialize)]
pub struct DescriptionMeta {
    lower: AntichainMeta,
    upper: AntichainMeta,
    since: AntichainMeta,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateRollupMeta {
    shard_id: ShardId,
    key_codec: String,
    val_codec: String,
    ts_codec: String,
    diff_codec: String,

    seqno: SeqNo,
    readers: Vec<(ReaderId, AntichainMeta, SeqNo)>,
    since: AntichainMeta,
    trace: Vec<(Vec<String>, DescriptionMeta)>,
}

mod codec_impls {
    use std::marker::PhantomData;

    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::trace::Description;
    use mz_persist_types::{Codec, Codec64};
    use timely::progress::{Antichain, Timestamp};

    use crate::error::InvalidUsage;
    use crate::r#impl::state::{
        AntichainMeta, DescriptionMeta, ReadCapability, State, StateCollections, StateRollupMeta,
    };

    impl<K, V, T, D> Codec for State<K, V, T, D>
    where
        K: Codec,
        V: Codec,
        T: Timestamp + Lattice + Codec64,
        D: Codec64,
    {
        fn codec_name() -> String {
            "StateRollupMeta".into()
        }

        fn encode<B>(&self, buf: &mut B)
        where
            B: bytes::BufMut,
        {
            let bytes = bincode::serialize(&StateRollupMeta::from(self))
                .expect("unable to serialize BlobState");
            buf.put_slice(&bytes);
        }

        fn decode<'a>(buf: &'a [u8]) -> Result<Self, String> {
            let state: StateRollupMeta = bincode::deserialize(buf)
                .map_err(|err| format!("unable to decode state: {}", err))?;
            // This map_err goes away when we do incremental state.
            State::try_from(&state).map_err(|err| err.to_string())
        }
    }

    impl<K, V, T, D> From<&State<K, V, T, D>> for StateRollupMeta
    where
        K: Codec,
        V: Codec,
        T: Codec64,
        D: Codec64,
    {
        fn from(x: &State<K, V, T, D>) -> Self {
            StateRollupMeta {
                shard_id: x.shard_id,
                seqno: x.seqno,
                key_codec: K::codec_name(),
                val_codec: V::codec_name(),
                ts_codec: T::codec_name(),
                diff_codec: D::codec_name(),
                readers: x
                    .collections
                    .readers
                    .iter()
                    .map(|(id, cap)| (id.clone(), (&cap.since).into(), cap.seqno))
                    .collect(),
                since: (&x.collections.since).into(),
                trace: x
                    .collections
                    .trace
                    .iter()
                    .map(|(key, desc)| (key.clone(), desc.into()))
                    .collect(),
            }
        }
    }

    impl<K, V, T, D> TryFrom<&StateRollupMeta> for State<K, V, T, D>
    where
        K: Codec,
        V: Codec,
        T: Timestamp + Codec64,
        D: Codec64,
    {
        type Error = InvalidUsage<T>;

        fn try_from(x: &StateRollupMeta) -> Result<Self, Self::Error> {
            if K::codec_name() != x.key_codec
                || V::codec_name() != x.val_codec
                || T::codec_name() != x.ts_codec
                || D::codec_name() != x.diff_codec
            {
                return Err(InvalidUsage::CodecMismatch {
                    requested: (
                        K::codec_name(),
                        V::codec_name(),
                        T::codec_name(),
                        D::codec_name(),
                    ),
                    actual: (
                        x.key_codec.to_owned(),
                        x.val_codec.to_owned(),
                        x.ts_codec.to_owned(),
                        x.diff_codec.to_owned(),
                    ),
                });
            }

            let readers = x
                .readers
                .iter()
                .map(|(id, since, seqno)| {
                    let cap = ReadCapability {
                        since: since.into(),
                        seqno: *seqno,
                    };
                    (id.clone(), cap)
                })
                .collect();
            let since = (&x.since).into();
            let trace = x
                .trace
                .iter()
                .map(|(key, desc)| (key.clone(), desc.into()))
                .collect();
            let collections = StateCollections {
                readers,
                since,
                trace,
            };
            Ok(State {
                shard_id: x.shard_id,
                seqno: x.seqno,
                collections,
                _phantom: PhantomData,
            })
        }
    }

    impl<T: Codec64> From<&Description<T>> for DescriptionMeta {
        fn from(x: &Description<T>) -> Self {
            DescriptionMeta {
                lower: x.lower().into(),
                upper: x.upper().into(),
                since: x.since().into(),
            }
        }
    }

    impl<T: Timestamp + Codec64> From<&DescriptionMeta> for Description<T> {
        fn from(x: &DescriptionMeta) -> Self {
            Description::new((&x.lower).into(), (&x.upper).into(), (&x.since).into())
        }
    }

    impl<T: Codec64> From<&Antichain<T>> for AntichainMeta {
        fn from(x: &Antichain<T>) -> Self {
            AntichainMeta(x.elements().iter().map(|x| T::encode(x)).collect())
        }
    }

    impl<T: Timestamp + Codec64> From<&AntichainMeta> for Antichain<T> {
        fn from(x: &AntichainMeta) -> Self {
            Antichain::from(x.0.iter().map(|x| T::decode(*x)).collect::<Vec<_>>())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::InvalidUsage::{InvalidBounds, InvalidEmptyTimeInterval};

    fn desc<T: Timestamp>(lower: T, upper: T) -> Description<T> {
        Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(T::minimum()),
        )
    }

    #[test]
    fn downgrade_since() {
        let mut state = State::<(), (), u64, i64>::new(ShardId::new());
        let reader = ReaderId::new();
        let _ = state.collections.register(SeqNo::minimum(), &reader);

        // The shard global since == 0 initially.
        assert_eq!(state.collections.since, Antichain::from_elem(0));

        // Greater
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, &Antichain::from_elem(2)),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(2));
        // Equal (no-op)
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, &Antichain::from_elem(2)),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(2));
        // Less (no-op)
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, &Antichain::from_elem(1)),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(2));

        // Create a second reader.
        let reader2 = ReaderId::new();
        let _ = state.collections.register(SeqNo::minimum(), &reader2);

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader2, &Antichain::from_elem(3)),
            Continue(Since(Antichain::from_elem(3)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(2));
        // Shard since == 3 when all readers have since >= 3.
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, &Antichain::from_elem(5)),
            Continue(Since(Antichain::from_elem(5)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(3));

        // Shard since unaffected readers with since > shard since expiring.
        assert_eq!(state.collections.expire_reader(&reader), Continue(true));
        assert_eq!(state.collections.since, Antichain::from_elem(3));

        // Create a third reader.
        let reader3 = ReaderId::new();
        let _ = state.collections.register(SeqNo::minimum(), &reader3);

        // Shard since doesn't change until the meet (min) of all reader sinces changes.
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader3, &Antichain::from_elem(10)),
            Continue(Since(Antichain::from_elem(10)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(3));

        // Shard since advances when reader with the minimal since expires.
        assert_eq!(state.collections.expire_reader(&reader2), Continue(true));
        assert_eq!(state.collections.since, Antichain::from_elem(10));

        // Shard since unaffected when all readers are expired.
        assert_eq!(state.collections.expire_reader(&reader3), Continue(true));
        assert_eq!(state.collections.since, Antichain::from_elem(10));
    }

    #[test]
    fn empty_batch_optimization() {
        mz_ore::test::init_logging();

        let mut state = State::<String, String, u64, i64>::new(ShardId::new()).collections;

        // Initial empty batch should result in a padding batch.
        assert_eq!(state.compare_and_append(&[], &desc(0, 1)), Continue(()));
        assert_eq!(state.trace.len(), 1);

        // Writing data should create a new batch, so now there's two.
        assert_eq!(
            state.compare_and_append(&["key1".to_owned()], &desc(1, 2)),
            Continue(())
        );
        assert_eq!(state.trace.len(), 2);

        // The first empty batch after one with data doesn't get squished in,
        // instead becoming a padding batch.
        assert_eq!(state.compare_and_append(&[], &desc(2, 3)), Continue(()));
        assert_eq!(state.trace.len(), 3);

        // More empty batches should all get squished into the existing padding
        // batch.
        assert_eq!(state.compare_and_append(&[], &desc(3, 4)), Continue(()));
        assert_eq!(state.compare_and_append(&[], &desc(4, 5)), Continue(()));
        assert_eq!(state.trace.len(), 3);

        // Try it all again with a second non-empty batch, this one with 2 keys,
        // and then some more empty batches.
        assert_eq!(
            state.compare_and_append(&["key2".to_owned(), "key3".to_owned()], &desc(5, 6)),
            Continue(())
        );
        assert_eq!(state.trace.len(), 4);
        assert_eq!(state.compare_and_append(&[], &desc(6, 7)), Continue(()));
        assert_eq!(state.trace.len(), 5);
        assert_eq!(state.compare_and_append(&[], &desc(7, 8)), Continue(()));
        assert_eq!(state.compare_and_append(&[], &desc(8, 9)), Continue(()));
        assert_eq!(state.trace.len(), 5);

        // Confirm that we still have all the keys.
        let actual = state
            .trace
            .iter()
            .flat_map(|(keys, _)| keys.iter())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec!["key1", "key2", "key3"]);
    }

    #[test]
    fn compare_and_append() {
        mz_ore::test::init_logging();
        let mut state = State::<String, String, u64, i64>::new(ShardId::new()).collections;

        // State is initally empty.
        assert_eq!(state.trace.len(), 0);

        // Cannot insert a batch with a lower != current shard upper.
        assert_eq!(
            state.compare_and_append(&["key1".to_owned()], &desc(1, 2)),
            Break(Ok(Upper(Antichain::from_elem(0))))
        );

        // Insert an empty batch with an upper > lower..
        assert_eq!(state.compare_and_append(&[], &desc(0, 5)), Continue(()));

        // Cannot insert a batch with a upper less than the lower.
        assert_eq!(
            state.compare_and_append(&["key1".to_owned()], &desc(5, 4)),
            Break(Err(InvalidBounds {
                lower: Antichain::from_elem(5),
                upper: Antichain::from_elem(4)
            }))
        );

        // Cannot insert a nonempty batch with an upper equal to lower.
        assert_eq!(
            state.compare_and_append(&["key1".to_owned()], &desc(5, 5)),
            Break(Err(InvalidEmptyTimeInterval {
                lower: Antichain::from_elem(5),
                upper: Antichain::from_elem(5),
                keys: vec!["key1".to_owned()],
            }))
        );

        // Can insert an empty batch with an upper equal to lower.
        assert_eq!(state.compare_and_append(&[], &desc(5, 5)), Continue(()));
    }

    #[test]
    fn snapshot() {
        mz_ore::test::init_logging();

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

        // Advance upper to 5.
        assert_eq!(
            state
                .collections
                .compare_and_append(&["key1".to_owned()], &desc(0, 5)),
            Continue(())
        );

        // Can take a snapshot with as_of < upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(0)),
            Ok(Ok(vec![("key1".to_owned(), desc(0, 5))]))
        );

        // Can take a snapshot with as_of >= shard since, as long as as_of < shard_upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(4)),
            Ok(Ok(vec![("key1".to_owned(), desc(0, 5))]))
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
        let _ = state.collections.register(SeqNo::minimum(), &reader);
        assert_eq!(
            state
                .collections
                .downgrade_since(&reader, &Antichain::from_elem(2)),
            Continue(Since(Antichain::from_elem(2)))
        );
        assert_eq!(state.collections.since, Antichain::from_elem(2));
        // Cannot take a snapshot with as_of < shard_since.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(1)),
            Err(Since(Antichain::from_elem(2)))
        );

        // Advance the upper to 10 via an empty batch.
        assert_eq!(
            state.collections.compare_and_append(&[], &desc(5, 10)),
            Continue(())
        );

        // Can still take snapshots at times < upper, but the empty batch is missing
        // because of a performance optimization.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(7)),
            Ok(Ok(vec![("key1".to_owned(), desc(0, 5))]))
        );

        // Cannot take snapshots with as_of >= upper.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(Err(Upper(Antichain::from_elem(10))))
        );

        // Advance upper to 15.
        assert_eq!(
            state
                .collections
                .compare_and_append(&["key2".to_owned()], &desc(10, 15)),
            Continue(())
        );

        // Filter out batches whose lowers are less than the requested as of (the
        // batches that are too far in the future for the requested as_of).
        assert_eq!(
            state.snapshot(&Antichain::from_elem(9)),
            Ok(Ok(vec![("key1".to_owned(), desc(0, 5))]))
        );

        // Don't filter out batches whose lowers are <= the requested as_of.
        assert_eq!(
            state.snapshot(&Antichain::from_elem(10)),
            Ok(Ok(vec![
                ("key1".to_owned(), desc(0, 5)),
                ("key2".to_owned(), desc(10, 15))
            ]))
        );

        assert_eq!(
            state.snapshot(&Antichain::from_elem(11)),
            Ok(Ok(vec![
                ("key1".to_owned(), desc(0, 5)),
                ("key2".to_owned(), desc(10, 15))
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

        // Add two batches of data, one from [0, 5) and then another from [5, 10).
        assert_eq!(
            state
                .collections
                .compare_and_append(&["key1".to_owned()], &desc(0, 5)),
            Continue(())
        );
        assert_eq!(
            state
                .collections
                .compare_and_append(&["key2".to_owned()], &desc(5, 10)),
            Continue(())
        );

        // All frontiers in [0, 5) return the first batch.
        for t in 0..=4 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Some((vec!["key1".to_owned()].as_slice(), &desc(0, 5)))
            );
        }

        // All frontiers in [5, 10) return the second batch.
        for t in 5..=9 {
            assert_eq!(
                state.next_listen_batch(&Antichain::from_elem(t)),
                Some((vec!["key2".to_owned()].as_slice(), &desc(5, 10)))
            );
        }

        // There is no batch currently available for t = 10.
        assert_eq!(state.next_listen_batch(&Antichain::from_elem(10)), None);

        // By definition, there is no frontier ever at the empty antichain which
        // is the time after all possible times.
        assert_eq!(state.next_listen_batch(&Antichain::new()), None);
    }
}
