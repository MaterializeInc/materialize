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
use crate::write::WriterId;
use crate::ShardId;

#[derive(Clone, Debug, PartialEq)]
pub struct ReadCapability<T> {
    pub seqno: SeqNo,
    pub since: Antichain<T>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WriteCapability<T> {
    pub upper: Antichain<T>,
}

// TODO: Document invariants.
#[derive(Debug, Clone)]
pub struct StateCollections<T> {
    writers: HashMap<WriterId, WriteCapability<T>>,
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
        writer_id: &WriterId,
        reader_id: &ReaderId,
    ) -> ControlFlow<Infallible, (WriteCapability<T>, ReadCapability<T>)> {
        // TODO: Handle if the reader or writer already exist (probably with a
        // retry).
        let write_cap = WriteCapability {
            upper: self.upper(),
        };
        self.writers.insert(writer_id.clone(), write_cap.clone());
        let read_cap = ReadCapability {
            seqno,
            since: self.since.clone(),
        };
        self.readers.insert(reader_id.clone(), read_cap.clone());
        Continue((write_cap, read_cap))
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

    pub fn append(
        &mut self,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> ControlFlow<Upper<T>, ()> {
        // Sanity check that the writer is sending appends such that the lower
        // and upper frontiers line up with previous writes.
        let write_cap = self.writer(writer_id);
        if &write_cap.upper != desc.lower() {
            return Break(Upper(write_cap.upper.clone()));
        }
        write_cap.upper.clone_from(desc.upper());

        // Construct a new desc for the part of this append that the shard
        // didn't already know about: i.e. a lower of the *shard upper* to an
        // upper of the *append upper*. This might truncate the data we're
        // receiving now (if the shard upper is partially past the input desc)
        // or even make it a no-op (if the shard upper is entirely past the
        // input desc).
        let lower = self.upper();
        let desc = Description::new(lower, desc.upper().clone(), desc.since().clone());
        if PartialOrder::less_equal(desc.upper(), desc.lower()) {
            // No-op, but still commit the state change so that this gets
            // linearized.
            return Continue(());
        }

        self.trace.push((keys.to_vec(), desc.clone()));
        debug_assert_eq!(&self.upper(), desc.upper());

        Continue(())
    }

    pub fn compare_and_append(
        &mut self,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> ControlFlow<Result<Upper<T>, InvalidUsage<T>>, ()> {
        if PartialOrder::less_than(desc.upper(), desc.lower()) {
            return Break(Err(InvalidUsage::InvalidBounds {
                lower: desc.lower().clone(),
                upper: desc.upper().clone(),
            }));
        }

        let shard_upper = self.upper();
        let write_cap = self.writer(writer_id);
        debug_assert!(PartialOrder::less_equal(&write_cap.upper, &shard_upper));

        if &shard_upper != desc.lower() {
            return Break(Ok(Upper(shard_upper)));
        }
        write_cap.upper.clone_from(desc.upper());

        self.trace.push((keys.to_vec(), desc.clone()));
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

    pub fn expire_writer(&mut self, writer_id: &WriterId) -> ControlFlow<Infallible, bool> {
        let existed = self.writers.remove(writer_id).is_some();
        // No-op if existed is false, but still commit the state change so that
        // this gets linearized.
        Continue(existed)
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

    fn writer(&mut self, id: &WriterId) -> &mut WriteCapability<T> {
        self.writers
            .get_mut(id)
            // The only (tm) ways to hit this are (1) inventing a WriterId
            // instead of getting it from Register or (2) if a lease expired.
            // (1) is a gross mis-use and (2) isn't implemented yet, so it feels
            // okay to leave this for followup work.
            .expect("TODO: Implement automatic lease renewals")
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
}

// TODO: Document invariants.
#[derive(Debug)]
pub struct State<K, V, T, D> {
    shard_id: ShardId,

    seqno: SeqNo,
    collections: StateCollections<T>,

    _phantom: PhantomData<(K, V, D)>,
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
                writers: HashMap::new(),
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

#[derive(Debug, PartialEq)]
pub struct Since<T>(pub Antichain<T>);

// When used as an error, Since is determinate.
impl<T> Determinacy for Since<T> {
    const DETERMINANT: bool = true;
}

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
    writers: Vec<(WriterId, AntichainMeta)>,
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
        WriteCapability,
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
                writers: x
                    .collections
                    .writers
                    .iter()
                    .map(|(id, cap)| (id.clone(), (&cap.upper).into()))
                    .collect(),
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

            let writers = x
                .writers
                .iter()
                .map(|(id, upper)| {
                    let cap = WriteCapability {
                        upper: upper.into(),
                    };
                    (id.clone(), cap)
                })
                .collect();
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
                writers,
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
