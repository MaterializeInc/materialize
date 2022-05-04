// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::marker::PhantomData;

use anyhow::anyhow;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::{InvalidUsage, NoOp};
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
    ) -> (WriteCapability<T>, ReadCapability<T>) {
        // TODO: Handle if the reader or writer already exist (probably a
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
        (write_cap, read_cap)
    }

    pub fn clone_reader(&mut self, seqno: SeqNo, new_reader_id: &ReaderId) -> ReadCapability<T> {
        let read_cap = ReadCapability {
            seqno,
            since: self.since.clone(),
        };
        self.readers.insert(new_reader_id.clone(), read_cap.clone());
        read_cap
    }

    pub fn append(
        &mut self,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<Result<(), Antichain<T>>, InvalidUsage> {
        // Sanity check that the writer is sending appends such that the lower
        // and upper frontiers line up with previous writes.
        let write_cap = self.writer(writer_id)?;
        if &write_cap.upper != desc.lower() {
            return Ok(Err(write_cap.upper.clone()));
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
            // No-op
            return Ok(Ok(()));
        }

        self.trace.push((keys.to_vec(), desc.clone()));
        debug_assert_eq!(&self.upper(), desc.upper());

        Ok(Ok(()))
    }

    pub fn compare_and_append(
        &mut self,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<Result<(), Antichain<T>>, InvalidUsage> {
        if PartialOrder::less_than(desc.upper(), desc.lower()) {
            return Err(InvalidUsage(anyhow!("invalid desc: {:?}", desc)));
        }

        let shard_upper = self.upper();
        let write_cap = self.writer(writer_id)?;
        debug_assert!(PartialOrder::less_equal(&write_cap.upper, &shard_upper));

        if &shard_upper != desc.lower() {
            return Ok(Err(shard_upper));
        }
        write_cap.upper.clone_from(desc.upper());

        self.trace.push((keys.to_vec(), desc.clone()));
        debug_assert_eq!(&self.upper(), desc.upper());

        Ok(Ok(()))
    }

    pub fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> Result<(), InvalidUsage> {
        let read_cap = self.reader(reader_id)?;
        if !PartialOrder::less_than(&read_cap.since, new_since) {
            return Err(InvalidUsage(anyhow!(
                "reader since {:?} already in advance of new since: {:?}",
                read_cap.since,
                new_since
            )));
        }
        read_cap.since.clone_from(new_since);
        self.update_since();
        Ok(())
    }

    pub fn expire_writer(&mut self, seqno: SeqNo, writer_id: &WriterId) -> Result<(), NoOp> {
        self.writers.remove(writer_id).ok_or(NoOp { seqno })?;
        Ok(())
    }

    pub fn expire_reader(&mut self, seqno: SeqNo, reader_id: &ReaderId) -> Result<(), NoOp> {
        self.readers.remove(reader_id).ok_or(NoOp { seqno })?;
        self.update_since();
        Ok(())
    }

    fn upper(&self) -> Antichain<T> {
        self.trace.last().map_or_else(
            || Antichain::from_elem(T::minimum()),
            |(_, desc)| desc.upper().clone(),
        )
    }

    fn writer(&mut self, id: &WriterId) -> Result<&mut WriteCapability<T>, InvalidUsage> {
        self.writers
            .get_mut(id)
            // TODO: It is more likely that the lease expired.
            .ok_or_else(|| InvalidUsage(anyhow!("writer not registered: {}", id)))
    }

    fn reader(&mut self, id: &ReaderId) -> Result<&mut ReadCapability<T>, InvalidUsage> {
        self.readers
            .get_mut(id)
            // TODO: It is more likely that the lease expired.
            .ok_or_else(|| InvalidUsage(anyhow!("reader not registered: {}", id)))
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

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn seqno(&self) -> SeqNo {
        self.seqno
    }

    pub fn upper(&self) -> Antichain<T> {
        self.collections.upper()
    }

    pub fn clone_apply<R, E, WorkFn>(&self, work_fn: &mut WorkFn) -> Result<(R, Self), E>
    where
        WorkFn: FnMut(SeqNo, &mut StateCollections<T>) -> Result<R, E>,
    {
        let mut new_state = State {
            shard_id: self.shard_id,
            seqno: self.seqno.next(),
            collections: self.collections.clone(),
            _phantom: PhantomData,
        };
        let work_ret = work_fn(new_state.seqno, &mut new_state.collections)?;
        Ok((work_ret, new_state))
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

#[derive(Debug)]
pub struct Since<T>(pub Antichain<T>);

#[derive(Debug)]
pub struct Upper<T>(pub Antichain<T>);

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
            State::try_from(&state)
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
        type Error = String;

        fn try_from(x: &StateRollupMeta) -> Result<Self, String> {
            if K::codec_name() != x.key_codec {
                return Err(format!(
                    "key_codec {} doesn't match original: {}",
                    K::codec_name(),
                    x.key_codec
                ));
            }
            if V::codec_name() != x.val_codec {
                return Err(format!(
                    "val_codec {} doesn't match original: {}",
                    V::codec_name(),
                    x.val_codec
                ));
            }
            if T::codec_name() != x.ts_codec {
                return Err(format!(
                    "ts_codec {} doesn't match original: {}",
                    T::codec_name(),
                    x.ts_codec
                ));
            }
            if D::codec_name() != x.diff_codec {
                return Err(format!(
                    "diff_codec {} doesn't match original: {}",
                    D::codec_name(),
                    x.diff_codec
                ));
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
