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

use crate::error::InvalidUsage;
use crate::read::ReaderId;
use crate::write::WriterId;
use crate::Id;

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
#[derive(Debug)]
pub struct State<K, V, T, D> {
    id: Id,

    writers: HashMap<WriterId, WriteCapability<T>>,
    readers: HashMap<ReaderId, ReadCapability<T>>,

    since: Antichain<T>,
    trace: Vec<(Vec<String>, Description<T>)>,

    _phantom: PhantomData<(K, V, D)>,
}

// Impl Clone regardless of the type params.
impl<K, V, T: Clone, D> Clone for State<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            writers: self.writers.clone(),
            readers: self.readers.clone(),
            since: self.since.clone(),
            trace: self.trace.clone(),
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
    pub fn new(id: Id) -> Self {
        State {
            id,
            writers: HashMap::new(),
            readers: HashMap::new(),
            since: Antichain::from_elem(T::minimum()),
            trace: Vec::new(),
            _phantom: PhantomData,
        }
    }

    pub fn id(&self) -> Id {
        self.id
    }

    pub fn register(
        &mut self,
        seqno: SeqNo,
        writer_id: &WriterId,
        reader_id: &ReaderId,
    ) -> (WriteCapability<T>, ReadCapability<T>) {
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

    pub fn clone_reader(&mut self, seqno: SeqNo, reader_id: &ReaderId) -> ReadCapability<T> {
        let read_cap = ReadCapability {
            seqno,
            since: self.since.clone(),
        };
        self.readers.insert(reader_id.clone(), read_cap.clone());
        read_cap
    }

    pub fn append(
        &mut self,
        writer_id: &WriterId,
        keys: &[String],
        desc: &Description<T>,
    ) -> Result<(), InvalidUsage> {
        let write_cap = self.writer(writer_id)?;
        if &write_cap.upper != desc.lower() {
            return Err(InvalidUsage(anyhow!("WIP")));
        }
        write_cap.upper.clone_from(desc.upper());
        let lower = self.upper();
        let desc = Description::new(lower, desc.upper().clone(), desc.since().clone());
        if PartialOrder::less_equal(desc.upper(), desc.lower()) {
            // No-op
            return Ok(());
        }
        self.trace.push((keys.to_vec(), desc));
        Ok(())
    }

    pub fn downgrade_since(
        &mut self,
        reader_id: &ReaderId,
        new_since: &Antichain<T>,
    ) -> Result<(), InvalidUsage> {
        let read_cap = self.reader(reader_id)?;
        if !PartialOrder::less_equal(&read_cap.since, new_since) {
            return Err(InvalidUsage(anyhow!("WIP")));
        }
        read_cap.since.clone_from(new_since);
        self.update_since();
        Ok(())
    }

    pub fn expire_writer(&mut self, writer_id: &WriterId) -> Result<(), InvalidUsage> {
        self.writers
            .remove(writer_id)
            // TODO: It is more likely that the lease expired.
            .ok_or_else(|| InvalidUsage(anyhow!("writer not registered: {}", writer_id)))?;
        Ok(())
    }

    pub fn expire_reader(&mut self, reader_id: &ReaderId) -> Result<(), InvalidUsage> {
        self.readers
            .remove(reader_id)
            // TODO: It is more likely that the lease expired.
            .ok_or_else(|| InvalidUsage(anyhow!("reader not registered: {}", reader_id)))?;
        self.update_since();
        Ok(())
    }

    pub fn snapshot(&self, as_of: &Antichain<T>) -> Result<Vec<String>, InvalidUsage> {
        if PartialOrder::less_than(as_of, &self.since) {
            return Err(InvalidUsage(anyhow!(
                "snapshot with as_of {:?} cannot be served by shard with since: {:?}",
                as_of,
                self.since
            )));
        }
        let batches = self
            .trace
            .iter()
            .flat_map(|(keys, desc)| {
                if !PartialOrder::less_than(as_of, desc.lower()) {
                    keys.clone()
                } else {
                    vec![]
                }
            })
            .collect();
        Ok(batches)
    }

    pub fn next_listen_batch(
        &self,
        frontier: &Antichain<T>,
    ) -> Option<(&[String], &Description<T>)> {
        // TODO: Avoid the O(n^2) here: `next_listen_batch` is called once per
        // batch and this iterates though all batches to find the next one.
        for (keys, desc) in self.trace.iter() {
            if PartialOrder::less_equal(desc.lower(), frontier)
                && PartialOrder::less_than(frontier, desc.upper())
            {
                return Some((keys.as_slice(), desc));
            }
        }
        return None;
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

    pub fn update_since(&mut self) {
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
        let state: StateRollupMeta =
            bincode::deserialize(buf).map_err(|err| format!("unable to decode state: {}", err))?;
        State::try_from(&state)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct AntichainMeta(Vec<[u8; 8]>);

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

#[derive(Debug, Serialize, Deserialize)]
struct DescriptionMeta {
    lower: AntichainMeta,
    upper: AntichainMeta,
    since: AntichainMeta,
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

#[derive(Debug, Serialize, Deserialize)]
struct StateRollupMeta {
    id: Id,
    key_codec: String,
    val_codec: String,
    ts_codec: String,
    diff_codec: String,

    writers: Vec<(WriterId, AntichainMeta)>,
    readers: Vec<(ReaderId, AntichainMeta, SeqNo)>,
    since: AntichainMeta,
    trace: Vec<(Vec<String>, DescriptionMeta)>,
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
            id: x.id,
            key_codec: K::codec_name(),
            val_codec: V::codec_name(),
            ts_codec: T::codec_name(),
            diff_codec: D::codec_name(),
            writers: x
                .writers
                .iter()
                .map(|(id, cap)| (id.clone(), (&cap.upper).into()))
                .collect(),
            readers: x
                .readers
                .iter()
                .map(|(id, cap)| (id.clone(), (&cap.since).into(), cap.seqno))
                .collect(),
            since: (&x.since).into(),
            trace: x
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
        Ok(State {
            id: x.id,
            writers: x
                .writers
                .iter()
                .map(|(id, upper)| {
                    let cap = WriteCapability {
                        upper: upper.into(),
                    };
                    (id.clone(), cap)
                })
                .collect(),
            readers: x
                .readers
                .iter()
                .map(|(id, since, seqno)| {
                    let cap = ReadCapability {
                        since: since.into(),
                        seqno: *seqno,
                    };
                    (id.clone(), cap)
                })
                .collect(),
            since: (&x.since).into(),
            trace: x
                .trace
                .iter()
                .map(|(key, desc)| (key.clone(), desc.into()))
                .collect(),
            _phantom: PhantomData,
        })
    }
}
