// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_persist::location::SeqNo;
use mz_persist_types::{Codec, Codec64};
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::error::InvalidUsage;
use crate::r#impl::state::{HollowBatch, ReadCapability, State, StateCollections};
use crate::r#impl::trace::Trace;
use crate::read::ReaderId;
use crate::ShardId;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct AntichainMeta(Vec<[u8; 8]>);

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DescriptionMeta {
    lower: AntichainMeta,
    upper: AntichainMeta,
    since: AntichainMeta,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct BatchMeta {
    desc: DescriptionMeta,
    keys: Vec<String>,
    len: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TraceMeta {
    since: AntichainMeta,
    // TODO: Should this more directly reflect the SpineBatch structure?
    spine: Vec<BatchMeta>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct StateRollupMeta {
    shard_id: ShardId,
    key_codec: String,
    val_codec: String,
    ts_codec: String,
    diff_codec: String,

    seqno: SeqNo,
    readers: Vec<(ReaderId, AntichainMeta, SeqNo)>,
    trace: TraceMeta,
}

impl<K, V, T, D> State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
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
        // This map_err goes away when we do incremental state.
        State::try_from(&state).map_err(|err| err.to_string())
    }
}

impl<K, V, T, D> From<&State<K, V, T, D>> for StateRollupMeta
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
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
            trace: (&x.collections.trace).into(),
        }
    }
}

impl<K, V, T, D> TryFrom<&StateRollupMeta> for State<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
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
        let collections = StateCollections {
            readers,
            trace: (&x.trace).into(),
        };
        Ok(State {
            shard_id: x.shard_id,
            seqno: x.seqno,
            collections,
            _phantom: PhantomData,
        })
    }
}

impl<T> From<&Trace<T>> for TraceMeta
where
    T: Timestamp + Lattice + Codec64,
{
    fn from(x: &Trace<T>) -> Self {
        let mut spine = Vec::new();
        x.map_batches(|b| {
            spine.push(BatchMeta {
                desc: (&b.desc).into(),
                keys: b.keys.clone(),
                len: b.len,
            })
        });
        TraceMeta {
            since: x.since().into(),
            spine,
        }
    }
}

impl<T> From<&TraceMeta> for Trace<T>
where
    T: Timestamp + Lattice + Codec64,
{
    fn from(x: &TraceMeta) -> Self {
        let mut ret = Trace::default();
        ret.downgrade_since((&x.since).into());
        for batch in x.spine.iter() {
            let batch = HollowBatch::from(batch);
            if PartialOrder::less_than(ret.since(), batch.desc.since()) {
                panic!(
                    "invalid TraceMeta: the spine's since was less than a batch's since: {:?}",
                    x
                )
            }
            // We could perhaps more directly serialize and rehydrate the
            // internals of the Spine, but this is nice because it insulates
            // us against changes in the Spine logic. The current logic has
            // turned out to be relatively expensive in practice, but as we
            // tune things (especially when we add inc state) the rate of
            // this deserialization should go down. Revisit as necessary.
            ret.push_batch(batch);
        }
        let _ = ret.take_merge_reqs();
        ret
    }
}

impl<T: Codec64> From<&HollowBatch<T>> for BatchMeta {
    fn from(x: &HollowBatch<T>) -> Self {
        BatchMeta {
            desc: (&x.desc).into(),
            keys: x.keys.clone(),
            len: x.len,
        }
    }
}

impl<T: Timestamp + Codec64> From<&BatchMeta> for HollowBatch<T> {
    fn from(x: &BatchMeta) -> Self {
        HollowBatch {
            desc: (&x.desc).into(),
            keys: x.keys.clone(),
            len: x.len,
        }
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
