// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::anyhow;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::sync::Mutex;

use crate::error::InvalidUsage;
use crate::read::{ReadHandle, ReaderId};
use crate::write::{WriteHandle, WriterId};
use crate::Id;

#[derive(Debug)]
pub struct State {
    pub(crate) shard_id: Id,
    pub(crate) key_codec: String,
    pub(crate) val_codec: String,
    pub(crate) ts_codec: String,
    pub(crate) diff_codec: String,

    pub(crate) writers: HashMap<WriterId, Vec<[u8; 8]>>,
    pub(crate) since: Vec<[u8; 8]>,
    pub(crate) readers: HashMap<ReaderId, Vec<[u8; 8]>>,

    pub(crate) upper: Vec<[u8; 8]>,
    pub(crate) contents: Vec<(Vec<u8>, Vec<u8>, [u8; 8], [u8; 8])>,
}

pub struct Shard<K, V, T, D> {
    state: Arc<Mutex<State>>,
    _phantom: PhantomData<(K, V, T, D)>,
}

// Impl Clone regardless of the type params.
impl<K, V, T, D> Clone for Shard<K, V, T, D> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            _phantom: self._phantom.clone(),
        }
    }
}

// Impl Debug regardless of the type params.
impl<K, V, T, D> std::fmt::Debug for Shard<K, V, T, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Shard").field("state", &self.state).finish()
    }
}

impl<K, V, T, D> Shard<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    pub fn new(shard_id: Id) -> Self {
        let state = State {
            shard_id,
            key_codec: K::codec_name(),
            val_codec: V::codec_name(),
            ts_codec: T::codec_name(),
            diff_codec: D::codec_name(),

            upper: Antichain::from_elem(T::minimum())
                .iter()
                .map(|x| T::encode(x))
                .collect(),
            writers: HashMap::new(),
            since: Antichain::from_elem(T::minimum())
                .iter()
                .map(|x| T::encode(x))
                .collect(),
            readers: HashMap::new(),

            contents: Vec::new(),
        };
        Shard {
            state: Arc::new(Mutex::new(state)),
            _phantom: PhantomData,
        }
    }

    pub fn into_inner(self) -> Arc<Mutex<State>> {
        self.state
    }

    pub async fn decode(state: Arc<Mutex<State>>) -> Result<Self, InvalidUsage> {
        {
            let state = state.lock().await;
            if K::codec_name() != state.key_codec {
                return Err(InvalidUsage(anyhow!(
                    "key_codec {} doesn't match original: {}",
                    K::codec_name(),
                    state.key_codec
                )));
            }
            if V::codec_name() != state.val_codec {
                return Err(InvalidUsage(anyhow!(
                    "val_codec {} doesn't match original: {}",
                    V::codec_name(),
                    state.val_codec
                )));
            }
            if T::codec_name() != state.ts_codec {
                return Err(InvalidUsage(anyhow!(
                    "ts_codec {} doesn't match original: {}",
                    T::codec_name(),
                    state.ts_codec
                )));
            }
            if D::codec_name() != state.diff_codec {
                return Err(InvalidUsage(anyhow!(
                    "diff_codec {} doesn't match original: {}",
                    D::codec_name(),
                    state.diff_codec
                )));
            }
        }

        Ok(Shard {
            state,
            _phantom: PhantomData,
        })
    }

    fn decode_antichain(elements: &Vec<[u8; 8]>) -> Antichain<T> {
        Antichain::from(elements.iter().map(|x| T::decode(*x)).collect::<Vec<_>>())
    }

    pub async fn recover_capabilities(&self) -> (WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>) {
        let mut state = self.state.lock().await;

        let upper = state.upper.clone();
        let since = state.since.clone();

        let writer_id = WriterId::new();
        let reader_id = ReaderId::new();
        assert_eq!(state.writers.contains_key(&writer_id), false);
        assert_eq!(state.readers.contains_key(&reader_id), false);
        state.writers.insert(writer_id.clone(), upper.clone());
        state.readers.insert(reader_id.clone(), since.clone());

        let writer = WriteHandle {
            writer_id,
            shard: self.clone(),
            upper: Self::decode_antichain(&upper),
        };
        let reader = ReadHandle {
            reader_id,
            state: self.clone(),
            since: Self::decode_antichain(&since),
        };
        (writer, reader)
    }

    pub async fn write_batch<'i, I: IntoIterator<Item = ((&'i K, &'i V), &'i T, &'i D)>>(
        &self,
        writer_id: &WriterId,
        updates: I,
        new_upper: Antichain<T>,
    ) -> Result<Antichain<T>, InvalidUsage> {
        let mut state = self.state.lock().await;

        let writer_lower = match state.writers.get(writer_id) {
            Some(x) => Self::decode_antichain(x),
            // TODO: This is more likely an expired lease.
            None => return Err(InvalidUsage(anyhow!("unknown writer: {:?}", writer_id))),
        };

        let lower = Self::decode_antichain(&state.upper);
        if PartialOrder::less_than(&new_upper, &lower) {
            return Err(InvalidUsage(anyhow!(
                "new upper {:?} not in advance of current {:?}",
                new_upper,
                lower
            )));
        }
        for ((k, v), t, d) in updates {
            debug_assert!(writer_lower.less_equal(t));
            debug_assert!(!new_upper.less_equal(t));
            let (mut key_buf, mut val_buf) = (Vec::new(), Vec::new());
            K::encode(k, &mut key_buf);
            V::encode(v, &mut val_buf);
            state
                .contents
                .push((key_buf, val_buf, T::encode(t), D::encode(d)));
        }
        state.upper = new_upper.elements().iter().map(|x| T::encode(x)).collect();
        Ok(new_upper)
    }

    pub async fn clone_reader(&self, reader_id: &ReaderId) -> Result<ReaderId, InvalidUsage> {
        let mut state = self.state.lock().await;
        let since = match state.readers.get(reader_id) {
            Some(x) => x.clone(),
            // TODO: This is more likely an expired lease.
            None => return Err(InvalidUsage(anyhow!("unknown reader: {}", reader_id))),
        };
        let new_reader_id = ReaderId::new();
        state.readers.insert(new_reader_id.clone(), since);
        Ok(new_reader_id)
    }
}

impl<K, V, T, D> Shard<K, V, T, D> {
    pub async fn deregister_writer(&self, writer_id: &WriterId) {
        // TODO: Move this id to a graveyard?
        self.state.lock().await.writers.remove(writer_id);
    }

    pub async fn deregister_reader(&self, reader_id: &ReaderId) {
        // TODO: Move this id to a graveyard?
        self.state.lock().await.readers.remove(reader_id);
    }
}
