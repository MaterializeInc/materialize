// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::collections::{hash_map::Entry, BTreeMap};
use std::marker::PhantomData;

use async_trait::async_trait;

use crate::{Append, AppendBatch, Data, Diff, Id, Stash, StashCollection, StashError};

/// An in-memory Stash that is backed by another Stash but serves read requests
/// from its memory. Write requests are propogated to the other Stash.
#[derive(Debug)]
pub struct Memory<S> {
    stash: S,
    collections: HashMap<String, Id>,
    entries: HashMap<Id, BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl<S: Stash> Memory<S> {
    pub fn new(stash: S) -> Self {
        Self {
            stash,
            collections: HashMap::new(),
            entries: HashMap::new(),
        }
    }
}

#[async_trait]
impl<S: Stash> Stash for Memory<S> {
    async fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        Ok(match self.collections.entry(name.to_string()) {
            Entry::Occupied(entry) => StashCollection {
                id: *entry.get(),
                _kv: PhantomData,
            },
            Entry::Vacant(entry) => {
                let collection = self.stash.collection(name).await?;
                entry.insert(collection.id);
                collection
            }
        })
    }

    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        Ok(match self.entries.entry(collection.id) {
            Entry::Occupied(entry) => entry
                .get()
                .iter()
                .map(|(k, v)| {
                    let k: K = K::decode(k)?;
                    let v: V = V::decode(v)?;
                    Ok((k, v))
                })
                .collect::<Result<BTreeMap<_, _>, StashError>>()?,
            Entry::Vacant(entry) => {
                let entries = self.stash.iter(collection).await?;
                entry.insert(
                    entries
                        .iter()
                        .map(|(k, v)| {
                            let mut k_buf = Vec::new();
                            let mut v_buf = Vec::new();
                            k.encode(&mut k_buf);
                            v.encode(&mut v_buf);
                            (k_buf, v_buf)
                        })
                        .collect(),
                );
                entries
            }
        })
    }

    async fn get_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Data,
        V: Data,
    {
        self.stash.get_key(collection, key).await
    }

    async fn update_many<K, V, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
        I: IntoIterator<Item = ((K, V), Diff)> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let local_entries: Vec<_> = entries
            .iter()
            .map(|((k, v), diff)| {
                let mut k_buf = Vec::new();
                let mut v_buf = Vec::new();
                k.encode(&mut k_buf);
                v.encode(&mut v_buf);
                ((k_buf, v_buf), *diff)
            })
            .collect();
        self.stash.update_many(collection, entries).await?;
        // Only update the memory cache if it's already present.
        if let Some(entry) = self.entries.get_mut(&collection.id) {
            for ((k, v), diff) in local_entries {
                match diff {
                    Diff::Insert => entry.insert(k, v),
                    Diff::Delete => entry.remove(&k),
                };
            }
        }
        Ok(())
    }

    async fn confirm_leadership(&mut self) -> Result<(), StashError> {
        self.stash.confirm_leadership().await
    }
}

#[async_trait]
impl<S: Append> Append for Memory<S> {
    async fn append<I>(&mut self, batches: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = AppendBatch> + Send + 'static,
        I::IntoIter: Send,
    {
        let batches: Vec<_> = batches.into_iter().collect();
        self.stash.append(batches.clone()).await?;
        for batch in batches {
            // Only update the memory cache if it's already present.
            if let Some(entry) = self.entries.get_mut(&batch.collection_id) {
                for ((k, v), diff) in batch.entries {
                    match diff {
                        Diff::Insert => entry.insert(k, v),
                        Diff::Delete => entry.remove(&k),
                    };
                }
            }
        }
        Ok(())
    }
}
