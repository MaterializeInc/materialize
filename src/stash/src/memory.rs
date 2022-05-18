// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::marker::PhantomData;

use async_trait::async_trait;
use timely::{progress::Antichain, PartialOrder};

use timely::progress::frontier::AntichainRef;

use crate::{Append, AppendBatch, Data, Diff, Id, Stash, StashCollection, StashError, Timestamp};

/// An in-memory Stash that is backed by another Stash but serves read requests
/// from its memory. Write requests are propogated to the other Stash.
#[derive(Debug)]
pub struct Memory<S> {
    stash: S,
    collections: HashMap<String, Id>,
    uppers: HashMap<Id, Antichain<Timestamp>>,
    sinces: HashMap<Id, Antichain<Timestamp>>,
    entries: HashMap<Id, Vec<((Vec<u8>, Vec<u8>), Timestamp, Diff)>>,
}

impl<S: Stash> Memory<S> {
    pub fn new(stash: S) -> Self {
        Self {
            stash,
            collections: HashMap::new(),
            uppers: HashMap::new(),
            sinces: HashMap::new(),
            entries: HashMap::new(),
        }
    }

    async fn consolidate_collection<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let since = self.since(collection).await?;
        if let Some(entry) = self.entries.get_mut(&collection.id) {
            match since.as_option() {
                Some(since) => {
                    for ((_k, _v), ts, _diff) in entry.iter_mut() {
                        if ts.less_than(since) {
                            *ts = *since;
                        }
                    }
                    differential_dataflow::consolidation::consolidate_updates(entry);
                }
                None => {
                    // This will cause all calls to iter over this collection to always pass
                    // through to the underlying stash, making those calls not cached. This isn't
                    // currently a performance problem because the empty since is not used.
                    self.entries.remove(&collection.id);
                }
            }
        }
        Ok(())
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
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        Ok(match self.entries.entry(collection.id) {
            Entry::Occupied(entry) => entry
                .get()
                .iter()
                .map(|((k, v), ts, diff)| {
                    let k: K = K::decode(k)?;
                    let v: V = V::decode(v)?;
                    Ok(((k, v), *ts, *diff))
                })
                .collect::<Result<Vec<_>, StashError>>()?,
            Entry::Vacant(entry) => {
                let entries = self.stash.iter(collection).await?;
                entry.insert(
                    entries
                        .iter()
                        .map(|((k, v), ts, diff)| {
                            let mut k_buf = Vec::new();
                            let mut v_buf = Vec::new();
                            k.encode(&mut k_buf);
                            v.encode(&mut v_buf);
                            ((k_buf, v_buf), *ts, *diff)
                        })
                        .collect(),
                );
                entries
            }
        })
    }

    async fn iter_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        self.stash.iter_key(collection, key).await
    }

    async fn update_many<K, V, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)> + Send,
        I::IntoIter: Send,
    {
        let entries: Vec<_> = entries.into_iter().collect();
        let local_entries: Vec<_> = entries
            .iter()
            .map(|((k, v), ts, diff)| {
                let mut k_buf = Vec::new();
                let mut v_buf = Vec::new();
                k.encode(&mut k_buf);
                v.encode(&mut v_buf);
                ((k_buf, v_buf), *ts, *diff)
            })
            .collect();
        self.stash.update_many(collection, entries).await?;
        let entry = self.entries.entry(collection.id).or_insert_with(Vec::new);
        entry.extend(local_entries);
        self.consolidate_collection(collection).await?;
        Ok(())
    }

    async fn seal<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        new_upper: AntichainRef<'_, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.seal_batch(&[(collection, new_upper.to_owned())]).await
    }

    async fn seal_batch<K, V>(
        &mut self,
        seals: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.stash.seal_batch(seals).await?;
        for (collection, upper) in seals {
            self.uppers.insert(collection.id, upper.clone());
        }
        Ok(())
    }

    async fn compact<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
        new_since: AntichainRef<'a, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.compact_batch(&[(collection, new_since.to_owned())])
            .await
    }

    async fn compact_batch<K, V>(
        &mut self,
        compactions: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.stash.compact_batch(compactions).await?;
        for (collection, since) in compactions {
            self.sinces.insert(collection.id, since.clone());
            self.consolidate_collection(*collection).await?;
        }
        Ok(())
    }

    async fn consolidate<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch<K, V>(
        &mut self,
        collections: &[StashCollection<K, V>],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.stash.consolidate_batch(collections).await?;
        for collection in collections {
            self.consolidate_collection(*collection).await?;
        }
        Ok(())
    }

    async fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        Ok(match self.sinces.entry(collection.id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let since = self.stash.since(collection).await?;
                entry.insert(since.clone());
                since
            }
        })
    }

    async fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        Ok(match self.uppers.entry(collection.id) {
            Entry::Occupied(entry) => entry.get().clone(),
            Entry::Vacant(entry) => {
                let upper = self.stash.upper(collection).await?;
                entry.insert(upper.clone());
                upper
            }
        })
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
            self.uppers.insert(batch.collection_id, batch.upper);
            let entry = self
                .entries
                .entry(batch.collection_id)
                .or_insert_with(Vec::new);
            entry.extend(batch.entries);
        }
        Ok(())
    }
}
