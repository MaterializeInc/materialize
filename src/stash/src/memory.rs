// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
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

    fn consolidate_id(&mut self, collection_id: Id) {
        if let Some(entry) = self.entries.get_mut(&collection_id) {
            let since = match self.sinces.get(&collection_id) {
                Some(since) => since,
                // If we don't know the since for this collection, remove the entries. We can't
                // merely fetch the since because the API requires a full StashCollection, and
                // we only have the Id here.
                None => {
                    self.entries.remove(&collection_id);
                    return;
                }
            };
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
                    self.entries.remove(&collection_id);
                }
            }
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

    async fn collections(&mut self) -> Result<BTreeSet<String>, StashError> {
        self.stash.collections().await
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
                    let k: K = serde_json::from_slice(k)?;
                    let v: V = serde_json::from_slice(v)?;
                    Ok(((k, v), *ts, *diff))
                })
                .collect::<Result<Vec<_>, StashError>>()?,
            Entry::Vacant(entry) => {
                let entries = self.stash.iter(collection).await?;
                entry.insert(
                    entries
                        .iter()
                        .map(|((k, v), ts, diff)| {
                            let key = serde_json::to_vec(k).expect("must serialize");
                            let value = serde_json::to_vec(v).expect("must serialize");
                            ((key, value), *ts, *diff)
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
        Ok(match self.entries.entry(collection.id) {
            Entry::Occupied(entry) => entry
                .get()
                .iter()
                .filter_map(|((k, v), ts, diff)| {
                    let k: K = serde_json::from_slice(k).expect("must deserialize");
                    if &k == key {
                        let v: V = serde_json::from_slice(v).expect("must deserialize");
                        Some((v, *ts, *diff))
                    } else {
                        None
                    }
                })
                .collect(),
            Entry::Vacant(_) => {
                // If vacant, do a full `iter` to correctly populate the cache
                // (`entries`, if present, must contain all keys in the source
                // collection).
                let entries = self.iter(collection).await?;
                entries
                    .into_iter()
                    .filter_map(
                        |((k, v), ts, diff)| {
                            if &k == key {
                                Some((v, ts, diff))
                            } else {
                                None
                            }
                        },
                    )
                    .collect()
            }
        })
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
                let key = serde_json::to_vec(k).expect("must serialize");
                let value = serde_json::to_vec(v).expect("must serialize");
                ((key, value), *ts, *diff)
            })
            .collect();
        self.stash.update_many(collection, entries).await?;
        // Only update the memory cache if it's already present.
        if let Some(entry) = self.entries.get_mut(&collection.id) {
            entry.extend(local_entries);
            self.consolidate_id(collection.id);
        }
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
            self.consolidate_id(collection.id);
        }
        Ok(())
    }

    async fn consolidate(&mut self, collection: Id) -> Result<(), StashError> {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch(&mut self, collections: &[Id]) -> Result<(), StashError> {
        self.stash.consolidate_batch(collections).await?;
        for collection in collections {
            self.consolidate_id(*collection);
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

    async fn confirm_leadership(&mut self) -> Result<(), StashError> {
        self.stash.confirm_leadership().await
    }
}

#[async_trait]
impl<S: Append> Append for Memory<S> {
    async fn append_batch(&mut self, batches: &[AppendBatch]) -> Result<(), StashError> {
        self.stash.append_batch(batches).await?;
        for batch in batches {
            self.uppers.insert(batch.collection_id, batch.upper.clone());
            self.sinces
                .insert(batch.collection_id, batch.compact.clone());
            // Only update the memory cache if it's already present.
            if let Some(entry) = self.entries.get_mut(&batch.collection_id) {
                entry.extend(batch.entries.iter().map(|((key, value), ts, diff)| {
                    let key = serde_json::to_vec(key).expect("must serialise");
                    let value = serde_json::to_vec(value).expect("must serialize");
                    ((key, value), *ts, *diff)
                }));
            }
        }
        Ok(())
    }
}
