// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::marker::PhantomData;
use std::num::NonZeroI64;

use async_trait::async_trait;
use futures::Future;
use timely::{progress::Antichain, PartialOrder};

use timely::progress::frontier::AntichainRef;

use crate::{
    AntichainFormatter, Append, AppendBatch, Data, Diff, Id, Stash, StashCollection, StashError,
    Timestamp,
};

/// An in-memory Stash that is backed by another Stash but serves read requests
/// from its memory. Write requests are propogated to the other Stash.
#[derive(Debug, Clone)]
pub struct Memory {
    next_id: Id,
    collections: HashMap<String, Id>,
    uppers: HashMap<Id, Antichain<Timestamp>>,
    sinces: HashMap<Id, Antichain<Timestamp>>,
    entries: HashMap<Id, Vec<((Vec<u8>, Vec<u8>), Timestamp, Diff)>>,
}

impl Memory {
    pub fn new() -> Self {
        Self {
            next_id: 1,
            collections: HashMap::new(),
            uppers: HashMap::new(),
            sinces: HashMap::new(),
            entries: HashMap::new(),
        }
    }

    async fn transact<F, U>(&mut self, f: F) -> Result<(), StashError>
    where
        F: Fn(Self) -> U,
        U: Future<Output = Result<Self, StashError>>,
    {
        let copy = self.clone();
        let copy = f(copy).await?;
        let Self {
            next_id,
            collections,
            uppers,
            sinces,
            entries,
        } = copy;
        self.next_id = next_id;
        self.collections = collections;
        self.uppers = uppers;
        self.sinces = sinces;
        self.entries = entries;
        Ok(())
    }

    fn consolidate_id(&mut self, collection_id: Id) {
        let entry = self.entries.get_mut(&collection_id).unwrap();
        match self.sinces.get(&collection_id).unwrap().as_option() {
            Some(since) => {
                for ((_k, _v), ts, _diff) in entry.iter_mut() {
                    if ts.less_than(since) {
                        *ts = *since;
                    }
                }
                differential_dataflow::consolidation::consolidate_updates(entry);
            }
            None => {
                entry.clear();
            }
        }
    }

    fn seal_tx<'a, I>(&mut self, seals: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        for (collection, upper) in seals {
            let prev = self.uppers.get(&collection).unwrap();
            if PartialOrder::less_than(upper, prev) {
                return Err(StashError::from(format!(
                    "seal request {} is less than the current upper frontier {}",
                    AntichainFormatter(upper),
                    AntichainFormatter(prev),
                )));
            }
            self.uppers.insert(collection, upper.clone()).unwrap();
        }
        Ok(())
    }

    fn update_many_tx<I>(&mut self, collection: Id, entries: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = ((Vec<u8>, Vec<u8>), Timestamp, Diff)>,
    {
        let upper = self.uppers.get(&collection).unwrap();
        let entry = self.entries.entry(collection).or_default();
        for ((k, v), ts, diff) in entries.into_iter() {
            if !upper.less_equal(&ts) {
                return Err(StashError::from(format!(
                    "entry time {} is less than the current upper frontier {}",
                    ts,
                    AntichainFormatter(upper)
                )));
            }
            entry.push(((k, v), ts, diff));
        }
        Ok(())
    }

    fn compact_batch_tx<'a, I>(&mut self, compactions: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        for (collection, new_since) in compactions {
            let prev_since = self.sinces.get(&collection).unwrap();
            if PartialOrder::less_than(new_since, prev_since) {
                return Err(StashError::from(format!(
                    "compact request {} is less than the current since frontier {}",
                    AntichainFormatter(new_since),
                    AntichainFormatter(prev_since)
                )));
            }
            let upper = self.uppers.get(&collection).unwrap();
            if PartialOrder::less_than(upper, new_since) {
                return Err(StashError::from(format!(
                    "compact request {} is greater than the current upper frontier {}",
                    AntichainFormatter(new_since),
                    AntichainFormatter(upper)
                )));
            }
            self.sinces.insert(collection, new_since.clone()).unwrap();
            self.consolidate_id(collection);
        }
        Ok(())
    }
}

#[async_trait]
impl Stash for Memory {
    async fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let id = self.collections.entry(name.to_string()).or_insert_with(|| {
            self.next_id += 1;
            self.sinces
                .insert(self.next_id, Antichain::from_elem(Timestamp::MIN));
            self.uppers
                .insert(self.next_id, Antichain::from_elem(Timestamp::MIN));
            self.entries.insert(self.next_id, Vec::new());
            self.next_id
        });
        Ok(StashCollection {
            id: *id,
            _kv: PhantomData,
        })
    }

    async fn collections(&mut self) -> Result<BTreeSet<String>, StashError> {
        Ok(self.collections.keys().cloned().collect())
    }

    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let since = match self
            .sinces
            .get(&collection.id)
            .unwrap()
            .clone()
            .into_option()
        {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        match self.entries.get(&collection.id) {
            Some(entries) => {
                let mut rows = entries
                    .iter()
                    .map(|((k, v), ts, diff)| {
                        let k: K = serde_json::from_slice(k).expect("must deserialize");
                        let v: V = serde_json::from_slice(v).expect("must deserialize");
                        ((k, v), std::cmp::max(*ts, since), *diff)
                    })
                    .collect();
                differential_dataflow::consolidation::consolidate_updates(&mut rows);
                Ok(rows)
            }
            None => Ok(Vec::new()),
        }
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
        let since = match self
            .sinces
            .get(&collection.id)
            .unwrap()
            .clone()
            .into_option()
        {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        match self.entries.get(&collection.id) {
            Some(entries) => {
                let mut rows = entries
                    .iter()
                    .filter_map(|((k, v), ts, diff)| {
                        let k: K = serde_json::from_slice(k).expect("must deserialize");
                        if &k == key {
                            let v: V = serde_json::from_slice(v).expect("must deserialize");
                            Some((v, std::cmp::max(*ts, since), *diff))
                        } else {
                            None
                        }
                    })
                    .collect();
                differential_dataflow::consolidation::consolidate_updates(&mut rows);
                Ok(rows)
            }
            None => Ok(Vec::new()),
        }
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
        let entries = entries
            .into_iter()
            .map(|((k, v), ts, diff)| {
                let key = serde_json::to_vec(&k).expect("must serialize");
                let value = serde_json::to_vec(&v).expect("must serialize");
                ((key, value), ts, diff)
            })
            .collect::<Vec<_>>();
        self.transact(|mut _self| {
            let entries = entries.clone();
            async {
                _self.update_many_tx(collection.id, entries)?;
                Ok(_self)
            }
        })
        .await
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
        self.transact(|mut _self| async {
            _self.seal_tx(
                seals
                    .iter()
                    .map(|(collection, upper)| (collection.id, upper)),
            )?;
            Ok(_self)
        })
        .await
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
        self.transact(|mut _self| async {
            _self.compact_batch_tx(
                compactions
                    .iter()
                    .map(|(collection, since)| (collection.id, since)),
            )?;
            Ok(_self)
        })
        .await
    }

    async fn consolidate(&mut self, collection: Id) -> Result<(), StashError> {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch(&mut self, collections: &[Id]) -> Result<(), StashError> {
        self.transact(|mut _self| async {
            for collection in collections {
                _self.consolidate_id(*collection);
            }
            Ok(_self)
        })
        .await
    }

    async fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        match self.sinces.get(&collection.id) {
            Some(since) => Ok(since.clone()),
            None => Err(format!("unknown collection in sinces: {}", collection.id).into()),
        }
    }

    async fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        match self.uppers.get(&collection.id) {
            Some(upper) => Ok(upper.clone()),
            None => Err(format!("unknown collection in uppers: {}", collection.id).into()),
        }
    }

    async fn confirm_leadership(&mut self) -> Result<(), StashError> {
        Ok(())
    }

    fn epoch(&self) -> Option<NonZeroI64> {
        None
    }
}

#[async_trait]
impl Append for Memory {
    async fn append_batch(&mut self, batches: &[AppendBatch]) -> Result<(), StashError> {
        self.transact(|mut _self| async {
            for AppendBatch {
                collection_id,
                lower,
                upper,
                compact,
                timestamp: _,
                entries,
            } in batches
            {
                let current_upper = _self.uppers.get(collection_id).unwrap();
                if current_upper != lower {
                    return Err(StashError::from("unexpected lower"));
                }

                let entries = entries.iter().map(|((k, v), ts, diff)| {
                    let k = serde_json::to_vec(&k).unwrap();
                    let v = serde_json::to_vec(&v).unwrap();
                    ((k, v), *ts, *diff)
                });
                _self.update_many_tx(*collection_id, entries)?;
                _self.seal_tx([(*collection_id, upper)])?;
                _self.compact_batch_tx([(*collection_id, compact)])?;
            }
            Ok(_self)
        })
        .await
    }
}
