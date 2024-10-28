// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The crate provides a durable key-value cache abstraction implemented by persist.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use mz_ore::collections::{AssociativeExt, HashSet};
use mz_persist_client::error::UpperMismatch;
use mz_persist_client::read::{ListenEvent, Subscribe};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::{Codec, ShardId};
use timely::progress::Antichain;
use tracing::debug;

pub trait DurableCacheCodec {
    type Key: Ord + Hash + Clone + Debug;
    type Val: Eq + Debug;
    type KeyCodec: Codec + Ord + Debug;
    type ValCodec: Codec + Ord + Debug;

    fn schemas() -> (
        <Self::KeyCodec as Codec>::Schema,
        <Self::ValCodec as Codec>::Schema,
    );
    fn encode(key: &Self::Key, val: &Self::Val) -> (Self::KeyCodec, Self::ValCodec);
    fn decode(key: Self::KeyCodec, val: Self::ValCodec) -> (Self::Key, Self::Val);
}

#[derive(Debug)]
pub enum Error {
    WriteConflict(UpperMismatch<u64>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::WriteConflict(err) => write!(f, "{err}"),
        }
    }
}

#[derive(Debug)]
pub struct DurableCache<C: DurableCacheCodec> {
    write: WriteHandle<C::KeyCodec, C::ValCodec, u64, i64>,
    subscribe: Subscribe<C::KeyCodec, C::ValCodec, u64, i64>,

    local: BTreeMap<C::Key, C::Val>,
    local_progress: u64,
}

impl<C: DurableCacheCodec> DurableCache<C> {
    /// Opens a [`DurableCache`] using shard `shard_id`.
    pub async fn new(persist: &PersistClient, shard_id: ShardId, purpose: &str) -> Self {
        let use_critical_since = true;
        let (key_schema, val_schema) = C::schemas();
        let (mut write, read) = persist
            .open(
                shard_id,
                Arc::new(key_schema),
                Arc::new(val_schema),
                Diagnostics {
                    shard_name: format!("{purpose}_cache"),
                    handle_purpose: format!("durable persist cache: {purpose}"),
                },
                use_critical_since,
            )
            .await
            .expect("shard codecs should not change");
        // Ensure that at least one ts is immediately readable, for convenience.
        let res = write
            .compare_and_append_batch(&mut [], Antichain::from_elem(0), Antichain::from_elem(1))
            .await
            .expect("usage was valid");
        match res {
            // We made the ts readable.
            Ok(()) => {}
            // Someone else made it readable.
            Err(UpperMismatch { .. }) => {}
        }

        let as_of = read.since().clone();
        let subscribe = read
            .subscribe(as_of)
            .await
            .expect("capability should be held at this since");
        let mut ret = DurableCache {
            write,
            subscribe,
            local: BTreeMap::new(),
            local_progress: 0,
        };
        ret.sync_to(ret.write.upper().as_option().copied()).await;
        ret
    }

    async fn sync_to(&mut self, progress: Option<u64>) -> u64 {
        let progress = progress.expect("cache shard should not be closed");
        while self.local_progress < progress {
            let events = self.subscribe.fetch_next().await;
            for event in events {
                match event {
                    ListenEvent::Updates(x) => {
                        for ((k, v), t, d) in x {
                            let (key, val) = C::decode(k.unwrap(), v.unwrap());
                            if d == 1 {
                                self.local.expect_insert(key, val, "duplicate cache entry");
                            } else if d == -1 {
                                let prev = self.local.expect_remove(&key, "entry does not exist");
                                assert_eq!(val, prev, "removed val does not match expected val");
                            } else {
                                panic!("unexpected diff: (({key:?}, {val:?}), {t}, {d})");
                            }
                        }
                    }
                    ListenEvent::Progress(x) => {
                        self.local_progress =
                            x.into_option().expect("cache shard should not be closed")
                    }
                }
            }
        }
        progress
    }

    /// Get and return the value associated with `key` if it exists, without syncing with the
    /// durable store.
    pub fn get_local(&self, key: &C::Key) -> Option<&C::Val> {
        self.local.get(key)
    }

    /// Get and return the value associated with `key`, syncing with the durable store if
    /// necessary. If `key` does not exist, then a value is computed via `val_fn` and durably
    /// stored in the cache.
    pub async fn get(&mut self, key: &C::Key, val_fn: impl FnOnce() -> C::Val) -> &C::Val {
        // Fast-path if it's already in the local cache.
        // N.B. This pattern of calling `contains_key` followed by `expect` is required to appease
        // the borrow checker.
        if self.local.contains_key(key) {
            return self.local.get(key).expect("checked above");
        }

        // Reduce wasted work by ensuring we're caught up to at least the
        // pubsub-updated shared_upper, and then trying again.
        self.sync_to(self.write.shared_upper().into_option()).await;
        if self.local.contains_key(key) {
            return self.local.get(key).expect("checked above");
        }

        // Okay compute it and write it durably to the cache.
        let val = val_fn();
        let mut expected_upper = self.local_progress;
        loop {
            let update = (C::encode(key, &val), expected_upper, 1);
            let new_upper = expected_upper + 1;
            let ret = self
                .write
                .compare_and_append(
                    &[update],
                    Antichain::from_elem(expected_upper),
                    Antichain::from_elem(new_upper),
                )
                .await
                .expect("usage should be valid");
            match ret {
                Ok(()) => {
                    self.sync_to(Some(new_upper)).await;
                    return self.get_local(key).expect("just inserted");
                }
                Err(err) => {
                    expected_upper = self.sync_to(err.current.into_option()).await;
                    if self.local.contains_key(key) {
                        return self.local.get(key).expect("checked above");
                    }
                    continue;
                }
            }
        }
    }

    /// Return all entries stored in the cache, without syncing with the durable store.
    pub fn entries_local(&self) -> impl Iterator<Item = (&C::Key, &C::Val)> {
        self.local.iter()
    }

    /// Durably set `key` to `value`. A `value` of `None` deletes the entry from the cache.
    ///
    /// Failures will update the cache and retry until the cache is written successfully.
    pub async fn set(&mut self, key: &C::Key, value: Option<&C::Val>) {
        while let Err(err) = self.try_set(key, value).await {
            debug!("failed to set entry: {err} ... retrying");
        }
    }

    /// Durably set multiple key-value pairs in `entries`. Values of `None` deletes the
    /// corresponding entries from the cache.
    ///
    /// Failures will update the cache and retry until the cache is written successfully.
    pub async fn set_many(&mut self, entries: &[(&C::Key, Option<&C::Val>)]) {
        while let Err(err) = self.try_set_many(entries).await {
            debug!("failed to set entries: {err} ... retrying");
        }
    }

    /// Tries to durably set `key` to `value`. A `value` of `None` deletes the entry from the cache.
    ///
    /// On both successes and failures, the cache will update its contents with the most recent
    /// updates from the durable store.
    pub async fn try_set(&mut self, key: &C::Key, value: Option<&C::Val>) -> Result<(), Error> {
        self.try_set_many(&[(key, value)]).await
    }

    /// Tries to durably set multiple key-value pairs in `entries`. Values of `None` deletes the
    /// corresponding entries from the cache.
    ///
    /// On both successes and failures, the cache will update its contents with the most recent
    /// updates from the durable store.
    pub async fn try_set_many(
        &mut self,
        entries: &[(&C::Key, Option<&C::Val>)],
    ) -> Result<(), Error> {
        let expected_upper = self.local_progress;
        let mut updates = Vec::new();
        let mut seen_keys = HashSet::new();

        for (key, val) in entries {
            // If there are duplicate keys we ignore all but the first one.
            if seen_keys.insert(key) {
                if let Some(prev) = self.local.get(key) {
                    updates.push((C::encode(key, prev), expected_upper, -1));
                }
                if let Some(val) = val {
                    updates.push((C::encode(key, val), expected_upper, 1));
                }
            }
        }
        consolidate_updates(&mut updates);

        let new_upper = expected_upper + 1;
        let ret = self
            .write
            .compare_and_append(
                updates,
                Antichain::from_elem(expected_upper),
                Antichain::from_elem(new_upper),
            )
            .await
            .expect("usage should be valid");
        match ret {
            Ok(()) => {
                self.sync_to(Some(new_upper)).await;
                Ok(())
            }
            Err(err) => {
                self.sync_to(err.current.clone().into_option()).await;
                Err(Error::WriteConflict(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_none;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_types::codec_impls::StringSchema;
    use mz_persist_types::PersistLocation;

    use super::*;

    struct TestCodec;

    impl DurableCacheCodec for TestCodec {
        type Key = String;
        type Val = String;
        type KeyCodec = String;
        type ValCodec = String;

        fn schemas() -> (
            <Self::KeyCodec as Codec>::Schema,
            <Self::ValCodec as Codec>::Schema,
        ) {
            (StringSchema, StringSchema)
        }

        fn encode(key: &Self::Key, val: &Self::Val) -> (Self::KeyCodec, Self::ValCodec) {
            (key.clone(), val.clone())
        }

        fn decode(key: Self::KeyCodec, val: Self::ValCodec) -> (Self::Key, Self::Val) {
            (key, val)
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn durable_cache() {
        let persist = PersistClientCache::new_no_metrics();
        let persist = persist
            .open(PersistLocation::new_in_mem())
            .await
            .expect("location should be valid");
        let shard_id = ShardId::new();

        let mut cache0 = DurableCache::<TestCodec>::new(&persist, shard_id, "test1").await;
        assert_none!(cache0.get_local(&"foo".into()));
        assert_eq!(cache0.get(&"foo".into(), || "bar".into()).await, "bar");
        assert_eq!(
            cache0.entries_local().collect::<Vec<_>>(),
            vec![(&"foo".into(), &"bar".into())]
        );

        cache0.set(&"k1".into(), Some(&"v1".into())).await;
        cache0.set(&"k2".into(), Some(&"v2".into())).await;
        assert_eq!(cache0.get_local(&"k1".into()), Some(&"v1".into()));
        assert_eq!(cache0.get(&"k1".into(), || "v10".into()).await, &"v1");
        assert_eq!(cache0.get_local(&"k2".into()), Some(&"v2".into()));
        assert_eq!(cache0.get(&"k2".into(), || "v20".into()).await, &"v2");
        assert_eq!(
            cache0.entries_local().collect::<Vec<_>>(),
            vec![
                (&"foo".into(), &"bar".into()),
                (&"k1".into(), &"v1".into()),
                (&"k2".into(), &"v2".into())
            ]
        );

        cache0.set(&"k1".into(), None).await;
        assert_none!(cache0.get_local(&"k1".into()));
        assert_eq!(
            cache0.entries_local().collect::<Vec<_>>(),
            vec![(&"foo".into(), &"bar".into()), (&"k2".into(), &"v2".into())]
        );

        cache0
            .set_many(&[
                (&"k1".into(), Some(&"v10".into())),
                (&"k2".into(), None),
                (&"k3".into(), None),
            ])
            .await;
        assert_eq!(cache0.get_local(&"k1".into()), Some(&"v10".into()));
        assert_none!(cache0.get_local(&"k2".into()));
        assert_none!(cache0.get_local(&"k3".into()));
        assert_eq!(
            cache0.entries_local().collect::<Vec<_>>(),
            vec![
                (&"foo".into(), &"bar".into()),
                (&"k1".into(), &"v10".into()),
            ]
        );

        cache0
            .set_many(&[
                (&"k4".into(), Some(&"v40".into())),
                (&"k4".into(), Some(&"v41".into())),
                (&"k4".into(), Some(&"v42".into())),
                (&"k5".into(), Some(&"v50".into())),
                (&"k5".into(), Some(&"v51".into())),
                (&"k5".into(), Some(&"v52".into())),
            ])
            .await;
        assert_eq!(cache0.get_local(&"k4".into()), Some(&"v40".into()));
        assert_eq!(cache0.get_local(&"k5".into()), Some(&"v50".into()));
        assert_eq!(
            cache0.entries_local().collect::<Vec<_>>(),
            vec![
                (&"foo".into(), &"bar".into()),
                (&"k1".into(), &"v10".into()),
                (&"k4".into(), &"v40".into()),
                (&"k5".into(), &"v50".into()),
            ]
        );

        let mut cache1 = DurableCache::<TestCodec>::new(&persist, shard_id, "test2").await;
        assert_eq!(cache1.get(&"foo".into(), || panic!("boom")).await, "bar");
        assert_eq!(cache1.get(&"k1".into(), || panic!("boom")).await, &"v10");
        assert_none!(cache1.get_local(&"k2".into()));
        assert_none!(cache1.get_local(&"k3".into()));
        assert_eq!(
            cache1.entries_local().collect::<Vec<_>>(),
            vec![
                (&"foo".into(), &"bar".into()),
                (&"k1".into(), &"v10".into()),
                (&"k4".into(), &"v40".into()),
                (&"k5".into(), &"v50".into()),
            ]
        );
    }
}
