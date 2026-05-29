// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Monotonic in-memory cache of per-shard consensus head state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use mz_persist::location::VersionedData;

/// Per-shard cached state. The cache self-corrects only via the
/// CaS-mismatch refresh path; there is no subscriber-driven freshness loop,
/// so entries carry only a monotonic value and an LRU access tick.
#[derive(Debug)]
pub struct CachedState {
    pub current: Option<VersionedData>,
    /// Monotonically increasing access tick used by LRU eviction.
    pub last_access: u64,
}

#[derive(Debug)]
pub struct ShardCache {
    inner: Mutex<Inner>,
    max_shards: usize,
}

#[derive(Debug)]
struct Inner {
    map: BTreeMap<String, Arc<Mutex<CachedState>>>,
    tick: u64,
}

impl ShardCache {
    pub fn new(max_shards: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                map: BTreeMap::new(),
                tick: 0,
            }),
            max_shards,
        }
    }

    pub fn insert(&self, shard: &str, new: VersionedData) {
        let entry = self.entry(shard);
        let mut guard = entry.lock().expect("ShardCache lock poisoned");
        match &guard.current {
            Some(cur) if cur.seqno >= new.seqno => {}
            _ => guard.current = Some(new),
        }
        guard.last_access = self.bump_tick();
    }

    pub fn get(&self, shard: &str) -> Option<VersionedData> {
        let inner = self.inner.lock().expect("ShardCache lock poisoned");
        let entry = Arc::clone(inner.map.get(shard)?);
        drop(inner);
        let guard = entry.lock().expect("ShardCache lock poisoned");
        guard.current.clone()
    }

    fn entry(&self, shard: &str) -> Arc<Mutex<CachedState>> {
        let mut inner = self.inner.lock().expect("ShardCache lock poisoned");
        if let Some(e) = inner.map.get(shard) {
            return Arc::clone(e);
        }
        if inner.map.len() >= self.max_shards {
            self.evict_one_locked(&mut inner);
        }
        let tick = inner.tick;
        let entry = Arc::new(Mutex::new(CachedState {
            current: None,
            last_access: tick,
        }));
        inner.map.insert(shard.to_string(), Arc::clone(&entry));
        entry
    }

    fn evict_one_locked(&self, inner: &mut Inner) {
        let victim = inner
            .map
            .iter()
            .map(|(k, v)| {
                let g = v.lock().expect("ShardCache lock poisoned");
                (k.clone(), g.last_access)
            })
            .min_by_key(|(_, tick)| *tick)
            .map(|(k, _)| k);
        if let Some(k) = victim {
            inner.map.remove(&k);
        }
    }

    fn bump_tick(&self) -> u64 {
        let mut inner = self.inner.lock().expect("ShardCache lock poisoned");
        inner.tick += 1;
        inner.tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::{SeqNo, VersionedData};

    fn v(seqno: u64) -> VersionedData {
        VersionedData {
            seqno: SeqNo(seqno),
            data: Bytes::from(vec![u8::try_from(seqno & 0xff).unwrap()]),
        }
    }

    #[mz_ore::test]
    fn insert_advances_forward() {
        let c = ShardCache::new(10);
        c.insert("s1", v(5));
        c.insert("s1", v(7));
        assert_eq!(c.get("s1").unwrap().seqno, SeqNo(7));
    }

    #[mz_ore::test]
    fn insert_never_goes_backward() {
        let c = ShardCache::new(10);
        c.insert("s1", v(7));
        c.insert("s1", v(5));
        assert_eq!(c.get("s1").unwrap().seqno, SeqNo(7));
    }

    #[mz_ore::test]
    fn lru_evicts_oldest() {
        let c = ShardCache::new(2);
        c.insert("a", v(1));
        c.insert("b", v(1));
        c.insert("c", v(1));
        // "a" is the least-recently-accessed entry and must be evicted.
        assert!(c.get("a").is_none());
        assert!(c.get("b").is_some());
        assert!(c.get("c").is_some());
    }
}
