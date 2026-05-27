# Persist committer implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an envd-hosted persist committer service that proxies all CRDB consensus traffic from clusterds, replacing the current per-process pool with a single shared pool.

**Architecture:** New crate `mz-persist-committer` runs inside `environmentd`. It exposes the `Consensus` trait over a gRPC service (`ProtoPersistConsensus`) and owns the only `PostgresConsensus` pool. Clusterds use a new `RpcConsensus` client that implements the `Consensus` trait against the service. A monotonic in-memory `ShardCache` short-circuits `head` reads. Rollout is behind LD flags.

**Tech stack:** Rust, tonic (gRPC), prost (protobuf), tokio, `mz-persist`, `mz-persist-client`, deadpool-postgres.

**Spec:** [`doc/developer/design/20260527_persist_committer.md`](../../../doc/developer/design/20260527_persist_committer.md)

---

## File structure

* Create
  * `src/persist-committer/Cargo.toml` — new crate manifest.
  * `src/persist-committer/build.rs` — prost build for the proto.
  * `src/persist-committer/src/lib.rs` — crate root, exports `PersistCommitter`.
  * `src/persist-committer/src/service.proto` — `ProtoPersistConsensus` definition.
  * `src/persist-committer/src/server.rs` — gRPC service impl.
  * `src/persist-committer/src/cache.rs` — `ShardCache` with monotonic insert + LRU.
  * `src/persist-committer/src/subscribe.rs` — subscriber bookkeeping + broadcast.
  * `src/persist-committer/src/refresh.rs` — TTL refresh task.
  * `src/persist-committer/src/metrics.rs` — Prometheus metrics.
  * `src/persist-client/src/rpc_consensus.rs` — client-side `Consensus` impl wrapping tonic channel.
  * `test/persist-committer/mzcompose.py` — integration test.
  * `misc/python/materialize/checks/all_checks/persist_committer.py` — platform check.
* Modify
  * `Cargo.toml` (root) — add `src/persist-committer` to workspace members.
  * `src/persist-client/Cargo.toml` — add tonic dep for `RpcConsensus`.
  * `src/persist-client/src/lib.rs` — `pub mod rpc_consensus`.
  * `src/persist-client/src/cache.rs` — `open_consensus` chooses `PostgresConsensus` vs `RpcConsensus` based on LD flag.
  * `src/persist-client/src/cfg.rs` — new flags `persist_consensus_use_committer`, `persist_committer_cache_enabled`, `persist_committer_max_cached_shards`, `persist_committer_cache_refresh_interval`.
  * `src/environmentd/src/lib.rs` — start `PersistCommitter` at boot; expose its in-process channel and its gRPC server.
  * `deny.toml` / `about.toml` — only if a new dep adds a new SPDX (unlikely).

---

## Scope check

One subsystem, one plan. No decomposition needed.

---

## Task 1: Scaffold the `mz-persist-committer` crate

**Files:**
* Create: `src/persist-committer/Cargo.toml`
* Create: `src/persist-committer/src/lib.rs`
* Modify: `Cargo.toml` (workspace root)

- [ ] **Step 1: Add crate skeleton**

`src/persist-committer/Cargo.toml`:

```toml
[package]
name = "mz-persist-committer"
description = "Centralized persist consensus committer service."
version = "0.1.0"
edition.workspace = true
rust-version.workspace = true
publish = false

[lints]
workspace = true

[dependencies]
anyhow.workspace = true
async-trait.workspace = true
bytes.workspace = true
futures.workspace = true
mz-ore = { path = "../ore", features = ["async", "tracing"] }
mz-persist = { path = "../persist" }
mz-persist-client = { path = "../persist-client" }
mz-proto = { path = "../proto" }
prometheus.workspace = true
prost.workspace = true
serde.workspace = true
thiserror.workspace = true
tokio = { workspace = true, features = ["sync", "rt-multi-thread", "macros", "time"] }
tonic.workspace = true
tracing.workspace = true
uuid.workspace = true

[build-dependencies]
mz-build-tools = { path = "../build-tools", default-features = false }
prost-build.workspace = true
tonic-build.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
```

`src/persist-committer/src/lib.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Centralized persist consensus committer service.
//!
//! See `doc/developer/design/20260527_persist_committer.md`.

pub mod cache;
pub mod metrics;
pub mod refresh;
pub mod server;
pub mod subscribe;

pub use server::PersistCommitter;
```

Workspace `Cargo.toml`: add `"src/persist-committer"` to `[workspace] members` in alphabetical position.

- [ ] **Step 2: Verify crate compiles standalone**

Empty module files for `cache.rs`, `metrics.rs`, `refresh.rs`, `server.rs`, `subscribe.rs` — each with just a copyright header and `//! Placeholder.` doc.

Run: `cargo check -p mz-persist-committer`
Expected: pass with warnings about empty modules (acceptable).

- [ ] **Step 3: Commit**

```bash
git add src/persist-committer Cargo.toml
git commit -m "persist-committer: scaffold new crate"
```

---

## Task 2: Define the `ProtoPersistConsensus` service

**Files:**
* Create: `src/persist-committer/src/service.proto`
* Create: `src/persist-committer/build.rs`
* Modify: `src/persist-committer/src/lib.rs`

- [ ] **Step 1: Write the proto**

`src/persist-committer/src/service.proto`:

```proto
syntax = "proto3";

package mz_persist_committer;

// Wire types mirror mz_persist::location.

message ProtoVersionedData {
  uint64 seqno = 1;
  bytes data = 2;
}

message ProtoHeadRequest { string shard = 1; }
message ProtoHeadResponse { optional ProtoVersionedData current = 1; }

message ProtoCompareAndSetRequest {
  string shard = 1;
  optional uint64 expected = 2; // None means "no prior version"
  ProtoVersionedData new = 3;
}
message ProtoCompareAndSetResponse {
  oneof result {
    Empty committed = 1;          // CaS succeeded
    ProtoVersionedData mismatch = 2; // Current state at time of attempt
  }
}
message Empty {}

message ProtoScanRequest {
  string shard = 1;
  uint64 from = 2;
  uint64 limit = 3;
}
message ProtoScanResponse { repeated ProtoVersionedData versions = 1; }

message ProtoTruncateRequest {
  string shard = 1;
  uint64 seqno = 2;
}
message ProtoTruncateResponse { uint64 deleted = 1; }

message ProtoSubscribeRequest { string shard = 1; }
// Server-streamed: first message is the snapshot, subsequent are diffs.
message ProtoSubscribeMessage {
  oneof kind {
    ProtoVersionedData snapshot = 1;
    ProtoVersionedData diff = 2;
  }
}

service ProtoPersistConsensus {
  rpc Head(ProtoHeadRequest) returns (ProtoHeadResponse);
  rpc Scan(ProtoScanRequest) returns (ProtoScanResponse);
  rpc CompareAndSet(ProtoCompareAndSetRequest) returns (ProtoCompareAndSetResponse);
  rpc Truncate(ProtoTruncateRequest) returns (ProtoTruncateResponse);
  rpc Subscribe(ProtoSubscribeRequest) returns (stream ProtoSubscribeMessage);
}
```

- [ ] **Step 2: Write the build script**

`src/persist-committer/build.rs`:

```rust
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

fn main() {
    let mut config = prost_build::Config::new();
    config
        .protoc_executable(mz_build_tools::protoc())
        .btree_map(["."]);

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos_with_config(
            config,
            &["src/service.proto"],
            &["src/", "../proto/"],
        )
        .expect("compile_protos");

    println!("cargo:rerun-if-changed=src/service.proto");
}
```

Add to `src/persist-committer/src/lib.rs`:

```rust
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/mz_persist_committer.rs"));
}
```

- [ ] **Step 3: Verify it builds**

Run: `cargo check -p mz-persist-committer`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/persist-committer
git commit -m "persist-committer: define ProtoPersistConsensus service"
```

---

## Task 3: Implement `ShardCache` with monotonic merge + LRU

**Files:**
* Create: `src/persist-committer/src/cache.rs`
* Test: same file (`#[cfg(test)] mod tests`)

- [ ] **Step 1: Write the failing tests first**

`src/persist-committer/src/cache.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist::location::{SeqNo, VersionedData};
    use bytes::Bytes;

    fn v(seqno: u64) -> VersionedData {
        VersionedData { seqno: SeqNo(seqno), data: Bytes::from(vec![seqno as u8]) }
    }

    #[test]
    fn insert_advances_forward() {
        let c = ShardCache::new(10);
        let shard = "s1".to_string();
        c.insert(&shard, v(5));
        c.insert(&shard, v(7));
        assert_eq!(c.get(&shard).unwrap().seqno, SeqNo(7));
    }

    #[test]
    fn insert_never_goes_backward() {
        let c = ShardCache::new(10);
        let shard = "s1".to_string();
        c.insert(&shard, v(7));
        c.insert(&shard, v(5));
        assert_eq!(c.get(&shard).unwrap().seqno, SeqNo(7));
    }

    #[test]
    fn lru_evicts_unsubscribed() {
        let c = ShardCache::new(2);
        c.insert(&"a".to_string(), v(1));
        c.insert(&"b".to_string(), v(1));
        c.insert(&"c".to_string(), v(1));
        // One of a/b must have been evicted; c must be present.
        assert!(c.get(&"c".to_string()).is_some());
        let surviving = c.get(&"a".to_string()).is_some() as u8
            + c.get(&"b".to_string()).is_some() as u8;
        assert_eq!(surviving, 1);
    }

    #[test]
    fn subscribers_pin_cache_entry() {
        let c = ShardCache::new(2);
        let token = c.subscribe(&"a".to_string());
        c.insert(&"a".to_string(), v(1));
        c.insert(&"b".to_string(), v(1));
        c.insert(&"c".to_string(), v(1));
        assert!(c.get(&"a".to_string()).is_some());
        drop(token);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p mz-persist-committer --lib cache::tests`
Expected: FAIL (`ShardCache` undefined).

- [ ] **Step 3: Implement `ShardCache`**

```rust
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

/// Per-shard cached state plus a refcount of active subscribers.
#[derive(Debug)]
pub struct CachedState {
    pub current: Option<VersionedData>,
    pub subscribers: usize,
    /// Monotonically increasing access tick used by the LRU eviction policy.
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
            inner: Mutex::new(Inner { map: BTreeMap::new(), tick: 0 }),
            max_shards,
        }
    }

    /// Insert or merge `new`, only if it advances the cached state.
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
        let entry = inner.map.get(shard)?.clone();
        drop(inner);
        let guard = entry.lock().expect("ShardCache lock poisoned");
        guard.current.clone()
    }

    /// Returns a `SubscriberToken`; the entry is pinned while at least one token is alive.
    pub fn subscribe(&self, shard: &str) -> SubscriberToken {
        let entry = self.entry(shard);
        {
            let mut guard = entry.lock().expect("ShardCache lock poisoned");
            guard.subscribers += 1;
        }
        SubscriberToken { shard: shard.to_string(), entry }
    }

    fn entry(&self, shard: &str) -> Arc<Mutex<CachedState>> {
        let mut inner = self.inner.lock().expect("ShardCache lock poisoned");
        if let Some(e) = inner.map.get(shard) {
            return e.clone();
        }
        // Evict if at capacity.
        if inner.map.len() >= self.max_shards {
            self.evict_one(&mut inner);
        }
        let tick = inner.tick;
        let entry = Arc::new(Mutex::new(CachedState {
            current: None,
            subscribers: 0,
            last_access: tick,
        }));
        inner.map.insert(shard.to_string(), entry.clone());
        entry
    }

    fn evict_one(&self, inner: &mut Inner) {
        // Find oldest (by last_access) entry with zero subscribers.
        let victim = inner
            .map
            .iter()
            .filter_map(|(k, v)| {
                let g = v.lock().expect("ShardCache lock poisoned");
                if g.subscribers == 0 {
                    Some((k.clone(), g.last_access))
                } else {
                    None
                }
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

pub struct SubscriberToken {
    shard: String,
    entry: Arc<Mutex<CachedState>>,
}

impl Drop for SubscriberToken {
    fn drop(&mut self) {
        let mut guard = self.entry.lock().expect("ShardCache lock poisoned");
        guard.subscribers = guard.subscribers.saturating_sub(1);
    }
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test -p mz-persist-committer --lib cache::tests`
Expected: PASS, all four tests.

- [ ] **Step 5: Commit**

```bash
git add src/persist-committer/src/cache.rs
git commit -m "persist-committer: add monotonic ShardCache with LRU"
```

---

## Task 4: Implement subscriber broadcast

**Files:**
* Create: `src/persist-committer/src/subscribe.rs`
* Test: same file

- [ ] **Step 1: Write failing tests**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist::location::{SeqNo, VersionedData};
    use bytes::Bytes;

    fn v(seqno: u64) -> VersionedData {
        VersionedData { seqno: SeqNo(seqno), data: Bytes::from(vec![seqno as u8]) }
    }

    #[tokio::test]
    async fn broadcast_reaches_all_subscribers() {
        let registry = SubscriberRegistry::new();
        let mut a = registry.register("s1");
        let mut b = registry.register("s1");
        registry.publish("s1", v(1));
        assert_eq!(a.recv().await.unwrap().seqno, SeqNo(1));
        assert_eq!(b.recv().await.unwrap().seqno, SeqNo(1));
    }

    #[tokio::test]
    async fn other_shards_do_not_receive() {
        let registry = SubscriberRegistry::new();
        let mut a = registry.register("s1");
        registry.publish("s2", v(1));
        // No data should be buffered.
        assert!(a.try_recv().is_err());
    }

    #[tokio::test]
    async fn slow_subscriber_drops_old_messages() {
        // capacity 2: third publish must drop the oldest, not block.
        let registry = SubscriberRegistry::with_buffer(2);
        let mut a = registry.register("s1");
        registry.publish("s1", v(1));
        registry.publish("s1", v(2));
        registry.publish("s1", v(3));
        // Subscriber receives the latest two.
        let first = a.recv().await.unwrap();
        let second = a.recv().await.unwrap();
        assert_eq!(second.seqno, SeqNo(3));
        assert!(first.seqno >= SeqNo(2));
    }
}
```

- [ ] **Step 2: Run tests, verify fail**

Run: `cargo test -p mz-persist-committer --lib subscribe::tests`
Expected: FAIL.

- [ ] **Step 3: Implement subscriber registry**

```rust
//! Per-shard subscriber registry with bounded broadcast.

use std::collections::BTreeMap;
use std::sync::Mutex;

use mz_persist::location::VersionedData;
use tokio::sync::broadcast;

const DEFAULT_BUFFER: usize = 64;

pub struct SubscriberRegistry {
    channels: Mutex<BTreeMap<String, broadcast::Sender<VersionedData>>>,
    buffer: usize,
}

impl SubscriberRegistry {
    pub fn new() -> Self {
        Self::with_buffer(DEFAULT_BUFFER)
    }

    pub fn with_buffer(buffer: usize) -> Self {
        Self { channels: Mutex::new(BTreeMap::new()), buffer }
    }

    pub fn register(&self, shard: &str) -> broadcast::Receiver<VersionedData> {
        let mut channels = self.channels.lock().expect("registry lock poisoned");
        let sender = channels
            .entry(shard.to_string())
            .or_insert_with(|| broadcast::channel(self.buffer).0);
        sender.subscribe()
    }

    pub fn publish(&self, shard: &str, data: VersionedData) {
        let sender = {
            let channels = self.channels.lock().expect("registry lock poisoned");
            channels.get(shard).cloned()
        };
        if let Some(s) = sender {
            // Ignore SendError(_) — it just means no subscribers.
            let _ = s.send(data);
        }
    }
}

impl Default for SubscriberRegistry {
    fn default() -> Self { Self::new() }
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test -p mz-persist-committer --lib subscribe::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/persist-committer/src/subscribe.rs
git commit -m "persist-committer: add per-shard subscriber broadcast"
```

---

## Task 5: Implement the gRPC server (Head, Scan, CompareAndSet, Truncate)

**Files:**
* Create/extend: `src/persist-committer/src/server.rs`
* Test: same file (uses an in-memory `Consensus` impl for the underlying)

- [ ] **Step 1: Write failing integration test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::{Consensus, MemConsensus, SeqNo, VersionedData};
    use std::sync::Arc;

    fn v(seqno: u64, byte: u8) -> VersionedData {
        VersionedData { seqno: SeqNo(seqno), data: Bytes::from(vec![byte]) }
    }

    #[tokio::test]
    async fn head_reads_from_consensus_on_miss() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        underlying
            .compare_and_set("s1", None, v(1, 0xAA))
            .await
            .unwrap()
            .unwrap();
        let committer = PersistCommitter::new(underlying, ShardCache::new(100), SubscriberRegistry::new());

        let got = committer.head_inner("s1").await.unwrap();
        assert_eq!(got.unwrap().seqno, SeqNo(1));
    }

    #[tokio::test]
    async fn head_returns_cached_value() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        let cache = ShardCache::new(100);
        cache.insert("s1", v(5, 0xCC));
        let committer = PersistCommitter::new(underlying, cache, SubscriberRegistry::new());
        let got = committer.head_inner("s1").await.unwrap();
        assert_eq!(got.unwrap().seqno, SeqNo(5));
    }

    #[tokio::test]
    async fn cas_success_updates_cache_and_publishes() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        let cache = ShardCache::new(100);
        let registry = SubscriberRegistry::new();
        let mut sub = registry.register("s1");
        let committer = PersistCommitter::new(underlying.clone(), cache, registry);

        committer.cas_inner("s1", None, v(1, 0xAA)).await.unwrap().unwrap();
        let pushed = sub.recv().await.unwrap();
        assert_eq!(pushed.seqno, SeqNo(1));
        let head = committer.head_inner("s1").await.unwrap().unwrap();
        assert_eq!(head.seqno, SeqNo(1));
    }

    #[tokio::test]
    async fn cas_mismatch_returns_current_and_updates_cache() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        underlying.compare_and_set("s1", None, v(1, 0xAA)).await.unwrap().unwrap();
        let committer = PersistCommitter::new(underlying, ShardCache::new(100), SubscriberRegistry::new());

        let result = committer.cas_inner("s1", Some(SeqNo(99)), v(100, 0xBB)).await.unwrap();
        match result {
            Err(current) => assert_eq!(current.unwrap().seqno, SeqNo(1)),
            Ok(()) => panic!("expected mismatch"),
        }
    }
}
```

- [ ] **Step 2: Run tests, verify fail**

Run: `cargo test -p mz-persist-committer --lib server::tests`
Expected: FAIL (`PersistCommitter` undefined).

- [ ] **Step 3: Implement `PersistCommitter` with inner methods**

```rust
//! gRPC service implementation for persist consensus.

use std::sync::Arc;

use anyhow::Result;
use mz_persist::location::{Consensus, SeqNo, VersionedData};

use crate::cache::ShardCache;
use crate::subscribe::SubscriberRegistry;

pub struct PersistCommitter {
    consensus: Arc<dyn Consensus + Send + Sync>,
    cache: ShardCache,
    registry: SubscriberRegistry,
}

impl PersistCommitter {
    pub fn new(
        consensus: Arc<dyn Consensus + Send + Sync>,
        cache: ShardCache,
        registry: SubscriberRegistry,
    ) -> Self {
        Self { consensus, cache, registry }
    }

    pub async fn head_inner(&self, shard: &str) -> Result<Option<VersionedData>> {
        if let Some(cached) = self.cache.get(shard) {
            return Ok(Some(cached));
        }
        let head = self.consensus.head(shard).await?;
        if let Some(v) = &head {
            self.cache.insert(shard, v.clone());
        }
        Ok(head)
    }

    pub async fn scan_inner(
        &self,
        shard: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>> {
        // No caching; cold path.
        Ok(self.consensus.scan(shard, from, limit).await?)
    }

    pub async fn cas_inner(
        &self,
        shard: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<std::result::Result<(), Option<VersionedData>>> {
        let result = self.consensus.compare_and_set(shard, expected, new.clone()).await?;
        match result {
            Ok(()) => {
                self.cache.insert(shard, new.clone());
                self.registry.publish(shard, new);
                Ok(Ok(()))
            }
            Err(current) => {
                if let Some(v) = &current {
                    self.cache.insert(shard, v.clone());
                }
                Ok(Err(current))
            }
        }
    }

    pub async fn truncate_inner(&self, shard: &str, seqno: SeqNo) -> Result<usize> {
        Ok(self.consensus.truncate(shard, seqno).await?)
    }
}
```

- [ ] **Step 4: Run tests, verify pass**

Run: `cargo test -p mz-persist-committer --lib server::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/persist-committer/src/server.rs
git commit -m "persist-committer: implement Head/Scan/CompareAndSet/Truncate logic"
```

---

## Task 6: Wire `PersistCommitter` to the tonic-generated service trait

**Files:**
* Modify: `src/persist-committer/src/server.rs`

- [ ] **Step 1: Implement the generated trait**

Add after `PersistCommitter`:

```rust
use crate::proto::proto_persist_consensus_server::{
    ProtoPersistConsensus, ProtoPersistConsensusServer,
};
use crate::proto::{
    proto_compare_and_set_response, proto_subscribe_message, Empty, ProtoCompareAndSetRequest,
    ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse, ProtoScanRequest,
    ProtoScanResponse, ProtoSubscribeMessage, ProtoSubscribeRequest, ProtoTruncateRequest,
    ProtoTruncateResponse, ProtoVersionedData,
};
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use tokio_stream::wrappers::BroadcastStream;
use tonic::{Request, Response, Status};

type SubscribeStream =
    Pin<Box<dyn Stream<Item = std::result::Result<ProtoSubscribeMessage, Status>> + Send>>;

fn to_proto(v: VersionedData) -> ProtoVersionedData {
    ProtoVersionedData { seqno: v.seqno.0, data: v.data.into() }
}

fn from_proto(p: ProtoVersionedData) -> VersionedData {
    VersionedData { seqno: SeqNo(p.seqno), data: Bytes::from(p.data) }
}

#[async_trait::async_trait]
impl ProtoPersistConsensus for Arc<PersistCommitter> {
    async fn head(
        &self,
        request: Request<ProtoHeadRequest>,
    ) -> std::result::Result<Response<ProtoHeadResponse>, Status> {
        let shard = request.into_inner().shard;
        let current = self.head_inner(&shard).await.map_err(internal)?;
        Ok(Response::new(ProtoHeadResponse { current: current.map(to_proto) }))
    }

    async fn scan(
        &self,
        request: Request<ProtoScanRequest>,
    ) -> std::result::Result<Response<ProtoScanResponse>, Status> {
        let r = request.into_inner();
        let versions = self
            .scan_inner(&r.shard, SeqNo(r.from), r.limit as usize)
            .await
            .map_err(internal)?;
        Ok(Response::new(ProtoScanResponse {
            versions: versions.into_iter().map(to_proto).collect(),
        }))
    }

    async fn compare_and_set(
        &self,
        request: Request<ProtoCompareAndSetRequest>,
    ) -> std::result::Result<Response<ProtoCompareAndSetResponse>, Status> {
        let r = request.into_inner();
        let new = from_proto(r.new.ok_or_else(|| Status::invalid_argument("missing new"))?);
        let expected = r.expected.map(SeqNo);
        let result = self.cas_inner(&r.shard, expected, new).await.map_err(internal)?;
        let response = match result {
            Ok(()) => proto_compare_and_set_response::Result::Committed(Empty {}),
            Err(current) => proto_compare_and_set_response::Result::Mismatch(
                current.map(to_proto).unwrap_or_default(),
            ),
        };
        Ok(Response::new(ProtoCompareAndSetResponse { result: Some(response) }))
    }

    async fn truncate(
        &self,
        request: Request<ProtoTruncateRequest>,
    ) -> std::result::Result<Response<ProtoTruncateResponse>, Status> {
        let r = request.into_inner();
        let deleted = self
            .truncate_inner(&r.shard, SeqNo(r.seqno))
            .await
            .map_err(internal)?;
        Ok(Response::new(ProtoTruncateResponse { deleted: deleted as u64 }))
    }

    type SubscribeStream = SubscribeStream;

    async fn subscribe(
        &self,
        request: Request<ProtoSubscribeRequest>,
    ) -> std::result::Result<Response<Self::SubscribeStream>, Status> {
        let shard = request.into_inner().shard;
        let snapshot = self.head_inner(&shard).await.map_err(internal)?;
        let _token = self.cache.subscribe(&shard);
        let rx = self.registry.register(&shard);
        let snapshot_msg = ProtoSubscribeMessage {
            kind: Some(proto_subscribe_message::Kind::Snapshot(
                snapshot.map(to_proto).unwrap_or_default(),
            )),
        };
        let stream = async_stream::try_stream! {
            yield snapshot_msg;
            let mut rx = BroadcastStream::new(rx);
            while let Some(item) = futures::StreamExt::next(&mut rx).await {
                match item {
                    Ok(v) => yield ProtoSubscribeMessage {
                        kind: Some(proto_subscribe_message::Kind::Diff(to_proto(v))),
                    },
                    Err(_) => continue, // lagged: subscriber must re-sync via Head
                }
            }
            drop(_token);
        };
        Ok(Response::new(Box::pin(stream)))
    }
}

fn internal(e: anyhow::Error) -> Status {
    Status::internal(e.to_string())
}

impl PersistCommitter {
    pub fn into_service(self: Arc<Self>) -> ProtoPersistConsensusServer<Arc<PersistCommitter>> {
        ProtoPersistConsensusServer::new(self)
    }
}
```

Add to `src/persist-committer/Cargo.toml`:

```toml
async-stream.workspace = true
tokio-stream = { workspace = true, features = ["sync"] }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo check -p mz-persist-committer`
Expected: pass.

- [ ] **Step 3: Run existing tests, still pass**

Run: `cargo test -p mz-persist-committer`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/persist-committer
git commit -m "persist-committer: wire gRPC service trait impl"
```

---

## Task 7: Implement TTL refresh task

**Files:**
* Create: `src/persist-committer/src/refresh.rs`
* Modify: `src/persist-committer/src/server.rs` (start refresh task in `PersistCommitter::new`)

- [ ] **Step 1: Write the failing test**

`src/persist-committer/src/refresh.rs`:

```rust
//! TTL refresh task for subscribed shards.

use std::sync::Arc;
use std::time::Duration;

use mz_persist::location::Consensus;
use tokio::task::JoinHandle;

use crate::cache::ShardCache;
use crate::subscribe::SubscriberRegistry;

/// Spawns a background task that periodically re-`head`s every cached shard with active subscribers.
pub fn spawn_refresh(
    consensus: Arc<dyn Consensus + Send + Sync>,
    cache: Arc<ShardCache>,
    registry: Arc<SubscriberRegistry>,
    interval: Duration,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let subscribed: Vec<String> = cache.shards_with_subscribers();
            for shard in subscribed {
                match consensus.head(&shard).await {
                    Ok(Some(v)) => {
                        let prev = cache.get(&shard).map(|p| p.seqno);
                        let new_seqno = v.seqno;
                        cache.insert(&shard, v.clone());
                        if Some(new_seqno) != prev {
                            registry.publish(&shard, v);
                        }
                    }
                    Ok(None) | Err(_) => {} // ignore; next tick retries
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::{MemConsensus, SeqNo, VersionedData};

    #[tokio::test(start_paused = true)]
    async fn refresh_updates_cache_when_underlying_changes() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        let cache = Arc::new(ShardCache::new(100));
        let registry = Arc::new(SubscriberRegistry::new());
        let _token = cache.subscribe("s1");
        cache.insert("s1", VersionedData { seqno: SeqNo(1), data: Bytes::from(vec![0]) });
        // Underlying jumps ahead to seqno=5 without going through committer.
        underlying
            .compare_and_set("s1", None, VersionedData { seqno: SeqNo(5), data: Bytes::from(vec![5]) })
            .await
            .unwrap()
            .unwrap();
        let _handle = spawn_refresh(underlying, cache.clone(), registry, Duration::from_secs(1));
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        assert_eq!(cache.get("s1").unwrap().seqno, SeqNo(5));
    }
}
```

Add to `src/persist-committer/src/cache.rs`:

```rust
impl ShardCache {
    pub fn shards_with_subscribers(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("ShardCache lock poisoned");
        inner
            .map
            .iter()
            .filter_map(|(k, v)| {
                let g = v.lock().expect("ShardCache lock poisoned");
                if g.subscribers > 0 { Some(k.clone()) } else { None }
            })
            .collect()
    }
}
```

- [ ] **Step 2: Run, verify fail**

Run: `cargo test -p mz-persist-committer --lib refresh::tests`
Expected: FAIL.

- [ ] **Step 3: Already implemented — re-run to verify pass**

Run: `cargo test -p mz-persist-committer`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/persist-committer/src/refresh.rs src/persist-committer/src/cache.rs
git commit -m "persist-committer: add TTL refresh task"
```

---

## Task 8: Add metrics

**Files:**
* Create: `src/persist-committer/src/metrics.rs`
* Modify: `src/persist-committer/src/server.rs` (record metrics in each inner method)

- [ ] **Step 1: Define metrics struct**

```rust
//! Prometheus metrics for the persist committer.

use mz_ore::metric;
use mz_ore::metrics::{IntCounterVec, HistogramVec, IntGauge, MetricsRegistry};

#[derive(Clone, Debug)]
pub struct CommitterMetrics {
    pub rpc_total: IntCounterVec,
    pub rpc_duration_seconds: HistogramVec,
    pub cache_hits_total: IntCounterVec,
    pub cache_misses_total: IntCounterVec,
    pub cached_shards: IntGauge,
    pub inflight_rpcs: IntGauge,
}

impl CommitterMetrics {
    pub fn register(registry: &MetricsRegistry) -> Self {
        Self {
            rpc_total: registry.register(metric!(
                name: "mz_persist_committer_rpc_total",
                help: "Count of committer RPCs by op and result.",
                var_labels: ["op", "result"],
            )),
            rpc_duration_seconds: registry.register(metric!(
                name: "mz_persist_committer_rpc_duration_seconds",
                help: "Latency of committer RPCs.",
                var_labels: ["op"],
            )),
            cache_hits_total: registry.register(metric!(
                name: "mz_persist_committer_cache_hits_total",
                help: "Cache hits by op.",
                var_labels: ["op"],
            )),
            cache_misses_total: registry.register(metric!(
                name: "mz_persist_committer_cache_misses_total",
                help: "Cache misses by op.",
                var_labels: ["op"],
            )),
            cached_shards: registry.register(metric!(
                name: "mz_persist_committer_cached_shards",
                help: "Number of shards in the in-memory cache.",
            )),
            inflight_rpcs: registry.register(metric!(
                name: "mz_persist_committer_inflight_rpcs",
                help: "In-flight committer RPCs.",
            )),
        }
    }
}
```

- [ ] **Step 2: Thread metrics through `PersistCommitter`**

Modify `PersistCommitter::new` to take `metrics: CommitterMetrics`. Increment counters and observe histograms in each `*_inner` method. Update existing tests to pass `CommitterMetrics::register(&MetricsRegistry::new())`.

- [ ] **Step 3: Compile + tests**

Run: `cargo test -p mz-persist-committer`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add src/persist-committer
git commit -m "persist-committer: add Prometheus metrics"
```

---

## Task 9: Implement client-side `RpcConsensus`

**Files:**
* Create: `src/persist-client/src/rpc_consensus.rs`
* Modify: `src/persist-client/Cargo.toml` (add `mz-persist-committer` dep for the proto module)
* Modify: `src/persist-client/src/lib.rs`

- [ ] **Step 1: Write failing test**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::{Consensus, MemConsensus, SeqNo, VersionedData};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tonic::transport::{Endpoint, Server};

    #[tokio::test]
    async fn rpc_consensus_round_trip() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        let committer = Arc::new(mz_persist_committer::PersistCommitter::new_test(underlying.clone()));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            Server::builder()
                .add_service(committer.into_service())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let channel = Endpoint::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let rpc = RpcConsensus::new(channel);

        // CAS, then head.
        rpc.compare_and_set("s1", None, VersionedData { seqno: SeqNo(1), data: Bytes::from(vec![0xAA]) })
            .await
            .unwrap()
            .unwrap();
        let head = rpc.head("s1").await.unwrap().unwrap();
        assert_eq!(head.seqno, SeqNo(1));
    }
}
```

(Adds `PersistCommitter::new_test` convenience to `mz-persist-committer` that defaults all cache/metrics knobs.)

- [ ] **Step 2: Run, verify fail**

Run: `cargo test -p mz-persist-client --lib rpc_consensus::tests`
Expected: FAIL.

- [ ] **Step 3: Implement `RpcConsensus`**

```rust
//! Client-side `Consensus` implementation that routes through the persist committer.

use async_trait::async_trait;
use bytes::Bytes;
use mz_persist::location::{Consensus, ExternalError, SeqNo, VersionedData};
use mz_persist_committer::proto::proto_persist_consensus_client::ProtoPersistConsensusClient;
use mz_persist_committer::proto::{
    proto_compare_and_set_response, ProtoCompareAndSetRequest, ProtoHeadRequest, ProtoScanRequest,
    ProtoTruncateRequest, ProtoVersionedData,
};
use tonic::transport::Channel;

#[derive(Debug, Clone)]
pub struct RpcConsensus {
    client: ProtoPersistConsensusClient<Channel>,
}

impl RpcConsensus {
    pub fn new(channel: Channel) -> Self {
        Self { client: ProtoPersistConsensusClient::new(channel) }
    }
}

fn into_external(e: tonic::Status) -> ExternalError {
    ExternalError::from(anyhow::anyhow!("committer rpc: {}", e))
}

fn from_proto(p: ProtoVersionedData) -> VersionedData {
    VersionedData { seqno: SeqNo(p.seqno), data: Bytes::from(p.data) }
}

fn to_proto(v: VersionedData) -> ProtoVersionedData {
    ProtoVersionedData { seqno: v.seqno.0, data: v.data.into() }
}

#[async_trait]
impl Consensus for RpcConsensus {
    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .head(ProtoHeadRequest { shard: key.to_string() })
            .await
            .map_err(into_external)?
            .into_inner();
        Ok(resp.current.map(from_proto))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .compare_and_set(ProtoCompareAndSetRequest {
                shard: key.to_string(),
                expected: expected.map(|s| s.0),
                new: Some(to_proto(new)),
            })
            .await
            .map_err(into_external)?
            .into_inner();
        match resp.result {
            Some(proto_compare_and_set_response::Result::Committed(_)) => Ok(Ok(())),
            Some(proto_compare_and_set_response::Result::Mismatch(v)) => {
                let current = if v.seqno == 0 && v.data.is_empty() {
                    None
                } else {
                    Some(from_proto(v))
                };
                Ok(Err(current))
            }
            None => Err(ExternalError::from(anyhow::anyhow!(
                "committer rpc: empty CompareAndSet response"
            ))),
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .scan(ProtoScanRequest {
                shard: key.to_string(),
                from: from.0,
                limit: u64::try_from(limit).unwrap_or(u64::MAX),
            })
            .await
            .map_err(into_external)?
            .into_inner();
        Ok(resp.versions.into_iter().map(from_proto).collect())
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let mut client = self.client.clone();
        let resp = client
            .truncate(ProtoTruncateRequest { shard: key.to_string(), seqno: seqno.0 })
            .await
            .map_err(into_external)?
            .into_inner();
        Ok(usize::try_from(resp.deleted).unwrap_or(usize::MAX))
    }
}
```

(Encoding of `None` for the mismatch current is via the all-zero `ProtoVersionedData`; if this turns out to be ambiguous in practice, switch the proto to use `optional`.)

- [ ] **Step 4: Run, verify pass**

Run: `cargo test -p mz-persist-client --lib rpc_consensus::tests`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/persist-client/src/rpc_consensus.rs src/persist-client/Cargo.toml src/persist-client/src/lib.rs
git commit -m "persist-client: add RpcConsensus client for persist committer"
```

---

## Task 10: Add LD flags

**Files:**
* Modify: `src/persist-client/src/cfg.rs`

- [ ] **Step 1: Add flags**

Follow the pattern of existing `CONSENSUS_CONNECTION_POOL_MAX_SIZE` in the same file:

```rust
pub(crate) const PERSIST_CONSENSUS_USE_COMMITTER: Config<bool> = Config::new(
    "persist_consensus_use_committer",
    false,
    "If true, route consensus traffic through the in-envd persist committer over gRPC \
     instead of opening a direct CockroachDB connection pool per process.",
);

pub(crate) const PERSIST_COMMITTER_CACHE_ENABLED: Config<bool> = Config::new(
    "persist_committer_cache_enabled",
    true,
    "If false, the committer server forwards every Head to CRDB without using its cache.",
);

pub(crate) const PERSIST_COMMITTER_MAX_CACHED_SHARDS: Config<usize> = Config::new(
    "persist_committer_max_cached_shards",
    10_000,
    "Hard cap on the number of shards held in the committer's in-memory cache.",
);

pub(crate) const PERSIST_COMMITTER_CACHE_REFRESH_INTERVAL: Config<Duration> = Config::new(
    "persist_committer_cache_refresh_interval",
    Duration::from_secs(5),
    "Interval at which the committer re-reads subscribed shards from CRDB to bound staleness.",
);
```

Register them in `all_dyn_configs` at the same call site as the existing flags.

- [ ] **Step 2: Compile**

Run: `cargo check -p mz-persist-client`
Expected: pass.

- [ ] **Step 3: Commit**

```bash
git add src/persist-client/src/cfg.rs
git commit -m "persist-client: add LD flags for persist committer"
```

---

## Task 11: Wire `PersistClientCache` to choose `RpcConsensus` vs `PostgresConsensus`

**Files:**
* Modify: `src/persist-client/src/cache.rs`

- [ ] **Step 1: Add a constructor variant**

Replace the bare `PersistClientCache::new(persist_cfg, ...)` callers paths to also pass an optional `committer_channel: Option<Channel>`. When `PERSIST_CONSENSUS_USE_COMMITTER` is `true` and a channel is provided, `open_consensus` returns `Arc::new(RpcConsensus::new(channel.clone()))`; otherwise the existing `PostgresConsensus` path is used.

Concrete change in `open_consensus(location)`:

```rust
let use_committer = PERSIST_CONSENSUS_USE_COMMITTER.get(&self.cfg.configs);
let consensus: Arc<dyn Consensus + Send + Sync> = if use_committer {
    let channel = self
        .committer_channel
        .as_ref()
        .ok_or_else(|| anyhow!("committer flag on but no channel configured"))?;
    Arc::new(crate::rpc_consensus::RpcConsensus::new(channel.clone()))
} else {
    Arc::new(PostgresConsensus::open(location, &self.cfg).await?)
};
```

(Adapt names to match the actual `PersistClientCache` field layout — verify by reading the file before editing.)

- [ ] **Step 2: Update existing callers**

Every `PersistClientCache::new` call site (envd, clusterd, tests) gets the new arg. Tests pass `None`. The flag default is `false`, so behavior is unchanged.

- [ ] **Step 3: Compile**

Run: `cargo check --workspace --tests`
Expected: pass.

- [ ] **Step 4: Commit**

Identify call sites with: `cargo check --workspace --tests 2>&1 | grep "PersistClientCache::new"`.
Update each touched file, then:

```bash
git add src/persist-client/src/cache.rs
git add $(git diff --name-only -- 'src/**/*.rs')
git commit -m "persist-client: select RpcConsensus when committer flag is on"
```

---

## Task 12: Start `PersistCommitter` in `environmentd`

**Files:**
* Modify: `src/environmentd/src/lib.rs` (or wherever the persist cache is constructed)
* Modify: `src/environmentd/src/server.rs` (gRPC server registration; check actual location)

- [ ] **Step 1: Construct the committer**

In the envd startup, after the existing `PostgresConsensus` is opened (still used as the committer's underlying), construct:

```rust
let consensus = Arc::new(PostgresConsensus::open(consensus_uri, &persist_cfg).await?);
let cache = Arc::new(ShardCache::new(
    PERSIST_COMMITTER_MAX_CACHED_SHARDS.get(&persist_cfg.configs),
));
let registry = Arc::new(SubscriberRegistry::new());
let metrics = CommitterMetrics::register(&metrics_registry);
let committer = Arc::new(PersistCommitter::new(
    consensus.clone(),
    cache.clone(),
    registry.clone(),
    metrics,
));
let _refresh_handle = spawn_refresh(
    consensus,
    cache,
    registry,
    PERSIST_COMMITTER_CACHE_REFRESH_INTERVAL.get(&persist_cfg.configs),
);
```

- [ ] **Step 2: Register the gRPC service**

Where envd already configures a tonic server (search for `Server::builder` in `src/environmentd/`), add:

```rust
.add_service(committer.clone().into_service())
```

Use a separate listener if envd's existing gRPC port is already serving controller traffic; otherwise multiplex on the same port via tonic's `Routes`.

- [ ] **Step 3: Build an in-process channel for envd's own client**

```rust
let (in_proc_client, in_proc_server) = tonic::transport::Channel::loopback();
// Spawn an inner server on `in_proc_server` with only the committer service.
```

Pass `in_proc_client` as the `committer_channel` into envd's `PersistClientCache::new` so envd uses the same code path (when the LD flag is on) instead of a special case.

- [ ] **Step 4: Compile + run smoke**

Run: `bin/environmentd --reset` (locally, with cockroach available per CLAUDE.md instructions).
Confirm `mz_persist_committer_cached_shards` is exposed at the metrics endpoint.

- [ ] **Step 5: Commit**

```bash
git add src/environmentd
git commit -m "environmentd: host persist committer and expose its gRPC service"
```

---

## Task 13: Plumb the committer channel into clusterd's `PersistClientCache`

**Files:**
* Modify: `src/clusterd/src/lib.rs`
* Modify: `src/compute-client/...` and/or `src/storage-client/...` (the place that supplies the envd endpoint URL today)

- [ ] **Step 1: Add CLI flag**

`src/clusterd/src/lib.rs`: add `--persist-committer-url` clap arg. Plumb through to `PersistClientCache::new(... , committer_channel)` where the channel is built by `Channel::from_shared(url).unwrap().connect_lazy()`.

- [ ] **Step 2: Orchestrator wires the URL**

Wherever envd hands out clusterd command-line args (search for `clusterd` argv construction in `src/cluster-client/`, `src/orchestrator-*`, `src/environmentd`), pass the new URL.

- [ ] **Step 3: Compile + lint**

Run: `cargo check --workspace && bin/lint`
Expected: pass.

- [ ] **Step 4: Commit**

```bash
git add src/clusterd src/cluster-client src/orchestrator-* src/environmentd
git commit -m "clusterd: accept persist committer URL and wire to PersistClientCache"
```

---

## Task 14: mzcompose integration test

**Files:**
* Create: `test/persist-committer/mzcompose.py`

- [ ] **Step 1: Add the composition**

```python
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized
from materialize.mzcompose.services.testdrive import Testdrive

SERVICES = [
    Materialized(
        additional_system_parameter_defaults={
            "persist_consensus_use_committer": "true",
        }
    ),
    Testdrive(),
]


def workflow_default(c: Composition) -> None:
    c.up("materialized")
    c.run_testdrive_files("smoke.td")
    # Restart envd; clusterds must reconnect to the new committer.
    c.kill("materialized")
    c.up("materialized")
    c.run_testdrive_files("smoke.td")
```

`test/persist-committer/smoke.td`:

```
$ set-sql-timeout duration=30s

> CREATE TABLE t (a int);
> INSERT INTO t VALUES (1), (2), (3);
> CREATE MATERIALIZED VIEW mv AS SELECT count(*) FROM t;
> SELECT * FROM mv;
3

> INSERT INTO t VALUES (4);
> SELECT * FROM mv;
4
```

- [ ] **Step 2: Run it**

Run: `bin/mzcompose --find persist-committer run default`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add test/persist-committer
git commit -m "test: persist-committer mzcompose smoke and restart test"
```

---

## Task 15: Platform check for upgrade survival

**Files:**
* Create: `misc/python/materialize/checks/all_checks/persist_committer.py`

- [ ] **Step 1: Implement the check**

Pattern after an existing simple check in `misc/python/materialize/checks/all_checks/`:

```python
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from materialize.checks.actions import Testdrive
from materialize.checks.checks import Check


class PersistCommitter(Check):
    """Verifies that shard state written via the committer survives envd restart."""

    def initialize(self) -> Testdrive:
        return Testdrive(
            """
            > CREATE TABLE committer_t (a int);
            > INSERT INTO committer_t VALUES (1), (2), (3);
            > CREATE MATERIALIZED VIEW committer_mv AS SELECT count(*) FROM committer_t;
            """
        )

    def manipulate(self) -> list[Testdrive]:
        return [
            Testdrive("> INSERT INTO committer_t VALUES (4);"),
            Testdrive("> INSERT INTO committer_t VALUES (5);"),
        ]

    def validate(self) -> Testdrive:
        return Testdrive(
            """
            > SELECT * FROM committer_mv;
            5
            """
        )
```

- [ ] **Step 2: Run it**

Run: `bin/mzcompose --find platform-checks run default --check PersistCommitter`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add misc/python/materialize/checks/all_checks/persist_committer.py
git commit -m "platform-checks: add PersistCommitter check"
```

---

## Task 16: Lint + format + final compile

- [ ] **Step 1: Format**

Run: `cargo fmt && bin/pyfmt`

- [ ] **Step 2: Clippy**

Run: `cargo clippy --workspace --all-targets`
Expected: no warnings.

- [ ] **Step 3: Lint**

Run: `bin/lint`
Expected: pass.

- [ ] **Step 4: Full test suite for touched crates**

Run: `cargo nextest run -p mz-persist-committer -p mz-persist-client`
Expected: PASS.

- [ ] **Step 5: Commit any format/lint fixes**

```bash
git add $(git diff --name-only)
git commit -m "persist-committer: fmt + clippy fixes"
```

---

## Self-review checklist

* Every spec section maps to at least one task: connection-reduction goal → Tasks 1-13; caching rules → Tasks 3, 5, 7; CaS forwarding (no fast-reject) → Task 5 (`cas_inner` always calls underlying); failover handling → Tasks 11-13 (LD flag + lazy connect + retryable errors via `ExternalError`); metrics → Task 8; mzcompose + platform check → Tasks 14-15.
* No placeholders or "TBD" in task bodies.
* Type names consistent across tasks: `PersistCommitter`, `ShardCache`, `SubscriberRegistry`, `RpcConsensus`, `CommitterMetrics`, `ProtoPersistConsensus`.
* Open spec questions (leader discovery for clusterd's committer URL, in-process channel choice) are addressed concretely in Tasks 12 and 13 with explicit search-and-wire instructions, rather than left to the implementer's imagination.

---

## Known gaps that require follow-up tasks once early implementation settles

* Same-shard CaS serialization (spec alternative C). Add when CaS-retry metrics justify it.
* PubSub stream consolidation onto the committer channel. Tracked as a v2 optimization.
* `mz-debug` / observability tooling updates to surface the committer's metrics in the standard dashboards.
