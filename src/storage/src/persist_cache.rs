// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A process-global cache of persist clients to reduce contention on the SQLite consensus
//! implementation

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use once_cell::sync::Lazy;
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_persist_client::{
    error::InvalidUsage, read::ReadHandle, write::WriteHandle, PersistClient, PersistLocation,
    ShardId,
};
use mz_persist_types::{Codec, Codec64};

static CACHE: Lazy<Mutex<HashMap<PersistLocation, PersistClient>>> = Lazy::new(Default::default);

pub(crate) async fn open_reader<K, V, T, D>(
    location: PersistLocation,
    shard: ShardId,
) -> Result<ReadHandle<K, V, T, D>, InvalidUsage<T>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    let mut cache = CACHE.lock().await;
    let client = match cache.entry(location.clone()) {
        Entry::Vacant(e) => e.insert(location.open().await.expect("unrecoverable external error")),
        Entry::Occupied(e) => e.into_mut(),
    };
    client.open_reader(shard).await
}

pub(crate) async fn open_writer<K, V, T, D>(
    location: PersistLocation,
    shard: ShardId,
) -> Result<WriteHandle<K, V, T, D>, InvalidUsage<T>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    let mut cache = CACHE.lock().await;
    let client = match cache.entry(location.clone()) {
        Entry::Vacant(e) => e.insert(location.open().await.expect("unrecoverable external error")),
        Entry::Occupied(e) => e.into_mut(),
    };
    client.open_writer(shard).await
}
