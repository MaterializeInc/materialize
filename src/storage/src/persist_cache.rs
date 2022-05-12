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
use lazy_static::lazy_static;
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_persist_client::{
    error::InvalidUsage, read::ReadHandle, write::WriteHandle, PersistClient, PersistLocation,
    ShardId,
};
use mz_persist_types::{Codec, Codec64};

lazy_static! {
    static ref CACHE: Mutex<HashMap<PersistLocation, PersistClient>> = Mutex::new(HashMap::new());
}

pub(crate) async fn open<K, V, T, D>(
    location: PersistLocation,
    shard: ShardId,
) -> Result<(WriteHandle<K, V, T, D>, ReadHandle<K, V, T, D>), InvalidUsage<T>>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64,
{
    let mut cache = CACHE.lock().await;
    let persist_client = match cache.entry(location.clone()) {
        Entry::Vacant(e) => e.insert(location.open().await.expect("unrecoverable external error")),
        Entry::Occupied(e) => e.into_mut(),
    };

    persist_client.open::<K, V, T, D>(shard).await
}
