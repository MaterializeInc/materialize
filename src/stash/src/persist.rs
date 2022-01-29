// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{hash_map, HashMap};
use std::fmt;
use std::hash::Hash;
use std::vec;

use ore::metrics::MetricsRegistry;
use persist::client::{RuntimeClient, StreamWriteHandle};
use persist::indexed::Snapshot;
use persist::runtime::{self, RuntimeConfig};
use persist::storage::{Blob, Log};
use persist_types::Codec;
use timely::progress::Antichain;

use crate::{Stash, StashError, StashOp};

type Error = StashError<persist::error::Error>;

/// A stash backed by a [`persist`] collection.
#[derive(Debug)]
pub struct PersistStash<K, V> {
    _client: RuntimeClient,
    write_handle: StreamWriteHandle<K, V>,
    data: HashMap<K, V>,
    timestamp: u64,
}

impl<K, V> PersistStash<K, V>
where
    K: Codec + Clone + Eq + Hash + Ord + fmt::Debug,
    V: Codec + Clone + Eq + Hash + Ord + fmt::Debug,
{
    /// Opens the stash specified by the provided blob and log.
    pub fn open<B, L>(
        blob: B,
        log: L,
        metrics_registry: &MetricsRegistry,
    ) -> Result<PersistStash<K, V>, Error>
    where
        B: Blob + Send + 'static,
        L: Log + Send + 'static,
    {
        let client = runtime::start(
            RuntimeConfig::default(),
            log,
            blob,
            build_info::DUMMY_BUILD_INFO,
            metrics_registry,
            None,
        )?;
        let (write_handle, read_handle) = client.create_or_load("stash");
        write_handle.allow_compaction(Antichain::new());

        let mut stage = vec![];
        for entry in read_handle.snapshot()?.into_iter() {
            let ((key, val), _ts, diff) = entry?;
            stage.push(((key, val), diff));
        }
        differential_dataflow::consolidation::consolidate(&mut stage);

        let mut data = HashMap::new();
        for ((key, val), diff) in stage {
            if diff != 1 {
                return Err(StashError::Corruption(format!(
                    "unexpected diff {} for key {:?}",
                    diff, key
                )));
            }
            data.insert(key, val);
        }

        Ok(PersistStash {
            _client: client,
            write_handle,
            data,
            timestamp: 0,
        })
    }
}

impl<K, V> Stash<K, V> for PersistStash<K, V>
where
    K: Codec + Clone + Eq + Hash,
    V: Codec + Clone + Eq + Hash,
{
    type EngineError = persist::error::Error;

    type ReplayIterator = hash_map::IntoIter<K, V>;

    fn write_batch(&mut self, ops: Vec<StashOp<K, V>>) -> Result<(), Error> {
        let timestamp = self.timestamp;
        self.timestamp += 1;

        let mut writes = vec![];
        for op in &ops {
            match op {
                StashOp::Put(key, val) => {
                    if let Some(val) = self.data.get(&key) {
                        writes.push(((key.clone(), val.clone()), timestamp, -1));
                    }
                    writes.push(((key.clone(), val.clone()), timestamp, 1));
                }
                StashOp::Delete(key) => {
                    if let Some(val) = self.data.get(&key) {
                        writes.push(((key.clone(), val.clone()), timestamp, -1));
                    }
                }
            }
        }

        let _ = self.write_handle.write(&writes).recv()?;
        let _ = self.write_handle.seal(timestamp).recv()?;

        for op in ops {
            match op {
                StashOp::Put(key, val) => {
                    self.data.insert(key, val);
                }
                StashOp::Delete(key) => {
                    self.data.remove(&key);
                }
            }
        }

        Ok(())
    }

    fn replay(&self) -> Result<Self::ReplayIterator, Error> {
        Ok(self.data.clone().into_iter())
    }
}
