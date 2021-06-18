// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A persistent, compacting data structure of `(Key, Value, Time, Diff)`
//! updates, indexed by key.
//!
//! This is directly a persistent analog of [differential_dataflow::trace::Trace].

use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::encoding::BlobTraceMeta;
use crate::indexed::BlobTraceBatch;
use crate::persister::Snapshot;
use crate::storage::Blob;

/// A persistent, compacting data structure containing `(Key, Value, Time,
/// Diff)` entries indexed by `(key, value, time)`.
///
/// Invariants:
/// - All entries are before some time frontier.
/// - This acts as an append-only log. Data is added in advancing batches and
///   logically immutable after that (modulo compactions, which preserve it, but
///   the ability to read at old times is lost).
/// - TODO: Explain since and logical compactions.
/// - TODO: Space usage.
pub struct BlobTrace {
    since: Antichain<u64>,
    // NB: The Descriptions here are sorted and contiguous half-open intervals
    // `[lower, upper)`.
    batches: Vec<(Description<u64>, String)>,
}

impl Default for BlobTrace {
    fn default() -> Self {
        BlobTrace::new(BlobTraceMeta {
            batches: Vec::new(),
        })
    }
}

impl BlobTrace {
    /// Returns a BlobTrace re-instantiated with the previously serialized
    /// state.
    pub fn new(meta: BlobTraceMeta) -> Self {
        let mut since = Antichain::from_elem(Timestamp::minimum());
        for (desc, _) in meta.batches.iter() {
            since = since.join(desc.since());
        }
        BlobTrace {
            since: since,
            batches: meta.batches,
        }
    }

    /// Serializes the state of this BlobTrace for later re-instantiation.
    pub fn meta(&self) -> BlobTraceMeta {
        BlobTraceMeta {
            batches: self.batches.clone(),
        }
    }

    /// An upper bound on the times of contained updates.
    pub fn ts_upper(&self) -> Antichain<u64> {
        match self.batches.last() {
            Some((desc, _)) => desc.upper().clone(),
            None => Antichain::from_elem(Timestamp::minimum()),
        }
    }

    /// A lower bound on the time at which updates may have been logically
    /// compacted together.
    pub fn since(&self) -> Antichain<u64> {
        self.since.clone()
    }

    /// Writes the given batch to [Blob] storage at the given key and logically
    /// adds the contained updates to this trace.
    pub fn append<L: Blob>(
        &mut self,
        key: String,
        batch: BlobTraceBatch,
        blob: &mut BlobCache<L>,
    ) -> Result<(), Error> {
        if &self.ts_upper() != batch.desc.lower() {
            return Err(Error::from(format!(
                "batch lower doesn't match trace upper {:?}: {:?}",
                self.ts_upper(),
                batch.desc
            )));
        }
        // TODO: Sort the updates in the batch by `(key, value, time)` (or
        // ensure that they're sorted, if it turns out this work should have
        // happened somewhere else).
        let desc = batch.desc.clone();
        blob.set_trace_batch(key.clone(), batch)?;
        self.batches.push((desc, key));
        Ok(())
    }

    /// Returns a consistent read of all the updates contained in this trace.
    pub fn snapshot<L: Blob>(&self, blob: &BlobCache<L>) -> Result<TraceSnapshot, Error> {
        let ts_upper = self.ts_upper();
        let mut updates = Vec::with_capacity(self.batches.len());
        for (_, key) in self.batches.iter() {
            updates.push(blob.get_trace_batch(key)?);
        }
        Ok(TraceSnapshot { ts_upper, updates })
    }
}

/// A consistent snapshot of the data currently in a persistent [BlobTrace].
pub struct TraceSnapshot {
    /// An open upper bound on the times of contained updates.
    pub ts_upper: Antichain<u64>,
    updates: Vec<Arc<BlobTraceBatch>>,
}

impl Snapshot for TraceSnapshot {
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        if let Some(batch) = self.updates.pop() {
            buf.extend(batch.updates.iter().cloned());
            return true;
        }
        false
    }
}
