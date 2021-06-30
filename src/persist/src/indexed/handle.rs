// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The public API used by the rest of the crate for interacting with an
//! instance of [crate::indexed::Indexed] via [RuntimeClient].

use std::sync::mpsc;

use crate::error::Error;
use crate::indexed::encoding::Id;
use crate::indexed::runtime::RuntimeClient;
use crate::indexed::IndexedSnapshot;

/// A handle that allows writes of ((Key, Value), Time, Diff) updates into an
/// [crate::indexed::Indexed] via a [RuntimeClient].
pub struct StreamWriteHandle {
    id: Id,
    runtime: RuntimeClient,
}

impl StreamWriteHandle {
    /// Returns a new [StreamWriteHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient) -> Self {
        StreamWriteHandle { id, runtime }
    }

    /// Synchronously writes (Key, Value, Time, Diff) updates.
    pub fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error> {
        // TODO: Make Write::write_sync signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.write(self.id, updates, rx.into());
        tx.recv().map_err(|_| Error::RuntimeShutdown)?
    }

    /// Closes the stream at the given timestamp, migrating data strictly less
    /// than it into the trace.
    pub fn seal(&mut self, upper: u64) -> Result<(), Error> {
        // TODO: Make Write::write_sync signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.seal(self.id, upper, rx.into());
        tx.recv().map_err(|_| Error::RuntimeShutdown)?
    }
}

/// A handle for a persisted stream of ((Key, Value), Time, Diff) updates backed
/// by an [crate::indexed::Indexed] via a [RuntimeClient].
pub struct StreamMetaHandle {
    id: Id,
    runtime: RuntimeClient,
}

impl Clone for StreamMetaHandle {
    fn clone(&self) -> Self {
        StreamMetaHandle {
            id: self.id,
            runtime: self.runtime.clone(),
        }
    }
}

impl StreamMetaHandle {
    /// Returns a new [StreamMetaHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient) -> Self {
        StreamMetaHandle { id, runtime }
    }

    /// Returns a consistent snapshot of all previously persisted stream data.
    pub fn snapshot(&self) -> Result<IndexedSnapshot, Error> {
        // TODO: Make Meta::snapshot signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.snapshot(self.id, rx.into());
        tx.recv()
            .map_err(|_| Error::RuntimeShutdown)
            .and_then(std::convert::identity)
    }

    /// Unblocks compaction for updates before a time.
    pub fn allow_compaction(&mut self, _ts: u64) -> Result<(), Error> {
        // No-op for now.
        Ok(())
    }
}
