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
use crate::persister::{Meta, Write};

/// An implementation of [Write] in terms of an [crate::indexed::Indexed] via a
/// [RuntimeClient].
pub struct StreamWriteHandle {
    id: Id,
    runtime: RuntimeClient,
}

impl Write for StreamWriteHandle {
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error> {
        // TODO: Make Write::write_sync signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.write(self.id, updates, rx.into());
        tx.recv().map_err(|_| Error::RuntimeShutdown)?
    }

    fn seal(&mut self, upper: u64) -> Result<(), Error> {
        // TODO: Make Write::write_sync signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.seal(self.id, upper, rx.into());
        tx.recv().map_err(|_| Error::RuntimeShutdown)?
    }
}

impl StreamWriteHandle {
    /// Returns a new [StreamWriteHandle] for the given stream.
    pub fn new(id: Id, runtime: RuntimeClient) -> Self {
        StreamWriteHandle { id, runtime }
    }
}

/// An implementation of [Meta] in terms of an [crate::indexed::Indexed] via a
/// [RuntimeClient].
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
}

impl Meta for StreamMetaHandle {
    type Snapshot = IndexedSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, Error> {
        // TODO: Make Meta::snapshot signature non-blocking.
        let (rx, tx) = mpsc::channel();
        self.runtime.snapshot(self.id, rx.into());
        tx.recv()
            .map_err(|_| Error::RuntimeShutdown)
            .and_then(std::convert::identity)
    }

    fn allow_compaction(&mut self, _ts: u64) -> Result<(), Error> {
        // No-op for now.
        Ok(())
    }
}
