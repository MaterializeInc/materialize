// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The public API used by the rest of the crate for interacting with an
//! instance of [Indexed].

use std::sync::Arc;
use std::sync::Mutex;

use crate::error::Error;
use crate::indexed::{Indexed, IndexedSnapshot};
use crate::persister::{Meta, Write};
use crate::storage::{Blob, Buffer};
use crate::Id;

/// An implementation of [Write] in terms of an [Indexed].
pub struct StreamWriteHandle<U: Buffer, L: Blob> {
    id: Id,
    indexed: Arc<Mutex<Indexed<U, L>>>,
}

impl<U: Buffer, L: Blob> Write for StreamWriteHandle<U, L> {
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error> {
        self.indexed.lock()?.write_sync(self.id, updates)
    }

    fn seal(&mut self, upper: u64) -> Result<(), Error> {
        self.indexed.lock()?.seal(self.id, upper)
    }
}

impl<U: Buffer, L: Blob> StreamWriteHandle<U, L> {
    /// Returns a new [StreamWriteHandle] for the given stream.
    pub fn new(id: Id, indexed: Arc<Mutex<Indexed<U, L>>>) -> Self {
        StreamWriteHandle { id, indexed }
    }
}

/// An implementation of [Meta] in terms of an [Indexed].
pub struct StreamMetaHandle<U: Buffer, L: Blob> {
    id: Id,
    indexed: Arc<Mutex<Indexed<U, L>>>,
}

impl<U: Buffer, L: Blob> Clone for StreamMetaHandle<U, L> {
    fn clone(&self) -> Self {
        StreamMetaHandle {
            id: self.id,
            indexed: self.indexed.clone(),
        }
    }
}

impl<U: Buffer, L: Blob> StreamMetaHandle<U, L> {
    /// Returns a new [StreamMetaHandle] for the given stream.
    pub fn new(id: Id, indexed: Arc<Mutex<Indexed<U, L>>>) -> Self {
        StreamMetaHandle { id, indexed }
    }
}

impl<U: Buffer, L: Blob> Meta for StreamMetaHandle<U, L> {
    type Snapshot = IndexedSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, Error> {
        self.indexed.lock()?.snapshot(self.id)
    }

    fn allow_compaction(&mut self, _ts: u64) -> Result<(), Error> {
        // No-op for now.
        Ok(())
    }
}
