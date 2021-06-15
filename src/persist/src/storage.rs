// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions over files, cloud storage, etc used in persistence.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use abomonation_derive::Abomonation;

use crate::error::Error;
use crate::indexed::handle::{StreamMetaHandle, StreamWriteHandle};
use crate::indexed::Indexed;
use crate::persister::Persister;
use crate::{Id, Token};

/// A "sequence number", uniquely associated with an entry in a Buffer.
#[derive(Clone, Copy, Debug, PartialOrd, Ord, PartialEq, Eq, Abomonation)]
pub struct SeqNo(pub u64);

impl timely::PartialOrder for SeqNo {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

/// An abstraction over an append-only bytes log.
///
/// Each written entry is assigned a unique, incrementing SeqNo, which can be
/// later used when draining data back out of the buffer.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
pub trait Buffer {
    /// Synchronously appends an entry.
    ///
    /// TODO: Figure out our async story so we can batch up multiple of these
    /// into one disk flush.
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error>;

    /// Returns a consistent snapshot of all written but not yet truncated
    /// entries.
    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>;

    /// Removes all entries with a SeqNo strictly less than the given upper
    /// bound.
    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error>;
}

/// An abstraction over a `bytes key`->`bytes value` store.
///
/// - Invariant: Implementations are responsible for ensuring that they are
///   exclusive writers to this location.
pub trait Blob {
    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &str) -> Result<Option<&Vec<u8>>, Error>;

    /// Inserts a key-value pair into the map.
    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error>;
}

/// An implementation of [Persister] in terms of a [Buffer] and [Blob].
pub struct StoragePersister<U: Buffer, L: Blob> {
    registered: HashSet<Id>,
    indexed: Arc<Mutex<Indexed<U, L>>>,
}

impl<U: Buffer, L: Blob> StoragePersister<U, L> {
    /// Returns a [StoragePersister] initialized with previous data, if any.
    pub fn new(buf: U, blob: L) -> Result<Self, Error> {
        Ok(StoragePersister {
            registered: HashSet::new(),
            indexed: Arc::new(Mutex::new(Indexed::new(buf, blob)?)),
        })
    }
}

impl<U: Buffer, L: Blob> Persister for StoragePersister<U, L> {
    type Write = StreamWriteHandle<U, L>;

    type Meta = StreamMetaHandle<U, L>;

    fn create_or_load(&mut self, id: Id) -> Result<Token<Self::Write, Self::Meta>, Error> {
        if self.registered.contains(&id) {
            return Err(format!("internal error: {:?} already registered", id).into());
        }
        self.registered.insert(id);
        let write = StreamWriteHandle::new(id, self.indexed.clone());
        let meta = StreamMetaHandle::new(id, self.indexed.clone());
        Ok(Token { write, meta })
    }

    fn destroy(&mut self, _id: Id) -> Result<(), Error> {
        unimplemented!()
    }
}
