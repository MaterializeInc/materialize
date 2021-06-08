// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Abstractions over files, cloud storage, etc used in persistence.

use crate::error::Error;

/// An abstraction over a `bytes key->bytes value` store.
pub trait Blob {
    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &str) -> Result<Option<&Vec<u8>>, Error>;

    /// Inserts a key-value pair into the map.
    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error>;
}

/// An abstraction over an append-only bytes log.
pub trait Buffer {
    /// Synchronously appends an entry.
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<(), Error>;

    /// Returns a consistent snapshot of all previously written entries.
    fn snapshot<F>(&self, logic: F) -> Result<(), Error>
    where
        F: FnMut(&[u8]) -> Result<(), Error>;
}

// TODO: Implement [Persister] in terms of a [Buffer] and [Blob].
