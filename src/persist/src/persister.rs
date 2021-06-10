// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An abstraction for multiplexed streams of persisted data.

use crate::error::Error;
use crate::{Id, Token};

/// An abstraction for a writer of (Key, Value, Time, Diff) updates.
pub trait Write {
    /// Synchronously writes (Key, Value, Time, Diff) updates.
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error>;
}

/// An isolated, consistent read of previously written (Key, Value, Time, Diff)
/// updates.
pub trait Snapshot {
    /// A partial read of the data in the snapshot.
    ///
    /// Returns true if read needs to be called again for more data.
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool;

    /// A full read of the data in the snapshot.
    #[cfg(test)]
    fn read_to_end(&mut self) -> Vec<((String, String), u64, isize)> {
        let mut buf = Vec::new();
        while self.read(&mut buf) {}
        buf
    }
}

/// A handle for a persisted stream.
pub trait Meta: Clone {
    /// The type of snapshots returned by [Meta::snapshot].
    type Snapshot: Snapshot;

    /// Returns a consistent snapshot of all previously persisted stream data.
    fn snapshot(&self) -> Result<Self::Snapshot, Error>;

    /// Unblocks compaction for updates before a time.
    fn allow_compaction(&mut self, ts: u64) -> Result<(), Error>;
}

/// An implementation of a persister for multiplexed streams of (Key, Value,
/// Time, Diff) updates.
pub trait Persister: Sized {
    /// The persister's associated [Write] implementation.
    type Write: Write;

    /// The persister's associated [Meta] implementation.
    type Meta: Meta;

    /// Returns a token used to construct a persisted stream operator.
    ///
    /// If data was written by a previous Persister for this id, it's loaded and
    /// replayed into the stream once constructed.
    ///
    /// Within a process, this can only be called once per id, even if that id
    /// has since been destroyed. An `Err` is returned for calls after the
    /// first. TODO: Is this restriction necessary/helpful?
    fn create_or_load(&mut self, id: Id) -> Result<Token<Self::Write, Self::Meta>, Error>;

    ///
    /// TODO: Should this live on Meta?
    fn destroy(&mut self, id: Id) -> Result<(), Error>;
}
