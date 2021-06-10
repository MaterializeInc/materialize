// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence for Materialize dataflows.

#![warn(missing_docs)]

pub mod error;
pub mod file;
pub mod mem;
pub mod operators;
pub mod persister;
pub mod storage;

use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::persister::Persister;

// TODO
// - Finish implementing StoragePersister.
// - Should we hard-code the Key, Val, Time, Diff types everywhere or introduce
//   them as type parameters? Materialize will only be using one combination of
//   them (two with `()` vals?) but the generality might make things easier to
//   read. Of course, it also might make things harder to read.
// - Is PersistManager getting us anything over a type alias? At the moment, the
//   only thing it does is wrap mutex poison errors in this crate's error type.
// - It's intended that Persister will multiplex and batch multiple streams, but
//   the APIs probably aren't quite right for this yet.
// - Support progress messages.
// - This method of getting the metadata handle ends up being pretty clunky in
//   practice. Maybe instead the user should pass in a mutable reference to a
//   `Meta` they've constructed like `probe_with`?
// - The async story. I haven't grokked how async plays with timely yet, so
//   dunno if/where that fits in yet.
// - Error handling. Right now, there are a bunch of `expect`s and this likely
//   needs to hook into the error streams that Materialize hands around.
// - Blobs (think S3) should likely be cached in some sort of LRU. Unclear where
//   this lives. I originally was thinking inside the S3Blob impl, but maybe we
//   want it to be after decode? Dunno.
// - What's our story with poisoned mutexes?
// - Backward compatibility of persisted data, particularly the encoded keys and
//   values.
// - Restarting with a different number of workers.
// - Meta TODO: These were my immediate thoughts but there's stuff I'm
//   forgetting. Flesh this list out.

/// A unique id for a persisted stream.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Id(pub u64);

/// A thread-safe, clone-able wrapper for [Persister].
pub struct PersistManager<P> {
    persister: Arc<Mutex<P>>,
}

// The derived Send, Sync, and Clone don't work because of the type parameter.
unsafe impl<P> Send for PersistManager<P> {}
unsafe impl<P> Sync for PersistManager<P> {}
impl<P> Clone for PersistManager<P> {
    fn clone(&self) -> Self {
        PersistManager {
            persister: self.persister.clone(),
        }
    }
}

impl<P: Persister> PersistManager<P> {
    /// Returns a [PersistManager] wrapper for the given [Persister].
    pub fn new(persister: P) -> Self {
        PersistManager {
            persister: Arc::new(Mutex::new(persister)),
        }
    }

    /// A wrapper for [Persister::create_or_load].
    pub fn create_or_load(&mut self, id: Id) -> Result<Token<P::Write, P::Meta>, Error> {
        self.persister.lock()?.create_or_load(id)
    }

    /// A wrapper for [Persister::destroy].
    pub fn destroy(&mut self, id: Id) -> Result<(), Error> {
        self.persister.lock()?.destroy(id)
    }
}

/// An exclusivity token needed to construct persistence [operators].
///
/// Intentionally not Clone since it's an exclusivity token.
pub struct Token<W, M> {
    write: W,
    meta: M,
}

impl<W, M> Token<W, M> {
    fn into_inner(self) -> (W, M) {
        (self.write, self.meta)
    }
}
