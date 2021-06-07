// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory implementations for testing and benchmarking.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::persister::{Meta, Persister, Snapshot, Write};
use crate::storage::{Blob, Buffer};
use crate::{Id, Token};

/// An in-memory implementation of [Buffer].
pub struct MemBuffer {
    dataz: Vec<Vec<u8>>,
}

impl MemBuffer {
    /// Constructs a new, empty MemBuffer.
    pub fn new() -> Self {
        MemBuffer { dataz: Vec::new() }
    }
}

impl Buffer for MemBuffer {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<(), Error> {
        self.dataz.push(buf);
        Ok(())
    }

    fn snapshot<F>(&self, mut logic: F) -> Result<(), Error>
    where
        F: FnMut(&[u8]) -> Result<(), Error>,
    {
        self.dataz.iter().map(|x| logic(&x[..])).collect()
    }
}

/// An in-memory implementation of [Blob].
pub struct MemBlob {
    dataz: HashMap<String, Vec<u8>>,
}

impl MemBlob {
    /// Constructs a new, empty MemBlob.
    pub fn new() -> Self {
        MemBlob {
            dataz: HashMap::new(),
        }
    }
}

impl Blob for MemBlob {
    fn get(&self, key: &str) -> Result<Option<&Vec<u8>>, Error> {
        Ok(self.dataz.get(key))
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        if allow_overwrite {
            self.dataz.insert(key.to_owned(), value);
        } else if self.dataz.contains_key(key) {
            return Err(format!("not allowed to overwrite: {}", key).into());
        } else {
            self.dataz.insert(key.to_owned(), value);
        };
        Ok(())
    }
}

/// An in-memory implementation of [Persister].
pub struct MemPersister {
    registered: HashSet<Id>,
    dataz: HashMap<Id, MemStream>,
}

impl MemPersister {
    /// Constructs a new, empty MemPersister.
    pub fn new() -> Self {
        MemPersister {
            registered: HashSet::new(),
            dataz: HashMap::new(),
        }
    }

    /// Consumes this MemPersister, returning the underlying data.
    #[cfg(test)]
    pub fn into_inner(self) -> HashMap<Id, MemStream> {
        self.dataz
    }

    /// Constructs a MemPersister from a previous MemPersister's data but with
    /// the create_or_load registrations reset.
    #[cfg(test)]
    pub fn from_inner(inner: HashMap<Id, MemStream>) -> Self {
        MemPersister {
            registered: HashSet::new(),
            dataz: inner,
        }
    }
}

impl Persister for MemPersister {
    type Write = MemStream;
    type Meta = MemStream;

    fn create_or_load(&mut self, id: Id) -> Result<Token<Self::Write, Self::Meta>, Error> {
        if self.registered.contains(&id) {
            return Err(format!("internal error: {:?} already registered", id).into());
        }
        self.registered.insert(id);
        let p = self.dataz.entry(id).or_insert_with(MemStream::new);
        let t = Token {
            write: p.clone(),
            meta: p.clone(),
        };
        Ok(t)
    }

    fn destroy(&mut self, id: Id) -> Result<(), Error> {
        if self.dataz.remove(&id).is_none() {
            return Err(format!("internal error: {:?} not registered", id).into());
        }
        Ok(())
    }
}

/// An in-memory implementation of [Write] and [Meta].
#[derive(Clone, Debug)]
pub struct MemStream {
    dataz: Arc<Mutex<Vec<((String, String), u64, isize)>>>,
}

impl MemStream {
    fn new() -> Self {
        MemStream {
            dataz: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Write for MemStream {
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error> {
        self.dataz.lock()?.extend_from_slice(&updates);
        Ok(())
    }
}

impl Meta for MemStream {
    type Snapshot = MemSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, Error> {
        let dataz = self.dataz.lock()?.clone();
        Ok(MemSnapshot { dataz })
    }

    fn allow_compaction(&mut self, _ts: u64) -> Result<(), Error> {
        // No-op for now.
        Ok(())
    }
}

/// An in-memory implementation of [Snapshot].
pub struct MemSnapshot {
    dataz: Vec<((String, String), u64, isize)>,
}

impl Snapshot for MemSnapshot {
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        buf.extend(self.dataz.drain(..));
        false
    }
}
