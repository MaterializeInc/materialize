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
use std::ops::Range;
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::persister::{Meta, Persister, Snapshot, Write};
use crate::storage::{Blob, Buffer, SeqNo};
use crate::{Id, Token};

/// An in-memory implementation of [Buffer].
pub struct MemBuffer {
    seqno: Range<SeqNo>,
    dataz: Vec<Vec<u8>>,
}

impl MemBuffer {
    /// Constructs a new, empty MemBuffer.
    pub fn new() -> Self {
        MemBuffer {
            seqno: SeqNo(0)..SeqNo(0),
            dataz: Vec::new(),
        }
    }
}

impl Buffer for MemBuffer {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        self.seqno = self.seqno.start..SeqNo(self.seqno.end.0 + 1);
        self.dataz.push(buf);
        debug_assert_eq!(
            (self.seqno.end.0 - self.seqno.start.0) as usize,
            self.dataz.len()
        );
        Ok(self.seqno.end)
    }

    fn snapshot<F>(&self, mut logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.dataz
            .iter()
            .enumerate()
            .map(|(idx, x)| logic(SeqNo(self.seqno.start.0 + idx as u64), &x[..]))
            .collect::<Result<(), Error>>()?;
        Ok(self.seqno.clone())
    }

    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        // TODO: Test the edge cases here.
        if upper <= self.seqno.start || upper > self.seqno.end {
            return Err(format!(
                "invalid truncation {:?} for buffer containing: {:?}",
                upper, self.seqno
            )
            .into());
        }
        let removed = upper.0 - self.seqno.start.0;
        self.seqno = upper..self.seqno.end;
        self.dataz.drain(0..removed as usize);
        debug_assert_eq!(
            (self.seqno.end.0 - self.seqno.start.0) as usize,
            self.dataz.len()
        );
        Ok(())
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
    /// Create a new MemStream.
    pub fn new() -> Self {
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

    fn seal(&mut self, _upper: u64) -> Result<(), Error> {
        // No-op for now.
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
#[derive(Debug)]
pub struct MemSnapshot {
    dataz: Vec<((String, String), u64, isize)>,
}

impl MemSnapshot {
    /// Create a new MemSnapshot.
    pub fn new(dataz: Vec<((String, String), u64, isize)>) -> Self {
        MemSnapshot { dataz }
    }
}

impl Snapshot for MemSnapshot {
    fn read<E: Extend<((String, String), u64, isize)>>(&mut self, buf: &mut E) -> bool {
        buf.extend(self.dataz.drain(..));
        false
    }
}
