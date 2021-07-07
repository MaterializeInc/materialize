// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory implementations for testing and benchmarking.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use ore::cast::CastFrom;

use crate::error::Error;
use crate::indexed::runtime::{self, RuntimeClient};
use crate::storage::{Blob, Buffer, SeqNo};
use crate::Data;

struct MemBufferCore {
    seqno: Range<SeqNo>,
    dataz: Vec<Vec<u8>>,
    lock: Option<String>,
}

impl MemBufferCore {
    fn new() -> Self {
        MemBufferCore {
            seqno: SeqNo(0)..SeqNo(0),
            dataz: Vec::new(),
            lock: None,
        }
    }

    fn open(&mut self, lock_info: &str) -> Result<(), Error> {
        if let Some(lock) = &self.lock {
            return Err(format!("buffer is already open: {}", lock).into());
        }

        self.lock = Some(lock_info.to_string());

        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        self.lock = None;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.lock.is_none() {
            return Err("buffer unexpectedly closed".into());
        }

        Ok(())
    }

    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        self.ensure_open()?;
        let write_seqno = self.seqno.end;
        self.seqno = self.seqno.start..SeqNo(self.seqno.end.0 + 1);
        self.dataz.push(buf);
        debug_assert_eq!(
            usize::cast_from(self.seqno.end.0 - self.seqno.start.0),
            self.dataz.len()
        );
        Ok(write_seqno)
    }

    fn snapshot<F>(&self, mut logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.ensure_open()?;
        self.dataz
            .iter()
            .enumerate()
            .map(|(idx, x)| logic(SeqNo(self.seqno.start.0 + u64::cast_from(idx)), &x[..]))
            .collect::<Result<(), Error>>()?;
        Ok(self.seqno.clone())
    }

    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.ensure_open()?;
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
        self.dataz.drain(0..usize::cast_from(removed));
        debug_assert_eq!(
            usize::cast_from(self.seqno.end.0 - self.seqno.start.0),
            self.dataz.len()
        );
        Ok(())
    }
}

/// An in-memory implementation of [Buffer].
pub struct MemBuffer {
    core: Arc<Mutex<MemBufferCore>>,
}

impl MemBuffer {
    /// Constructs a new, empty MemBuffer.
    pub fn new(lock_info: &str) -> Result<Self, Error> {
        let mut core = MemBufferCore::new();
        core.open(lock_info)?;
        Ok(Self {
            core: Arc::new(Mutex::new(core)),
        })
    }

    /// Open a pre-existing MemBuffer.
    fn open(core: Arc<Mutex<MemBufferCore>>, lock_info: &str) -> Result<Self, Error> {
        core.lock()?.open(lock_info)?;
        Ok(Self { core })
    }
}

impl Drop for MemBuffer {
    fn drop(&mut self) {
        self.close().expect("closing MemBuffer cannot fail");
    }
}

impl Buffer for MemBuffer {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        self.core.lock()?.write_sync(buf)
    }

    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.core.lock()?.snapshot(logic)
    }

    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.core.lock()?.truncate(upper)
    }

    fn close(&mut self) -> Result<(), Error> {
        self.core.lock()?.close()
    }
}

struct MemBlobCore {
    dataz: HashMap<String, Vec<u8>>,
    lock: Option<String>,
}

impl MemBlobCore {
    fn new() -> Self {
        MemBlobCore {
            dataz: HashMap::new(),
            lock: None,
        }
    }

    fn open(&mut self, lock_info: &str) -> Result<(), Error> {
        if let Some(lock) = &self.lock {
            return Err(format!("blob is already open: {}", lock).into());
        }

        self.lock = Some(lock_info.to_string());

        Ok(())
    }

    fn close(&mut self) -> Result<(), Error> {
        self.lock = None;
        Ok(())
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.lock.is_none() {
            return Err("blob unexpectedly closed".into());
        }

        Ok(())
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.ensure_open()?;
        Ok(self.dataz.get(key).cloned())
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        self.ensure_open()?;
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

/// An in-memory implementation of [Blob].
pub struct MemBlob {
    core: Arc<Mutex<MemBlobCore>>,
}

impl MemBlob {
    /// Constructs a new, empty MemBlob.
    pub fn new(lock_info: &str) -> Result<Self, Error> {
        let mut core = MemBlobCore::new();
        core.open(lock_info)?;
        Ok(MemBlob {
            core: Arc::new(Mutex::new(core)),
        })
    }

    /// Open a pre-existing MemBlob.
    fn open(core: Arc<Mutex<MemBlobCore>>, lock_info: &str) -> Result<Self, Error> {
        core.lock()?.open(lock_info)?;
        Ok(Self { core })
    }
}

impl Drop for MemBlob {
    fn drop(&mut self) {
        self.close().expect("closing MemBlob cannot fail");
    }
}

impl Blob for MemBlob {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core.lock()?.get(key)
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        self.core.lock()?.set(key, value, allow_overwrite)
    }

    fn close(&mut self) -> Result<(), Error> {
        self.core.lock()?.close()
    }
}

/// An in-memory representation of a set of [Buffer]s and [Blob]s that can be reused
/// across dataflows
pub struct MemRegistry {
    by_path: HashMap<String, (Arc<Mutex<MemBufferCore>>, Arc<Mutex<MemBlobCore>>)>,
}

impl MemRegistry {
    /// Constructs a new, empty MemRegistry
    pub fn new() -> Self {
        MemRegistry {
            by_path: HashMap::new(),
        }
    }

    fn buffer(&mut self, path: &str, lock_info: &str) -> Result<MemBuffer, Error> {
        let buffer = if let Some((buffer, _)) = self.by_path.get(path) {
            buffer.clone()
        } else {
            let buffer = Arc::new(Mutex::new(MemBufferCore::new()));
            let blob = Arc::new(Mutex::new(MemBlobCore::new()));
            self.by_path
                .insert(path.to_string(), (buffer.clone(), blob));
            buffer
        };
        MemBuffer::open(buffer, lock_info)
    }

    fn blob(&mut self, path: &str, lock_info: &str) -> Result<MemBlob, Error> {
        let blob = if let Some((_, blob)) = self.by_path.get(path) {
            blob.clone()
        } else {
            let buffer = Arc::new(Mutex::new(MemBufferCore::new()));
            let blob = Arc::new(Mutex::new(MemBlobCore::new()));
            self.by_path
                .insert(path.to_string(), (buffer, blob.clone()));
            blob
        };
        MemBlob::open(blob, lock_info)
    }

    /// Open a [RuntimeClient] associated with `path`.
    pub fn open<K, V>(&mut self, path: &str, lock_info: &str) -> Result<RuntimeClient<K, V>, Error>
    where
        K: Data + Send + Sync + 'static,
        V: Data + Send + Sync + 'static,
    {
        let buffer = self.buffer(path, lock_info)?;
        let blob = self.blob(path, lock_info)?;
        runtime::start(buffer, blob)
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::tests::{blob_impl_test, buffer_impl_test};

    use super::*;

    #[test]
    fn mem_buffer() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        buffer_impl_test(move |path| registry.buffer(path, "buffer_impl_test"))
    }

    #[test]
    fn mem_blob() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        blob_impl_test(move |path| registry.blob(path, "blob_impl_test"))
    }
}
