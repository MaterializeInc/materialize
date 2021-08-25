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
use ore::metrics::MetricsRegistry;

use crate::error::Error;
use crate::indexed::runtime::{self, RuntimeClient};
use crate::storage::{Blob, LockInfo, Log, SeqNo};
use crate::unreliable::{UnreliableBlob, UnreliableHandle, UnreliableLog};

struct MemLogCore {
    seqno: Range<SeqNo>,
    dataz: Vec<Vec<u8>>,
    lock: Option<LockInfo>,
}

impl MemLogCore {
    fn new(lock_info: LockInfo) -> Self {
        MemLogCore {
            seqno: SeqNo(0)..SeqNo(0),
            dataz: Vec::new(),
            lock: Some(lock_info),
        }
    }

    fn open(&mut self, new_lock: LockInfo) -> Result<(), Error> {
        if let Some(existing) = &self.lock {
            let _ = new_lock.check_reentrant_for(&"MemLog", existing.to_string().as_bytes())?;
        }

        self.lock = Some(new_lock);

        Ok(())
    }

    fn close(&mut self) -> Result<bool, Error> {
        Ok(self.lock.take().is_some())
    }

    fn ensure_open(&self) -> Result<(), Error> {
        if self.lock.is_none() {
            return Err("log unexpectedly closed".into());
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
        if upper <= self.seqno.start || upper > self.seqno.end {
            return Err(format!(
                "invalid truncation {:?} for log containing: {:?}",
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

/// An in-memory implementation of [Log].
pub struct MemLog {
    core: Arc<Mutex<MemLogCore>>,
}

impl MemLog {
    /// Constructs a new, empty MemLog.
    pub fn new(lock_info: LockInfo) -> Self {
        MemLog {
            core: Arc::new(Mutex::new(MemLogCore::new(lock_info))),
        }
    }

    /// Constructs a new, empty MemLog with a unique reentrance id.
    ///
    /// Helper for tests that don't care about locking reentrance (which is most
    /// of them).
    #[cfg(test)]
    pub fn new_no_reentrance(lock_info_details: &str) -> Self {
        Self::new(LockInfo::new_no_reentrance(lock_info_details.to_owned()))
    }

    /// Open a pre-existing MemLog.
    fn open(core: Arc<Mutex<MemLogCore>>, lock_info: LockInfo) -> Result<Self, Error> {
        core.lock()?.open(lock_info)?;
        Ok(Self { core })
    }
}

impl Drop for MemLog {
    fn drop(&mut self) {
        let did_work = self.close().expect("closing MemLog cannot fail");
        // MemLog should have been closed gracefully; this drop is only here
        // as a failsafe. If it actually did anything, that's surprising.
        if did_work {
            log::warn!("MemLog dropped without close");
        }
    }
}

impl Log for MemLog {
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

    fn close(&mut self) -> Result<bool, Error> {
        self.core.lock()?.close()
    }
}

struct MemBlobCore {
    dataz: HashMap<String, Vec<u8>>,
    lock: Option<LockInfo>,
}

impl MemBlobCore {
    fn new(lock_info: LockInfo) -> Self {
        MemBlobCore {
            dataz: HashMap::new(),
            lock: Some(lock_info),
        }
    }

    fn open(&mut self, new_lock: LockInfo) -> Result<(), Error> {
        if let Some(existing) = &self.lock {
            let _ = new_lock.check_reentrant_for(&"MemBlob", existing.to_string().as_bytes())?;
        }

        self.lock = Some(new_lock);

        Ok(())
    }

    fn close(&mut self) -> Result<bool, Error> {
        Ok(self.lock.take().is_some())
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

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.ensure_open()?;
        match self.dataz.remove(key) {
            Some(_) => Ok(()),
            None => Err(format!("key does not exist: {}", key).into()),
        }
    }
}

/// An in-memory implementation of [Blob].
pub struct MemBlob {
    core: Arc<Mutex<MemBlobCore>>,
}

impl MemBlob {
    /// Constructs a new, empty MemBlob.
    pub fn new(lock_info: LockInfo) -> Self {
        MemBlob {
            core: Arc::new(Mutex::new(MemBlobCore::new(lock_info))),
        }
    }

    /// Constructs a new, empty MemBlob with a unique reentrance id.
    ///
    /// Helper for tests that don't care about locking reentrance (which is most
    /// of them).
    #[cfg(test)]
    pub fn new_no_reentrance(lock_info_details: &str) -> Self {
        Self::new(LockInfo::new_no_reentrance(lock_info_details.to_owned()))
    }

    /// Open a pre-existing MemBlob.
    fn open(core: Arc<Mutex<MemBlobCore>>, lock_info: LockInfo) -> Result<Self, Error> {
        core.lock()?.open(lock_info)?;
        Ok(Self { core })
    }
}

impl Drop for MemBlob {
    fn drop(&mut self) {
        let did_work = self.close().expect("closing MemBlob cannot fail");
        // MemLog should have been closed gracefully; this drop is only here
        // as a failsafe. If it actually did anything, that's surprising.
        if did_work {
            log::warn!("MemBlob dropped without close");
        }
    }
}

impl Blob for MemBlob {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core.lock()?.get(key)
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        self.core.lock()?.set(key, value, allow_overwrite)
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.core.lock()?.delete(key)
    }

    fn close(&mut self) -> Result<bool, Error> {
        self.core.lock()?.close()
    }
}

/// An in-memory representation of a set of [Log]s and [Blob]s that can be reused
/// across dataflows
pub struct MemRegistry {
    log_by_path: HashMap<String, Arc<Mutex<MemLogCore>>>,
    blob_by_path: HashMap<String, Arc<Mutex<MemBlobCore>>>,
}

impl MemRegistry {
    /// Constructs a new, empty MemRegistry
    pub fn new() -> Self {
        MemRegistry {
            log_by_path: HashMap::new(),
            blob_by_path: HashMap::new(),
        }
    }

    fn log(&mut self, path: &str, lock_info: LockInfo) -> Result<MemLog, Error> {
        if let Some(log) = self.log_by_path.get(path) {
            MemLog::open(log.clone(), lock_info)
        } else {
            let log = MemLog::new(lock_info);
            self.log_by_path.insert(path.to_string(), log.core.clone());
            Ok(log)
        }
    }

    fn blob(&mut self, path: &str, lock_info: LockInfo) -> Result<MemBlob, Error> {
        if let Some(blob) = self.blob_by_path.get(path) {
            MemBlob::open(blob.clone(), lock_info)
        } else {
            let blob = MemBlob::new(lock_info);
            self.blob_by_path
                .insert(path.to_string(), blob.core.clone());
            Ok(blob)
        }
    }

    /// Open a [RuntimeClient] associated with `path`.
    pub fn open(&mut self, path: &str, lock_info: &str) -> Result<RuntimeClient, Error> {
        let lock_info = LockInfo::new_no_reentrance(lock_info.to_owned());
        let log = self.log(path, lock_info.clone())?;
        let blob = self.blob(path, lock_info)?;
        runtime::start(log, blob, &MetricsRegistry::new())
    }

    /// Open a [RuntimeClient] with unreliable storage associated with `path`.
    pub fn open_unreliable(
        &mut self,
        path: &str,
        lock_info: &str,
        unreliable: UnreliableHandle,
    ) -> Result<RuntimeClient, Error> {
        let lock_info = LockInfo::new_no_reentrance(lock_info.to_owned());
        let log = self.log(path, lock_info.clone())?;
        let log = UnreliableLog::from_handle(log, unreliable.clone());
        let blob = self.blob(path, lock_info)?;
        let blob = UnreliableBlob::from_handle(blob, unreliable);
        runtime::start(log, blob, &MetricsRegistry::new())
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::tests::{blob_impl_test, log_impl_test};

    use super::*;

    #[test]
    fn mem_log() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        log_impl_test(move |t| registry.log(t.path, (t.reentrance_id, "log_impl_test").into()))
    }

    #[test]
    fn mem_blob() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        blob_impl_test(move |t| registry.blob(t.path, (t.reentrance_id, "blob_impl_test").into()))
    }
}
