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
use std::sync::{Arc, Mutex, MutexGuard};

use ore::cast::CastFrom;
use ore::metrics::MetricsRegistry;
use tokio::runtime::Runtime;

use crate::error::Error;
use crate::indexed::background::Maintainer;
use crate::indexed::cache::BlobCache;
use crate::indexed::metrics::Metrics;
use crate::indexed::runtime::{self, RuntimeClient, RuntimeConfig};
use crate::indexed::Indexed;
use crate::storage::{Blob, LockInfo, Log, SeqNo};
use crate::unreliable::{UnreliableBlob, UnreliableHandle, UnreliableLog};

#[derive(Debug)]
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
#[derive(Debug)]
pub struct MemLog {
    core: Option<Arc<Mutex<MemLogCore>>>,
}

impl MemLog {
    /// Constructs a new, empty MemLog.
    pub fn new(lock_info: LockInfo) -> Self {
        MemLog {
            core: Some(Arc::new(Mutex::new(MemLogCore::new(lock_info)))),
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
        Ok(Self { core: Some(core) })
    }

    fn core_lock<'c>(&'c self) -> Result<MutexGuard<'c, MemLogCore>, Error> {
        match self.core.as_ref() {
            None => return Err("MemLog has been closed".into()),
            Some(core) => Ok(core.lock()?),
        }
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
        self.core_lock()?.write_sync(buf)
    }

    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.core_lock()?.snapshot(logic)
    }

    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.core_lock()?.truncate(upper)
    }

    fn close(&mut self) -> Result<bool, Error> {
        match self.core.take() {
            None => Ok(false), // Someone already called close.
            Some(core) => core.lock()?.close(),
        }
    }
}

#[derive(Debug)]
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

    fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.ensure_open()?;
        Ok(self.dataz.keys().cloned().collect())
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.ensure_open()?;
        self.dataz.remove(key);
        Ok(())
    }
}

/// An in-memory implementation of [Blob].
#[derive(Debug)]
pub struct MemBlob {
    core: Option<Arc<Mutex<MemBlobCore>>>,
}

impl MemBlob {
    /// Constructs a new, empty MemBlob.
    pub fn new(lock_info: LockInfo) -> Self {
        MemBlob {
            core: Some(Arc::new(Mutex::new(MemBlobCore::new(lock_info)))),
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
        Ok(Self { core: Some(core) })
    }

    fn core_lock<'c>(&'c self) -> Result<MutexGuard<'c, MemBlobCore>, Error> {
        match self.core.as_ref() {
            None => return Err("MemBlob has been closed".into()),
            Some(core) => Ok(core.lock()?),
        }
    }

    #[cfg(test)]
    pub fn all_blobs(&self) -> Result<Vec<(String, Vec<u8>)>, Error> {
        let core = self.core_lock()?;
        Ok(core
            .dataz
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
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
        self.core_lock()?.get(key)
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        self.core_lock()?.set(key, value, allow_overwrite)
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.core_lock()?.delete(key)
    }

    fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core.lock()?.list_keys()
    }

    fn close(&mut self) -> Result<bool, Error> {
        match self.core.take() {
            None => Ok(false), // Someone already called close.
            Some(core) => core.lock()?.close(),
        }
    }
}

/// An in-memory representation of a [Log] and [Blob] that can be reused
/// across dataflows
#[derive(Clone, Debug)]
pub struct MemRegistry {
    log: Arc<Mutex<MemLogCore>>,
    blob: Arc<Mutex<MemBlobCore>>,
}

impl MemRegistry {
    /// Constructs a new, empty [MemRegistry]
    pub fn new() -> Self {
        let mut log = MemLogCore::new(LockInfo::new_no_reentrance("".into()));
        log.close()
            .expect("newly opened MemLogCore close is infallible");
        let mut blob = MemBlobCore::new(LockInfo::new_no_reentrance("".into()));
        blob.close()
            .expect("newly opened MemBlobCore close is infallible");
        MemRegistry {
            log: Arc::new(Mutex::new(log)),
            blob: Arc::new(Mutex::new(blob)),
        }
    }

    /// Opens the [MemLog] contained by this registry.
    pub fn log_no_reentrance(&self) -> Result<MemLog, Error> {
        MemLog::open(
            self.log.clone(),
            LockInfo::new_no_reentrance("MemRegistry".to_owned()),
        )
    }

    /// Opens the [MemBlob] contained by this registry.
    pub fn blob_no_reentrance(&self) -> Result<MemBlob, Error> {
        MemBlob::open(
            self.blob.clone(),
            LockInfo::new_no_reentrance("MemRegistry".to_owned()),
        )
    }

    /// Returns a [RuntimeClient] using the [MemLog] and [MemBlob] contained by
    /// this registry.
    pub fn indexed_no_reentrance(&mut self) -> Result<Indexed<MemLog, MemBlob>, Error> {
        let log = self.log_no_reentrance()?;
        let metrics = Metrics::register_with(&MetricsRegistry::new());
        let blob = BlobCache::new(metrics.clone(), self.blob_no_reentrance()?);
        let compacter = Maintainer::new(blob.clone(), Arc::new(Runtime::new()?));
        Indexed::new(log, blob, compacter, metrics)
    }

    /// Starts a [RuntimeClient] using the [MemLog] and [MemBlob] contained by
    /// this registry.
    pub fn runtime_no_reentrance(&mut self) -> Result<RuntimeClient, Error> {
        let log = self.log_no_reentrance()?;
        let blob = self.blob_no_reentrance()?;
        runtime::start(
            RuntimeConfig::for_tests(),
            log,
            blob,
            &MetricsRegistry::new(),
            None,
        )
    }

    /// Open a [RuntimeClient] with unreliable storage backed by the given
    /// [`UnreliableHandle`].
    pub fn runtime_unreliable(
        &mut self,
        unreliable: UnreliableHandle,
    ) -> Result<RuntimeClient, Error> {
        let log = self.log_no_reentrance()?;
        let log = UnreliableLog::from_handle(log, unreliable.clone());
        let blob = self.blob_no_reentrance()?;
        let blob = UnreliableBlob::from_handle(blob, unreliable);
        runtime::start(
            RuntimeConfig::for_tests(),
            log,
            blob,
            &MetricsRegistry::new(),
            None,
        )
    }
}

/// An in-memory representation of a set of [Log]s and [Blob]s that can be reused
/// across dataflows
#[cfg(test)]
#[derive(Debug)]
pub struct MemMultiRegistry {
    log_by_path: HashMap<String, Arc<Mutex<MemLogCore>>>,
    blob_by_path: HashMap<String, Arc<Mutex<MemBlobCore>>>,
}

#[cfg(test)]
impl MemMultiRegistry {
    /// Constructs a new, empty [MemMultiRegistry].
    pub fn new() -> Self {
        MemMultiRegistry {
            log_by_path: HashMap::new(),
            blob_by_path: HashMap::new(),
        }
    }

    /// Opens the [MemLog] associated with `path`.
    pub fn log(&mut self, path: &str, lock_info: LockInfo) -> Result<MemLog, Error> {
        if let Some(log) = self.log_by_path.get(path) {
            MemLog::open(log.clone(), lock_info)
        } else {
            let log = Arc::new(Mutex::new(MemLogCore::new(lock_info)));
            self.log_by_path.insert(path.to_string(), log.clone());
            let log = MemLog { core: Some(log) };
            Ok(log)
        }
    }

    /// Opens the [MemBlob] associated with `path`.
    pub fn blob(&mut self, path: &str, lock_info: LockInfo) -> Result<MemBlob, Error> {
        if let Some(blob) = self.blob_by_path.get(path) {
            MemBlob::open(blob.clone(), lock_info)
        } else {
            let blob = Arc::new(Mutex::new(MemBlobCore::new(lock_info)));
            self.blob_by_path.insert(path.to_string(), blob.clone());
            let blob = MemBlob { core: Some(blob) };
            Ok(blob)
        }
    }

    /// Open a [RuntimeClient] associated with `path`.
    pub fn open(&mut self, path: &str, lock_info: &str) -> Result<RuntimeClient, Error> {
        let lock_info = LockInfo::new_no_reentrance(lock_info.to_owned());
        let log = self.log(path, lock_info.clone())?;
        let blob = self.blob(path, lock_info)?;
        runtime::start(
            RuntimeConfig::for_tests(),
            log,
            blob,
            &MetricsRegistry::new(),
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::tests::{blob_impl_test, log_impl_test};

    use super::*;

    #[test]
    fn mem_log() -> Result<(), Error> {
        let mut registry = MemMultiRegistry::new();
        log_impl_test(move |t| registry.log(t.path, (t.reentrance_id, "log_impl_test").into()))
    }

    #[test]
    fn mem_blob() -> Result<(), Error> {
        let mut registry = MemMultiRegistry::new();
        blob_impl_test(move |t| registry.blob(t.path, (t.reentrance_id, "blob_impl_test").into()))
    }

    // This test covers a regression that was affecting the nemesis tests where
    // async fetches happening in background threads could race with a close and
    // re-open of MemBlob and then incorrectly still affect the newly open
    // MemBlob though the handler for the previous (now-closed) MemBlob.
    //
    // This is really only a problem for tests, but it's a common pattern in
    // tests to model restarts, so it's worth getting right.
    #[test]
    fn regression_delayed_close() -> Result<(), Error> {
        let registry = MemRegistry::new();

        // Put a blob in an Arc<Mutex<..>> and copy it (like we do to in
        // BlobCache to share it between the main persist loop and maintenance).
        let blob_gen1_1 = Arc::new(Mutex::new(registry.blob_no_reentrance()?));
        let blob_gen1_2 = blob_gen1_1.clone();

        // Close one of them because the runtime is shutting down, but keep the
        // other around (to simulate an async fetch in maintenance).
        assert_eq!(blob_gen1_1.lock()?.close()?, true);
        drop(blob_gen1_1);

        // Now "restart" everything and reuse this blob like nemesis does.
        let blob_gen2 = Arc::new(Mutex::new(registry.blob_no_reentrance()?));

        // Write some data with the new handle.
        blob_gen2.lock()?.set("a", "1".into(), true)?;

        // The old handle should not be usable anymore. Writes and reads using
        // it should fail and the value set by blob_gen2 should not be affected.
        assert_eq!(
            blob_gen1_2.lock()?.get("a"),
            Err(Error::from("MemBlob has been closed"))
        );
        assert_eq!(
            blob_gen1_2.lock()?.set("a", "2".as_bytes().to_vec(), true),
            Err(Error::from("MemBlob has been closed"))
        );
        assert_eq!(
            blob_gen1_2.lock()?.delete("a"),
            Err(Error::from("MemBlob has been closed"))
        );
        assert_eq!(blob_gen2.lock()?.get("a")?, Some("1".into()));

        // The async fetch finishes. This causes the Arc to run the MemBlob Drop
        // impl because it's the last copy of the original Arc.
        drop(blob_gen1_2);

        // There was a regression where the previous drop closed the current
        // MemBlob, make sure it's still usable.
        blob_gen2
            .lock()?
            .set("b", "3".into(), true)
            .expect("blob_take2 should still be open");

        Ok(())
    }
}
