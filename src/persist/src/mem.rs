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
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use tokio::runtime::Runtime as AsyncRuntime;
use tracing::warn;

use crate::client::RuntimeClient;
use crate::error::Error;
use crate::indexed::cache::BlobCache;
use crate::indexed::metrics::Metrics;
use crate::indexed::Indexed;
use crate::location::{
    Atomicity, Blob, BlobMulti, BlobRead, Consensus, ExternalError, LockInfo, Log, SeqNo,
    VersionedData,
};
use crate::runtime::{self, RuntimeConfig};
use crate::unreliable::{UnreliableBlob, UnreliableHandleOld, UnreliableLog};

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
            warn!("MemLog dropped without close");
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

    #[cfg(test)]
    fn new_read() -> Self {
        MemBlobCore {
            dataz: HashMap::new(),
            lock: None,
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
        Ok(self.dataz.get(key).cloned())
    }

    fn set(&mut self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        self.ensure_open()?;
        self.dataz.insert(key.to_owned(), value);
        Ok(())
    }

    fn list_keys(&self) -> Result<Vec<String>, Error> {
        Ok(self.dataz.keys().cloned().collect())
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.ensure_open()?;
        self.dataz.remove(key);
        Ok(())
    }
}

/// Configuration for opening a [MemBlob] or [MemBlobRead].
#[derive(Debug)]
pub struct MemBlobConfig {
    core: Arc<Mutex<MemBlobCore>>,
}

/// An in-memory implementation of [BlobRead].
#[derive(Debug)]
pub struct MemBlobRead {
    core: Option<Arc<Mutex<MemBlobCore>>>,
}

impl MemBlobRead {
    fn open(config: MemBlobConfig) -> MemBlobRead {
        MemBlobRead {
            core: Some(config.core),
        }
    }

    fn core_lock<'c>(&'c self) -> Result<MutexGuard<'c, MemBlobCore>, Error> {
        match self.core.as_ref() {
            None => return Err("MemBlob has been closed".into()),
            Some(core) => Ok(core.lock()?),
        }
    }
}

#[async_trait]
impl BlobRead for MemBlobRead {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core_lock()?.get(key)
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core_lock()?.list_keys()
    }

    async fn close(&mut self) -> Result<bool, Error> {
        match self.core.take() {
            None => Ok(false), // Someone already called close.
            Some(core) => core.lock()?.close(),
        }
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
        let core = Some(Arc::new(Mutex::new(MemBlobCore::new(lock_info))));
        MemBlob { core }
    }

    /// Constructs a new, empty MemBlob with a unique reentrance id.
    ///
    /// Helper for tests that don't care about locking reentrance (which is most
    /// of them).
    #[cfg(test)]
    pub fn new_no_reentrance(lock_info_details: &str) -> Self {
        Self::new(LockInfo::new_no_reentrance(lock_info_details.to_owned()))
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
        let did_work =
            futures_executor::block_on(self.close()).expect("closing MemBlob cannot fail");
        // MemLog should have been closed gracefully; this drop is only here
        // as a failsafe. If it actually did anything, that's surprising.
        if did_work {
            warn!("MemBlob dropped without close");
        }
    }
}

#[async_trait]
impl BlobRead for MemBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core_lock()?.get(key)
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core_lock()?.list_keys()
    }

    async fn close(&mut self) -> Result<bool, Error> {
        match self.core.take() {
            None => Ok(false), // Someone already called close.
            Some(core) => core.lock()?.close(),
        }
    }
}

#[async_trait]
impl Blob for MemBlob {
    type Config = MemBlobConfig;
    type Read = MemBlobRead;

    fn open_exclusive(config: MemBlobConfig, lock_info: LockInfo) -> Result<Self, Error> {
        let core = config.core;
        core.lock()?.open(lock_info)?;
        Ok(MemBlob { core: Some(core) })
    }

    fn open_read(config: MemBlobConfig) -> Result<MemBlobRead, Error> {
        Ok(MemBlobRead::open(config))
    }

    async fn set(&mut self, key: &str, value: Vec<u8>, _atomic: Atomicity) -> Result<(), Error> {
        // NB: This is always atomic, so we're free to ignore the atomic param.
        self.core_lock()?.set(key, value)
    }

    async fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.core_lock()?.delete(key)
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
            Arc::clone(&self.log),
            LockInfo::new_no_reentrance("MemRegistry".to_owned()),
        )
    }

    /// Opens the [MemBlob] contained by this registry.
    pub fn blob_no_reentrance(&self) -> Result<MemBlob, Error> {
        MemBlob::open_exclusive(
            MemBlobConfig {
                core: Arc::clone(&self.blob),
            },
            LockInfo::new_no_reentrance("MemRegistry".to_owned()),
        )
    }

    /// Returns a [RuntimeClient] using the [MemLog] and [MemBlob] contained by
    /// this registry.
    pub fn indexed_no_reentrance(&mut self) -> Result<Indexed<MemLog, MemBlob>, Error> {
        let log = self.log_no_reentrance()?;
        let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::clone(&metrics),
            async_runtime,
            self.blob_no_reentrance()?,
            None,
        );
        Indexed::new(log, blob, metrics)
    }

    /// Returns a [RuntimeClient] with unreliable storage backed by the given
    /// [`UnreliableHandleOld`].
    pub fn indexed_unreliable(
        &mut self,
        unreliable: UnreliableHandleOld,
    ) -> Result<Indexed<UnreliableLog<MemLog>, UnreliableBlob<MemBlob>>, Error> {
        let log = self.log_no_reentrance()?;
        let log = UnreliableLog::from_handle(log, unreliable.clone());
        let metrics = Arc::new(Metrics::register_with(&MetricsRegistry::new()));
        let async_runtime = Arc::new(AsyncRuntime::new()?);
        let blob = self.blob_no_reentrance()?;
        let blob = UnreliableBlob::from_handle(blob, unreliable);
        let blob = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::clone(&metrics),
            async_runtime,
            blob,
            None,
        );
        Indexed::new(log, blob, metrics)
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
            mz_build_info::DUMMY_BUILD_INFO,
            &MetricsRegistry::new(),
            None,
        )
    }

    /// Open a [RuntimeClient] with unreliable storage backed by the given
    /// [`UnreliableHandleOld`].
    pub fn runtime_unreliable(
        &mut self,
        unreliable: UnreliableHandleOld,
    ) -> Result<RuntimeClient, Error> {
        let log = self.log_no_reentrance()?;
        let log = UnreliableLog::from_handle(log, unreliable.clone());
        let blob = self.blob_no_reentrance()?;
        let blob = UnreliableBlob::from_handle(blob, unreliable);
        runtime::start(
            RuntimeConfig::for_tests(),
            log,
            blob,
            mz_build_info::DUMMY_BUILD_INFO,
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
    blob_multi_by_path: HashMap<String, Arc<tokio::sync::Mutex<MemBlobMultiCore>>>,
}

#[cfg(test)]
impl MemMultiRegistry {
    /// Constructs a new, empty [MemMultiRegistry].
    pub fn new() -> Self {
        MemMultiRegistry {
            log_by_path: HashMap::new(),
            blob_by_path: HashMap::new(),
            blob_multi_by_path: HashMap::new(),
        }
    }

    /// Opens a [MemLog] associated with `path`.
    pub fn log(&mut self, path: &str, lock_info: LockInfo) -> Result<MemLog, Error> {
        if let Some(log) = self.log_by_path.get(path) {
            MemLog::open(Arc::clone(&log), lock_info)
        } else {
            let log = Arc::new(Mutex::new(MemLogCore::new(lock_info)));
            self.log_by_path.insert(path.to_string(), Arc::clone(&log));
            let log = MemLog { core: Some(log) };
            Ok(log)
        }
    }

    /// Opens a [MemBlob] associated with `path`.
    pub fn blob(&mut self, path: &str, lock_info: LockInfo) -> Result<MemBlob, Error> {
        if let Some(blob) = self.blob_by_path.get(path) {
            MemBlob::open_exclusive(
                MemBlobConfig {
                    core: Arc::clone(&blob),
                },
                lock_info,
            )
        } else {
            let blob = Arc::new(Mutex::new(MemBlobCore::new(lock_info)));
            self.blob_by_path
                .insert(path.to_string(), Arc::clone(&blob));
            let blob = MemBlob { core: Some(blob) };
            Ok(blob)
        }
    }

    /// Opens a [MemBlobRead] associated with `path`.
    pub fn blob_read(&mut self, path: &str) -> MemBlobRead {
        if let Some(blob) = self.blob_by_path.get(path) {
            MemBlobRead::open(MemBlobConfig {
                core: Arc::clone(&blob),
            })
        } else {
            let blob = Arc::new(Mutex::new(MemBlobCore::new_read()));
            self.blob_by_path
                .insert(path.to_string(), Arc::clone(&blob));
            MemBlobRead::open(MemBlobConfig { core: blob })
        }
    }

    /// Opens a [MemBlobMulti] associated with `path`.
    pub async fn blob_multi(&mut self, path: &str) -> MemBlobMulti {
        if let Some(blob) = self.blob_multi_by_path.get(path) {
            MemBlobMulti::open(MemBlobMultiConfig {
                core: Arc::clone(&blob),
            })
        } else {
            let blob = Arc::new(tokio::sync::Mutex::new(MemBlobMultiCore::default()));
            self.blob_multi_by_path
                .insert(path.to_string(), Arc::clone(&blob));
            MemBlobMulti::open(MemBlobMultiConfig { core: blob })
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
            mz_build_info::DUMMY_BUILD_INFO,
            &MetricsRegistry::new(),
            None,
        )
    }
}

#[derive(Debug, Default)]
struct MemBlobMultiCore {
    dataz: HashMap<String, Vec<u8>>,
}

impl MemBlobMultiCore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        Ok(self.dataz.get(key).cloned())
    }

    fn set(&mut self, key: &str, value: Vec<u8>) -> Result<(), ExternalError> {
        self.dataz.insert(key.to_owned(), value);
        Ok(())
    }

    fn list_keys(&self) -> Result<Vec<String>, ExternalError> {
        Ok(self.dataz.keys().cloned().collect())
    }

    fn delete(&mut self, key: &str) -> Result<(), ExternalError> {
        self.dataz.remove(key);
        Ok(())
    }
}

/// Configuration for opening a [MemBlobMulti].
#[derive(Debug, Default)]
pub struct MemBlobMultiConfig {
    core: Arc<tokio::sync::Mutex<MemBlobMultiCore>>,
}

/// An in-memory implementation of [BlobMulti].
#[derive(Debug)]
pub struct MemBlobMulti {
    core: Arc<tokio::sync::Mutex<MemBlobMultiCore>>,
}

impl MemBlobMulti {
    /// Opens the given location for non-exclusive read-write access.
    pub fn open(config: MemBlobMultiConfig) -> Self {
        MemBlobMulti { core: config.core }
    }
}

#[async_trait]
impl BlobMulti for MemBlobMulti {
    async fn get(&self, _deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        self.core.lock().await.get(key)
    }

    async fn list_keys(&self, _deadline: Instant) -> Result<Vec<String>, ExternalError> {
        self.core.lock().await.list_keys()
    }

    async fn set(
        &self,
        _deadline: Instant,
        key: &str,
        value: Vec<u8>,
        _atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        // NB: This is always atomic, so we're free to ignore the atomic param.
        self.core.lock().await.set(key, value)
    }

    async fn delete(&self, _deadline: Instant, key: &str) -> Result<(), ExternalError> {
        self.core.lock().await.delete(key)
    }
}

/// An in-memory implementation of [Consensus].
#[derive(Debug)]
pub struct MemConsensus {
    // TODO: This was intended to be a tokio::sync::Mutex but that seems to
    // regularly deadlock in the `concurrency` test.
    data: Arc<Mutex<HashMap<String, Vec<VersionedData>>>>,
}

impl Default for MemConsensus {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Consensus for MemConsensus {
    async fn head(
        &self,
        _deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        let store = self.data.lock().map_err(Error::from)?;
        let values = match store.get(key) {
            None => return Ok(None),
            Some(values) => values,
        };

        Ok(values.last().cloned())
    }

    async fn compare_and_set(
        &self,
        _deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(ExternalError::from(
                        anyhow!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)));
            }
        }

        if new.seqno.0 > i64::MAX.try_into().expect("i64::MAX known to fit in u64") {
            return Err(ExternalError::from(anyhow!(
                "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                new.seqno
            )));
        }
        let mut store = self.data.lock().map_err(Error::from)?;

        let data = match store.get(key) {
            None => None,
            Some(values) => values.last(),
        };

        let seqno = data.as_ref().map(|data| data.seqno);

        if seqno != expected {
            return Ok(Err(data.cloned()));
        }

        store.entry(key.to_string()).or_default().push(new);

        Ok(Ok(()))
    }

    async fn scan(
        &self,
        _deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let store = self.data.lock().map_err(Error::from)?;
        let mut results = vec![];
        if let Some(values) = store.get(key) {
            // TODO: we could instead binary search to find the first valid
            // key and then binary search the rest.
            for value in values {
                if value.seqno >= from {
                    results.push(value.clone());
                }
            }
        }

        if results.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {:?}",
                from
            )))
        } else {
            Ok(results)
        }
    }

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        let current = self.head(deadline, key).await?;
        if current.map_or(true, |data| data.seqno < seqno) {
            return Err(ExternalError::from(anyhow!(
                "upper bound too high for truncate: {:?}",
                seqno
            )));
        }

        let mut store = self.data.lock().map_err(Error::from)?;

        if let Some(values) = store.get_mut(key) {
            values.retain(|val| val.seqno >= seqno);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO(#12633): Remove override.
    #![allow(clippy::await_holding_lock)]
    use crate::location::tests::{
        blob_impl_test, blob_multi_impl_test, consensus_impl_test, log_impl_test,
    };
    use crate::location::Atomicity::RequireAtomic;

    use super::*;

    #[test]
    fn mem_log() -> Result<(), Error> {
        let mut registry = MemMultiRegistry::new();
        log_impl_test(move |t| registry.log(t.path, (t.reentrance_id, "log_impl_test").into()))
    }

    #[tokio::test]
    async fn mem_blob() -> Result<(), Error> {
        let registry = Arc::new(Mutex::new(MemMultiRegistry::new()));
        let registry_read = Arc::clone(&registry);
        blob_impl_test(
            move |t| {
                registry
                    .lock()?
                    .blob(t.path, (t.reentrance_id, "blob_impl_test").into())
            },
            move |path| Ok(registry_read.lock()?.blob_read(path)),
        )
        .await
    }

    #[tokio::test]
    async fn mem_blob_multi() -> Result<(), ExternalError> {
        let registry = Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new()));
        blob_multi_impl_test(move |path| {
            let path = path.to_owned();
            let registry = Arc::clone(&registry);
            async move { Ok(registry.lock().await.blob_multi(&path).await) }
        })
        .await
    }

    #[tokio::test]
    async fn mem_consensus() -> Result<(), ExternalError> {
        consensus_impl_test(|| async { Ok(MemConsensus::default()) }).await
    }

    // This test covers a regression that was affecting the nemesis tests where
    // async fetches happening in background threads could race with a close and
    // re-open of MemBlob and then incorrectly still affect the newly open
    // MemBlob though the handler for the previous (now-closed) MemBlob.
    //
    // This is really only a problem for tests, but it's a common pattern in
    // tests to model restarts, so it's worth getting right.
    #[tokio::test]
    async fn regression_delayed_close() -> Result<(), Error> {
        let registry = MemRegistry::new();

        // Put a blob in an Arc<Mutex<..>> and copy it (like we do to in
        // BlobCache to share it between the main persist loop and maintenance).
        let blob_gen1_1 = Arc::new(Mutex::new(registry.blob_no_reentrance()?));
        let blob_gen1_2 = Arc::clone(&blob_gen1_1);

        // Close one of them because the runtime is shutting down, but keep the
        // other around (to simulate an async fetch in maintenance).
        assert_eq!(blob_gen1_1.lock()?.close().await?, true);
        drop(blob_gen1_1);

        // Now "restart" everything and reuse this blob like nemesis does.
        let blob_gen2 = Arc::new(Mutex::new(registry.blob_no_reentrance()?));

        // Write some data with the new handle.
        blob_gen2
            .lock()?
            .set("a", "1".into(), RequireAtomic)
            .await?;

        // The old handle should not be usable anymore. Writes and reads using
        // it should fail and the value set by blob_gen2 should not be affected.
        assert_eq!(
            blob_gen1_2.lock()?.get("a").await,
            Err(Error::from("MemBlob has been closed"))
        );
        assert_eq!(
            blob_gen1_2
                .lock()?
                .set("a", "2".as_bytes().to_vec(), RequireAtomic)
                .await,
            Err(Error::from("MemBlob has been closed"))
        );
        assert_eq!(
            blob_gen1_2.lock()?.delete("a").await,
            Err(Error::from("MemBlob has been closed"))
        );
        assert_eq!(blob_gen2.lock()?.get("a").await?, Some("1".into()));

        // The async fetch finishes. This causes the Arc to run the MemBlob Drop
        // impl because it's the last copy of the original Arc.
        drop(blob_gen1_2);

        // There was a regression where the previous drop closed the current
        // MemBlob, make sure it's still usable.
        blob_gen2
            .lock()?
            .set("b", "3".into(), RequireAtomic)
            .await
            .expect("blob_take2 should still be open");

        Ok(())
    }
}
