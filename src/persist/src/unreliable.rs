// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Test utilities for injecting latency and errors.

use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use async_trait::async_trait;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};

use crate::error::Error;
use crate::location::{
    Atomicity, Blob, BlobMulti, BlobRead, Consensus, ExternalError, LockInfo, Log, SeqNo,
    VersionedData,
};

#[derive(Debug)]
struct UnreliableCoreOld {
    unavailable: bool,
    // TODO: Delays, what else?
}

/// A handle for controlling the behavior of an unreliable delegate.
#[derive(Clone, Debug)]
pub struct UnreliableHandleOld {
    core: Arc<Mutex<UnreliableCoreOld>>,
}

impl Default for UnreliableHandleOld {
    fn default() -> Self {
        UnreliableHandleOld {
            core: Arc::new(Mutex::new(UnreliableCoreOld { unavailable: false })),
        }
    }
}

impl UnreliableHandleOld {
    fn check_unavailable(&self, details: &str) -> Result<(), Error> {
        let unavailable = self
            .core
            .lock()
            .expect("never panics while holding lock")
            .unavailable;
        if unavailable {
            Err(format!("unavailable: {}", details).into())
        } else {
            Ok(())
        }
    }

    /// Cause all later operators to return an "unavailable" error.
    pub fn make_unavailable(&mut self) -> &mut UnreliableHandleOld {
        self.core
            .lock()
            .expect("never panics while holding lock")
            .unavailable = true;
        self
    }

    /// Cause all later operators to succeed.
    pub fn make_available(&mut self) -> &mut UnreliableHandleOld {
        self.core
            .lock()
            .expect("never panics while holding lock")
            .unavailable = false;
        self
    }
}

/// An unreliable delegate to [Log].
#[derive(Debug)]
pub struct UnreliableLog<L> {
    handle: UnreliableHandleOld,
    log: L,
}

impl<L: Log> UnreliableLog<L> {
    /// Returns a new [UnreliableLog] and a handle for controlling it.
    pub fn new(log: L) -> (Self, UnreliableHandleOld) {
        let h = UnreliableHandleOld::default();
        let log = Self::from_handle(log, h.clone());
        (log, h)
    }

    /// Returns a new [UnreliableLog] sharing the given handle.
    pub fn from_handle(log: L, handle: UnreliableHandleOld) -> Self {
        UnreliableLog { handle, log }
    }
}

impl<L: Log> Log for UnreliableLog<L> {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        self.handle.check_unavailable("log write")?;
        self.log.write_sync(buf)
    }

    fn snapshot<F>(&self, logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.handle.check_unavailable("log snapshot")?;
        self.log.snapshot(logic)
    }

    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.handle.check_unavailable("log truncate")?;
        self.log.truncate(upper)
    }

    fn close(&mut self) -> Result<bool, Error> {
        // TODO: This check_unavailable is a different order from the others
        // mostly for convenience in the nemesis tests. While we do want to
        // prevent a normal read/write from going though when the storage is
        // unavailable, it makes for a very uninteresting test if we can't clean
        // up LOCK files. OTOH this feels like a smell, revisit.
        let did_work = self.log.close()?;
        self.handle.check_unavailable("log close")?;
        Ok(did_work)
    }
}

/// Configuration for opening an [UnreliableBlob].
#[derive(Debug)]
pub struct UnreliableBlobConfig<B: Blob> {
    handle: UnreliableHandleOld,
    blob: B::Config,
}

/// An unreliable delegate to [Blob].
#[derive(Debug)]
pub struct UnreliableBlob<B> {
    handle: UnreliableHandleOld,
    blob: B,
}

impl<B: BlobRead> UnreliableBlob<B> {
    /// Returns a new [UnreliableBlob] and a handle for controlling it.
    pub fn new(blob: B) -> (Self, UnreliableHandleOld) {
        let h = UnreliableHandleOld::default();
        let blob = Self::from_handle(blob, h.clone());
        (blob, h)
    }

    /// Returns a new [UnreliableLog] sharing the given handle.
    pub fn from_handle(blob: B, handle: UnreliableHandleOld) -> Self {
        UnreliableBlob { handle, blob }
    }
}

#[async_trait]
impl<B: BlobRead + Sync> BlobRead for UnreliableBlob<B> {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.handle.check_unavailable("blob get")?;
        self.blob.get(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.handle.check_unavailable("blob list keys")?;
        self.blob.list_keys().await
    }

    async fn close(&mut self) -> Result<bool, Error> {
        // TODO: This check_unavailable is a different order from the others
        // mostly for convenience in the nemesis tests. While we do want to
        // prevent a normal read/write from going though when the storage is
        // unavailable, it makes for a very uninteresting test if we can't clean
        // up LOCK files. OTOH this feels like a smell, revisit.
        let did_work = self.blob.close().await?;
        self.handle.check_unavailable("blob close")?;
        Ok(did_work)
    }
}

#[async_trait]
impl<B> Blob for UnreliableBlob<B>
where
    B: Blob + Sync,
    B::Read: Sync,
{
    type Config = UnreliableBlobConfig<B>;
    type Read = UnreliableBlob<B::Read>;

    fn open_exclusive(config: UnreliableBlobConfig<B>, lock_info: LockInfo) -> Result<Self, Error> {
        let blob = B::open_exclusive(config.blob, lock_info)?;
        Ok(UnreliableBlob {
            blob,
            handle: config.handle,
        })
    }

    fn open_read(config: UnreliableBlobConfig<B>) -> Result<UnreliableBlob<B::Read>, Error> {
        let blob = B::open_read(config.blob)?;
        Ok(UnreliableBlob {
            blob,
            handle: config.handle,
        })
    }

    async fn set(&mut self, key: &str, value: Vec<u8>, atomic: Atomicity) -> Result<(), Error> {
        self.handle.check_unavailable("blob set")?;
        self.blob.set(key, value, atomic).await
    }

    async fn delete(&mut self, key: &str) -> Result<(), Error> {
        self.handle.check_unavailable("blob delete")?;
        self.blob.delete(key).await
    }
}

/// An unreliable delegate to [BlobMulti].
#[derive(Debug)]
pub struct UnreliableBlobMulti {
    rng: tokio::sync::Mutex<SmallRng>,
    should_happen: f64,
    should_timeout: f64,
    blob: Arc<dyn BlobMulti + Send + Sync>,
}

impl UnreliableBlobMulti {
    /// Returns a new [UnreliableBlobMulti].
    ///
    /// TODO: Once we turn down the old persist API, return an
    /// [UnreliableHandleOld] with the previous totally available or unavailable
    /// behavior, plus the new partially available behavior
    ///
    /// TODO: Ditto the unreliability should be set via [UnreliableHandleOld].
    pub fn new_from_seed(
        seed: u64,
        should_happen: f64,
        should_timeout: f64,
        blob: Arc<dyn BlobMulti + Send + Sync>,
    ) -> Self {
        assert!(should_happen >= 0.0);
        assert!(should_happen <= 1.0);
        assert!(should_timeout >= 0.0);
        assert!(should_timeout <= 1.0);
        UnreliableBlobMulti {
            rng: tokio::sync::Mutex::new(SmallRng::seed_from_u64(seed)),
            should_happen,
            should_timeout,
            blob,
        }
    }

    async fn should_happen(&self) -> bool {
        self.rng.lock().await.gen_bool(self.should_happen)
    }

    async fn should_timeout(&self) -> bool {
        self.rng.lock().await.gen_bool(self.should_timeout)
    }
}

#[async_trait]
impl BlobMulti for UnreliableBlobMulti {
    async fn get(&self, deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        if self.should_happen().await {
            let res = self.blob.get(deadline, key).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }

    async fn list_keys(&self, deadline: Instant) -> Result<Vec<String>, ExternalError> {
        if self.should_happen().await {
            let res = self.blob.list_keys(deadline).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }

    async fn set(
        &self,
        deadline: Instant,
        key: &str,
        value: Vec<u8>,
        atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        if self.should_happen().await {
            let res = self.blob.set(deadline, key, value, atomic).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }

    async fn delete(&self, deadline: Instant, key: &str) -> Result<(), ExternalError> {
        if self.should_happen().await {
            let res = self.blob.delete(deadline, key).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }
}

/// An unreliable delegate to [Consensus].
#[derive(Debug)]
pub struct UnreliableConsensus {
    rng: tokio::sync::Mutex<SmallRng>,
    should_happen: f64,
    should_timeout: f64,
    consensus: Arc<dyn Consensus + Send + Sync>,
}

impl UnreliableConsensus {
    /// Returns a new [UnreliableConsensus].
    ///
    /// TODO: Once we turn down the old persist API, return an
    /// [UnreliableHandleOld] with the previous totally available or unavailable
    /// behavior, plus the new partially available behavior
    ///
    /// TODO: Ditto the unreliability should be set via [UnreliableHandleOld].
    pub fn new_from_seed(
        seed: u64,
        should_happen: f64,
        should_timeout: f64,
        consensus: Arc<dyn Consensus + Send + Sync>,
    ) -> Self {
        assert!(should_happen >= 0.0);
        assert!(should_happen <= 1.0);
        assert!(should_timeout >= 0.0);
        assert!(should_timeout <= 1.0);
        UnreliableConsensus {
            rng: tokio::sync::Mutex::new(SmallRng::seed_from_u64(seed)),
            should_happen,
            should_timeout,
            consensus,
        }
    }

    async fn should_happen(&self) -> bool {
        self.rng.lock().await.gen_bool(self.should_happen)
    }

    async fn should_timeout(&self) -> bool {
        self.rng.lock().await.gen_bool(self.should_timeout)
    }
}

#[async_trait]
impl Consensus for UnreliableConsensus {
    async fn head(
        &self,
        deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        if self.should_happen().await {
            let res = self.consensus.head(deadline, key).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }

    async fn compare_and_set(
        &self,
        deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        if self.should_happen().await {
            let res = self
                .consensus
                .compare_and_set(deadline, key, expected, new)
                .await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }

    async fn scan(
        &self,
        deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        if self.should_happen().await {
            let res = self.consensus.scan(deadline, key, from).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        if self.should_happen().await {
            let res = self.consensus.truncate(deadline, key, seqno).await;
            if !self.should_timeout().await {
                return res;
            }
        }
        Err(ExternalError::new_timeout(deadline))
    }
}

#[cfg(test)]
mod tests {
    use crate::location::Atomicity::RequireAtomic;
    use crate::mem::{MemBlob, MemLog};

    use super::*;

    #[test]
    fn log() {
        let (mut log, mut handle) = UnreliableLog::new(MemLog::new_no_reentrance("unreliable"));

        // Initially starts reliable.
        assert!(log.write_sync(vec![]).is_ok());
        assert!(log.snapshot(|_, _| { Ok(()) }).is_ok());
        assert!(log.truncate(SeqNo(1)).is_ok());

        // Setting it to unavailable causes all operations to fail.
        handle.make_unavailable();
        assert!(log.write_sync(vec![]).is_err());
        assert!(log.snapshot(|_, _| { Ok(()) }).is_err());
        assert!(log.truncate(SeqNo(1)).is_err());

        // Can be set back to working.
        handle.make_available();
        assert!(log.write_sync(vec![]).is_ok());
        assert!(log.snapshot(|_, _| { Ok(()) }).is_ok());
        assert!(log.truncate(SeqNo(2)).is_ok());
    }

    #[tokio::test]
    async fn blob() {
        let (mut blob, mut handle) = UnreliableBlob::new(MemBlob::new_no_reentrance("unreliable"));

        // Initially starts reliable.
        assert!(blob.set("a", b"1".to_vec(), RequireAtomic).await.is_ok());
        assert!(blob.get("a").await.is_ok());

        // Setting it to unavailable causes all operations to fail.
        handle.make_unavailable();
        assert!(blob.set("a", b"2".to_vec(), RequireAtomic).await.is_err());
        assert!(blob.get("a").await.is_err());

        // Can be set back to working.
        handle.make_available();
        assert!(blob.set("a", b"3".to_vec(), RequireAtomic).await.is_ok());
        assert!(blob.get("a").await.is_ok());
    }
}
