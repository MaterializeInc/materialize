// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use mz_repr::CatalogItemId;

use crate::{CachingPolicy, SecretsReader};

/// Default "time to live" for a single cache value, represented in __seconds__.
pub const DEFAULT_TTL_SECS: AtomicU64 = AtomicU64::new(Duration::from_secs(300).as_secs());

#[derive(Debug)]
struct CachingParameters {
    /// Whether caching is enabled, can be changed at runtime.
    enabled: AtomicBool,
    /// Cache values only live for so long.
    ttl_secs: AtomicU64,
}

impl CachingParameters {
    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn set_enabled(&self, enabled: bool) -> bool {
        self.enabled.swap(enabled, Ordering::Relaxed)
    }

    fn ttl(&self) -> Duration {
        let secs = self.ttl_secs.load(Ordering::Relaxed);
        Duration::from_secs(secs)
    }

    fn set_ttl(&self, ttl: Duration) -> Duration {
        let prev = self.ttl_secs.swap(ttl.as_secs(), Ordering::Relaxed);
        Duration::from_secs(prev)
    }
}

impl Default for CachingParameters {
    fn default() -> Self {
        CachingParameters {
            enabled: AtomicBool::new(true),
            ttl_secs: DEFAULT_TTL_SECS,
        }
    }
}

/// Values we store in the cache.
///
/// Note: we manually implement Debug to prevent leaking secrets in logs.
struct CacheItem {
    secret: Vec<u8>,
    ts: Instant,
}

impl CacheItem {
    fn new(secret: Vec<u8>, ts: Instant) -> Self {
        CacheItem { secret, ts }
    }
}

impl fmt::Debug for CacheItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CacheItem")
            .field("secret", &"( ... )")
            .field("ts", &self.ts)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct CachingSecretsReader {
    /// The underlying secrets, source of truth.
    inner: Arc<dyn SecretsReader>,
    /// In-memory cache, not having a size limit or eviction policy is okay because we limit users
    /// to 100 secrets, which should not be a problem to store in-memory.
    cache: Arc<RwLock<BTreeMap<CatalogItemId, CacheItem>>>,
    /// Caching policy, can change at runtime, e.g. via LaunchDarkly.
    policy: Arc<CachingParameters>,
}

impl CachingSecretsReader {
    pub fn new(reader: Arc<dyn SecretsReader>) -> Self {
        CachingSecretsReader {
            inner: reader,
            cache: Arc::new(RwLock::new(BTreeMap::new())),
            policy: Arc::new(CachingParameters::default()),
        }
    }

    pub fn set_policy(&self, policy: CachingPolicy) {
        if policy.enabled {
            let prev = self.enable_caching();
            tracing::info!("Enabling secrets caching, previously enabled {prev}");
        } else {
            let prev = self.disable_caching();
            tracing::info!("Disabling secrets caching, previously enabled {prev}");
        }

        let prev_ttl = self.set_ttl(policy.ttl);
        if prev_ttl != policy.ttl {
            tracing::info!(
                "Updated secrets caching TTL, new {} seconds, prev {} seconds",
                policy.ttl.as_secs(),
                prev_ttl.as_secs()
            );
        }
    }

    /// Enables caching, returning whether we were previously enabled.
    fn enable_caching(&self) -> bool {
        self.policy.set_enabled(true)
    }

    /// Disables caching, returning whether we were previously enabled.
    fn disable_caching(&self) -> bool {
        // Disable and clear the cache of all existing values.
        let was_enabled = self.policy.set_enabled(false);
        self.cache
            .write()
            .expect("CachingSecretsReader panicked!")
            .clear();

        was_enabled
    }

    /// Sets a new "time to live" for cache values, returning the old TTL.
    fn set_ttl(&self, ttl: Duration) -> Duration {
        self.policy.set_ttl(ttl)
    }
}

#[async_trait]
impl SecretsReader for CachingSecretsReader {
    async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error> {
        // Iff our cache is enabled will we read from it.
        if self.policy.enabled() {
            let read_guard = self.cache.read().expect("CachingSecretsReader panicked!");
            let ttl = self.policy.ttl();

            // If we have a cached value we still need to check if it's expired.
            if let Some(CacheItem { secret, ts }) = read_guard.get(&id) {
                if Instant::now().duration_since(*ts) < ttl {
                    return Ok(secret.clone());
                }
            }
        }

        // Otherwise, we need to read from source!
        let value = self.inner.read(id).await?;

        // Cache it, if caching is enabled.
        if self.policy.enabled() {
            let cache_value = CacheItem::new(value.clone(), Instant::now());
            self.cache
                .write()
                .expect("CachingSecretsReader panicked!")
                .insert(id, cache_value);
        }

        Ok(value)
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use mz_repr::CatalogItemId;

    use crate::cache::CachingSecretsReader;
    use crate::{InMemorySecretsController, SecretsController, SecretsReader};

    #[mz_ore::test(tokio::test)]
    async fn test_read_from_cache() {
        let controller = InMemorySecretsController::new();
        let testing_reader = TestingSecretsReader::new(controller.reader());
        let caching_reader = CachingSecretsReader::new(Arc::new(testing_reader.clone()));

        let secret = [42, 42, 42, 42];
        let id = CatalogItemId::User(1);

        // Add a new secret and read it back.
        controller
            .ensure(CatalogItemId::User(1), &secret[..])
            .await
            .expect("success");
        let roundtrip = caching_reader.read(id).await.expect("success");

        // The secret should be correct.
        assert_eq!(roundtrip, secret.to_vec());

        // Read it a second time, our cache should be populated now.
        let roundtrip2 = caching_reader.read(id).await.expect("success");
        assert_eq!(roundtrip2, secret.to_vec());

        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 1);

        // We should only have one read, as the second should have hit the cache.
        assert_eq!(reads[0], id);
    }

    #[mz_ore::test(tokio::test)]
    async fn test_reading_expired_secret() {
        let controller = InMemorySecretsController::new();
        let testing_reader = TestingSecretsReader::new(controller.reader());
        let caching_reader = CachingSecretsReader::new(Arc::new(testing_reader.clone()));

        // Update the caching policy the expire secrets quickly.
        caching_reader.set_ttl(Duration::from_secs(1));

        let secret = [42, 42, 42, 42];
        let id = CatalogItemId::User(1);

        // Store our secret.
        controller.ensure(id, &secret).await.expect("success");
        // Read it once to populate the cache.
        caching_reader.read(id).await.expect("success");

        // Wait for it to timeout.
        std::thread::sleep(Duration::from_secs(2));

        // Read it again, which should hit the underlying source.
        caching_reader.read(id).await.expect("success");

        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 2);

        // Our value should have expired so we should have read from the underlying source.
        assert_eq!(reads[0], id);
        assert_eq!(reads[1], id);
    }

    #[mz_ore::test(tokio::test)]
    async fn test_disabling_cache() {
        let controller = InMemorySecretsController::new();
        let testing_reader = TestingSecretsReader::new(controller.reader());
        let caching_reader = CachingSecretsReader::new(Arc::new(testing_reader.clone()));

        let secret = [42, 42, 42, 42];
        let id = CatalogItemId::User(1);

        // Store a value.
        controller.ensure(id, &secret).await.expect("success");

        // Read twice, which should hit the cache the second time.
        caching_reader.read(id).await.expect("success");
        caching_reader.read(id).await.expect("success");

        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 1);

        // Disable caching.
        caching_reader.disable_caching();

        // Read twice again, both should hit the source.
        caching_reader.read(id).await.expect("success");
        caching_reader.read(id).await.expect("success");

        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 2);

        // Re-enable caching.
        caching_reader.enable_caching();

        // Read twice again, first should populate the cache, second should hit it.
        caching_reader.read(id).await.expect("success");
        caching_reader.read(id).await.expect("success");

        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn updating_cache_values() {
        let controller = InMemorySecretsController::new();
        let testing_reader = TestingSecretsReader::new(controller.reader());
        let caching_reader = CachingSecretsReader::new(Arc::new(testing_reader.clone()));

        let secret = [42, 42, 42, 42];
        let id = CatalogItemId::User(1);

        // Store an initial value.
        controller.ensure(id, &secret).await.expect("success");
        // Read to load the value into the cache.
        caching_reader.read(id).await.expect("success");

        // Update the stored secret.
        let new_secret = [100, 100];
        controller.ensure(id, &new_secret).await.expect("success");

        // Reading from the cache should give us the _old_ value.
        let cached_secret = caching_reader.read(id).await.expect("success");
        assert_eq!(cached_secret, secret);

        // We should only have registered one read, since we made a stale read from the cache.
        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 1);

        // Wait for the secret to expire.
        caching_reader.set_ttl(Duration::from_secs(2));
        std::thread::sleep(Duration::from_secs(2));

        // Since the cache value is expired, we should read from source, and get the new value.
        let read1 = caching_reader.read(id).await.expect("success");
        let read2 = caching_reader.read(id).await.expect("success");
        assert_eq!(read1, new_secret);
        assert_eq!(read1, read2);

        // Should only have one read since we updated the cache.
        let reads = testing_reader.drain();
        assert_eq!(reads.len(), 1);
    }

    /// A "secrets controller" that logs all of the actions it takes and allows us to inject
    /// failures. Used to test the implementation of our caching secrets controller.
    #[derive(Debug, Clone)]
    pub struct TestingSecretsReader {
        /// The underlying secrets controller.
        reader: Arc<dyn SecretsReader>,
        /// A log of reads that have been made.
        reads: Arc<Mutex<Vec<CatalogItemId>>>,
    }

    impl TestingSecretsReader {
        pub fn new(reader: Arc<dyn SecretsReader>) -> Self {
            TestingSecretsReader {
                reader,
                reads: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Drain all of the actions for introspection.
        pub fn drain(&self) -> Vec<CatalogItemId> {
            self.reads
                .lock()
                .expect("TracingSecretsController panicked!")
                .drain(..)
                .collect()
        }

        /// Record that an action has occurred.
        fn record(&self, id: CatalogItemId) {
            self.reads
                .lock()
                .expect("TracingSecretsController panicked!")
                .push(id);
        }
    }

    #[async_trait]
    impl SecretsReader for TestingSecretsReader {
        async fn read(&self, id: CatalogItemId) -> Result<Vec<u8>, anyhow::Error> {
            let result = self.reader.read(id).await;
            self.record(id);
            result
        }
    }
}
