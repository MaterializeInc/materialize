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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use mz_ore::cast::CastFrom;
use mz_ore::pool::{ChunkHandle, ChunkHints, ExtentCodec, Pool};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Block identities are process-local and never reused, including across stores.
static NEXT_BLOCK: AtomicU64 = AtomicU64::new(0);

/// A non-owning row locator, valid only through a manifest or read lease.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    columnar::Columnar
)]
#[columnar(derive(PartialEq, Eq, PartialOrd, Ord))]
pub struct RowHandle {
    block: u64,
    offset: u32,
}

/// Invalid configuration, unresolved ownership, or an oversized working set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreError {
    /// Block sizes must be word-aligned and between 16 bytes and 8 MiB.
    BlockSize,
    /// Admission must accommodate a full block and at least one read job.
    ReadBudget,
    /// One row, including its length prefix, exceeds the block size.
    RowTooLarge,
    /// The complete read set exceeds decoded-byte admission.
    ReadTooLarge,
    /// None of the supplied owners resolves this row.
    UnownedRow,
    /// A selected index is outside its batch.
    InvalidIndex,
}

impl fmt::Display for StoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "payload store: {self:?}")
    }
}

impl std::error::Error for StoreError {}

struct Block {
    handle: ChunkHandle,
    words: usize,
}

/// An immutable collection of payload block owners.
#[derive(Clone, Default)]
pub struct Manifest {
    blocks: BTreeMap<u64, Arc<Block>>,
}

impl Manifest {
    /// Retain exactly the blocks needed by `rows`, before their previous owners retire.
    pub fn retain<'a>(
        rows: impl IntoIterator<Item = RowHandle>,
        owners: impl IntoIterator<Item = &'a Manifest>,
    ) -> Result<Self, StoreError> {
        let owners: BTreeMap<_, _> = owners
            .into_iter()
            .flat_map(|owner| owner.blocks.iter().map(|(id, block)| (*id, block)))
            .collect();
        let mut result = Self::default();
        for row in rows {
            let block = owners.get(&row.block).ok_or(StoreError::UnownedRow)?;
            result
                .blocks
                .entry(row.block)
                .or_insert_with(|| Arc::clone(block));
        }
        Ok(result)
    }

    /// Number of owned blocks, including blocks retained by only one live row.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
}

struct Admission {
    bytes: Arc<Semaphore>,
    jobs: Arc<Semaphore>,
    limit: u32,
    peak: AtomicU64,
    decoded: AtomicU64,
    reads: AtomicU64,
}

/// Shared payload allocation and read admission for several consumers.
#[derive(Clone)]
pub struct Store {
    pool: Pool,
    block_words: usize,
    codec: &'static dyn ExtentCodec,
    admission: Arc<Admission>,
}

/// Read counters, including reservations retained by completed or canceled work.
#[derive(Debug)]
pub struct StoreStats {
    /// Bytes reserved by pending jobs and live leases.
    pub charged_bytes: usize,
    /// Largest observed decoded-byte reservation total.
    pub peak_charged_bytes: u64,
    /// Bytes copied out of the pool, counting repeated reads.
    pub decoded_bytes: u64,
    /// Submitted blocking jobs.
    pub read_jobs: u64,
}

impl Store {
    /// Create a store over an independently configured pool.
    ///
    /// Read admission charges decoded buffer lengths. Metadata and builder
    /// scratch remain outside this prototype's shared byte budget.
    pub fn new(
        pool: Pool,
        block_bytes: usize,
        read_bytes: usize,
        read_jobs: usize,
        codec: &'static dyn ExtentCodec,
    ) -> Result<Self, StoreError> {
        if !(16..=8 << 20).contains(&block_bytes) || block_bytes % 8 != 0 {
            return Err(StoreError::BlockSize);
        }
        if read_bytes < block_bytes
            || read_bytes > usize::cast_from(u32::MAX)
            || read_bytes > Semaphore::MAX_PERMITS
            || read_jobs == 0
            || read_jobs > Semaphore::MAX_PERMITS
        {
            return Err(StoreError::ReadBudget);
        }
        Ok(Self {
            pool,
            block_words: block_bytes / 8,
            codec,
            admission: Arc::new(Admission {
                bytes: Arc::new(Semaphore::new(read_bytes)),
                jobs: Arc::new(Semaphore::new(read_jobs)),
                limit: u32::try_from(read_bytes).expect("validated read budget"),
                peak: AtomicU64::new(0),
                decoded: AtomicU64::new(0),
                reads: AtomicU64::new(0),
            }),
        })
    }

    /// Start a builder with at most one block of word staging.
    pub fn builder(&self) -> PayloadBuilder {
        PayloadBuilder {
            store: self.clone(),
            manifest: Manifest::default(),
            block: None,
            words: Vec::with_capacity(self.block_words),
        }
    }

    /// Resolve and own all blocks for one execution step before it can suspend.
    ///
    /// Repeated references to a block are coalesced. Callers must release prior
    /// leases before requesting another complete step, or reserve the combined
    /// working set here. Splitting a step's acquisition can deadlock on admission.
    pub fn prepare_read<'a>(
        &self,
        rows: impl IntoIterator<Item = (&'a Manifest, RowHandle)>,
    ) -> Result<ReadRequest, StoreError> {
        let mut blocks = BTreeMap::new();
        let mut bytes = 0usize;
        for (manifest, row) in rows {
            let block = manifest
                .blocks
                .get(&row.block)
                .ok_or(StoreError::UnownedRow)?;
            if let std::collections::btree_map::Entry::Vacant(entry) = blocks.entry(row.block) {
                bytes = bytes
                    .checked_add(block.words * 8)
                    .ok_or(StoreError::ReadTooLarge)?;
                if bytes > usize::cast_from(self.admission.limit) {
                    return Err(StoreError::ReadTooLarge);
                }
                entry.insert(Arc::clone(block));
            }
        }
        Ok(ReadRequest {
            blocks,
            bytes: u32::try_from(bytes).expect("checked read size"),
            admission: Arc::clone(&self.admission),
        })
    }

    /// Snapshot the shared read ledger.
    pub fn stats(&self) -> StoreStats {
        StoreStats {
            charged_bytes: usize::cast_from(self.admission.limit)
                - self.admission.bytes.available_permits(),
            peak_charged_bytes: self.admission.peak.load(Ordering::Relaxed),
            decoded_bytes: self.admission.decoded.load(Ordering::Relaxed),
            read_jobs: self.admission.reads.load(Ordering::Relaxed),
        }
    }
}

/// Bounded word staging plus ownership of its completed payload blocks.
pub struct PayloadBuilder {
    store: Store,
    manifest: Manifest,
    block: Option<u64>,
    words: Vec<u64>,
}

impl PayloadBuilder {
    /// Append a row, returning a locator owned by this builder until `finish`.
    ///
    /// An oversized row is rejected without modifying the builder.
    pub fn push(&mut self, row: &[u8]) -> Result<RowHandle, StoreError> {
        let words = 1 + row.len().div_ceil(8);
        if words > self.store.block_words {
            return Err(StoreError::RowTooLarge);
        }
        if self.words.len() + words > self.store.block_words {
            self.flush();
        }
        let block = *self.block.get_or_insert_with(|| {
            NEXT_BLOCK
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
                .expect("block identity space exhausted")
        });
        let offset = self.words.len();
        self.words
            .push(u64::try_from(row.len()).expect("row fits block"));
        self.words.resize(offset + words, 0);
        bytemuck::cast_slice_mut::<u64, u8>(&mut self.words[offset + 1..])[..row.len()]
            .copy_from_slice(row);
        Ok(RowHandle {
            block,
            offset: u32::try_from(offset).expect("offset fits block"),
        })
    }

    fn flush(&mut self) {
        if let Some(id) = self.block.take() {
            let handle = self.store.pool.insert_with(
                self.words.len(),
                ChunkHints::default(),
                self.store.codec,
                |destination| destination.copy_from_slice(&self.words),
            );
            self.manifest.blocks.insert(
                id,
                Arc::new(Block {
                    handle,
                    words: self.words.len(),
                }),
            );
            self.words.clear();
        }
    }

    /// Publish ownership of all appended rows.
    pub fn finish(mut self) -> Manifest {
        self.flush();
        self.manifest
    }
}

/// An owned read set that is safe to retain after input batches are dropped.
pub struct ReadRequest {
    blocks: BTreeMap<u64, Arc<Block>>,
    bytes: u32,
    admission: Arc<Admission>,
}

impl ReadRequest {
    /// Reserve the full working set and read it on a blocking executor.
    ///
    /// Requires a Tokio runtime. Cancellation after submission retains both
    /// byte and job admission until the blocking work actually finishes.
    pub async fn read(self) -> ReadLease {
        let bytes = Arc::clone(&self.admission.bytes)
            .acquire_many_owned(self.bytes)
            .await
            .expect("read admission remains open");
        let charged =
            usize::cast_from(self.admission.limit) - self.admission.bytes.available_permits();
        self.admission.peak.fetch_max(
            u64::try_from(charged).expect("budget fits u64"),
            Ordering::Relaxed,
        );
        let job = Arc::clone(&self.admission.jobs)
            .acquire_owned()
            .await
            .expect("job admission remains open");
        self.admission.reads.fetch_add(1, Ordering::Relaxed);
        mz_ore::task::spawn_blocking(
            || "payload_read",
            move || {
                // Both permits travel with the work, not the cancelable waiter.
                let _job = job;
                let mut decoded = BTreeMap::new();
                for (id, owner) in self.blocks {
                    let mut words = vec![0; owner.words];
                    owner.handle.read_into(&mut words);
                    self.admission.decoded.fetch_add(
                        u64::try_from(words.len() * 8).expect("block size fits u64"),
                        Ordering::Relaxed,
                    );
                    decoded.insert(
                        id,
                        DecodedBlock {
                            _owner: owner,
                            words,
                        },
                    );
                }
                ReadLease {
                    blocks: decoded,
                    _bytes: bytes,
                }
            },
        )
        .await
    }
}

struct DecodedBlock {
    _owner: Arc<Block>,
    words: Vec<u64>,
}

/// Owned decoded blocks and the byte reservation backing their borrowed rows.
pub struct ReadLease {
    blocks: BTreeMap<u64, DecodedBlock>,
    _bytes: OwnedSemaphorePermit,
}

impl ReadLease {
    /// Borrow a row from a block covered by this lease.
    pub fn get(&self, row: RowHandle) -> Result<&[u8], StoreError> {
        let block = self.blocks.get(&row.block).ok_or(StoreError::UnownedRow)?;
        let offset = usize::cast_from(row.offset);
        let len = usize::try_from(*block.words.get(offset).ok_or(StoreError::UnownedRow)?)
            .map_err(|_| StoreError::UnownedRow)?;
        bytemuck::cast_slice::<u64, u8>(&block.words[offset + 1..])
            .get(..len)
            .ok_or(StoreError::UnownedRow)
    }
}

/// Operator-local exact payload identities, with weak ownership of stored blocks.
///
/// Fingerprints select candidates only. Reuse requires byte equality. Dead entries
/// are swept incrementally, and the index itself remains resident.
pub struct PayloadInterner {
    store: Store,
    entries: BTreeMap<(u64, RowHandle), std::sync::Weak<Block>>,
    sweep_after: Option<(u64, RowHandle)>,
}

impl PayloadInterner {
    /// Create an independent identity domain over a shared payload store.
    pub fn new(store: Store) -> Self {
        Self {
            store,
            entries: BTreeMap::new(),
            sweep_after: None,
        }
    }

    /// Store a batch, reusing the identity of any live, byte-identical payload.
    pub async fn intern(
        &mut self,
        rows: &[Vec<u8>],
    ) -> Result<(Vec<RowHandle>, Manifest), StoreError> {
        use std::hash::{Hash, Hasher};
        self.intern_hashed(rows, |row| {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            row.hash(&mut hasher);
            hasher.finish()
        })
        .await
    }

    async fn intern_hashed(
        &mut self,
        rows: &[Vec<u8>],
        hash: impl Fn(&[u8]) -> u64,
    ) -> Result<(Vec<RowHandle>, Manifest), StoreError> {
        use std::ops::Bound::{Excluded, Unbounded};
        // Sweep work grows with arrivals so wide batches cannot outrun reclamation.
        let sweep_budget = rows.len().saturating_mul(4).max(1024);
        let keys: Vec<_> = self
            .entries
            .range((self.sweep_after.map_or(Unbounded, Excluded), Unbounded))
            .take(sweep_budget)
            .map(|(key, _)| *key)
            .collect();
        self.sweep_after = if keys.last() == self.entries.last_key_value().map(|(key, _)| key) {
            None
        } else {
            keys.last().copied()
        };
        for key in keys {
            if self.entries[&key].strong_count() == 0 {
                self.entries.remove(&key);
            }
        }
        let mut builder = self.store.builder();
        let mut result = Vec::with_capacity(rows.len());
        let mut owners = Manifest::default();
        let mut fresh: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
        let mut cached: Option<ReadLease> = None;
        for (index, bytes) in rows.iter().enumerate() {
            let fingerprint = hash(bytes);
            if let Some(prior) = fresh
                .get(&fingerprint)
                .and_then(|indices| indices.iter().find(|i| rows[**i] == *bytes))
            {
                result.push(result[*prior]);
                continue;
            }
            let mut found = None;
            let lower = (
                fingerprint,
                RowHandle {
                    block: 0,
                    offset: 0,
                },
            );
            let upper = (
                fingerprint,
                RowHandle {
                    block: u64::MAX,
                    offset: u32::MAX,
                },
            );
            for ((_, row), owner) in self.entries.range(lower..=upper) {
                let Some(owner) = owner.upgrade() else {
                    continue;
                };
                if cached.as_ref().is_none_or(|lease| lease.get(*row).is_err()) {
                    // Release the previous reservation before awaiting another block.
                    drop(cached.take());
                    let manifest = Manifest {
                        blocks: BTreeMap::from([(row.block, Arc::clone(&owner))]),
                    };
                    cached = Some(self.store.prepare_read([(&manifest, *row)])?.read().await);
                }
                if cached.as_ref().expect("loaded candidate").get(*row)? == bytes {
                    owners.blocks.insert(row.block, owner);
                    found = Some(*row);
                    break;
                }
            }
            if let Some(row) = found {
                result.push(row);
            } else {
                result.push(builder.push(bytes)?);
                fresh.entry(fingerprint).or_default().push(index);
            }
        }
        let fresh_owners = builder.finish();
        for (hash, indices) in fresh {
            for index in indices {
                let row = result[index];
                let owner = fresh_owners
                    .blocks
                    .get(&row.block)
                    .expect("published payload");
                self.entries.insert((hash, row), Arc::downgrade(owner));
            }
        }
        owners.blocks.extend(fresh_owners.blocks);
        Ok((result, owners))
    }
}

#[cfg(test)]
mod interner_tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn exact_identity_handles_collisions_and_weak_ownership() {
        let pool = mz_ore::pool::Pool::new().unwrap();
        pool.set_budget(0);
        pool.set_rss_target(1 << 20);
        let store = Store::new(pool.clone(), 128, 256, 1, &mz_ore::pool::IDENTITY_CODEC).unwrap();
        let mut interner = PayloadInterner::new(store.clone());
        let bytes = vec![b"first".to_vec(), b"second".to_vec(), b"first".to_vec()];
        let (ids, owner) = interner.intern_hashed(&bytes, |_| 0).await.unwrap();
        assert_eq!(ids[0], ids[2]);
        assert_ne!(ids[0], ids[1]);
        let (again, next_owner) = interner
            .intern_hashed(&[bytes[1].clone(), bytes[0].clone()], |_| 0)
            .await
            .unwrap();
        assert_eq!(again, vec![ids[1], ids[0]]);
        drop(owner);
        let lease = store
            .prepare_read([(&next_owner, again[0]), (&next_owner, again[1])])
            .unwrap()
            .read()
            .await;
        assert_eq!(lease.get(again[0]).unwrap(), b"second");
        assert_eq!(lease.get(again[1]).unwrap(), b"first");
        drop(lease);
        drop(next_owner);
        assert_eq!(
            pool.stats().live_chunks,
            0,
            "the equality index must not own blocks"
        );
        let (replacement, owner) = interner.intern_hashed(&bytes, |_| 0).await.unwrap();
        assert_ne!(replacement[0], ids[0]);
        assert_eq!(replacement[0], replacement[2]);
        drop(owner);
        assert_eq!(store.stats().charged_bytes, 0);
    }
    #[mz_ore::test(tokio::test)]
    async fn equality_index_reclaims_dead_entries_during_large_batches() {
        let pool = mz_ore::pool::Pool::new().unwrap();
        let store = Store::new(pool.clone(), 128, 256, 1, &mz_ore::pool::IDENTITY_CODEC).unwrap();
        let mut interner = PayloadInterner::new(store);
        for epoch in 0..12u64 {
            let rows: Vec<_> = (0..2048)
                .map(|i| (epoch * 2048 + i).to_le_bytes().to_vec())
                .collect();
            let (_, owner) = interner
                .intern_hashed(&rows, |row| u64::from_le_bytes(row.try_into().unwrap()))
                .await
                .unwrap();
            assert!(
                interner.entries.len() <= 4096,
                "dead identities must not accumulate with each batch"
            );
            drop(owner);
        }
        for _ in 0..4 {
            interner.intern(&[]).await.unwrap();
        }
        assert!(interner.entries.is_empty());
        assert_eq!(pool.stats().live_chunks, 0);
    }
}
