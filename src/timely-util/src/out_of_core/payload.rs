// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Pool-backed row storage, block ownership, and decoded-read admission.
//!
//! Builders publish immutable blocks and non-owning row handles. Manifests keep
//! referenced blocks alive independently of metadata batches. A prepared read
//! takes ownership before it can suspend, and its returned lease retains both
//! decoded bytes and their admission reservation until the consumer releases it.
//!
//! The pool governs resident storage. This module separately budgets decoded
//! copies returned by reads. Builder scratch, manifests, and synchronous comparison
//! buffers are not charged to that decoded-byte budget.

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

    /// Whether a row and its length prefix fit in one payload block.
    pub fn can_store(&self, row_bytes: usize) -> bool {
        row_bytes <= (self.block_words - 1) * 8
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
        if !self.store.can_store(row.len()) {
            return Err(StoreError::RowTooLarge);
        }
        let words = 1 + row.len().div_ceil(8);
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

/// A payload borrowed inline or addressed through an owning manifest.
#[derive(Clone, Copy)]
pub enum PayloadRef<'a> {
    /// Bytes already available to the caller.
    Inline(&'a [u8]),
    /// Bytes resolved through the manifests supplied to the comparison.
    External(RowHandle),
}

/// Exact byte comparisons for synchronous trace maintenance.
///
/// Retains at most two decoded blocks. These call-scoped buffers are outside
/// asynchronous read admission. A cache miss blocks the caller on the pool read.
/// TODO: Drive comparisons through resumable merge work and shared read admission.
#[derive(Default)]
pub struct PayloadComparator {
    left: ComparisonBlock,
    right: ComparisonBlock,
}

impl PayloadComparator {
    /// Compare payload contents, resolving external rows through `owners`.
    pub fn compare(
        &mut self,
        left: PayloadRef<'_>,
        right: PayloadRef<'_>,
        owners: &[&Manifest],
    ) -> Result<std::cmp::Ordering, StoreError> {
        if let (PayloadRef::External(a), PayloadRef::External(b)) = (left, right) {
            if a == b {
                return Ok(std::cmp::Ordering::Equal);
            }
        }
        Ok(self
            .left
            .resolve(left, owners)?
            .cmp(self.right.resolve(right, owners)?))
    }
}

#[derive(Default)]
struct ComparisonBlock {
    block: Option<u64>,
    words: Vec<u64>,
}

impl ComparisonBlock {
    fn resolve<'a>(
        &'a mut self,
        value: PayloadRef<'a>,
        owners: &[&Manifest],
    ) -> Result<&'a [u8], StoreError> {
        let row = match value {
            PayloadRef::Inline(bytes) => return Ok(bytes),
            PayloadRef::External(row) => row,
        };
        if self.block != Some(row.block) {
            let owner = owners
                .iter()
                .find_map(|owner| owner.blocks.get(&row.block))
                .ok_or(StoreError::UnownedRow)?;
            owner.handle.read_into(&mut self.words);
            self.block = Some(row.block);
        }
        let offset = usize::cast_from(row.offset);
        let len = usize::try_from(*self.words.get(offset).ok_or(StoreError::UnownedRow)?)
            .map_err(|_| StoreError::UnownedRow)?;
        bytemuck::cast_slice::<u64, u8>(&self.words[offset + 1..])
            .get(..len)
            .ok_or(StoreError::UnownedRow)
    }
}

#[cfg(test)]
mod comparison_tests {
    use super::*;

    #[mz_ore::test]
    fn comparison_cache_stays_bounded_across_distinct_blocks() {
        let pool = Pool::new().unwrap();
        pool.set_budget(0);
        let store = Store::new(pool.clone(), 128, 256, 1, &mz_ore::pool::IDENTITY_CODEC).unwrap();
        let mut builder = store.builder();
        let rows: Vec<_> = (0..200u8)
            .map(|i| builder.push(&[i; 96]).unwrap())
            .collect();
        let owner = builder.finish();
        let mut cache = PayloadComparator::default();
        for pair in rows.windows(2) {
            assert_eq!(
                cache
                    .compare(
                        PayloadRef::External(pair[0]),
                        PayloadRef::External(pair[1]),
                        &[&owner]
                    )
                    .unwrap(),
                std::cmp::Ordering::Less
            );
            assert!(cache.left.words.capacity() <= 16);
            assert!(cache.right.words.capacity() <= 16);
        }
        let mut builder = store.builder();
        let copy = builder.push(&[0; 96]).unwrap();
        let copy_owner = builder.finish();
        assert_eq!(
            cache
                .compare(
                    PayloadRef::External(rows[0]),
                    PayloadRef::External(copy),
                    &[&owner, &copy_owner]
                )
                .unwrap(),
            std::cmp::Ordering::Equal
        );
        assert_eq!(
            cache
                .compare(
                    PayloadRef::Inline(&[0; 96]),
                    PayloadRef::External(copy),
                    &[&copy_owner]
                )
                .unwrap(),
            std::cmp::Ordering::Equal
        );
        drop(owner);
        drop(copy_owner);
        assert_eq!(
            pool.stats().live_chunks,
            0,
            "comparison cache must not own payload blocks"
        );
    }
}
