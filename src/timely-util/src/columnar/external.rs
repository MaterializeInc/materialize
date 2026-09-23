// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Byte strings stored outside the records of a columnar arrangement.
//!
//! A [`Blob`] stands in for a byte string inside a record: a keyed 128-bit
//! content digest plus the location of the bytes in a block. Blobs order and
//! compare by digest alone, so sorting, merging, and consolidating records that
//! hold blobs gives the result the bytes themselves would give, and never reads
//! them. Bytes are touched when a [`BlobWriter`] appends them and when a
//! [`Resolver`] reads them back for a consumer.
//!
//! Blocks are owned by [`BlockSet`]s, which travel with the chunks whose
//! records reference them ([`ExternalChunk`]). A block is freed when the last
//! set naming it drops. A block's bytes live in the buffer pool once it fills,
//! and on the heap until then.
//!
//! NOTE: equal digests are treated as equal bytes. The digest is SHA-256 keyed
//! with a per-process random secret and truncated to 128 bits, so colliding
//! inputs cannot be constructed without the secret, and the chance of an
//! accidental collision among n live values is about n^2 / 2^129. Blobs are
//! meaningful only in the process that wrote them and must not be persisted or
//! sent to another process.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use mz_ore::cast::CastFrom;
use mz_ore::pool::{ChunkHandle, ChunkHints};
use sha2::{Digest, Sha256};

pub mod chunk;

pub use chunk::{BlobLayout, ExternalChunk, ExternalChunker, ExternalStaging, Externalize};

/// Bytes per block. Matches the column chunk commit size, so a full block
/// occupies one 2 MiB pool size class.
const BLOCK_BYTES: usize = 2 << 20;

/// Pool depth hint for sealed blocks. Payload bytes are written once and read
/// only by consumers, so they are the pool's preferred eviction victims.
const BLOCK_DEPTH: u8 = u8::MAX;

/// An out-of-line byte string: ordered and compared by content, located
/// through a block owned by some [`BlockSet`].
///
/// NOTE: the derive also generates `PartialEq<Blob>` for the reference type,
/// which compares every field. Compare references with references, or owned
/// blobs with owned blobs, so that only the digest decides.
#[derive(Clone, Copy, Debug, Default, columnar::Columnar)]
pub struct Blob {
    /// Keyed digest of the bytes. The only field ordering and equality read.
    digest: u128,
    /// The block holding the bytes.
    block: u64,
    /// Byte offset of the bytes within the block.
    offset: u32,
    /// Length of the bytes.
    len: u32,
}

impl Blob {
    /// The keyed content digest.
    pub fn digest(&self) -> u128 {
        self.digest
    }

    /// The block holding the bytes.
    pub fn block(&self) -> u64 {
        self.block
    }

    /// The number of bytes.
    pub fn len(&self) -> usize {
        usize::cast_from(self.len)
    }

    /// Whether the byte string is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl PartialEq for Blob {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl Eq for Blob {}

impl PartialOrd for Blob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Blob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.digest.cmp(&other.digest)
    }
}

impl<R0: PartialEq, R1, R2, R3> PartialEq for BlobReference<R0, R1, R2, R3> {
    fn eq(&self, other: &Self) -> bool {
        self.digest == other.digest
    }
}

impl<R0: Eq, R1, R2, R3> Eq for BlobReference<R0, R1, R2, R3> {}

impl<R0: Ord, R1, R2, R3> PartialOrd for BlobReference<R0, R1, R2, R3> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<R0: Ord, R1, R2, R3> Ord for BlobReference<R0, R1, R2, R3> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.digest.cmp(&other.digest)
    }
}

/// The keyed digest [`BlobWriter::push`] assigns to `bytes`.
pub fn digest(bytes: &[u8]) -> u128 {
    static KEY: OnceLock<[u8; 32]> = OnceLock::new();
    let key = KEY.get_or_init(rand::random);
    let mut hasher = Sha256::new();
    hasher.update(key);
    hasher.update(bytes);
    let out = hasher.finalize();
    let mut digest = [0u8; 16];
    digest.copy_from_slice(&out[..16]);
    u128::from_le_bytes(digest)
}

/// Process-wide block identifiers. Identifiers are never reused, so a stale
/// locator can only fail to resolve, never resolve to other bytes.
static NEXT_BLOCK: AtomicU64 = AtomicU64::new(0);

/// A block of appended byte strings.
struct Block {
    id: u64,
    state: RefCell<BlockState>,
}

enum BlockState {
    /// Still accepting appends, or sealed without a pool.
    Heap(Vec<u8>),
    /// Sealed into the buffer pool.
    Pool {
        handle: Arc<ChunkHandle>,
        len: usize,
    },
}

impl Block {
    fn new(capacity: usize) -> Rc<Self> {
        Rc::new(Block {
            id: NEXT_BLOCK.fetch_add(1, Ordering::Relaxed),
            state: RefCell::new(BlockState::Heap(Vec::with_capacity(capacity))),
        })
    }

    /// Moves a heap block's bytes into the pool, if one is configured.
    fn seal(&self) {
        let mut state = self.state.borrow_mut();
        let BlockState::Heap(bytes) = &mut *state else {
            return;
        };
        let Some(pool) = super::chunk::spill_pool() else {
            return;
        };
        let len = bytes.len();
        let words = len.div_ceil(8);
        let handle = pool.insert_with(
            words,
            ChunkHints { depth: BLOCK_DEPTH },
            &super::chunk::LZ4_CODEC,
            |dst| {
                let dst: &mut [u8] = bytemuck::cast_slice_mut(dst);
                dst[..len].copy_from_slice(bytes);
                dst[len..].fill(0);
            },
        );
        *state = BlockState::Pool {
            handle: Arc::new(handle),
            len,
        };
    }
}

/// Shared ownership of a set of blocks, sorted by identifier.
///
/// A set may name more blocks than the records it travels with reference.
/// Naming fewer frees bytes a record still points at, which is why every
/// operation that creates a record must carry a covering set.
#[derive(Clone, Default)]
pub struct BlockSet(Rc<[Rc<Block>]>);

impl std::fmt::Debug for BlockSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.0.iter().map(|b| b.id)).finish()
    }
}

impl BlockSet {
    fn from_sorted(blocks: Vec<Rc<Block>>) -> Self {
        debug_assert!(blocks.windows(2).all(|w| w[0].id < w[1].id));
        BlockSet(blocks.into())
    }

    /// The number of blocks named.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the set names no blocks.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn find(&self, id: u64) -> Option<&Rc<Block>> {
        self.0
            .binary_search_by_key(&id, |b| b.id)
            .ok()
            .map(|i| &self.0[i])
    }

    /// The union of `sets`.
    pub fn union<'a>(sets: impl IntoIterator<Item = &'a BlockSet>) -> BlockSet {
        let mut sets = sets.into_iter().filter(|s| !s.is_empty());
        let Some(first) = sets.next() else {
            return BlockSet::default();
        };
        let rest: Vec<_> = sets.collect();
        if rest.iter().all(|s| Rc::ptr_eq(&s.0, &first.0)) {
            return first.clone();
        }
        let mut all: Vec<Rc<Block>> = first.0.iter().cloned().collect();
        for set in rest {
            all.extend(set.0.iter().cloned());
        }
        all.sort_by_key(|b| b.id);
        all.dedup_by_key(|b| b.id);
        BlockSet::from_sorted(all)
    }

    /// The subset naming exactly `ids`, which must be sorted and deduplicated.
    ///
    /// Panics if an identifier is not in this set: a record would reference a
    /// block nothing keeps alive.
    pub fn restrict(&self, ids: &[u64]) -> BlockSet {
        if ids.len() == self.0.len() {
            return self.clone();
        }
        let blocks = ids
            .iter()
            .map(|id| {
                Rc::clone(
                    self.find(*id)
                        .unwrap_or_else(|| panic!("block {id} is referenced but not owned")),
                )
            })
            .collect();
        BlockSet::from_sorted(blocks)
    }
}

/// Appends byte strings to blocks and hands out their [`Blob`]s.
///
/// The open block is shared with every set the writer returns, so records can
/// reference it before it fills. It moves to the pool when it fills.
#[derive(Default)]
pub struct BlobWriter {
    open: Option<Rc<Block>>,
    /// Blocks written since the last [`BlobWriter::take_owners`], in creation
    /// order.
    ///
    /// NOTE: the block open at a `take_owners` call stays listed for the next
    /// one. Later pushes can append to it and then seal it before the next
    /// call, and the records they return point into it.
    written: Vec<Rc<Block>>,
}

impl BlobWriter {
    /// Hashes and appends `bytes`, returning its handle.
    pub fn push(&mut self, bytes: &[u8]) -> Blob {
        let digest = digest(bytes);
        let fits = self.open.as_ref().is_some_and(|block| {
            let BlockState::Heap(buf) = &*block.state.borrow() else {
                unreachable!("the open block is on the heap");
            };
            buf.len() + bytes.len() <= BLOCK_BYTES
        });
        if !fits {
            if let Some(block) = self.open.take() {
                block.seal();
            }
            let block = Block::new(bytes.len().max(BLOCK_BYTES));
            self.written.push(Rc::clone(&block));
            self.open = Some(block);
        }
        let block = self.open.as_ref().expect("an open block");
        let mut state = block.state.borrow_mut();
        let BlockState::Heap(buf) = &mut *state else {
            unreachable!("the open block is on the heap");
        };
        let offset = u32::try_from(buf.len()).expect("block offsets fit in u32");
        buf.extend_from_slice(bytes);
        Blob {
            digest,
            block: block.id,
            offset,
            len: u32::try_from(bytes.len()).expect("blob lengths fit in u32"),
        }
    }

    /// A set owning every block written since the previous call, including
    /// the open one.
    pub fn take_owners(&mut self) -> BlockSet {
        let mut blocks = std::mem::take(&mut self.written);
        if let Some(open) = &self.open {
            if blocks.last().map(|b| b.id) != Some(open.id) {
                blocks.push(Rc::clone(open));
            }
            self.written.push(Rc::clone(open));
        }
        BlockSet::from_sorted(blocks)
    }
}

/// The blocks a consumer asked for, keyed by block: pool blocks as read-back
/// copies, heap blocks borrowed in place.
#[derive(Default)]
pub struct Resolver {
    blocks: BTreeMap<u64, Loaded>,
}

enum Loaded {
    Heap(Rc<Block>),
    Read { words: Vec<u64>, len: usize },
}

impl Resolver {
    /// Blocks `blobs` reference that are not loaded yet, found in `owners`.
    fn missing<'a>(
        &self,
        blobs: impl IntoIterator<Item = &'a Blob>,
        owners: &[BlockSet],
    ) -> Vec<Rc<Block>> {
        let mut ids: Vec<u64> = blobs
            .into_iter()
            .map(|b| b.block)
            .filter(|id| !self.blocks.contains_key(id))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter()
            .map(|id| {
                owners
                    .iter()
                    .find_map(|set| set.find(id))
                    .map(Rc::clone)
                    .unwrap_or_else(|| panic!("block {id} is referenced but not owned"))
            })
            .collect()
    }

    /// Records a heap block, or returns the pool handle to read.
    fn hold(&mut self, block: Rc<Block>) -> Option<(u64, Arc<ChunkHandle>, usize)> {
        let pool = match &*block.state.borrow() {
            BlockState::Heap(_) => None,
            BlockState::Pool { handle, len } => Some((block.id, Arc::clone(handle), *len)),
        };
        if pool.is_none() {
            self.blocks.insert(block.id, Loaded::Heap(block));
        }
        pool
    }

    /// Loads every block `blobs` reference, reading each once. Nonresident
    /// pool blocks read off the calling thread.
    ///
    /// Panics if a referenced block is not in `owners`.
    pub async fn load<'a>(
        &mut self,
        blobs: impl IntoIterator<Item = &'a Blob>,
        owners: &[BlockSet],
    ) {
        for block in self.missing(blobs, owners) {
            if let Some((id, handle, len)) = self.hold(block) {
                let words = handle.read_async().await;
                self.blocks.insert(id, Loaded::Read { words, len });
            }
        }
    }

    /// As [`Resolver::load`], reading on the calling thread.
    pub fn load_sync<'a>(
        &mut self,
        blobs: impl IntoIterator<Item = &'a Blob>,
        owners: &[BlockSet],
    ) {
        for block in self.missing(blobs, owners) {
            if let Some((id, handle, len)) = self.hold(block) {
                let mut words = Vec::new();
                handle.read_into(&mut words);
                self.blocks.insert(id, Loaded::Read { words, len });
            }
        }
    }

    /// Calls `f` with the bytes of `blob`. Panics if its block was not loaded.
    pub fn with<R>(&self, blob: &Blob, f: impl FnOnce(&[u8]) -> R) -> R {
        let range = usize::cast_from(blob.offset)..usize::cast_from(blob.offset + blob.len);
        match self.blocks.get(&blob.block) {
            Some(Loaded::Heap(block)) => match &*block.state.borrow() {
                BlockState::Heap(bytes) => f(&bytes[range]),
                // The writer sealed the block after it was loaded.
                BlockState::Pool { handle, len } => {
                    let mut words = Vec::new();
                    handle.read_into(&mut words);
                    let bytes: &[u8] = bytemuck::cast_slice(&words);
                    f(&bytes[..*len][range])
                }
            },
            Some(Loaded::Read { words, len }) => {
                let bytes: &[u8] = bytemuck::cast_slice(words);
                f(&bytes[..*len][range])
            }
            None => panic!("block {} was not loaded", blob.block),
        }
    }

    /// Drops the loaded blocks.
    pub fn clear(&mut self) {
        self.blocks.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn digests_distinguish_content_and_ignore_location() {
        let mut writer = BlobWriter::default();
        let a = writer.push(b"alpha");
        let b = writer.push(b"beta");
        let a2 = writer.push(b"alpha");
        assert_eq!(a, a2);
        assert_ne!((a.block, a.offset), (a2.block, a2.offset));
        assert_ne!(a, b);
        assert_eq!(a.cmp(&b), a.digest.cmp(&b.digest));
    }

    #[mz_ore::test]
    fn resolve_round_trips_open_and_filled_blocks() {
        let mut writer = BlobWriter::default();
        let big = vec![7u8; BLOCK_BYTES - 2];
        let small = b"small".to_vec();
        let blobs = [writer.push(&big), writer.push(&small), writer.push(b"")];
        assert_ne!(
            blobs[0].block, blobs[1].block,
            "the second push opens a block"
        );
        let owners = writer.take_owners();
        assert_eq!(owners.len(), 2);
        let mut resolver = Resolver::default();
        resolver.load_sync(&blobs, std::slice::from_ref(&owners));
        resolver.with(&blobs[0], |bytes| assert_eq!(bytes, &big[..]));
        resolver.with(&blobs[1], |bytes| assert_eq!(bytes, &small[..]));
        resolver.with(&blobs[2], |bytes| assert_eq!(bytes, b""));
    }

    #[mz_ore::test]
    fn owners_cover_a_block_opened_earlier_and_filled_later() {
        let mut writer = BlobWriter::default();
        let early = writer.push(b"early");
        let first = writer.take_owners();
        assert!(first.find(early.block).is_some());
        // The block opened before the previous call fills during this one.
        let filling = writer.push(&vec![1u8; BLOCK_BYTES - 10]);
        let next = writer.push(&[2u8; 64]);
        assert_eq!(filling.block, early.block);
        assert_ne!(next.block, early.block, "the second push opens a new block");
        let second = writer.take_owners();
        assert!(
            second.find(filling.block).is_some(),
            "records written into the carried-over block keep it owned: {second:?}"
        );
        assert!(second.find(next.block).is_some());
        // A call that writes nothing still covers the open block only.
        let third = writer.take_owners();
        assert_eq!(third.len(), 1);
        assert!(third.find(next.block).is_some());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn filled_blocks_seal_into_the_pool_and_read_back() {
        let pool = mz_ore::pool::Pool::new().expect("pool creation");
        pool.set_budget(0);
        crate::columnar::chunk::set_spill_override(Some(pool.clone()));
        let mut writer = BlobWriter::default();
        let first: Vec<u8> = (0..BLOCK_BYTES - 3)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let blobs = [writer.push(&first), writer.push(b"next")];
        let owners = writer.take_owners();
        assert_eq!(
            pool.stats().inserts,
            1,
            "the filled block moved to the pool"
        );
        let mut resolver = Resolver::default();
        let mut read = Box::pin(resolver.load(&blobs, std::slice::from_ref(&owners)));
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);
        assert!(
            std::future::Future::poll(read.as_mut(), &mut cx).is_ready(),
            "without spill threads the read completes inline"
        );
        drop(read);
        resolver.with(&blobs[0], |bytes| assert_eq!(bytes, &first[..]));
        resolver.with(&blobs[1], |bytes| assert_eq!(bytes, b"next"));
        crate::columnar::chunk::set_spill_override(None);
    }

    #[mz_ore::test]
    fn restricted_sets_free_unreferenced_blocks() {
        let mut writer = BlobWriter::default();
        let first = writer.push(&vec![1u8; BLOCK_BYTES]);
        let second = writer.push(b"second");
        let owners = writer.take_owners();
        let weak = Rc::downgrade(owners.find(first.block).unwrap());
        drop(writer);
        let narrowed = owners.restrict(&[second.block]);
        drop(owners);
        assert!(weak.upgrade().is_none(), "the unreferenced block is freed");
        assert_eq!(narrowed.len(), 1);
    }

    #[mz_ore::test]
    #[should_panic(expected = "referenced but not owned")]
    fn restricting_to_an_unowned_block_panics() {
        let mut writer = BlobWriter::default();
        let blob = writer.push(b"x");
        let _ = BlockSet::default().restrict(&[blob.block]);
    }
}
