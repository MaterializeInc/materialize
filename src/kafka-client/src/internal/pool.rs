// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Recycled buffers for large received data: Fetch response frames and the
//! record sets the consumer copies out of them.
//!
//! A buffer is handed out as `Bytes` (see [`BufferPool::freeze`]); when the
//! last `Bytes` referencing it drops, it returns to its pool, up to the
//! pool's limit, or is freed.
//!
//! NOTE: This is about the allocator, not about allocation cost. Client tasks
//! run on whichever `kafka-io` worker polls them, so fresh buffers come from
//! several jemalloc arenas, each of which keeps freed pages resident for its
//! decay time: consuming at a gigabyte per second kept two to three times
//! the live memory resident. Recycled buffers keep the resident set close to
//! the live set.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use bytes::Bytes;

/// The smallest buffer capacity.
const MIN_CAPACITY: usize = 64 << 10;

/// How long a pool keeps its limit before lowering it, so that a
/// fluctuating demand does not free buffers it needs again right away.
const LIMIT_DECAY: Duration = Duration::from_secs(1);

#[derive(Debug, Default)]
struct PoolState {
    /// Free buffers by capacity.
    free: BTreeMap<usize, Vec<Vec<u8>>>,
    /// The total capacity of the free buffers.
    pooled: usize,
    /// The most capacity kept free; buffers returned beyond it are freed.
    limit: usize,
    /// When the limit was last raised or confirmed.
    limit_at: Option<Instant>,
}

impl PoolState {
    /// Adds a returned buffer, if it fits under the limit. Older, smaller
    /// buffers make room for it, so that the pool adapts to growing sizes
    /// instead of freeing every larger buffer. Returns the evicted buffers
    /// and the buffer itself if it does not fit, to be freed outside the
    /// lock.
    fn put(&mut self, buf: Vec<u8>) -> (Vec<Vec<u8>>, Option<Vec<u8>>) {
        let capacity = buf.capacity();
        let mut evicted = Vec::new();
        while self.pooled + capacity > self.limit {
            let Some(mut entry) = self.free.first_entry() else {
                break;
            };
            if *entry.key() >= capacity {
                break;
            }
            let old = entry.get_mut().pop().expect("no empty entries");
            if entry.get().is_empty() {
                entry.remove();
            }
            self.pooled -= old.capacity();
            evicted.push(old);
        }
        if self.pooled + capacity > self.limit {
            return (evicted, Some(buf));
        }
        self.pooled += capacity;
        self.free.entry(capacity).or_default().push(buf);
        (evicted, None)
    }

    /// Frees buffers until at most `limit` capacity is pooled, returning
    /// them to be dropped outside the lock.
    fn trim(&mut self) -> Vec<Vec<u8>> {
        let mut freed = Vec::new();
        while self.pooled > self.limit {
            let Some(mut entry) = self.free.last_entry() else {
                break;
            };
            let buf = entry.get_mut().pop().expect("no empty entries");
            if entry.get().is_empty() {
                entry.remove();
            }
            self.pooled -= buf.capacity();
            freed.push(buf);
        }
        freed
    }
}

/// A pool of buffers. Initially it keeps nothing; see
/// [`BufferPool::decay_limit`].
#[derive(Debug)]
pub(crate) struct BufferPool {
    state: Arc<Mutex<PoolState>>,
    /// A free buffer is reused for data needing at least `1 / fit` of its
    /// capacity.
    fit: usize,
}

impl Default for BufferPool {
    fn default() -> BufferPool {
        BufferPool::with_fit(2)
    }
}

/// A pooled buffer, returned to its pool when dropped.
struct Pooled {
    buf: Vec<u8>,
    pool: Weak<Mutex<PoolState>>,
}

impl AsRef<[u8]> for Pooled {
    fn as_ref(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for Pooled {
    fn drop(&mut self) {
        let Some(pool) = self.pool.upgrade() else {
            return;
        };
        let buf = std::mem::take(&mut self.buf);
        let mut state = pool.lock().expect("lock poisoned");
        let (evicted, rejected) = state.put(buf);
        drop(state);
        // Freed outside the lock.
        drop(evicted);
        drop(rejected);
    }
}

/// The capacity of the buffer for `len` bytes: the next of four steps per
/// doubling, so that buffers fit later data of similar size while wasting at
/// most a fifth.
fn capacity_class(len: usize) -> usize {
    if len <= MIN_CAPACITY {
        return MIN_CAPACITY;
    }
    let power = len.next_power_of_two() / 2;
    let step = power / 4;
    let steps = (len - power).div_ceil(step);
    power + steps * step
}

impl BufferPool {
    /// A pool that reuses a free buffer for data needing at least
    /// `1 / fit` of its capacity.
    pub(crate) fn with_fit(fit: usize) -> BufferPool {
        BufferPool {
            state: Arc::default(),
            fit,
        }
    }

    /// An empty buffer with room for at least `len` bytes, recycled if
    /// possible.
    pub(crate) fn take(&self, len: usize) -> Vec<u8> {
        let capacity = capacity_class(len);
        let recycled = {
            let mut state = self.state.lock().expect("lock poisoned");
            let fit = state
                .free
                .range(capacity..=capacity.saturating_mul(self.fit))
                .next()
                .map(|(c, _)| *c);
            fit.map(|fit| {
                let bufs = state.free.get_mut(&fit).expect("found");
                let buf = bufs.pop().expect("no empty entries");
                if bufs.is_empty() {
                    state.free.remove(&fit);
                }
                state.pooled -= fit;
                buf
            })
        };
        let mut buf = recycled.unwrap_or_else(|| Vec::with_capacity(capacity));
        buf.clear();
        buf
    }

    /// Wraps a buffer from [`BufferPool::take`] so that it returns to the
    /// pool when the last `Bytes` referencing it drops.
    pub(crate) fn freeze(&self, buf: Vec<u8>) -> Bytes {
        Bytes::from_owner(Pooled {
            buf,
            pool: Arc::downgrade(&self.state),
        })
    }

    /// Copies `data` into a pooled buffer.
    pub(crate) fn copy(&self, data: &[u8]) -> Bytes {
        let mut buf = self.take(data.len());
        buf.extend_from_slice(data);
        self.freeze(buf)
    }

    #[cfg(test)]
    fn set_limit(&self, limit: usize) {
        let freed = {
            let mut state = self.state.lock().expect("lock poisoned");
            state.limit = limit;
            state.limit_at = Some(Instant::now());
            state.trim()
        };
        drop(freed);
    }

    /// Sets how much free capacity the pool keeps, freeing buffers beyond
    /// it. Lowers the limit only once it has not been raised to or above its
    /// current value for [`LIMIT_DECAY`].
    pub(crate) fn decay_limit(&self, limit: usize) {
        let now = Instant::now();
        let freed = {
            let mut state = self.state.lock().expect("lock poisoned");
            if limit >= state.limit {
                state.limit = limit;
                state.limit_at = Some(now);
                Vec::new()
            } else if state
                .limit_at
                .is_none_or(|at| now.saturating_duration_since(at) >= LIMIT_DECAY)
            {
                state.limit = limit;
                state.limit_at = Some(now);
                state.trim()
            } else {
                Vec::new()
            }
        };
        drop(freed);
    }

    #[cfg(test)]
    fn pooled(&self) -> usize {
        self.state.lock().expect("lock poisoned").pooled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn classes() {
        assert_eq!(capacity_class(0), MIN_CAPACITY);
        assert_eq!(capacity_class(MIN_CAPACITY), MIN_CAPACITY);
        assert_eq!(capacity_class(MIN_CAPACITY + 1), 80 << 10);
        assert_eq!(capacity_class(1 << 20), 1 << 20);
        assert_eq!(capacity_class((1 << 20) + 1), 1280 << 10);
        assert_eq!(capacity_class(1700 << 10), 1792 << 10);
        for len in [1, 1000, 70_000, 1 << 20, 3_000_000, 100_000_000] {
            let c = capacity_class(len);
            assert!(
                c >= len && (c <= MIN_CAPACITY || c * 4 <= len * 5 + 4),
                "{len} {c}"
            );
        }
    }

    #[mz_ore::test]
    fn recycles() {
        let pool = BufferPool::default();
        // Nothing is kept by default.
        drop(pool.copy(b"a"));
        assert_eq!(pool.pooled(), 0);
        pool.set_limit(1 << 20);
        let a = pool.copy(b"hello");
        let slice = a.slice(1..3);
        assert_eq!(&a[..], b"hello");
        drop(a);
        assert_eq!(pool.pooled(), 0, "still referenced by a slice");
        assert_eq!(&slice[..], b"el");
        drop(slice);
        assert_eq!(pool.pooled(), MIN_CAPACITY);
        let mut b = pool.take(5);
        assert_eq!(pool.pooled(), 0, "reused");
        b.extend_from_slice(b"world");
        let b = pool.freeze(b);
        assert_eq!(&b[..], b"world");
        drop(b);
        // Lowering the limit frees pooled buffers.
        pool.set_limit(0);
        assert_eq!(pool.pooled(), 0);
        drop(pool.copy(&[7; 100]));
        assert_eq!(pool.pooled(), 0);
        // A larger buffer evicts smaller ones to fit under the limit.
        pool.set_limit(MIN_CAPACITY * 2);
        drop(pool.copy(b"small"));
        assert_eq!(pool.pooled(), MIN_CAPACITY);
        drop(pool.copy(&vec![0; MIN_CAPACITY + 1]));
        assert_eq!(pool.pooled(), 80 << 10, "the smaller buffer was evicted");
        // Buffers outliving the pool are freed.
        pool.set_limit(1 << 20);
        let d = pool.copy(b"x");
        drop(pool);
        drop(d);
    }
}
