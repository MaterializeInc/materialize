// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The [bytes] crate but backed by [lgalloc].

use std::sync::Arc;
use std::time::Instant;

use bytes::Buf;
use lgalloc::AllocError;
use prometheus::{Counter, CounterVec, Histogram, IntCounter, IntCounterVec};
use tracing::debug;

use crate::cast::{CastFrom, CastLossy};
use crate::metric;
use crate::metrics::MetricsRegistry;
use crate::region::Region;

/// [bytes::Bytes] but backed by [lgalloc].
#[derive(Clone)]
pub struct LgBytes {
    offset: usize,
    region: Arc<MetricsRegion>,
}

// A [Region] wrapper that increments metrics when it is dropped.
struct MetricsRegion {
    buf: Region<u8>,
    free_count: IntCounter,
    free_capacity_bytes: IntCounter,
}

impl std::fmt::Debug for LgBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.as_slice(), f)
    }
}

impl Drop for MetricsRegion {
    fn drop(&mut self) {
        self.free_count.inc();
        self.free_capacity_bytes
            .inc_by(u64::cast_from(self.buf.capacity()));
    }
}

impl LgBytes {
    /// Presents this buf as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        // This implementation of [bytes::Buf] chooses to panic instead of
        // allowing the offset to advance past remaining, which means this
        // invariant should always hold and we shouldn't need the std::cmp::min.
        // Be defensive anyway.
        debug_assert!(self.offset <= self.region.buf.len());
        let offset = std::cmp::min(self.offset, self.region.buf.len());
        &self.region.buf[offset..]
    }
}

impl Buf for LgBytes {
    /// Returns the number of bytes between the current position and the end of
    /// the buffer.
    ///
    /// This value is greater than or equal to the length of the slice returned
    /// by `chunk()`.
    ///
    /// # Implementer notes
    ///
    /// Implementations of `remaining` should ensure that the return value does
    /// not change unless a call is made to `advance` or any other function that
    /// is documented to change the `Buf`'s current position.
    fn remaining(&self) -> usize {
        self.as_slice().len()
    }

    /// Returns a slice starting at the current position and of length between 0
    /// and `Buf::remaining()`. Note that this *can* return shorter slice (this
    /// allows non-continuous internal representation).
    ///
    /// This is a lower level function. Most operations are done with other
    /// functions.
    ///
    /// # Implementer notes
    ///
    /// This function should never panic. Once the end of the buffer is reached,
    /// i.e., `Buf::remaining` returns 0, calls to `chunk()` should return an
    /// empty slice.
    fn chunk(&self) -> &[u8] {
        self.as_slice()
    }

    /// Advance the internal cursor of the Buf
    ///
    /// The next call to `chunk()` will return a slice starting `cnt` bytes
    /// further into the underlying buffer.
    ///
    /// # Panics
    ///
    /// This function panics if `cnt > self.remaining()`.
    ///
    /// # Implementer notes
    ///
    /// It is recommended for implementations of `advance` to panic if `cnt >
    /// self.remaining()`. If the implementation does not panic, the call must
    /// behave as if `cnt == self.remaining()`.
    ///
    /// A call with `cnt == 0` should never panic and be a no-op.
    fn advance(&mut self, cnt: usize) {
        if cnt > self.remaining() {
            panic!(
                "cannot advance by {} only {} remaining",
                cnt,
                self.remaining()
            )
        };
        self.offset += cnt;
    }
}

/// Metrics for [LgBytes].
#[derive(Debug, Clone)]
pub struct LgBytesMetrics {
    heap: LgBytesRegionMetrics,
    mmap: LgBytesRegionMetrics,
    mmap_disabled_count: IntCounter,
    mmap_error_count: IntCounter,
    // NB: Unlike the _bytes per-Region metrics, which are capacity, this is
    // intentionally the requested len.
    len_sizes: Histogram,
}

#[derive(Debug, Clone)]
struct LgBytesRegionMetrics {
    alloc_count: IntCounter,
    alloc_capacity_bytes: IntCounter,
    alloc_seconds: Counter,
    free_count: IntCounter,
    free_capacity_bytes: IntCounter,
}

impl LgBytesMetrics {
    /// Returns a new [LgBytesMetrics] connected to the given metrics registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let alloc_count: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_alloc_count",
            help: "count of LgBytes allocations",
            var_labels: ["region"],
        ));
        let alloc_capacity_bytes: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_alloc_capacity_bytes",
            help: "total capacity bytes of LgBytes allocations",
            var_labels: ["region"],
        ));
        let alloc_seconds: CounterVec = registry.register(metric!(
            name: "mz_lgbytes_alloc_seconds",
            help: "seconds spent getting LgBytes allocations and copying in data",
            var_labels: ["region"],
        ));
        let free_count: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_free_count",
            help: "count of LgBytes frees",
            var_labels: ["region"],
        ));
        let free_capacity_bytes: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_free_capacity_bytes",
            help: "total capacity bytes of LgBytes frees",
            var_labels: ["region"],
        ));
        LgBytesMetrics {
            heap: LgBytesRegionMetrics {
                alloc_count: alloc_count.with_label_values(&["heap"]),
                alloc_capacity_bytes: alloc_capacity_bytes.with_label_values(&["heap"]),
                alloc_seconds: alloc_seconds.with_label_values(&["heap"]),
                free_count: free_count.with_label_values(&["heap"]),
                free_capacity_bytes: free_capacity_bytes.with_label_values(&["heap"]),
            },
            mmap: LgBytesRegionMetrics {
                alloc_count: alloc_count.with_label_values(&["mmap"]),
                alloc_capacity_bytes: alloc_capacity_bytes.with_label_values(&["mmap"]),
                alloc_seconds: alloc_seconds.with_label_values(&["mmap"]),
                free_count: free_count.with_label_values(&["mmap"]),
                free_capacity_bytes: free_capacity_bytes.with_label_values(&["mmap"]),
            },
            mmap_disabled_count: registry.register(metric!(
                name: "mz_bytes_mmap_disabled_count",
                help: "count alloc attempts with lgalloc disabled",
            )),
            mmap_error_count: registry.register(metric!(
                name: "mz_bytes_mmap_error_count",
                help: "count of errors when attempting file-based mapped alloc",
            )),
            len_sizes: registry.register(metric!(
                name: "mz_bytes_alloc_len_sizes",
                help: "histogram of LgBytes alloc len sizes",
                buckets: crate::stats::HISTOGRAM_BYTE_BUCKETS.to_vec(),
            )),
        }
    }

    /// Attempts to copy the given buf into an lgalloc managed file-based mapped
    /// region, falling back to a heap allocation.
    pub fn try_mmap<T: AsRef<[u8]>>(&self, buf: T) -> LgBytes {
        let start = Instant::now();
        let buf = buf.as_ref();
        self.len_sizes.observe(f64::cast_lossy(buf.len()));
        // Round the capacity up to the minimum lgalloc mmap size.
        let capacity = std::cmp::max(buf.len(), 1 << lgalloc::VALID_SIZE_CLASS.start);
        let (metrics, buf) = match Region::new_mmap(capacity) {
            Ok(mut region) => {
                region.extend_from_slice(buf);
                (&self.mmap, region)
            }
            Err(err) => {
                match err {
                    AllocError::Disabled => self.mmap_disabled_count.inc(),
                    err => {
                        debug!("failed to mmap allocate: {}", err);
                        self.mmap_error_count.inc();
                    }
                };
                let region = Region::Heap(buf.as_ref().to_owned());
                (&self.heap, region)
            }
        };
        let region = Arc::new(MetricsRegion {
            buf,
            free_count: metrics.free_count.clone(),
            free_capacity_bytes: metrics.free_capacity_bytes.clone(),
        });
        let bytes = LgBytes { offset: 0, region };
        metrics.alloc_count.inc();
        metrics
            .alloc_capacity_bytes
            .inc_by(u64::cast_from(bytes.region.buf.capacity()));
        metrics.alloc_seconds.inc_by(start.elapsed().as_secs_f64());
        bytes
    }
}
