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

use std::fmt::Debug;
use std::time::Instant;

use bytes::Bytes;
use lgalloc::AllocError;
use prometheus::{Counter, CounterVec, Histogram, IntCounter, IntCounterVec};
use tracing::debug;

use crate::cast::{CastFrom, CastLossy};
use crate::metric;
use crate::metrics::MetricsRegistry;
use crate::region::Region;

impl From<MetricsRegion<u8>> for Bytes {
    fn from(bytes: MetricsRegion<u8>) -> Bytes {
        // This will handle the drop correctly when the refcount goes to 0...
        // see the rustdoc on this method for more details.
        Bytes::from_owner(bytes)
    }
}

/// A [Region] wrapper that increments metrics when it is dropped.
///
/// The `T: Copy` bound ensures that the `Region` doesn't leak resources when
/// dropped.
pub struct MetricsRegion<T: Copy> {
    buf: Region<T>,
    free_count: IntCounter,
    free_capacity_bytes: IntCounter,
}

impl<T: Copy + Debug> Debug for MetricsRegion<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self.buf.as_vec(), f)
    }
}

impl<T: Copy> MetricsRegion<T> {
    fn capacity_bytes(&self) -> usize {
        self.buf.capacity() * std::mem::size_of::<T>()
    }

    /// Copy all of the elements from `slice` into the [`Region`].
    ///
    /// # Panics
    ///
    /// * If the [`Region`] does not have enough capacity.
    pub fn extend_from_slice(&mut self, slice: &[T]) {
        self.buf.extend_from_slice(slice);
    }
}

impl<T: Copy + PartialEq> PartialEq for MetricsRegion<T> {
    fn eq(&self, other: &Self) -> bool {
        self.buf.as_vec() == other.buf.as_vec()
    }
}

impl<T: Copy + Eq> Eq for MetricsRegion<T> {}

impl<T: Copy> Drop for MetricsRegion<T> {
    fn drop(&mut self) {
        self.free_count.inc();
        self.free_capacity_bytes
            .inc_by(u64::cast_from(self.capacity_bytes()));
    }
}

impl<T: Copy> AsRef<[T]> for MetricsRegion<T> {
    fn as_ref(&self) -> &[T] {
        &self.buf[..]
    }
}

/// Metrics for lgalloc'd bytes..
#[derive(Debug, Clone)]
pub struct LgBytesMetrics {
    /// Metrics for the "persist_s3" usage of lgalloc bytes.
    pub persist_s3: LgBytesOpMetrics,
    /// Metrics for the "persist_azure" usage of lgalloc bytes.
    pub persist_azure: LgBytesOpMetrics,
    /// Metrics for the "persist_arrow" usage of lgalloc bytes.
    pub persist_arrow: LgBytesOpMetrics,
    /// Metrics for the "persist_gcs" usage of lgalloc bytes.
    pub persist_gcs: LgBytesOpMetrics,
}

/// Metrics for an individual usage of lgalloc bytes.
#[derive(Clone)]
pub struct LgBytesOpMetrics {
    heap: LgBytesRegionMetrics,
    mmap: LgBytesRegionMetrics,
    alloc_seconds: Counter,
    mmap_disabled_count: IntCounter,
    mmap_error_count: IntCounter,
    // NB: Unlike the _bytes per-Region metrics, which are capacity, this is
    // intentionally the requested len.
    len_sizes: Histogram,
}

impl std::fmt::Debug for LgBytesOpMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LgBytesOperationMetrics")
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
struct LgBytesRegionMetrics {
    alloc_count: IntCounter,
    alloc_capacity_bytes: IntCounter,
    free_count: IntCounter,
    free_capacity_bytes: IntCounter,
}

impl LgBytesMetrics {
    /// Returns a new [LgBytesMetrics] connected to the given metrics registry.
    pub fn new(registry: &MetricsRegistry) -> Self {
        let alloc_count: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_alloc_count",
            help: "count of LgBytes allocations",
            var_labels: ["op", "region"],
        ));
        let alloc_capacity_bytes: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_alloc_capacity_bytes",
            help: "total capacity bytes of LgBytes allocations",
            var_labels: ["op", "region"],
        ));
        let free_count: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_free_count",
            help: "count of LgBytes frees",
            var_labels: ["op", "region"],
        ));
        let free_capacity_bytes: IntCounterVec = registry.register(metric!(
            name: "mz_lgbytes_free_capacity_bytes",
            help: "total capacity bytes of LgBytes frees",
            var_labels: ["op", "region"],
        ));
        let alloc_seconds: CounterVec = registry.register(metric!(
            name: "mz_lgbytes_alloc_seconds",
            help: "seconds spent getting LgBytes allocations and copying in data",
            var_labels: ["op"],
        ));
        let mmap_disabled_count: IntCounter = registry.register(metric!(
            name: "mz_bytes_mmap_disabled_count",
            help: "count alloc attempts with lgalloc disabled",
        ));
        let mmap_error_count: IntCounter = registry.register(metric!(
            name: "mz_bytes_mmap_error_count",
            help: "count of errors when attempting file-based mapped alloc",
        ));
        let len_sizes: Histogram = registry.register(metric!(
            name: "mz_bytes_alloc_len_sizes",
            help: "histogram of LgBytes alloc len sizes",
            buckets: crate::stats::HISTOGRAM_BYTE_BUCKETS.to_vec(),
        ));
        let op = |name: &str| LgBytesOpMetrics {
            heap: LgBytesRegionMetrics {
                alloc_count: alloc_count.with_label_values(&[name, "heap"]),
                alloc_capacity_bytes: alloc_capacity_bytes.with_label_values(&[name, "heap"]),
                free_count: free_count.with_label_values(&[name, "heap"]),
                free_capacity_bytes: free_capacity_bytes.with_label_values(&[name, "heap"]),
            },
            mmap: LgBytesRegionMetrics {
                alloc_count: alloc_count.with_label_values(&[name, "mmap"]),
                alloc_capacity_bytes: alloc_capacity_bytes.with_label_values(&[name, "mmap"]),
                free_count: free_count.with_label_values(&[name, "mmap"]),
                free_capacity_bytes: free_capacity_bytes.with_label_values(&[name, "mmap"]),
            },
            alloc_seconds: alloc_seconds.with_label_values(&[name]),
            mmap_disabled_count: mmap_disabled_count.clone(),
            mmap_error_count: mmap_error_count.clone(),
            len_sizes: len_sizes.clone(),
        };
        LgBytesMetrics {
            persist_s3: op("persist_s3"),
            persist_azure: op("persist_azure"),
            persist_arrow: op("persist_arrow"),
            persist_gcs: op("persist_gcs"),
        }
    }
}

impl LgBytesOpMetrics {
    /// Returns a new empty [`MetricsRegion`] to hold at least `T` elements.
    pub fn new_region<T: Copy>(&self, capacity: usize) -> MetricsRegion<T> {
        let start = Instant::now();

        // Round the capacity up to the minimum lgalloc mmap size.
        let capacity = std::cmp::max(capacity, 1 << lgalloc::VALID_SIZE_CLASS.start);
        let region = match Region::new_mmap(capacity) {
            Ok(region) => region,
            Err(err) => {
                if let AllocError::Disabled = err {
                    self.mmap_disabled_count.inc()
                } else {
                    debug!("failed to mmap allocate: {}", err);
                    self.mmap_error_count.inc();
                }
                Region::new_heap(capacity)
            }
        };
        let region = self.metrics_region(region);
        self.alloc_seconds.inc_by(start.elapsed().as_secs_f64());

        region
    }

    /// Attempts to copy the given bytes into an lgalloc-managed file-based mapped
    /// region. If that fails, we return the original bytes.
    pub fn try_mmap_bytes(&self, buf: Bytes) -> Bytes {
        self.try_mmap_region(buf.as_ref())
            .map(Bytes::from)
            .unwrap_or(buf)
    }

    /// Attempts to copy the given buf into an lgalloc managed file-based mapped region.
    pub fn try_mmap_region<T: Copy>(
        &self,
        buf: impl AsRef<[T]>,
    ) -> Result<MetricsRegion<T>, AllocError> {
        let start = Instant::now();
        let buf = buf.as_ref();
        // Round the capacity up to the minimum lgalloc mmap size.
        let capacity = std::cmp::max(buf.len(), 1 << lgalloc::VALID_SIZE_CLASS.start);
        let buf = match Region::new_mmap(capacity) {
            Ok(mut region) => {
                region.extend_from_slice(buf);
                Ok(region)
            }
            Err(err) => {
                match &err {
                    AllocError::Disabled => self.mmap_disabled_count.inc(),
                    err => {
                        debug!("failed to mmap allocate: {}", err);
                        self.mmap_error_count.inc();
                    }
                };
                Err(err)
            }
        }?;
        let region = self.metrics_region(buf);
        self.alloc_seconds.inc_by(start.elapsed().as_secs_f64());
        Ok(region)
    }

    /// Wraps the already owned buf into a [Region::Heap] with metrics.
    ///
    /// Besides metrics, this is essentially a no-op.
    pub fn heap_region<T: Copy>(&self, buf: Vec<T>) -> MetricsRegion<T> {
        // Intentionally don't bother incrementing alloc_seconds here.
        self.metrics_region(Region::Heap(buf))
    }

    fn metrics_region<T: Copy>(&self, buf: Region<T>) -> MetricsRegion<T> {
        let metrics = match buf {
            Region::MMap(_) => &self.mmap,
            Region::Heap(_) => &self.heap,
        };
        let region = MetricsRegion {
            buf,
            free_count: metrics.free_count.clone(),
            free_capacity_bytes: metrics.free_capacity_bytes.clone(),
        };
        metrics.alloc_count.inc();
        metrics
            .alloc_capacity_bytes
            .inc_by(u64::cast_from(region.capacity_bytes()));
        let len_bytes = region.buf.len() * std::mem::size_of::<T>();
        self.len_sizes.observe(f64::cast_lossy(len_bytes));
        region
    }
}
