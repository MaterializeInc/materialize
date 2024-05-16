// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Chooses a global memory allocator based on Cargo features.

use mz_ore::metrics::MetricsRegistry;

#[cfg(all(feature = "jemalloc", not(miri)))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Registers metrics for the global allocator into the provided registry.
///
/// What metrics are registered varies by platform. Not all platforms use
/// allocators that support metrics.
#[cfg(any(not(feature = "jemalloc"), miri))]
#[allow(clippy::unused_async)]
pub async fn register_metrics_into(_: &MetricsRegistry) {
    // No-op on platforms that don't use jemalloc.
}

/// Registers metrics for the global allocator into the provided registry.
///
/// What metrics are registered varies by platform. Not all platforms use
/// allocators that support metrics.
#[cfg(all(feature = "jemalloc", not(miri)))]
pub async fn register_metrics_into(registry: &MetricsRegistry) {
    mz_prof::jemalloc::JemallocMetrics::register_into(registry).await;
}
