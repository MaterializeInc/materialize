// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A thin wrapper around jemalloc::JemallocMetrics that can be used independent of jemalloc
/// actually being used.

#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
use crate::jemalloc;
use mz_ore::metrics::MetricsRegistry;

#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
pub fn register_into(registry: &MetricsRegistry) {
    jemalloc::JemallocMetrics::register_into(registry);
}

#[cfg(not(all(not(target_os = "macos"), feature = "jemalloc")))]
pub fn register_into(_registry: &MetricsRegistry) {
    // No-op if we dont use jemalloc
}
