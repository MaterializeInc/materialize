// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Metrics for storage dataflow operators

use std::sync::Arc;

use mz_ore::metrics::{DeleteOnDropCounter, DeleteOnDropGauge};
use prometheus::core::AtomicU64;

/// Metrics used by the `backpressure` operator.
#[derive(Debug, Clone)]
pub struct BackpressureMetrics {
    /// A counter with the number of emitted bytes.
    pub emitted_bytes: Arc<DeleteOnDropCounter<'static, AtomicU64, Vec<String>>>,
    /// The last count of bytes we are waiting to be retired in the operator. This cannot
    /// be directly compared to `retired_bytes`, but CAN indicate that backpressure is happening.
    pub last_backpressured_bytes: Arc<DeleteOnDropGauge<'static, AtomicU64, Vec<String>>>,
    /// A counter with the number of bytes retired by downstream processing.
    pub retired_bytes: Arc<DeleteOnDropCounter<'static, AtomicU64, Vec<String>>>,
}
