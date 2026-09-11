// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Experimental batch primitives separating payload storage from operator metadata.
//!
//! Payloads use the pool, while immutable manifests own the blocks referenced by
//! an index. Reads reserve their whole decoded working set before submission and
//! retain that reservation through cancellation and consumption. Selection and
//! equijoin exercise the same ownership and read interfaces with bounded steps.
//!
//! Indexes and manifests are resident in this prototype. Builder scratch is
//! bounded per builder, but is not included in read admission. This is not yet a
//! globally budgeted operator runtime or a replacement for Differential's trace,
//! frontier, and compaction machinery. See the out-of-core operator design.

mod batch;
mod operators;
mod payload;

pub use batch::{Batch, Record};
pub use operators::{JoinCursor, JoinMatch, JoinStep, LatestCursor, SelectionStep};
pub use payload::{
    Manifest, PayloadBuilder, PayloadComparator, PayloadRef, ReadLease, ReadRequest, RowHandle,
    Store, StoreError, StoreStats,
};

#[cfg(test)]
mod tests;
