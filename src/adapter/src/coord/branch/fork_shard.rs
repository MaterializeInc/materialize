// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Placeholder for the per-shard fork primitive.
//!
//! The full implementation snapshots a source persist shard's trace at a
//! caller-supplied `branch_ts`, rewrites each part's key from
//! `PartialBatchKey::Relative` to `PartialBatchKey::Absolute` pointing at
//! the source shard's blob namespace, stamps every inherited batch with the
//! supplied `cutoff_ts`, and bootstraps a fresh persist shard via
//! `Machine::initialize_from_snapshot`. The new shard's id and the list of
//! absolute blob keys it now references are returned so the caller can
//! bulk-insert reference rows into the shared `fork_blob_refs` table.
//!
//! Concretely landing that requires `Machine`, `HollowBatch`, and
//! `PartialBatchKey` to be reachable from this crate. Those types are
//! currently crate-private in `mz-persist-client`. The follow-on to
//! commit 10 lifts the visibility of the minimum surface needed and fills
//! in the body here.

#![allow(dead_code)]

// Intentionally empty: the public API lives on [`super::fork_shard`] and is
// deferred to a follow-on commit alongside the persist-internal exposure.
