// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sharing arrangements across timely runtimes.
//!
//! Every spine in [`crate::typedefs`] is a
//! [`SharedSpine`](mz_timely_util::shared_trace::SharedSpine): a trace that, once attached to a
//! publication point, mirrors its batch chain and frontiers there after every mutation, and applies
//! the holds readers register there to its own compaction. This module is the Materialize-side
//! glue over that primitive.
//!
//! * [`Published`] is a publication point. A runtime that reads it as a peer holds it through a
//!   logical-only *peer handle*, which tracks the frontier that runtime has applied.
//! * [`adopt_trace`] attaches an arrangement's trace to a point on the owning worker.
//! * [`SharedReader`] is the `Clone + Send` reader, implementing
//!   [`TraceReader`] so it drives compaction and cursors
//!   like any trace handle, from any thread. [`SharedReader::import_frontier_core`] replays the
//!   shared arrangement into another scope.

// TODO(CPU-215): drop once `crate::sharing` calls this module. Nothing in the crate does yet
// outside tests, so every item here reads as dead and the re-exports below as unused. The
// alternative, exporting the module publicly to keep the analysis quiet, would ship a
// crate-internal primitive on the public surface. Comments throughout name `crate::sharing` for the
// same reason: it is the caller this module is built for.
#![cfg_attr(not(test), expect(unused))]

mod publish;

use differential_dataflow::trace::TraceReader;
use mz_repr::{Diff, Timestamp};
use mz_timely_util::shared_trace::SharedReader;

pub(crate) use self::publish::{Published, adopt_trace};

use crate::typedefs::RowRowSpine;

/// A `Send` reader handle for a published `oks` arrangement.
pub(crate) type SharedOksHandle =
    SharedReader<<RowRowSpine<Timestamp, Diff> as TraceReader>::Batch>;

// `pub(crate)` for sibling test modules. The peek and render tests in `crate::render` and
// `crate::sharing` read a published arrangement through `SharedReaderExt::snapshot_at` and inspect
// holds through `Published::logical_holds`.
#[cfg(test)]
pub(crate) mod tests;
