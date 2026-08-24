// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::hashable::Hashable;
use mz_ore::cast::CastFrom;
use mz_repr::GlobalId;

mod copy_to_s3_oneshot;
#[cfg(feature = "bench")]
pub mod correction;
#[cfg(not(feature = "bench"))]
mod correction;
#[cfg(feature = "bench")]
pub mod correction_v2;
#[cfg(not(feature = "bench"))]
mod correction_v2;
mod materialized_view;
mod materialized_view_v2;
mod metric_sink;
mod refresh;
mod subscribe;

/// The worker that maintains a persist sink's shared write frontier.
///
/// The `mint` operator tracks the output shard's upper on this worker alone and clears the shared
/// frontier on all the others, so only this worker's copy carries write progress. Anything that
/// reads a persist sink's shared frontier as a measure of writing, rather than as an input to the
/// controller-visible meet, must agree with this election.
///
/// NOTE: this is not the general rule for "the worker that owns a sink's frontier". A sink whose
/// shared frontier is written by every worker has no elected owner, and a sink that elects a
/// worker for some other purpose is not electing one for this. Such a sink must not be routed
/// through here just because the expression matches.
pub(crate) fn frontier_owner(sink_id: GlobalId, peers: usize) -> usize {
    usize::cast_from(sink_id.hashed()) % peers
}
