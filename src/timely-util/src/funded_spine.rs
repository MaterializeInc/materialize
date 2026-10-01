// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Differential's fueled spine with optional exertion funded by its input.
//!
//! `arrange_core` calls `Trace::exert` once per operator activation, and
//! every call that the exertion policy answers grants a fixed allowance of
//! fuel, or a virtual introduction that rolls up the layers below it. Nothing
//! in the stock spine ties the number of grants to the amount of input, so the
//! optional consolidation it performs scales with how often its operator is
//! scheduled. Enough idle turns between two published batches lift each batch
//! into the largest layer on its own, and merge work per row approaches the
//! size of the largest batch divided by the published batch size.
//!
//! This spine pays for policy-requested effort from two sources: credit that
//! inserted updates accrue, and a bounded bank of allowances that each
//! inserted batch tops up. `arrange_core` inserts batches as its input
//! frontier advances, so the bank tracks upstream progress rather than
//! scheduling, and a quiet input whose frontier advances still converges. An
//! unfunded request that would start new work is declined and the spine stays
//! quiet until the next insert. Once the input closes, exertion is unbounded
//! again so the trace reaches the policy's reduced form.
//!
//! What funding bounds is the number of granted requests: one per
//! `effort / 8` inserted updates from credit, about 125 at the cluster
//! policy's effort of 1000, plus eight per inserted batch from a bank capped
//! at 64. For small batches the bank is the bound. It does not bound the work
//! a grant causes. A grant either
//! applies one `effort` of fuel to the merges in progress or, with none in
//! progress, introduces a virtual batch that can start a merge up into the
//! largest layer, and a merge in progress always finishes, funded or not.
//! Work still stops scaling with scheduling: an unfunded turn can only
//! advance a merge already in progress, which must complete before anything
//! else lands at its level, and every new merge a grant starts is paid for by
//! input.
//!
//! NOTE: `spine_fueled` is a copy, not a dependency. Bumping
//! differential-dataflow does not update it, so upstream fixes to its spine
//! reach this one only when someone ports them. Review upstream's
//! `spine_fueled.rs` changes on every bump. As this copy diverges, not every
//! change will apply.
//!
//! `spine_fueled` is `trace/implementations/spine_fueled.rs` from
//! differential-dataflow 0.25.1 with its crate-internal paths rewritten. The
//! funding changes are its two funding fields and their constants, the
//! `insert` path that funds them, and the `exert` path that spends them, so
//! the diff against upstream stays reviewable. Everything else, including the
//! `Trace` and `TraceReader` contracts, is upstream's.

#[rustfmt::skip]
#[allow(clippy::as_conversions, clippy::needless_pass_by_ref_mut)]
pub mod spine_fueled;

pub use spine_fueled::Spine;

#[cfg(test)]
mod tests;
