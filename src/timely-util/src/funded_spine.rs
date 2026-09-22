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
//! inserted batch, one per input frontier advance, tops up. An unfunded
//! request is declined and the spine stays quiet until the next insert. Once
//! the input closes, exertion is unbounded again so the trace reaches the
//! policy's reduced form.
//!
//! `spine_fueled` is `trace/implementations/spine_fueled.rs` from
//! differential-dataflow 0.25.1 with its crate-internal paths rewritten. The
//! funding changes are confined to `Spine::exert`, `Spine::insert`, the
//! constructor, and the constants that anchor them, so the diff against
//! upstream stays reviewable. Everything else, including the `Trace` and
//! `TraceReader` contracts, is upstream's.

#[rustfmt::skip]
#[allow(clippy::as_conversions, clippy::needless_pass_by_ref_mut)]
pub mod spine_fueled;

pub use spine_fueled::Spine;

#[cfg(test)]
mod tests;
