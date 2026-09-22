// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A copy of differential-dataflow's fueled spine.
//!
//! `spine_fueled` is `trace/implementations/spine_fueled.rs` from
//! differential-dataflow 0.25.1 with its crate-internal paths rewritten. It
//! is kept verbatim so that the diff against upstream stays reviewable.

#[rustfmt::skip]
#[allow(clippy::as_conversions, clippy::needless_pass_by_ref_mut)]
pub mod spine_fueled;

pub use spine_fueled::Spine;
