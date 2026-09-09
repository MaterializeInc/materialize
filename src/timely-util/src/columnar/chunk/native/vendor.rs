// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See vendor/LICENSE.

//! Resumable maintenance machinery vendored from Differential Dataflow.
//!
//! Source: `TimelyDataflow/differential-dataflow`, upstream base
//! `626c80ce`, with the local async changes at `339f01522823171f2a8c710967502f3e1d09360b`.
//! The source files are `trace/implementations/spine_fueled.rs`,
//! `trace/chunk/mod.rs`, and `batcher/{mod.rs,merge/mod.rs}`.
//!
//! This copy uses MZ's existing Differential and Timely dependencies for timestamps,
//! lattices, descriptions, and logging. The enclosing adapter implements DD's trace
//! interfaces, leaving reader holds and operator progress with DD and Timely.
//! The local methods preserve the source spine's fuel, level, and publication policy.
//! Cursor implementations, general operators, and reference storage backends are
//! outside this module.

// Preserve the synchronous entry points and generic helpers alongside their
// pollable counterparts so the vendored algorithms remain comparable to DD.
#![allow(dead_code)]

pub mod batcher;
pub mod chunk;
pub mod spine;

#[cfg(test)]
mod tests;
