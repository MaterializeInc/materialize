// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence for differential dataflow collections

#![doc = include_str!("../README.md")]
#![warn(missing_docs, missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::clone_on_ref_ptr
)]

use std::fmt;

pub mod cfg;
pub mod client;
pub mod error;
pub mod file;
pub mod gen;
pub mod indexed;
pub mod location;
pub mod mem;
pub mod operators;
pub mod pfuture;
pub mod postgres;
pub mod retry;
pub mod runtime;
pub mod s3;
pub mod sqlite;
pub mod unreliable;
pub mod workload;

#[cfg(test)]
pub mod golden_test;
#[cfg(test)]
pub mod nemesis;

// TODO
// - Backward compatibility of persisted data, particularly the encoded keys and
//   values.
// - Tighten up the jargon and usage of that jargon: write, update, persist,
//   drain, entry, update, data, log, blob, indexed, unsealed, trace.
// - Think through all the <, <=, !<= usages and document them more correctly
//   (aka replace before/after an antichain with in advance of/not in advance
//   of).

// Testing edge cases:
// - Failure while draining from log into unsealed.
// - Equality edge cases around all the various timestamp/frontier checks.

/// A type usable as a persisted key or value.
pub trait Data: Clone + mz_persist_types::Codec + fmt::Debug + Ord {}
impl<T: Clone + mz_persist_types::Codec + fmt::Debug + Ord> Data for T {}
