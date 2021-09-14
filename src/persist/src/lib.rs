// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence for Materialize dataflows.

#![warn(missing_docs)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

use std::fmt;

pub mod error;
pub mod file;
pub mod future;
pub mod indexed;
pub mod mem;
pub mod operators;
pub mod s3;
pub mod storage;
pub mod unreliable;

#[cfg(test)]
pub mod golden_test;
#[cfg(test)]
pub mod nemesis;

// TODO
// - This method of getting the metadata handle ends up being pretty clunky in
//   practice. Maybe instead the user should pass in a mutable reference to a
//   `Meta` they've constructed like `probe_with`?
// - Error handling. Right now, there are a bunch of `expect`s and this likely
//   needs to hook into the error streams that Materialize hands around.
// - Backward compatibility of persisted data, particularly the encoded keys and
//   values.
// - Restarting with a different number of workers.
// - Abomonation is convenient for prototyping, but we'll likely want to reuse
//   one of the popular serialization libraries.
// - Tighten up the jargon and usage of that jargon: write, update, persist,
//   drain, entry, update, data, log, blob, indexed, unsealed, trace.
// - Think through all the <, <=, !<= usages and document them more correctly
//   (aka replace before/after an antichain with in advance of/not in advance
//   of).
// - Clean up the various ways we pass a (sometimes encoded) entry/update
//   around, there are many for no particular reason: returning an iterator,
//   accepting a closure, accepting a mutable Vec, implementing Snapshot, etc.
// - Meta TODO: These were my immediate thoughts but there's stuff I'm
//   forgetting. Flesh this list out.

// Testing edge cases:
// - Failure while draining from log into unsealed.
// - Equality edge cases around all the various timestamp/frontier checks.

/// A type usable as a persisted key or value.
pub trait Data: Clone + persist_types::Codec + fmt::Debug + Ord {}
impl<T: Clone + persist_types::Codec + fmt::Debug + Ord> Data for T {}
