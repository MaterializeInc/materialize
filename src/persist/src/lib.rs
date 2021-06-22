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

pub mod error;
pub mod file;
pub mod indexed;
pub mod mem;
pub mod operators;
pub mod persister;
pub mod storage;

// TODO
// - The concurrency story. I imagine this working something like the following:
//   - Writes from all streams are funnelled to a single thread which
//     essentially runs in a loop, writing and fsync-ing a batch of updates (via
//     an impl of Buffer). When it finishes persisting a batch, it grabs
//     whatever has buffered since it started and uses that as the next batch.
//     If there's ever no work to do, it sleeps until the next write arrives in
//     the buffer.
//   - Another thread loops and periodically moves data from the Buffer into the
//     Indexed structure. Periodically, this thread will get notifications that
//     a timestamp has been "sealed" on some stream, which unblocks moving data
//     from the "Future" part of Indexed into the "Trace" part. Similarly, it
//     will periodically get a notification that the Trace can be compacted.
//   - We also need to be able to read from Indexed, TBD how the concurrency
//     part of this should work.
//   - Also TBD is who controls the two threads described above. Presumably one
//     of regular rust threads, tokio, or timely.
// - Should we hard-code the Key, Val, Time, Diff types everywhere or introduce
//   them as type parameters? Materialize will only be using one combination of
//   them (two with `()` vals?) but the generality might make things easier to
//   read. Of course, it also might make things harder to read.
// - This method of getting the metadata handle ends up being pretty clunky in
//   practice. Maybe instead the user should pass in a mutable reference to a
//   `Meta` they've constructed like `probe_with`?
// - Error handling. Right now, there are a bunch of `expect`s and this likely
//   needs to hook into the error streams that Materialize hands around.
// - What's our story with poisoned mutexes?
// - Backward compatibility of persisted data, particularly the encoded keys and
//   values.
// - Restarting with a different number of workers.
// - Abomonation is convenient for prototyping, but we'll likely want to reuse
//   one of the popular serialization libraries.
// - Tighten up the jargon and usage of that jargon: write, update, persist,
//   drain, entry, update, data, buffer, blob, buffer (again), indexed, future,
//   trace.
// - Think through all the <, <=, !<= usages and document them more correctly
//   (aka replace before/after an antichain with in advance of/not in advance
//   of).
// - Clean up the various ways we pass a (sometimes encoded) entry/update
//   around, there are many for no particular reason: returning an iterator,
//   accepting a closure, accepting a mutable Vec, implementing Snapshot, etc.
// - Meta TODO: These were my immediate thoughts but there's stuff I'm
//   forgetting. Flesh this list out.

// Testing edge cases:
// - Failure while draining from buffer into future.
// - Equality edge cases around all the various timestamp/frontier checks.

/// A unique id for a persisted stream.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Id(pub u64);

/// An exclusivity token needed to construct persistence [operators].
///
/// Intentionally not Clone since it's an exclusivity token.
pub struct Token<W, M> {
    write: W,
    meta: M,
}

impl<W, M> Token<W, M> {
    fn into_inner(self) -> (W, M) {
        (self.write, self.meta)
    }
}
