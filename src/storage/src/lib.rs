// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Storing data on disk in a compacted format.
//!
//! We want to be able to store ephemeral input data (such as TABLE inputs) on
//! persistent storage so that users don't have to replay those inputs when Materialize
//! restarts. However, we are still subject to a few constraints:
//!   1. We'd like to only assume a key-value storage API so that we can reuse
//!      code and ideas across many different storage backends. More specifically, we
//!      want our solution to only work with `get(key, blob)` and `put(key, blob)` APIs
//!      (so no random access or appending to a blob).
//!   2. We'd like to remove as much work as possible from the dataflow threads
//!      and the coordinator thread to maintain performance.
//!   3. We'd like to utilize storage space proportional to the number of distinct
//!      rows in our data over time (we don't want utilize O(N) storage space for N
//!      updates to one key for example). Also, we want to respect the compaction frontier
//!      and consolidate updates when we don't care about historical changes to data.
//!
//! Importantly, we also have one degree of freedom that most database storage engines do
//! not have. We don't need to provide efficient random access from the persisted representation,
//! and we only need to care about quickly persisting data, and being able to read it all
//! back out on restart in a reasonable amount of time.
//!
//! In order to handle these constraints, the storage code consists of two components:
//!
//! 1. A per-table write-ahead-log (WAL) which consists of statements like:
//!    (close t0) <-- indicates there will be no further updates at timestamps <= t0 in the log
//!    (row1, t1, diff1)
//!    (row1, t1, diff2)
//!    (row2, t1, diff3)
//!    (row3, t2, diff4)
//!    (close t2) <-- indicates there will be no further updates at timestamps <= t2 in the log
//!    ...
//! The WAL doesn't conform to a key-value API and has to live on either local disk (for laptops)
//! or an EBS volume on AWS. The WAL is broken up into multiple files, or segments. Each WAL
//! segment (or log segment) starts and ends with a timestamp closure (or progress) record, to indicate a
//! lower and upper bound of timestamps represented in that log segment. Periodically, when
//! a log segment file grows too large, the log writer closes it (after a progress record) and
//! marks it as finished. The log writer then starts writing to a new file. Note that the timestamp progress
//! has to be duplicated, once to close the old segment and provide an upper bound, and once to open the
//! new segment and provide a lower bound.
//!
//! The coordinator thread is the only entity that appends to the write ahead log or creates new log segment
//! files, and it does so when it determines that we need to write new data to a table or a timestamp has
//! been closed.
//!
//! 2. An asynchronous Compacter thread (really tokio task) which periodically checks each WAL for
//! finished log segments and converts them into Batches of data for the relation, and adds those
//! batches into a Trace. The compacter also then removes the WAL segment as it is no longer needed.
//!
//! A batch is meant to mimic a Differential batch or CDCv2 data record, and provide a unambiguous
//! representation of data from some range of timestamps [lower, upper). Importantly each Batch
//!   * stores all of the records from [lower, upper)
//!   * stores exactly one copy of each (row, time) pair with a nonzero diff. Taking the example above the
//!     batch from t0 to t3 would consist of the following records:
//!     (row1, t1, diff1 + diff2) <-- note that the two records containing (row1, t1) have been consolidated.
//!     (row2, t1, diff3)
//!     (row3, t2, diff4)
//!
//! Each batch stores its data in a separate file or in the future, a separate S3 object. We only keep the metadata
//! required to find the data (a path or S3 prefix + object id) in memory + a description of the timestamps the batch
//! covers.
//!
//! The Compacter keeps a Trace for each relation. A Trace is (perhaps superficially) similar to a
//! differential Trace, but in this context it has two main components:
//!   * A list of batches that together represent a contiguous interval of time. For example, if the
//!     "earliest" batch in a trace represents data from [t0, t3), then the next batch has to start from
//!     t3.
//!   * A compaction frontier. Periodically, the coordinator tells the compacter statements like "I
//!     don't care about historical detail before t_x". The compacter uses this information and when
//!     the list of batches in a trace grows too large, it combines them all into a single batch and compacts
//!     the batch by converting all (row, t) updates at t <= t_x to (row, t_x) which gives us a potential
//!     opportunity to consolidate updates that happened before the compaction frontier.
//!
//! Each trace keeps only its compaction frontier, list of batches, and the WAL path where it looks for finished
//! log segments, and a trace path (where it stores finished batches) in memory. The compacter drives traces around
//! and the coordinator tells the compacter when a relation is added or dropped, or the compaction frontier is
//! updated. Note that there's no in-memory communication between the coordinator and the compacter when theres a write
//! or a timestamp closure - those things just get written into the WAL and the compacter picks them later.
//!
//! On restart, the coordinator lists all of the files in the relation's trace directory (or S3 bucket) to
//! determine the available batches and reinitialize an in-memory trace object. Then, the coordinator reads
//! all of the persisted batches and unused WAL segments, loads all of the data into memory, and sends it
//! to be arranged in memory as a table. Finally, the coordinator sends the reinitialized trace to the
//! compacter.
//!
//! TODOs: There are at least three broad classes of todos, in no particular order:
//! 1. All of the batches and WAL segments need to check invariants more carefully and be more resilient
//!    to errors. More specifically we need to:
//!       a. Checksum all WAL writes and batches
//!       b. Check that invariants like "all of my batches form a contiguous interval of timestamps" actually
//!          hold
//!       c. Check for partial writes to the WAL or batch files on restarts and if so, remove them.
//! 2. We need to revisit the compaction algorithms to not load all of the data into memory, and perhaps also
//!    think about better compaction mechanisms. I think that storing records sorted by (row, time) (or (time, row))
//!    could make it so that we could compact things with bounded footprint linear merges?
//! 3. Actually build out support for S3.

// The batch, trace and compacter definitions live in the `compacter` module.
mod compacter;

// The WAL writer + encoding definitions live in the WAL module.
mod wal;

pub use compacter::{Compacter, CompacterMessage, Trace};
pub use wal::{Message, WriteAheadLogs};
