// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Invariant-based random tests for the persist crate.
//!
//! This module generates random traffic against the external API of the persist
//! crate. It records the requests sent and responses received and afterward
//! checks this history against the API invariants.
//!
//! API traffic is modeled as a series of [Input]s. These are created by a
//! [Generator], which is totally deterministic given a seed. The [Input]s are
//! handed to an implementation of [Runtime] which executes the embedded [Req]
//! and saves the [Res] wrapped up with metadata about the execution into a
//! [Step]. These steps collectively form a history, which is handed to
//! [Validator] to check for API invariant violations (representing a bug in the
//! persist crate).
//!
//! The initial Runtime implementation is [Direct].
//!
//! Several of these components can be configured. Particularly interesting
//! combinations of these are committed as unit tests. By default, these unit
//! tests all select and use a random seed for traffic generation. This runs the
//! test randomly once and is of limited usefulness.
//!
//! ```shell
//! cargo test -p persist -- nemesis
//! ```
//!
//! The seed is printed when the test is run and override-able, so if a test
//! does fail (persist violated one of its invariants aka a bug), the previous
//! traffic can be exactly replayed. For a deterministic runtime (the "direct_*"
//! variants) this results in exactly the same execution sequence and can be
//! used to verify the bug fix.
//!
//! ```shell
//! NEMESIS_SEED=16385522461931935384 cargo test -p persist -- failing_test_name
//! ```
//!
//! If the runtime is not deterministic, the seed can still be fixed to increase
//! the likelihood of ticking the same bug again. The cargo-stress crate, which
//! runs a unit test (or set of tests) in a tight loop until failure, is useful
//! for this.
//!
//! ```shell
//! NEMESIS_SEED=16385522461931935384 cargo stress -p persist -- failing_test_name
//! ```
//!
//! Finally, the cargo-stress crate is also useful without fixing the seed to
//! search for new invariant violations. This repeatedly runs all nemesis tests,
//! each with new random traffic to maximize the chances of finding an
//! interesting history.
//!
//! ```shell
//! cargo stress -p persist -- nemesis
//! ```

// TODO
// - Variant with S3Blob
// - Impl of Runtime directly using Indexed
// - Impl of Runtime with Timely workers running in threads
// - Impl of Runtime with Timely workers running in processes
// - Storage (log/blob) with variable latency/slow requests
// - Vary key size
// - Deleting streams

use std::env;

use ore::test::init_logging;
use rand::rngs::OsRng;
use rand::RngCore;
use timely::progress::Antichain;

use crate::error::Error;
use crate::nemesis::generator::{Generator, GeneratorConfig};
use crate::nemesis::validator::Validator;

pub mod direct;
pub mod generator;
pub mod validator;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReqId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SnapshotId(u64);

/// A single logical operator to run against the persist API.
///
/// This attempts to give the ability to tickle edge cases while maintaining as
/// much determinism as possible. For example, an actual persist user would
/// receive a snapshot as an in-memory object and immediately read it. To stress
/// race conditions between snapshots and various write/compaction/etc operators
/// as well as eventually the edge cases around the still TODO disk-backed LRU
/// cache for the persisted data, snapshots are modeled here as two requests.
/// One takes a snapshot and registers it with a known id in a nemesis-specific
/// snapshot registry. The second removes this snapshot from the registry, reads
/// the contents, and returns them. This allows us to insert whatever other
/// `Input`s we like in between them.
#[derive(Clone, Debug)]
pub struct Input {
    req_id: ReqId,
    req: Req,
}

#[derive(Debug)]
pub struct Step {
    req_id: ReqId,
    res: Res,
}

#[derive(Clone, Debug)]
pub enum Req {
    Write(WriteReq),
    ReadOutput(ReadOutputReq),
    Seal(SealReq),
    AllowCompaction(AllowCompactionReq),
    TakeSnapshot(TakeSnapshotReq),
    ReadSnapshot(ReadSnapshotReq),
    Start,
    Stop,
    StorageUnavailable,
    StorageAvailable,
}

#[derive(Debug)]
pub enum Res {
    Write(WriteReq, Result<WriteRes, Error>),
    Seal(SealReq, Result<(), Error>),
    ReadOutput(ReadOutputReq, Result<ReadOutputRes, Error>),
    AllowCompaction(AllowCompactionReq, Result<(), Error>),
    TakeSnapshot(TakeSnapshotReq, Result<(), Error>),
    ReadSnapshot(ReadSnapshotReq, Result<ReadSnapshotRes, Error>),
    Start(Result<(), Error>),
    Stop(Result<(), Error>),
    StorageUnavailable,
    StorageAvailable,
}

#[derive(Clone, Debug)]
pub struct WriteReqSingle {
    stream: String,
    update: ((String, ()), u64, isize),
}

#[derive(Clone, Debug)]
pub struct WriteReqMulti {
    writes: Vec<WriteReqSingle>,
}

#[derive(Clone, Debug)]
pub enum WriteReq {
    Single(WriteReqSingle),
    Multi(WriteReqMulti),
}

#[derive(Clone, Debug)]
pub struct WriteRes {
    seqno: u64,
}

#[derive(Clone, Debug)]
pub struct ReadOutputReq {
    stream: String,
}

#[derive(Clone, Debug)]
pub enum ReadOutputEvent<D> {
    /// Records in the data stream.
    Records(Vec<D>),
    /// Progress of the data stream.
    Sealed(u64),
}

#[derive(Clone, Debug)]
pub struct ReadOutputRes {
    contents: Vec<ReadOutputEvent<(Result<(String, ()), String>, u64, isize)>>,
}

#[derive(Clone, Debug)]
pub struct SealReq {
    stream: String,
    ts: u64,
}

#[derive(Clone, Debug)]
pub struct AllowCompactionReq {
    stream: String,
    ts: u64,
}

#[derive(Clone, Debug)]
pub struct TakeSnapshotReq {
    stream: String,
    snap: SnapshotId,
}

#[derive(Clone, Debug)]
pub struct ReadSnapshotReq {
    snap: SnapshotId,
}

#[derive(Clone, Debug)]
pub struct ReadSnapshotRes {
    seqno: u64,
    since: Antichain<u64>,
    contents: Vec<((String, ()), u64, isize)>,
}

pub trait Runtime {
    fn run(&mut self, i: Input) -> Step;
    fn finish(self);
}

#[derive(Debug)]
pub struct Runner<R: Runtime> {
    generator: Generator,
    runtime: R,
    steps: Vec<Step>,
}

impl<R: Runtime> Runner<R> {
    pub fn new(generator: Generator, runtime: R) -> Self {
        Runner {
            generator,
            runtime,
            steps: Vec::new(),
        }
    }

    pub fn run(mut self, steps: usize) -> Vec<Step> {
        for input in self.generator.take(steps) {
            self.steps.push(self.runtime.run(input));
        }
        self.runtime.finish();
        self.steps
    }
}

pub fn run<R: Runtime>(steps: usize, config: GeneratorConfig, runtime: R) {
    init_logging();
    let seed =
        env::var("MZ_NEMESIS_SEED").map_or_else(|_| OsRng.next_u64(), |s| s.parse().unwrap());
    let steps = env::var("MZ_NEMESIS_STEPS").map_or(steps, |s| s.parse().unwrap());
    log::info!("MZ_NEMESIS_SEED={} MZ_NEMESIS_STEPS={}", seed, steps);
    let generator = Generator::new(seed, config);
    let runner = Runner::new(generator, runtime);
    let history = runner.run(steps);
    if let Err(errors) = Validator::validate(history) {
        for err in errors.iter() {
            log::warn!("invariant violation: {}", err)
        }
        assert!(errors.is_empty());
    }
}
