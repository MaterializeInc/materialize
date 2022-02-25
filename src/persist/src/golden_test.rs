// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use mz_ore::metrics::MetricsRegistry;
use mz_ore::test::init_logging;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tokio::runtime::Runtime as AsyncRuntime;
use tracing::{debug, error, info};

use crate::client::RuntimeClient;
use crate::error::{Error, ErrorLog};
use crate::mem::{MemBlob, MemRegistry};
use crate::nemesis::direct::{Direct, StartRuntime};
use crate::nemesis::generator::{Generator, GeneratorConfig};
use crate::nemesis::{Input, Runtime, RuntimeWorker};
use crate::runtime::{self, RuntimeConfig};
use crate::storage::{Atomicity, Blob, BlobRead};
use crate::unreliable::UnreliableBlob;

/// A test to catch changes in any part of the end-to-end persist encoding.
///
/// In the short-term, we are persisting only data that can safely be wiped in a
/// pinch (specifically the mz_metrics and mz_metric_histogram system tables).
/// At the very root of the metadata and data stored by persist, and the first
/// thing read on startup, is BlobMeta. It's encoded with a version
/// (BlobMeta::CURRENT_VERSION) and stored in blob storage. Our short-term
/// strategy (~1-2 months) to deal with backward compatibility is to bump this
/// version when any of the encodings used by persist change. If persist starts
/// up and encounters a BlobMeta encoding version less than its current one,
/// it's safe to simply wipe it and start fresh. (If it encounters one that's
/// greater, it should error out, because that could be from a version of
/// materialize where we _are_ persisting un-wipe-able data.)
///
/// For this to work, we need a robust way to catch changes that indicate that a
/// BlobMeta encoding version bump is needed. Code review will catch some of
/// them, and this test will catch more of them. It will have false positives,
/// which is absolutely fine (a manual verification that we don't need to bump
/// the version can be declared by updating the "golden"), but it hopefully will
/// not have any false negatives.
///
/// Reminder that the real answer here is to make persist transparently handle
/// data written by previous versions of persist. It's a solvable problem, we
/// just haven't gotten there yet. Once we do, this test will be modified to be
/// a series of tests that verify we do correctly handle old versions of persist
/// data.
///
/// Test initial setup:
/// - A persistence runtime is started working against mem storage.
/// - A deterministic series of operations are run against the runtime, one at a
///   time, waiting for each to finish before running the next to eliminate
///   concurrency. Given this workload pattern, the behavior of key external
///   elements of the persist API are deterministic (the registered streams and
///   the data in each, along with the seal and since timestamps).
/// - For the above, we already have a model of the traffic that can be run
///   against the persist API as well as a deterministic traffic generator in
///   the nemesis test. These are reused here for convenience.
/// - The persist API is stopped and the full contents of mem storage are
///   gathered (into the Blobs struct below) and serialized (using serde_json).
/// - This state is checked in to source control as the `golden_test.json` file.
///
/// Test runtime:
/// - When the test is run, it loads the state in the `golden_test.json` file
///   into mem storage. A persist runtime is started with that storage but no
///   write traffic is run against it. Instead we read the key deterministic
///   state mentioned above (into the PersistState struct).
/// - This does the bulk of the work. If any of the metadata or data formats
///   have changed in a backward compatible way, this previous step should error
///   somewhere along the way.
/// - For additional measure, we run the "initial setup" steps again, read that
///   state also into a PersistState struct and compare them. This is mostly a
///   sanity check that we don't succeed in reading the old data in an
///   uninteresting way (e.g. if we change the name of the META key), it's
///   unlikely given the current encoding library that some small detail will be
///   different between the two states.
/// - Unfortunately, this is probably the most likely source of false positives.
///   The random traffic generator is deterministic at a given commit, but we do
///   change its logic from time to time (and this will cause the states to not
///   match). The resolution is to simply update the test golden as described
///   below without the BlobMeta::CURRENT_VERSION bump.
///
/// IF THE TEST FAILS:
/// - It should only ever happen in response to changes in the persist crate. If
///   it fails otherwise (in particular if it's flaky), that's unexpected and
///   #[ignore] should be added to the test. We'll look into it async.
/// - If a failure happens in response to a persist crate change, the change
///   author determines whether any on-disk encodings changed. If any did, the
///   change author simply bumps the BlobMeta::CURRENT_VERSION and updates the
///   test golden data.
/// - If no on-disk encodings were changed (and the random traffic generator
///   wasn't changed), this is a red flag that something that should be
///   deterministic from the persist crate's external API is no longer
///   deterministic. This should be investigated.
///
/// HOW TO UPDATE "golden_test.json":
/// 1. Truncate the file so that it exists but it's empty (e.g `truncate -s 0 golden_test.json`)
/// 2. Run `cargo test golden_test`
/// 3. The test will print out the new serialization of the golden data as a log line. To update
///    the data, simply copy the printed JSON in to the golden_test.json file as part of the
///    commit. The formatting of the json shouldn't matter, but it should probably stay one line
///    for space reasons, be careful when you save if your editor auto-formats.
#[test]
fn golden() -> Result<(), Error> {
    init_logging();

    let seed = 0;
    let mut config = GeneratorConfig::default();
    // These events are for correctness testing in nemesis and don't really add
    // value to the state generation of this test. Disable them to make room for
    // more interesting events.
    config.storage_unavailable = 0;
    config.storage_available = 0;
    config.start_weight = 0;
    config.stop_weight = 0;
    config.read_output_weight = 0;
    let g = Generator::new(seed, config);
    let reqs = g.into_iter().take(100).collect::<Vec<_>>();

    let (current, raw_blobs) = current_state(&reqs)?;
    let golden = golden_state(DATAZ).map_err(|e| {
        for req in reqs.iter() {
            debug!("req {:?}", req);
        }
        info!("current impl blob state: {}", raw_blobs);
        e
    })?;

    if golden != current {
        for req in reqs {
            debug!("req {:?}", req);
        }
        info!("current impl blob state: {}", raw_blobs);
        assert_eq!(golden, current);
    }

    Ok(())
}

fn golden_state(blob_json: &str) -> Result<PersistState, Error> {
    let async_runtime = Arc::new(AsyncRuntime::new()?);
    let mut blob = MemBlob::new_no_reentrance("");
    if let Err(err) = async_runtime.block_on(Blobs::deserialize_to(blob_json, &mut blob)) {
        error!("error deserializing golden: {}", err);
    }
    let mut persist = runtime::start(
        RuntimeConfig::for_tests(),
        ErrorLog,
        blob,
        mz_build_info::DUMMY_BUILD_INFO,
        &MetricsRegistry::new(),
        Some(async_runtime),
    )?;
    let state = PersistState::slurp_from(&persist)?;
    persist.stop()?;
    Ok(state)
}

fn current_state(reqs: &[Input]) -> Result<(PersistState, String), Error> {
    #[derive(Debug)]
    struct GoldenStartRuntime(MemRegistry);

    impl StartRuntime for GoldenStartRuntime {
        fn start_runtime(
            &mut self,
            unreliable: crate::unreliable::UnreliableHandle,
        ) -> Result<RuntimeClient, Error> {
            let blob = self.0.blob_no_reentrance()?;
            let blob = UnreliableBlob::from_handle(blob, unreliable);
            runtime::start(
                RuntimeConfig::for_tests(),
                ErrorLog,
                blob,
                mz_build_info::DUMMY_BUILD_INFO,
                &MetricsRegistry::new(),
                None,
            )
        }
    }

    let async_runtime = Arc::new(AsyncRuntime::new()?);
    let reg = MemRegistry::new();
    let start_fn = GoldenStartRuntime(reg.clone());
    let mut persist = Direct::new(start_fn)?;
    let mut persist_worker = persist.add_worker();
    for req in reqs.iter() {
        let _ = persist_worker.run(req.clone()).recv();
    }
    let persist_state = PersistState::slurp_from(&persist.runtime()?)?;
    persist.finish();

    let mut blob = reg.blob_no_reentrance()?;
    let raw_blobs = Blobs::serialize_from(&blob)?;
    async_runtime.block_on(blob.close())?;
    Ok((persist_state, raw_blobs))
}

#[derive(Serialize, Deserialize)]
struct Blobs {
    blobs: Vec<(String, Vec<u8>)>,
}

impl Blobs {
    async fn deserialize_to<B: Blob>(blob_json: &str, blob: &mut B) -> Result<(), Error> {
        let blobs: Blobs =
            serde_json::from_str(blob_json).map_err(|err| Error::from(err.to_string()))?;
        for (key, val) in blobs.blobs.iter() {
            blob.set(&key, val.to_owned(), Atomicity::AllowNonAtomic)
                .await?;
        }
        Ok(())
    }

    fn serialize_from(blob: &MemBlob) -> Result<String, Error> {
        let blobs = Blobs {
            blobs: blob.all_blobs()?,
        };
        let blob_json =
            serde_json::to_string(&blobs).map_err(|err| Error::from(err.to_string()))?;
        Ok(blob_json)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct PersistState {
    streams: Vec<PersistStreamState>,
}

impl PersistState {
    fn slurp_from(persist: &RuntimeClient) -> Result<Self, Error> {
        // TODO: There is currently no way to get the set of active streams out
        // of a RuntimeClient, so hardcode the ones nemesis uses.
        let mut streams = Vec::new();
        for name in ('a'..='e').map(|x| x.to_string()) {
            let (_, read) = persist.create_or_load(&name);
            let snap = read.snapshot()?;
            let (seal, since) = (snap.get_seal(), snap.since());
            let mut snap_data = snap.into_iter().collect::<Result<Vec<_>, Error>>()?;
            differential_dataflow::consolidation::consolidate_updates(&mut snap_data);
            streams.push(PersistStreamState {
                name,
                seal,
                since,
                snap: snap_data,
            });
        }
        Ok(PersistState { streams })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct PersistStreamState {
    name: String,
    seal: Antichain<u64>,
    since: Antichain<u64>,
    snap: Vec<((String, ()), u64, i64)>,
}

const DATAZ: &'static str = include_str!("golden_test.json");
