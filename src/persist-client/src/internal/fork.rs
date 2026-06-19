// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shard-fork primitive used by schema branching.
//!
//! Forking a shard at a `branch_ts` is a four-step operation:
//!
//! 1. Snapshot the source shard's `Trace` at `branch_ts`: collect every
//!    `HollowBatch` whose `lower <= branch_ts`. This includes straddling
//!    batches whose `upper > branch_ts`.
//! 2. Rewrite each batch part's `PartialBatchKey` from `Relative(s)` to
//!    `Absolute("<source_shard>/<s>")` so the fork shard's manifest
//!    references the source's blobs directly. Inline parts are left as-is.
//! 3. Stamp every inherited batch with `cutoff_ts = branch_ts`. On read,
//!    persist drops updates from inherited batches whose original time is
//!    strictly greater than the cutoff.
//! 4. Allocate a fresh `ShardId` and write its initial state to consensus
//!    via [`Machine::initialize_from_snapshot`], with
//!    `upper = branch_ts + 1` and `since = T::minimum()`.

use std::sync::Arc;

use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use mz_ore::soft_panic_or_log;
use mz_persist_types::{Codec, Codec64};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

use crate::ShardId;
use crate::async_runtime::IsolatedRuntime;
use crate::cache::StateCache;
use crate::cfg::PersistConfig;
use crate::error::CodecMismatch;
use crate::internal::machine::{InitializeFromSnapshotError, Machine};
use crate::internal::metrics::Metrics;
use crate::internal::paths::PartialBatchKey;
use differential_dataflow::trace::Description;

use crate::internal::state::{BatchPart, HollowBatch, RunPart};
use crate::internal::state_versions::StateVersions;
use crate::rpc::PubSubSender;
use crate::{Diagnostics, Schemas};

/// Failure modes for [`fork_shard`].
#[derive(Debug)]
pub enum ForkShardError {
    /// The source shard cannot be opened with the supplied codecs.
    CodecMismatch(Box<CodecMismatch>),
    /// Installing the fork shard's initial state in consensus failed.
    InstallFailed(String),
    /// The supplied `branch_ts` is at or after the source shard's upper, so
    /// the requested snapshot is not yet available.
    BranchTsNotYetAvailable {
        branch_ts: String,
        source_upper: String,
    },
}

impl std::fmt::Display for ForkShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ForkShardError::CodecMismatch(err) => write!(f, "codec mismatch on source: {err}"),
            ForkShardError::InstallFailed(msg) => write!(f, "fork shard install failed: {msg}"),
            ForkShardError::BranchTsNotYetAvailable {
                branch_ts,
                source_upper,
            } => write!(
                f,
                "branch_ts {branch_ts} is not yet available on source shard (upper={source_upper})",
            ),
        }
    }
}

impl std::error::Error for ForkShardError {}

impl From<InitializeFromSnapshotError> for ForkShardError {
    fn from(value: InitializeFromSnapshotError) -> Self {
        match value {
            InitializeFromSnapshotError::AlreadyInitialized => {
                ForkShardError::InstallFailed("fork shard id collided with an existing shard".into())
            }
            InitializeFromSnapshotError::InvalidArgs(msg) => ForkShardError::InstallFailed(msg),
            InitializeFromSnapshotError::CodecMismatch(err) => ForkShardError::CodecMismatch(err),
        }
    }
}

/// Successful result of [`fork_shard`]: the new shard's id and the set of
/// absolute blob keys its initial manifest references. The caller bulk-inserts
/// one `fork_blob_refs` row per blob key to pin them against GC.
#[derive(Debug, Clone)]
pub struct ForkShardOutput {
    pub fork_shard_id: ShardId,
    pub absolute_blob_keys: Vec<String>,
}

/// Fork `source_shard` at `branch_ts`. See the module-level doc for the
/// step-by-step. Callers must hold `branch_ts < min(source_shard.upper)` so
/// the snapshot is observable.
pub async fn fork_shard<K, V, T, D>(
    cfg: PersistConfig,
    source_shard: ShardId,
    branch_ts: T,
    metrics: Arc<Metrics>,
    state_versions: Arc<StateVersions>,
    shared_states: Arc<StateCache>,
    pubsub_sender: Arc<dyn PubSubSender>,
    isolated_runtime: Arc<IsolatedRuntime>,
    diagnostics: Diagnostics,
    schemas: Schemas<K, V>,
) -> Result<ForkShardOutput, ForkShardError>
where
    K: std::fmt::Debug + Codec,
    V: std::fmt::Debug + Codec,
    T: Timestamp + Lattice + Codec64 + Sync + Ord,
    D: Monoid + Codec64 + Send + Sync,
{
    let _ = schemas;
    // Open the source shard so we can read its trace.
    let source_machine = Machine::<K, V, T, D>::new(
        cfg.clone(),
        source_shard,
        Arc::clone(&metrics),
        Arc::clone(&state_versions),
        Arc::clone(&shared_states),
        Arc::clone(&pubsub_sender),
        Arc::clone(&isolated_runtime),
        diagnostics.clone(),
    )
    .await
    .map_err(ForkShardError::CodecMismatch)?;

    let source_upper = source_machine.applier.clone_upper();
    if !is_branch_ts_observable(&branch_ts, &source_upper) {
        return Err(ForkShardError::BranchTsNotYetAvailable {
            branch_ts: format!("{branch_ts:?}"),
            source_upper: format!("{:?}", source_upper.elements()),
        });
    }

    // Snapshot the trace: every batch with `lower <= branch_ts`, including
    // those that straddle the cutoff (lower <= branch_ts < upper).
    let mut source_batches: Vec<HollowBatch<T>> = source_machine.applier.all_batches();
    source_batches.retain(|b| {
        b.desc.lower().less_equal(&branch_ts)
    });

    // Rewrite each batch's part keys to absolute and stamp the cutoff. Track
    // the absolute blob keys so the caller can pin them against GC.
    //
    // Each batch also has its `since` reset to `T::minimum()`. The source
    // shard may have compacted past its initial since (e.g. shard.since
    // is at branch_ts - delta), and its batches carry that since. But the
    // fork shard starts fresh with `since = T::minimum()`, and pushing a
    // batch whose since is greater than the spine's since panics. Resetting
    // to minimum makes the batches compatible with the new spine; readers
    // get the same results because compaction since is a "no updates
    // before this ts" hint, not a data filter.
    let minimum_since: Antichain<T> = Antichain::from_elem(T::minimum());
    let mut absolute_blob_keys = Vec::new();
    let mut fork_batches = Vec::with_capacity(source_batches.len());
    for mut batch in source_batches {
        rewrite_parts_to_absolute(&mut batch, source_shard, &mut absolute_blob_keys);
        batch.cutoff_ts = Some(branch_ts.clone());
        batch.desc = Description::new(
            batch.desc.lower().clone(),
            batch.desc.upper().clone(),
            minimum_since.clone(),
        );
        fork_batches.push(batch);
    }

    // The fork shard's upper is the join over the inherited batches'
    // uppers. `initialize_from_snapshot` insists the supplied `upper`
    // matches what the trace ends up at after pushing those batches.
    let upper = fork_batches
        .iter()
        .map(|b| b.desc.upper().clone())
        .reduce(|a, b| a.join(&b))
        .unwrap_or_else(|| Antichain::from_elem(T::minimum()));

    // Allocate a fresh shard id and install the initial state.
    let fork_shard_id = ShardId::new();
    let _fork_machine = Machine::<K, V, T, D>::initialize_from_snapshot(
        cfg,
        fork_shard_id,
        fork_batches,
        upper,
        metrics,
        state_versions,
        shared_states,
        pubsub_sender,
        isolated_runtime,
        diagnostics,
    )
    .await?;

    Ok(ForkShardOutput {
        fork_shard_id,
        absolute_blob_keys,
    })
}

fn is_branch_ts_observable<T: Timestamp>(branch_ts: &T, upper: &Antichain<T>) -> bool {
    // `branch_ts` is observable if every element of `upper` is strictly
    // greater. With a single-element antichain (totally ordered T), the
    // check collapses to `branch_ts < upper`.
    upper.elements().iter().all(|u| branch_ts.less_than(u))
}

fn rewrite_parts_to_absolute<T: Timestamp + Codec64>(
    batch: &mut HollowBatch<T>,
    source_shard: ShardId,
    sink: &mut Vec<String>,
) {
    let shard_str = source_shard.to_string();
    for run_part in batch.parts.iter_mut() {
        rewrite_run_part(run_part, &shard_str, sink);
    }
}

fn rewrite_run_part<T: Timestamp + Codec64>(
    part: &mut RunPart<T>,
    shard_str: &str,
    sink: &mut Vec<String>,
) {
    match part {
        RunPart::Single(BatchPart::Hollow(hollow)) => {
            let absolute = absolute_for(&hollow.key, shard_str);
            sink.push(absolute.clone());
            hollow.key = PartialBatchKey::Absolute(absolute);
        }
        RunPart::Single(BatchPart::Inline { .. }) => {
            // Inline parts carry their bytes directly and don't reference
            // a blob; nothing to rewrite.
        }
        RunPart::Many(run_ref) => {
            let absolute = absolute_for(&run_ref.key, shard_str);
            sink.push(absolute.clone());
            run_ref.key = PartialBatchKey::Absolute(absolute);
            // The contents of the hollow run live in a blob that's pinned
            // by the key we just rewrote. Reading the run later resolves
            // through the absolute key.
        }
    }
}

fn absolute_for(key: &PartialBatchKey, shard_str: &str) -> String {
    match key {
        PartialBatchKey::Relative(s) => format!("{shard_str}/{s}"),
        PartialBatchKey::Absolute(s) => {
            // Already absolute (e.g., a sub-branch's fork shard referencing
            // a parent fork shard's blobs); keep it as-is so the chain
            // resolves to the original owner.
            soft_panic_or_log!(
                "fork_shard saw an already-absolute key on the source shard's manifest: {s}"
            );
            warn!("absolute key on source shard: {s}");
            s.clone()
        }
    }
}
