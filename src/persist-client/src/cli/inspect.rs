// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context};
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::indexed::columnar::parquet::{decode_trace_parquet, encode_trace_parquet};
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist::location::Blob;
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use prost::Message;
use serde::Serialize;
use serde_json::json;
use tokio::sync::mpsc;

use crate::async_runtime::IsolatedRuntime;
use crate::cache::StateCache;
use crate::cli::admin::info_log_non_zero_metrics;
use crate::cli::args::{make_blob, make_consensus, StateArgs, NO_COMMIT, READ_ALL_BUILD_INFO};
use crate::error::CodecConcreteType;
use crate::fetch::{Cursor, EncodedPart};
use crate::internal::encoding::{Rollup, UntypedState};
use crate::internal::paths::{
    BlobKey, BlobKeyPrefix, PartialBatchKey, PartialBlobKey, PartialRollupKey, WriterKey,
};
use crate::internal::state::{BatchPart, ProtoRollup, ProtoStateDiff, State};
use crate::rpc::NoopPubSubSender;
use crate::usage::{HumanBytes, StorageUsageClient};
use crate::{Metrics, PersistClient, PersistConfig, ShardId};

/// Commands for read-only inspection of persist state
#[derive(Debug, clap::Args)]
pub struct InspectArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of inspect
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Prints latest consensus state as JSON
    State(StateArgs),

    /// Prints latest consensus rollup state as JSON
    StateRollup(StateRollupArgs),

    /// Prints consensus rollup state of all known rollups as JSON
    StateRollups(StateArgs),

    /// Prints the count and size of blobs in an environment
    BlobCount(BlobArgs),

    /// Prints blob batch part contents
    BlobBatchPart(BlobBatchPartArgs),

    /// Prints consolidated and unconsolidated size, in bytes and update count
    ConsolidatedSize(StateArgs),

    /// Prints the unreferenced blobs across all shards
    UnreferencedBlobs(StateArgs),

    /// Prints various statistics about the latest rollups for all the shards in an environment
    ShardStats(BlobArgs),

    /// Prints information about blob usage for a shard
    BlobUsage(StateArgs),

    /// Checks if the shard roundtrips through arrow-rs
    ArrowRsRoundtrip(BlobArgs),

    /// Prints each consensus state change as JSON. Output includes the full consensus state
    /// before and after each state transitions:
    ///
    /// ```text
    /// {
    ///     "previous": previous_consensus_state,
    ///     "new": new_consensus_state,
    /// }
    /// ```
    ///
    /// This is most helpfully consumed using a JSON diff tool like `jd`. A useful incantation
    /// to show only the changed fields between state transitions:
    ///
    /// ```text
    /// persistcli inspect state-diff --shard-id <shard> --consensus-uri <consensus_uri> |
    ///     while read diff; do
    ///         echo $diff | jq '.new' > temp_new
    ///         echo $diff | jq '.previous' > temp_previous
    ///         echo $diff | jq '.new.seqno'
    ///         jd -color -set temp_previous temp_new
    ///     done
    /// ```
    ///
    #[clap(verbatim_doc_comment)]
    StateDiff(StateArgs),
}

/// Runs the given read-only inspect command.
pub async fn run(command: InspectArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::State(args) => {
            let state = fetch_latest_state(&args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state).expect("unserializable state")
            );
        }
        Command::StateRollup(args) => {
            let state_rollup = fetch_state_rollup(&args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollup).expect("unserializable state")
            );
        }
        Command::StateRollups(args) => {
            let state_rollups = fetch_state_rollups(&args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollups).expect("unserializable state")
            );
        }
        Command::StateDiff(args) => {
            let states = fetch_state_diffs(&args).await?;
            for window in states.windows(2) {
                println!(
                    "{}",
                    json!({
                        "previous": window[0],
                        "new": window[1]
                    })
                );
            }
        }
        Command::BlobCount(args) => {
            let blob_counts = blob_counts(&args.blob_uri).await?;
            println!("{}", json!(blob_counts));
        }
        Command::BlobBatchPart(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let updates = blob_batch_part(&args.blob_uri, shard_id, args.key, args.limit).await?;
            println!("{}", json!(updates));
        }
        Command::ConsolidatedSize(args) => {
            let () = consolidated_size(&args).await?;
        }
        Command::UnreferencedBlobs(args) => {
            let unreferenced_blobs = unreferenced_blobs(&args).await?;
            println!("{}", json!(unreferenced_blobs));
        }
        Command::ArrowRsRoundtrip(args) => {
            let report = arrow_rs_roundtrip(&args.blob_uri).await?;
            println!("{}", json!(report));
        }
        Command::BlobUsage(args) => {
            let () = blob_usage(&args).await?;
        }
        Command::ShardStats(args) => {
            shard_stats(&args.blob_uri).await?;
        }
    }

    Ok(())
}

/// Arguments for viewing the state rollup of a shard
#[derive(Debug, Clone, clap::Parser)]
pub struct StateRollupArgs {
    #[clap(flatten)]
    pub(crate) state: StateArgs,

    /// Inspect the state rollup with the given ID, if available.
    #[clap(long)]
    pub(crate) rollup_key: Option<String>,
}

/// Fetches the current state of a given shard
pub async fn fetch_latest_state(args: &StateArgs) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    let state = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0.clone())
        .await;
    Ok(Rollup::from_untyped_state_without_diffs(state).into_proto())
}

/// Fetches a state rollup of a given shard. If the seqno is not provided, choose the latest;
/// if the rollup id is not provided, discover it by inspecting state.
pub async fn fetch_state_rollup(
    args: &StateRollupArgs,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.state.shard_id();
    let state_versions = args.state.open().await?;

    let rollup_key = if let Some(rollup_key) = &args.rollup_key {
        PartialRollupKey(rollup_key.to_owned())
    } else {
        let latest_state = state_versions.consensus.head(&shard_id.to_string()).await?;
        let diff_buf = latest_state.ok_or_else(|| anyhow!("unknown shard"))?;
        let diff = ProtoStateDiff::decode(diff_buf.data).expect("invalid encoded diff");
        PartialRollupKey(diff.latest_rollup_key)
    };
    let rollup_buf = state_versions
        .blob
        .get(&rollup_key.complete(&shard_id))
        .await?
        .expect("fetching the specified state rollup");
    let proto = ProtoRollup::decode(rollup_buf).expect("invalid encoded state");
    Ok(proto)
}

/// Fetches the state from all known rollups of a given shard
pub async fn fetch_state_rollups(args: &StateArgs) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut rollup_keys = BTreeSet::new();
    let mut state_iter = state_versions
        .fetch_all_live_states::<u64>(shard_id)
        .await
        .expect("requested shard should exist")
        .check_ts_codec()?;
    while let Some(v) = state_iter.next(|_| {}) {
        for rollup in v.collections.rollups.values() {
            rollup_keys.insert(rollup.key.clone());
        }
    }

    if rollup_keys.is_empty() {
        return Err(anyhow!("unknown shard"));
    }

    let mut rollup_states = BTreeMap::new();
    for key in rollup_keys {
        let rollup_buf = state_versions
            .blob
            .get(&key.complete(&shard_id))
            .await
            .unwrap();
        if let Some(rollup_buf) = rollup_buf {
            let proto = ProtoRollup::decode(rollup_buf).expect("invalid encoded state");
            rollup_states.insert(key.to_string(), proto);
        }
    }

    Ok(rollup_states)
}

/// Fetches each state in a shard
pub async fn fetch_state_diffs(
    args: &StateArgs,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut live_states = vec![];
    let mut state_iter = state_versions
        .fetch_all_live_states::<u64>(shard_id)
        .await
        .expect("requested shard should exist")
        .check_ts_codec()?;
    while let Some(_) = state_iter.next(|_| {}) {
        live_states.push(state_iter.into_rollup_proto_without_diffs());
    }

    Ok(live_states)
}

/// Arguments for viewing contents of a batch part
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobBatchPartArgs {
    /// Shard to view
    #[clap(long)]
    shard_id: String,

    /// Blob key (without shard)
    #[clap(long)]
    key: String,

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long)]
    blob_uri: String,

    /// Number of updates to output. Default is unbounded.
    #[clap(long, default_value = "18446744073709551615")]
    limit: usize,
}

#[derive(Debug, serde::Serialize)]
struct BatchPartOutput {
    desc: Description<u64>,
    updates: Vec<BatchPartUpdate>,
}

#[derive(Debug, serde::Serialize)]
struct BatchPartUpdate {
    k: String,
    v: String,
    t: u64,
    d: i64,
}

#[derive(PartialOrd, Ord, PartialEq, Eq)]
struct PrettyBytes<'a>(&'a [u8]);

impl fmt::Debug for PrettyBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.0) {
            Ok(x) => fmt::Debug::fmt(x, f),
            Err(_) => fmt::Debug::fmt(self.0, f),
        }
    }
}

/// Fetches the updates in a blob batch part
pub async fn blob_batch_part(
    blob_uri: &str,
    shard_id: ShardId,
    partial_key: String,
    limit: usize,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(&cfg, blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;

    let key = PartialBatchKey(partial_key);
    let buf = blob
        .get(&*key.complete(&shard_id))
        .await
        .expect("blob exists")
        .expect("part exists");
    let parsed = BlobTraceBatchPart::<u64>::decode(&buf, &metrics.columnar).expect("decodable");
    let desc = parsed.desc.clone();

    let encoded_part = EncodedPart::new(
        metrics.read.snapshot.clone(),
        parsed.desc.clone(),
        &key.0,
        None,
        parsed,
    );
    let mut out = BatchPartOutput {
        desc,
        updates: Vec::new(),
    };
    let mut cursor = Cursor::default();
    while let Some((k, v, t, d)) = cursor.pop(&encoded_part) {
        if out.updates.len() > limit {
            break;
        }
        out.updates.push(BatchPartUpdate {
            k: format!("{:?}", PrettyBytes(k)),
            v: format!("{:?}", PrettyBytes(v)),
            t,
            d: i64::from_le_bytes(d),
        });
    }

    Ok(out)
}

async fn consolidated_size(args: &StateArgs) -> Result<(), anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    let state = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0.clone())
        .await;
    let state = state.check_ts_codec(&shard_id)?;
    let shard_metrics = state_versions.metrics.shards.shard(&shard_id, "unknown");
    // This is odd, but advance by the upper to get maximal consolidation.
    let as_of = state.upper().borrow();

    let mut updates = Vec::new();
    for batch in state.collections.trace.batches() {
        for part in batch.parts.iter() {
            tracing::info!("fetching {}", part.printable_name());
            let encoded_part = EncodedPart::fetch(
                &shard_id,
                &*state_versions.blob,
                &state_versions.metrics,
                &shard_metrics,
                &state_versions.metrics.read.snapshot,
                &batch.desc,
                part,
            )
            .await
            .expect("part exists");
            let mut cursor = Cursor::default();
            while let Some((k, v, mut t, d)) = cursor.pop(&encoded_part) {
                t.advance_by(as_of);
                let d = <i64 as Codec64>::decode(d);
                updates.push(((k.to_owned(), v.to_owned()), t, d));
            }
        }
    }

    let bytes: usize = updates.iter().map(|((k, v), _, _)| k.len() + v.len()).sum();
    println!("before: {} updates {} bytes", updates.len(), bytes);
    differential_dataflow::consolidation::consolidate_updates(&mut updates);
    let bytes: usize = updates.iter().map(|((k, v), _, _)| k.len() + v.len()).sum();
    println!("after : {} updates {} bytes", updates.len(), bytes);

    Ok(())
}

/// Arguments for commands that run only against the blob store.
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobArgs {
    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long)]
    blob_uri: String,
}

#[derive(Debug, Default, serde::Serialize)]
struct BlobCounts {
    batch_part_count: usize,
    batch_part_bytes: usize,
    rollup_count: usize,
    rollup_bytes: usize,
}

/// Fetches the blob count for given path
pub async fn blob_counts(blob_uri: &str) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(&cfg, blob_uri, NO_COMMIT, metrics).await?;

    let mut blob_counts = BTreeMap::new();
    let () = blob
        .list_keys_and_metadata(&BlobKeyPrefix::All.to_string(), &mut |metadata| {
            match BlobKey::parse_ids(metadata.key) {
                Ok((shard, PartialBlobKey::Batch(_, _))) => {
                    let blob_count = blob_counts.entry(shard).or_insert_with(BlobCounts::default);
                    blob_count.batch_part_count += 1;
                    blob_count.batch_part_bytes += usize::cast_from(metadata.size_in_bytes);
                }
                Ok((shard, PartialBlobKey::Rollup(_, _))) => {
                    let blob_count = blob_counts.entry(shard).or_insert_with(BlobCounts::default);
                    blob_count.rollup_count += 1;
                    blob_count.rollup_bytes += usize::cast_from(metadata.size_in_bytes);
                }
                Err(err) => {
                    eprintln!("error parsing blob: {}", err);
                }
            }
        })
        .await?;

    Ok(blob_counts)
}

/// Rummages through S3 to find the latest rollup for each shard, then calculates summary stats.
pub async fn shard_stats(blob_uri: &str) -> anyhow::Result<()> {
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(&cfg, blob_uri, NO_COMMIT, metrics).await?;

    // Collect the latest rollup for every shard with the given blob_uri
    let mut rollup_keys = BTreeMap::new();
    blob.list_keys_and_metadata(&BlobKeyPrefix::All.to_string(), &mut |metadata| {
        if let Ok((shard, PartialBlobKey::Rollup(seqno, rollup_id))) =
            BlobKey::parse_ids(metadata.key)
        {
            let key = (seqno, rollup_id);
            match rollup_keys.entry(shard) {
                Entry::Vacant(v) => {
                    v.insert(key);
                }
                Entry::Occupied(o) => {
                    if key.0 > o.get().0 {
                        *o.into_mut() = key;
                    }
                }
            };
        }
    })
    .await?;

    println!("shard,bytes,parts,runs,batches,empty_batches,longest_run,byte_width,leased_readers,critical_readers,writers");
    for (shard, (seqno, rollup)) in rollup_keys {
        let rollup_key = PartialRollupKey::new(seqno, &rollup).complete(&shard);
        // Basic stats about the trace.
        let mut bytes = 0;
        let mut parts = 0;
        let mut runs = 0;
        let mut batches = 0;
        let mut empty_batches = 0;
        let mut longest_run = 0;
        // The sum of the largest part in every run, measured in bytes.
        // A rough proxy for the worst-case amount of data we'd need to fetch to consolidate
        // down a single key-value pair.
        let mut byte_width = 0;

        let Some(rollup) = blob.get(&rollup_key).await? else {
            // Deleted between listing and now?
            continue;
        };

        let state: State<u64> =
            UntypedState::decode(&cfg.build_version, rollup).check_ts_codec(&shard)?;

        let leased_readers = state.collections.leased_readers.len();
        let critical_readers = state.collections.critical_readers.len();
        let writers = state.collections.writers.len();

        state.collections.trace.map_batches(|b| {
            bytes += b.encoded_size_bytes();
            parts += b.part_count();
            batches += 1;
            if b.is_empty() {
                empty_batches += 1;
            }
            for run in b.runs() {
                let largest_part = run
                    .iter()
                    .map(|p| p.encoded_size_bytes())
                    .max()
                    .unwrap_or(0);
                runs += 1;
                longest_run = longest_run.max(run.len());
                byte_width += largest_part;
            }
        });
        println!("{shard},{bytes},{parts},{runs},{batches},{empty_batches},{longest_run},{byte_width},{leased_readers},{critical_readers},{writers}");
    }

    Ok(())
}

#[derive(Debug, Default, serde::Serialize)]
struct UnreferencedBlobs {
    batch_parts: BTreeSet<PartialBatchKey>,
    rollups: BTreeSet<PartialRollupKey>,
}

/// Fetches the unreferenced blobs for given environment
pub async fn unreferenced_blobs(args: &StateArgs) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut all_parts = vec![];
    let mut all_rollups = vec![];
    let () = state_versions
        .blob
        .list_keys_and_metadata(
            &BlobKeyPrefix::Shard(&shard_id).to_string(),
            &mut |metadata| match BlobKey::parse_ids(metadata.key) {
                Ok((_, PartialBlobKey::Batch(writer, part))) => {
                    all_parts.push((PartialBatchKey::new(&writer, &part), writer.clone()));
                }
                Ok((_, PartialBlobKey::Rollup(seqno, rollup))) => {
                    all_rollups.push(PartialRollupKey::new(seqno, &rollup));
                }
                Err(_) => {}
            },
        )
        .await?;

    let mut state_iter = state_versions
        .fetch_all_live_states::<u64>(shard_id)
        .await
        .expect("requested shard should exist")
        .check_ts_codec()?;

    let mut known_parts = BTreeSet::new();
    let mut known_rollups = BTreeSet::new();
    let mut known_writers = BTreeSet::new();
    while let Some(v) = state_iter.next(|_| {}) {
        for writer_id in v.collections.writers.keys() {
            known_writers.insert(writer_id.clone());
        }
        for batch in v.collections.trace.batches() {
            for batch_part in &batch.parts {
                match batch_part {
                    BatchPart::Hollow(x) => known_parts.insert(x.key.clone()),
                    BatchPart::Inline { .. } => continue,
                };
            }
        }
        for rollup in v.collections.rollups.values() {
            known_rollups.insert(rollup.key.clone());
        }
    }

    let mut unreferenced_blobs = UnreferencedBlobs::default();
    // In the future, this is likely to include a "grace period" so recent but non-current
    // versions are also considered live
    let minimum_version = WriterKey::for_version(&state_versions.cfg.build_version);
    for (part, writer) in all_parts {
        let is_unreferenced = match writer {
            WriterKey::Id(writer) => !known_writers.contains(&writer),
            version @ WriterKey::Version(_) => version < minimum_version,
        };
        if is_unreferenced && !known_parts.contains(&part) {
            unreferenced_blobs.batch_parts.insert(part);
        }
    }
    for rollup in all_rollups {
        if !known_rollups.contains(&rollup) {
            unreferenced_blobs.rollups.insert(rollup);
        }
    }

    Ok(unreferenced_blobs)
}

/// Roundtrips all blobs through arrow-rs and ensures they match what is currently in S3.
pub async fn arrow_rs_roundtrip(blob_uri: &str) -> Result<impl Serialize, anyhow::Error> {
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());

    let metrics_registry = MetricsRegistry::new();
    let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));

    // Updates the 'persist_use_arrow_rs_library' feature flag.
    let update_feature_flag = |val: &str| {
        let mut updates = mz_dyncfg::ConfigUpdates::default();
        let val = mz_dyncfg::ConfigVal::String(val.to_string());
        updates.add_dynamic("persist_use_arrow_rs_library", val);
        updates.apply(&cfg.configs);
    };

    #[derive(Debug, Default, Serialize)]
    struct BlobReport {
        num_shards: usize,
        num_blobs: usize,
        missing_blobs: usize,
        num_rows: usize,
        num_bytes: usize,
    }

    /// Helper method that lists all shards in a Blob store.
    async fn list_shards(
        blob: Arc<dyn Blob + Send + Sync>,
    ) -> Result<BTreeMap<ShardId, Vec<PartialBatchKey>>, anyhow::Error> {
        let mut shards = BTreeMap::new();
        blob.list_keys_and_metadata(&BlobKeyPrefix::All.to_string(), &mut |metadata| {
            match BlobKey::parse_ids(metadata.key) {
                Ok((shard, PartialBlobKey::Batch(writer_key, part_id))) => {
                    let partial_keys = shards.entry(shard).or_insert_with(Vec::new);
                    partial_keys.push(PartialBatchKey::new(&writer_key, &part_id));
                }
                Err(err) => {
                    tracing::warn!("error parsing blob: {}", err);
                }
                _ => (),
            }
        })
        .await?;

        Ok(shards)
    }

    // Open a handle to S3.
    let blob = make_blob(&cfg, blob_uri, NO_COMMIT, Arc::clone(&metrics))
        .await
        .context("making blob")?;
    tracing::info!(?blob_uri, "opened blob");

    // List all shards.
    let shards = mz_ore::retry::Retry::default()
        .max_tries(5)
        .retry_async(|state| {
            let blob = Arc::clone(&blob);
            if state.i > 0 {
                tracing::warn!(attempt = state.i + 1, ?blob_uri, "listing shards");
            }
            async move { list_shards(Arc::clone(&blob)).await }
        })
        .await?;

    let num_parts: usize = shards.values().map(|parts| parts.len()).sum();
    tracing::info!(
        ?blob_uri,
        num_shards = shards.len(),
        num_parts,
        "listed blob"
    );

    let mut report = BlobReport::default();
    for (shard_id, partial_keys) in shards {
        // Using a bounded channel prevents us from fetching too many blobs at once.
        //
        // 128MiB max blob size * 12 = 1.5GiB.
        let (blob_tx, mut blob_rx) = mpsc::channel(12);

        // Spawn an individual task to fetch each blob.
        for partial_key in &partial_keys {
            let blob = Arc::clone(&blob);
            let blob_tx = blob_tx.clone();
            let key = partial_key.complete(&shard_id);

            mz_ore::task::spawn(|| "persistcli-fetch", async move {
                // Reserving a permit bounds the number of inflight requests.
                let permit = blob_tx.reserve().await.expect("shutting down");
                let blob_result = mz_ore::retry::Retry::default()
                    .retry_async(move |state| {
                        let blob = Arc::clone(&blob);
                        let key = key.clone();
                        if state.i > 0 {
                            tracing::warn!(attempt = state.i + 1, ?key, "fetching blob");
                        }
                        async move { blob.get(&key).await.map(|b| (b, key)) }
                    })
                    .await;
                permit.send(blob_result);
            });
        }
        // Make sure we drop our sender so the below channel will close when
        // all of our blob fetches have completed.
        drop(blob_tx);

        // Validate each blob.
        while let Some(blob_result) = blob_rx.recv().await {
            let (maybe_blob, key) = blob_result?;
            let Some(blob) = maybe_blob else {
                report.missing_blobs += 1;
                continue;
            };

            // Decode with both `arrow2` and `arrow-rs`.
            let arrow2_records: BlobTraceBatchPart<i64> = {
                update_feature_flag("off");
                decode_trace_parquet(blob.clone(), &metrics.columnar)?
            };
            let arrow_rs_records: BlobTraceBatchPart<i64> = {
                update_feature_flag("read_and_write");
                decode_trace_parquet(blob.clone(), &metrics.columnar)?
            };

            // Make sure the records match.
            if arrow2_records != arrow_rs_records {
                anyhow::bail!(
                    "arrow2 and arrow-rs return different values, {key:?} @ {blob_uri:?}"
                );
            }

            report.num_rows += arrow2_records
                .updates
                .iter()
                .map(|r| r.len())
                .sum::<usize>();
            report.num_bytes += arrow2_records
                .updates
                .iter()
                .map(|r| r.goodbytes())
                .sum::<usize>();

            // Now roundtrip through arrow-rs.
            let mut buf = Vec::new();
            {
                update_feature_flag("read_and_write");
                encode_trace_parquet(&mut buf, &arrow2_records, &metrics.columnar)?;
            }
            let buf = SegmentedBytes::from(buf);

            // Read back the arrow-rs encoded blob and make sure they also match.
            let arrow2_records_rnd: BlobTraceBatchPart<i64> = {
                update_feature_flag("off");
                decode_trace_parquet(buf.clone(), &metrics.columnar)?
            };
            let arrow_rs_records_rnd: BlobTraceBatchPart<i64> = {
                update_feature_flag("read_and_write");
                decode_trace_parquet(buf.clone(), &metrics.columnar)?
            };

            if arrow2_records_rnd != arrow_rs_records_rnd {
                anyhow::bail!("failed to roundtrip through arrow-rs, {blob_uri:?}");
            }
        }

        report.num_shards += 1;
        report.num_blobs += partial_keys.len();
        tracing::info!(
            ?blob_uri,
            ?shard_id,
            num_parts = partial_keys.len(),
            "completed one check"
        );
    }

    // Make sure our metrics indicated we did the right things.
    let metrics = metrics_registry.gather();
    let arrow_metrics = metrics
        .iter()
        .find(|family| family.get_name() == "mz_persist_arrow_operations")
        .expect("failed to get metrics");
    let get_arrow_metric = |library: &str, op: &str| {
        arrow_metrics
            .get_metric()
            .iter()
            .find(|metric| {
                let library = metric
                    .get_label()
                    .iter()
                    .find(|label| label.get_name() == "library" && label.get_value() == library)
                    .is_some();
                let op = metric
                    .get_label()
                    .iter()
                    .find(|label| label.get_name() == "op" && label.get_value() == op)
                    .is_some();

                library && op
            })
            .map(|metric| {
                let val = metric.get_counter().get_value();
                u64::cast_lossy(val)
            })
            .expect("failed to find metric")
    };

    let arrow2_encode = get_arrow_metric("arrow2", "encode");
    let arrow2_decode = get_arrow_metric("arrow2", "decode");
    let arrow_rs_encode = get_arrow_metric("arrow_rs", "encode");
    let arrow_rs_decode = get_arrow_metric("arrow_rs", "decode");

    assert_eq!(arrow2_decode, arrow_rs_decode);
    assert_eq!(arrow2_encode, 0);
    assert!(arrow_rs_encode > 0);

    // Also log any "interesting" metrics.
    info_log_non_zero_metrics(&metrics);
    tracing::info!(?blob_uri, "DONE");

    Ok(report)
}

/// Returns information about blob usage for a shard
pub async fn blob_usage(args: &StateArgs) -> Result<(), anyhow::Error> {
    let shard_id = if args.shard_id.is_empty() {
        None
    } else {
        Some(args.shard_id())
    };
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics_registry = MetricsRegistry::new();
    let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));
    let consensus =
        make_consensus(&cfg, &args.consensus_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, &args.blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
    let isolated_runtime = Arc::new(IsolatedRuntime::default());
    let state_cache = Arc::new(StateCache::new(
        &cfg,
        Arc::clone(&metrics),
        Arc::new(NoopPubSubSender),
    ));
    let usage = StorageUsageClient::open(PersistClient::new(
        cfg,
        blob,
        consensus,
        metrics,
        isolated_runtime,
        state_cache,
        Arc::new(NoopPubSubSender),
    )?);

    if let Some(shard_id) = shard_id {
        let usage = usage.shard_usage_audit(shard_id).await;
        println!("{}\n{}", shard_id, usage);
    } else {
        let usage = usage.shards_usage_audit().await;
        let mut by_shard = usage.by_shard.iter().collect::<Vec<_>>();
        by_shard.sort_by_key(|(_, x)| x.total_bytes());
        by_shard.reverse();
        for (shard_id, usage) in by_shard {
            println!("{}\n{}\n", shard_id, usage);
        }
        println!("unattributable: {}", HumanBytes(usage.unattributable_bytes));
    }

    Ok(())
}

/// The following is a very terrible hack that no one should draw inspiration from. Currently State
/// is generic over <K, V, T, D>, with KVD being represented as phantom data for type safety and to
/// detect persisted codec mismatches. However, reading persisted States does not require actually
/// decoding KVD, so we only need their codec _names_ to match, not the full types. For the purposes
/// of `persistcli inspect`, which only wants to read the persistent data, we create new types that
/// return static Codec names, and rebind the names if/when we get a CodecMismatch, so we can convince
/// the type system and our safety checks that we really can read the data.

#[derive(Debug)]
pub(crate) struct K;
#[derive(Debug)]
pub(crate) struct V;
#[derive(Debug)]
struct T;
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
struct D(i64);

pub(crate) static KVTD_CODECS: Mutex<(String, String, String, String, Option<CodecConcreteType>)> =
    Mutex::new((
        String::new(),
        String::new(),
        String::new(),
        String::new(),
        None,
    ));

impl Codec for K {
    type Storage = ();
    type Schema = TodoSchema<K>;

    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").0.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec for V {
    type Storage = ();
    type Schema = TodoSchema<V>;

    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").1.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec for T {
    type Storage = ();
    type Schema = TodoSchema<T>;

    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").2.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec64 for D {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").3.clone()
    }

    fn encode(&self) -> [u8; 8] {
        [0; 8]
    }

    fn decode(_buf: [u8; 8]) -> Self {
        Self(0)
    }
}

impl Semigroup for D {
    fn plus_equals(&mut self, _rhs: &Self) {}

    fn is_zero(&self) -> bool {
        false
    }
}
