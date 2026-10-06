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
use std::pin::pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BinaryArray, Int64Array, UInt64Array};
use bytes::{BufMut, Bytes};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::{StreamExt, TryStreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist::indexed::encoding::{BlobTraceBatchPart, BlobTraceUpdates};
use mz_persist::location::{SeqNo, VersionedData};
use mz_persist_types::arrow::{ArrayIdx, ArrayOrd};
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::schema::SchemaId;
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use prost::Message;
use serde_json::json;
use timely::progress::Antichain;

use crate::async_runtime::IsolatedRuntime;
use crate::cache::StateCache;
use crate::cli::args::{NO_COMMIT, READ_ALL_BUILD_INFO, StateArgs, make_blob, make_consensus};
use crate::error::CodecConcreteType;
use crate::fetch::{EncodedPart, FetchConfig};
use crate::internal::encoding::{Rollup, UntypedState};
use crate::internal::paths::{
    BlobKey, BlobKeyPrefix, PartialBatchKey, PartialBlobKey, PartialRollupKey, WriterKey,
};
use crate::internal::state::{
    BatchPart, HollowRunRef, ProtoRollup, ProtoStateDiff, RunPart, State,
};
use crate::internal::state_diff::StateDiff;
use crate::internal::state_versions::{StateVersions, UntypedStateVersionsIter};
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

    /// Checks that every blob referenced by a live state version exists, as JSON
    AuditBlobs(StateArgs),

    /// Reports updates whose accumulated diff goes negative, as JSON
    AuditMultiplicities(AuditMultiplicitiesArgs),

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
        Command::BlobUsage(args) => {
            let () = blob_usage(&args).await?;
        }
        Command::ShardStats(args) => {
            shard_stats(&args.blob_uri).await?;
        }
        Command::AuditBlobs(args) => {
            let versions = args.open().await?;
            let report = audit_blobs(&versions, args.shard_id()).await?;
            println!("{}", json!(report));
        }
        Command::AuditMultiplicities(args) => {
            let versions = args.state.open().await?;
            let report =
                audit_multiplicities(&versions, args.state.shard_id(), args.max_reported).await?;
            println!("{}", json!(report));
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
    blob_uri: SensitiveUrl,

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

/// Fetches the updates in a blob batch part
pub async fn blob_batch_part(
    blob_uri: &SensitiveUrl,
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
        &FetchConfig::from_persist_config(&cfg),
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
    let records = encoded_part
        .updates()
        .as_part()
        .ok_or_else(|| anyhow!("expected structured data"))?
        .as_ord();
    for (k, v, t, d) in records.iter() {
        if out.updates.len() > limit {
            break;
        }
        out.updates.push(BatchPartUpdate {
            k: k.to_string(),
            v: v.to_string(),
            t: u64::from_le_bytes(t),
            d: i64::from_le_bytes(d),
        });
    }

    Ok(out)
}

async fn consolidated_size(args: &StateArgs) -> Result<(), anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;
    let cfg = &state_versions.cfg;
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

    let mut parts = Vec::new();
    for batch in state.collections.trace.batches() {
        let mut part_stream =
            pin!(batch.part_stream(shard_id, &*state_versions.blob, &*state_versions.metrics));
        while let Some(part) = part_stream.try_next().await? {
            tracing::info!("fetching {}", part.printable_name());
            let encoded_part = EncodedPart::fetch(
                &FetchConfig::from_persist_config(cfg),
                &shard_id,
                &*state_versions.blob,
                &state_versions.metrics,
                &shard_metrics,
                &state_versions.metrics.read.snapshot,
                &batch.desc,
                &part,
            )
            .await
            .expect("part exists");
            let part = encoded_part.updates();
            let part = part
                .as_part()
                .ok_or_else(|| anyhow!("expected structured data"))?
                .as_ord();
            parts.push(part);
        }
    }

    let mut updates = vec![];
    for part in &parts {
        for (k, v, t, d) in part.iter() {
            let mut t = <u64 as Codec64>::decode(t);
            t.advance_by(as_of);
            let d = <i64 as Codec64>::decode(d);
            updates.push(((k, v), t, d));
        }
    }

    let bytes: usize = updates
        .iter()
        .map(|((k, v), _, _)| k.goodbytes() + v.goodbytes())
        .sum();
    println!("before: {} updates {} bytes", updates.len(), bytes);
    differential_dataflow::consolidation::consolidate_updates(&mut updates);
    let bytes: usize = updates
        .iter()
        .map(|((k, v), _, _)| k.goodbytes() + v.goodbytes())
        .sum();
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
    blob_uri: SensitiveUrl,
}

#[derive(Debug, Default, serde::Serialize)]
struct BlobCounts {
    batch_part_count: usize,
    batch_part_bytes: usize,
    rollup_count: usize,
    rollup_bytes: usize,
}

/// Fetches the blob count for given path
pub async fn blob_counts(blob_uri: &SensitiveUrl) -> Result<impl serde::Serialize, anyhow::Error> {
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
pub async fn shard_stats(blob_uri: &SensitiveUrl) -> anyhow::Result<()> {
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

    println!(
        "shard,bytes,parts,runs,batches,empty_batches,longest_run,byte_width,leased_readers,critical_readers,writers"
    );
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
            for (_meta, run) in b.runs() {
                let largest_part = run.iter().map(|p| p.max_part_bytes()).max().unwrap_or(0);
                runs += 1;
                longest_run = longest_run.max(run.len());
                byte_width += largest_part;
            }
        });
        println!(
            "{shard},{bytes},{parts},{runs},{batches},{empty_batches},{longest_run},{byte_width},{leased_readers},{critical_readers},{writers}"
        );
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
    while let Some(v) = state_iter.next(|_| {}) {
        for batch in v.collections.trace.batches() {
            // TODO: this may end up refetching externally-stored runs once per batch...
            // but if we have enough parts for this to be a problem, we may need to track a more
            // efficient state representation.
            let mut parts =
                pin!(batch.part_stream(shard_id, &*state_versions.blob, &*state_versions.metrics));
            while let Some(batch_part) = parts.next().await {
                match &*batch_part? {
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
        let is_unreferenced = writer < minimum_version;
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
    let isolated_runtime = Arc::new(IsolatedRuntime::new(&metrics_registry, None));
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

/// How many times the audits reload a shard whose blobs disappear while they read it.
const AUDIT_LOAD_ATTEMPTS: usize = 5;
const AUDIT_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// A shard's live consensus diffs, with the states at the first and last of them.
struct LiveShard {
    diffs: Vec<VersionedData>,
    earliest: UntypedState<u64>,
    current: UntypedState<u64>,
}

enum LoadedShard {
    Uninitialized,
    Live(LiveShard),
    /// A rollup referenced by the head of consensus is absent from blob.
    MissingRollup(PartialRollupKey),
}

/// Reconstructs the earliest and latest live states of a shard from a single
/// consensus scan.
///
/// Unlike [StateVersions::fetch_all_live_states], this does not retry forever
/// when a rollup is missing: it returns the missing key instead.
async fn load_live_shard(
    versions: &StateVersions,
    shard_id: ShardId,
) -> Result<LoadedShard, anyhow::Error> {
    let diffs = versions.fetch_all_live_diffs(&shard_id).await;
    let (Some(first), Some(last)) = (diffs.first(), diffs.last()) else {
        return Ok(LoadedShard::Uninitialized);
    };
    let earliest_seqno = first.seqno;
    let latest_rollup_key =
        StateDiff::<u64>::decode(&versions.cfg.build_version, last.data.clone()).latest_rollup_key;
    let Some(mut current) = versions
        .fetch_rollup_at_key::<u64>(&shard_id, &latest_rollup_key)
        .await
    else {
        return Ok(LoadedShard::MissingRollup(latest_rollup_key));
    };
    current.apply_encoded_diffs(&versions.cfg, &versions.metrics, &diffs);
    // GC removes a rollup from state only after truncating consensus past it,
    // so a consistent scan always finds the earliest live diff's rollup here.
    let earliest_rollup_key = current
        .rollups()
        .get(&earliest_seqno)
        .map(|rollup| rollup.key.clone())
        .ok_or_else(|| {
            anyhow!("head of {shard_id} has no rollup for the earliest live diff {earliest_seqno}")
        })?;
    let Some(earliest) = versions
        .fetch_rollup_at_key::<u64>(&shard_id, &earliest_rollup_key)
        .await
    else {
        return Ok(LoadedShard::MissingRollup(earliest_rollup_key));
    };
    Ok(LoadedShard::Live(LiveShard {
        diffs,
        earliest,
        current,
    }))
}

/// [load_live_shard], retried until a missing rollup is confirmed or the shard
/// loads.
///
/// A rollup is reported missing only when two consecutive loads find the same
/// key referenced by the head and absent from blob. Rollup keys are never
/// re-added once removed, so the head referenced it in between as well, and
/// GC never deletes a blob the head references. A key that a concurrent GC
/// deleted after one scan is no longer referenced by the next.
async fn load_live_shard_confirmed(
    versions: &StateVersions,
    shard_id: ShardId,
) -> Result<LoadedShard, anyhow::Error> {
    let mut previous_missing = None;
    for _ in 0..AUDIT_LOAD_ATTEMPTS {
        match load_live_shard(versions, shard_id).await? {
            LoadedShard::MissingRollup(key) => {
                if previous_missing.as_ref() == Some(&key) {
                    return Ok(LoadedShard::MissingRollup(key));
                }
                previous_missing = Some(key);
                tokio::time::sleep(AUDIT_RETRY_BACKOFF).await;
            }
            loaded => return Ok(loaded),
        }
    }
    Err(anyhow!(
        "rollups referenced by the head of {shard_id} disappeared on each of {AUDIT_LOAD_ATTEMPTS} loads"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum AuditedBlobKind {
    Part,
    Run,
    Rollup,
}

#[derive(Debug, serde::Serialize)]
struct MissingBlobReport {
    key: String,
    kind: AuditedBlobKind,
    /// The latest audited state version that references the key, if known.
    referenced_through_seqno: Option<u64>,
}

/// Output of `persistcli inspect audit-blobs`.
#[derive(Debug, Default, serde::Serialize)]
pub struct BlobAuditReport {
    shard_id: String,
    initialized: bool,
    tombstone: Option<bool>,
    since: Option<Vec<u64>>,
    upper: Option<Vec<u64>>,
    seqno: Option<u64>,
    seqno_since: Option<u64>,
    versions_audited: usize,
    referenced_parts: usize,
    referenced_runs: usize,
    referenced_rollups: usize,
    missing: Vec<MissingBlobReport>,
}

/// Checks that every batch part, hollow run, and rollup referenced by a live
/// state version at or above the shard's `seqno_since` exists in blob.
///
/// GC only deletes blobs that no version at or above `seqno_since` references,
/// so any such blob that is absent is a durability bug. GC may advance
/// `seqno_since` while the audit runs, so a key found missing is reported only
/// if a fresh load of the shard still protects a version that references it
/// and a direct `get` confirms the absence. Writes nothing and registers no
/// reader.
pub(crate) async fn audit_blobs(
    versions: &StateVersions,
    shard_id: ShardId,
) -> Result<BlobAuditReport, anyhow::Error> {
    let mut report = BlobAuditReport {
        shard_id: shard_id.to_string(),
        ..Default::default()
    };
    let live = match load_live_shard_confirmed(versions, shard_id).await? {
        LoadedShard::Uninitialized => return Ok(report),
        LoadedShard::MissingRollup(key) => {
            report.initialized = true;
            report.missing.push(MissingBlobReport {
                key: key.complete(&shard_id).to_string(),
                kind: AuditedBlobKind::Rollup,
                referenced_through_seqno: None,
            });
            return Ok(report);
        }
        LoadedShard::Live(live) => live,
    };
    report.initialized = true;

    let current = live.current.check_ts_codec(&shard_id)?;
    let seqno_since = current.seqno_since();
    report.tombstone = Some(current.collections.is_tombstone());
    report.since = Some(current.collections.trace.since().elements().to_vec());
    report.upper = Some(current.collections.trace.upper().elements().to_vec());
    report.seqno = Some(current.seqno.0);
    report.seqno_since = Some(seqno_since.0);

    // Every referenced key, with the latest version that references it.
    let mut referenced: BTreeMap<String, (AuditedBlobKind, SeqNo)> = BTreeMap::new();
    let mut runs: BTreeMap<String, (HollowRunRef<u64>, SeqNo)> = BTreeMap::new();
    let mut states = UntypedStateVersionsIter::new(
        shard_id,
        versions.cfg.clone(),
        Arc::clone(&versions.metrics),
        live.earliest,
        live.diffs,
    )
    .check_ts_codec()?;
    while let Some(state) = states.next(|_| {}) {
        if state.seqno < seqno_since {
            continue;
        }
        report.versions_audited += 1;
        for rollup in state.collections.rollups.values() {
            referenced.insert(
                rollup.key.complete(&shard_id).to_string(),
                (AuditedBlobKind::Rollup, state.seqno),
            );
        }
        for batch in state.collections.trace.batches() {
            for part in &batch.parts {
                match part {
                    RunPart::Single(BatchPart::Hollow(part)) => {
                        referenced.insert(
                            part.key.complete(&shard_id).to_string(),
                            (AuditedBlobKind::Part, state.seqno),
                        );
                    }
                    RunPart::Single(BatchPart::Inline { .. }) => {}
                    RunPart::Many(run) => {
                        runs.insert(
                            run.key.complete(&shard_id).to_string(),
                            (run.clone(), state.seqno),
                        );
                    }
                }
            }
        }
    }

    let mut candidates = Vec::new();
    let mut visited_runs = BTreeSet::new();
    let mut run_queue: Vec<_> = runs.into_iter().collect();
    while let Some((key, (run_ref, seqno))) = run_queue.pop() {
        if !visited_runs.insert(key.clone()) {
            continue;
        }
        let Some(run) = run_ref
            .get(shard_id, &*versions.blob, &versions.metrics)
            .await
        else {
            candidates.push((key, AuditedBlobKind::Run, seqno));
            continue;
        };
        for part in run.parts {
            match part {
                RunPart::Single(BatchPart::Hollow(part)) => {
                    let entry = referenced
                        .entry(part.key.complete(&shard_id).to_string())
                        .or_insert((AuditedBlobKind::Part, seqno));
                    entry.1 = std::cmp::max(entry.1, seqno);
                }
                RunPart::Single(BatchPart::Inline { .. }) => {}
                RunPart::Many(nested) => {
                    run_queue.push((nested.key.complete(&shard_id).to_string(), (nested, seqno)));
                }
            }
        }
    }
    report.referenced_runs = visited_runs.len();
    for (kind, _) in referenced.values() {
        match kind {
            AuditedBlobKind::Rollup => report.referenced_rollups += 1,
            AuditedBlobKind::Part | AuditedBlobKind::Run => report.referenced_parts += 1,
        }
    }

    let mut present = BTreeSet::new();
    versions
        .blob
        .list_keys_and_metadata(&BlobKeyPrefix::Shard(&shard_id).to_string(), &mut |m| {
            present.insert(m.key.to_owned());
        })
        .await?;
    candidates.extend(
        referenced
            .into_iter()
            .filter(|(key, _)| !present.contains(key))
            .map(|(key, (kind, seqno))| (key, kind, seqno)),
    );
    if candidates.is_empty() {
        return Ok(report);
    }

    let fresh_seqno_since = match load_live_shard_confirmed(versions, shard_id).await? {
        LoadedShard::Live(fresh) => fresh.current.check_ts_codec(&shard_id)?.seqno_since(),
        LoadedShard::MissingRollup(key) => {
            report.missing.push(MissingBlobReport {
                key: key.complete(&shard_id).to_string(),
                kind: AuditedBlobKind::Rollup,
                referenced_through_seqno: None,
            });
            seqno_since
        }
        LoadedShard::Uninitialized => {
            return Err(anyhow!(
                "{shard_id} lost all of its live diffs during the audit"
            ));
        }
    };
    for (key, kind, seqno) in candidates {
        // Versions below the fresh seqno_since are no longer protected from GC.
        if seqno < fresh_seqno_since {
            continue;
        }
        if versions.blob.get(&key).await?.is_some() {
            continue;
        }
        if report.missing.iter().any(|m| m.key == key) {
            continue;
        }
        report.missing.push(MissingBlobReport {
            key,
            kind,
            referenced_through_seqno: Some(seqno.0),
        });
    }
    Ok(report)
}

/// Arguments for `persistcli inspect audit-multiplicities`.
#[derive(Debug, Clone, clap::Parser)]
pub struct AuditMultiplicitiesArgs {
    #[clap(flatten)]
    pub(crate) state: StateArgs,

    /// Maximum number of negative accumulations to list. All of them are counted.
    #[clap(long, default_value_t = 100)]
    pub(crate) max_reported: usize,
}

#[derive(Debug, serde::Serialize)]
struct NegativeAccumulation {
    time: u64,
    /// Codec bytes, or a single-row `ProtoArrayData` for structured parts.
    key_hex: String,
    val_hex: String,
    /// Human-readable rendering, truncated.
    key: String,
    val: String,
    /// The accumulated diff of (key, val) as of `time`.
    diff: i64,
}

/// Output of `persistcli inspect audit-multiplicities`.
#[derive(Debug, Default, serde::Serialize)]
pub struct MultiplicityAuditReport {
    shard_id: String,
    initialized: bool,
    tombstone: Option<bool>,
    since: Option<Vec<u64>>,
    upper: Option<Vec<u64>>,
    seqno: Option<u64>,
    /// `structured` or `codec`: which encoding of the keys and values was compared.
    format: Option<&'static str>,
    /// Schema ids of the audited parts, `null` for parts written without one.
    /// Rows written under different schemas compare unequal, so with more than
    /// one schema a retraction can appear as a spurious negative.
    schema_ids: Vec<Option<String>>,
    checked_updates: usize,
    negative_count: usize,
    negative: Vec<NegativeAccumulation>,
}

/// Reads every update in the shard's current trace and reports each
/// (key, val, time) at which the accumulated diff of (key, val) is negative.
///
/// Keys and values are compared by their encoded bytes (or structured arrow
/// values), never decoded, so this works for any shard with `u64` timestamps
/// and `i64` diffs. Times are advanced by the since, so the check covers every
/// time in `[since, upper)`.
///
/// This registers no reader, so it never holds back the shard's since or
/// `seqno_since` and does not change compaction or GC. The cost is that GC may
/// delete a part between loading state and fetching it, after compaction
/// replaced it. Such a load is retried from fresh state, and the audit fails
/// if parts keep disappearing. `audit-blobs` is what reports a missing blob.
pub(crate) async fn audit_multiplicities(
    versions: &StateVersions,
    shard_id: ShardId,
    max_reported: usize,
) -> Result<MultiplicityAuditReport, anyhow::Error> {
    let mut report = MultiplicityAuditReport {
        shard_id: shard_id.to_string(),
        ..Default::default()
    };
    for _ in 0..AUDIT_LOAD_ATTEMPTS {
        let live = match load_live_shard_confirmed(versions, shard_id).await? {
            LoadedShard::Uninitialized => return Ok(report),
            LoadedShard::MissingRollup(key) => {
                return Err(anyhow!(
                    "rollup {key} referenced by the head of {shard_id} is missing"
                ));
            }
            LoadedShard::Live(live) => live,
        };
        report.initialized = true;
        let diff_codec = live.current.diff_codec.clone();
        if diff_codec != <i64 as Codec64>::codec_name() {
            return Err(anyhow!("{shard_id} has diff codec {diff_codec}, not i64"));
        }
        let state = live.current.check_ts_codec(&shard_id)?;
        let Some(parts) = fetch_trace_parts(versions, shard_id, &state).await? else {
            tokio::time::sleep(AUDIT_RETRY_BACKOFF).await;
            continue;
        };

        let since = state.collections.trace.since().clone();
        report.tombstone = Some(state.collections.is_tombstone());
        report.since = Some(since.elements().to_vec());
        report.upper = Some(state.collections.trace.upper().elements().to_vec());
        report.seqno = Some(state.seqno.0);
        let schema_ids: BTreeSet<_> = parts.iter().map(|(_, schema_id)| *schema_id).collect();
        report.schema_ids = schema_ids
            .into_iter()
            .map(|id| id.map(|id| id.to_string()))
            .collect();
        if since.is_empty() {
            return Ok(report);
        }
        let updates: Vec<_> = parts.into_iter().map(|(updates, _)| updates).collect();
        accumulate_opaque(&updates, &since, max_reported, &mut report)?;
        return Ok(report);
    }
    Err(anyhow!(
        "parts of {shard_id} were deleted while reading them on each of {AUDIT_LOAD_ATTEMPTS} attempts"
    ))
}

/// Fetches and normalizes every part of the state's trace, or returns `None`
/// if one of them is no longer in blob.
async fn fetch_trace_parts(
    versions: &StateVersions,
    shard_id: ShardId,
    state: &State<u64>,
) -> Result<Option<Vec<(BlobTraceUpdates, Option<SchemaId>)>>, anyhow::Error> {
    let fetch_cfg = FetchConfig::from_persist_config(&versions.cfg);
    let shard_metrics = versions.metrics.shards.shard(&shard_id, "unknown");
    let mut parts = Vec::new();
    for batch in state.collections.trace.batches() {
        let mut part_stream =
            pin!(batch.part_stream(shard_id, &*versions.blob, &*versions.metrics));
        while let Some(part) = part_stream.next().await {
            let Ok(part) = part else {
                return Ok(None);
            };
            let Ok(encoded) = EncodedPart::fetch(
                &fetch_cfg,
                &shard_id,
                &*versions.blob,
                &versions.metrics,
                &shard_metrics,
                &versions.metrics.read.snapshot,
                &batch.desc,
                &part,
            )
            .await
            else {
                return Ok(None);
            };
            // Applies batch truncation and timestamp rewrites.
            let updates = encoded.normalize(&versions.metrics.columnar);
            parts.push((updates, part.schema_id()));
        }
    }
    Ok(Some(parts))
}

/// The key or value columns of a part, in the one encoding shared by every
/// audited part.
enum OpaqueColumn {
    Codec(BinaryArray),
    Structured { raw: ArrayRef, ord: ArrayOrd },
}

impl OpaqueColumn {
    fn structured(raw: &ArrayRef) -> Self {
        OpaqueColumn::Structured {
            raw: Arc::clone(raw),
            ord: ArrayOrd::new(raw.as_ref()),
        }
    }

    fn at(&self, idx: usize) -> OpaqueDatum<'_> {
        match self {
            OpaqueColumn::Codec(array) => OpaqueDatum::Codec(array.value(idx)),
            OpaqueColumn::Structured { ord, .. } => OpaqueDatum::Structured(ord.at(idx)),
        }
    }

    /// Whether `datum` points into this column.
    fn owns(&self, datum: &OpaqueDatum<'_>) -> bool {
        match (self, datum) {
            (OpaqueColumn::Structured { ord, .. }, OpaqueDatum::Structured(idx)) => {
                std::ptr::eq(ord, idx.array)
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum OpaqueDatum<'a> {
    Codec(&'a [u8]),
    Structured(ArrayIdx<'a>),
}

struct OpaquePart {
    keys: OpaqueColumn,
    vals: OpaqueColumn,
    times: Int64Array,
    diffs: Int64Array,
}

/// Accumulates `updates` per (key, val) in time order and records every
/// negative running total into `report`.
fn accumulate_opaque(
    updates: &[BlobTraceUpdates],
    since: &Antichain<u64>,
    max_reported: usize,
    report: &mut MultiplicityAuditReport,
) -> Result<(), anyhow::Error> {
    // Codec and structured encodings of the same row compare unequal, so all
    // parts must be compared in one of them.
    let parts: Vec<OpaquePart> = if updates.iter().all(|u| u.structured().is_some()) {
        report.format = Some("structured");
        updates
            .iter()
            .map(|u| {
                let ext = u.structured().expect("checked above");
                OpaquePart {
                    keys: OpaqueColumn::structured(&ext.key),
                    vals: OpaqueColumn::structured(&ext.val),
                    times: u.timestamps().clone(),
                    diffs: u.diffs().clone(),
                }
            })
            .collect()
    } else if updates.iter().all(|u| u.records().is_some()) {
        report.format = Some("codec");
        updates
            .iter()
            .map(|u| {
                let records = u.records().expect("checked above");
                OpaquePart {
                    keys: OpaqueColumn::Codec(records.keys().clone()),
                    vals: OpaqueColumn::Codec(records.vals().clone()),
                    times: u.timestamps().clone(),
                    diffs: u.diffs().clone(),
                }
            })
            .collect()
    } else {
        return Err(anyhow!(
            "shard mixes codec-only and structured-only parts, which cannot be compared opaquely"
        ));
    };

    let mut accumulated = Vec::new();
    for part in &parts {
        for idx in 0..part.times.len() {
            let mut time = <u64 as Codec64>::decode(part.times.value(idx).to_le_bytes());
            time.advance_by(since.borrow());
            let diff = <i64 as Codec64>::decode(part.diffs.value(idx).to_le_bytes());
            accumulated.push(((part.keys.at(idx), part.vals.at(idx)), time, diff));
        }
    }
    report.checked_updates = accumulated.len();
    // Sorts by (key, val), then time.
    differential_dataflow::consolidation::consolidate_updates(&mut accumulated);

    let mut current = None;
    let mut total = 0i64;
    for ((key, val), time, diff) in &accumulated {
        if current != Some((key, val)) {
            current = Some((key, val));
            total = 0;
        }
        total += diff;
        if total >= 0 {
            continue;
        }
        report.negative_count += 1;
        if report.negative.len() < max_reported {
            report.negative.push(NegativeAccumulation {
                time: *time,
                key_hex: hex::encode(datum_bytes(&parts, |p| &p.keys, key)),
                val_hex: hex::encode(datum_bytes(&parts, |p| &p.vals, val)),
                key: truncated_display(key),
                val: truncated_display(val),
                diff: total,
            });
        }
    }
    Ok(())
}

fn datum_bytes(
    parts: &[OpaquePart],
    column: impl Fn(&OpaquePart) -> &OpaqueColumn,
    datum: &OpaqueDatum<'_>,
) -> Vec<u8> {
    match datum {
        OpaqueDatum::Codec(bytes) => bytes.to_vec(),
        OpaqueDatum::Structured(idx) => {
            let raw = parts
                .iter()
                .map(column)
                .find(|c| c.owns(datum))
                .and_then(|c| match c {
                    OpaqueColumn::Structured { raw, .. } => Some(raw),
                    OpaqueColumn::Codec(_) => None,
                })
                .expect("datum points into one of the parts");
            // `take` copies just this row, where a slice would encode the whole buffer.
            let indices = UInt64Array::from_value(u64::cast_from(idx.idx), 1);
            arrow::compute::take(raw.as_ref(), &indices, None)
                .expect("index in bounds")
                .into_data()
                .into_proto()
                .encode_to_vec()
        }
    }
}

fn truncated_display(datum: &OpaqueDatum<'_>) -> String {
    const MAX_CHARS: usize = 256;
    let rendered = match datum {
        OpaqueDatum::Codec(bytes) => String::from_utf8_lossy(bytes).into_owned(),
        OpaqueDatum::Structured(idx) => idx.to_string(),
    };
    rendered.chars().take(MAX_CHARS).collect()
}

/// The following is a very terrible hack that no one should draw inspiration from. Currently State
/// is generic over <K, V, T, D>, with KVD being represented as phantom data for type safety and to
/// detect persisted codec mismatches. However, reading persisted States does not require actually
/// decoding KVD, so we only need their codec _names_ to match, not the full types. For the purposes
/// of `persistcli inspect`, which only wants to read the persistent data, we create new types that
/// return static Codec names, and rebind the names if/when we get a CodecMismatch, so we can convince
/// the type system and our safety checks that we really can read the data.

#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct K;
#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct V;

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

    fn decode(_buf: &[u8], _schema: &TodoSchema<K>) -> Result<Self, String> {
        Ok(Self)
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        TodoSchema::default()
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

    fn decode(_buf: &[u8], _schema: &TodoSchema<V>) -> Result<Self, String> {
        Ok(Self)
    }

    fn encode_schema(_schema: &Self::Schema) -> Bytes {
        Bytes::new()
    }

    fn decode_schema(buf: &Bytes) -> Self::Schema {
        assert_eq!(*buf, Bytes::new());
        TodoSchema::default()
    }
}

pub(crate) static FAKE_OPAQUE_CODEC: Mutex<String> = Mutex::new(String::new());

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct O([u8; 8]);

impl Codec64 for O {
    fn codec_name() -> String {
        FAKE_OPAQUE_CODEC.lock().expect("lockable").clone()
    }

    fn encode(&self) -> [u8; 8] {
        self.0
    }

    fn decode(buf: [u8; 8]) -> Self {
        Self(buf)
    }
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;

    use crate::batch::{INLINE_WRITES_SINGLE_MAX_BYTES, INLINE_WRITES_TOTAL_MAX_BYTES};
    use crate::tests::new_test_client;

    use super::*;

    async fn client_without_inline_writes(dyncfgs: &ConfigUpdates) -> PersistClient {
        let client = new_test_client(dyncfgs).await;
        client.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
        client.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
        client
    }

    fn state_versions(client: &PersistClient) -> StateVersions {
        StateVersions::new(
            client.cfg.clone(),
            Arc::clone(&client.consensus),
            Arc::clone(&client.blob),
            Arc::clone(&client.metrics),
        )
    }

    async fn write_updates(
        client: &PersistClient,
        updates: &[((String, String), u64, i64)],
    ) -> ShardId {
        let shard_id = ShardId::new();
        let (mut write, _read) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;
        let upper = updates.iter().map(|(_, t, _)| *t).max().unwrap_or(0) + 1;
        write.expect_compare_and_append(updates, 0, upper).await;
        shard_id
    }

    async fn shard_keys(
        client: &PersistClient,
        shard_id: ShardId,
    ) -> Vec<(String, PartialBlobKey)> {
        let mut keys = Vec::new();
        client
            .blob
            .list_keys_and_metadata(&BlobKeyPrefix::Shard(&shard_id).to_string(), &mut |m| {
                let (_, partial) = BlobKey::parse_ids(m.key).expect("valid key");
                keys.push((m.key.to_owned(), partial));
            })
            .await
            .expect("listable");
        keys
    }

    fn kv(k: &str, v: &str) -> (String, String) {
        (k.to_owned(), v.to_owned())
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn audit_multiplicities_reports_negative_accumulations(dyncfgs: ConfigUpdates) {
        let client = client_without_inline_writes(&dyncfgs).await;
        let shard_id = write_updates(
            &client,
            &[
                (kv("balanced", "v"), 0, 1),
                (kv("balanced", "v"), 1, -1),
                (kv("early_retraction", "v"), 1, -1),
                (kv("early_retraction", "v"), 2, 1),
                (kv("positive", "v"), 2, 3),
            ],
        )
        .await;

        let report = audit_multiplicities(&state_versions(&client), shard_id, 10)
            .await
            .expect("audit succeeds");
        assert!(report.initialized);
        assert_eq!(report.checked_updates, 5);
        assert_eq!(report.negative_count, 1, "{report:?}");
        let negative = &report.negative[0];
        assert_eq!((negative.time, negative.diff), (1, -1));
        assert!(negative.key.contains("early_retraction"), "{negative:?}");
        assert!(!negative.key_hex.is_empty());
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn audit_blobs_reports_deleted_blobs(dyncfgs: ConfigUpdates) {
        let client = client_without_inline_writes(&dyncfgs).await;
        let shard_id = write_updates(&client, &[(kv("a", "1"), 0, 1), (kv("b", "2"), 1, 1)]).await;
        let versions = state_versions(&client);

        let report = audit_blobs(&versions, shard_id)
            .await
            .expect("audit succeeds");
        assert_eq!(report.tombstone, Some(false));
        assert!(report.referenced_parts > 0, "{report:?}");
        assert!(report.referenced_rollups > 0, "{report:?}");
        assert!(report.missing.is_empty(), "{report:?}");

        let mut deleted = BTreeSet::new();
        for (key, partial) in shard_keys(&client, shard_id).await {
            if let PartialBlobKey::Batch(..) = partial {
                client.blob.delete(&key).await.expect("deletable");
                deleted.insert(key);
            }
        }
        let report = audit_blobs(&versions, shard_id)
            .await
            .expect("audit succeeds");
        assert!(!report.missing.is_empty(), "{report:?}");
        for missing in &report.missing {
            assert!(deleted.contains(&missing.key), "{missing:?}");
            assert_ne!(missing.kind, AuditedBlobKind::Rollup);
        }
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn audit_blobs_reports_deleted_head_rollup(dyncfgs: ConfigUpdates) {
        let client = client_without_inline_writes(&dyncfgs).await;
        let shard_id = write_updates(&client, &[(kv("a", "1"), 0, 1)]).await;
        for (key, partial) in shard_keys(&client, shard_id).await {
            if let PartialBlobKey::Rollup(..) = partial {
                client.blob.delete(&key).await.expect("deletable");
            }
        }

        let report = audit_blobs(&state_versions(&client), shard_id)
            .await
            .expect("audit succeeds");
        assert_eq!(report.missing.len(), 1, "{report:?}");
        assert_eq!(report.missing[0].kind, AuditedBlobKind::Rollup);
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn audits_report_uninitialized_shard(dyncfgs: ConfigUpdates) {
        let client = new_test_client(&dyncfgs).await;
        let versions = state_versions(&client);
        let shard_id = ShardId::new();
        assert!(!audit_blobs(&versions, shard_id).await.unwrap().initialized);
        assert!(
            !audit_multiplicities(&versions, shard_id, 10)
                .await
                .unwrap()
                .initialized
        );
    }
}
