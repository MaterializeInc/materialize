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

use anyhow::anyhow;
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::Description;
use mz_build_info::BuildInfo;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist_types::codec_impls::TodoSchema;
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use prost::Message;
use serde_json::json;

use crate::async_runtime::CpuHeavyRuntime;
use crate::cache::StateCache;
use crate::cli::admin::{make_blob, make_consensus};
use crate::error::CodecConcreteType;
use crate::fetch::EncodedPart;
use crate::internal::encoding::UntypedState;
use crate::internal::paths::{
    BlobKey, BlobKeyPrefix, PartialBatchKey, PartialBlobKey, PartialRollupKey, WriterKey,
};
use crate::internal::state::{ProtoStateDiff, ProtoStateRollup, State};
use crate::rpc::NoopPubSubSender;
use crate::usage::{HumanBytes, StorageUsageClient};
use crate::{Metrics, PersistClient, PersistConfig, ShardId, StateVersions};

// BuildInfo with a larger version than any version we expect to see in prod,
// to ensure that any data read is from a smaller version and does not trigger
// alerts.
const READ_ALL_BUILD_INFO: BuildInfo = BuildInfo {
    version: "99.999.99+test",
    sha: "0000000000000000000000000000000000000000",
    time: "",
};

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

    /// Prints the unreferenced blobs across all shards
    UnreferencedBlobs(StateArgs),

    /// Prints various statistics about the latest rollups for all the shards in an environment
    ShardStats(BlobArgs),

    /// Prints information about blob usage for a shard
    BlobUsage(StateArgs),

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

/// Arguments for viewing the current state of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct StateArgs {
    /// Shard to view
    #[clap(long)]
    pub(crate) shard_id: String,

    /// Consensus to use.
    ///
    /// When connecting to a deployed environment's consensus table, the Postgres/CRDB connection
    /// string must contain the database name and `options=--search_path=consensus`.
    ///
    /// When connecting to Cockroach Cloud, use the following format:
    ///
    /// ```text
    /// postgresql://<user>:$COCKROACH_PW@<hostname>:<port>/environment_<environment-id>
    ///   ?sslmode=verify-full
    ///   &sslrootcert=/path/to/cockroach-cloud/certs/cluster-ca.crt
    ///   &options=--search_path=consensus
    /// ```
    ///
    #[clap(long, verbatim_doc_comment, env = "CONSENSUS_URI")]
    pub(crate) consensus_uri: String,

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long, env = "BLOB_URI")]
    pub(crate) blob_uri: String,
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
        .await
        .into_proto();
    Ok(state)
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
    let proto = ProtoStateRollup::decode(rollup_buf).expect("invalid encoded state");
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
            let proto = ProtoStateRollup::decode(rollup_buf).expect("invalid encoded state");
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
        live_states.push(state_iter.into_proto());
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
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(&cfg, blob_uri, NO_COMMIT, metrics).await?;

    let key = PartialBatchKey(partial_key).complete(&shard_id);
    let part = blob
        .get(&*key)
        .await
        .expect("blob exists")
        .expect("part exists");
    let part = BlobTraceBatchPart::<u64>::decode(&part).expect("decodable");
    let desc = part.desc.clone();

    let mut encoded_part = EncodedPart::new(&*key, part.desc.clone(), part);
    let mut out = BatchPartOutput {
        desc,
        updates: Vec::new(),
    };
    while let Some((k, v, t, d)) = encoded_part.next() {
        if out.updates.len() > limit {
            break;
        }
        out.updates.push(BatchPartUpdate {
            k: format!("{:?}", PrettyBytes(k)),
            v: format!("{:?}", PrettyBytes(v)),
            t,
            d: i64::from_le_bytes(d),
        })
    }

    Ok(out)
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
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
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
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
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
            bytes += b.parts.iter().map(|p| p.encoded_size_bytes).sum::<usize>();
            parts += b.parts.len();
            batches += 1;
            if b.parts.is_empty() {
                empty_batches += 1;
            }
            for run in b.runs() {
                let largest_part = run.iter().map(|p| p.encoded_size_bytes).max().unwrap_or(0);
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
                known_parts.insert(batch_part.key.clone());
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

/// Returns information about blob usage for a shard
pub async fn blob_usage(args: &StateArgs) -> Result<(), anyhow::Error> {
    let shard_id = if args.shard_id.is_empty() {
        None
    } else {
        Some(args.shard_id())
    };
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics_registry = MetricsRegistry::new();
    let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));
    let consensus =
        make_consensus(&cfg, &args.consensus_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, &args.blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
    let cpu_heavy_runtime = Arc::new(CpuHeavyRuntime::new());
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
        cpu_heavy_runtime,
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

// All `inspect` command are read-only.
const NO_COMMIT: bool = false;

impl StateArgs {
    fn shard_id(&self) -> ShardId {
        ShardId::from_str(&self.shard_id).expect("invalid shard id")
    }

    async fn open(&self) -> Result<StateVersions, anyhow::Error> {
        let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
        let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
        let consensus =
            make_consensus(&cfg, &self.consensus_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
        let blob = make_blob(&cfg, &self.blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
        Ok(StateVersions::new(cfg, consensus, blob, metrics))
    }
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
