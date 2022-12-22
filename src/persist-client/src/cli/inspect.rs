// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::Description;
use prost::Message;

use mz_build_info::BuildInfo;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use serde_json::json;

use crate::fetch::EncodedPart;
use crate::internal::paths::{
    BlobKey, BlobKeyPrefix, PartialBatchKey, PartialBlobKey, PartialRollupKey,
};
use crate::internal::state::{ProtoStateDiff, ProtoStateRollup};
use crate::{Metrics, PersistConfig, ShardId, StateVersions};

const READ_ALL_BUILD_INFO: BuildInfo = BuildInfo {
    version: "10000000.0.0+test",
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
    StateRollup(StateArgs),

    /// Prints consensus rollup state of all known rollups as JSON
    StateRollups(StateArgs),

    /// Prints the count and size of blobs in an environment
    BlobCount(BlobCountArgs),

    /// Prints blob batch part contents
    BlobBatchPart(BlobBatchPartArgs),

    /// Prints the unreferenced blobs across all shards
    UnreferencedBlobs(StateArgs),

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
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state = fetch_latest_state(shard_id, &args.consensus_uri, &args.blob_uri).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state).expect("unserializable state")
            );
        }
        Command::StateRollup(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state_rollup =
                fetch_latest_state_rollup(shard_id, &args.consensus_uri, &args.blob_uri).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollup).expect("unserializable state")
            );
        }
        Command::StateRollups(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let state_rollups =
                fetch_state_rollups(shard_id, &args.consensus_uri, &args.blob_uri).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollups).expect("unserializable state")
            );
        }
        Command::StateDiff(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let states = fetch_state_diffs(shard_id, &args.consensus_uri, &args.blob_uri).await?;
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
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let unreferenced_blobs =
                unreferenced_blobs(&shard_id, &args.consensus_uri, &args.blob_uri).await?;
            println!("{}", json!(unreferenced_blobs));
        }
    }

    Ok(())
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
pub async fn fetch_latest_state(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
    )?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

    let state_versions = StateVersions::new(cfg, consensus, blob, Arc::clone(&metrics));
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;

    let state = match state_versions
        .fetch_current_state::<K, V, u64, D>(&shard_id, versions.0.clone())
        .await
    {
        Ok(s) => s.into_proto(),
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_current_state::<K, V, u64, D>(&shard_id, versions.0)
                .await
                .expect("codecs match")
                .into_proto()
        }
    };

    Ok(state)
}

/// Fetches the current state rollup of a given shard
pub async fn fetch_latest_state_rollup(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
    )?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

    if let Some(diff_buf) = consensus.head(&shard_id.to_string()).await? {
        let diff = ProtoStateDiff::decode(diff_buf.data).expect("invalid encoded diff");
        let rollup_key = PartialRollupKey(diff.latest_rollup_key);
        let rollup_buf = blob
            .get(&rollup_key.complete(&shard_id))
            .await
            .unwrap()
            .unwrap();
        let proto = ProtoStateRollup::decode(rollup_buf.as_slice()).expect("invalid encoded state");
        return Ok(proto);
    }

    Err(anyhow!("unknown shard"))
}

/// Fetches the state from all known rollups of a given shard
pub async fn fetch_state_rollups(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
    )?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

    let mut rollup_keys = HashSet::new();

    let state_versions =
        StateVersions::new(cfg, consensus, Arc::clone(&blob), Arc::clone(&metrics));
    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    while let Some(v) = state_iter.next() {
        for key in v.collections.rollups.values() {
            rollup_keys.insert(key.clone());
        }
    }

    if rollup_keys.is_empty() {
        return Err(anyhow!("unknown shard"));
    }

    let mut rollup_states = HashMap::with_capacity(rollup_keys.len());
    for key in rollup_keys {
        let rollup_buf = blob.get(&key.complete(&shard_id)).await.unwrap();
        if let Some(rollup_buf) = rollup_buf {
            let proto =
                ProtoStateRollup::decode(rollup_buf.as_slice()).expect("invalid encoded state");
            rollup_states.insert(key.to_string(), proto);
        }
    }

    Ok(rollup_states)
}

/// Fetches each state in a shard
pub async fn fetch_state_diffs(
    shard_id: ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
    )?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

    let state_versions = StateVersions::new(cfg, consensus, blob, Arc::clone(&metrics));

    let mut live_states = vec![];
    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    while let Some(v) = state_iter.next() {
        live_states.push(v.into_proto());
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
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

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

/// Arguments for viewing the blobs of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobCountArgs {
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
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

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

#[derive(Debug, Default, serde::Serialize)]
struct UnreferencedBlobs {
    batch_parts: BTreeSet<PartialBatchKey>,
    rollups: BTreeSet<PartialRollupKey>,
}

/// Fetches the unreferenced blobs for given environment
pub async fn unreferenced_blobs(
    shard_id: &ShardId,
    consensus_uri: &str,
    blob_uri: &str,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus = ConsensusConfig::try_from(
        consensus_uri,
        Box::new(cfg.clone()),
        metrics.postgres_consensus.clone(),
    )?;
    let consensus = consensus.clone().open().await?;
    let blob = BlobConfig::try_from(blob_uri).await?;
    let blob = blob.clone().open().await?;

    let mut all_parts = vec![];
    let mut all_rollups = vec![];
    let () = blob
        .list_keys_and_metadata(
            &BlobKeyPrefix::Shard(shard_id).to_string(),
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

    let state_versions = StateVersions::new(cfg, consensus, blob, Arc::clone(&metrics));
    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(shard_id)
                .await?
        }
    };

    let mut known_parts = HashSet::new();
    let mut known_rollups = HashSet::new();
    let mut known_writers = HashSet::new();
    while let Some(v) = state_iter.next() {
        for writer_id in v.collections.writers.keys() {
            known_writers.insert(writer_id.clone());
        }
        for batch in v.collections.trace.batches() {
            for batch_part in &batch.parts {
                known_parts.insert(batch_part.key.clone());
            }
        }
        for rollup in v.collections.rollups.values() {
            known_rollups.insert(rollup.clone());
        }
    }

    let mut unreferenced_blobs = UnreferencedBlobs::default();
    for (part, writer) in all_parts {
        if !known_writers.contains(&writer) && !known_parts.contains(&part) {
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

pub(crate) static KVTD_CODECS: Mutex<(String, String, String, String)> =
    Mutex::new((String::new(), String::new(), String::new(), String::new()));

impl Codec for K {
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
