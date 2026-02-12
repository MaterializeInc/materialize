// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI tools for exporting and importing persist shard data.
//!
//! Export copies a shard's blob parts and state metadata to a local directory.
//! Import uploads those blob parts to a new shard and reconstructs the shard
//! state.

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use bytes::Bytes;
use futures_util::stream::{self, StreamExt};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::url::SensitiveUrl;
use mz_persist_types::codec_impls::TodoSchema;
use mz_proto::RustType;
use prost::Message;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::async_runtime::IsolatedRuntime;
use crate::batch::ProtoBatch;
use crate::cache::StateCache;
use crate::cfg::PersistConfig;
use crate::cli::args::{READ_ALL_BUILD_INFO, StateArgs, make_blob, make_consensus};
use crate::internal::paths::PartialBatchKey;
use crate::internal::state::{BatchPart, HollowBatch, ProtoHollowBatch, RunPart};
use crate::rpc::{NoopPubSubSender, PubSubSender};
use crate::write::WriteHandle;
use crate::{Diagnostics, Metrics, PersistClient, ShardId};

/// Commands for exporting and importing persist shard data.
#[derive(Debug, clap::Args)]
pub struct StashArgs {
    #[clap(subcommand)]
    command: StashCommand,
}

#[derive(Debug, clap::Subcommand)]
enum StashCommand {
    /// Export a shard's data to a local directory.
    Export(ExportArgs),
    /// Import previously exported shard data into a new shard.
    Import(ImportArgs),
}

/// Arguments for exporting a shard.
#[derive(Debug, clap::Parser)]
struct ExportArgs {
    #[clap(flatten)]
    state: StateArgs,

    /// Local directory to write the export.
    #[clap(long)]
    output_dir: PathBuf,

    /// Number of concurrent blob downloads.
    #[clap(long, default_value_t = 16)]
    concurrency: usize,
}

/// Arguments for importing a shard.
#[derive(Debug, clap::Parser)]
struct ImportArgs {
    /// Target shard ID (must be a fresh/unused shard).
    #[clap(long)]
    shard_id: String,

    /// Consensus URI for the target environment.
    #[clap(long, env = "CONSENSUS_URI")]
    consensus_uri: SensitiveUrl,

    /// Blob URI for the target environment.
    #[clap(long, env = "BLOB_URI")]
    blob_uri: SensitiveUrl,

    /// Local directory containing exported data.
    #[clap(long)]
    input_dir: PathBuf,

    /// Number of concurrent blob uploads.
    #[clap(long, default_value_t = 16)]
    concurrency: usize,

    /// Replay data at the original ingestion pace. Batch timestamps (assumed
    /// to be epoch milliseconds) are used to compute delays between appends,
    /// so data arrives in the target shard at the same cadence it was
    /// originally written.
    #[clap(long)]
    timed: bool,

    /// Speed multiplier for timed replay. 1.0 = real-time, 2.0 = 2x faster,
    /// 0.5 = half speed. Only meaningful with --timed.
    #[clap(long, default_value_t = 1.0)]
    speed: f64,

    /// Only import batches whose lower timestamp is below this value (epoch ms).
    /// Batches at or above this timestamp are skipped.
    #[clap(long)]
    cutoff_ts: Option<u64>,

    /// Only import batches whose lower timestamp is at or above this value (epoch ms).
    /// Batches below this timestamp are skipped.
    #[clap(long)]
    min_ts: Option<u64>,
}

// --- Manifest types ---

#[derive(Debug, Serialize, Deserialize)]
struct ExportManifest {
    manifest_version: String,
    source_shard_id: String,
    persist_version: String,
    key_codec: String,
    val_codec: String,
    ts_codec: String,
    diff_codec: String,
    batches: Vec<ExportBatchEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExportBatchEntry {
    lower: Vec<u64>,
    upper: Vec<u64>,
    since: Vec<u64>,
    len: usize,
    proto_batch_file: String,
    parts: Vec<ExportPartEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ExportPartEntry {
    kind: String,
    original_partial_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    local_filename: Option<String>,
    encoded_size_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    contained_parts: Option<Vec<ExportPartEntry>>,
}

/// Runs the given stash command.
pub async fn run(command: StashArgs) -> Result<(), anyhow::Error> {
    match command.command {
        StashCommand::Export(args) => run_export(args).await,
        StashCommand::Import(args) => run_import(args).await,
    }
}

/// Sanitize a partial key for use as a local filename.
fn sanitize_key(key: &str) -> String {
    key.replace('/', "_").replace('\\', "_")
}

/// Export a shard's blob parts and state metadata to a local directory.
async fn run_export(args: ExportArgs) -> Result<(), anyhow::Error> {
    let shard_id = args.state.shard_id();

    eprintln!(
        "stash export: shard={} consensus={} blob={}",
        shard_id, args.state.consensus_uri, args.state.blob_uri,
    );
    eprintln!("stash export: opening consensus and blob connections...");
    let state_versions = args.state.open().await?;
    let blob = Arc::clone(&state_versions.blob);
    let metrics = Arc::clone(&state_versions.metrics);

    // Fetch state. First get codec names from UntypedState, then verify ts codec.
    eprintln!("stash export: fetching live diffs from consensus...");
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;

    eprintln!("stash export: fetching current state (rollup from blob)...");
    let untyped = state_versions
        .fetch_current_state::<u64>(&shard_id, versions.0.clone())
        .await;
    let key_codec = untyped.key_codec.clone();
    let val_codec = untyped.val_codec.clone();
    let ts_codec = untyped.ts_codec.clone();
    let diff_codec = untyped.diff_codec.clone();

    let state = match untyped.check_ts_codec(&shard_id) {
        Ok(state) => state,
        Err(codec) => {
            bail!(
                "shard ts codec mismatch: expected u64, got {}",
                codec.actual
            );
        }
    };

    // Create output directories.
    let blobs_dir = args.output_dir.join("blobs");
    let protos_dir = args.output_dir.join("protos");
    tokio::fs::create_dir_all(&blobs_dir).await?;
    tokio::fs::create_dir_all(&protos_dir).await?;

    let mut manifest_batches = Vec::new();

    for (batch_idx, batch) in state.collections.trace.batches().enumerate() {
        info!(
            "exporting batch {}: lower={:?} upper={:?} since={:?} parts={}",
            batch_idx,
            batch.desc.lower().elements(),
            batch.desc.upper().elements(),
            batch.desc.since().elements(),
            batch.parts.len(),
        );

        // Serialize the HollowBatch as a protobuf.
        let proto_batch: crate::internal::state::ProtoHollowBatch = batch.into_proto();
        let proto_bytes = proto_batch.encode_to_vec();
        let proto_file = format!("protos/batch_{}.binpb", batch_idx);
        tokio::fs::write(args.output_dir.join(&proto_file), &proto_bytes).await?;

        // Download blob parts.
        let mut manifest_parts = Vec::new();
        let parts_to_download = collect_parts_for_export(batch, shard_id, &blob, &metrics).await?;

        // Download blobs concurrently.
        let download_results: Vec<Result<ExportPartEntry, anyhow::Error>> =
            stream::iter(parts_to_download)
                .map(|part_info| {
                    let blob = Arc::clone(&blob);
                    let shard_id = shard_id;
                    let blobs_dir = blobs_dir.clone();
                    async move { download_part(&blob, shard_id, &blobs_dir, part_info).await }
                })
                .buffer_unordered(args.concurrency)
                .collect()
                .await;

        for result in download_results {
            manifest_parts.push(result?);
        }

        manifest_batches.push(ExportBatchEntry {
            lower: batch.desc.lower().elements().to_vec(),
            upper: batch.desc.upper().elements().to_vec(),
            since: batch.desc.since().elements().to_vec(),
            len: batch.len,
            proto_batch_file: proto_file,
            parts: manifest_parts,
        });
    }

    let manifest = ExportManifest {
        manifest_version: "1.0.0".to_string(),
        source_shard_id: shard_id.to_string(),
        persist_version: READ_ALL_BUILD_INFO.version.to_string(),
        key_codec,
        val_codec,
        ts_codec,
        diff_codec,
        batches: manifest_batches,
    };

    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    tokio::fs::write(args.output_dir.join("manifest.json"), manifest_json).await?;

    info!(
        "export complete: {} batches written to {}",
        manifest.batches.len(),
        args.output_dir.display()
    );

    Ok(())
}

/// Information needed to download a single part.
struct PartDownloadInfo {
    kind: String,
    original_partial_key: String,
    encoded_size_bytes: usize,
    /// For `hollow_run` parts, the contained parts to download.
    contained: Option<Vec<PartDownloadInfo>>,
}

/// Collect all parts from a batch that need to be downloaded.
async fn collect_parts_for_export(
    batch: &HollowBatch<u64>,
    shard_id: ShardId,
    blob: &Arc<dyn mz_persist::location::Blob>,
    metrics: &Metrics,
) -> Result<Vec<PartDownloadInfo>, anyhow::Error> {
    let mut parts = Vec::new();
    for run_part in &batch.parts {
        match run_part {
            RunPart::Single(BatchPart::Hollow(part)) => {
                parts.push(PartDownloadInfo {
                    kind: "hollow".to_string(),
                    original_partial_key: part.key.0.clone(),
                    encoded_size_bytes: part.encoded_size_bytes,
                    contained: None,
                });
            }
            RunPart::Single(BatchPart::Inline { updates, .. }) => {
                parts.push(PartDownloadInfo {
                    kind: "inline".to_string(),
                    original_partial_key: String::new(),
                    encoded_size_bytes: updates.encoded_size_bytes(),
                    contained: None,
                });
            }
            RunPart::Many(run_ref) => {
                // For hollow runs, we need to download the run blob itself,
                // then discover and download contained parts.
                let mut contained = Vec::new();
                if let Some(hollow_run) = run_ref.get(shard_id, blob.as_ref(), metrics).await {
                    for inner_part in &hollow_run.parts {
                        match inner_part {
                            RunPart::Single(BatchPart::Hollow(part)) => {
                                contained.push(PartDownloadInfo {
                                    kind: "hollow".to_string(),
                                    original_partial_key: part.key.0.clone(),
                                    encoded_size_bytes: part.encoded_size_bytes,
                                    contained: None,
                                });
                            }
                            RunPart::Single(BatchPart::Inline { updates, .. }) => {
                                contained.push(PartDownloadInfo {
                                    kind: "inline".to_string(),
                                    original_partial_key: String::new(),
                                    encoded_size_bytes: updates.encoded_size_bytes(),
                                    contained: None,
                                });
                            }
                            RunPart::Many(_) => {
                                // Nested runs - shouldn't normally happen but handle gracefully.
                                info!("warning: nested hollow run found, skipping inner run");
                            }
                        }
                    }
                }
                parts.push(PartDownloadInfo {
                    kind: "hollow_run".to_string(),
                    original_partial_key: run_ref.key.0.clone(),
                    encoded_size_bytes: run_ref.hollow_bytes,
                    contained: Some(contained),
                });
            }
        }
    }
    Ok(parts)
}

/// Download a single part's blob data and return its manifest entry.
async fn download_part(
    blob: &Arc<dyn mz_persist::location::Blob>,
    shard_id: ShardId,
    blobs_dir: &std::path::Path,
    info: PartDownloadInfo,
) -> Result<ExportPartEntry, anyhow::Error> {
    match info.kind.as_str() {
        "inline" => Ok(ExportPartEntry {
            kind: "inline".to_string(),
            original_partial_key: String::new(),
            local_filename: None,
            encoded_size_bytes: info.encoded_size_bytes,
            contained_parts: None,
        }),
        "hollow" => {
            let partial_key = PartialBatchKey(info.original_partial_key.clone());
            let blob_key = partial_key.complete(&shard_id);
            let local_filename = format!("{}.blob", sanitize_key(&info.original_partial_key));

            if let Some(data) = blob.get(&blob_key).await? {
                let bytes: Vec<u8> = data.into_contiguous();
                tokio::fs::write(blobs_dir.join(&local_filename), &bytes).await?;
            } else {
                info!(
                    "warning: blob not found for key {}, skipping",
                    info.original_partial_key
                );
            }

            Ok(ExportPartEntry {
                kind: "hollow".to_string(),
                original_partial_key: info.original_partial_key,
                local_filename: Some(local_filename),
                encoded_size_bytes: info.encoded_size_bytes,
                contained_parts: None,
            })
        }
        "hollow_run" => {
            // Download the run blob itself.
            let partial_key = PartialBatchKey(info.original_partial_key.clone());
            let blob_key = partial_key.complete(&shard_id);
            let local_filename = format!("{}.blob", sanitize_key(&info.original_partial_key));

            if let Some(data) = blob.get(&blob_key).await? {
                let bytes: Vec<u8> = data.into_contiguous();
                tokio::fs::write(blobs_dir.join(&local_filename), &bytes).await?;
            }

            // Download contained parts.
            let mut contained_entries = Vec::new();
            if let Some(contained) = info.contained {
                for child in contained {
                    let entry = Box::pin(download_part(blob, shard_id, blobs_dir, child)).await?;
                    contained_entries.push(entry);
                }
            }

            Ok(ExportPartEntry {
                kind: "hollow_run".to_string(),
                original_partial_key: info.original_partial_key,
                local_filename: Some(local_filename),
                encoded_size_bytes: info.encoded_size_bytes,
                contained_parts: Some(contained_entries),
            })
        }
        other => bail!("unknown part kind: {}", other),
    }
}

/// Import previously exported shard data into a new shard.
async fn run_import(args: ImportArgs) -> Result<(), anyhow::Error> {
    let target_shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");

    // Read manifest.
    let manifest_path = args.input_dir.join("manifest.json");
    let manifest_json = tokio::fs::read_to_string(&manifest_path).await?;
    let manifest: ExportManifest = serde_json::from_str(&manifest_json)?;

    info!(
        "importing {} batches from shard {} into {}",
        manifest.batches.len(),
        manifest.source_shard_id,
        target_shard_id
    );

    // Set KVTD codecs so the type system is satisfied.
    {
        let mut kvtd = crate::cli::inspect::KVTD_CODECS.lock().expect("lockable");
        *kvtd = (
            manifest.key_codec.clone(),
            manifest.val_codec.clone(),
            manifest.ts_codec.clone(),
            manifest.diff_codec.clone(),
            None,
        );
    }

    // Use the real BUILD_INFO (not READ_ALL_BUILD_INFO) for import since it
    // writes data. READ_ALL_BUILD_INFO uses version 99.999.99+test which is
    // designed for read-only inspection and causes version mismatches when
    // materialized later reads the imported batches.
    let cfg = PersistConfig::new_default_configs(&crate::BUILD_INFO, SYSTEM_TIME.clone());
    let metrics_registry = MetricsRegistry::new();
    let metrics = Arc::new(Metrics::new(&cfg, &metrics_registry));

    // Open blob + consensus with commit = true.
    let consensus = make_consensus(&cfg, &args.consensus_uri, true, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, &args.blob_uri, true, Arc::clone(&metrics)).await?;

    let isolated_runtime = Arc::new(IsolatedRuntime::new(&metrics_registry, None));
    let pubsub_sender: Arc<dyn PubSubSender> = Arc::new(NoopPubSubSender);
    let shared_states = Arc::new(StateCache::new(
        &cfg,
        Arc::clone(&metrics),
        Arc::clone(&pubsub_sender),
    ));

    let persist_client = PersistClient::new(
        cfg.clone(),
        Arc::clone(&blob),
        Arc::clone(&consensus),
        Arc::clone(&metrics),
        isolated_runtime,
        shared_states,
        pubsub_sender,
    )?;

    let diagnostics = Diagnostics {
        shard_name: target_shard_id.to_string(),
        handle_purpose: "persistcli stash import".to_string(),
    };

    let mut write_handle: WriteHandle<crate::cli::inspect::K, crate::cli::inspect::V, u64, i64> =
        persist_client
            .open_writer(
                target_shard_id,
                Arc::new(TodoSchema::<crate::cli::inspect::K>::default()),
                Arc::new(TodoSchema::<crate::cli::inspect::V>::default()),
                diagnostics,
            )
            .await?;

    let blobs_dir = args.input_dir.join("blobs");
    let timed = args.timed;
    let speed = if args.speed > 0.0 { args.speed } else { 1.0 };

    // For timed replay: determine the first batch's lower timestamp as the
    // logical "start", and record the wall-clock start time.
    let first_lower_ts: Option<u64> = if timed {
        manifest
            .batches
            .first()
            .and_then(|b| b.lower.first().copied())
    } else {
        None
    };
    let wall_start = Instant::now();

    for (batch_idx, batch_entry) in manifest.batches.iter().enumerate() {
        // Filter by timestamp range.
        if let Some(&batch_ts) = batch_entry.lower.first() {
            if let Some(cutoff) = args.cutoff_ts {
                if batch_ts >= cutoff {
                    continue;
                }
            }
            if let Some(min) = args.min_ts {
                if batch_ts < min {
                    continue;
                }
            }
        }

        // Timed replay: sleep until the wall-clock time corresponding to
        // this batch's lower timestamp.
        if let Some(origin_ts) = first_lower_ts {
            if let Some(&batch_ts) = batch_entry.lower.first() {
                let logical_offset_ms = batch_ts.saturating_sub(origin_ts);
                let target_wall_offset =
                    Duration::from_millis((logical_offset_ms as f64 / speed) as u64);
                let elapsed = wall_start.elapsed();
                if target_wall_offset > elapsed {
                    let sleep_dur = target_wall_offset - elapsed;
                    info!(
                        "timed replay: sleeping {:.2}s before batch {} (ts {})",
                        sleep_dur.as_secs_f64(),
                        batch_idx,
                        batch_ts,
                    );
                    tokio::time::sleep(sleep_dur).await;
                }
            }
        }

        info!(
            "importing batch {}/{}: lower={:?} upper={:?}",
            batch_idx + 1,
            manifest.batches.len(),
            batch_entry.lower,
            batch_entry.upper
        );

        // Upload blobs concurrently.
        let parts_to_upload = collect_parts_for_import(batch_entry);
        let upload_results: Vec<Result<(), anyhow::Error>> = stream::iter(parts_to_upload)
            .map(|part_info| {
                let blob = Arc::clone(&blob);
                let target_shard_id = target_shard_id;
                let blobs_dir = blobs_dir.clone();
                async move { upload_part(&blob, target_shard_id, &blobs_dir, part_info).await }
            })
            .buffer_unordered(args.concurrency)
            .collect()
            .await;

        for result in upload_results {
            result?;
        }

        // Read the proto batch and construct a ProtoBatch for the target shard.
        let proto_path = args.input_dir.join(&batch_entry.proto_batch_file);
        let proto_bytes = tokio::fs::read(&proto_path).await?;
        let proto_hollow_batch = ProtoHollowBatch::decode(proto_bytes.as_slice())?;

        let proto_batch = ProtoBatch {
            shard_id: target_shard_id.into_proto(),
            version: crate::BUILD_INFO.version.to_string(),
            batch: Some(proto_hollow_batch),
            key_schema: Bytes::new(),
            val_schema: Bytes::new(),
        };

        let mut batch = write_handle.batch_from_transmittable_batch(proto_batch);
        let expected_upper = timely::progress::Antichain::from(batch_entry.lower.clone());
        let new_upper = timely::progress::Antichain::from(batch_entry.upper.clone());

        match write_handle
            .compare_and_append_batch(&mut [&mut batch], expected_upper, new_upper, false)
            .await
        {
            Ok(Ok(())) => {
                info!("batch {} appended successfully", batch_idx);
            }
            Ok(Err(mismatch)) => {
                info!(
                    "batch {} upper mismatch (expected {:?}, actual {:?}), skipping",
                    batch_idx, mismatch.expected, mismatch.current
                );
            }
            Err(e) => {
                bail!("invalid usage while appending batch {}: {}", batch_idx, e);
            }
        }
    }

    write_handle.expire().await;

    info!(
        "import complete: {} batches imported",
        manifest.batches.len()
    );

    Ok(())
}

/// Information needed to upload a single part.
struct PartUploadInfo {
    original_partial_key: String,
    local_filename: String,
}

/// Collect all parts from a manifest batch entry that need to be uploaded.
fn collect_parts_for_import(batch_entry: &ExportBatchEntry) -> Vec<PartUploadInfo> {
    let mut uploads = Vec::new();
    for part in &batch_entry.parts {
        collect_uploads_recursive(part, &mut uploads);
    }
    uploads
}

fn collect_uploads_recursive(part: &ExportPartEntry, uploads: &mut Vec<PartUploadInfo>) {
    if let Some(ref filename) = part.local_filename {
        uploads.push(PartUploadInfo {
            original_partial_key: part.original_partial_key.clone(),
            local_filename: filename.clone(),
        });
    }
    if let Some(ref contained) = part.contained_parts {
        for child in contained {
            collect_uploads_recursive(child, uploads);
        }
    }
}

/// Upload a single part's blob data to the target blob store.
async fn upload_part(
    blob: &Arc<dyn mz_persist::location::Blob>,
    target_shard_id: ShardId,
    blobs_dir: &std::path::Path,
    info: PartUploadInfo,
) -> Result<(), anyhow::Error> {
    let local_path = blobs_dir.join(&info.local_filename);
    let data = tokio::fs::read(&local_path).await?;
    let partial_key = PartialBatchKey(info.original_partial_key);
    let blob_key = partial_key.complete(&target_shard_id);
    blob.set(&blob_key, Bytes::from(data)).await?;
    Ok(())
}
