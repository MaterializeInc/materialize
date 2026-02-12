// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Export shard data as Parquet.

use std::io::{self, Write};
use std::pin::pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, AsArray, Int64Array, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use differential_dataflow::lattice::Lattice;
use futures_util::{StreamExt, TryStreamExt};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_types::Codec64;
use mz_persist_types::part::Part;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use timely::progress::Antichain;

use crate::cli::args::{NO_COMMIT, READ_ALL_BUILD_INFO, make_blob, make_consensus};
use crate::fetch::{EncodedPart, FetchConfig};
use crate::internal::state_versions::StateVersions;
use crate::{Metrics, PersistConfig, ShardId};

/// Maximum number of rows per chunk when writing Parquet via `arrow::compute::take`.
/// Keeps each chunk well under the ~2GB i32 offset limit for Utf8/Binary arrays.
const CHUNK_SIZE: usize = 500_000;

/// Export shard data as Parquet.
#[derive(Debug, clap::Args)]
pub struct ExportArgs {
    /// Shard to export
    #[clap(long)]
    shard_id: String,

    /// Consensus URI
    #[clap(long, env = "CONSENSUS_URI")]
    consensus_uri: mz_ore::url::SensitiveUrl,

    /// Blob URI
    #[clap(long, env = "BLOB_URI")]
    blob_uri: mz_ore::url::SensitiveUrl,

    /// Export mode: "snapshot" reads consolidated state at as_of,
    /// "subscribe" outputs all updates from as_of onward with timestamps and diffs.
    #[clap(long, default_value = "snapshot")]
    mode: ExportMode,

    /// Timestamp to read at (snapshot) or start from (subscribe).
    /// Defaults to the shard's current upper - 1 for snapshot,
    /// or the since for subscribe.
    #[clap(long)]
    as_of: Option<u64>,

    /// Output file path (Parquet). Defaults to stdout.
    #[clap(long)]
    output: Option<String>,
}

#[derive(Debug, Clone, clap::ValueEnum)]
enum ExportMode {
    Snapshot,
    Subscribe,
}

/// Run the export command.
pub async fn run(args: ExportArgs) -> Result<(), anyhow::Error> {
    let shard_id = ShardId::from_str(&args.shard_id).map_err(|e| anyhow!("{}", e))?;
    let cfg = PersistConfig::new_default_configs(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let consensus =
        make_consensus(&cfg, &args.consensus_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
    let blob = make_blob(&cfg, &args.blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
    let state_versions = StateVersions::new(cfg.clone(), consensus, blob, Arc::clone(&metrics));

    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;
    if versions.0.is_empty() {
        // Shard exists in mz_storage_shards but has no consensus data — skip gracefully.
        eprintln!("Shard {} has no data in consensus, skipping.", shard_id);
        return Ok(());
    }
    // fetch_current_state can loop forever if the rollup blob is missing
    // (e.g. shard has consensus entries but no blob data). Add a timeout.
    let state = tokio::time::timeout(
        Duration::from_secs(30),
        state_versions.fetch_current_state::<u64>(&shard_id, versions.0.clone()),
    )
    .await
    .map_err(|_| {
        anyhow!(
            "timed out fetching state for shard {} — this usually means blob storage \
             is inaccessible or doesn't match the consensus data. Check the blob URI.",
            shard_id,
        )
    })?;
    let state = state.check_ts_codec(&shard_id)?;
    let shard_metrics = metrics.shards.shard(&shard_id, "unknown");

    let as_of = match args.as_of {
        Some(ts) => ts,
        None => match args.mode {
            ExportMode::Snapshot => {
                let upper = state.upper();
                let upper_ts = upper
                    .as_option()
                    .ok_or_else(|| anyhow!("shard upper is the empty antichain"))?;
                upper_ts
                    .checked_sub(1)
                    .ok_or_else(|| anyhow!("shard upper is 0, nothing to read"))?
            }
            ExportMode::Subscribe => {
                let since = state.since();
                *since
                    .as_option()
                    .ok_or_else(|| anyhow!("shard since is the empty antichain"))?
            }
        },
    };

    let since = state.since();
    let upper = state.upper();
    eprintln!("Shard {} since={:?} upper={:?}", shard_id, since, upper);

    if let Some(since_ts) = since.as_option() {
        if as_of < *since_ts {
            eprintln!(
                "Warning: as_of={} is below since={}, data may have been compacted/GC'd",
                as_of, since_ts
            );
        }
    }

    let batches: Vec<_> = state.collections.trace.batches().collect();
    eprintln!(
        "Reading shard {} at as_of={} ({} batches)",
        shard_id,
        as_of,
        batches.len()
    );

    // Phase 1: Collect lightweight part references (metadata only, no row data).
    // RunPart::Many refs involve a blob fetch to resolve the run, but the refs
    // themselves are small (~100 bytes each).
    let fetch_config = FetchConfig::from_persist_config(&cfg);
    let mut part_refs = Vec::new();
    for batch in &batches {
        let mut stream =
            pin!(batch.part_stream(shard_id, &*state_versions.blob, &*state_versions.metrics));
        while let Some(part) = stream.try_next().await? {
            let name = part.printable_name().to_string();
            part_refs.push((batch.desc.clone(), part.into_owned(), name));
        }
    }
    let total_parts = part_refs.len();
    eprintln!("Found {} parts to fetch", total_parts);

    if part_refs.is_empty() {
        eprintln!("No data in shard.");
        return Ok(());
    }

    // Phase 2: Fetch parts with bounded concurrency (overlap blob I/O with processing).
    // buffered(2) keeps at most 2 decoded parts in memory (~256MB) while ensuring the
    // next part is being fetched while the current one is processed+written.
    let blob = &*state_versions.blob;
    let sv_metrics = &state_versions.metrics;
    let sm = &shard_metrics;
    let rm = &metrics.read.snapshot;

    let mut part_stream = futures_util::stream::iter(part_refs.iter().enumerate())
        .map(|(i, (desc, part_ref, name))| {
            let fc = fetch_config.clone();
            async move {
                eprintln!("  Fetching part {}/{} ({})", i + 1, total_parts, name);
                let encoded =
                    EncodedPart::fetch(&fc, &shard_id, blob, sv_metrics, sm, rm, desc, part_ref)
                        .await
                        .expect("part exists");
                encoded
                    .updates()
                    .as_part()
                    .ok_or_else(|| anyhow!("expected structured data"))
            }
        })
        .buffered(2);

    let mut writer: Option<ArrowWriter<Box<dyn Write + Send>>> = None;
    let mut parquet_schema: Option<Arc<Schema>> = None;
    let mut total_rows = 0usize;

    while let Some(part_result) = part_stream.next().await {
        let part = part_result?;

        // First part: extract schema, create writer.
        if writer.is_none() {
            let key_struct = part.key.as_struct();
            let ok_idx = key_struct
                .fields()
                .iter()
                .position(|f| f.name() == "ok")
                .ok_or_else(|| anyhow!("key struct missing 'ok' field"))?;
            let ok_column = key_struct.column(ok_idx);
            if ok_column.data_type().is_null() {
                anyhow::bail!("ok field is NullArray — no data columns to export");
            }
            let ok_schema = ok_column_schema(ok_column.as_ref())?;

            let output: Box<dyn Write + Send> = match &args.output {
                Some(path) => Box::new(std::fs::File::create(path)?),
                None => Box::new(io::stdout()),
            };

            let schema = Arc::new(match &args.mode {
                ExportMode::Snapshot => ok_schema,
                ExportMode::Subscribe => subscribe_schema(&ok_schema),
            });
            let props = WriterProperties::builder()
                .set_compression(Compression::SNAPPY)
                .build();
            parquet_schema = Some(Arc::clone(&schema));
            writer = Some(ArrowWriter::try_new(output, schema, Some(props))?);
        }

        let w = writer.as_mut().unwrap();
        total_rows += match &args.mode {
            ExportMode::Snapshot => process_snapshot_part(&part, as_of, w)?,
            ExportMode::Subscribe => {
                process_subscribe_part(&part, as_of, parquet_schema.as_ref().unwrap(), w)?
            }
        };

        w.flush()?; // Finalize row group, free writer buffers.
        // `part` dropped here — memory freed before consuming next from stream.
    }

    if let Some(w) = writer {
        eprintln!("Wrote {} total rows from {} parts", total_rows, total_parts);
        w.close()?;
        eprintln!("Export complete.");
    } else {
        eprintln!("No data in shard.");
    }

    Ok(())
}

/// Process a single part in snapshot mode: advance timestamps to as_of,
/// consolidate within the part, write surviving data rows.
///
/// Correct for fully compacted shards where each key appears in exactly one part.
/// Emits a warning if negative diffs remain after within-part consolidation
/// (which indicates cross-part consolidation would be needed).
fn process_snapshot_part(
    part: &Part,
    as_of: u64,
    writer: &mut ArrowWriter<Box<dyn Write + Send>>,
) -> Result<usize, anyhow::Error> {
    let part_ord = part.as_ord();
    let as_of_frontier = Antichain::from_elem(as_of);

    let mut updates = Vec::new();
    for (k, v, t, d) in part_ord.iter() {
        let mut t = <u64 as Codec64>::decode(t);
        t.advance_by(as_of_frontier.borrow());
        let d = <i64 as Codec64>::decode(d);
        updates.push(((k, v), t, d));
    }

    differential_dataflow::consolidation::consolidate_updates(&mut updates);

    let key_struct = part.key.as_struct();
    let ok_idx = key_struct
        .fields()
        .iter()
        .position(|f| f.name() == "ok")
        .ok_or_else(|| anyhow!("key struct missing 'ok' field"))?;
    let err_idx = key_struct.fields().iter().position(|f| f.name() == "err");
    let ok_column = key_struct.column(ok_idx);
    let err_column = err_idx.map(|i| key_struct.column(i));

    let mut take_indices: Vec<u32> = Vec::new();
    let mut has_negative_diffs = false;
    for ((k, _v), _t, d) in &updates {
        if *d < 0 {
            has_negative_diffs = true;
            continue;
        }
        if *d == 0 {
            continue;
        }
        // Skip error rows (err column is non-null).
        if let Some(ec) = &err_column {
            if !ec.is_null(k.idx) {
                continue;
            }
        }
        let idx =
            u32::try_from(k.idx).map_err(|_| anyhow!("row index {} exceeds u32::MAX", k.idx))?;
        for _ in 0..*d {
            take_indices.push(idx);
        }
    }

    if has_negative_diffs {
        eprintln!("    Warning: negative diffs remain after within-part consolidation");
    }

    if !take_indices.is_empty() {
        write_snapshot_chunks(ok_column.as_ref(), &take_indices, writer)?;
    }

    Ok(take_indices.len())
}

/// Process a single part in subscribe mode: write all non-error rows with
/// timestamps and diffs (skipping rows at the as_of timestamp which represent
/// initial state already captured by snapshot mode).
fn process_subscribe_part(
    part: &Part,
    as_of: u64,
    schema: &Arc<Schema>,
    writer: &mut ArrowWriter<Box<dyn Write + Send>>,
) -> Result<usize, anyhow::Error> {
    let part_ord = part.as_ord();
    let as_of_frontier = Antichain::from_elem(as_of);

    let key_struct = part.key.as_struct();
    let ok_idx = key_struct
        .fields()
        .iter()
        .position(|f| f.name() == "ok")
        .ok_or_else(|| anyhow!("key struct missing 'ok' field"))?;
    let err_idx = key_struct.fields().iter().position(|f| f.name() == "err");
    let ok_column = key_struct.column(ok_idx);
    let err_column = err_idx.map(|i| key_struct.column(i));

    let mut timestamps: Vec<u64> = Vec::new();
    let mut diffs: Vec<i64> = Vec::new();
    let mut take_indices: Vec<u32> = Vec::new();

    for (k, _v, t, d) in part_ord.iter() {
        let mut t = <u64 as Codec64>::decode(t);
        t.advance_by(as_of_frontier.borrow());
        let d = <i64 as Codec64>::decode(d);

        // Skip rows at the as_of timestamp — those represent the initial state
        // which is already captured by snapshot mode. Only include changes
        // that happen strictly after the snapshot point.
        if t == as_of {
            continue;
        }
        if let Some(ec) = &err_column {
            if !ec.is_null(k.idx) {
                continue;
            }
        }
        let idx =
            u32::try_from(k.idx).map_err(|_| anyhow!("row index {} exceeds u32::MAX", k.idx))?;
        timestamps.push(t);
        diffs.push(d);
        take_indices.push(idx);
    }

    if !take_indices.is_empty() {
        write_subscribe_chunks(
            ok_column.as_ref(),
            &take_indices,
            &timestamps,
            &diffs,
            schema,
            writer,
        )?;
    }

    Ok(take_indices.len())
}

/// Convert an "ok" StructArray into a flat RecordBatch with its fields as top-level columns.
fn ok_to_record_batch(ok_array: &dyn Array) -> Result<RecordBatch, anyhow::Error> {
    let ok_struct = ok_array.as_struct().clone();
    if ok_struct.null_count() == 0 {
        Ok(RecordBatch::from(ok_struct))
    } else {
        // Decompose and drop the struct-level null bitmap.
        // Child arrays carry their own null bitmaps.
        let (fields, columns, _nulls) = ok_struct.into_parts();
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns,
        )?)
    }
}

/// Derive an Arrow Schema from an "ok" StructArray column.
fn ok_column_schema(ok_column: &dyn Array) -> Result<Schema, anyhow::Error> {
    match ok_column.data_type() {
        DataType::Struct(fields) => Ok(Schema::new(fields.clone())),
        other => anyhow::bail!("expected Struct ok column, got {:?}", other),
    }
}

/// Build the subscribe-mode schema: mz_timestamp + mz_diff + ok columns.
fn subscribe_schema(ok_schema: &Schema) -> Schema {
    let mut fields: Vec<Arc<Field>> = vec![
        Arc::new(Field::new("mz_timestamp", DataType::UInt64, false)),
        Arc::new(Field::new("mz_diff", DataType::Int64, false)),
    ];
    fields.extend(ok_schema.fields().iter().cloned());
    Schema::new(fields)
}

/// Write take_indices in chunks, calling `arrow::compute::take` on each chunk
/// to avoid exceeding the i32 offset limit for Utf8/Binary arrays.
fn write_snapshot_chunks(
    ok_column: &dyn Array,
    take_indices: &[u32],
    writer: &mut ArrowWriter<Box<dyn Write + Send>>,
) -> Result<(), anyhow::Error> {
    for chunk in take_indices.chunks(CHUNK_SIZE) {
        let indices = UInt32Array::from(chunk.to_vec());
        let taken = arrow::compute::take(ok_column, &indices, None)?;
        let batch = ok_to_record_batch(taken.as_ref())?;
        writer.write(&batch)?;
    }
    Ok(())
}

/// Write subscribe-mode data in chunks, each with mz_timestamp and mz_diff columns.
fn write_subscribe_chunks(
    ok_column: &dyn Array,
    take_indices: &[u32],
    timestamps: &[u64],
    diffs: &[i64],
    schema: &Arc<Schema>,
    writer: &mut ArrowWriter<Box<dyn Write + Send>>,
) -> Result<(), anyhow::Error> {
    for (i, chunk) in take_indices.chunks(CHUNK_SIZE).enumerate() {
        let start = i * CHUNK_SIZE;
        let end = start + chunk.len();

        let indices = UInt32Array::from(chunk.to_vec());
        let taken = arrow::compute::take(ok_column, &indices, None)?;
        let data_batch = ok_to_record_batch(taken.as_ref())?;

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(UInt64Array::from(timestamps[start..end].to_vec())),
            Arc::new(Int64Array::from(diffs[start..end].to_vec())),
        ];
        columns.extend(data_batch.columns().iter().cloned());

        let batch = RecordBatch::try_new(Arc::clone(schema), columns)?;
        writer.write(&batch)?;
    }
    Ok(())
}
