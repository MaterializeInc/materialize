// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Parquet encodings and utils for persist data

use std::collections::BTreeSet;
use std::io::Write;
use std::sync::Arc;

use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec64;
use mz_persist_types::parquet::EncodingConfig;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::basic::Encoding;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::SchemaDescriptor;
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

use crate::error::Error;
use crate::generated::persist::ProtoBatchFormat;
use crate::generated::persist::proto_batch_part_inline::FormatMetadata as ProtoFormatMetadata;
use crate::indexed::columnar::arrow::{decode_arrow_batch, encode_arrow_batch};
use crate::indexed::encoding::{
    BlobTraceBatchPart, BlobTraceUpdates, decode_trace_inline_meta, encode_trace_inline_meta,
};
use crate::metrics::{ColumnarMetrics, ParquetColumnMetrics};

const INLINE_METADATA_KEY: &str = "MZ:inline";

/// Encodes a [`BlobTraceBatchPart`] into the Parquet format.
pub fn encode_trace_parquet<W: Write + Send, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
    metrics: &ColumnarMetrics,
    cfg: &EncodingConfig,
) -> Result<(), Error> {
    // Better to error now than write out an invalid batch.
    batch.validate()?;

    let inline_meta = encode_trace_inline_meta(batch);
    encode_parquet_kvtd(w, inline_meta, &batch.updates, metrics, cfg)
}

/// Decodes a BlobTraceBatchPart from the Parquet format.
pub fn decode_trace_parquet<T: Timestamp + Codec64>(
    buf: SegmentedBytes,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceBatchPart<T>, Error> {
    decode_trace_parquet_with_demand(buf, metrics, None)
}

/// Like [`decode_trace_parquet`], but pushes a column-demand mask down into the
/// parquet reader so leaves under un-demanded sub-fields of the row struct
/// (`k_s` for source/MV shards) are not decoded. `None` decodes every column.
/// `row_demand`, when present, must be sorted ascending with no duplicates so
/// the projection logic can use `binary_search` for membership tests.
pub fn decode_trace_parquet_with_demand<T: Timestamp + Codec64>(
    buf: SegmentedBytes,
    metrics: &ColumnarMetrics,
    row_demand: Option<&[usize]>,
) -> Result<BlobTraceBatchPart<T>, Error> {
    let metadata = ArrowReaderMetadata::load(&buf, Default::default())?;
    let metadata = metadata
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .as_ref()
        .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));

    let (format, metadata) = decode_trace_inline_meta(metadata.and_then(|x| x.value.as_ref()))?;
    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKvtd => {
            return Err("ArrowKVTD format not supported in parquet".into());
        }
        // Codec-only format has no `k_s` to project; demand is ignored.
        ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(buf, None, metrics, None)?,
        ProtoBatchFormat::ParquetStructured => {
            // Even though `format_metadata` is optional, we expect it when
            // our format is ParquetStructured.
            let format_metadata = metadata
                .format_metadata
                .as_ref()
                .ok_or_else(|| "missing field 'format_metadata'".to_string())?;
            decode_parquet_file_kvtd(buf, Some(format_metadata), metrics, row_demand)?
        }
    };

    let ret = BlobTraceBatchPart {
        desc: metadata.desc.map_or_else(
            || {
                Description::new(
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                )
            },
            |x| x.into(),
        ),
        index: metadata.index,
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

/// Encodes [`BlobTraceUpdates`] to Parquet using the [`parquet`] crate.
pub fn encode_parquet_kvtd<W: Write + Send>(
    w: &mut W,
    inline_base64: String,
    updates: &BlobTraceUpdates,
    metrics: &ColumnarMetrics,
    cfg: &EncodingConfig,
) -> Result<(), Error> {
    let metadata = KeyValue::new(INLINE_METADATA_KEY.to_string(), inline_base64);

    // Note: most of these settings are the defaults from `arrow2` which we
    // previously used and maintain until we tune with benchmarking.
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(cfg.use_dictionary)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(cfg.compression.into())
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(1024 * 1024)
        .set_max_row_group_size(usize::MAX)
        .set_key_value_metadata(Some(vec![metadata]))
        .build();

    let batch = encode_arrow_batch(updates);
    let format = match updates {
        BlobTraceUpdates::Row(_) => "k,v,t,d",
        BlobTraceUpdates::Both(_, _) => "k,v,t,d,k_s,v_s",
        BlobTraceUpdates::Structured { .. } => "t,d,k_s,v_s",
    };

    let mut writer = ArrowWriter::try_new(w, batch.schema(), Some(properties))?;
    writer.write(&batch)?;

    writer.flush()?;
    let bytes_written = writer.bytes_written();
    let file_metadata = writer.close()?;

    report_parquet_metrics(metrics, &file_metadata, bytes_written, format);

    Ok(())
}

/// A parquet [`ProjectionMask`] together with the leaf indices it omits, so
/// callers can attribute byte savings to projection pushdown.
struct RowProjection {
    mask: ProjectionMask,
    excluded_leaves: Vec<usize>,
}

/// Build a parquet [`ProjectionMask`] that retains all leaves of top-level
/// columns *other than* `k_s`, plus only the leaves under demanded sub-fields
/// of `k_s`. Returns `None` if no projection should be applied — either the
/// schema lacks `k_s`, or every sub-field of `k_s` is demanded.
///
/// For source and materialized-view shards, the row data lives on the key
/// (`k_s`) side of the persist `(K, V)` pair (`K = SourceData`, `V = ()`), so
/// MFP column demand projects sub-fields under `k_s`.
///
/// `k_s` carries `SourceData = Result<Row, DataflowError>`, which is encoded
/// as a struct with an `ok` branch (the row) and an `err` branch (the error).
/// So Row sub-fields appear at `k_s/ok/<stable_idx>/...` rather than directly
/// under `k_s/<stable_idx>/...`. The `err` branch is always retained because
/// projection demand only describes row columns. Sub-field names under `ok`
/// are the user column index as a string (e.g. `"0"`, `"1"`, ...), matching
/// the layout produced by [`mz_persist_types::columnar::Schema`]
/// implementations on `RelationDesc`.
fn build_row_projection_mask(schema: &SchemaDescriptor, demand: &[usize]) -> Option<RowProjection> {
    let mut leaves_to_include = Vec::new();
    let mut excluded_leaves = Vec::new();
    let mut k_s_subfields = BTreeSet::new();
    let mut has_k_s = false;

    for (leaf_idx, col_desc) in schema.columns().iter().enumerate() {
        let path = col_desc.path().parts();
        match path.first().map(String::as_str) {
            Some("k_s") => {
                has_k_s = true;
                // Locate the stable sub-field index. SourceData wraps the row
                // in an `ok`/`err` envelope, so probe `k_s/ok/<idx>` first and
                // fall back to `k_s/<idx>` for schemas that aren't wrapped.
                // Anything else (notably `k_s/err/...`) is retained as-is.
                let sub_idx = match path.get(1).map(String::as_str) {
                    Some("ok") => path.get(2).and_then(|s| s.parse::<usize>().ok()),
                    Some(other) => other.parse::<usize>().ok(),
                    None => None,
                };
                if let Some(idx) = sub_idx {
                    k_s_subfields.insert(idx);
                    if demand.binary_search(&idx).is_ok() {
                        leaves_to_include.push(leaf_idx);
                    } else {
                        excluded_leaves.push(leaf_idx);
                    }
                } else {
                    // Unrecognized sub-field name (e.g. the `err` branch);
                    // include defensively so we never drop data the reader
                    // might need.
                    leaves_to_include.push(leaf_idx);
                }
            }
            _ => {
                leaves_to_include.push(leaf_idx);
            }
        }
    }

    // No projection if there's no k_s, or every k_s sub-field is demanded.
    if !has_k_s
        || k_s_subfields
            .iter()
            .all(|idx| demand.binary_search(idx).is_ok())
    {
        return None;
    }

    Some(RowProjection {
        mask: ProjectionMask::leaves(schema, leaves_to_include),
        excluded_leaves,
    })
}

/// Decodes [`BlobTraceUpdates`] from a reader, using [`arrow`].
///
/// `row_demand`, when present, must be sorted ascending with no duplicates.
pub fn decode_parquet_file_kvtd(
    r: impl parquet::file::reader::ChunkReader + 'static,
    format_metadata: Option<&ProtoFormatMetadata>,
    metrics: &ColumnarMetrics,
    row_demand: Option<&[usize]>,
) -> Result<BlobTraceUpdates, Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(r)?;

    // To match arrow2, we default the batch size to the number of rows in the RowGroup.
    let row_groups = builder.metadata().row_groups();
    if row_groups.len() > 1 {
        return Err(Error::String("found more than 1 RowGroup".to_string()));
    }
    let num_rows = usize::try_from(row_groups[0].num_rows())
        .map_err(|_| Error::String("found negative rows".to_string()))?;
    let builder = builder.with_batch_size(num_rows);

    // Apply column projection pushdown to the parquet reader so un-demanded
    // sub-fields of the row struct (`k_s` for source/MV shards) are not
    // decompressed or materialized.
    let builder = match row_demand {
        Some(demand) => match build_row_projection_mask(builder.parquet_schema(), demand) {
            Some(projection) => {
                report_projection_metrics(metrics, builder.metadata(), &projection.excluded_leaves);
                builder.with_projection(projection.mask)
            }
            None => {
                // Demand was supplied but every k_s sub-field present in the
                // blob is demanded (or there is no k_s), so the mask would
                // not drop anything. Track separately so we can tell this
                // apart from "demand was never supplied" when investigating
                // metrics.
                metrics.parquet().projection_no_op_count.inc();
                builder
            }
        },
        None => builder,
    };

    let schema = Arc::clone(builder.schema());
    let mut reader = builder.build()?;

    match format_metadata {
        None => {
            let mut ret = Vec::new();
            for batch in reader {
                let batch = batch.map_err(|e| Error::String(e.to_string()))?;
                ret.push(batch);
            }
            if ret.len() != 1 {
                warn!("unexpected number of row groups: {}", ret.len());
            }
            let batch = ::arrow::compute::concat_batches(&schema, &ret)?;
            let updates = decode_arrow_batch(&batch, metrics).map_err(|e| e.to_string())?;
            Ok(updates)
        }
        Some(ProtoFormatMetadata::StructuredMigration(v @ 1..=3)) => {
            let mut batch = reader
                .next()
                .ok_or_else(|| Error::String("found empty batch".to_string()))??;

            // We enforce an invariant that we have a single RowGroup.
            if reader.next().is_some() {
                return Err(Error::String("found more than one RowGroup".to_string()));
            }

            // Version 1 is a deprecated format so we just ignored the k_s and v_s columns.
            if *v == 1 && batch.num_columns() > 4 {
                batch = batch.project(&[0, 1, 2, 3])?;
            }

            let updates = decode_arrow_batch(&batch, metrics).map_err(|e| e.to_string())?;
            Ok(updates)
        }
        unknown => Err(format!("unkown ProtoFormatMetadata, {unknown:?}"))?,
    }
}

/// Best effort reporting of metrics from the resulting [`parquet::format::FileMetaData`] returned
/// from the [`ArrowWriter`].
fn report_parquet_metrics(
    metrics: &ColumnarMetrics,
    metadata: &ParquetMetaData,
    bytes_written: usize,
    format: &'static str,
) {
    metrics
        .parquet()
        .num_row_groups
        .with_label_values(&[format])
        .inc_by(u64::cast_from(metadata.row_groups().len()));
    metrics
        .parquet()
        .encoded_size
        .with_label_values(&[format])
        .inc_by(u64::cast_from(bytes_written));

    let report_column_size = |col_name: &str, metrics: &ParquetColumnMetrics| {
        let (uncomp, comp) = metadata
            .row_groups()
            .iter()
            .map(|row_group| row_group.columns().iter())
            .flatten()
            .filter(|m| m.column_path().parts().first().map(|s| s.as_str()) == Some(col_name))
            .map(|m| (m.uncompressed_size(), m.compressed_size()))
            .fold((0, 0), |(tot_u, tot_c), (u, c)| (tot_u + u, tot_c + c));

        let uncomp = uncomp.try_into().unwrap_or(0u64);
        let comp = comp.try_into().unwrap_or(0u64);

        metrics.report_sizes(uncomp, comp);
    };

    report_column_size("k", &metrics.parquet().k_metrics);
    report_column_size("v", &metrics.parquet().v_metrics);
    report_column_size("t", &metrics.parquet().t_metrics);
    report_column_size("d", &metrics.parquet().d_metrics);
    report_column_size("k_s", &metrics.parquet().k_s_metrics);
    report_column_size("v_s", &metrics.parquet().v_s_metrics);
}

/// Sum the compressed and uncompressed sizes of the column chunks at
/// `excluded_leaves` across every row group, and increment the projection
/// pushdown counters. Called only when the mask actually drops at least one
/// leaf, so a non-zero increment here means projection saved real
/// decompression and arrow-decode work (the bytes are still fetched as part
/// of the blob; persist does not range-read).
fn report_projection_metrics(
    metrics: &ColumnarMetrics,
    metadata: &ParquetMetaData,
    excluded_leaves: &[usize],
) {
    if excluded_leaves.is_empty() {
        return;
    }
    let mut uncompressed: i64 = 0;
    let mut compressed: i64 = 0;
    for row_group in metadata.row_groups() {
        let cols = row_group.columns();
        for &leaf in excluded_leaves {
            if let Some(col) = cols.get(leaf) {
                uncompressed += col.uncompressed_size();
                compressed += col.compressed_size();
            }
        }
    }
    let parquet = metrics.parquet();
    parquet.projection_applied_count.inc();
    parquet
        .projection_skipped_bytes_uncompressed
        .inc_by(uncompressed.try_into().unwrap_or(0));
    parquet
        .projection_skipped_bytes_compressed
        .inc_by(compressed.try_into().unwrap_or(0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::arrow::array::{Array, Int64Array, NullArray, StringArray, StructArray};
    use ::arrow::datatypes::{DataType, Field};
    use mz_persist_types::parquet::EncodingConfig;

    use crate::indexed::columnar::ColumnarRecordsStructuredExt;
    use crate::indexed::encoding::BlobTraceBatchPart;
    use crate::metrics::ColumnarMetrics;

    /// Build a tiny structured part with `k_s = struct { "0": Int64, "1": Utf8,
    /// "2": Utf8, "3": Int64 }`. Mirrors the source/MV layout where row data
    /// lives on the key side. Used to exercise parquet projection.
    fn synth_structured_part(num_rows: usize) -> BlobTraceBatchPart<u64> {
        synth_structured_part_inner(num_rows, false)
    }

    /// Like [`synth_structured_part`], but wraps the row struct in a
    /// `Result`-shaped `{ ok: Row, err: Utf8 }` envelope, mirroring how the
    /// real persist source writes `SourceData = Result<Row, DataflowError>`.
    fn synth_source_data_part(num_rows: usize) -> BlobTraceBatchPart<u64> {
        synth_structured_part_inner(num_rows, true)
    }

    fn synth_structured_part_inner(
        num_rows: usize,
        wrap_in_result: bool,
    ) -> BlobTraceBatchPart<u64> {
        use differential_dataflow::trace::Description;
        use timely::progress::Antichain;

        let num_rows_i64 = i64::try_from(num_rows).expect("test row count fits in i64");
        let col0: Int64Array = (0..num_rows_i64).collect();
        let col1_strs: Vec<String> = (0..num_rows).map(|i| format!("col1-{i}")).collect();
        let col1 = StringArray::from_iter_values(col1_strs.iter().map(String::as_str));
        let col2_strs: Vec<String> = (0..num_rows).map(|i| format!("col2-{i}")).collect();
        let col2 = StringArray::from_iter_values(col2_strs.iter().map(String::as_str));
        let col3: Int64Array = (100..(100 + num_rows_i64)).collect();
        let col0_arr: Arc<dyn ::arrow::array::Array> = Arc::new(col0);
        let col1_arr: Arc<dyn ::arrow::array::Array> = Arc::new(col1);
        let col2_arr: Arc<dyn ::arrow::array::Array> = Arc::new(col2);
        let col3_arr: Arc<dyn ::arrow::array::Array> = Arc::new(col3);
        let row_struct = StructArray::from(vec![
            (Arc::new(Field::new("0", DataType::Int64, true)), col0_arr),
            (Arc::new(Field::new("1", DataType::Utf8, true)), col1_arr),
            (Arc::new(Field::new("2", DataType::Utf8, true)), col2_arr),
            (Arc::new(Field::new("3", DataType::Int64, true)), col3_arr),
        ]);

        let key_array: Arc<dyn ::arrow::array::Array> = if wrap_in_result {
            // Build `struct { ok: row_struct, err: Utf8 }` to match the
            // SourceData encoding the real persist source emits.
            let err_strs: Vec<Option<&str>> = (0..num_rows).map(|_| None).collect();
            let err = StringArray::from(err_strs);
            let row_arr: Arc<dyn ::arrow::array::Array> = Arc::new(row_struct);
            let err_arr: Arc<dyn ::arrow::array::Array> = Arc::new(err);
            let result_struct = StructArray::from(vec![
                (
                    Arc::new(Field::new("ok", row_arr.data_type().clone(), true)),
                    row_arr,
                ),
                (Arc::new(Field::new("err", DataType::Utf8, true)), err_arr),
            ]);
            Arc::new(result_struct)
        } else {
            Arc::new(row_struct)
        };

        // Unit-valued: persist's `UnitSchema` encodes the value as a `NullArray`.
        let val_array = NullArray::new(num_rows);

        let timestamps: Int64Array = (0..num_rows_i64).collect();
        let diffs: Int64Array = std::iter::repeat(1i64).take(num_rows).collect();

        let updates = BlobTraceUpdates::Structured {
            key_values: ColumnarRecordsStructuredExt {
                key: key_array,
                val: Arc::new(val_array),
            },
            timestamps,
            diffs,
        };

        BlobTraceBatchPart {
            desc: Description::new(
                Antichain::from_elem(0u64),
                Antichain::from_elem(u64::MAX),
                Antichain::from_elem(0u64),
            ),
            index: 0,
            updates,
        }
    }

    #[mz_ore::test]
    fn projection_drops_undemanded_row_subfields() {
        let metrics = ColumnarMetrics::disconnected();
        let part = synth_structured_part(8);

        let mut buf = Vec::new();
        encode_trace_parquet(&mut buf, &part, &metrics, &EncodingConfig::default()).unwrap();
        let buf = SegmentedBytes::from(buf);

        // Demand only sub-fields 0 and 2; 1 and 3 should be dropped from `k_s`.
        let demand: Vec<usize> = vec![0, 2];
        let projected =
            decode_trace_parquet_with_demand::<u64>(buf.clone(), &metrics, Some(&demand)).unwrap();

        let k_s = match &projected.updates {
            BlobTraceUpdates::Structured { key_values, .. } => Arc::clone(&key_values.key),
            _ => panic!("expected structured updates"),
        };
        let k_s = k_s.as_any().downcast_ref::<StructArray>().unwrap();
        let names: Vec<&str> = k_s.column_names().into_iter().collect();
        assert_eq!(names, vec!["0", "2"], "expected only demanded sub-fields");

        // Demand-None should preserve the full schema.
        let full = decode_trace_parquet_with_demand::<u64>(buf, &metrics, None).unwrap();
        let k_s_full = match &full.updates {
            BlobTraceUpdates::Structured { key_values, .. } => Arc::clone(&key_values.key),
            _ => panic!("expected structured updates"),
        };
        let k_s_full = k_s_full.as_any().downcast_ref::<StructArray>().unwrap();
        let names: Vec<&str> = k_s_full.column_names().into_iter().collect();
        assert_eq!(names, vec!["0", "1", "2", "3"]);
    }

    #[mz_ore::test]
    fn projection_metrics_increment_when_mask_drops_leaves() {
        let metrics = ColumnarMetrics::disconnected();
        let parquet = metrics.parquet();

        // Sanity: counters start at zero.
        assert_eq!(parquet.projection_applied_count.get(), 0);
        assert_eq!(parquet.projection_skipped_bytes_uncompressed.get(), 0);
        assert_eq!(parquet.projection_skipped_bytes_compressed.get(), 0);

        let part = synth_structured_part(64);
        let mut buf = Vec::new();
        encode_trace_parquet(&mut buf, &part, &metrics, &EncodingConfig::default()).unwrap();
        let buf = SegmentedBytes::from(buf);

        // No-op projection: count and byte counters unchanged.
        let no_op_demand: Vec<usize> = (0..4).collect();
        let _ = decode_trace_parquet_with_demand::<u64>(buf.clone(), &metrics, Some(&no_op_demand))
            .unwrap();
        assert_eq!(parquet.projection_applied_count.get(), 0);
        assert_eq!(parquet.projection_skipped_bytes_uncompressed.get(), 0);

        // Real projection: drop sub-fields 1 and 3.
        let demand: Vec<usize> = vec![0, 2];
        let _ = decode_trace_parquet_with_demand::<u64>(buf, &metrics, Some(&demand)).unwrap();
        assert_eq!(parquet.projection_applied_count.get(), 1);
        assert!(
            parquet.projection_skipped_bytes_uncompressed.get() > 0,
            "expected non-zero uncompressed skipped bytes"
        );
        assert!(
            parquet.projection_skipped_bytes_compressed.get() > 0,
            "expected non-zero compressed skipped bytes"
        );
    }

    #[mz_ore::test]
    fn projection_drops_undemanded_subfields_under_source_data_envelope() {
        // Real persist source data wraps the row struct in `Result<Row, _>`,
        // surfacing leaves as `k_s/ok/<stable_idx>/...` and `k_s/err/...`.
        let metrics = ColumnarMetrics::disconnected();
        let parquet = metrics.parquet();

        let part = synth_source_data_part(64);
        let mut buf = Vec::new();
        encode_trace_parquet(&mut buf, &part, &metrics, &EncodingConfig::default()).unwrap();
        let buf = SegmentedBytes::from(buf);

        // Demand only sub-fields 0 and 2 of the wrapped row.
        let demand: Vec<usize> = vec![0, 2];
        let projected =
            decode_trace_parquet_with_demand::<u64>(buf.clone(), &metrics, Some(&demand)).unwrap();

        // The decoded `k_s/ok` struct should contain only the demanded
        // sub-fields; `k_s/err` is always retained.
        let k_s = match &projected.updates {
            BlobTraceUpdates::Structured { key_values, .. } => Arc::clone(&key_values.key),
            _ => panic!("expected structured updates"),
        };
        let k_s = k_s.as_any().downcast_ref::<StructArray>().unwrap();
        let k_s_names: Vec<&str> = k_s.column_names().into_iter().collect();
        assert_eq!(k_s_names, vec!["ok", "err"], "envelope preserved");
        let ok = k_s
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let ok_names: Vec<&str> = ok.column_names().into_iter().collect();
        assert_eq!(
            ok_names,
            vec!["0", "2"],
            "expected only demanded sub-fields"
        );

        assert_eq!(parquet.projection_applied_count.get(), 1);
        assert_eq!(parquet.projection_no_op_count.get(), 0);
        assert!(
            parquet.projection_skipped_bytes_uncompressed.get() > 0,
            "expected non-zero uncompressed skipped bytes"
        );
    }

    #[mz_ore::test]
    fn projection_no_op_when_demand_covers_every_subfield() {
        let metrics = ColumnarMetrics::disconnected();
        let part = synth_structured_part(4);

        let mut buf = Vec::new();
        encode_trace_parquet(&mut buf, &part, &metrics, &EncodingConfig::default()).unwrap();
        let buf = SegmentedBytes::from(buf);

        // Demand covers all four sub-fields; the helper should detect "no
        // projection needed" and return the full struct unchanged.
        let demand: Vec<usize> = (0..4).collect();
        let result = decode_trace_parquet_with_demand::<u64>(buf, &metrics, Some(&demand)).unwrap();
        let k_s = match &result.updates {
            BlobTraceUpdates::Structured { key_values, .. } => Arc::clone(&key_values.key),
            _ => panic!("expected structured updates"),
        };
        let k_s = k_s.as_any().downcast_ref::<StructArray>().unwrap();
        let names: Vec<&str> = k_s.column_names().into_iter().collect();
        assert_eq!(names, vec!["0", "1", "2", "3"]);
    }
}
