// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Parquet serialization and deserialization for persist data.
//!
//! TODO: Move this into mz_persist_client::internal once we don't need
//! [validate_roundtrip].

use std::fmt::Debug;
use std::io::Write;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::RecordBatch;
use arrow::datatypes::Schema as ArrowSchema;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::ChunkReader;

use crate::codec_impls::UnitSchema;
use crate::columnar::{PartDecoder, Schema};
use crate::part::{Part, PartBuilder};

/// Encodes the given part into our parquet-based serialization format.
///
/// It doesn't particularly get any anything to use more than one "chunk" per
/// blob, and it's simpler to only have one, so do that.
pub fn encode_part<W: Write + Send>(w: &mut W, part: &Part) -> Result<(), anyhow::Error> {
    let (fields, arrays) = part.to_arrow();

    let schema = Arc::new(ArrowSchema::new(fields));
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(1024 * 1024)
        .set_max_row_group_size(usize::MAX)
        .build();
    let mut writer = ArrowWriter::try_new(w, Arc::clone(&schema), Some(props))?;

    let record_batch = RecordBatch::try_new(schema, arrays)?;

    writer.write(&record_batch)?;
    writer.flush()?;
    writer.close()?;

    Ok(())
}

/// Decodes a part with the given schema from our parquet-based serialization
/// format.
pub fn decode_part<R: ChunkReader + 'static, K, KS: Schema<K>, V, VS: Schema<V>>(
    r: R,
    key_schema: &KS,
    val_schema: &VS,
) -> Result<Part, anyhow::Error> {
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(r)?.build()?;

    // encode_part documents that there is exactly one chunk in every blob.
    // Verify that here by ensuring the first call to `next` is Some and the
    // second call to it is None.
    let record_batch = reader
        .next()
        .ok_or_else(|| anyhow!("not enough chunks in part"))?
        .map_err(anyhow::Error::new)?;
    let part = Part::from_arrow(key_schema, val_schema, record_batch.columns())
        .map_err(anyhow::Error::msg)?;

    if let Some(_) = reader.next() {
        return Err(anyhow!("too many chunks in part"));
    }

    Ok(part)
}

/// A helper for writing tests that validate that a piece of data roundtrips
/// through the parquet serialization format.
pub fn validate_roundtrip<T: Default + PartialEq + Debug, S: Schema<T>>(
    schema: &S,
    value: &T,
) -> Result<(), String> {
    let mut builder = PartBuilder::new(schema, &UnitSchema)?;
    builder.push(value, &(), 1u64, 1i64);
    let part = builder.finish();

    // Sanity check that we can compute stats.
    let _stats = part.key_stats().expect("stats should be compute-able");

    let mut encoded = Vec::new();
    let () = encode_part(&mut encoded, &part).map_err(|err| err.to_string())?;

    let encoded = bytes::Bytes::from(encoded);
    let part = decode_part(encoded, schema, &UnitSchema).map_err(|err| err.to_string())?;

    let mut actual = T::default();
    assert_eq!(part.len(), 1);
    let part = part.key_ref();
    schema.decoder(part)?.decode(0, &mut actual);
    if &actual != value {
        Err(format!(
            "validate_roundtrip expected {:?} but got {:?}",
            value, actual
        ))
    } else {
        Ok(())
    }
}
