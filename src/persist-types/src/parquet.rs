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
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{Fields, Schema as ArrowSchema};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::ChunkReader;
use proptest::prelude::*;
use proptest_derive::Arbitrary;

use crate::codec_impls::UnitSchema;
use crate::columnar::{PartDecoder, Schema};
use crate::part::{Part, PartBuilder};

/// Configuration for encoding columnar data.
#[derive(Debug, Copy, Clone, Arbitrary)]
pub struct EncodingConfig {
    /// Enable dictionary encoding for Parquet data.
    pub use_dictionary: bool,
    /// Compression format for Parquet data.
    pub compression: CompressionFormat,
}

impl Default for EncodingConfig {
    fn default() -> Self {
        EncodingConfig {
            use_dictionary: false,
            compression: CompressionFormat::default(),
        }
    }
}

/// Compression format to apply to columnar data.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Arbitrary)]
pub enum CompressionFormat {
    /// No compression.
    #[default]
    None,
    /// snappy
    Snappy,
    /// lz4
    Lz4,
    /// brotli
    Brotli(CompressionLevel<1, 11, 1>),
    /// zstd
    Zstd(CompressionLevel<1, 22, 1>),
    /// gzip
    Gzip(CompressionLevel<1, 9, 6>),
}

impl CompressionFormat {
    /// Parse a [`CompressionFormat`] from a string, falling back to defaults if the string is not valid.
    pub fn from_str(s: &str) -> Self {
        fn parse_level<const MIN: i32, const MAX: i32, const D: i32>(
            name: &'static str,
            val: &str,
        ) -> CompressionLevel<MIN, MAX, D> {
            match CompressionLevel::from_str(val) {
                Ok(level) => level,
                Err(err) => {
                    tracing::error!("invalid {name} compression level, err: {err}");
                    CompressionLevel::default()
                }
            }
        }

        match s.to_lowercase().as_str() {
            "" => CompressionFormat::None,
            "none" => CompressionFormat::None,
            "snappy" => CompressionFormat::Snappy,
            "lz4" => CompressionFormat::Lz4,
            other => match other.split_once('-') {
                Some(("brotli", level)) => CompressionFormat::Brotli(parse_level("brotli", level)),
                Some(("zstd", level)) => CompressionFormat::Zstd(parse_level("zstd", level)),
                Some(("gzip", level)) => CompressionFormat::Gzip(parse_level("gzip", level)),
                _ => {
                    tracing::error!("unrecognized compression format {s}, returning None");
                    CompressionFormat::None
                }
            },
        }
    }
}

impl From<CompressionFormat> for parquet::basic::Compression {
    fn from(value: CompressionFormat) -> Self {
        match value {
            CompressionFormat::None => parquet::basic::Compression::UNCOMPRESSED,
            CompressionFormat::Lz4 => parquet::basic::Compression::LZ4_RAW,
            CompressionFormat::Snappy => parquet::basic::Compression::SNAPPY,
            CompressionFormat::Brotli(level) => {
                let level: u32 = level.0.try_into().expect("known not negative");
                let level = parquet::basic::BrotliLevel::try_new(level).expect("known valid");
                parquet::basic::Compression::BROTLI(level)
            }
            CompressionFormat::Zstd(level) => {
                let level = parquet::basic::ZstdLevel::try_new(level.0).expect("known valid");
                parquet::basic::Compression::ZSTD(level)
            }
            CompressionFormat::Gzip(level) => {
                let level: u32 = level.0.try_into().expect("known not negative");
                let level = parquet::basic::GzipLevel::try_new(level).expect("known valid");
                parquet::basic::Compression::GZIP(level)
            }
        }
    }
}

/// Level of compression for columnar data.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct CompressionLevel<const MIN: i32, const MAX: i32, const DEFAULT: i32>(i32);

impl<const MIN: i32, const MAX: i32, const DEFAULT: i32> Default
    for CompressionLevel<MIN, MAX, DEFAULT>
{
    fn default() -> Self {
        CompressionLevel(DEFAULT)
    }
}

impl<const MIN: i32, const MAX: i32, const DEFAULT: i32> CompressionLevel<MIN, MAX, DEFAULT> {
    /// Try creating a [`CompressionLevel`] from the provided value, returning an error if it is
    /// outside the `MIN` and `MAX` bounds.
    pub const fn try_new(val: i32) -> Result<Self, i32> {
        if val < MIN {
            Err(val)
        } else if val > MAX {
            Err(val)
        } else {
            Ok(CompressionLevel(val))
        }
    }

    /// Parse a [`CompressionLevel`] form the provided string, returning an error if the string is
    /// not valid.
    pub fn from_str(s: &str) -> Result<Self, String> {
        let val = s.parse::<i32>().map_err(|e| e.to_string())?;
        Self::try_new(val).map_err(|e| e.to_string())
    }
}

impl<const MIN: i32, const MAX: i32, const DEFAULT: i32> Arbitrary
    for CompressionLevel<MIN, MAX, DEFAULT>
{
    type Parameters = ();
    type Strategy = BoxedStrategy<CompressionLevel<MIN, MAX, DEFAULT>>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        ({ MIN }..={ MAX }).prop_map(CompressionLevel).boxed()
    }
}

/// Encodes the given part into our parquet-based serialization format.
///
/// It doesn't particularly get any anything to use more than one "chunk" per
/// blob, and it's simpler to only have one, so do that.
pub fn encode_part<W: Write + Send>(w: &mut W, part: &Part) -> Result<(), anyhow::Error> {
    let config = EncodingConfig::default();
    let (fields, arrays) = part.to_arrow();
    encode_arrays(w, Fields::from(fields), arrays, &config)
}

/// Encodes a set of [`Array`]s into Parquet.
pub fn encode_arrays<W: Write + Send>(
    w: &mut W,
    fields: Fields,
    arrays: Vec<Arc<dyn Array>>,
    config: &EncodingConfig,
) -> Result<(), anyhow::Error> {
    let schema = Arc::new(ArrowSchema::new(fields));
    let props = WriterProperties::builder()
        .set_dictionary_enabled(config.use_dictionary)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(config.compression.into())
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
    let mut reader = decode_arrays(r)?;

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

/// Decodes a [`RecordBatch`] from the provided reader.
pub fn decode_arrays<R: ChunkReader + 'static>(
    r: R,
) -> Result<ParquetRecordBatchReader, anyhow::Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(r)?;

    // To match arrow2, we default the batch size to the number of rows in the RowGroup.
    let row_groups = builder.metadata().row_groups();
    if row_groups.len() > 1 {
        anyhow::bail!("found more than 1 RowGroup")
    }
    let num_rows = row_groups
        .get(0)
        .map(|g| g.num_rows())
        .unwrap_or(1024)
        .try_into()
        .unwrap();
    let builder = builder.with_batch_size(num_rows);

    let reader = builder.build()?;
    Ok(reader)
}

/// A helper for writing tests that validate that a piece of data roundtrips
/// through the parquet serialization format.
pub fn validate_roundtrip<T: Default + Clone + PartialEq + Debug, S: Schema<T>>(
    schema: &S,
    values: &[T],
) -> Result<(), String> {
    let mut builder = PartBuilder::new(schema, &UnitSchema)?;
    for value in values {
        builder.push(value, &(), 1u64, 1i64);
    }
    let part = builder.finish();

    // Sanity check that we can compute stats.
    let _stats = part.key_stats().expect("stats should be compute-able");

    let mut encoded = Vec::new();
    let () = encode_part(&mut encoded, &part).map_err(|err| err.to_string())?;

    let encoded = bytes::Bytes::from(encoded);
    let part = decode_part(encoded, schema, &UnitSchema).map_err(|err| err.to_string())?;

    assert_eq!(part.len(), values.len());
    let part = part.key_ref();
    let decoder = schema.decoder(part)?;

    let mut decoded = vec![T::default(); values.len()];
    for (idx, val) in decoded.iter_mut().enumerate() {
        decoder.decode(idx, val);
    }

    if decoded != values {
        return Err(format!(
            "validate_roundtrip expected {:?} but got {:?}",
            values, decoded
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn smoketest_compression_level_parsing() {
        let cases = &[
            ("", CompressionFormat::None),
            ("none", CompressionFormat::None),
            ("snappy", CompressionFormat::Snappy),
            ("lz4", CompressionFormat::Lz4),
            ("lZ4", CompressionFormat::Lz4),
            ("gzip-1", CompressionFormat::Gzip(CompressionLevel(1))),
            ("GZIp-6", CompressionFormat::Gzip(CompressionLevel(6))),
            ("gzip-9", CompressionFormat::Gzip(CompressionLevel(9))),
            ("brotli-1", CompressionFormat::Brotli(CompressionLevel(1))),
            ("BROtli-8", CompressionFormat::Brotli(CompressionLevel(8))),
            ("brotli-11", CompressionFormat::Brotli(CompressionLevel(11))),
            ("zstd-1", CompressionFormat::Zstd(CompressionLevel(1))),
            ("zstD-10", CompressionFormat::Zstd(CompressionLevel(10))),
            ("zstd-22", CompressionFormat::Zstd(CompressionLevel(22))),
            ("foo", CompressionFormat::None),
            // Invalid values that fallback to the default values.
            ("gzip-0", CompressionFormat::Gzip(Default::default())),
            ("gzip-10", CompressionFormat::Gzip(Default::default())),
            ("brotli-0", CompressionFormat::Brotli(Default::default())),
            ("brotli-12", CompressionFormat::Brotli(Default::default())),
            ("zstd-0", CompressionFormat::Zstd(Default::default())),
            ("zstd-23", CompressionFormat::Zstd(Default::default())),
        ];
        for (s, val) in cases {
            assert_eq!(CompressionFormat::from_str(s), *val);
        }
    }
}
