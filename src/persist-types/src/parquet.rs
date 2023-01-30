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
use std::io::{Read, Seek, Write};

use anyhow::anyhow;
use arrow2::datatypes::Schema as ArrowSchema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};
use arrow2::io::parquet::write::{
    CompressionOptions, FileWriter, RowGroupIterator, Version, WriteOptions,
};

use crate::codec_impls::UnitSchema;
use crate::columnar::{PartDecoder, PartEncoder, Schema};
use crate::part::{Part, PartBuilder};

/// Encodes the given part into our parquet-based serialization format.
///
/// It doesn't particularly get any anything to use more than one "chunk" per
/// blob, and it's simpler to only have one, so do that.
pub fn encode_part<W: Write>(w: &mut W, part: &Part) -> Result<(), anyhow::Error> {
    let metadata = Vec::new();
    let (fields, encodings, chunk) = part.to_arrow();

    let schema = ArrowSchema::from(fields);
    let options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
    };
    let mut writer = FileWriter::try_new(w, schema.clone(), options)?;

    let row_groups =
        RowGroupIterator::try_new(std::iter::once(Ok(chunk)), &schema, options, encodings)?;
    for row_group in row_groups {
        writer.write(row_group?)?;
    }
    writer.end(Some(metadata))?;
    Ok(())
}

/// Decodes a part with the given schema from our parquet-based serialization
/// format.
pub fn decode_part<R: Read + Seek, K, KS: Schema<K>, V, VS: Schema<V>>(
    r: &mut R,
    key_schema: &KS,
    val_schema: &VS,
) -> Result<Part, anyhow::Error> {
    let metadata = read_metadata(r)?;
    let schema = infer_schema(&metadata)?;
    let mut reader = FileReader::new(r, metadata.row_groups, schema, None, None, None);

    // encode_part documents that there is exactly one chunk in every blob.
    // Verify that here by ensuring the first call to `next` is Some and the
    // second call to it is None.
    let chunk = reader
        .next()
        .ok_or_else(|| anyhow!("not enough chunks in part"))?
        .map_err(anyhow::Error::new)?;
    let part = Part::from_arrow(key_schema, val_schema, chunk).map_err(anyhow::Error::msg)?;

    if let Some(_) = reader.next() {
        return Err(anyhow!("too many chunks in part"));
    }

    Ok(part)
}

/// A helper for writing tests that validate that a piece of data roundtrips
/// through the parquet serialization format.
pub fn validate_roundtrip<T: Default + PartialEq + Debug, S: Schema<T>>(
    schema: &S,
    val: &T,
) -> Result<(), String> {
    let mut part = PartBuilder::new(schema, &UnitSchema);
    let (keys, _vals, mut ts_diff) = part.mut_handles();
    schema.encoder(keys)?.encode(val);
    ts_diff.push(1, 1);
    let part = part.finish()?;

    let mut encoded = Vec::new();
    let () = encode_part(&mut encoded, &part).map_err(|err| err.to_string())?;
    let part = decode_part(&mut std::io::Cursor::new(&encoded), schema, &UnitSchema)
        .map_err(|err| err.to_string())?;

    let mut actual = T::default();
    assert_eq!(part.len(), 1);
    let part = part.key_ref();
    schema.decoder(part)?.decode(0, &mut actual);
    if &actual != val {
        Err(format!(
            "validate_roundtrip expected {:?} but got {:?}",
            val, actual
        ))
    } else {
        Ok(())
    }
}
