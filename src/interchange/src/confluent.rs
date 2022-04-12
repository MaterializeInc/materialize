// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{bail, Result};
use byteorder::{BigEndian, ByteOrder};

/// Extracts the schema_id placed in front of the serialized message by the confluent stack
/// Optionally expect an empty
///
/// This function returns the schema_id and a subslice of the rest of the buffer
fn extract_schema_id<'buf>(buf: &'buf [u8], protocol: &str) -> Result<(i32, &'buf [u8])> {
    // The first byte is a magic byte (0) that indicates the Confluent
    // serialization format version, and the next four bytes are a big
    // endian 32-bit schema ID.
    //
    // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
    //
    // For formats like protobuf, confluent adds additional information related to
    // which message in the proto file

    let expected_len = 5;

    if buf.len() < expected_len {
        bail!(
            "Confluent-style {} datum is too few bytes: expected at least {} bytes, got {}",
            protocol,
            expected_len,
            buf.len()
        );
    }
    let magic = buf[0];
    let schema_id = BigEndian::read_i32(&buf[1..5]);

    if magic != 0 {
        bail!(
            "wrong Confluent-style {} serialization magic: expected 0, got {}",
            protocol,
            magic
        );
    }

    Ok((schema_id, &buf[expected_len..]))
}

pub fn extract_avro_header(buf: &[u8]) -> Result<(i32, &[u8])> {
    extract_schema_id(buf, "avro")
}

pub fn extract_protobuf_header(buf: &[u8]) -> Result<(i32, &[u8])> {
    let (schema_id, buf) = extract_schema_id(buf, "protobuf")?;

    match buf.get(0) {
        Some(0) => Ok((schema_id, &buf[1..])),
        Some(message_id) => bail!(
            "unsupported Confluent-style protobuf message descriptor id: \
                expected 0, but found: {}. \
                See https://github.com/MaterializeInc/materialize/issues/9250",
            message_id
        ),
        None => bail!(
            "Confluent-style protobuf datum is too few bytes: expected a message id after magic \
            and schema id, got a buffer of length {}",
            buf.len()
        ),
    }
}
