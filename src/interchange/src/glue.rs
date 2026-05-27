// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wire-format helpers for the AWS Glue Schema Registry framing.
//!
//! Glue prepends an 18-byte header to each Kafka record payload:
//!
//! | Offset | Bytes | Meaning                                              |
//! |--------|-------|------------------------------------------------------|
//! | 0      | 1     | Header version. Glue currently emits `0x03`.         |
//! | 1      | 1     | Compression byte. `0x00` = none, `0x05` = zlib.      |
//! | 2..18  | 16    | Schema-version UUID, big-endian.                     |
//! | 18..   | N     | The serialized record payload.                       |
//!
//! Materialize only supports the uncompressed framing (`compression =
//! 0x00`). Compressed records are rejected — supporting zlib at the wire
//! layer is straightforward but no consumer in Materialize asks for it yet,
//! and silently decompressing would mask producer misconfiguration.
//!
//! The Confluent analogue lives in [`crate::confluent`].

use anyhow::{Result, bail};
use uuid::Uuid;

/// Glue wire-format header version, written at byte 0.
const HEADER_VERSION: u8 = 0x03;

/// Compression byte indicating an uncompressed payload.
const COMPRESSION_NONE: u8 = 0x00;

/// Length of the Glue header in bytes (version + compression + UUID).
pub const HEADER_LEN: usize = 1 + 1 + 16;

/// Parse the Glue Avro header from the front of `buf`, returning the
/// schema-version UUID and a subslice covering the record payload.
///
/// Returns an error if the buffer is shorter than the fixed header, if the
/// header-version byte is not `0x03`, or if the compression byte is
/// anything other than `0x00`.
pub fn extract_avro_header(buf: &[u8]) -> Result<(Uuid, &[u8])> {
    if buf.len() < HEADER_LEN {
        bail!(
            "Glue-style avro datum is too few bytes: expected at least {} bytes, got {}",
            HEADER_LEN,
            buf.len()
        );
    }
    let version = buf[0];
    if version != HEADER_VERSION {
        bail!(
            "wrong Glue-style avro serialization header version: expected {:#04x}, got {:#04x}",
            HEADER_VERSION,
            version
        );
    }
    let compression = buf[1];
    if compression != COMPRESSION_NONE {
        bail!(
            "unsupported Glue-style avro compression byte: \
             expected {:#04x} (uncompressed), got {:#04x}",
            COMPRESSION_NONE,
            compression
        );
    }
    // `Uuid::from_slice` only fails on length mismatch, which we've already
    // validated above; the unwrap is sound.
    let uuid = Uuid::from_slice(&buf[2..HEADER_LEN]).expect("18-byte header validated above");
    Ok((uuid, &buf[HEADER_LEN..]))
}

/// Frame `payload` with the Glue Avro header, producing a buffer suitable
/// to publish to Kafka. The header is laid down using the uncompressed
/// framing (`compression = 0x00`).
pub fn prepend_avro_header(schema_version_id: Uuid, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.push(HEADER_VERSION);
    out.push(COMPRESSION_NONE);
    out.extend_from_slice(schema_version_id.as_bytes());
    out.extend_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_uuid() -> Uuid {
        // Fixed value so the encoded byte layout is exact in assertions.
        Uuid::parse_str("12345678-1234-5678-1234-567812345678").unwrap()
    }

    #[mz_ore::test]
    fn roundtrip() {
        let uuid = fixture_uuid();
        let payload = b"avro-bytes-here";
        let framed = prepend_avro_header(uuid, payload);
        assert_eq!(framed.len(), HEADER_LEN + payload.len());
        let (parsed_uuid, rest) = extract_avro_header(&framed).unwrap();
        assert_eq!(parsed_uuid, uuid);
        assert_eq!(rest, payload);
    }

    #[mz_ore::test]
    fn header_byte_layout() {
        let uuid = fixture_uuid();
        let framed = prepend_avro_header(uuid, &[]);
        assert_eq!(framed[0], HEADER_VERSION);
        assert_eq!(framed[1], COMPRESSION_NONE);
        assert_eq!(&framed[2..HEADER_LEN], uuid.as_bytes());
    }

    #[mz_ore::test]
    fn rejects_buffer_too_short() {
        // 17 bytes — one short of the minimum header.
        let buf = [0u8; HEADER_LEN - 1];
        let err = extract_avro_header(&buf).unwrap_err();
        assert!(err.to_string().contains("too few bytes"), "{err}");
    }

    #[mz_ore::test]
    fn rejects_wrong_header_version() {
        let mut buf = prepend_avro_header(fixture_uuid(), b"payload");
        buf[0] = 0x02;
        let err = extract_avro_header(&buf).unwrap_err();
        assert!(
            err.to_string()
                .contains("wrong Glue-style avro serialization header version"),
            "{err}"
        );
    }

    #[mz_ore::test]
    fn rejects_compressed_payload() {
        let mut buf = prepend_avro_header(fixture_uuid(), b"payload");
        buf[1] = 0x05; // zlib
        let err = extract_avro_header(&buf).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported Glue-style avro compression byte"),
            "{err}"
        );
    }

    #[mz_ore::test]
    fn empty_payload_is_legal() {
        let uuid = fixture_uuid();
        let framed = prepend_avro_header(uuid, &[]);
        let (parsed_uuid, rest) = extract_avro_header(&framed).unwrap();
        assert_eq!(parsed_uuid, uuid);
        assert!(rest.is_empty());
    }
}
