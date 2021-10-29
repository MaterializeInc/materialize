// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A permanent storage encoding for rows.
//!
//! To minimize cycles spent encoding/decoding, we use Row's internal storage
//! format, with a prefix to allow for migrations if the internal format
//! changes.
//!
//! The following is an EBNF-ish spec for the format:
//!
//! ```none
//! |   alternation
//! {}  repetition (any number of times)
//!
//! row = 0u8 v0_encoding
//!
//! v0_encoding = len_bytes row_data
//! row_data = { tagged_datum }
//!
//! tagged_datum =
//!   null |
//!   false |
//!   true |
//!   3u8 int32 |
//!   4u8 int64 |
//!   5u8 float32 |
//!   6u8 float64 |
//!   7u8 date |
//!   TODO: Finish this once 7092 lands and it's not all lies.
//!
//!   null = 0u8
//! false = 1u8
//! true = 2u8
//! int32 = u8 u8 u8 u8 (little endian)
//! uint32 = u8 u8 u8 u8 (little endian)
//! int64 = u8 u8 u8 u8 u8 u8 u8 u8 (little endian)
//! float32 = u8 u8 u8 u8 (little endian)
//! float64 = u8 u8 u8 u8 u8 u8 u8 u8 (little endian)
//! date = date_year date_ordinal
//! date_year = int32
//! date_ordinal = uint32
//! ```

use std::io::Read;

use ore::cast::CastFrom;
use persist_types::error::CodecError;
use persist_types::Codec;

use crate::Row;

const CURRENT_VERSION: u8 = 0u8;

impl Codec for Row {
    fn codec_name() -> &'static str {
        "RowExperimental"
    }

    fn size_hint(&self) -> usize {
        1 + 8 + self.data.len()
    }

    /// Encodes a row into the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::decode]. It's guaranteed to be
    /// readable by future versions of Materialize through v(TODO: Figure out
    /// our policy).
    fn encode<E: for<'a> Extend<&'a u8>>(&self, buf: &mut E) {
        buf.extend(&[CURRENT_VERSION]);
        // TODO: Storing the length here will be pretty wasteful. Revisit.
        let len: u64 = u64::cast_from(self.data.len());
        buf.extend(&len.to_le_bytes());
        buf.extend(&self.data[..]);
    }

    /// Decodes a row from the permanent storage format.
    ///
    /// This perfectly round-trips through [Row::encode]. It can read rows
    /// encoded by historical versions of Materialize back to v(TODO: Figure out
    /// our policy).
    //
    // TODO: Return a RowRef instead?
    fn decode(buf: &[u8]) -> Result<Row, CodecError> {
        let mut buf = buf;

        let mut version_raw = [0u8; 1];
        buf.read_exact(&mut version_raw[..])
            .map_err(|_| CodecError::InvalidEncodingVersion(None))?;
        // Only one version supported at the moment. This will get more
        // complicated once we change the format and have to migrate old formats
        // to the current one.
        if version_raw[0] != CURRENT_VERSION {
            return Err(CodecError::InvalidEncodingVersion(Some(
                version_raw[0] as usize,
            )));
        }

        let mut len_raw = [0u8; 8];
        buf.read_exact(&mut len_raw[..])
            .map_err(|_| CodecError::from("missing len"))?;
        let len = usize::cast_from(u64::from_le_bytes(len_raw));

        // NB: The read calls modify buf to truncate off what they read, so
        // index 0 now corresponds to the part of the original buf immediately
        // after the encoded len.
        let row_data = buf.get(0..len).ok_or_else(|| {
            CodecError::from(format!(
                "wanted {} row data bytes but had {}",
                len,
                buf.len()
            ))
        })?;

        // SAFETY: This was serialized with Row::encode at the same version.
        let row = unsafe { Row::from_bytes_unchecked(row_data.to_owned()) };
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use persist_types::error::CodecError;
    use persist_types::Codec;

    use crate::{Datum, Row};

    // TODO: datadriven golden tests for various interesting Datums and Rows to
    // catch any changes in the encoding.

    #[test]
    fn decode_errors() {
        let row = Row::pack(vec![Datum::Int64(7)]);
        let mut encoded = Vec::new();
        row.encode(&mut encoded);

        // Every subset that's missing at least one byte should error, not panic
        // or succeed.
        for i in 0..encoded.len() - 1 {
            assert!(Row::decode(&encoded[..i]).is_err());
        }

        // Sanity check that we don't just always return errors.
        assert_eq!(Row::decode(&encoded), Ok(row));

        // Check that an invalid encoding version returns the appropriate error.
        encoded[0] = u8::MAX;
        assert_eq!(
            Row::decode(&encoded),
            Err(CodecError::InvalidEncodingVersion(Some(u8::MAX as usize)))
        );
    }
}
