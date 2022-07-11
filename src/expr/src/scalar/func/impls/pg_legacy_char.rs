// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str;

use mz_ore::fmt::FormatBuffer;
use mz_repr::adt::system::PgLegacyChar;

use crate::EvalError;

pub fn format_pg_legacy_char<B>(buf: &mut B, c: u8) -> Result<(), EvalError>
where
    B: FormatBuffer,
{
    // PostgreSQL is willing to hold invalid UTF-8 in a `Datum::String`, but
    // we are not.
    match str::from_utf8(&[c]) {
        Ok(s) => {
            buf.write_str(s);
            Ok(())
        }
        Err(_) => Err(EvalError::InvalidByteSequence {
            byte_sequence: format!("{:#02x}", c),
            encoding_name: "UTF8".into(),
        }),
    }
}

sqlfunc!(
    #[sqlname = "\"char\"_to_text"]
    #[preserves_uniqueness = true]
    fn cast_pg_legacy_char_to_string(a: PgLegacyChar) -> Result<String, EvalError> {
        let mut buf = String::new();
        format_pg_legacy_char(&mut buf, a.0)?;
        Ok(buf)
    }
);

sqlfunc!(
    #[sqlname = "\"char\"_to_integer"]
    #[preserves_uniqueness = true]
    fn cast_pg_legacy_char_to_int32(a: PgLegacyChar) -> i32 {
        // Per PostgreSQL, casts to `i32` are performed as if `PgLegacyChar` is
        // signed.
        // See: https://github.com/postgres/postgres/blob/791b1b71da35d9d4264f72a87e4078b85a2fcfb4/src/backend/utils/adt/char.c#L91-L96
        i32::from(i8::from_ne_bytes([a.0]))
    }
);
