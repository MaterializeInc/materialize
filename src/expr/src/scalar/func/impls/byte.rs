// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::strconv;

use crate::EvalError;

sqlfunc!(
    #[sqlname = "bytea_to_text"]
    #[preserves_uniqueness = true]
    fn cast_bytes_to_string(a: &'a [u8]) -> String {
        let mut buf = String::new();
        strconv::format_bytes(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "bit_length"]
    fn bit_length_bytes<'a>(a: &'a [u8]) -> Result<i32, EvalError> {
        i32::try_from(a.len() * 8).or(Err(EvalError::Int32OutOfRange))
    }
);

sqlfunc!(
    #[sqlname = "octet_length"]
    fn byte_length_bytes<'a>(a: &'a [u8]) -> Result<i32, EvalError> {
        i32::try_from(a.len()).or(Err(EvalError::Int32OutOfRange))
    }
);
