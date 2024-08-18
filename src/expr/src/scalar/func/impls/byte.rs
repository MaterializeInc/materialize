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
    #[inverse = to_unary!(super::CastStringToBytes)]
    fn cast_bytes_to_string(a: &'a [u8]) -> String {
        let mut buf = String::new();
        strconv::format_bytes(&mut buf, a);
        buf
    }
);

sqlfunc!(
    #[sqlname = "crc32_bytes"]
    fn crc32_bytes<'a>(a: &'a [u8]) -> u32 {
        crc32fast::hash(a)
    }
);

sqlfunc!(
    #[sqlname = "crc32_string"]
    fn crc32_string<'a>(a: &'a str) -> u32 {
        crc32_bytes(a.as_bytes())
    }
);

sqlfunc!(
    #[sqlname = "kafka_murmur2_bytes"]
    fn kafka_murmur2_bytes<'a>(a: &'a [u8]) -> i32 {
        i32::from_ne_bytes((murmur2::murmur2(a, murmur2::KAFKA_SEED) & 0x7fffffff).to_ne_bytes())
    }
);

sqlfunc!(
    #[sqlname = "kafka_murmur2_string"]
    fn kafka_murmur2_string<'a>(a: &'a str) -> i32 {
        kafka_murmur2_bytes(a.as_bytes())
    }
);

sqlfunc!(
    #[sqlname = "seahash_bytes"]
    fn seahash_bytes<'a>(a: &'a [u8]) -> u64 {
        seahash::hash(a)
    }
);

sqlfunc!(
    #[sqlname = "seahash_string"]
    fn seahash_string<'a>(a: &'a str) -> u64 {
        seahash_bytes(a.as_bytes())
    }
);

sqlfunc!(
    #[sqlname = "bit_length"]
    fn bit_length_bytes<'a>(a: &'a [u8]) -> Result<i32, EvalError> {
        let val = a.len() * 8;
        i32::try_from(val).or(Err(EvalError::Int32OutOfRange(val.to_string())))
    }
);

sqlfunc!(
    #[sqlname = "octet_length"]
    fn byte_length_bytes<'a>(a: &'a [u8]) -> Result<i32, EvalError> {
        let val = a.len();
        i32::try_from(val).or(Err(EvalError::Int32OutOfRange(val.to_string())))
    }
);
