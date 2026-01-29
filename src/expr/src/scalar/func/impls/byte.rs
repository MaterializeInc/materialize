// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_derive::sqlfunc;
use mz_ore::cast::CastFrom;
use mz_repr::strconv;

use crate::EvalError;

#[sqlfunc(
    sqlname = "bytea_to_text",
    preserves_uniqueness = true,
    inverse = to_unary!(super::CastStringToBytes)
)]
fn cast_bytes_to_string(a: &[u8]) -> String {
    let mut buf = String::new();
    strconv::format_bytes(&mut buf, a);
    buf
}

/// Computes the 32-bit cyclic redundancy check of the given bytea `data` using the IEEE 802.3 polynomial.
#[sqlfunc(sqlname = "crc32_bytes", category = "Hash")]
fn crc32_bytes<'a>(a: &'a [u8]) -> u32 {
    crc32fast::hash(a)
}

/// Computes the 32-bit cyclic redundancy check of the given text `data` using the IEEE 802.3 polynomial.
#[sqlfunc(sqlname = "crc32_string", category = "Hash")]
fn crc32_string<'a>(a: &'a str) -> u32 {
    crc32_bytes(a.as_bytes())
}

/// Computes the Murmur2 hash of the given bytea `data` using the seed used by Kafka's default partitioner and with the high bit cleared.
#[sqlfunc(sqlname = "kafka_murmur2_bytes", category = "Hash")]
fn kafka_murmur2_bytes<'a>(a: &'a [u8]) -> i32 {
    i32::from_ne_bytes((murmur2::murmur2(a, murmur2::KAFKA_SEED) & 0x7fffffff).to_ne_bytes())
}

/// Computes the Murmur2 hash of the given text `data` using the seed used by Kafka's default partitioner and with the high bit cleared.
#[sqlfunc(sqlname = "kafka_murmur2_string", category = "Hash")]
fn kafka_murmur2_string<'a>(a: &'a str) -> i32 {
    kafka_murmur2_bytes(a.as_bytes())
}

/// Computes the [SeaHash](https://docs.rs/seahash) hash of the given bytea `data`.
#[sqlfunc(sqlname = "seahash_bytes", category = "Hash")]
fn seahash_bytes<'a>(a: &'a [u8]) -> u64 {
    seahash::hash(a)
}

/// Computes the [SeaHash](https://docs.rs/seahash) hash of the given text `data`.
#[sqlfunc(sqlname = "seahash_string", category = "Hash")]
fn seahash_string<'a>(a: &'a str) -> u64 {
    seahash_bytes(a.as_bytes())
}

/// Returns the number of bits set in the bit string `a` (also known as _popcount_).
#[sqlfunc(sqlname = "bit_count", category = "String")]
fn bit_count_bytes<'a>(a: &'a [u8]) -> Result<i64, EvalError> {
    let count: u64 = a.iter().map(|b| u64::cast_from(b.count_ones())).sum();
    i64::try_from(count).or_else(|_| Err(EvalError::Int64OutOfRange(count.to_string().into())))
}

/// Number of bits in `a`.
#[sqlfunc(sqlname = "bit_length", category = "String")]
fn bit_length_bytes<'a>(a: &'a [u8]) -> Result<i32, EvalError> {
    let val = a.len() * 8;
    i32::try_from(val).or_else(|_| Err(EvalError::Int32OutOfRange(val.to_string().into())))
}

/// Number of bytes in `a`.
#[sqlfunc(
    sqlname = "octet_length",
    category = "String",
    alias = "length",
    url = "/sql/functions/length"
)]
fn byte_length_bytes<'a>(a: &'a [u8]) -> Result<i32, EvalError> {
    let val = a.len();
    i32::try_from(val).or_else(|_| Err(EvalError::Int32OutOfRange(val.to_string().into())))
}
