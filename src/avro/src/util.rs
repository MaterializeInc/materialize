// Copyright 2018 Flavien Raynaud.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file is derived from the avro-rs project, available at
// https://github.com/flavray/avro-rs. It was incorporated
// directly into Materialize on March 3, 2020.
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

use std::i64;
use std::io::Read;
use std::sync::Once;

use serde_json::{Map, Value};

use crate::error::{DecodeError, Error as AvroError};

/// Maximum number of bytes that can be allocated when decoding
/// Avro-encoded values. This is a protection against ill-formed
/// data, whose length field might be interpreted as enormous.
/// See max_allocation_bytes to change this limit.
pub static mut MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;
static MAX_ALLOCATION_BYTES_ONCE: Once = Once::new();

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TsUnit {
    Millis,
    Micros,
}

pub trait MapHelper {
    fn string(&self, key: &str) -> Option<String>;

    fn name(&self) -> Option<String> {
        self.string("name")
    }

    fn doc(&self) -> Option<String> {
        self.string("doc")
    }
}

impl MapHelper for Map<String, Value> {
    fn string(&self, key: &str) -> Option<String> {
        self.get(key)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
    }
}

pub fn read_long<R: Read>(reader: &mut R) -> Result<i64, AvroError> {
    zag_i64(reader)
}

pub fn zig_i32(n: i32, buffer: &mut Vec<u8>) {
    zig_i64(n as i64, buffer)
}

pub fn zig_i64(n: i64, buffer: &mut Vec<u8>) {
    encode_variable(((n << 1) ^ (n >> 63)) as u64, buffer)
}

pub fn zag_i32<R: Read>(reader: &mut R) -> Result<i32, AvroError> {
    let i = zag_i64(reader)?;
    if i < i64::from(i32::min_value()) || i > i64::from(i32::max_value()) {
        Err(AvroError::Decode(DecodeError::I32OutOfRange(i)))
    } else {
        Ok(i as i32)
    }
}

pub fn zag_i64<R: Read>(reader: &mut R) -> Result<i64, AvroError> {
    let z = decode_variable(reader)?;
    Ok(if z & 0x1 == 0 {
        (z >> 1) as i64
    } else {
        !(z >> 1) as i64
    })
}

fn encode_variable(mut z: u64, buffer: &mut Vec<u8>) {
    loop {
        if z <= 0x7F {
            buffer.push((z & 0x7F) as u8);
            break;
        } else {
            buffer.push((0x80 | (z & 0x7F)) as u8);
            z >>= 7;
        }
    }
}

fn decode_variable<R: Read>(reader: &mut R) -> Result<u64, AvroError> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(AvroError::Decode(DecodeError::IntDecodeOverflow));
        }
        reader.read_exact(&mut buf[..])?;
        i |= (u64::from(buf[0] & 0x7F)) << (j * 7);
        if (buf[0] >> 7) == 0 {
            break;
        } else {
            j += 1;
        }
    }

    Ok(i)
}

/// Set a new maximum number of bytes that can be allocated when decoding data.
/// Once called, the limit cannot be changed.
///
/// **NOTE** This function must be called before decoding **any** data. The
/// library leverages [`std::sync::Once`](https://doc.rust-lang.org/std/sync/struct.Once.html)
/// to set the limit either when calling this method, or when decoding for
/// the first time.
pub fn max_allocation_bytes(num_bytes: usize) -> usize {
    unsafe {
        MAX_ALLOCATION_BYTES_ONCE.call_once(|| {
            MAX_ALLOCATION_BYTES = num_bytes;
        });
        MAX_ALLOCATION_BYTES
    }
}

pub fn safe_len(len: usize) -> Result<usize, AvroError> {
    let max_bytes = max_allocation_bytes(512 * 1024 * 1024);

    if len <= max_bytes {
        Ok(len)
    } else {
        Err(AvroError::Allocation {
            attempted: len,
            allowed: max_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zigzag() {
        let mut a = Vec::new();
        let mut b = Vec::new();
        zig_i32(42i32, &mut a);
        zig_i64(42i64, &mut b);
        assert_eq!(a, b);
    }

    #[test]
    fn test_zig_i64() {
        let mut s = Vec::new();
        zig_i64(2_147_483_647_i64, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 15]);

        s.clear();
        zig_i64(2_147_483_648_i64, &mut s);
        assert_eq!(s, [128, 128, 128, 128, 16]);

        s.clear();
        zig_i64(-2_147_483_648_i64, &mut s);
        assert_eq!(s, [255, 255, 255, 255, 15]);

        s.clear();
        zig_i64(-2_147_483_649_i64, &mut s);
        assert_eq!(s, [129, 128, 128, 128, 16]);

        s.clear();
        zig_i64(i64::MAX, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 255, 255, 255, 255, 255, 1]);

        s.clear();
        zig_i64(i64::MIN, &mut s);
        assert_eq!(s, [255, 255, 255, 255, 255, 255, 255, 255, 255, 1]);
    }

    #[test]
    fn test_zig_i32() {
        let mut s = Vec::new();
        zig_i32(1_073_741_823_i32, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 7]);

        s.clear();
        zig_i32(-1_073_741_824_i32, &mut s);
        assert_eq!(s, [255, 255, 255, 255, 7]);

        s.clear();
        zig_i32(1_073_741_824_i32, &mut s);
        assert_eq!(s, [128, 128, 128, 128, 8]);

        s.clear();
        zig_i32(-1_073_741_825_i32, &mut s);
        assert_eq!(s, [129, 128, 128, 128, 8]);

        s.clear();
        zig_i32(2_147_483_647_i32, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 15]);

        s.clear();
        zig_i32(-2_147_483_648_i32, &mut s);
        assert_eq!(s, [255, 255, 255, 255, 15]);
    }

    #[test]
    fn test_overflow() {
        let causes_left_shift_overflow: &[u8] = &[0xe1, 0xe1, 0xe1, 0xe1, 0xe1];
        assert!(decode_variable(&mut &causes_left_shift_overflow[..]).is_err());
    }

    #[test]
    fn test_safe_len() {
        assert_eq!(42usize, safe_len(42usize).unwrap());
        assert!(safe_len(1024 * 1024 * 1024).is_err());
    }
}
