// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka's zigzag variable-length integers (protobuf `sint64` encoding).
//!
//! Values in `i32` range encode identically as varint and varlong, so the
//! encoder only has a varlong form.

use mz_ore::cast::ReinterpretCast;

use crate::internal::compression::CodecError;

fn zigzag(v: i64) -> u64 {
    u64::reinterpret_cast((v << 1) ^ (v >> 63))
}

fn unzigzag(z: u64) -> i64 {
    i64::reinterpret_cast(z >> 1) ^ -i64::reinterpret_cast(z & 1)
}

/// The encoded size of `v`, 1 to 10 bytes.
pub(crate) fn varlong_size(v: i64) -> usize {
    let bits = 64 - (zigzag(v) | 1).leading_zeros();
    // At most 10, so the conversion cannot fail.
    usize::try_from(bits.div_ceil(7)).expect("at most 10")
}

/// Appends the encoding of `v` to `buf`.
pub(crate) fn put_varlong(buf: &mut Vec<u8>, v: i64) {
    let mut z = zigzag(v);
    while z >= 0x80 {
        buf.push(z.to_le_bytes()[0] | 0x80);
        z >>= 7;
    }
    buf.push(z.to_le_bytes()[0]);
}

/// Decodes a varlong at `*pos`, advancing `*pos` past it.
///
/// Like Kafka's `ByteUtils.readVarlong`, at most 10 bytes are read.
#[inline]
pub(crate) fn read_varlong(data: &[u8], pos: &mut usize) -> Result<i64, CodecError> {
    let mut z: u64 = 0;
    let mut shift = 0;
    loop {
        let Some(&b) = data.get(*pos) else {
            return Err(CodecError::malformed("truncated varint"));
        };
        *pos += 1;
        z |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(unzigzag(z));
        }
        shift += 7;
        if shift > 63 {
            return Err(CodecError::malformed("varint longer than 10 bytes"));
        }
    }
}

/// Decodes a varint at `*pos`, advancing `*pos` past it. Fails if the value
/// does not fit in an `i32`.
#[inline]
pub(crate) fn read_varint(data: &[u8], pos: &mut usize) -> Result<i32, CodecError> {
    // Fast path: most lengths and deltas fit in one byte.
    if let Some(&b) = data.get(*pos) {
        if b & 0x80 == 0 {
            *pos += 1;
            let b = i32::from(b);
            return Ok((b >> 1) ^ -(b & 1));
        }
    }
    let v = read_varlong(data, pos)?;
    i32::try_from(v).map_err(|_| CodecError::malformed("varint out of int32 range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Known zigzag encodings, as Kafka's `ByteUtils.writeVarlong` writes them.
    #[mz_ore::test]
    fn known_encodings() {
        let cases: [(i64, &[u8]); 10] = [
            (0, &[0x00]),
            (-1, &[0x01]),
            (1, &[0x02]),
            (63, &[0x7e]),
            (-64, &[0x7f]),
            (64, &[0x80, 0x01]),
            (-65, &[0x81, 0x01]),
            (8191, &[0xfe, 0x7f]),
            (-8192, &[0xff, 0x7f]),
            (8192, &[0x80, 0x80, 0x01]),
        ];
        for (v, expected) in cases {
            let mut buf = Vec::new();
            put_varlong(&mut buf, v);
            assert_eq!(buf, expected, "{v}");
            assert_eq!(varlong_size(v), expected.len(), "{v}");
            let mut pos = 0;
            assert_eq!(read_varlong(expected, &mut pos), Ok(v));
            assert_eq!(pos, expected.len());
            let mut pos = 0;
            assert_eq!(
                read_varint(expected, &mut pos),
                Ok(i32::try_from(v).unwrap())
            );
            assert_eq!(pos, expected.len());
        }
        let mut buf = Vec::new();
        put_varlong(&mut buf, i64::from(i32::MAX));
        assert_eq!(buf, [0xfe, 0xff, 0xff, 0xff, 0x0f]);
        buf.clear();
        put_varlong(&mut buf, i64::from(i32::MIN));
        assert_eq!(buf, [0xff, 0xff, 0xff, 0xff, 0x0f]);
        buf.clear();
        put_varlong(&mut buf, i64::MIN);
        assert_eq!(
            buf,
            [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
        );
    }

    #[mz_ore::test]
    fn round_trip_edges() {
        let mut values = vec![
            0,
            1,
            -1,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
            i64::from(i32::MAX) + 1,
            i64::from(i32::MIN) - 1,
            i64::MAX,
            i64::MIN,
        ];
        for shift in 0..64 {
            let v = 1_i64 << shift;
            values.extend([v, v.wrapping_neg(), v.wrapping_sub(1)]);
        }
        for v in values {
            let mut buf = vec![0xaa];
            put_varlong(&mut buf, v);
            assert_eq!(buf.len() - 1, varlong_size(v), "{v}");
            let mut pos = 1;
            assert_eq!(read_varlong(&buf, &mut pos), Ok(v));
            assert_eq!(pos, buf.len());
            let mut pos = 1;
            match i32::try_from(v) {
                Ok(v32) => assert_eq!(read_varint(&buf, &mut pos), Ok(v32)),
                Err(_) => assert!(read_varint(&buf, &mut pos).is_err()),
            }
            // Every strict prefix is truncated.
            for end in 1..buf.len() {
                let mut pos = 1;
                assert!(read_varlong(&buf[..end], &mut pos).is_err(), "{v} {end}");
            }
        }
    }

    #[mz_ore::test]
    fn too_long() {
        let eleven = [0x80; 11];
        assert!(read_varlong(&eleven, &mut 0).is_err());
        assert!(read_varint(&eleven, &mut 0).is_err());
        assert!(read_varlong(&[], &mut 0).is_err());
        assert!(read_varint(&[], &mut 0).is_err());
    }
}
