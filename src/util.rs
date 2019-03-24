use std::i64;
use std::io::Read;
use std::sync::{Once, ONCE_INIT};

use failure::{Error, Fail};
use serde_json::{Map, Value};

/// Maximum number of bytes that can be allocated when decoding
/// Avro-encoded values. This is a protection against ill-formed
/// data, whose length field might be interpreted as enourmous.
/// See max_allocation_bytes to change this limit.
pub static mut MAX_ALLOCATION_BYTES: usize = 512 * 1024 * 1024;
static MAX_ALLOCATION_BYTES_ONCE: Once = ONCE_INIT;

/// Describes errors happened trying to allocate too many bytes
#[derive(Fail, Debug)]
#[fail(display = "Allocation error: {}", _0)]
pub struct AllocationError(String);

impl AllocationError {
    pub fn new<S>(msg: S) -> AllocationError
    where
        S: Into<String>,
    {
        AllocationError(msg.into())
    }
}

/// Describes errors happened while decoding Avro data.
#[derive(Fail, Debug)]
#[fail(display = "Decoding error: {}", _0)]
pub struct DecodeError(String);

impl DecodeError {
    pub fn new<S>(msg: S) -> DecodeError
    where
        S: Into<String>,
    {
        DecodeError(msg.into())
    }
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

pub fn read_long<R: Read>(reader: &mut R) -> Result<i64, Error> {
    zag_i64(reader)
}

pub fn zig_i32(n: i32, buffer: &mut Vec<u8>) {
    zig_i64(n as i64, buffer)
}

pub fn zig_i64(n: i64, buffer: &mut Vec<u8>) {
    encode_variable(((n << 1) ^ (n >> 63)) as u64, buffer)
}

pub fn zag_i32<R: Read>(reader: &mut R) -> Result<i32, Error> {
    let i = zag_i64(reader)?;
    if i < i64::from(i32::min_value()) || i > i64::from(i32::max_value()) {
        Err(DecodeError::new("int out of range").into())
    } else {
        Ok(i as i32)
    }
}

pub fn zag_i64<R: Read>(reader: &mut R) -> Result<i64, Error> {
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

fn decode_variable<R: Read>(reader: &mut R) -> Result<u64, Error> {
    let mut i = 0u64;
    let mut buf = [0u8; 1];

    let mut j = 0;
    loop {
        if j > 9 {
            // if j * 7 > 64
            return Err(DecodeError::new("Overflow when decoding integer value").into());
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

pub fn safe_len(len: usize) -> Result<usize, Error> {
    let max_bytes = max_allocation_bytes(512 * 1024 * 1024);

    if len <= max_bytes {
        Ok(len)
    } else {
        Err(AllocationError::new(format!(
            "Unable to allocate {} bytes (Maximum allowed: {})",
            len, max_bytes
        ))
        .into())
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
        zig_i64(2147483647i64, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 15]);

        s.clear();
        zig_i64(2147483648i64, &mut s);
        assert_eq!(s, [128, 128, 128, 128, 16]);

        s.clear();
        zig_i64(-2147483648i64, &mut s);
        assert_eq!(s, [255, 255, 255, 255, 15]);

        s.clear();
        zig_i64(-2147483649i64, &mut s);
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
        zig_i32(1073741823i32, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 7]);

        s.clear();
        zig_i32(-1073741824i32, &mut s);
        assert_eq!(s, [255, 255, 255, 255, 7]);

        s.clear();
        zig_i32(1073741824i32, &mut s);
        assert_eq!(s, [128, 128, 128, 128, 8]);

        s.clear();
        zig_i32(-1073741825i32, &mut s);
        assert_eq!(s, [129, 128, 128, 128, 8]);

        s.clear();
        zig_i32(2147483647i32, &mut s);
        assert_eq!(s, [254, 255, 255, 255, 15]);

        s.clear();
        zig_i32(-2147483648i32, &mut s);
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
