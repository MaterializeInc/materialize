// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::{LittleEndian as LE, ReadBytesExt, WriteBytesExt};
use bytes::BufMut;
use std::{cmp::min, io};

use crate::proto::MyDeserialize;

pub trait BufMutExt: BufMut {
    /// Writes an unsigned integer to self as MySql length-encoded integer.
    fn put_lenenc_int(&mut self, n: u64) {
        if n < 251 {
            self.put_u8(n as u8);
        } else if n < 65_536 {
            self.put_u8(0xFC);
            self.put_uint_le(n, 2);
        } else if n < 16_777_216 {
            self.put_u8(0xFD);
            self.put_uint_le(n, 3);
        } else {
            self.put_u8(0xFE);
            self.put_uint_le(n, 8);
        }
    }

    /// Writes a slice to self as MySql length-encoded string.
    fn put_lenenc_str(&mut self, s: &[u8]) {
        self.put_lenenc_int(s.len() as u64);
        self.put_slice(s);
    }

    /// Writes a 3-bytes unsigned integer.
    fn put_u24_le(&mut self, x: u32) {
        self.put_uint_le(x as u64, 3);
    }

    /// Writes a 3-bytes signed integer.
    fn put_i24_le(&mut self, x: i32) {
        self.put_int_le(x as i64, 3);
    }

    /// Writes a 6-bytes unsigned integer.
    fn put_u48_le(&mut self, x: u64) {
        self.put_uint_le(x, 6);
    }

    /// Writes a 7-bytes unsigned integer.
    fn put_u56_le(&mut self, x: u64) {
        self.put_uint_le(x, 7);
    }

    /// Writes a 7-bytes signed integer.
    fn put_i56_le(&mut self, x: i64) {
        self.put_int_le(x, 7);
    }

    /// Writes a string with u8 length prefix. Truncates, if the length is greater that `u8::MAX`.
    fn put_u8_str(&mut self, s: &[u8]) {
        let len = std::cmp::min(s.len(), u8::MAX as usize);
        self.put_u8(len as u8);
        self.put_slice(&s[..len]);
    }

    /// Writes a string with u32 length prefix. Truncates, if the length is greater that `u32::MAX`.
    fn put_u32_str(&mut self, s: &[u8]) {
        let len = std::cmp::min(s.len(), u32::MAX as usize);
        self.put_u32_le(len as u32);
        self.put_slice(&s[..len]);
    }
}

impl<T: BufMut> BufMutExt for T {}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ParseBuf<'a>(pub &'a [u8]);

impl io::Read for ParseBuf<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let count = min(self.0.len(), buf.len());
        (buf[..count]).copy_from_slice(&self.0[..count]);
        self.0 = &self.0[count..];
        Ok(count)
    }
}

macro_rules! eat_num {
    ($name:ident, $checked:ident, $t:ident::$fn:ident) => {
        #[doc = "Consumes a number from the head of the buffer."]
        pub fn $name(&mut self) -> $t {
            const SIZE: usize = std::mem::size_of::<$t>();
            let bytes = self.eat(SIZE);
            unsafe { $t::$fn(*(bytes as *const _ as *const [_; SIZE])) }
        }

        #[doc = "Consumes a number from the head of the buffer. Returns `None` if buffer is too small."]
        pub fn $checked(&mut self) -> Option<$t> {
            if self.len() >= std::mem::size_of::<$t>() {
                Some(self.$name())
            } else {
                None
            }
        }
    };
    ($name:ident, $checked:ident, $size:literal, $offset:literal, $t:ident::$fn:ident) => {
        #[doc = "Consumes a number from the head of the buffer."]
        pub fn $name(&mut self) -> $t {
            const SIZE: usize = $size;
            let mut x: $t = 0;
            let bytes = self.eat(SIZE);
            for (i, b) in bytes.iter().enumerate() {
                x |= (*b as $t) << ((8 * i) + (8 * $offset));
            }
            $t::$fn(x)
        }

        #[doc = "Consumes a number from the head of the buffer. Returns `None` if buffer is too small."]
        pub fn $checked(&mut self) -> Option<$t> {
            if self.len() >= $size {
                Some(self.$name())
            } else {
                None
            }
        }
    };
}

impl<'a> ParseBuf<'a> {
    /// Returns `T: MyDeserialize` deserialized from `self`.
    ///
    /// Note, that this may panic if `T::SIZE.is_some()` and less than `self.0.len()`.
    #[inline(always)]
    pub fn parse_unchecked<T>(&mut self, ctx: T::Ctx) -> io::Result<T>
    where
        T: MyDeserialize<'a>,
    {
        T::deserialize(ctx, self)
    }

    /// Checked `parse`.
    #[inline(always)]
    pub fn parse<T>(&mut self, ctx: T::Ctx) -> io::Result<T>
    where
        T: MyDeserialize<'a>,
    {
        match T::SIZE {
            Some(size) => {
                let mut buf: ParseBuf = self.parse_unchecked(size)?;
                buf.parse_unchecked(ctx)
            }
            None => self.parse_unchecked(ctx),
        }
    }

    /// Returns true if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of bytes in the buffer.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Skips the given number of bytes.
    ///
    /// Afterwards self contains elements `[cnt, len)`.
    pub fn skip(&mut self, cnt: usize) {
        self.0 = &self.0[cnt..];
    }

    /// Same as `skip` but returns `false` if buffer is too small.
    pub fn checked_skip(&mut self, cnt: usize) -> bool {
        if self.len() >= cnt {
            self.skip(cnt);
            true
        } else {
            false
        }
    }

    /// Splits the buffer into two at the given index. Returns elements `[0, n)`.
    ///
    /// Afterwards self contains elements `[n, len)`.
    ///
    /// # Panic
    ///
    /// Will panic if `n > self.len()`.
    pub fn eat(&mut self, n: usize) -> &'a [u8] {
        let (left, right) = self.0.split_at(n);
        self.0 = right;
        left
    }

    pub fn eat_buf(&mut self, n: usize) -> Self {
        Self(self.eat(n))
    }

    /// Same as `eat`. Returns `None` if buffer is too small.
    pub fn checked_eat(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.len() >= n {
            Some(self.eat(n))
        } else {
            None
        }
    }

    pub fn checked_eat_buf(&mut self, n: usize) -> Option<Self> {
        Some(Self(self.checked_eat(n)?))
    }

    pub fn eat_all(&mut self) -> &'a [u8] {
        self.eat(self.len())
    }

    eat_num!(eat_u8, checked_eat_u8, u8::from_le_bytes);
    eat_num!(eat_i8, checked_eat_i8, i8::from_le_bytes);
    eat_num!(eat_u16_le, checked_eat_u16_le, u16::from_le_bytes);
    eat_num!(eat_i16_le, checked_eat_i16_le, i16::from_le_bytes);
    eat_num!(eat_u16_be, checked_eat_u16_be, u16::from_be_bytes);
    eat_num!(eat_i16_be, checked_eat_i16_be, i16::from_be_bytes);
    eat_num!(eat_u24_le, checked_eat_u24_le, 3, 0, u32::from_le);
    eat_num!(eat_i24_le, checked_eat_i24_le, 3, 0, i32::from_le);
    eat_num!(eat_u24_be, checked_eat_u24_be, 3, 1, u32::from_be);
    eat_num!(eat_i24_be, checked_eat_i24_be, 3, 1, i32::from_be);
    eat_num!(eat_u32_le, checked_eat_u32_le, u32::from_le_bytes);
    eat_num!(eat_i32_le, checked_eat_i32_le, i32::from_le_bytes);
    eat_num!(eat_u32_be, checked_eat_u32_be, u32::from_be_bytes);
    eat_num!(eat_i32_be, checked_eat_i32_be, i32::from_be_bytes);
    eat_num!(eat_u40_le, checked_eat_u40_le, 5, 0, u64::from_le);
    eat_num!(eat_i40_le, checked_eat_i40_le, 5, 0, i64::from_le);
    eat_num!(eat_u40_be, checked_eat_u40_be, 5, 3, u64::from_be);
    eat_num!(eat_i40_be, checked_eat_i40_be, 5, 3, i64::from_be);
    eat_num!(eat_u48_le, checked_eat_u48_le, 6, 0, u64::from_le);
    eat_num!(eat_i48_le, checked_eat_i48_le, 6, 0, i64::from_le);
    eat_num!(eat_u48_be, checked_eat_u48_be, 6, 2, u64::from_be);
    eat_num!(eat_i48_be, checked_eat_i48_be, 6, 2, i64::from_be);
    eat_num!(eat_u56_le, checked_eat_u56_le, 7, 0, u64::from_le);
    eat_num!(eat_i56_le, checked_eat_i56_le, 7, 0, i64::from_le);
    eat_num!(eat_u56_be, checked_eat_u56_be, 7, 1, u64::from_be);
    eat_num!(eat_i56_be, checked_eat_i56_be, 7, 1, i64::from_be);
    eat_num!(eat_u64_le, checked_eat_u64_le, u64::from_le_bytes);
    eat_num!(eat_i64_le, checked_eat_i64_le, i64::from_le_bytes);
    eat_num!(eat_u64_be, checked_eat_u64_be, u64::from_be_bytes);
    eat_num!(eat_i64_be, checked_eat_i64_be, i64::from_be_bytes);
    eat_num!(eat_u128_le, checked_eat_u128_le, u128::from_le_bytes);
    eat_num!(eat_i128_le, checked_eat_i128_le, i128::from_le_bytes);
    eat_num!(eat_u128_be, checked_eat_u128_be, u128::from_be_bytes);
    eat_num!(eat_i128_be, checked_eat_i128_be, i128::from_be_bytes);

    eat_num!(eat_f32_le, checked_eat_f32_le, f32::from_le_bytes);
    eat_num!(eat_f32_be, checked_eat_f32_be, f32::from_be_bytes);

    eat_num!(eat_f64_le, checked_eat_f64_le, f64::from_le_bytes);
    eat_num!(eat_f64_be, checked_eat_f64_be, f64::from_be_bytes);

    /// Consumes MySql length-encoded integer from the head of the buffer.
    ///
    /// Returns `0` if integer is maliformed (starts with 0xff or 0xfb). First byte will be eaten.
    pub fn eat_lenenc_int(&mut self) -> u64 {
        match self.eat_u8() {
            x @ 0..=0xfa => x as u64,
            0xfc => self.eat_u16_le() as u64,
            0xfd => self.eat_u24_le() as u64,
            0xfe => self.eat_u64_le(),
            0xfb | 0xff => 0,
        }
    }

    /// Same as `eat_lenenc_int`. Returns `None` if buffer is too small.
    pub fn checked_eat_lenenc_int(&mut self) -> Option<u64> {
        match self.checked_eat_u8()? {
            x @ 0..=0xfa => Some(x as u64),
            0xfc => self.checked_eat_u16_le().map(|x| x as u64),
            0xfd => self.checked_eat_u24_le().map(|x| x as u64),
            0xfe => self.checked_eat_u64_le(),
            0xfb | 0xff => Some(0),
        }
    }

    /// Consumes MySql length-encoded string from the head of the buffer.
    ///
    /// Returns an empty slice if length is maliformed (starts with 0xff). First byte will be eaten.
    pub fn eat_lenenc_str(&mut self) -> &'a [u8] {
        let len = self.eat_lenenc_int();
        self.eat(len as usize)
    }

    /// Same as `eat_lenenc_str`. Returns `None` if buffer is too small.
    pub fn checked_eat_lenenc_str(&mut self) -> Option<&'a [u8]> {
        let len = self.checked_eat_lenenc_int()?;
        self.checked_eat(len as usize)
    }

    /// Consumes MySql string with u8 length prefix from the head of the buffer.
    pub fn eat_u8_str(&mut self) -> &'a [u8] {
        let len = self.eat_u8();
        self.eat(len as usize)
    }

    /// Same as `eat_u8_str`. Returns `None` if buffer is too small.
    pub fn checked_eat_u8_str(&mut self) -> Option<&'a [u8]> {
        let len = self.checked_eat_u8()?;
        self.checked_eat(len as usize)
    }

    /// Consumes MySql string with u32 length prefix from the head of the buffer.
    pub fn eat_u32_str(&mut self) -> &'a [u8] {
        let len = self.eat_u32_le();
        self.eat(len as usize)
    }

    /// Same as `eat_u32_str`. Returns `None` if buffer is too small.
    pub fn checked_eat_u32_str(&mut self) -> Option<&'a [u8]> {
        let len = self.checked_eat_u32_le()?;
        self.checked_eat(len as usize)
    }

    /// Consumes null-terminated string from the head of the buffer.
    ///
    /// Consumes whole buffer if there is no `0`-byte.
    pub fn eat_null_str(&mut self) -> &'a [u8] {
        let pos = self
            .0
            .iter()
            .position(|x| *x == 0)
            .map(|x| x + 1)
            .unwrap_or_else(|| self.len());
        match self.eat(pos) {
            [head @ .., 0_u8] => head,
            x => x,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Invalid length-encoded integer value (starts with 0xfb|0xff)")]
pub struct InvalidLenghEncodedInteger;

pub trait ReadMysqlExt: ReadBytesExt {
    /// Reads MySql's length-encoded integer.
    fn read_lenenc_int(&mut self) -> io::Result<u64> {
        match self.read_u8()? {
            x if x <= 0xfa => Ok(x.into()),
            0xfc => self.read_uint::<LE>(2),
            0xfd => self.read_uint::<LE>(3),
            0xfe => self.read_uint::<LE>(8),
            0xfb | 0xff => Err(io::Error::new(
                io::ErrorKind::Other,
                InvalidLenghEncodedInteger,
            )),
            _ => unreachable!(),
        }
    }

    /// Reads MySql's length-encoded string.
    fn read_lenenc_str(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_lenenc_int()?;
        let mut output = vec![0_u8; len as usize];
        self.read_exact(&mut output)?;
        Ok(output)
    }
}

pub trait WriteMysqlExt: WriteBytesExt {
    /// Writes MySql's length-encoded integer.
    fn write_lenenc_int(&mut self, x: u64) -> io::Result<u64> {
        if x < 251 {
            self.write_u8(x as u8)?;
            Ok(1)
        } else if x < 65_536 {
            self.write_u8(0xFC)?;
            self.write_uint::<LE>(x, 2)?;
            Ok(3)
        } else if x < 16_777_216 {
            self.write_u8(0xFD)?;
            self.write_uint::<LE>(x, 3)?;
            Ok(4)
        } else {
            self.write_u8(0xFE)?;
            self.write_uint::<LE>(x, 8)?;
            Ok(9)
        }
    }

    /// Writes MySql's length-encoded string.
    fn write_lenenc_str(&mut self, bytes: &[u8]) -> io::Result<u64> {
        let written = self.write_lenenc_int(bytes.len() as u64)?;
        self.write_all(bytes)?;
        Ok(written + bytes.len() as u64)
    }
}

impl<T> ReadMysqlExt for T where T: ReadBytesExt {}
impl<T> WriteMysqlExt for T where T: WriteBytesExt {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn be_le() {
        let buf = ParseBuf(&[0, 1, 2]);
        assert_eq!(buf.clone().eat_u24_le(), 0x00020100);
        assert_eq!(buf.clone().eat_u24_be(), 0x00000102);
        let buf = ParseBuf(&[0, 1, 2, 3, 4]);
        assert_eq!(buf.clone().eat_u40_le(), 0x0000000403020100);
        assert_eq!(buf.clone().eat_u40_be(), 0x0000000001020304);
        let buf = ParseBuf(&[0, 1, 2, 3, 4, 5]);
        assert_eq!(buf.clone().eat_u48_le(), 0x0000050403020100);
        assert_eq!(buf.clone().eat_u48_be(), 0x0000000102030405);
        let buf = ParseBuf(&[0, 1, 2, 3, 4, 5, 6]);
        assert_eq!(buf.clone().eat_u56_le(), 0x0006050403020100);
        assert_eq!(buf.clone().eat_u56_be(), 0x0000010203040506);
    }
}
