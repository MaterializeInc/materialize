// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use bytes::BufMut;

use std::{
    fmt,
    hash::Hash,
    io,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::{
    io::{BufMutExt, ParseBuf},
    proto::{MyDeserialize, MySerialize},
};

/// Wrapper for an integer, that defines serialization and deserialization.
#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct RawInt<T: IntRepr>(pub T::Primitive, PhantomData<T>);

impl<T: IntRepr> fmt::Debug for RawInt<T>
where
    T::Primitive: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: IntRepr> RawInt<T> {
    pub fn new(x: T::Primitive) -> Self {
        Self(x, PhantomData)
    }
}

impl<T: IntRepr> Deref for RawInt<T> {
    type Target = T::Primitive;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: IntRepr> DerefMut for RawInt<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'de, T: IntRepr> MyDeserialize<'de> for RawInt<T> {
    const SIZE: Option<usize> = T::SIZE;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        T::deserialize(buf).map(Self::new)
    }
}

impl<T: IntRepr> MySerialize for RawInt<T> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        T::serialize(self.0, buf);
    }
}

/// Serialized representation of an integer.
pub trait IntRepr {
    const SIZE: Option<usize>;
    type Primitive: fmt::Debug + Default + Copy + Eq + Ord + Hash;

    fn serialize(val: Self::Primitive, buf: &mut Vec<u8>);
    fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive>;
}

impl IntRepr for u8 {
    const SIZE: Option<usize> = Some(1);
    type Primitive = Self;

    fn serialize(val: Self::Primitive, buf: &mut Vec<u8>) {
        buf.put_u8(val)
    }

    fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
        Ok(buf.eat_u8())
    }
}

impl IntRepr for i8 {
    const SIZE: Option<usize> = Some(1);
    type Primitive = Self;

    fn serialize(val: Self::Primitive, buf: &mut Vec<u8>) {
        buf.put_i8(val)
    }

    fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
        Ok(buf.eat_i8())
    }
}

macro_rules! def_end_repr {
    ($( $(#[$m:meta])* $name:ident, $t:ty, $size:expr, $ser:ident, $de:ident; )+) => {
        $(
            $(#[$m])*
            ///
            /// # Panic
            ///
            /// Note, that `IntPepr::deserialize` won't check buffer length.
            #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct $name;

            impl IntRepr for $name {
                const SIZE: Option<usize> = $size;
                type Primitive = $t;

                fn serialize(val: Self::Primitive, buf: &mut Vec<u8>) {
                    buf.$ser(val)
                }

                fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
                    Ok(buf.$de())
                }
            }
        )+
    };
    ($( $(#[$m:meta])* checked $name:ident, $t:ty, $size:expr, $ser:ident, $de:ident; )+) => {
        $(
            $(#[$m])*
            #[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct $name;

            impl IntRepr for $name {
                const SIZE: Option<usize> = $size;
                type Primitive = $t;

                fn serialize(val: Self::Primitive, buf: &mut Vec<u8>) {
                    buf.$ser(val)
                }

                fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
                    buf.$de().ok_or_else(crate::misc::unexpected_buf_eof)
                }
            }
        )+
    };
}

def_end_repr! {
    /// Little-endian u16.
    LeU16, u16, Some(2), put_u16_le, eat_u16_le;
    /// Little-endian u24.
    LeU24, u32, Some(3), put_u24_le, eat_u24_le;
    /// Little-endian u32.
    LeU32, u32, Some(4), put_u32_le, eat_u32_le;
    /// Little-endian u48.
    LeU48, u64, Some(6), put_u48_le, eat_u48_le;
    /// Little-endian u56.
    LeU56, u64, Some(7), put_u56_le, eat_u56_le;
    /// Little-endian u64.
    LeU64, u64, Some(8), put_u64_le, eat_u64_le;
    /// Little-endian i16.
    LeI16, i16, Some(2), put_i16_le, eat_i16_le;
    /// Little-endian i24.
    LeI24, i32, Some(3), put_i24_le, eat_i24_le;
    /// Little-endian i32.
    LeI32, i32, Some(4), put_i32_le, eat_i32_le;
    /// Little-endian i56.
    LeI56, i64, Some(7), put_i56_le, eat_i56_le;
    /// Little-endian i64.
    LeI64, i64, Some(8), put_i64_le, eat_i64_le;
}

def_end_repr! {
    /// Length-encoded integer.
    checked LenEnc, u64, None, put_lenenc_int, checked_eat_lenenc_int;
}

/// Lower 2 bytes of a little-endian u32.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeU32LowerHalf;

impl IntRepr for LeU32LowerHalf {
    const SIZE: Option<usize> = Some(2);
    type Primitive = u32;

    fn serialize(val: Self::Primitive, buf: &mut Vec<u8>) {
        LeU16::serialize((val & 0x0000_FFFF) as u16, buf);
    }

    fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
        LeU16::deserialize(buf).map(|x| x as u32)
    }
}

/// Upper 2 bytes of a little-endian u32.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeU32UpperHalf;

impl IntRepr for LeU32UpperHalf {
    const SIZE: Option<usize> = Some(2);
    type Primitive = u32;

    fn serialize(val: Self::Primitive, buf: &mut Vec<u8>) {
        LeU16::serialize((val >> 16) as u16, buf);
    }

    fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
        LeU16::deserialize(buf).map(|x| (x as u32) << 16)
    }
}

/// Constant u8 value.
///
/// `T` is an error type if parsed value does not match.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstU8<T, const N: u8>(PhantomData<T>);

impl<T, const N: u8> ConstU8<T, N> {
    pub const fn new() -> Self {
        Self(PhantomData)
    }

    pub const fn value(&self) -> u8 {
        N
    }
}

impl<'de, T, const N: u8> MyDeserialize<'de> for ConstU8<T, N>
where
    T: std::error::Error + Send + Sync + 'static,
    T: Default,
{
    const SIZE: Option<usize> = Some(1);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        if buf.eat_u8() == N {
            Ok(Self(PhantomData))
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, T::default()))
        }
    }
}

impl<T, const N: u8> MySerialize for ConstU8<T, N> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u8(N);
    }
}

/// Constant u8 value.
///
/// `T` is an error type if parsed value does not match.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstU32<T, const N: u32>(PhantomData<T>);

impl<T, const N: u32> ConstU32<T, N> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<'de, T, const N: u32> MyDeserialize<'de> for ConstU32<T, N>
where
    T: std::error::Error + Send + Sync + 'static,
    T: Default,
{
    const SIZE: Option<usize> = Some(4);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        if buf.eat_u32_le() == N {
            Ok(Self(PhantomData))
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidData, T::default()))
        }
    }
}

impl<T, const N: u32> MySerialize for ConstU32<T, N> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(N);
    }
}

/// Varialbe-length integer (used within JSONB).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VarLen;

impl IntRepr for VarLen {
    const SIZE: Option<usize> = None;
    type Primitive = u32;

    fn serialize(mut val: Self::Primitive, buf: &mut Vec<u8>) {
        loop {
            let mut byte = (val & 0x7F) as u8;
            val >>= 7;
            if val != 0 {
                byte |= 0x80;
                buf.put_u8(byte);
                break;
            } else {
                buf.put_u8(byte);
            }
        }
    }

    fn deserialize(buf: &mut ParseBuf<'_>) -> io::Result<Self::Primitive> {
        // variable-length integer should take up to 5 bytes
        const MAX_REPR_LEN: usize = 5;

        let mut len = 0_u64;
        for i in 0..MAX_REPR_LEN {
            let byte = *buf.parse::<RawInt<u8>>(())? as u64;
            len |= (byte & 0x7f) << (7 * i);
            if byte & 0x80 == 0 {
                if len > (u32::MAX as u64) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid variable-length value (> u32::MAX)",
                    ));
                }
                return Ok(len as u32);
            }
        }

        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid variable-length value (more than 5 bytes)",
        ))
    }
}
