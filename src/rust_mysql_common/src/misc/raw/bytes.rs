// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

pub use super::int::LenEnc;

use std::{borrow::Cow, cmp::min, fmt, io, marker::PhantomData};

use bytes::BufMut;

use crate::{
    io::{BufMutExt, ParseBuf},
    misc::unexpected_buf_eof,
    proto::{MyDeserialize, MySerialize},
};

use super::{int::VarLen, RawInt};

/// Wrapper for a raw byte sequence, that came from a server.
///
/// `T` encodes the serialized representation.
#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct RawBytes<'a, T: BytesRepr>(pub Cow<'a, [u8]>, PhantomData<T>);

impl<'a, T: BytesRepr> RawBytes<'a, T> {
    /// Wraps the given value.
    pub fn new(text: impl Into<Cow<'a, [u8]>>) -> Self {
        Self(text.into(), PhantomData)
    }

    /// Converts self to a 'static version.
    pub fn into_owned(self) -> RawBytes<'static, T> {
        RawBytes(Cow::Owned(self.0.into_owned()), PhantomData)
    }

    /// Returns `true` if bytes is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the _effective_ length of a string, which is no more than `T::MAX_LEN`.
    pub fn len(&self) -> usize {
        min(self.0.as_ref().len(), T::MAX_LEN)
    }

    /// Returns the _effective_ bytes (see `RawBytes::len`).
    pub fn as_bytes(&self) -> &[u8] {
        &self.0.as_ref()[..self.len()]
    }

    /// Returns the value as a UTF-8 string (lossy contverted).
    pub fn as_str(&'a self) -> Cow<'a, str> {
        String::from_utf8_lossy(self.as_bytes())
    }
}

impl<'a, T: Into<Cow<'a, [u8]>>, U: BytesRepr> From<T> for RawBytes<'a, U> {
    fn from(bytes: T) -> RawBytes<'a, U> {
        RawBytes::new(bytes)
    }
}

impl<T: BytesRepr> PartialEq<[u8]> for RawBytes<'_, T> {
    fn eq(&self, other: &[u8]) -> bool {
        self.0.as_ref().eq(other)
    }
}

impl<T: BytesRepr> MySerialize for RawBytes<'_, T> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        T::serialize(self.0.as_ref(), buf)
    }
}

impl<'de, T: BytesRepr> MyDeserialize<'de> for RawBytes<'de, T> {
    const SIZE: Option<usize> = T::SIZE;
    type Ctx = T::Ctx;

    #[inline(always)]
    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        Ok(Self(T::deserialize(ctx, buf)?, PhantomData))
    }
}

impl<T: BytesRepr> fmt::Debug for RawBytes<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RawBytes")
            .field("value", &self.as_str())
            .field(
                "max_len",
                &(if self.0.len() <= T::MAX_LEN {
                    format!("{}", T::MAX_LEN)
                } else {
                    format!("{} EXCEEDED!", T::MAX_LEN)
                }),
            )
            .finish()
    }
}

/// Representation of a serialized bytes.
pub trait BytesRepr {
    /// Maximum length of bytes for this repr (depends on how lenght is stored).
    const MAX_LEN: usize;
    const SIZE: Option<usize>;
    type Ctx;

    fn serialize(text: &[u8], buf: &mut Vec<u8>);

    /// Implementation must check the length of the buffer if `Self::SIZE.is_none()`.
    fn deserialize<'de>(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>>;
}

impl BytesRepr for LenEnc {
    const MAX_LEN: usize = usize::MAX;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        buf.put_lenenc_int(text.len() as u64);
        buf.put_slice(text);
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        let len = buf.parse::<RawInt<LenEnc>>(())?;
        buf.checked_eat(len.0 as usize)
            .map(Cow::Borrowed)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// A byte sequence prepended by it's u8 length.
///
/// `serialize` will truncate byte sequence if its too long.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct U8Bytes;

impl BytesRepr for U8Bytes {
    const MAX_LEN: usize = u8::MAX as usize;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        buf.put_u8_str(text);
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        let len: RawInt<u8> = buf.parse(())?;
        buf.checked_eat(len.0 as usize)
            .map(Cow::Borrowed)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// A byte sequence prepended by it's u32 length.
///
/// `serialize` will truncate byte sequence if its too long.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct U32Bytes;

impl BytesRepr for U32Bytes {
    const MAX_LEN: usize = u32::MAX as usize;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        buf.put_u32_str(text);
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        buf.checked_eat_u32_str()
            .map(Cow::Borrowed)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// Null-terminated byte sequence.
///
/// `deserialize()` will error with `InvalidData` if there is no `0`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NullBytes;

impl BytesRepr for NullBytes {
    const MAX_LEN: usize = usize::MAX;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        let last = text.iter().position(|x| *x == 0).unwrap_or(text.len());
        buf.put_slice(&text[..last]);
        buf.put_u8(0);
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        match buf.0.iter().position(|x| *x == 0) {
            Some(i) => {
                let out = buf.eat(i);
                buf.skip(1);
                Ok(Cow::Borrowed(out))
            }
            None => panic!("invalid bytes: {:?}", &buf.0),
        }
    }
}

/// A byte sequence that lasts from the current position to the end of the buffer.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EofBytes;

impl BytesRepr for EofBytes {
    const MAX_LEN: usize = usize::MAX;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        buf.put_slice(text);
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        Ok(Cow::Borrowed(buf.eat_all()))
    }
}

/// A byte sequence without length.
///
/// Its length is stored somewhere else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BareBytes<const MAX_LEN: usize>;

impl<const MAX_LEN: usize> BytesRepr for BareBytes<MAX_LEN> {
    const MAX_LEN: usize = MAX_LEN;
    const SIZE: Option<usize> = None;
    type Ctx = usize;

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        let len = min(text.len(), MAX_LEN);
        buf.put_slice(&text[..len]);
    }

    fn deserialize<'de>(len: usize, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        buf.checked_eat(len)
            .ok_or_else(unexpected_buf_eof)
            .map(Cow::Borrowed)
    }
}

/// `BareBytes` with `u8` len.
pub type BareU8Bytes = BareBytes<{ u8::MAX as usize }>;

/// `BareBytes` with `u16` len.
pub type BareU16Bytes = BareBytes<{ u16::MAX as usize }>;

/// A fixed length byte sequence (right-padded with `0x00`).
///
/// `serialize()` truncates the value if it's loo long.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FixedLengthText<const LEN: usize>;

impl<const LEN: usize> BytesRepr for FixedLengthText<LEN> {
    const MAX_LEN: usize = LEN;
    const SIZE: Option<usize> = Some(LEN);
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        let len = min(LEN, text.len());
        buf.put_slice(&text[..len]);
        for _ in 0..(LEN - len) {
            buf.put_u8(0);
        }
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        match Self::SIZE {
            Some(len) => Ok(Cow::Borrowed(buf.eat(len))),
            None => buf
                .checked_eat(LEN)
                .map(Cow::Borrowed)
                .ok_or_else(unexpected_buf_eof),
        }
    }
}

impl BytesRepr for VarLen {
    const MAX_LEN: usize = u32::MAX as usize;
    const SIZE: Option<usize> = None;
    type Ctx = ();

    fn serialize(text: &[u8], buf: &mut Vec<u8>) {
        buf.put_lenenc_int(text.len() as u64);
        buf.put_slice(text);
    }

    fn deserialize<'de>((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [u8]>> {
        let len = buf.parse::<RawInt<VarLen>>(())?;
        buf.checked_eat(len.0 as usize)
            .map(Cow::Borrowed)
            .ok_or_else(unexpected_buf_eof)
    }
}

/// Constantly known byte string.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConstBytes<T, const LEN: usize>(PhantomData<T>);

pub trait ConstBytesValue<const LEN: usize> {
    const VALUE: [u8; LEN];
    type Error: Default + std::error::Error + Send + Sync + 'static;
}

impl<'de, T, const LEN: usize> MyDeserialize<'de> for ConstBytes<T, LEN>
where
    T: Default,
    T: ConstBytesValue<LEN>,
{
    const SIZE: Option<usize> = Some(LEN);
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let bytes: [u8; LEN] = buf.parse_unchecked(())?;
        if bytes == T::VALUE {
            Ok(Default::default())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                T::Error::default(),
            ))
        }
    }
}

impl<T, const LEN: usize> MySerialize for ConstBytes<T, LEN>
where
    T: ConstBytesValue<LEN>,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        T::VALUE.serialize(buf)
    }
}
