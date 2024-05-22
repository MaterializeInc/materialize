// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{borrow::Cow, convert::TryFrom, fmt, io, marker::PhantomData, ops::Deref};

use bytes::BufMut;

use crate::{
    io::ParseBuf,
    proto::{MyDeserialize, MySerialize},
};

use super::{
    int::{IntRepr, LeU32, LeU64},
    RawConst, RawInt,
};

/// Sequence of serialized values (length serialized as `U`).
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Seq<'a, T: Clone, U>(pub Cow<'a, [T]>, PhantomData<U>);

impl<'a, T: Clone, U> Deref for Seq<'a, T, U> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl<'a, T: Clone, U> Seq<'a, T, U> {
    pub fn new(s: impl Into<Cow<'a, [T]>>) -> Self {
        Self(s.into(), PhantomData)
    }

    /// Returns true if this sequence is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns a length of this sequence.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Appends an element to this sequence.
    pub fn push(&mut self, element: T) {
        match self.0 {
            Cow::Borrowed(seq) => {
                let mut seq = seq.to_vec();
                seq.push(element);
                self.0 = Cow::Owned(seq);
            }
            Cow::Owned(ref mut seq) => {
                seq.push(element);
            }
        };
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> Seq<'static, T, U> {
        Seq(Cow::Owned(self.0.into_owned()), self.1)
    }
}

impl<'a, T: Clone, U> Default for Seq<'a, T, U> {
    fn default() -> Self {
        Seq::new(Vec::new())
    }
}

impl<T, U> MySerialize for Seq<'_, T, U>
where
    T: Clone + MySerialize,
    U: SeqRepr,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        U::serialize(&self.0, buf);
    }
}

impl<'de, T, U> MyDeserialize<'de> for Seq<'de, T, U>
where
    T: Clone + MyDeserialize<'de, Ctx = ()>,
    U: SeqRepr,
{
    const SIZE: Option<usize> = None;
    type Ctx = U::Ctx;

    fn deserialize(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        U::deserialize(ctx, &mut *buf).map(Self::new)
    }
}

/// Representation of a serialized bytes.
pub trait SeqRepr {
    /// Maximum number of items in a sequence (depends on how lenght is stored).
    const MAX_LEN: usize;
    const SIZE: Option<usize>;
    type Ctx;

    fn serialize<T: MySerialize>(seq: &[T], buf: &mut Vec<u8>);
    fn deserialize<'de, T>(ctx: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Cow<'de, [T]>>
    where
        T: Clone,
        T: MyDeserialize<'de, Ctx = ()>;
}

macro_rules! impl_seq_repr {
    ($t:ty, $name:ident) => {
        impl SeqRepr for $name {
            const MAX_LEN: usize = <$t>::MAX as usize;
            const SIZE: Option<usize> = None;
            type Ctx = ();

            fn serialize<T: MySerialize>(seq: &[T], buf: &mut Vec<u8>) {
                let len = std::cmp::min(Self::MAX_LEN, seq.len());
                <$name as IntRepr>::serialize(len as $t, &mut *buf);
                for x in seq.iter().take(len) {
                    x.serialize(&mut *buf);
                }
            }

            fn deserialize<'de, T>(
                (): Self::Ctx,
                buf: &mut ParseBuf<'de>,
            ) -> io::Result<Cow<'de, [T]>>
            where
                T: Clone,
                T: MyDeserialize<'de, Ctx = ()>,
            {
                let len = *buf.parse::<RawInt<$name>>(())? as usize;
                let mut seq = Vec::with_capacity(len);
                match T::SIZE {
                    Some(count) => {
                        let mut buf: ParseBuf = buf.parse(count * len)?;
                        for _ in 0..len {
                            seq.push(buf.parse(())?);
                        }
                    }
                    None => {
                        for _ in 0..len {
                            seq.push(buf.parse(())?);
                        }
                    }
                }
                Ok(Cow::Owned(seq))
            }
        }
    };
}

impl_seq_repr!(u64, LeU64);
impl_seq_repr!(u32, LeU32);

/// Same as `RawCons` but for a sequence of values.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct RawSeq<'a, T: IntRepr, U>(pub Cow<'a, [T::Primitive]>, PhantomData<U>);

impl<'a, T: IntRepr, U> RawSeq<'a, T, U> {
    /// Creates a new wrapper.
    pub fn new(t: impl Into<Cow<'a, [T::Primitive]>>) -> Self {
        Self(t.into(), PhantomData)
    }

    /// Returns a length of this sequence.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if the sequence has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns a `'static` version of `self`.
    pub fn into_owned(self) -> RawSeq<'static, T, U> {
        RawSeq(Cow::Owned(self.0.into_owned()), self.1)
    }
}

impl<'a, T: IntRepr, U> RawSeq<'a, T, U>
where
    T: Copy,
    U: TryFrom<T::Primitive>,
{
    /// Returns raw value at the given position.
    pub fn get(&self, index: usize) -> Option<RawConst<T, U>> {
        self.0.get(index).copied().map(RawConst::new)
    }
}

impl<'de, T: IntRepr<Primitive = u8>, U> MyDeserialize<'de> for RawSeq<'de, T, U> {
    const SIZE: Option<usize> = None;
    type Ctx = usize;

    fn deserialize(length: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let bytes: &[u8] = buf.parse(length)?;
        Ok(Self::new(bytes))
    }
}

impl<T: IntRepr<Primitive = u8>, U> MySerialize for RawSeq<'_, T, U> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.put_slice(self.0.as_ref());
    }
}

impl<T: IntRepr, U: fmt::Debug> fmt::Debug for RawSeq<'_, T, U>
where
    T: fmt::Debug,
    U: TryFrom<T::Primitive>,
    U::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0
            .iter()
            .copied()
            .map(RawConst::<T, U>::new)
            .collect::<Vec<_>>()
            .fmt(f)
    }
}
