// Copyright (c) 2021 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use std::{
    convert::TryFrom,
    fmt, io,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::{
    io::ParseBuf,
    proto::{MyDeserialize, MySerialize},
};

use super::{int::IntRepr, RawInt};

/// Same as `RawConst<T, U>` but holds `U` instead of `T`, i.e. holds the parsed value.
///
/// `MyDeserialize::deserialize` will error with `io::ErrorKind::InvalidData`
/// if `T::try_from(_: U::Primitive)` fails.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Const<T, U>(pub T, PhantomData<U>);

impl<T, U> Const<T, U> {
    /// Creates a new `Const`.
    pub fn new(t: T) -> Self {
        Self(t, PhantomData)
    }
}

impl<T, U> Deref for Const<T, U> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T, U> DerefMut for Const<T, U> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<'de, T, U> MyDeserialize<'de> for Const<T, U>
where
    U: IntRepr,
    T: TryFrom<U::Primitive>,
    <T as TryFrom<U::Primitive>>::Error: std::error::Error + Send + Sync + 'static,
{
    const SIZE: Option<usize> = U::SIZE;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let raw_val = buf.parse_unchecked::<RawInt<U>>(())?;
        T::try_from(*raw_val)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
            .map(Const::new)
    }
}

impl<T, U> MySerialize for Const<T, U>
where
    T: Copy,
    T: Into<U::Primitive>,
    U: IntRepr,
{
    fn serialize(&self, buf: &mut Vec<u8>) {
        RawInt::<U>::new(self.0.into()).serialize(buf);
    }
}

/// Wrapper for a raw value of a MySql constant, enum variant or flags value.
///
/// * `T` – specifies the raw value,
/// * `U` – specifies the parsed value.
#[derive(Clone, Default, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct RawConst<T: IntRepr, U>(pub T::Primitive, PhantomData<U>);

impl<T: IntRepr, U> RawConst<T, U> {
    /// Creates a new wrapper.
    pub fn new(t: T::Primitive) -> Self {
        Self(t, PhantomData)
    }
}

impl<T: IntRepr, U> Deref for RawConst<T, U> {
    type Target = T::Primitive;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: IntRepr, U> DerefMut for RawConst<T, U> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: IntRepr, U> RawConst<T, U>
where
    T::Primitive: Copy,
    U: TryFrom<T::Primitive>,
{
    /// Tries to parse the raw value as `U`.
    pub fn get(&self) -> Result<U, U::Error> {
        U::try_from(self.0)
    }
}

impl<T: IntRepr, U> fmt::Debug for RawConst<T, U>
where
    T: fmt::Debug,
    T::Primitive: Copy,
    U: fmt::Debug + TryFrom<T::Primitive>,
    U::Error: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match U::try_from(self.0) {
            Ok(u) => u.fmt(f),
            Err(t) => write!(
                f,
                "Unknown value for type {}: {:?}",
                std::any::type_name::<U>(),
                t
            ),
        }
    }
}

impl<'de, T: IntRepr, U> MyDeserialize<'de> for RawConst<T, U> {
    const SIZE: Option<usize> = T::SIZE;
    type Ctx = ();

    fn deserialize((): Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let value = buf.parse_unchecked::<RawInt<T>>(())?.0;
        Ok(Self::new(value))
    }
}

impl<T: IntRepr, U> MySerialize for RawConst<T, U> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        RawInt::<T>::new(self.0).serialize(buf);
    }
}
