// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.

use mz_ore::cast::CastFrom;
use proptest::prelude::Strategy;
use std::{char::CharTryFromError, num::TryFromIntError};
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/mz_repr.proto.rs"));

/// An error thrown when trying to convert from a `*.proto`-generated type
/// `Proto$T` to `$T`.
#[derive(Debug)]
pub enum TryFromProtoError {
    /// A wrapped [`TryFromIntError`] due to failed integer downcast.
    TryFromIntError(TryFromIntError),
    /// A wrapped [`CharTryFromError`] due to failed [`char`] conversion.
    CharTryFromError(CharTryFromError),
    /// A date conversion failed
    DateConversionError(String),
    /// A regex compilation failed
    RegexError(regex::Error),
    /// A [crate::Row] conversion failed
    RowConversionError(String),
    /// A JSON deserialization failed.
    /// TODO: Remove this when we have complete coverage for source and sink structs.
    DeserializationError(serde_json::Error),
    /// Indicates an `Option<U>` field in the `Proto$T` that should be set,
    /// but for some reason it is not. In practice this should never occur.
    MissingField(String),
    /// Indicates that the serialized ShardId value failed to deserialize, according
    /// to its custom deserialization logic.
    InvalidShardId(String),
    /// Failed to parse a serialized URI
    InvalidUri(http::uri::InvalidUri),
    /// Failed to read back a serialized Glob
    GlobError(globset::Error),
}

impl TryFromProtoError {
    /// Construct a new [`TryFromProtoError::MissingField`] instance.
    pub fn missing_field<T: ToString>(s: T) -> TryFromProtoError {
        TryFromProtoError::MissingField(s.to_string())
    }
}

impl From<TryFromIntError> for TryFromProtoError {
    fn from(error: TryFromIntError) -> Self {
        TryFromProtoError::TryFromIntError(error)
    }
}

impl From<CharTryFromError> for TryFromProtoError {
    fn from(error: CharTryFromError) -> Self {
        TryFromProtoError::CharTryFromError(error)
    }
}

impl From<regex::Error> for TryFromProtoError {
    fn from(error: regex::Error) -> Self {
        TryFromProtoError::RegexError(error)
    }
}

impl From<serde_json::Error> for TryFromProtoError {
    fn from(error: serde_json::Error) -> Self {
        TryFromProtoError::DeserializationError(error)
    }
}

impl From<http::uri::InvalidUri> for TryFromProtoError {
    fn from(error: http::uri::InvalidUri) -> Self {
        TryFromProtoError::InvalidUri(error)
    }
}

impl From<globset::Error> for TryFromProtoError {
    fn from(error: globset::Error) -> Self {
        TryFromProtoError::GlobError(error)
    }
}

impl std::fmt::Display for TryFromProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => error.fmt(f),
            CharTryFromError(error) => error.fmt(f),
            DateConversionError(msg) => write!(f, "Date conversion failed: `{}`", msg),
            RegexError(error) => error.fmt(f),
            DeserializationError(error) => error.fmt(f),
            RowConversionError(msg) => write!(f, "Row packing failed: `{}`", msg),
            MissingField(field) => write!(f, "Missing value for `{}`", field),
            InvalidShardId(value) => write!(f, "Invalid value of ShardId found: `{}`", value),
            InvalidUri(error) => error.fmt(f),
            GlobError(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for TryFromProtoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => Some(error),
            CharTryFromError(error) => Some(error),
            RegexError(error) => Some(error),
            DeserializationError(error) => Some(error),
            DateConversionError(_) => None,
            RowConversionError(_) => None,
            MissingField(_) => None,
            InvalidShardId(_) => None,
            InvalidUri(error) => Some(error),
            GlobError(error) => Some(error),
        }
    }
}

/// A trait for representing `Self` as a value of type `Self::Repr` for
/// the purpose of serializing this value as part of a Protobuf message.
///
/// To encode a value, use [`ProtoRepr::into_proto()`] (which
/// should always be an infallible conversion).
///
/// To decode a value, use the fallible [`ProtoRepr::from_proto()`].
/// Since the representation type can be "bigger" than the original,
/// decoding may fail, indicated by returning a [`TryFromProtoError`]
/// wrapped in a [`Result::Err`].
pub trait ProtoRepr: Sized {
    /// A Protobuf type to represent `Self`.
    type Repr;

    /// Consume and convert a `Self` into a `Self::Repr` value.
    fn into_proto(self: Self) -> Self::Repr;

    /// Consume and convert a `Self::Repr` back into a `Self` value.
    ///
    /// Since `Self::Repr` can be "bigger" than the original, this
    /// may fail, indicated by returning a [`TryFromProtoError`]
    /// wrapped in a [`Result::Err`].
    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError>;
}

impl ProtoRepr for usize {
    type Repr = u64;

    fn into_proto(self: Self) -> Self::Repr {
        u64::cast_from(self)
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        usize::try_from(repr).map_err(|err| err.into())
    }
}

impl ProtoRepr for char {
    type Repr = u32;

    fn into_proto(self: Self) -> Self::Repr {
        self.into()
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        char::try_from(repr).map_err(|err| err.into())
    }
}

impl ProtoRepr for u8 {
    type Repr = u32;

    fn into_proto(self: Self) -> Self::Repr {
        self as u32
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        u8::try_from(repr).map_err(TryFromProtoError::TryFromIntError)
    }
}

impl ProtoRepr for u16 {
    type Repr = u32;

    fn into_proto(self: Self) -> Self::Repr {
        self as u32
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        u16::try_from(repr).map_err(TryFromProtoError::TryFromIntError)
    }
}

impl ProtoRepr for u128 {
    type Repr = ProtoU128;

    fn into_proto(self: Self) -> Self::Repr {
        let lo = (self & (u64::MAX as u128)) as u64;
        let hi = (self >> 64) as u64;
        ProtoU128 { hi, lo }
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        Ok((repr.hi as u128) << 64 | (repr.lo as u128))
    }
}

impl ProtoRepr for Uuid {
    type Repr = ProtoU128;

    fn into_proto(self: Self) -> Self::Repr {
        self.as_u128().into_proto()
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        Ok(Uuid::from_u128(u128::from_proto(repr)?))
    }
}

impl ProtoRepr for std::num::NonZeroUsize {
    type Repr = u64;

    fn into_proto(self: Self) -> Self::Repr {
        usize::from(self).into_proto()
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        Ok(usize::from_proto(repr)?.try_into()?)
    }
}

pub fn any_uuid() -> impl Strategy<Value = Uuid> {
    (0..u128::MAX).prop_map(Uuid::from_u128)
}

impl<T: ProtoRepr> ProtoRepr for Option<T> {
    type Repr = Option<T::Repr>;

    fn into_proto(self: Self) -> Self::Repr {
        self.map(|x| x.into_proto())
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        repr.map(T::from_proto).transpose()
    }
}

/// Convenience syntax for trying to convert a `Self` value of type
/// `Option<U>` to `T` if the value is `Some(value)`, or returning
/// [`TryFromProtoError::MissingField`] if the value is `None`.
pub trait TryIntoIfSome<T> {
    fn try_into_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError>;
}

/// A blanket implementation for `Option<U>` where `U` is the
/// `Proto$T` type for `T`.
impl<T, U> TryIntoIfSome<T> for Option<U>
where
    T: TryFrom<U, Error = TryFromProtoError>,
{
    fn try_into_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError> {
        self.ok_or_else(|| TryFromProtoError::missing_field(field))?
            .try_into()
    }
}

/// Convenience syntax for trying to convert a `Self` value of type
/// `Option<U>` to `T` if the value is `Some(value)`, or returning
/// [`TryFromProtoError::MissingField`] if the value is `None`.
pub trait FromProtoIfSome<T> {
    fn from_proto_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError>;
}

/// A blanket implementation for `Option<U>` where `U` is the
/// `ProtoRepr::Repr` type for `T`.
impl<T> FromProtoIfSome<T> for Option<T::Repr>
where
    T: ProtoRepr,
{
    fn from_proto_if_some<S: ToString>(self, field: S) -> Result<T, TryFromProtoError> {
        T::from_proto(self.ok_or_else(|| TryFromProtoError::missing_field(field))?)
    }
}

pub fn protobuf_roundtrip<'t, T, U>(val: &'t T) -> anyhow::Result<T>
where
    T: TryFrom<U, Error = TryFromProtoError>,
    U: From<&'t T> + ::prost::Message + Default,
{
    let vec = U::from(&val).encode_to_vec();
    let val = U::decode(&*vec)?.try_into()?;
    Ok(val)
}

pub fn protobuf_repr_roundtrip<'t, T, U>(val: &'t T) -> anyhow::Result<T>
where
    T: ProtoRepr<Repr = U> + Clone,
    U: ::prost::Message + Default,
{
    let t: U = val.clone().into_proto();
    let vec = t.encode_to_vec();
    Ok(T::from_proto(U::decode(&*vec)?)?)
}
