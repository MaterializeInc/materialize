// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generated protobuf code and companion impls.

pub mod adt;
pub mod row;

use mz_ore::cast::CastFrom;
use std::{char::CharTryFromError, num::TryFromIntError};

/// An error thrown when trying to convert from a `*.proto`-generated type
/// `Proto$T` to `$T`.
#[derive(Debug)]
pub enum TryFromProtoError {
    TryFromIntError(TryFromIntError),
    CharTryFromError(CharTryFromError),
    MissingField(String),
    InvalidChar(String),
}

impl TryFromProtoError {
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

impl std::fmt::Display for TryFromProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => error.fmt(f),
            CharTryFromError(error) => error.fmt(f),
            MissingField(field) => write!(f, "Missing value for `{}`", field),
            InvalidChar(str) => write!(f, "String '{}' does not encode a single UTF8 char", str),
        }
    }
}

impl std::error::Error for TryFromProtoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => Some(error),
            CharTryFromError(error) => Some(error),
            MissingField(_) | InvalidChar(_) => None,
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

impl<T: ProtoRepr> ProtoRepr for Option<T> {
    type Repr = Option<T::Repr>;

    fn into_proto(self: Self) -> Self::Repr {
        self.map(|x| x.into_proto())
    }

    fn from_proto(repr: Self::Repr) -> Result<Self, TryFromProtoError> {
        repr.map(T::from_proto).transpose()
    }
}
