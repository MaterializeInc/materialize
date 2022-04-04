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

use std::num::TryFromIntError;

/// An error thrown when trying to convert from a `*.proto`-generated type
/// `Proto$T` to `$T`.
#[derive(Debug)]
pub enum TryFromProtoError {
    TryFromIntError(TryFromIntError),
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

impl std::fmt::Display for TryFromProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use TryFromProtoError::*;
        match self {
            TryFromIntError(error) => error.fmt(f),
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
            MissingField(_) | InvalidChar(_) => None,
        }
    }
}
