// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::array`].

include!(concat!(env!("OUT_DIR"), "/adt.array.rs"));

use super::super::{ProtoRepr, TryFromProtoError};
use crate::adt::array::InvalidArrayError;

impl From<&InvalidArrayError> for ProtoInvalidArrayError {
    fn from(error: &InvalidArrayError) -> Self {
        use proto_invalid_array_error::*;
        use Kind::*;
        let kind = match error {
            InvalidArrayError::TooManyDimensions(dims) => TooManyDimensions(dims.into_proto()),
            InvalidArrayError::WrongCardinality { actual, expected } => {
                WrongCardinality(ProtoWrongCardinality {
                    actual: actual.into_proto(),
                    expected: expected.into_proto(),
                })
            }
        };
        ProtoInvalidArrayError { kind: Some(kind) }
    }
}

impl TryFrom<ProtoInvalidArrayError> for InvalidArrayError {
    type Error = TryFromProtoError;

    fn try_from(error: ProtoInvalidArrayError) -> Result<Self, Self::Error> {
        use proto_invalid_array_error::Kind::*;
        match error.kind {
            Some(kind) => match kind {
                TooManyDimensions(dims) => Ok(InvalidArrayError::TooManyDimensions(
                    usize::from_proto(dims)?,
                )),
                WrongCardinality(v) => Ok(InvalidArrayError::WrongCardinality {
                    actual: usize::from_proto(v.actual)?,
                    expected: usize::from_proto(v.expected)?,
                }),
            },
            None => Err(TryFromProtoError::missing_field(
                "`ProtoInvalidArrayError::kind`",
            )),
        }
    }
}
