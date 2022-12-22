// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;
use mz_proto::{ProtoType, RustType, TryFromProtoError};

use crate::adt::timestamp::TimestampError;

include!(concat!(env!("OUT_DIR"), "/mz_repr.error.rs"));

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum AdtError {
    Timestamp(TimestampError),
    UInt16OutOfRange,
    UInt32OutOfRange,
}

impl std::fmt::Display for AdtError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AdtError::Timestamp(e) => e.fmt(f),
            AdtError::UInt16OutOfRange => f.write_str("uint2 out of range"),
            AdtError::UInt32OutOfRange => f.write_str("uint4 out of range"),
        }
    }
}

impl From<TimestampError> for AdtError {
    fn from(value: TimestampError) -> Self {
        AdtError::Timestamp(value)
    }
}

impl RustType<ProtoAdtError> for AdtError {
    fn into_proto(&self) -> ProtoAdtError {
        use proto_adt_error::Kind::*;
        let kind = match self {
            AdtError::Timestamp(t) => Timestamp(t.into_proto()),
            AdtError::UInt16OutOfRange => UInt16OutOfRange(()),
            AdtError::UInt32OutOfRange => UInt32OutOfRange(()),
        };
        ProtoAdtError { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoAdtError) -> Result<Self, TryFromProtoError> {
        use proto_adt_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                Timestamp(t) => Ok(AdtError::Timestamp(t.into_rust()?)),
                UInt16OutOfRange(()) => Ok(AdtError::UInt16OutOfRange),
                UInt32OutOfRange(()) => Ok(AdtError::UInt32OutOfRange),
            },
            None => Err(TryFromProtoError::missing_field("ProtoAdtError::kind")),
        }
    }
}

pub trait GenericError {
    fn out_of_range() -> AdtError;
}

impl GenericError for u16 {
    fn out_of_range() -> AdtError {
        AdtError::UInt16OutOfRange
    }
}

impl GenericError for u32 {
    fn out_of_range() -> AdtError {
        AdtError::UInt32OutOfRange
    }
}

pub trait FallibleNeg: Sized {
    fn fallible_neg(&self) -> Result<Self, AdtError>;
}

impl<T: num_traits::ops::checked::CheckedNeg + GenericError> FallibleNeg for T {
    fn fallible_neg(&self) -> Result<Self, AdtError> {
        self.checked_neg().ok_or(Self::out_of_range())
    }
}

pub trait FallibleAdd<I, O>: Sized {
    fn fallible_add(&self, other: &I) -> Result<O, AdtError>;
}

impl<T: num_traits::ops::checked::CheckedAdd + GenericError> FallibleAdd<T, T> for T {
    fn fallible_add(&self, other: &T) -> Result<Self, AdtError> {
        self.checked_add(&other).ok_or(Self::out_of_range())
    }
}
