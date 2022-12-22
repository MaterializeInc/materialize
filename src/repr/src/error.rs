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
}

impl std::fmt::Display for AdtError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AdtError::Timestamp(e) => e.fmt(f),
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
        };
        ProtoAdtError { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoAdtError) -> Result<Self, TryFromProtoError> {
        use proto_adt_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                Timestamp(t) => Ok(AdtError::Timestamp(t.into_rust()?)),
            },
            None => Err(TryFromProtoError::missing_field("ProtoAdtError::kind")),
        }
    }
}
