// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::numeric`].

include!(concat!(env!("OUT_DIR"), "/adt.numeric.rs"));

use crate::adt::numeric::NumericMaxScale;
use crate::proto::{ProtoRepr, TryFromProtoError};

impl From<&NumericMaxScale> for ProtoNumericMaxScale {
    fn from(value: &NumericMaxScale) -> Self {
        ProtoNumericMaxScale {
            value: value.0.into_proto(),
        }
    }
}

impl TryFrom<ProtoNumericMaxScale> for NumericMaxScale {
    type Error = TryFromProtoError;

    fn try_from(repr: ProtoNumericMaxScale) -> Result<Self, Self::Error> {
        Ok(NumericMaxScale(u8::from_proto(repr.value)?))
    }
}
