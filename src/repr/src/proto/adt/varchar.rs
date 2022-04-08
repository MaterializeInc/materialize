// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::adt::varchar`].

use crate::adt::varchar::VarCharMaxLength;
use crate::proto::gen::adt::varchar::*;
use crate::proto::TryFromProtoError;

impl From<&VarCharMaxLength> for ProtoVarCharMaxLength {
    fn from(value: &VarCharMaxLength) -> Self {
        ProtoVarCharMaxLength { value: value.0 }
    }
}

impl TryFrom<ProtoVarCharMaxLength> for VarCharMaxLength {
    type Error = TryFromProtoError;

    fn try_from(repr: ProtoVarCharMaxLength) -> Result<Self, Self::Error> {
        Ok(VarCharMaxLength(repr.value))
    }
}
