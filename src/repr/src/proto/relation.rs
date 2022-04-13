// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring `crate::relation`

pub use super::private::{ProtoColumnName, ProtoColumnType};
use crate::proto::TryFromProtoError;
use crate::proto::TryIntoIfSome;
use crate::{ColumnName, ColumnType};

impl From<&ColumnName> for ProtoColumnName {
    fn from(x: &ColumnName) -> Self {
        ProtoColumnName {
            value: Some(x.0.clone()),
        }
    }
}

impl TryFrom<ProtoColumnName> for ColumnName {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoColumnName) -> Result<Self, Self::Error> {
        Ok(ColumnName(x.value.ok_or_else(|| {
            TryFromProtoError::MissingField("ProtoColumnName::value".into())
        })?))
    }
}

impl From<&ColumnType> for ProtoColumnType {
    fn from(x: &ColumnType) -> Self {
        ProtoColumnType {
            nullable: x.nullable,
            scalar_type: Some((&x.scalar_type).into()),
        }
    }
}

impl TryFrom<ProtoColumnType> for ColumnType {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoColumnType) -> Result<Self, Self::Error> {
        Ok(ColumnType {
            nullable: x.nullable,
            scalar_type: x
                .scalar_type
                .try_into_if_some("ProtoColumnType::scalar_type")?,
        })
    }
}
