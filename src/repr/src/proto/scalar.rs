// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring `crate::scalar`

pub use super::private::proto_scalar_type::ProtoRecordField;
pub use super::private::ProtoScalarType;
use crate::proto::TryFromProtoError;
use crate::proto::TryIntoIfSome;
use crate::scalar::ScalarType;
use crate::{ColumnName, ColumnType};

impl From<&(ColumnName, ColumnType)> for ProtoRecordField {
    fn from(x: &(ColumnName, ColumnType)) -> Self {
        ProtoRecordField {
            column_name: Some((&x.0).into()),
            column_type: Some((&x.1).into()),
        }
    }
}

impl TryFrom<ProtoRecordField> for (ColumnName, ColumnType) {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoRecordField) -> Result<Self, Self::Error> {
        Ok((
            x.column_name
                .try_into_if_some("ProtoRecordField::column_name")?,
            x.column_type
                .try_into_if_some("ProtoRecordField::column_type")?,
        ))
    }
}

impl From<&ScalarType> for Box<ProtoScalarType> {
    fn from(value: &ScalarType) -> Self {
        Box::new(value.into())
    }
}

impl From<&ScalarType> for ProtoScalarType {
    fn from(value: &ScalarType) -> Self {
        use super::private::proto_scalar_type::Kind::*;
        use super::private::proto_scalar_type::*;

        ProtoScalarType {
            kind: Some(match value {
                ScalarType::Bool => Bool(()),
                ScalarType::Int16 => Int16(()),
                ScalarType::Int32 => Int32(()),
                ScalarType::Int64 => Int64(()),
                ScalarType::Float32 => Float32(()),
                ScalarType::Float64 => Float64(()),
                ScalarType::Date => Date(()),
                ScalarType::Time => Time(()),
                ScalarType::Timestamp => Timestamp(()),
                ScalarType::TimestampTz => TimestampTz(()),
                ScalarType::Interval => Interval(()),
                ScalarType::PgLegacyChar => PgLegacyChar(()),
                ScalarType::Bytes => Bytes(()),
                ScalarType::String => String(()),
                ScalarType::Jsonb => Jsonb(()),
                ScalarType::Uuid => Uuid(()),
                ScalarType::Oid => Oid(()),
                ScalarType::RegProc => RegProc(()),
                ScalarType::RegType => RegType(()),
                ScalarType::RegClass => RegClass(()),
                ScalarType::Int2Vector => Int2Vector(()),

                ScalarType::Numeric { max_scale } => Numeric(ProtoNumeric {
                    max_scale: max_scale.as_ref().map(Into::into),
                }),
                ScalarType::Char { length } => Char(ProtoChar {
                    length: length.as_ref().map(Into::into),
                }),
                ScalarType::VarChar { max_length } => VarChar(ProtoVarChar {
                    max_length: max_length.as_ref().map(Into::into),
                }),

                ScalarType::List {
                    element_type,
                    custom_oid,
                } => List(Box::new(ProtoList {
                    element_type: Some(element_type.as_ref().into()),
                    custom_oid: *custom_oid,
                })),
                ScalarType::Record {
                    custom_oid,
                    fields,
                    custom_name,
                } => Record(ProtoRecord {
                    custom_oid: *custom_oid,
                    fields: fields.into_iter().map(Into::into).collect(),
                    custom_name: custom_name.clone(),
                }),
                ScalarType::Array(typ) => Array(typ.as_ref().into()),
                ScalarType::Map {
                    value_type,
                    custom_oid,
                } => Map(Box::new(ProtoMap {
                    value_type: Some(value_type.as_ref().into()),
                    custom_oid: *custom_oid,
                })),
            }),
        }
    }
}

impl TryFrom<ProtoScalarType> for ScalarType {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoScalarType) -> Result<Self, TryFromProtoError> {
        use super::private::proto_scalar_type::Kind::*;

        let kind = value
            .kind
            .ok_or_else(|| TryFromProtoError::MissingField("ProtoScalarType::Kind".into()))?;

        match kind {
            Bool(()) => Ok(ScalarType::Bool),
            Int16(()) => Ok(ScalarType::Int16),
            Int32(()) => Ok(ScalarType::Int32),
            Int64(()) => Ok(ScalarType::Int64),
            Float32(()) => Ok(ScalarType::Float32),
            Float64(()) => Ok(ScalarType::Float64),
            Date(()) => Ok(ScalarType::Date),
            Time(()) => Ok(ScalarType::Time),
            Timestamp(()) => Ok(ScalarType::Timestamp),
            TimestampTz(()) => Ok(ScalarType::TimestampTz),
            Interval(()) => Ok(ScalarType::Interval),
            PgLegacyChar(()) => Ok(ScalarType::PgLegacyChar),
            Bytes(()) => Ok(ScalarType::Bytes),
            String(()) => Ok(ScalarType::String),
            Jsonb(()) => Ok(ScalarType::Jsonb),
            Uuid(()) => Ok(ScalarType::Uuid),
            Oid(()) => Ok(ScalarType::Oid),
            RegProc(()) => Ok(ScalarType::RegProc),
            RegType(()) => Ok(ScalarType::RegType),
            RegClass(()) => Ok(ScalarType::RegClass),
            Int2Vector(()) => Ok(ScalarType::Int2Vector),

            Numeric(pn) => Ok(ScalarType::Numeric {
                max_scale: pn.max_scale.map(TryInto::try_into).transpose()?,
            }),
            Char(x) => Ok(ScalarType::Char {
                length: x.length.map(TryInto::try_into).transpose()?,
            }),

            VarChar(x) => Ok(ScalarType::VarChar {
                max_length: x.max_length.map(TryInto::try_into).transpose()?,
            }),
            Array(x) => Ok(ScalarType::Array({
                let st: ScalarType = (*x).try_into()?;
                st.into()
            })),
            List(x) => Ok(ScalarType::List {
                element_type: Box::new(
                    x.element_type
                        .map(|x| *x)
                        .try_into_if_some("ProtoList::element_type")?,
                ),
                custom_oid: x.custom_oid,
            }),
            Record(x) => Ok(ScalarType::Record {
                custom_oid: x.custom_oid,
                fields: x
                    .fields
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
                custom_name: x.custom_name,
            }),
            Map(x) => Ok(ScalarType::Map {
                value_type: Box::new(
                    x.value_type
                        .map(|x| *x)
                        .try_into_if_some("ProtoMap::value_type")?,
                ),
                custom_oid: x.custom_oid,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
       #[test]
        fn scalar_type_serialization_roundtrip(expect in any::<ScalarType>() ) {
            let actual = protobuf_roundtrip::<_, ProtoScalarType>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
