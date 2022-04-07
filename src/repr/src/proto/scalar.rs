// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Protobuf structs mirroring [`crate::scalar`].

include!(concat!(env!("OUT_DIR"), "/scalar.rs"));
use crate::proto::TryFromProtoError;
use crate::scalar::ScalarType;

impl From<&ScalarType> for ProtoScalarType {
    fn from(value: &ScalarType) -> Self {
        use proto_scalar_type::Kind::*;
        use proto_scalar_type::*;

        // Apply .into to contents of &Box<..> returning a Box<..>
        fn box_into<'a, S, T: 'a>(inp: &'a Box<T>) -> Box<S>
        where
            S: From<&'a T>,
        {
            Box::new((&**inp).into())
        }

        // Apply .into to contents of &Option<..> returning a Option<..>
        fn option_into<'a, S, T: 'a>(inp: &'a Option<T>) -> Option<S>
        where
            S: From<&'a T>,
        {
            match inp {
                Some(x) => Some(x.into()),
                None => None,
            }
        }

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
                    max_scale: option_into(max_scale),
                }),
                ScalarType::Char { length } => Char(ProtoChar {
                    length: option_into(length),
                }),
                ScalarType::VarChar { max_length } => VarChar(ProtoVarChar {
                    max_length: option_into(max_length),
                }),
                ScalarType::List {
                    element_type,
                    custom_oid,
                } => List(Box::new(ProtoList {
                    element_type: Some(box_into(element_type)),
                    custom_oid: *custom_oid,
                })),
                ScalarType::Record {
                    custom_oid,
                    fields: _,
                    custom_name,
                } => Record(ProtoRecord {
                    custom_oid: *custom_oid,
                    fields: vec![], // TODO: Replace me with ProtoRecordField
                    custom_name: custom_name.clone(),
                }),
                ScalarType::Array(typ) => Array(box_into(typ)),
                ScalarType::Map {
                    value_type,
                    custom_oid,
                } => Map(Box::new(ProtoMap {
                    value_type: Some(box_into(value_type)),
                    custom_oid: *custom_oid,
                })),
            }),
        }
    }
}

impl TryFrom<ProtoScalarType> for ScalarType {
    type Error = TryFromProtoError;

    fn try_from(value: ProtoScalarType) -> Result<Self, Self::Error> {
        use proto_scalar_type::Kind::*;

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
                max_scale: match pn.max_scale {
                    Some(x) => Some(x.try_into()?),
                    None => None,
                },
            }),
            Char(x) => Ok(ScalarType::Char {
                length: match x.length {
                    Some(x) => Some(x.try_into()?),
                    None => None,
                },
            }),

            VarChar(x) => Ok(ScalarType::VarChar {
                max_length: match x.max_length {
                    Some(x) => Some(x.try_into()?),
                    None => Err(TryFromProtoError::MissingField(
                        "ProtoVarChar::max_length".into(),
                    ))?,
                },
            }),
            Array(x) => {
                // Can't inline this without type hints
                let st: ScalarType = (*x).try_into()?;
                Ok(ScalarType::Array(st.into()))
            }
            List(x) => Ok(ScalarType::List {
                element_type: match x.element_type {
                    Some(x) => {
                        let st: ScalarType = (*x).try_into()?;
                        st.into()
                    }
                    None => Err(TryFromProtoError::MissingField(
                        "ProtoList::element_type".into(),
                    ))?,
                },
                custom_oid: x.custom_oid,
            }),
            Record(x) => {
                let fields = vec![]; //TODO: Replace me with ColumnType proto
                Ok(ScalarType::Record {
                    custom_oid: x.custom_oid,
                    fields,
                    custom_name: x.custom_name,
                })
            }
            Map(x) => Ok(ScalarType::Map {
                value_type: match x.value_type {
                    Some(x) => {
                        let st: ScalarType = (*x).try_into()?;
                        st.into()
                    }
                    None => Err(TryFromProtoError::MissingField(
                        "ProtoMap::value_type".into(),
                    ))?,
                },
                custom_oid: x.custom_oid,
            }),
        }
    }
}
