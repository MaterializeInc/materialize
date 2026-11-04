// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod array;
mod boolean;
mod byte;
mod case_literal;
mod catalog;
mod char;
mod date;
mod datum;
mod float32;
mod float64;
mod int16;
mod int2vector;
mod int32;
mod int64;
mod interval;
mod jsonb;
mod list;
mod map;
mod mz_acl_item;
mod mz_timestamp;
mod numeric;
mod oid;
mod pg_legacy_char;
mod range;
mod record;
mod regproc;
mod string;
mod time;
mod timestamp;
mod try_cast;
mod uint16;
mod uint32;
mod uint64;
mod uuid;
mod varchar;

pub use crate::scalar::func::impls::array::*;
pub use crate::scalar::func::impls::boolean::*;
pub use crate::scalar::func::impls::byte::*;
pub use crate::scalar::func::impls::case_literal::*;
pub use crate::scalar::func::impls::catalog::*;
pub use crate::scalar::func::impls::char::*;
pub use crate::scalar::func::impls::date::*;
pub use crate::scalar::func::impls::datum::*;
pub use crate::scalar::func::impls::float32::*;
pub use crate::scalar::func::impls::float64::*;
pub use crate::scalar::func::impls::int2vector::*;
pub use crate::scalar::func::impls::int16::*;
pub use crate::scalar::func::impls::int32::*;
pub use crate::scalar::func::impls::int64::*;
pub use crate::scalar::func::impls::interval::*;
pub use crate::scalar::func::impls::jsonb::*;
pub use crate::scalar::func::impls::list::*;
pub use crate::scalar::func::impls::map::*;
pub use crate::scalar::func::impls::mz_acl_item::*;
pub use crate::scalar::func::impls::mz_timestamp::*;
pub use crate::scalar::func::impls::numeric::*;
pub use crate::scalar::func::impls::oid::*;
pub use crate::scalar::func::impls::pg_legacy_char::*;
pub use crate::scalar::func::impls::range::*;
pub use crate::scalar::func::impls::record::*;
pub use crate::scalar::func::impls::regproc::*;
pub use crate::scalar::func::impls::string::*;
pub use crate::scalar::func::impls::time::*;
pub use crate::scalar::func::impls::timestamp::*;
pub use crate::scalar::func::impls::try_cast::*;
pub use crate::scalar::func::impls::uint16::*;
pub use crate::scalar::func::impls::uint32::*;
pub use crate::scalar::func::impls::uint64::*;
pub use crate::scalar::func::impls::uuid::*;
pub use crate::scalar::func::impls::varchar::*;

use mz_repr::SqlScalarType;

/// Reports whether rendering values of type `ty` as text maps distinct values
/// to distinct strings.
///
/// Values of `time`, `timestamp` and `timestamptz` can carry nanoseconds, for
/// example from `date + time`, but their text form is rounded to microseconds.
/// Any type that contains one of them is therefore not uniqueness-preserving.
pub(crate) fn text_preserves_uniqueness(ty: &SqlScalarType) -> bool {
    match ty {
        SqlScalarType::Time
        | SqlScalarType::Timestamp { .. }
        | SqlScalarType::TimestampTz { .. } => false,
        SqlScalarType::Array(element_type)
        | SqlScalarType::List { element_type, .. }
        | SqlScalarType::Range { element_type }
        | SqlScalarType::Map {
            value_type: element_type,
            ..
        } => text_preserves_uniqueness(element_type),
        SqlScalarType::Record { fields, .. } => fields
            .iter()
            .all(|(_, typ)| text_preserves_uniqueness(&typ.scalar_type)),
        SqlScalarType::Bool
        | SqlScalarType::Int16
        | SqlScalarType::Int32
        | SqlScalarType::Int64
        | SqlScalarType::UInt16
        | SqlScalarType::UInt32
        | SqlScalarType::UInt64
        | SqlScalarType::Float32
        | SqlScalarType::Float64
        | SqlScalarType::Numeric { .. }
        | SqlScalarType::Date
        | SqlScalarType::Interval
        | SqlScalarType::PgLegacyChar
        | SqlScalarType::PgLegacyName
        | SqlScalarType::Bytes
        | SqlScalarType::String
        | SqlScalarType::Char { .. }
        | SqlScalarType::VarChar { .. }
        | SqlScalarType::Jsonb
        | SqlScalarType::Uuid
        | SqlScalarType::Oid
        | SqlScalarType::RegProc
        | SqlScalarType::RegType
        | SqlScalarType::RegClass
        | SqlScalarType::Int2Vector
        | SqlScalarType::MzTimestamp
        | SqlScalarType::MzAclItem
        | SqlScalarType::AclItem => true,
    }
}
