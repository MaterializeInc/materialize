// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use bytes::{BufMut, BytesMut};
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

use crate::oid;

/// A wrapper for 16-bit unsigned integers that can be serialized to and
/// deserialized from the PostgreSQL binary format.
#[derive(Debug, Clone, Copy)]
pub struct UInt2(pub u16);

impl<'a> FromSql<'a> for UInt2 {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<UInt2, Box<dyn Error + Sync + Send>> {
        Ok(UInt2(u16::from_be_bytes(raw.try_into()?)))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == oid::TYPE_UINT2_OID
    }
}

impl ToSql for UInt2 {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        out.put_u16(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == oid::TYPE_UINT2_OID
    }

    to_sql_checked!();
}

/// A wrapper for 32-bit unsigned integers that can be serialized to and
/// deserialized from the PostgreSQL binary format.
#[derive(Debug, Clone, Copy)]
pub struct UInt4(pub u32);

impl<'a> FromSql<'a> for UInt4 {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<UInt4, Box<dyn Error + Sync + Send>> {
        Ok(UInt4(u32::from_be_bytes(raw.try_into()?)))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == oid::TYPE_UINT4_OID
    }
}

impl ToSql for UInt4 {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        out.put_u32(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == oid::TYPE_UINT4_OID
    }

    to_sql_checked!();
}

/// A wrapper for 64-bit unsigned integers that can be serialized to and
/// deserialized from the PostgreSQL binary format.
#[derive(Debug, Clone, Copy)]
pub struct UInt8(pub u64);

impl<'a> FromSql<'a> for UInt8 {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<UInt8, Box<dyn Error + Sync + Send>> {
        Ok(UInt8(u64::from_be_bytes(raw.try_into()?)))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == oid::TYPE_UINT8_OID
    }
}

impl ToSql for UInt8 {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        out.put_u64(self.0);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == oid::TYPE_UINT8_OID
    }

    to_sql_checked!();
}
