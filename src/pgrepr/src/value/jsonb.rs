// Copyright Materialize, Inc. All rights reserved.
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

/// A wrapper for [`repr::Jsonb`] that can be serialized and deserialized
/// to the PostgreSQL binary format.
#[derive(Debug)]
pub struct Jsonb(pub repr::jsonb::Jsonb);

impl ToSql for Jsonb {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        out.put_u8(1); // version
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::JSONB => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Jsonb {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Jsonb, Box<dyn Error + Sync + Send>> {
        if raw.len() < 1 || raw[0] != 1 {
            return Err("unsupported jsonb encoding version".into());
        }
        Ok(Jsonb(repr::jsonb::Jsonb::from_slice(&raw[1..])?))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::JSONB => true,
            _ => false,
        }
    }
}
