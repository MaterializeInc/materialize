// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use bytes::buf::{BufMut, BufMutExt};
use bytes::BytesMut;
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

/// A wrapper for the `repr` crate's [`Jsonb`](repr::adt::jsonb::Jsonb) type
/// that can be serialized to and deserialized from the PostgreSQL binary
/// format.
#[derive(Debug)]
pub struct Jsonb(pub repr::adt::jsonb::Jsonb);

impl ToSql for Jsonb {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        out.put_u8(1); // version
        self.0.as_ref().to_writer(out.writer())?;
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
        Ok(Jsonb(repr::adt::jsonb::Jsonb::from_slice(&raw[1..])?))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::JSONB => true,
            _ => false,
        }
    }
}
