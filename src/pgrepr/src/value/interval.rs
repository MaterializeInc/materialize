// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;

use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

/// A wrapper for [`repr::Interval`] that can be serialized and deserialized
/// to the PostgreSQL binary format.
#[derive(Debug, Clone)]
pub struct Interval(pub repr::Interval);

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl ToSql for Interval {
    fn to_sql(
        &self,
        _: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + 'static + Send + Sync>> {
        // Postgres represents intervals as an i64 representing the whole number
        // of microseconds, followed by an i32 representing the whole number of
        // days, followed by an i32 representing the whole number of months.
        //
        // Postgres implementation: https://github.com/postgres/postgres/blob/517bf2d91/src/backend/utils/adt/timestamp.c#L1008
        // Diesel implementation: https://github.com/diesel-rs/diesel/blob/a8b52bd05/diesel/src/pg/types/date_and_time/mod.rs#L39
        //
        // Our intervals are guaranteed to fit within SQL's min/max intervals,
        // so this is compression is guaranteed to be lossless. For details, see
        // `repr::scalar::datetime::compute_interval`.
        let days = std::cmp::min(self.0.days() as i128, i32::MAX as i128);
        let ns = self.0.duration - days * 24 * 60 * 60 * 1_000_000_000;
        out.put_i64((ns / 1000) as i64);
        out.put_i32(days as i32);
        out.put_i32(self.0.months);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::INTERVAL => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Interval {
    fn from_sql(_: &Type, mut raw: &'a [u8]) -> Result<Interval, Box<dyn Error + Sync + Send>> {
        let micros = raw.read_i64::<NetworkEndian>()?;
        let days = raw.read_i32::<NetworkEndian>()?;
        let months = raw.read_i32::<NetworkEndian>()?;
        Ok(Interval(repr::Interval::new(
            months,
            days as i64 * 24 * 60 * 60,
            micros * 1000,
        )))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::INTERVAL => true,
            _ => false,
        }
    }
}
