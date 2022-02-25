// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::as_conversions)]

use std::error::Error;
use std::fmt;

use byteorder::{NetworkEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use postgres_types::{to_sql_checked, FromSql, IsNull, ToSql, Type};

use mz_repr::adt::interval::Interval as ReprInterval;

/// A wrapper for the `repr` crate's [`Interval`](mz_repr::adt::interval::Interval)
/// type that can be serialized to and deserialized from the PostgreSQL binary
/// format.
#[derive(Debug, Clone)]
pub struct Interval(pub ReprInterval);

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
        out.put_i64(self.0.micros);
        out.put_i32(self.0.days);
        out.put_i32(self.0.months);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for Interval {
    fn from_sql(_: &Type, mut raw: &'a [u8]) -> Result<Interval, Box<dyn Error + Sync + Send>> {
        let micros = raw.read_i64::<NetworkEndian>()?;
        let days = raw.read_i32::<NetworkEndian>()?;
        let months = raw.read_i32::<NetworkEndian>()?;
        Ok(Interval(ReprInterval::new(months, days, micros).unwrap()))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::INTERVAL)
    }
}
