// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::fmt;

use bytes::{BufMut, BytesMut};
use postgres_types::{to_sql_checked, IsNull, ToSql, Type};

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
        match self.0 {
            repr::Interval::Months(months) => {
                out.put_i64(0);
                out.put_i32(0);
                out.put_i32(months as i32);
            }
            repr::Interval::Duration {
                duration,
                is_positive,
            } => {
                out.put_i64((duration.as_micros() as i64) * if is_positive { 1 } else { -1 });
                out.put_i32(0);
                out.put_i32(0);
            }
        }
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
