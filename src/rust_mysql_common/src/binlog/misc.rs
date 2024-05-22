// Copyright (c) 2020 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use byteorder::{BigEndian as BE, ReadBytesExt};
use saturating::Saturating as S;

use std::{
    cmp::min,
    io::{self, Write},
};

use crate::value::Value;

pub const TIMEF_INT_OFS: i64 = 0x800000;
pub const TIMEF_OFS: i64 = 0x800000000000;
pub const DATETIMEF_INT_OFS: i64 = 0x8000000000;

pub fn my_packed_time_make_int(i: i64) -> i64 {
    ((i as u64) << 24) as i64
}

pub fn my_packed_time_make(i: i64, f: i64) -> i64 {
    ((i as u64) << 24) as i64 + f
}

pub fn my_time_packed_from_binary<T: io::Read>(mut input: T, dec: u32) -> io::Result<i64> {
    match dec {
        1 | 2 => {
            let mut intpart = input.read_u24::<BE>()? as i64 - TIMEF_INT_OFS;
            let mut frac = input.read_u8()? as u32;
            if intpart < 0 && frac > 0 {
                intpart += 1;
                frac -= 0x100;
            }
            Ok(my_packed_time_make(intpart, (frac as i64) * 10_000))
        }
        3 | 4 => {
            let mut intpart = input.read_u24::<BE>()? as i64 - TIMEF_INT_OFS;
            let mut frac = input.read_u16::<BE>()? as i32;
            if intpart < 0 && frac > 0 {
                intpart += 1;
                frac -= 0x10000;
            }
            Ok(my_packed_time_make(intpart, (frac * 100) as i64))
        }
        5 | 6 => Ok((input.read_u48::<BE>()? as i64) - TIMEF_OFS),
        _ => {
            let i = input.read_u24::<BE>()? as i64 - TIMEF_INT_OFS;
            Ok(my_packed_time_make_int(i))
        }
    }
}

pub fn my_packed_time_get_int_part(i: i64) -> i64 {
    i >> 24
}

pub fn my_packed_time_get_frac_part(i: i64) -> i64 {
    i % (1 << 24)
}

pub fn time_from_packed(mut tmp: i64) -> Value {
    let neg = if tmp < 0 {
        tmp = -tmp;
        true
    } else {
        false
    };
    let hms = my_packed_time_get_int_part(tmp);
    let h = ((hms >> 12) as u32) % (1 << 10);
    let m = ((hms >> 6) as u32) % (1 << 6);
    let s = ((hms) as u32) % (1 << 6);
    let u = my_packed_time_get_frac_part(tmp);
    Value::Time(neg, 0, h as u8, m as u8, s as u8, u as u32)
}

pub fn my_datetime_packed_from_binary<T: io::Read>(mut input: T, dec: u32) -> io::Result<i64> {
    let intpart = (input.read_uint::<BE>(5)? as i64) - DATETIMEF_INT_OFS;
    let frac = match dec {
        1 | 2 => (input.read_i8()? as i32) * 10_000,
        3 | 4 => (input.read_i16::<BE>()? as i32) * 100,
        5 | 6 => input.read_i24::<BE>()?,
        _ => return Ok(my_packed_time_make_int(intpart)),
    };
    Ok(my_packed_time_make(intpart, frac as i64))
}

pub fn datetime_from_packed(mut tmp: i64) -> Value {
    if tmp < 0 {
        tmp = -tmp;
    }
    let usec = my_packed_time_get_frac_part(tmp);
    let ymdhms = my_packed_time_get_int_part(tmp);

    let ymd = ymdhms >> 17;
    let ym = ymd >> 5;
    let hms = ymdhms % (1 << 17);

    let day = ymd % (1 << 5);
    let mon = ym % 13;
    let year = (ym / 13) as u32;

    let sec = hms % (1 << 6);
    let min = (hms >> 6) % (1 << 6);
    let hour = (hms >> 12) as u32;

    Value::Date(
        year as u16,
        mon as u8,
        day as u8,
        hour as u8,
        min as u8,
        sec as u8,
        usec as u32,
    )
}

pub fn my_timestamp_from_binary<T: io::Read>(mut input: T, dec: u8) -> io::Result<(i32, i32)> {
    let sec = input.read_u32::<BE>()? as i32;
    let usec = match dec {
        1 | 2 => input.read_i8()? as i32 * 10000,
        3 | 4 => input.read_i16::<BE>()? as i32 * 100,
        5 | 6 => input.read_i24::<BE>()?,
        _ => 0,
    };
    Ok((sec, usec))
}

pub(crate) struct LimitedWrite<T> {
    limit: S<usize>,
    write: T,
}

impl<T> LimitedWrite<T> {
    pub fn new(write: T, limit: S<usize>) -> Self {
        Self { limit, write }
    }
}

impl<T: Write> Write for LimitedWrite<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let limit = min(buf.len(), self.limit.0);
        let count = self.write.write(&buf[..limit])?;
        self.limit -= S(count);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write.flush()
    }
}

pub(crate) trait LimitWrite: Write + Sized {
    fn limit(&mut self, limit: S<usize>) -> LimitedWrite<&mut Self> {
        LimitedWrite::new(self, limit)
    }
}

impl<T: Write> LimitWrite for T {}
