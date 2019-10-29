// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::decimal::Significand;
use crate::scalar::Interval;
use crate::Datum;
use chrono::{NaiveDate, NaiveDateTime};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::iter::FromIterator;
use std::mem::{size_of, transmute};

/// A compact representation for `Datum`s.
///
/// `Datum` is easy to work with but very space inefficent. A `Datum::Int32(42)` is laid out in memory like this:
///
///   tag: 3
///   padding: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
///   data: 0 0 0 42
///   padding: 0 0 0 0 0 0 0 0 0 0 0 0
///
/// For a total of 32 bytes! The second set of padding is needed in case we were to write a `Datum::Decimal` into this location. The first set of padding is needed to align that hypothetical decimal to a 16 bytes boundary.
///
/// A `Row` stores zero or more `Datum`s without any padding.
/// We avoid the need for the first set of padding by only providing access to the `Datum`s via calls to `ptr::read_unaligned`, which on modern x86 is barely penalized.
/// We avoid the need for the second set of padding by not providing mutable access to the `Datum`. Instead, `Row` is append-only.
///
/// `Row`s can be built by pushing `Datum`s on to the end or by `collect`ing an iterator of `Datum`s.
///
/// ```
/// # use repr::{Row, Datum, DatumsBuffer};
/// let mut row = Row::new();
/// row.push(Datum::Int32(0));
/// row.extend(vec![Datum::Int32(1), Datum::Int32(2)]);
/// row.extend(&[Datum::Int32(3), Datum::Int32(4)]);
/// assert_eq!(row.as_vec(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)])
/// ```
///
/// ```
/// # use repr::{Row, Datum, DatumsBuffer};
/// let row = (0..5).map(|i| Datum::Int32(i)).collect::<Row>();
/// assert_eq!(row.as_vec(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)])
/// ```
///
/// ```
/// # use repr::{Row, Datum, DatumsBuffer};
/// use std::iter::FromIterator;
/// let row = Row::from_iter((0..5).map(|i| Datum::Int32(i)));
/// assert_eq!(row.as_vec(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2), Datum::Int32(3), Datum::Int32(4)])
/// ```
///
/// `Row`s are iterable:
///
/// ```
/// # use repr::{Row, Datum, DatumsBuffer};
/// let row = (0..5).map(|i| Datum::Int32(i)).collect::<Row>();
/// assert_eq!(row.iter().nth(3).unwrap(), Datum::Int32(3));
/// ```
///
/// If you want random access to the `Datum`s in a `Row`, use `as_vec` to create a `Vec<Datum>`
/// ```
/// # use repr::{Row, Datum, DatumsBuffer};
/// let row = (0..5).map(|i| Datum::Int32(i)).collect::<Row>();
/// let datums = row.as_vec();
/// assert_eq!(datums[3], Datum::Int32(3));
/// ```
///
/// In performance-sensitive code, you may not want to allocate a `Vec` for each `Row`. See `DatumsBuffer` for a more frugal alternative.
#[derive(
    Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize, Abomonation,
)]
pub struct Row {
    data: Vec<u8>,
}

#[derive(Debug)]
pub struct RowIter<'a> {
    row: &'a Row,
    offset: usize,
}

/// `DatumsBuffer` provides a reusable buffer as an alternative to `Row::as_vec` for processing large numbers of `Row`s
///
/// ```
/// # use repr::{Row, Datum, DatumsBuffer};
/// use std::iter::FromIterator;
/// let rows = (0..5).map(|i| Row::from_iter(&[Datum::Int32(i), Datum::Null, Datum::Int32(i)]) ).collect::<Vec<Row>>();
///
/// let mut buffer = DatumsBuffer::new();
/// for row in rows {
///     let datums = buffer.from_iter(&row);
///     assert_eq!(datums[0], datums[2]);
/// }
/// ```
#[derive(Debug)]
pub struct DatumsBuffer {
    datums: Vec<Datum<'static>>,
}

/// 'Datums' is a temporay view over a `DatumsBuffer`. It `deref`s to `Vec<Datum>`.
#[derive(Debug)]
pub struct Datums<'a> {
    datums: &'a mut Vec<Datum<'a>>,
}

#[derive(Debug, Clone, Copy)]
enum Tag {
    Null,
    False,
    True,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal,
    Date,
    Timestamp,
    Interval,
    Bytes,
    String,
}

impl Row {
    /// Create an empty `Row`
    pub fn new() -> Row {
        Row { data: vec![] }
    }

    pub fn iter(&self) -> RowIter {
        RowIter {
            row: self,
            offset: 0,
        }
    }

    /// Return the first `Datum` in this `Row`.
    ///
    /// Panics if the `Row` is empty.
    pub fn first(&self) -> Datum {
        unsafe { self.read_datum(&mut 0) }
    }

    /// Unpack this `Row` into a `Vec<Datum>` for efficient random access
    pub fn as_vec(&self) -> Vec<Datum> {
        self.iter().collect()
    }

    /// Push `datum` onto the end of this `Row`
    pub fn push(&mut self, datum: Datum) {
        match datum {
            Datum::Null => self.data.push(Tag::Null as u8),
            Datum::False => self.data.push(Tag::False as u8),
            Datum::True => self.data.push(Tag::True as u8),
            Datum::Int32(i) => {
                self.data.push(Tag::Int32 as u8);
                self.data.extend(&i.to_le_bytes());
            }
            Datum::Int64(i) => {
                self.data.push(Tag::Int64 as u8);
                self.data.extend(&i.to_le_bytes());
            }
            Datum::Float32(f) => {
                self.data.push(Tag::Float32 as u8);
                self.data.extend(&f.to_bits().to_le_bytes());
            }
            Datum::Float64(f) => {
                self.data.push(Tag::Float64 as u8);
                self.data.extend(&f.to_bits().to_le_bytes());
            }
            Datum::Date(d) => {
                self.data.push(Tag::Date as u8);
                self.data
                    .extend(&unsafe { transmute::<NaiveDate, [u8; size_of::<NaiveDate>()]>(d) });
            }
            Datum::Timestamp(t) => {
                self.data.push(Tag::Timestamp as u8);
                self.data.extend(&unsafe {
                    transmute::<NaiveDateTime, [u8; size_of::<NaiveDateTime>()]>(t)
                });
            }
            Datum::Interval(i) => {
                self.data.push(Tag::Interval as u8);
                self.data
                    .extend(&unsafe { transmute::<Interval, [u8; size_of::<Interval>()]>(i) });
            }
            Datum::Decimal(s) => {
                self.data.push(Tag::Decimal as u8);
                self.data.extend(&unsafe {
                    transmute::<Significand, [u8; size_of::<Significand>()]>(s)
                });
            }
            Datum::Bytes(bytes) => {
                self.data.push(Tag::Bytes as u8);
                self.data.extend(&bytes.len().to_le_bytes());
                self.data.extend(bytes);
            }
            Datum::String(string) => {
                self.data.push(Tag::String as u8);
                let bytes = string.as_bytes();
                self.data.extend(&bytes.len().to_le_bytes());
                self.data.extend(bytes);
            }
        }
    }

    pub fn extend<'a, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        for datum in iter {
            self.push(*(datum.borrow()));
        }
    }

    /// Reads a `Copy` value starting at byte `offset`.
    ///
    /// Updates `offset` to point to the first byte after the end of the read region.
    ///
    /// Despite the assert, this function is still unsafe because if used incorrectly it could return invalid values, which is Undefined Behavior
    unsafe fn read_copy<T>(&self, offset: &mut usize) -> T
    where
        T: Copy,
    {
        assert!(self.data.len() >= *offset + size_of::<T>());
        let ptr = self.data.as_ptr().add(*offset);
        *offset += size_of::<T>();
        (ptr as *const T).read_unaligned()
    }

    /// Read a datum starting at byte `offset`.
    ///
    /// Updates `offset` to point to the first byte after the end of the read region.
    unsafe fn read_datum(&self, offset: &mut usize) -> Datum {
        let tag = self.read_copy::<Tag>(offset);
        match tag {
            Tag::Null => Datum::Null,
            Tag::False => Datum::False,
            Tag::True => Datum::True,
            Tag::Int32 => {
                let i = self.read_copy::<i32>(offset);
                Datum::Int32(i)
            }
            Tag::Int64 => {
                let i = self.read_copy::<i64>(offset);
                Datum::Int64(i)
            }
            Tag::Float32 => {
                let f = self.read_copy::<f32>(offset);
                Datum::Float32(OrderedFloat::from(f))
            }
            Tag::Float64 => {
                let f = self.read_copy::<f64>(offset);
                Datum::Float64(OrderedFloat::from(f))
            }
            Tag::Date => {
                let d = self.read_copy::<NaiveDate>(offset);
                Datum::Date(d)
            }
            Tag::Timestamp => {
                let t = self.read_copy::<NaiveDateTime>(offset);
                Datum::Timestamp(t)
            }
            Tag::Interval => {
                let i = self.read_copy::<Interval>(offset);
                Datum::Interval(i)
            }
            Tag::Decimal => {
                let s = self.read_copy::<Significand>(offset);
                Datum::Decimal(s)
            }
            Tag::Bytes => {
                let len = self.read_copy::<usize>(offset);
                let bytes =
                    std::slice::from_raw_parts(self.data.as_ptr().add(*offset), len as usize);
                *offset += len;
                Datum::Bytes(bytes)
            }
            Tag::String => {
                let len = self.read_copy::<usize>(offset);
                let bytes =
                    std::slice::from_raw_parts(self.data.as_ptr().add(*offset), len as usize);
                let string = std::str::from_utf8_unchecked(bytes);
                *offset += len;
                Datum::String(string)
            }
        }
    }
}

impl<'a, D> FromIterator<D> for Row
where
    D: Borrow<Datum<'a>>,
{
    fn from_iter<I>(iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
    {
        let mut row = Row::new();
        row.extend(iter);
        row
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = Datum<'a>;
    type IntoIter = RowIter<'a>;
    fn into_iter(self) -> RowIter<'a> {
        self.iter()
    }
}

impl<'a> Iterator for RowIter<'a> {
    type Item = Datum<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.row.data.len() {
            None
        } else {
            Some(unsafe { self.row.read_datum(&mut self.offset) })
        }
    }
}

impl DatumsBuffer {
    pub fn new() -> Self {
        DatumsBuffer { datums: vec![] }
    }

    /// Borrow the buffer.
    pub fn empty<'a>(&'a mut self) -> Datums<'a> {
        let inner = &mut self.datums;
        let datums = Datums {
            datums: unsafe {
                // this is safe because:
                //   nothing else can access buffer.datums while Datums is alive
                //   Datums can't live longer than self
                //   when Datums is dropped, it clears buffer.datums
                transmute::<&'a mut Vec<Datum<'static>>, &'a mut Vec<Datum<'a>>>(inner)
            },
        };
        datums
    }

    /// Borrow the buffer and fill it with `Datum`s copied from `iter`
    #[allow(clippy::wrong_self_convention)] // this function is deliberately mimicking Vec::from_iter, but with a custom allocator
    pub fn from_iter<'a, I, D>(&'a mut self, iter: I) -> Datums<'a>
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        let mut datums = self.empty();
        datums.extend(iter.into_iter().map(|d| *(d.borrow())));
        datums
    }
}

impl<'a> std::ops::Deref for Datums<'a> {
    type Target = Vec<Datum<'a>>;
    fn deref(&self) -> &Vec<Datum<'a>> {
        &self.datums
    }
}

impl<'a> std::ops::DerefMut for Datums<'a> {
    fn deref_mut(&mut self) -> &mut Vec<Datum<'a>> {
        &mut self.datums
    }
}

impl Drop for Datums<'_> {
    fn drop(&mut self) {
        self.datums.clear()
    }
}

impl Default for Row {
    fn default() -> Row {
        Row::new()
    }
}

impl Default for DatumsBuffer {
    fn default() -> DatumsBuffer {
        DatumsBuffer::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_assumptions() {
        assert_eq!(size_of::<Tag>(), 1);
        #[cfg(target_endian = "big")]
        {
            // if you want to run this on a big-endian cpu, we'll need big-endian versions of the serialization code
            assert!(false);
        }
    }

    #[test]
    fn miri_test_round_trip() {
        fn round_trip(datums: Vec<Datum>) {
            let row = Row::from_iter(datums.clone());
            let mut buffer = DatumsBuffer::new();
            let datums2 = row.iter().collect::<Vec<_>>();
            let datums3 = buffer.from_iter(&row);
            assert_eq!(datums, datums2);
            assert_eq!(&datums, &*datums3);
        }

        round_trip(vec![]);
        round_trip(vec![
            Datum::Null,
            Datum::Null,
            Datum::False,
            Datum::True,
            Datum::Int32(-42),
            Datum::Int64(-2147483648 - 42),
            Datum::Float32(OrderedFloat::from(-42.12)),
            Datum::Float64(OrderedFloat::from(-2147483648.0 - 42.12)),
            Datum::Date(NaiveDate::from_isoywd(2019, 30, chrono::Weekday::Wed)),
            Datum::Timestamp(
                NaiveDate::from_isoywd(2019, 30, chrono::Weekday::Wed).and_hms(14, 32, 11),
            ),
            Datum::Interval(Interval::Months(312)),
            Datum::Interval(Interval::Duration {
                is_positive: true,
                duration: std::time::Duration::from_nanos(1012312),
            }),
            Datum::Bytes(&[]),
            Datum::Bytes(&[0, 2, 1]),
            Datum::String(""),
            Datum::String("العَرَبِيَّة"),
        ]);
    }
}
