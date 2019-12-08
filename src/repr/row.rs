// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::borrow::Borrow;
use std::fmt;
use std::mem::{size_of, transmute};

use crate::decimal::Significand;
use crate::scalar::Interval;
use crate::Datum;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

/// A packed representation for `Datum`s.
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
/// A `Row` can be built from a collection of `Datum`s using `Row::pack`
///
/// ```
/// # use repr::{Row, Datum, RowUnpacker};
/// let row = Row::pack(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.unpack(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)])
/// ```
///
/// `Row`s can be unpacked by iterating over them:
///
/// ```
/// # use repr::{Row, Datum, RowUnpacker};
/// let row = Row::pack(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.iter().nth(1).unwrap(), Datum::Int32(1));
/// ```
///
/// If you want random access to the `Datum`s in a `Row`, use `Row::unpack` to create a `Vec<Datum>`
/// ```
/// # use repr::{Row, Datum, RowUnpacker};
/// let row = Row::pack(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// let datums = row.unpack();
/// assert_eq!(datums[1], Datum::Int32(1));
/// ```
///
/// `Row::pack` and `Row::unpack` can cause a surprising amount of allocation. In performance-sensitive code, use `RowPacker` and `RowUnpacker` instead to reuse intermediate storage.
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Row {
    data: Box<[u8]>,
}

#[derive(Debug)]
pub struct RowIter<'a> {
    row: &'a Row,
    offset: usize,
}

impl fmt::Debug for Row {
    /// Debug representation using the internal datums
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Row{")?;
        f.debug_list().entries(self.iter()).finish()?;
        f.write_str("}")
    }
}

/// `RowUnpacker` provides a reusable buffer as an alternative to `Row::unpack` for unpacking large numbers of `Row`s.
///
/// ```
/// # use repr::{Row, Datum, RowUnpacker};
/// let rows = (0..5).map(|i| Row::pack(&[Datum::Int32(i), Datum::Null, Datum::Int32(i)])).collect::<Vec<Row>>();
///
/// let mut unpacker = RowUnpacker::new();
/// for row in rows {
///     let datums = unpacker.unpack(&row);
///     assert_eq!(datums[0], datums[2]);
/// }
/// ```
#[derive(Debug)]
pub struct RowUnpacker {
    datums: Vec<Datum<'static>>,
}

/// 'UnpackedRow' is a tempory storage for unpacked `Datum`s. It is created by `RowUnpacker::unpack` and `deref`s to `Vec<Datum>`.
#[derive(Debug)]
pub struct UnpackedRow<'a> {
    datums: &'a mut Vec<Datum<'a>>,
}

/// `RowPacker` provides a reusable buffer as an alternative to `Row::pack` for packing large numbers of `Row`s.
///
/// ```
/// # use repr::{Row, Datum, RowPacker};
/// let mut packer = RowPacker::new();
/// let row1 = packer.pack(&[Datum::Int32(1), Datum::String("one")]);
/// let row2 = packer.pack(&[Datum::Int32(2), Datum::String("two")]);
/// ```
#[derive(Debug)]
pub struct RowPacker {
    data: Vec<u8>,
}

/// `PackableRow` is a builder struct used for building a `Row`. It is usually used via `RowPacker::pack`, but sometimes awkward control flow might require using `PackableRow` directly.
///
/// ```
/// # use repr::{Row, Datum, RowPacker};
/// let mut packer = RowPacker::new();
/// let mut packable = packer.packable();
/// packable.push(Datum::Int32(2));
/// packable.push(Datum::String("two"));
/// let row = packable.finish();
/// ```
#[derive(Debug)]
pub struct PackableRow<'a> {
    data: &'a mut Vec<u8>,
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
    TimestampTz,
    Interval,
    Bytes,
    String,
}

impl Row {
    /// Take some `Datum`s and pack them into a `Row`.
    ///
    /// This function can cause a surprising number of allocations. In performance-sensitive code, use `RowPacker::pack` instead to reuse intermediate storage.
    pub fn pack<'a, I, D>(iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        RowPacker::new().pack(iter)
    }

    /// Unpack `self` into a `Vec<Datum>` for efficient random access.
    ///
    /// This function can cause a surprising number of allocations. In performance-sensitive code, use `RowUnpacker::unpack` instead to reuse intermediate storage.
    pub fn unpack(&self) -> Vec<Datum> {
        self.iter().collect()
    }

    /// Return the first `Datum` in `self`
    ///
    /// Panics if the `Row` is empty.
    pub fn unpack_first(&self) -> Datum {
        unsafe { self.read_datum(&mut 0) }
    }

    pub fn iter(&self) -> RowIter {
        RowIter {
            row: self,
            offset: 0,
        }
    }

    /// Reads a `Copy` value starting at byte `offset`.
    ///
    /// Updates `offset` to point to the first byte after the end of the read region.
    ///
    /// # Safety
    ///
    /// This function is safe if a value of type `T` was previously written at this offset by `RowPacker::push`.
    /// Otherwise it could return invalid values, which is Undefined Behavior.
    #[inline(always)]
    unsafe fn read_copy<T>(&self, offset: &mut usize) -> T
    where
        T: Copy,
    {
        debug_assert!(self.data.len() >= *offset + size_of::<T>());
        let ptr = self.data.as_ptr().add(*offset);
        *offset += size_of::<T>();
        (ptr as *const T).read_unaligned()
    }

    /// Read a datum starting at byte `offset`.
    ///
    /// Updates `offset` to point to the first byte after the end of the read region.
    ///
    /// # Safety
    ///
    /// This function is safe is a `Datum` was previously written at this offset by `RowPacker::push`.
    /// Otherwise it could return invalid values, which is Undefined Behavior.
    #[inline(always)]
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
            Tag::TimestampTz => {
                let t = self.read_copy::<DateTime<Utc>>(offset);
                Datum::TimestampTz(t)
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

impl RowPacker {
    pub fn new() -> Self {
        RowPacker { data: vec![] }
    }

    /// Take some `Datum`s and pack them into a `Row`, using `self` as a buffer to reduce allocation
    pub fn pack<'a, I, D>(&mut self, iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        let mut packable = self.packable();
        packable.extend(iter);
        packable.finish()
    }

    /// Clears and then borrows the internal buffer from `self`.
    ///
    /// You can mutate this buffer in the same way as a row, and clone it to make a new row.
    ///
    /// There are some awkward cases where this function is needed, but prefer `RowPacker::pack` where possible.
    pub fn packable(&mut self) -> PackableRow {
        // we could clear on Drop instead, but having a custom Drop impl disables NLL which makes PackableRow unpleasant to use
        self.data.clear();
        PackableRow {
            data: &mut self.data,
        }
    }
}

impl<'a> PackableRow<'a> {
    /// Push `Datum::Bytes(bytes)` onto the end of `self` and return a reference to the stored bytes
    ///
    /// ```compile_fail
    /// use repr::RowPacker;
    /// let mut packer = RowPacker::new();
    /// let mut packable = packer.packable();
    /// let s = packable.push_bytes(&[1,2,3]);
    /// packer.packable(); // clears the storage for s
    /// println!("{:?}", s);
    /// ```
    pub fn push_bytes(&mut self, bytes: &[u8]) -> &'a [u8] {
        let data = &mut self.data;
        data.push(Tag::Bytes as u8);
        data.extend(&bytes.len().to_le_bytes());
        let start = data.len();
        data.extend(bytes);
        unsafe {
            let backed_bytes =
                std::slice::from_raw_parts(data.as_ptr().add(start), bytes.len() as usize);
            // it's safe to return &'a because so long as this PackableRow exists we will only every append to self.data
            transmute::<&[u8], &'a [u8]>(backed_bytes)
        }
    }

    /// Push `Datum::String(string)` onto the end of `self` and return a reference to the stored string
    ///
    /// ```compile_fail
    /// use repr::RowPacker;
    /// let mut packer = RowPacker::new();
    /// let mut packable = packer.packable();
    /// let s = packable.push_string("foo");
    /// packer.packable(); // clears the storage for s
    /// println!("{}", s);
    /// ```
    pub fn push_string(&mut self, string: &str) -> &'a str {
        let data = &mut self.data;
        data.push(Tag::String as u8);
        let bytes = string.as_bytes();
        data.extend(&bytes.len().to_le_bytes());
        let start = data.len();
        data.extend(bytes);
        unsafe {
            let backed_bytes =
                std::slice::from_raw_parts(data.as_ptr().add(start), bytes.len() as usize);
            let backed_string = std::str::from_utf8_unchecked(backed_bytes);
            // it's safe to return &'a because so long as this PackableRow exists we will only every append to self.data
            transmute::<&str, &'a str>(backed_string)
        }
    }

    /// Push `datum` onto the end of `self`
    pub fn push(&mut self, datum: Datum) {
        let data = &mut self.data;
        match datum {
            Datum::Null => data.push(Tag::Null as u8),
            Datum::False => data.push(Tag::False as u8),
            Datum::True => data.push(Tag::True as u8),
            Datum::Int32(i) => {
                data.push(Tag::Int32 as u8);
                data.extend(&i.to_le_bytes());
            }
            Datum::Int64(i) => {
                data.push(Tag::Int64 as u8);
                data.extend(&i.to_le_bytes());
            }
            Datum::Float32(f) => {
                data.push(Tag::Float32 as u8);
                data.extend(&f.to_bits().to_le_bytes());
            }
            Datum::Float64(f) => {
                data.push(Tag::Float64 as u8);
                data.extend(&f.to_bits().to_le_bytes());
            }
            Datum::Date(d) => {
                data.push(Tag::Date as u8);
                data.extend(&unsafe { transmute::<NaiveDate, [u8; size_of::<NaiveDate>()]>(d) });
            }
            Datum::Timestamp(t) => {
                data.push(Tag::Timestamp as u8);
                data.extend(&unsafe {
                    transmute::<NaiveDateTime, [u8; size_of::<NaiveDateTime>()]>(t)
                });
            }
            Datum::TimestampTz(t) => {
                data.push(Tag::TimestampTz as u8);
                data.extend(&unsafe {
                    transmute::<DateTime<Utc>, [u8; size_of::<DateTime<Utc>>()]>(t)
                });
            }
            Datum::Interval(i) => {
                data.push(Tag::Interval as u8);
                data.extend(&unsafe { transmute::<Interval, [u8; size_of::<Interval>()]>(i) });
            }
            Datum::Decimal(s) => {
                data.push(Tag::Decimal as u8);
                data.extend(&unsafe {
                    transmute::<Significand, [u8; size_of::<Significand>()]>(s)
                });
            }
            Datum::Bytes(bytes) => {
                self.push_bytes(bytes);
            }
            Datum::String(string) => {
                self.push_string(string);
            }
        }
    }

    pub fn extend<'b, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'b>>,
    {
        for datum in iter {
            self.push(*datum.borrow());
        }
    }

    pub fn finish(self) -> Row {
        Row {
            data: self.data.clone().into_boxed_slice(),
        }
    }
}

impl RowUnpacker {
    pub fn new() -> Self {
        RowUnpacker { datums: vec![] }
    }

    /// Unpack `row` into a `Vec<Datum>` for efficient random access, using `self` as a buffer to reduce allocation
    pub fn unpack<'a, I, D>(&'a mut self, iter: I) -> UnpackedRow<'a>
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        let inner = &mut self.datums;
        let mut unpacked = UnpackedRow {
            datums: unsafe {
                // this is safe because:
                //   nothing else can access buffer.datums while unpacked is alive
                //   unpacked can't live longer than self
                //   when unpacked is dropped, it clears buffer.datums
                transmute::<&'a mut Vec<Datum<'static>>, &'a mut Vec<Datum<'a>>>(inner)
            },
        };
        unpacked.extend(iter.into_iter().map(|d| *d.borrow()));
        unpacked
    }
}

impl<'a> std::ops::Deref for UnpackedRow<'a> {
    type Target = Vec<Datum<'a>>;
    fn deref(&self) -> &Vec<Datum<'a>> {
        &self.datums
    }
}

impl<'a> std::ops::DerefMut for UnpackedRow<'a> {
    fn deref_mut(&mut self) -> &mut Vec<Datum<'a>> {
        &mut self.datums
    }
}

impl Drop for UnpackedRow<'_> {
    fn drop(&mut self) {
        self.datums.clear()
    }
}

impl Default for RowPacker {
    fn default() -> RowPacker {
        RowPacker::new()
    }
}

impl Default for RowUnpacker {
    fn default() -> RowUnpacker {
        RowUnpacker::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn miri_test_push() {
        let mut packer = RowPacker::new();
        let mut packable = packer.packable();
        assert_eq!(packable.push_string(""), "");
        assert_eq!(packable.push_string("العَرَبِيَّة"), "العَرَبِيَّة");
        assert_eq!(packable.push_bytes(&[]), &[]);
        assert_eq!(packable.push_bytes(&[0, 2, 1, 255]), &[0, 2, 1, 255]);
    }

    #[test]
    fn miri_test_round_trip() {
        fn round_trip(datums: Vec<Datum>) {
            let row = Row::pack(datums.clone());
            let mut unpacker = RowUnpacker::new();
            let datums2 = row.iter().collect::<Vec<_>>();
            let datums3 = unpacker.unpack(&row);
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
            Datum::TimestampTz(DateTime::<Utc>::from_utc(
                NaiveDateTime::from_timestamp(61, 0),
                Utc,
            )),
            Datum::Interval(Interval::Months(312)),
            Datum::Interval(Interval::Duration {
                is_positive: true,
                duration: std::time::Duration::from_nanos(1012312),
            }),
            Datum::Bytes(&[]),
            Datum::Bytes(&[0, 2, 1, 255]),
            Datum::String(""),
            Datum::String("العَرَبِيَّة"),
        ]);
    }
}
