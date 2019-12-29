// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::borrow::Borrow;
use std::fmt;
use std::mem::{size_of, transmute};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::datetime::Interval;
use crate::decimal::Significand;
use crate::Datum;

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
pub struct DatumListIter<'a> {
    data: &'a [u8],
    offset: usize,
}

#[derive(Debug)]
pub struct DatumDictIter<'a> {
    data: &'a [u8],
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
#[must_use]
pub struct PackableRow<'a> {
    data: &'a mut Vec<u8>,
}

/// `RowArena` is used to allocate temporary data for building `Datum`s.
#[derive(Debug)]
pub struct RowArena<'a> {
    data: &'a mut Vec<u8>,
    owned_bytes: Vec<Box<[u8]>>,
    owned_rows: Vec<Row>,
}

// DatumList and DatumDict defined here rather than near Datum because we need private access to the unsafe data field

/// A sequence of Datums
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DatumList<'a> {
    /// Points at the serialized datums
    data: &'a [u8],
}

/// A mapping from string keys to Datums
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DatumDict<'a> {
    /// Points at the serialized datums, which should be sorted in key order
    data: &'a [u8],
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
    List,
    Dict,
    JsonNull,
}

// --------------------------------------------------------------------------------
// reading data

/// Reads a `Copy` value starting at byte `offset`.
///
/// Updates `offset` to point to the first byte after the end of the read region.
///
/// # Safety
///
/// This function is safe if a value of type `T` was previously written at this offset by `PackableRow::push`.
/// Otherwise it could return invalid values, which is Undefined Behavior.
#[inline(always)]
unsafe fn read_copy<T>(data: &[u8], offset: &mut usize) -> T
where
    T: Copy,
{
    debug_assert!(data.len() >= *offset + size_of::<T>());
    let ptr = data.as_ptr().add(*offset);
    *offset += size_of::<T>();
    (ptr as *const T).read_unaligned()
}

/// Read a byte slice starting at byte `offset`.
///
/// Updates `offset` to point to the first byte after the end of the read region.
///
/// # Safety
///
/// This function is safe if a `&[u8]` was previously written at this offset by `PackableRow::push_untagged_bytes`.
/// Otherwise it could return invalid values, which is Undefined Behavior.
unsafe fn read_untagged_bytes<'a>(data: &'a [u8], offset: &mut usize) -> &'a [u8] {
    let len = read_copy::<usize>(data, offset);
    let bytes = &data[*offset..(*offset + len)];
    *offset += len;
    bytes
}

/// Read a string starting at byte `offset`.
///
/// Updates `offset` to point to the first byte after the end of the read region.
///
/// # Safety
///
/// This function is safe if a `str` was previously written at this offset by `PackableRow::push_untagged_string`.
/// Otherwise it could return invalid values, which is Undefined Behavior.
unsafe fn read_untagged_string<'a>(data: &'a [u8], offset: &mut usize) -> &'a str {
    let bytes = read_untagged_bytes(data, offset);
    std::str::from_utf8_unchecked(bytes)
}

/// Read a datum starting at byte `offset`.
///
/// Updates `offset` to point to the first byte after the end of the read region.
///
/// # Safety
///
/// This function is safe if a `Datum` was previously written at this offset by `PackableRow::push`.
/// Otherwise it could return invalid values, which is Undefined Behavior.
unsafe fn read_datum<'a>(data: &'a [u8], offset: &mut usize) -> Datum<'a> {
    let tag = read_copy::<Tag>(data, offset);
    match tag {
        Tag::Null => Datum::Null,
        Tag::False => Datum::False,
        Tag::True => Datum::True,
        Tag::Int32 => {
            let i = read_copy::<i32>(data, offset);
            Datum::Int32(i)
        }
        Tag::Int64 => {
            let i = read_copy::<i64>(data, offset);
            Datum::Int64(i)
        }
        Tag::Float32 => {
            let f = read_copy::<f32>(data, offset);
            Datum::Float32(OrderedFloat::from(f))
        }
        Tag::Float64 => {
            let f = read_copy::<f64>(data, offset);
            Datum::Float64(OrderedFloat::from(f))
        }
        Tag::Date => {
            let d = read_copy::<NaiveDate>(data, offset);
            Datum::Date(d)
        }
        Tag::Timestamp => {
            let t = read_copy::<NaiveDateTime>(data, offset);
            Datum::Timestamp(t)
        }
        Tag::TimestampTz => {
            let t = read_copy::<DateTime<Utc>>(data, offset);
            Datum::TimestampTz(t)
        }
        Tag::Interval => {
            let i = read_copy::<Interval>(data, offset);
            Datum::Interval(i)
        }
        Tag::Decimal => {
            let s = read_copy::<Significand>(data, offset);
            Datum::Decimal(s)
        }
        Tag::Bytes => {
            let bytes = read_untagged_bytes(data, offset);
            Datum::Bytes(bytes)
        }
        Tag::String => {
            let string = read_untagged_string(data, offset);
            Datum::String(string)
        }
        Tag::List => {
            let bytes = read_untagged_bytes(data, offset);
            Datum::List(DatumList { data: bytes })
        }
        Tag::Dict => {
            let bytes = read_untagged_bytes(data, offset);
            Datum::Dict(DatumDict { data: bytes })
        }
        Tag::JsonNull => Datum::JsonNull,
    }
}

// --------------------------------------------------------------------------------
// writing data

// See https://github.com/rust-lang/rust/issues/43408 for why this can't be a function
// #[inline(always)]
// fn push_copy<T>(data: &mut Vec<u8>, t: T)
// where
//     T: Copy + Sized,
// {
//     data.extend_from_slice(&unsafe { transmute::<T, [u8; size_of::<T>()]>(t) })
// }
fn assert_is_copy<T: Copy>(_t: T) {}
macro_rules! push_copy {
    ($data:expr, $t:expr, $T:ty) => {
        let t: $T = $t;
        assert_is_copy(t);
        $data.extend_from_slice(&unsafe { transmute::<_, [u8; size_of::<$T>()]>(t) })
    };
}

fn push_untagged_bytes(data: &mut Vec<u8>, bytes: &[u8]) {
    push_copy!(data, bytes.len(), usize);
    data.extend_from_slice(bytes);
}

fn push_untagged_string(data: &mut Vec<u8>, string: &str) {
    push_untagged_bytes(data, string.as_bytes())
}

fn push_datum(data: &mut Vec<u8>, datum: Datum) {
    match datum {
        Datum::Null => data.push(Tag::Null as u8),
        Datum::False => data.push(Tag::False as u8),
        Datum::True => data.push(Tag::True as u8),
        Datum::Int32(i) => {
            data.push(Tag::Int32 as u8);
            push_copy!(data, i, i32);
        }
        Datum::Int64(i) => {
            data.push(Tag::Int64 as u8);
            push_copy!(data, i, i64);
        }
        Datum::Float32(f) => {
            data.push(Tag::Float32 as u8);
            push_copy!(data, f.to_bits(), u32);
        }
        Datum::Float64(f) => {
            data.push(Tag::Float64 as u8);
            push_copy!(data, f.to_bits(), u64);
        }
        Datum::Date(d) => {
            data.push(Tag::Date as u8);
            push_copy!(data, d, NaiveDate);
        }
        Datum::Timestamp(t) => {
            data.push(Tag::Timestamp as u8);
            push_copy!(data, t, NaiveDateTime);
        }
        Datum::TimestampTz(t) => {
            data.push(Tag::TimestampTz as u8);
            push_copy!(data, t, DateTime<Utc>);
        }
        Datum::Interval(i) => {
            data.push(Tag::Interval as u8);
            push_copy!(data, i, Interval);
        }
        Datum::Decimal(s) => {
            data.push(Tag::Decimal as u8);
            push_copy!(data, s, Significand);
        }
        Datum::Bytes(bytes) => {
            data.push(Tag::Bytes as u8);
            push_untagged_bytes(data, bytes);
        }
        Datum::String(string) => {
            data.push(Tag::String as u8);
            push_untagged_string(data, string);
        }
        Datum::List(list) => {
            data.push(Tag::List as u8);
            push_untagged_bytes(data, &list.data);
        }
        Datum::Dict(dict) => {
            data.push(Tag::Dict as u8);
            push_untagged_bytes(data, &dict.data);
        }
        Datum::JsonNull => data.push(Tag::JsonNull as u8),
    }
}

// --------------------------------------------------------------------------------
// public api

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
        unsafe { read_datum(&self.data, &mut 0) }
    }

    pub fn iter(&self) -> DatumListIter {
        DatumListIter {
            data: &self.data,
            offset: 0,
        }
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = Datum<'a>;
    type IntoIter = DatumListIter<'a>;
    fn into_iter(self) -> DatumListIter<'a> {
        self.iter()
    }
}

impl<'a> DatumList<'a> {
    pub fn empty() -> DatumList<'static> {
        DatumList { data: &[] }
    }

    pub fn iter(&self) -> DatumListIter<'a> {
        DatumListIter {
            data: self.data,
            offset: 0,
        }
    }
}

impl<'a> IntoIterator for &'a DatumList<'a> {
    type Item = Datum<'a>;
    type IntoIter = DatumListIter<'a>;
    fn into_iter(self) -> DatumListIter<'a> {
        self.iter()
    }
}

impl<'a> Iterator for DatumListIter<'a> {
    type Item = Datum<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            None
        } else {
            Some(unsafe { read_datum(self.data, &mut self.offset) })
        }
    }
}

impl<'a> DatumDict<'a> {
    pub fn empty() -> DatumDict<'static> {
        DatumDict { data: &[] }
    }

    pub fn iter(&self) -> DatumDictIter<'a> {
        DatumDictIter {
            data: self.data,
            offset: 0,
        }
    }
}

impl<'a> IntoIterator for &'a DatumDict<'a> {
    type Item = (&'a str, Datum<'a>);
    type IntoIter = DatumDictIter<'a>;
    fn into_iter(self) -> DatumDictIter<'a> {
        self.iter()
    }
}

impl<'a> Iterator for DatumDictIter<'a> {
    type Item = (&'a str, Datum<'a>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            None
        } else {
            Some(unsafe {
                let key = read_untagged_string(self.data, &mut self.offset);
                let val = read_datum(self.data, &mut self.offset);
                (key, val)
            })
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

    /// Borrows the internal buffer from `self`.
    ///
    /// You can mutate this buffer in the same way as a row, and clone it to make a new row. The buffer will be cleared on drop.
    ///
    /// There are some awkward cases where this function is needed, but prefer `RowPacker::pack` where possible.
    pub fn packable(&mut self) -> PackableRow {
        PackableRow {
            data: &mut self.data,
        }
    }

    pub fn arena(&mut self) -> RowArena {
        RowArena {
            data: &mut self.data,
            owned_bytes: vec![],
            owned_rows: vec![],
        }
    }
}

impl<'a> PackableRow<'a> {
    /// Push `datum` onto the end of `self`
    pub fn push(&mut self, datum: Datum) {
        push_datum(&mut self.data, datum)
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

impl Drop for PackableRow<'_> {
    fn drop(&mut self) {
        self.data.clear()
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

impl<'a> RowArena<'a> {
    /// Take ownership of `bytes`, for the lifetime of the arena
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn push_bytes(&mut self, bytes: Vec<u8>) -> &'a [u8] {
        self.owned_bytes.push(bytes.into_boxed_slice());
        let owned_bytes = &self.owned_bytes[self.owned_bytes.len() - 1];
        unsafe {
            // this is safe because we only ever append to self.owned_bytes
            transmute::<&[u8], &'a [u8]>(owned_bytes)
        }
    }

    /// Take ownership of `string`, for the lifetime of the arena
    pub fn push_string(&mut self, string: String) -> &'a str {
        let owned_bytes = self.push_bytes(string.into_bytes());
        unsafe {
            // this is safe because we know it was a String just before
            std::str::from_utf8_unchecked(owned_bytes)
        }
    }

    /// Take ownership of `row`, for the lifetime of the arena
    pub fn push_row(&mut self, row: Row) -> &'a Row {
        self.owned_rows.push(row);
        let owned_row = &self.owned_rows[self.owned_rows.len() - 1];
        unsafe {
            // this is safe because we only ever append to self.owned_rows
            transmute::<&Row, &'a Row>(owned_row)
        }
    }

    /// Allocate a `DatumList` from an iterator of `Datum`s
    ///
    /// Note: using this method to build up deeply nested data-structures produces a lot of copying. It may be more efficient to go via the unsafe interface in this module.
    #[allow(clippy::range_plus_one)]
    pub fn push_list<'b, I, D>(&mut self, iter: I) -> DatumList<'a>
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'b>>,
    {
        self.data.push(Tag::List as u8);
        // write a dummy len, will fix it up later
        push_copy!(&mut self.data, 0, usize);
        for datum in iter {
            push_datum(&mut self.data, *datum.borrow());
        }
        // fix up the len
        let len = self.data.len() - 1 - size_of::<usize>();
        self.data[1..(1 + size_of::<usize>())].copy_from_slice(&len.to_le_bytes());
        let row = self.push_row(Row {
            data: self.data.clone().into_boxed_slice(),
        });
        self.data.clear();
        row.unpack_first().unwrap_list()
    }

    /// Allocate a `DatumDict` from an iterator of `(&str, Datum)` pairs
    ///
    /// The pairs MUST be sorted by key and not contain duplicate keys
    ///
    /// Note: using this method to build up deeply nested data-structures produces a lot of copying. It may be more efficient to go via the unsafe interface in this module.
    #[allow(clippy::range_plus_one)]
    pub fn push_dict<'b, I, SD, S, D>(&mut self, iter: I) -> DatumDict<'a>
    where
        I: IntoIterator<Item = SD>,
        SD: Borrow<(S, D)>,
        S: Borrow<str>,
        D: Borrow<Datum<'b>>,
    {
        self.data.push(Tag::Dict as u8);
        // write a dummy len, will fix it up later
        push_copy!(&mut self.data, 0, usize);
        for pair in iter {
            let (key, datum) = pair.borrow();
            push_untagged_string(&mut self.data, key.borrow());
            push_datum(&mut self.data, *datum.borrow());
        }
        // fix up the len
        let len = self.data.len() - 1 - size_of::<usize>();
        self.data[1..(1 + size_of::<usize>())].copy_from_slice(&len.to_le_bytes());
        let row = self.push_row(Row {
            data: self.data.clone().into_boxed_slice(),
        });
        self.data.clear();
        let dict = row.unpack_first().unwrap_dict();

        // if in debug mode, sanity check keys
        if cfg!(debug_assertions) {
            let mut prev_key = None;
            for (key, _val) in dict.iter() {
                if let Some(prev_key) = prev_key {
                    debug_assert!(
                        prev_key < key,
                        "Dict keys must be unique and given in ascending order: {} came before {}",
                        prev_key,
                        key
                    );
                }
                prev_key = Some(key);
            }
        }

        dict
    }

    /// Take some `Datum`s and pack them into a `Row`, using `self` as a buffer to reduce allocation
    pub fn pack<'b, I, D>(&mut self, iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'b>>,
    {
        let mut packable = PackableRow { data: self.data };
        packable.extend(iter);
        packable.finish()
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
    fn miri_test_arena() {
        let mut packer = RowPacker::new();
        let mut arena = packer.arena();

        assert_eq!(arena.push_string("".to_owned()), "");
        assert_eq!(arena.push_string("العَرَبِيَّة".to_owned()), "العَرَبِيَّة");

        assert_eq!(arena.push_bytes(vec![]), &[]);
        assert_eq!(arena.push_bytes(vec![0, 2, 1, 255]), &[0, 2, 1, 255]);

        let list: &[Datum] = &[];
        assert_eq!(arena.push_list(list).iter().collect::<Vec<Datum>>(), vec![]);
        let list = vec![
            Datum::Null,
            Datum::Int32(-42),
            Datum::Interval(Interval::Months(312)),
        ];
        assert_eq!(
            arena.push_list(list.clone()).iter().collect::<Vec<Datum>>(),
            list
        );

        let dict: &[(&str, Datum)] = &[];
        assert_eq!(
            arena.push_dict(dict).iter().collect::<Vec<(&str, Datum)>>(),
            vec![]
        );
        let dict = vec![
            ("an int", Datum::Int32(-42)),
            ("an interval", Datum::Interval(Interval::Months(312))),
            ("null", Datum::Null),
        ];
        assert_eq!(
            arena
                .push_dict(dict.clone())
                .iter()
                .collect::<Vec<(&str, Datum)>>(),
            dict
        );
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
            Datum::Int64(-2_147_483_648 - 42),
            Datum::Float32(OrderedFloat::from(-42.12)),
            Datum::Float64(OrderedFloat::from(-2_147_483_648.0 - 42.12)),
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
                duration: std::time::Duration::from_nanos(1_012_312),
            }),
            Datum::Bytes(&[]),
            Datum::Bytes(&[0, 2, 1, 255]),
            Datum::String(""),
            Datum::String("العَرَبِيَّة"),
        ]);
    }
}
