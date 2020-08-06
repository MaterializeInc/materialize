// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::cell::RefCell;
use std::fmt;
use std::mem::{size_of, transmute};

use byteorder::{NetworkEndian, WriteBytesExt};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::adt::decimal::Significand;
use crate::adt::interval::Interval;
use crate::Datum;
use fmt::Debug;

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
/// A `Row` can be built from a collection of `Datum`s using `Row::pack`, but it often more efficient to use and re-use a `RowPacker` which can avoid unneccesary allocations.
///
/// ```
/// # use repr::{Row, Datum};
/// let row = Row::pack(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.unpack(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)])
/// ```
///
/// `Row`s can be unpacked by iterating over them:
///
/// ```
/// # use repr::{Row, Datum};
/// let row = Row::pack(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.iter().nth(1).unwrap(), Datum::Int32(1));
/// ```
///
/// If you want random access to the `Datum`s in a `Row`, use `Row::unpack` to create a `Vec<Datum>`
/// ```
/// # use repr::{Row, Datum};
/// let row = Row::pack(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// let datums = row.unpack();
/// assert_eq!(datums[1], Datum::Int32(1));
/// ```
///
/// # Performance
///
/// Rows are dynamically sized, but up to a fixed size their data is stored in-line.
/// It is best to re-use a `RowPacker` across multiple `Row` creation calls, as this
/// avoids the allocations involved in `RowPacker::new()`.
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Row {
    data: SmallVec<[u8; 16]>,
}

/// These implementations order first by length, and then by slice contents.
/// This allows many comparisons to complete without dereferencing memory.
impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.data.len().cmp(&other.data.len()) {
            std::cmp::Ordering::Less => Some(std::cmp::Ordering::Less),
            std::cmp::Ordering::Greater => Some(std::cmp::Ordering::Greater),
            std::cmp::Ordering::Equal => Some(self.data.cmp(&other.data)),
        }
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.data.len().cmp(&other.data.len()) {
            std::cmp::Ordering::Less => std::cmp::Ordering::Less,
            std::cmp::Ordering::Greater => std::cmp::Ordering::Greater,
            std::cmp::Ordering::Equal => self.data.cmp(&other.data),
        }
    }
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
    prev_key: Option<&'a str>,
}

/// `RowPacker` is used to build a `Row`.
///
/// ```
/// # use repr::{Row, Datum, RowPacker};
/// let mut packer = RowPacker::new();
/// packer.push(Datum::Int32(2));
/// packer.push(Datum::String("two"));
/// let row = packer.finish();
/// ```
#[derive(Debug)]
pub struct RowPacker {
    data: Vec<u8>,
}

/// `RowArena` is used to hold on to temporary `Row`s for functions like `eval` that need to create complex `Datum`s but don't have a `Row` to put them in yet.
#[derive(Debug)]
pub struct RowArena {
    inner: RefCell<RowArenaInner>,
}

#[derive(Debug)]
struct RowArenaInner {
    owned_bytes: Vec<Box<[u8]>>,
    owned_rows: Vec<Row>,
}

// DatumList and DatumDict defined here rather than near Datum because we need private access to the unsafe data field

/// A sequence of Datums
#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DatumList<'a> {
    /// Points at the serialized datums
    data: &'a [u8],
}

impl<'a> Debug for DatumList<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// A mapping from string keys to Datums
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DatumDict<'a> {
    /// Points at the serialized datums, which should be sorted in key order
    data: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Bytes,
    String,
    List,
    Dict,
    JsonNull,
    Dummy,
}

// --------------------------------------------------------------------------------
// reading data

/// Reads a `Copy` value starting at byte `offset`.
///
/// Updates `offset` to point to the first byte after the end of the read region.
///
/// # Safety
///
/// This function is safe if a value of type `T` was previously written at this offset by `push_copy!`.
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
/// This function is safe if a `&[u8]` was previously written at this offset by `push_untagged_bytes`.
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
/// This function is safe if a `str` was previously written at this offset by `push_untagged_string`.
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
/// This function is safe if a `Datum` was previously written at this offset by `push_datum`.
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
        Tag::Time => {
            let t = read_copy::<NaiveTime>(data, offset);
            Datum::Time(t)
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
            let months = read_copy::<i32>(data, offset);
            let duration = read_copy::<i128>(data, offset);
            Datum::Interval(Interval { months, duration })
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
        Tag::Dummy => Datum::Dummy,
    }
}

// --------------------------------------------------------------------------------
// writing data

fn assert_is_copy<T: Copy>(_t: T) {}

// See https://github.com/rust-lang/rust/issues/43408 for why this can't be a function
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
        Datum::Time(t) => {
            data.push(Tag::Time as u8);
            push_copy!(data, t, NaiveTime);
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
            push_copy!(data, i.months, i32);
            push_copy!(data, i.duration, i128);
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
        Datum::Dummy => data.push(Tag::Dummy as u8),
    }
}

/// Number of bytes required by the datum.
///
/// This is used to optimistically pre-allocate buffers for packing rows.
pub fn datum_size(datum: &Datum) -> usize {
    match datum {
        Datum::Null => 1,
        Datum::False => 1,
        Datum::True => 1,
        Datum::Int32(_) => 1 + size_of::<i32>(),
        Datum::Int64(_) => 1 + size_of::<i64>(),
        Datum::Float32(_) => 1 + size_of::<u32>(),
        Datum::Float64(_) => 1 + size_of::<u64>(),
        Datum::Date(_) => 1 + size_of::<NaiveDate>(),
        Datum::Time(_) => 1 + size_of::<NaiveTime>(),
        Datum::Timestamp(_) => 1 + size_of::<NaiveDateTime>(),
        Datum::TimestampTz(_) => 1 + size_of::<DateTime<Utc>>(),
        Datum::Interval(_) => 1 + size_of::<i32>() + size_of::<i128>(),
        Datum::Decimal(_) => 1 + size_of::<Significand>(),
        Datum::Bytes(bytes) => 1 + size_of::<usize>() + bytes.len(),
        Datum::String(string) => 1 + size_of::<usize>() + string.as_bytes().len(),
        Datum::List(list) => 1 + size_of::<usize>() + list.data.len(),
        Datum::Dict(dict) => 1 + size_of::<usize>() + dict.data.len(),
        Datum::JsonNull => 1,
        Datum::Dummy => 1,
    }
}

// --------------------------------------------------------------------------------
// public api

impl Row {
    /// Take some `Datum`s and pack them into a `Row`.
    pub fn pack<'a, I, D>(iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        // make a big buffer up front to avoid resizing
        let mut packer = RowPacker::new();
        packer.extend(iter);
        // drop the excess capacity
        packer.finish()
    }

    /// Like [`Row::pack`], but the provided iterator is allowed to produce an
    /// error, in which case the packing operation is aborted and the error
    /// returned.
    pub fn try_pack<'a, I, D, E>(iter: I) -> Result<Row, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        // make a big buffer up front to avoid resizing
        let mut packer = RowPacker::new();
        packer.try_extend(iter)?;
        // drop the excess capacity
        Ok(packer.finish())
    }

    /// Pack a slice of `Datum`s into a `Row`.
    ///
    /// This method has the advantage over `pack` that it can determine the required
    /// allocation before packing the elements, ensuring only one allocation and no
    /// redundant copies required.
    ///
    /// TODO: This could also be done for cloneable iterators, though we would need to be
    /// very careful to avoid using it when iterators are either expensive or have
    /// side effects.
    pub fn pack_slice<'a, I, D>(slice: &[Datum<'a>]) -> Row {
        let needed = slice.iter().map(|d| datum_size(d)).sum();
        let mut packer = RowPacker::with_capacity(needed);
        packer.extend(slice.iter());
        packer.finish()
    }

    /// Unpack `self` into a `Vec<Datum>` for efficient random access.
    pub fn unpack(&self) -> Vec<Datum> {
        // It's usually cheaper to unpack twice to figure out the right length than it is to grow the vec as we go
        let len = self.iter().count();
        let mut vec = Vec::with_capacity(len);
        vec.extend(self.iter());
        vec
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

    /// For debugging only
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Write a length-prefixed Row to a buffer
    pub fn encode(&self, buf: &mut Vec<u8>) {
        // TODO assert that the row is small enough for its length to fit in 4 bytes
        buf.write_u32::<NetworkEndian>(self.data.len() as u32)
            .expect("writes to vec cannot fail");
        buf.extend_from_slice(&self.data);
    }

    pub fn decode(data: Vec<u8>) -> Self {
        Row { data: data.into() }
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = Datum<'a>;
    type IntoIter = DatumListIter<'a>;
    fn into_iter(self) -> DatumListIter<'a> {
        self.iter()
    }
}

impl fmt::Debug for Row {
    /// Debug representation using the internal datums
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Row{")?;
        f.debug_list().entries(self.iter()).finish()?;
        f.write_str("}")
    }
}

impl fmt::Display for Row {
    /// Display representation using the internal datums
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("(")?;
        for (i, datum) in self.iter().enumerate() {
            if i != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}", datum)?;
        }
        f.write_str(")")
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

    /// For debugging only
    pub fn data(&self) -> &'a [u8] {
        &self.data
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
            prev_key: None,
        }
    }

    /// For debugging only
    pub fn data(&self) -> &'a [u8] {
        &self.data
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
                let key_tag = read_copy::<Tag>(self.data, &mut self.offset);
                assert!(
                    key_tag == Tag::String,
                    "Dict keys must be strings, got {:?}",
                    key_tag
                );
                let key = read_untagged_string(self.data, &mut self.offset);
                let val = read_datum(self.data, &mut self.offset);

                // if in debug mode, sanity check keys
                if cfg!(debug_assertions) {
                    if let Some(prev_key) = self.prev_key {
                        debug_assert!(
                            prev_key < key,
                            "Dict keys must be unique and given in ascending order: {} came before {}",
                            prev_key,
                            key
                        );
                    }
                    self.prev_key = Some(key);
                }

                (key, val)
            })
        }
    }
}

impl RowPacker {
    /// Allocates an empty row packer.
    pub fn new() -> Self {
        // TODO: Determine if this is the best default choice.
        Self::with_capacity(0)
    }
    /// Allocates an empty row packer with a supplied capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        RowPacker {
            // make a big buffer up front to avoid resizing
            data: Vec::with_capacity(capacity),
        }
    }

    /// Push `datum` onto the end of `self`
    pub fn push(&mut self, datum: Datum) {
        push_datum(&mut self.data, datum)
    }

    /// Extends `self` with the contents of an iterator.
    pub fn extend<'a, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        for datum in iter {
            self.push(*datum.borrow());
        }
    }

    /// Creates a new `Row` from an iterator.
    ///
    /// Unlike `Row::pack`, this re-uses the `RowPacker` to avoid
    /// unneccesary allocations.
    pub fn pack<'a, I, D>(&mut self, iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        self.data.clear();
        for datum in iter {
            self.push(*datum.borrow());
        }
        self.finish_and_reuse()
    }

    /// Like [`RowPacker::extend`], but the provided iterator is allowed to
    /// produce an error.
    ///
    /// `try_extend` is not atomic, so if the iterator produces an error `self`
    /// will still be extended with all elements the iterator produces before
    /// the error.
    pub fn try_extend<'a, I, E, D>(&mut self, iter: I) -> Result<(), E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        for datum in iter {
            self.push(*datum?.borrow());
        }
        Ok(())
    }

    /// Appends the datums of an entire `Row`.
    pub fn extend_by_row(&mut self, row: &Row) {
        self.data.extend(&*row.data);
    }

    /// Finish packing and return a `Row`.
    ///
    /// This does not re-use the allocation of `RowPacker`, which means this
    /// method has relatively few advantages over `finish_and_reuse()`.
    pub fn finish(self) -> Row {
        Row {
            data: SmallVec::from(&self.data[..]),
        }
    }

    /// Finish packing and return a `Row`.
    ///
    /// Unlike [`RowPacker::finish`], this method uses `self.data` to right-size an
    /// allocation for the new `Row` copied from `self.data`, rather than
    /// move the allocation (and potentially re-alloc to resize it).
    /// In principle this can reduce the amount of interaction with the
    /// allocator, as opposed to creating new row packers for each row.
    pub fn finish_and_reuse(&mut self) -> Row {
        let data = SmallVec::from(&self.data[..]);
        self.data.clear();
        Row { data }
    }

    /// Pushes a [`DatumList`] that is built from a closure.
    ///
    /// The supplied closure will be invoked once with a `RowPacker` that can
    /// be used to populate the list. It is valid to call any method on the
    /// [`RowPacker`] except for [`Row::finish_and_reuse`].
    ///
    /// Returns the value returned by the closure, if any.
    ///
    /// ```
    /// # use repr::{Row, Datum, RowPacker};
    /// let mut packer = RowPacker::new();
    /// packer.push_list_with(|packer| {
    ///     packer.push(Datum::String("age"));
    ///     packer.push(Datum::Int64(42));
    /// });
    /// let row = packer.finish();
    ///
    /// assert_eq!(
    ///     row.unpack_first().unwrap_list().iter().collect::<Vec<_>>(),
    ///     vec![Datum::String("age"), Datum::Int64(42)],
    /// );
    /// ```
    #[inline]
    pub fn push_list_with<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut RowPacker) -> R,
    {
        self.data.push(Tag::List as u8);
        let start = self.data.len();
        // write a dummy len, will fix it up later
        push_copy!(&mut self.data, 0, usize);

        let out = f(self);

        let len = self.data.len() - start - size_of::<usize>();
        // fix up the len
        self.data[start..start + size_of::<usize>()].copy_from_slice(&len.to_le_bytes());

        out
    }

    /// Pushes a [`DatumDict`] that is built from a closure.
    ///
    /// The supplied closure will be invoked once with a `RowPacker` that can be
    /// used to populate the dict.
    ///
    /// The closure **must** alternate pushing string keys and arbitary values,
    /// otherwise reading the dict will cause a panic.
    ///
    /// The closure **must** push keys in ascending order, otherwise equality
    /// checks on the resulting `Row` may be wrong and reading the dict IN DEBUG
    /// MODE will cause a panic.
    ///
    /// The closure **must not** call [`RowPacker::finish_and_reuse`].
    ///
    /// # Example
    ///
    /// ```
    /// # use repr::{Row, Datum, RowPacker};
    /// let mut packer = RowPacker::new();
    /// packer.push_dict_with(|packer| {
    ///
    ///     // key
    ///     packer.push(Datum::String("age"));
    ///     // value
    ///     packer.push(Datum::Int64(42));
    ///
    ///     // key
    ///     packer.push(Datum::String("name"));
    ///     // value
    ///     packer.push(Datum::String("bob"));
    /// });
    /// let row = packer.finish();
    ///
    /// assert_eq!(
    ///     row.unpack_first().unwrap_dict().iter().collect::<Vec<_>>(),
    ///     vec![("age", Datum::Int64(42)), ("name", Datum::String("bob"))]
    /// );
    /// ```
    pub fn push_dict_with<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut RowPacker) -> R,
    {
        self.data.push(Tag::Dict as u8);
        let start = self.data.len();
        // write a dummy len, will fix it up later
        push_copy!(&mut self.data, 0, usize);

        let res = f(self);

        let len = self.data.len() - start - size_of::<usize>();
        // fix up the len
        self.data[start..start + size_of::<usize>()].copy_from_slice(&len.to_le_bytes());

        res
    }

    /// Convenience function to push a `DatumList` from an iter of `Datum`s
    ///
    /// See [`push_dict_with`] if you need to be able to handle errors
    pub fn push_list<'a, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        self.push_list_with(|packer| {
            for elem in iter {
                packer.push(*elem.borrow())
            }
        });
    }

    /// Convenience function to push a `DatumDict` from an iter of `(&str, Datum)` pairs
    ///
    /// See [`try_push_dict_with`] if you need to be able to handle errors
    pub fn push_dict<'a, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (&'a str, D)>,
        D: Borrow<Datum<'a>>,
    {
        self.push_dict_with(|packer| {
            for (k, v) in iter {
                packer.push(Datum::String(k));
                packer.push(*v.borrow())
            }
        })
    }

    /// # Safety
    ///
    /// Truncates the underlying storage to the specified byte position.
    ///
    /// `pos` MUST specify a byte offset that lies on a datum boundary.
    /// If `pos` specifies a byte offset that is *within* a datum, the row
    /// packer will produce an invalid row, the unpacking of which may
    /// trigger undefined behavior!
    ///
    /// To find the byte offset of a datum boundary, inspect the the packer's
    /// byte length by calling `packer.data().len()` after pushing the desired
    /// number of datums onto the packer.
    pub unsafe fn truncate(&mut self, pos: usize) {
        self.data.truncate(pos)
    }

    /// For debugging only
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl RowArena {
    pub fn new() -> Self {
        RowArena {
            inner: RefCell::new(RowArenaInner {
                owned_bytes: vec![],
                owned_rows: vec![],
            }),
        }
    }

    /// Take ownership of `bytes` for the lifetime of the arena
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn push_bytes<'a>(&'a self, bytes: Vec<u8>) -> &'a [u8] {
        let mut inner = self.inner.borrow_mut();
        inner.owned_bytes.push(bytes.into_boxed_slice());
        let owned_bytes = &inner.owned_bytes[inner.owned_bytes.len() - 1];
        unsafe {
            // this is safe because we only ever append to self.owned_bytes
            transmute::<&[u8], &'a [u8]>(owned_bytes)
        }
    }

    /// Take ownership of `string` for the lifetime of the arena
    pub fn push_string(&self, string: String) -> &str {
        let owned_bytes = self.push_bytes(string.into_bytes());
        unsafe {
            // this is safe because we know it was a String just before
            std::str::from_utf8_unchecked(owned_bytes)
        }
    }

    /// Take ownership of `row` for the lifetime of the arena
    pub fn push_row<'a>(&'a self, row: Row) -> &'a Row {
        let mut inner = self.inner.borrow_mut();
        inner.owned_rows.push(row);
        let owned_row = &inner.owned_rows[inner.owned_rows.len() - 1];
        unsafe {
            // this is safe because we only ever append to self.owned_rows
            transmute::<&Row, &'a Row>(owned_row)
        }
    }

    /// Convenience function to make a new `Row` containing a single datum, and take ownership of it for the lifetime of the arena
    ///
    /// ```
    /// # use repr::{RowArena, Datum};
    /// let arena = RowArena::new();
    /// let datum = arena.make_datum(|packer| {
    ///   packer.push_list(&[Datum::String("hello"), Datum::String("world")]);
    /// });
    /// assert_eq!(datum.unwrap_list().iter().collect::<Vec<_>>(), vec![Datum::String("hello"), Datum::String("world")]);
    /// ```
    pub fn make_datum<'a, F>(&'a self, f: F) -> Datum<'a>
    where
        F: FnOnce(&mut RowPacker),
    {
        let mut packer = RowPacker::new();
        f(&mut packer);
        self.push_row(packer.finish()).unpack_first()
    }
}

impl Default for RowPacker {
    fn default() -> RowPacker {
        RowPacker::new()
    }
}

impl Default for RowArena {
    fn default() -> RowArena {
        RowArena::new()
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
        let arena = RowArena::new();

        assert_eq!(arena.push_string("".to_owned()), "");
        assert_eq!(arena.push_string("العَرَبِيَّة".to_owned()), "العَرَبِيَّة");

        let empty: &[u8] = &[];
        assert_eq!(arena.push_bytes(vec![]), empty);
        assert_eq!(arena.push_bytes(vec![0, 2, 1, 255]), &[0, 2, 1, 255]);

        let row = Row::pack(&[
            Datum::Null,
            Datum::Int32(-42),
            Datum::Interval(Interval {
                months: 312,
                ..Default::default()
            }),
        ]);
        assert_eq!(arena.push_row(row.clone()).unpack(), row.unpack());
    }

    #[test]
    fn miri_test_round_trip() {
        fn round_trip(datums: Vec<Datum>) {
            let row = Row::pack(datums.clone());

            // When run under miri this catchs undefined bytes written to data
            // eg by calling push_copy! on a type which contains undefined padding values
            dbg!(row.data());

            let datums2 = row.iter().collect::<Vec<_>>();
            let datums3 = row.unpack();
            assert_eq!(datums, datums2);
            assert_eq!(datums, datums3);
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
            Datum::Interval(Interval {
                months: 312,
                ..Default::default()
            }),
            Datum::Interval(Interval::new(0, 0, 1_012_312).unwrap()),
            Datum::Bytes(&[]),
            Datum::Bytes(&[0, 2, 1, 255]),
            Datum::String(""),
            Datum::String("العَرَبِيَّة"),
        ]);
    }

    #[test]
    fn test_nesting() {
        let mut packer = RowPacker::new();
        packer.push_dict_with(|packer| {
            packer.push(Datum::String("favourites"));
            packer.push_list_with(|packer| {
                packer.push(Datum::String("ice cream"));
                packer.push(Datum::String("oreos"));
                packer.push(Datum::String("cheesecake"));
            });
            packer.push(Datum::String("name"));
            packer.push(Datum::String("bob"));
        });
        let row = packer.finish();

        let mut iter = row.unpack_first().unwrap_dict().iter();

        let (k, v) = iter.next().unwrap();
        assert_eq!(k, "favourites");
        assert_eq!(
            v.unwrap_list().iter().collect::<Vec<_>>(),
            vec![
                Datum::String("ice cream"),
                Datum::String("oreos"),
                Datum::String("cheesecake"),
            ]
        );

        let (k, v) = iter.next().unwrap();
        assert_eq!(k, "name");
        assert_eq!(v, Datum::String("bob"));
    }

    #[test]
    fn test_dict_errors() -> Result<(), Box<dyn std::error::Error>> {
        let pack = |ok| {
            let mut packer = RowPacker::new();
            packer.push_dict_with(|packer| {
                if ok {
                    packer.push(Datum::String("key"));
                    packer.push(Datum::Int32(42));
                    Ok(7)
                } else {
                    Err("fail")
                }
            })?;
            Ok(packer.finish())
        };

        assert_eq!(pack(false), Err("fail"));

        let row = pack(true)?;
        let mut dict = row.unpack_first().unwrap_dict().iter();
        assert_eq!(dict.next(), Some(("key", Datum::Int32(42))));
        assert_eq!(dict.next(), None);

        Ok(())
    }

    #[test]
    fn test_datum_sizes() {
        // Test the claims about various datum sizes.
        let values_of_interest = vec![
            Datum::Null,
            Datum::False,
            Datum::Int32(0),
            Datum::Int64(0),
            Datum::Float32(OrderedFloat(0.0)),
            Datum::Float64(OrderedFloat(0.0)),
            Datum::Decimal(Significand::new(0)),
            Datum::Date(NaiveDate::from_ymd(1, 1, 1)),
            Datum::Timestamp(NaiveDateTime::from_timestamp(0, 0)),
            Datum::TimestampTz(DateTime::from_utc(NaiveDateTime::from_timestamp(0, 0), Utc)),
            Datum::Interval(Interval::default()),
            Datum::Bytes(&[]),
            Datum::String(""),
            Datum::JsonNull,
        ];
        for value in values_of_interest {
            if !datum_size(&value) == Row::pack(Some(value)).data.len() {
                panic!("Disparity in claimed size for {:?}", value);
            }
        }
    }
}
