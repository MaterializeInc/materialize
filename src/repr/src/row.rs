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
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;
use std::mem::{size_of, transmute};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use dec::{Decimal128, OrderedDecimal};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::adt::array::{
    Array, ArrayDimension, ArrayDimensions, InvalidArrayError, MAX_ARRAY_DIMENSIONS,
};
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
/// A `Row` can be built from a collection of `Datum`s using `Row::pack`, but it
/// is more efficient to use `Row::pack_slice` so that a right-sized allocation
/// can be created. If that is not possible, consider using the "packer"
/// pattern: allocate one row, pack into it, and then call [`Row::finish_and_reuse`]
/// to receive a copy of that row, leaving behind the original allocation to
/// pack future rows.
///
/// Creating a row via [`Row::pack_slice`]:
///
/// ```
/// # use repr::{Row, Datum};
/// let row = Row::pack_slice(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.unpack(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)])
/// ```
///
/// `Row`s can be unpacked by iterating over them:
///
/// ```
/// # use repr::{Row, Datum};
/// let row = Row::pack_slice(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.iter().nth(1).unwrap(), Datum::Int32(1));
/// ```
///
/// If you want random access to the `Datum`s in a `Row`, use `Row::unpack` to create a `Vec<Datum>`
/// ```
/// # use repr::{Row, Datum};
/// let row = Row::pack_slice(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// let datums = row.unpack();
/// assert_eq!(datums[1], Datum::Int32(1));
/// ```
///
/// # Performance
///
/// Rows are dynamically sized, but up to a fixed size their data is stored in-line.
/// It is best to re-use a `Row` across multiple `Row` creation calls, as this
/// avoids the allocations involved in `Row::new()`.
#[derive(Clone, Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Row {
    data: SmallVec<[u8; 24]>,
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

/// A wrapper around a byte slice that guarantees the data are row-formatted.
///
/// This type exists to allow row-formatted data to be stored in types that
/// need not contain a `Row`, for example large contiguous `[u8]` allocations.
/// It is not expected that most users will use this type, especially as its
/// only constructor is unsafe.
#[derive(Debug)]
pub struct RowRef<'a> {
    data: &'a [u8],
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

/// `RowArena` is used to hold on to temporary `Row`s for functions like `eval` that need to create complex `Datum`s but don't have a `Row` to put them in yet.
#[derive(Debug)]
pub struct RowArena {
    inner: RefCell<RowArenaInner>,
}

#[derive(Debug)]
struct RowArenaInner {
    // Semantically, `owned_bytes` is better represented by a `Vec<Box<[u8]>>`,
    // as once the arena takes ownership of a byte vector the vector is never
    // modified. But `RowArena::push_bytes` takes ownership of a `Vec<u8>`, so
    // storing that `Vec<u8>` directly avoids an allocation. The cost is
    // additional memory use, as the vector may have spare capacity, but row
    // arenas are short lived so this is the better tradeoff.
    owned_bytes: Vec<Vec<u8>>,
    owned_rows: Vec<Row>,
}

// DatumList and DatumDict defined here rather than near Datum because we need private access to the unsafe data field

/// A sequence of Datums
#[derive(Clone, Copy, Eq, PartialEq, Hash)]
pub struct DatumList<'a> {
    /// Points at the serialized datums
    data: &'a [u8],
}

impl<'a> Debug for DatumList<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl Ord for DatumList<'_> {
    fn cmp(&self, other: &DatumList) -> Ordering {
        self.iter().cmp(other.iter())
    }
}

impl PartialOrd for DatumList<'_> {
    fn partial_cmp(&self, other: &DatumList) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A mapping from string keys to Datums
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DatumMap<'a> {
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
    BytesTiny,
    BytesShort,
    BytesLong,
    BytesHuge,
    StringTiny,
    StringShort,
    StringLong,
    StringHuge,
    Uuid,
    Array,
    List,
    Dict,
    JsonNull,
    Dummy,
    Numeric,
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

/// Read a data whose length is encoded in the row before its contents.
///
/// Updates `offset` to point to the first byte after the end of the read region.
///
/// # Safety
///
/// This function is safe if the datum's length and contents were previously written by `push_lengthed_bytes`,
/// and it was only written with a `String` tag if it was indeed UTF-8.
unsafe fn read_lengthed_datum<'a>(data: &'a [u8], offset: &mut usize, tag: Tag) -> Datum<'a> {
    let len = match tag {
        Tag::BytesTiny | Tag::StringTiny => read_copy::<u8>(data, offset) as usize,
        Tag::BytesShort | Tag::StringShort => read_copy::<u16>(data, offset) as usize,
        Tag::BytesLong | Tag::StringLong => read_copy::<u32>(data, offset) as usize,
        Tag::BytesHuge | Tag::StringHuge => read_copy::<usize>(data, offset),
        _ => unreachable!(),
    };
    let bytes = &data[*offset..(*offset + len)];
    *offset += len;
    match tag {
        Tag::BytesTiny | Tag::BytesShort | Tag::BytesLong | Tag::BytesHuge => Datum::Bytes(bytes),
        Tag::StringTiny | Tag::StringShort | Tag::StringLong | Tag::StringHuge => {
            Datum::String(std::str::from_utf8_unchecked(bytes))
        }
        _ => unreachable!(),
    }
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
        Tag::BytesTiny
        | Tag::BytesShort
        | Tag::BytesLong
        | Tag::BytesHuge
        | Tag::StringTiny
        | Tag::StringShort
        | Tag::StringLong
        | Tag::StringHuge => {
            let datum = read_lengthed_datum(data, offset, tag);
            datum
        }
        Tag::Uuid => {
            let mut b: uuid::Bytes = [0; 16];
            b.copy_from_slice(read_untagged_bytes(data, offset));
            Datum::Uuid(Uuid::from_bytes(b))
        }
        Tag::Array => {
            // See the comment in `Row::push_array` for details on the encoding
            // of arrays.
            let ndims = read_copy::<u8>(data, offset);
            let dims_size = usize::from(ndims) * size_of::<usize>() * 2;
            let dims = &data[*offset..*offset + dims_size];
            *offset += dims_size;
            let data = read_untagged_bytes(data, offset);
            Datum::Array(Array {
                dims: ArrayDimensions { data: dims },
                elements: DatumList { data },
            })
        }
        Tag::List => {
            let bytes = read_untagged_bytes(data, offset);
            Datum::List(DatumList { data: bytes })
        }
        Tag::Dict => {
            let bytes = read_untagged_bytes(data, offset);
            Datum::Map(DatumMap { data: bytes })
        }
        Tag::JsonNull => Datum::JsonNull,
        Tag::Dummy => Datum::Dummy,
        Tag::Numeric => {
            let n = read_copy::<OrderedDecimal<Decimal128>>(data, offset);
            Datum::Numeric(n)
        }
    }
}

// --------------------------------------------------------------------------------
// writing data

fn assert_is_copy<T: Copy>() {}

/// A trait that abstracts over ways to push bytes into a buffer.
///
/// This trait exists to allow us to write the `push` logic once for
/// multiple recipients of the pushed data.
trait Bytes {
    fn extend_from_slice(&mut self, slice: &[u8]);
    fn push(&mut self, byte: u8);
}

impl Bytes for Vec<u8> {
    fn extend_from_slice(&mut self, slice: &[u8]) {
        self.extend_from_slice(slice);
    }
    fn push(&mut self, byte: u8) {
        self.push(byte);
    }
}

impl Bytes for Row {
    fn extend_from_slice(&mut self, slice: &[u8]) {
        self.data.extend_from_slice(slice);
    }
    fn push(&mut self, byte: u8) {
        self.data.push(byte);
    }
}

// See https://github.com/rust-lang/rust/issues/43408 for why this can't be a function
macro_rules! push_copy {
    ($data:expr, $t:expr, $T:ty) => {
        assert_is_copy::<$T>();
        $data.extend_from_slice(&unsafe { transmute::<$T, [u8; size_of::<$T>()]>($t) })
    };
}

fn push_untagged_bytes<T: Bytes>(data: &mut T, bytes: &[u8]) {
    push_copy!(data, bytes.len(), usize);
    data.extend_from_slice(bytes);
}

fn push_lengthed_bytes<T: Bytes>(data: &mut T, bytes: &[u8], tag: Tag) {
    match tag {
        Tag::BytesTiny | Tag::StringTiny => {
            push_copy!(data, bytes.len() as u8, u8);
        }
        Tag::BytesShort | Tag::StringShort => {
            push_copy!(data, bytes.len() as u16, u16);
        }
        Tag::BytesLong | Tag::StringLong => {
            push_copy!(data, bytes.len() as u32, u32);
        }
        Tag::BytesHuge | Tag::StringHuge => {
            push_copy!(data, bytes.len() as usize, usize);
        }
        _ => unreachable!(),
    }
    data.extend_from_slice(bytes);
}

fn push_datum<T: Bytes>(data: &mut T, datum: Datum) {
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
            let tag = match bytes.len() {
                0..=255 => Tag::BytesTiny,
                256..=65535 => Tag::BytesShort,
                65536..=4294967295 => Tag::BytesLong,
                _ => Tag::BytesHuge,
            };
            data.push(tag as u8);
            push_lengthed_bytes(data, bytes, tag);
        }
        Datum::String(string) => {
            let tag = match string.len() {
                0..=255 => Tag::StringTiny,
                256..=65535 => Tag::StringShort,
                65536..=4294967295 => Tag::StringLong,
                _ => Tag::StringHuge,
            };
            data.push(tag as u8);
            push_lengthed_bytes(data, string.as_bytes(), tag);
        }
        Datum::Uuid(u) => {
            data.push(Tag::Uuid as u8);
            push_untagged_bytes(data, u.as_bytes());
        }
        Datum::Array(array) => {
            // See the comment in `Row::push_array` for details on the encoding
            // of arrays.
            data.push(Tag::Array as u8);
            data.push(array.dims.ndims());
            data.extend_from_slice(array.dims.data);
            push_untagged_bytes(data, &array.elements.data);
        }
        Datum::List(list) => {
            data.push(Tag::List as u8);
            push_untagged_bytes(data, &list.data);
        }
        Datum::Map(dict) => {
            data.push(Tag::Dict as u8);
            push_untagged_bytes(data, &dict.data);
        }
        Datum::JsonNull => data.push(Tag::JsonNull as u8),
        Datum::Dummy => data.push(Tag::Dummy as u8),
        Datum::Numeric(n) => {
            data.push(Tag::Numeric as u8);
            push_copy!(data, n, OrderedDecimal<Decimal128>);
        }
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
        Datum::Bytes(bytes) => {
            // We use a variable length representation of slice length.
            let bytes_for_length = match bytes.len() {
                0..=255 => 1,
                256..=65535 => 2,
                65536..=4294967295 => 4,
                _ => 8,
            };
            1 + bytes_for_length + bytes.len()
        }
        Datum::String(string) => {
            // We use a variable length representation of slice length.
            let bytes_for_length = match string.len() {
                0..=255 => 1,
                256..=65535 => 2,
                65536..=4294967295 => 4,
                _ => 8,
            };
            1 + bytes_for_length + string.len()
        }
        Datum::Uuid(_) => 1 + size_of::<Uuid>(),
        Datum::Array(array) => {
            1 + size_of::<u8>() + array.dims.data.len() + array.elements.data.len()
        }
        Datum::List(list) => 1 + size_of::<usize>() + list.data.len(),
        Datum::Map(dict) => 1 + size_of::<usize>() + dict.data.len(),
        Datum::JsonNull => 1,
        Datum::Dummy => 1,
        Datum::Numeric(_) => 1 + size_of::<OrderedDecimal<Decimal128>>(),
    }
}

/// Number of bytes required by a sequence of datums.
///
/// This method can be used to right-size the allocation for a `Row`
/// before calling [`Row::extend`].
pub fn datums_size<'a, I, D>(iter: I) -> usize
where
    I: IntoIterator<Item = D>,
    D: Borrow<Datum<'a>>,
{
    iter.into_iter().map(|d| datum_size(d.borrow())).sum()
}

/// Number of bytes required by a list of datums. This computes the size that would be required if
/// the given datums were packed into a list.
///
/// This is used to optimistically pre-allocate buffers for packing rows.
pub fn datum_list_size<'a, I, D>(iter: I) -> usize
where
    I: IntoIterator<Item = D>,
    D: Borrow<Datum<'a>>,
{
    1 + size_of::<usize>() + datums_size(iter)
}

// --------------------------------------------------------------------------------
// public api

impl Row {
    /// Take some `Datum`s and pack them into a `Row`.
    ///
    /// This method builds a `Row` by repeatedly increasing the backing
    /// allocation. If the contents of the iterator are known ahead of
    /// time, consider [`Row::with_capacity`] to right-size the allocation
    /// first, and then [`Row::extend`] to populate it with `Datum`s.
    /// This avoids the repeated allocation resizing and copying.
    pub fn pack<'a, I, D>(iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        let mut row = Row::default();
        row.extend(iter);
        row
    }

    /// Like [`Row::pack`], but the provided iterator is allowed to produce an
    /// error, in which case the packing operation is aborted and the error
    /// returned.
    pub fn try_pack<'a, I, D, E>(iter: I) -> Result<Row, E>
    where
        I: IntoIterator<Item = Result<D, E>>,
        D: Borrow<Datum<'a>>,
    {
        let mut row = Row::default();
        row.try_extend(iter)?;
        Ok(row)
    }

    /// Allocate an empty `Row` with a pre-allocated capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            data: SmallVec::with_capacity(cap),
        }
    }

    /// Extend an existing `Row` with a `Datum`.
    #[inline]
    pub fn push<'a, D>(&mut self, datum: D)
    where
        D: Borrow<Datum<'a>>,
    {
        push_datum(self, *datum.borrow())
    }

    /// Extend an existing `Row` with additional `Datum`s.
    #[inline]
    pub fn extend<'a, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        for datum in iter {
            push_datum(self, *datum.borrow())
        }
    }

    /// Extend an existing `Row` with additional `Datum`s.
    ///
    /// In the case the iterator produces an error, the pushing of
    /// datums in terminated and the error returned. The `Row` will
    /// be incomplete, but it will be safe to read datums from it.
    #[inline]
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
        self.data.extend(row.data.iter().copied());
    }

    /// Clears the contents of the row without de-allocating its backing memory.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Creates a new row from supplied bytes.
    ///
    /// # Safety
    ///
    /// This method relies on `data` being an appropriate row encoding, and can
    /// result in unsafety if this is not the case.
    pub unsafe fn from_bytes_unchecked(data: Vec<u8>) -> Self {
        Row { data: data.into() }
    }

    /// Pack a slice of `Datum`s into a `Row`.
    ///
    /// This method has the advantage over `pack` that it can determine the required
    /// allocation before packing the elements, ensuring only one allocation and no
    /// redundant copies required.
    pub fn pack_slice<'a>(slice: &[Datum<'a>]) -> Row {
        // Pre-allocate the needed number of bytes.
        let mut row = Row::with_capacity(datums_size(slice.iter()));
        row.extend(slice.iter());
        row
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
        self.iter().next().unwrap()
    }

    /// Iterate the `Datum` elements of the `Row`.
    pub fn iter(&self) -> DatumListIter {
        DatumListIter {
            data: &self.data,
            offset: 0,
        }
    }

    /// Pushes a [`DatumList`] that is built from a closure.
    ///
    /// The supplied closure will be invoked once with a `Row` that can
    /// be used to populate the list. It is valid to call any method on the
    /// [`Row`] except for [`Row::finish_and_reuse`] or [`Row::truncate`].
    ///
    /// Returns the value returned by the closure, if any.
    ///
    /// ```
    /// # use repr::{Row, Datum};
    /// let mut row = Row::default();
    /// row.push_list_with(|row| {
    ///     row.push(Datum::String("age"));
    ///     row.push(Datum::Int64(42));
    /// });
    /// assert_eq!(
    ///     row.unpack_first().unwrap_list().iter().collect::<Vec<_>>(),
    ///     vec![Datum::String("age"), Datum::Int64(42)],
    /// );
    /// ```
    #[inline]
    pub fn push_list_with<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Row) -> R,
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

    /// Pushes a [`DatumMap`] that is built from a closure.
    ///
    /// The supplied closure will be invoked once with a `Row` that can be
    /// used to populate the dict.
    ///
    /// The closure **must** alternate pushing string keys and arbitary values,
    /// otherwise reading the dict will cause a panic.
    ///
    /// The closure **must** push keys in ascending order, otherwise equality
    /// checks on the resulting `Row` may be wrong and reading the dict IN DEBUG
    /// MODE will cause a panic.
    ///
    /// The closure **must not** call [`Row::finish_and_reuse`].
    ///
    /// # Example
    ///
    /// ```
    /// # use repr::{Row, Datum};
    /// let mut row = Row::default();
    /// row.push_dict_with(|row| {
    ///
    ///     // key
    ///     row.push(Datum::String("age"));
    ///     // value
    ///     row.push(Datum::Int64(42));
    ///
    ///     // key
    ///     row.push(Datum::String("name"));
    ///     // value
    ///     row.push(Datum::String("bob"));
    /// });
    /// assert_eq!(
    ///     row.unpack_first().unwrap_map().iter().collect::<Vec<_>>(),
    ///     vec![("age", Datum::Int64(42)), ("name", Datum::String("bob"))]
    /// );
    /// ```
    pub fn push_dict_with<F, R>(&mut self, f: F) -> R
    where
        F: FnOnce(&mut Row) -> R,
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

    /// Convenience function to construct an array from an iter of `Datum`s.
    ///
    /// Returns an error if the number of elements in `iter` does not match
    /// the cardinality of the array as described by `dims`, or if the
    /// number of dimensions exceeds [`MAX_ARRAY_DIMENSIONS`]. If an error
    /// occurs, the packer's state will be unchanged.
    pub fn push_array<'a, I, D>(
        &mut self,
        dims: &[ArrayDimension],
        iter: I,
    ) -> Result<(), InvalidArrayError>
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        // Arrays are encoded as follows.
        //
        // u8      ndims
        // usize   dim_0 lower bound
        // usize   dim_0 length
        // ...
        // usize   dim_n lower bound
        // usize   dim_n length
        // usize   element data size in bytes
        // u8      element data, where elements are encoded in row-major order

        if dims.len() > usize::from(MAX_ARRAY_DIMENSIONS) {
            return Err(InvalidArrayError::TooManyDimensions(dims.len()));
        }

        let start = self.data.len();
        self.data.push(Tag::Array as u8);

        // Write dimension information.
        self.data
            .push(dims.len().try_into().expect("ndims verified to fit in u8"));
        for dim in dims {
            push_copy!(&mut self.data, dim.lower_bound, usize);
            push_copy!(&mut self.data, dim.length, usize);
        }

        // Write elements.
        let off = self.data.len();
        push_copy!(&mut self.data, 0, usize); // dummy length fixed up below
        let mut nelements = 0;
        for datum in iter {
            self.push(*datum.borrow());
            nelements += 1;
        }
        let len = self.data.len() - off - size_of::<usize>();
        self.data[off..off + size_of::<usize>()].copy_from_slice(&len.to_le_bytes());

        // Check that the number of elements written matches the dimension
        // information.
        let cardinality = match dims {
            [] => 0,
            dims => dims.iter().map(|d| d.length).product(),
        };
        if nelements != cardinality {
            self.data.truncate(start);
            return Err(InvalidArrayError::WrongCardinality {
                actual: nelements,
                expected: cardinality,
            });
        }

        Ok(())
    }

    /// Convenience function to push a `DatumList` from an iter of `Datum`s
    ///
    /// See [`Row::push_dict_with`] if you need to be able to handle errors
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

    /// Convenience function to push a `DatumMap` from an iter of `(&str, Datum)` pairs
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

    /// Returns a copy of this `Row`, clearing the data but not the allocation
    /// in `self`.
    ///
    /// The intent is that `self`'s allocation can be used to pack additional
    /// rows, to reduce the amount of interaction with the allocator.
    pub fn finish_and_reuse(&mut self) -> Row {
        let data = SmallVec::from(&self.data[..]);
        self.data.clear();
        Row { data }
    }

    /// Truncates the underlying storage to the specified byte position.
    ///
    /// # Safety
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

    /// Truncates the row to contain at most the first `n` datums.
    ///
    /// # Panics
    pub fn truncate_datums(&mut self, n: usize) {
        let mut iter = self.iter();
        for _ in iter.by_ref().take(n) {}
        let offset = iter.offset;
        // SAFETY: iterator offsets always lie on a datum boundary.
        unsafe { self.truncate(offset) }
    }

    /// For debugging only
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> RowRef<'a> {
    /// Construct a `RowRef` from a byte slice.
    ///
    /// # Safety
    ///
    /// This method is unsafe because if the byte slice is not a valid
    /// row encoding, then unpacking its contents can cause undefined
    /// behavior.
    pub unsafe fn from_bytes_unchecked(data: &'a [u8]) -> Self {
        Self { data }
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
        self.iter().next().unwrap()
    }

    /// Iterate the `Datum` elements of the `Row`.
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

impl<'a> DatumMap<'a> {
    pub fn empty() -> DatumMap<'static> {
        DatumMap { data: &[] }
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

impl<'a> IntoIterator for &'a DatumMap<'a> {
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
                    key_tag == Tag::StringTiny
                        || key_tag == Tag::StringShort
                        || key_tag == Tag::StringLong
                        || key_tag == Tag::StringHuge,
                    "Dict keys must be strings, got {:?}",
                    key_tag
                );
                let key = read_lengthed_datum(self.data, &mut self.offset, key_tag).unwrap_str();
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

impl RowArena {
    pub fn new() -> Self {
        RowArena {
            inner: RefCell::new(RowArenaInner {
                owned_bytes: vec![],
                owned_rows: vec![],
            }),
        }
    }

    /// Take ownership of `bytes` for the lifetime of the arena.
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn push_bytes<'a>(&'a self, bytes: Vec<u8>) -> &'a [u8] {
        let mut inner = self.inner.borrow_mut();
        inner.owned_bytes.push(bytes);
        let owned_bytes = &inner.owned_bytes[inner.owned_bytes.len() - 1];
        unsafe {
            // This is safe because:
            //   * We only ever append to self.owned_bytes, so the byte vector
            //     will live as long as the arena.
            //   * We return a reference to the byte vector's contents, so it's
            //     okay if self.owned_bytes reallocates and moves the byte
            //     vector.
            //   * We don't allow access to the byte vector itself, so it will
            //     never reallocate.
            transmute::<&[u8], &'a [u8]>(owned_bytes)
        }
    }

    /// Take ownership of `string` for the lifetime of the arena.
    pub fn push_string<'a>(&'a self, string: String) -> &'a str {
        let owned_bytes = self.push_bytes(string.into_bytes());
        unsafe {
            // This is safe because we know it was a `String` just before.
            std::str::from_utf8_unchecked(owned_bytes)
        }
    }

    /// Take ownership of `row` for the lifetime of the arena, returning a
    /// reference to the first datum in the row.
    ///
    /// If we had an owned datum type, this method would be much clearer, and
    /// would be called `push_owned_datum`.
    pub fn push_unary_row<'a>(&'a self, row: Row) -> Datum<'a> {
        let mut inner = self.inner.borrow_mut();
        inner.owned_rows.push(row);
        let datum = inner.owned_rows[inner.owned_rows.len() - 1].unpack_first();
        unsafe {
            // This is safe because:
            //   * We only ever append to self.owned_rows, so the row will live
            //     as long as the arena.
            //   * We return a reference to the heap allocation inside the row,
            //     so it's okay if self.owned_rows reallocates and moves the
            //     row.
            //   * We don't allow access to the row itself, so row.data will
            //     never reallocate.
            transmute::<Datum<'_>, Datum<'a>>(datum)
        }
    }

    /// Convenience function to make a new `Row` containing a single datum, and
    /// take ownership of it for the lifetime of the arena
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
        F: FnOnce(&mut Row),
    {
        let mut row = Row::default();
        f(&mut row);
        self.push_unary_row(row)
    }

    /// Like [`RowArena::make_datum`], but the provided closure can return an error.
    pub fn try_make_datum<'a, F, E>(&'a self, f: F) -> Result<Datum<'a>, E>
    where
        F: FnOnce(&mut Row) -> Result<(), E>,
    {
        let mut row = Row::default();
        f(&mut row)?;
        Ok(self.push_unary_row(row))
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

        let mut row = Row::default();
        row.push_dict_with(|row| {
            row.push(Datum::String("a"));
            row.push_list_with(|row| {
                row.push(Datum::String("one"));
                row.push(Datum::String("two"));
                row.push(Datum::String("three"));
            });
            row.push(Datum::String("b"));
            row.push(Datum::String("c"));
        });
        assert_eq!(arena.push_unary_row(row.clone()), row.unpack_first());
    }

    #[test]
    fn miri_test_round_trip() {
        fn round_trip(datums: Vec<Datum>) {
            let row = Row::pack(datums.clone());

            // When run under miri this catchs undefined bytes written to data
            // eg by calling push_copy! on a type which contains undefined padding values
            println!("{:?}", row.data());

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
    fn test_array() {
        // Construct an array using `Row::push_array` and verify that it unpacks
        // correctly.
        const DIM: ArrayDimension = ArrayDimension {
            lower_bound: 2,
            length: 2,
        };
        let mut row = Row::default();
        row.push_array(&[DIM], vec![Datum::Int32(1), Datum::Int32(2)])
            .unwrap();
        let arr1 = row.unpack_first().unwrap_array();
        assert_eq!(arr1.dims().into_iter().collect::<Vec<_>>(), vec![DIM]);
        assert_eq!(
            arr1.elements().into_iter().collect::<Vec<_>>(),
            vec![Datum::Int32(1), Datum::Int32(2)]
        );

        // Pack a previously-constructed `Datum::Array` and verify that it
        // unpacks correctly.
        let row = Row::pack_slice(&[Datum::Array(arr1)]);
        let arr2 = row.unpack_first().unwrap_array();
        assert_eq!(arr1, arr2);
    }

    #[test]
    fn test_multidimensional_array() {
        let datums = vec![
            Datum::Int32(1),
            Datum::Int32(2),
            Datum::Int32(3),
            Datum::Int32(4),
            Datum::Int32(5),
            Datum::Int32(6),
            Datum::Int32(7),
            Datum::Int32(8),
        ];

        let mut row = Row::default();
        row.push_array(
            &[
                ArrayDimension {
                    lower_bound: 1,
                    length: 1,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 4,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
            ],
            &datums,
        )
        .unwrap();
        let array = row.unpack_first().unwrap_array();
        assert_eq!(array.elements().into_iter().collect::<Vec<_>>(), datums);
    }

    #[test]
    fn test_array_max_dimensions() {
        let mut row = Row::default();
        let max_dims = usize::from(MAX_ARRAY_DIMENSIONS);

        // An array with one too many dimensions should be rejected.
        let res = row.push_array(
            &vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 1
                };
                max_dims + 1
            ],
            vec![Datum::Int32(4)],
        );
        assert_eq!(res, Err(InvalidArrayError::TooManyDimensions(max_dims + 1)));
        assert!(row.data.is_empty());

        // An array with exactly the maximum allowable dimensions should be
        // accepted.
        row.push_array(
            &vec![
                ArrayDimension {
                    lower_bound: 1,
                    length: 1
                };
                max_dims
            ],
            vec![Datum::Int32(4)],
        )
        .unwrap();
    }

    #[test]
    fn test_array_wrong_cardinality() {
        let mut row = Row::default();
        let res = row.push_array(
            &[
                ArrayDimension {
                    lower_bound: 1,
                    length: 2,
                },
                ArrayDimension {
                    lower_bound: 1,
                    length: 3,
                },
            ],
            vec![Datum::Int32(1), Datum::Int32(2)],
        );
        assert_eq!(
            res,
            Err(InvalidArrayError::WrongCardinality {
                actual: 2,
                expected: 6,
            })
        );
        assert!(row.data.is_empty());
    }

    #[test]
    fn test_nesting() {
        let mut row = Row::default();
        row.push_dict_with(|row| {
            row.push(Datum::String("favourites"));
            row.push_list_with(|row| {
                row.push(Datum::String("ice cream"));
                row.push(Datum::String("oreos"));
                row.push(Datum::String("cheesecake"));
            });
            row.push(Datum::String("name"));
            row.push(Datum::String("bob"));
        });

        let mut iter = row.unpack_first().unwrap_map().iter();

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
            let mut row = Row::default();
            row.push_dict_with(|row| {
                if ok {
                    row.push(Datum::String("key"));
                    row.push(Datum::Int32(42));
                    Ok(7)
                } else {
                    Err("fail")
                }
            })?;
            Ok(row)
        };

        assert_eq!(pack(false), Err("fail"));

        let row = pack(true)?;
        let mut dict = row.unpack_first().unwrap_map().iter();
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
            if datum_size(&value) != Row::pack_slice(&[value]).data.len() {
                panic!("Disparity in claimed size for {:?}", value);
            }
        }
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn row_size_is_stable() {
        // nothin depends on this being exactly 32, we just want it to be an active decision if we
        // change it
        assert_eq!(std::mem::size_of::<super::Row>(), 32);
    }
}
