// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Debug};
use std::mem::{size_of, transmute};
use std::str;

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use mz_ore::soft_assert;
use mz_ore::vec::Vector;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use ordered_float::OrderedFloat;
use proptest::prelude::*;
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Deserialize, Serialize, Serializer};
use smallvec::SmallVec;
use uuid::Uuid;

use mz_ore::cast::CastFrom;

use crate::adt::array::{
    Array, ArrayDimension, ArrayDimensions, InvalidArrayError, MAX_ARRAY_DIMENSIONS,
};
use crate::adt::interval::Interval;
use crate::adt::numeric;
use crate::adt::numeric::Numeric;
use crate::scalar::arb_datum;
use crate::Datum;

mod encoding;

include!(concat!(env!("OUT_DIR"), "/mz_repr.row.rs"));

/// A packed representation for `Datum`s.
///
/// `Datum` is easy to work with but very space inefficient. A `Datum::Int32(42)`
/// is laid out in memory like this:
///
///   tag: 3
///   padding: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
///   data: 0 0 0 42
///   padding: 0 0 0 0 0 0 0 0 0 0 0 0
///
/// For a total of 32 bytes! The second set of padding is needed in case we were
/// to write a 16-byte datum into this location. The first set of padding is
/// needed to align that hypothetical decimal to a 16 bytes boundary.
///
/// A `Row` stores zero or more `Datum`s without any padding. We avoid the need
/// for the first set of padding by only providing access to the `Datum`s via
/// calls to `ptr::read_unaligned`, which on modern x86 is barely penalized. We
/// avoid the need for the second set of padding by not providing mutable access
/// to the `Datum`. Instead, `Row` is append-only.
///
/// A `Row` can be built from a collection of `Datum`s using `Row::pack`, but it
/// is more efficient to use `Row::pack_slice` so that a right-sized allocation
/// can be created. If that is not possible, consider using the row buffer
/// pattern: allocate one row, pack into it, and then call [`Row::clone`] to
/// receive a copy of that row, leaving behind the original allocation to pack
/// future rows.
///
/// Creating a row via [`Row::pack_slice`]:
///
/// ```
/// # use mz_repr::{Row, Datum};
/// let row = Row::pack_slice(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.unpack(), vec![Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)])
/// ```
///
/// `Row`s can be unpacked by iterating over them:
///
/// ```
/// # use mz_repr::{Row, Datum};
/// let row = Row::pack_slice(&[Datum::Int32(0), Datum::Int32(1), Datum::Int32(2)]);
/// assert_eq!(row.iter().nth(1).unwrap(), Datum::Int32(1));
/// ```
///
/// If you want random access to the `Datum`s in a `Row`, use `Row::unpack` to create a `Vec<Datum>`
/// ```
/// # use mz_repr::{Row, Datum};
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
#[derive(Default, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Row {
    data: SmallVec<[u8; Self::SIZE]>,
}

impl Arbitrary for Row {
    type Parameters = prop::collection::SizeRange;
    type Strategy = BoxedStrategy<Row>;

    fn arbitrary_with(size: Self::Parameters) -> Self::Strategy {
        prop::collection::vec(arb_datum(), size)
            .prop_map(|items| {
                let mut row = Row::default();
                let mut packer = row.packer();
                for item in items.iter() {
                    let datum: Datum<'_> = item.into();
                    packer.push(datum);
                }
                row
            })
            .boxed()
    }
}

// Implement Clone manually to use SmallVec's more efficient from_slice.
// TODO: Revisit once Rust supports specialization: https://github.com/rust-lang/rust/issues/31844
impl Clone for Row {
    fn clone(&self) -> Self {
        Self {
            data: SmallVec::from_slice(self.data.as_slice()),
        }
    }
}

impl Row {
    const SIZE: usize = 24;
}

/// These implementations order first by length, and then by slice contents.
/// This allows many comparisons to complete without dereferencing memory.
/// Warning: These order by the u8 array representation, and NOT by Datum::cmp.
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

/// Packs datums into a [`Row`].
///
/// Creating a `RowPacker` via [`Row::packer`] starts a packing operation on the
/// row. A packing operation always starts from scratch: the existing contents
/// of the underlying row are cleared.
///
/// To complete a packing operation, drop the `RowPacker`.
#[derive(Debug)]
pub struct RowPacker<'a> {
    row: &'a mut Row,
}

/// A wrapper around a byte slice that guarantees the data are row-formatted.
///
/// This type exists to allow row-formatted data to be stored in types that
/// need not contain a `Row`, for example large contiguous `[u8]` allocations.
/// It is not expected that most users will use this type, especially as its
/// only constructor is unsafe.
#[repr(transparent)]
pub struct RowRef {
    data: [u8],
}

#[derive(Debug, Clone)]
pub struct DatumListIter<'a> {
    data: &'a [u8],
    offset: usize,
}

#[derive(Debug, Clone)]
pub struct DatumDictIter<'a> {
    data: &'a [u8],
    offset: usize,
    prev_key: Option<&'a str>,
}

/// `RowArena` is used to hold on to temporary `Row`s for functions like `eval` that need to create complex `Datum`s but don't have a `Row` to put them in yet.
#[derive(Debug)]
pub struct RowArena {
    // Semantically, this field would be better represented by a `Vec<Box<[u8]>>`,
    // as once the arena takes ownership of a byte vector the vector is never
    // modified. But `RowArena::push_bytes` takes ownership of a `Vec<u8>`, so
    // storing that `Vec<u8>` directly avoids an allocation. The cost is
    // additional memory use, as the vector may have spare capacity, but row
    // arenas are short lived so this is the better tradeoff.
    inner: RefCell<Vec<Vec<u8>>>,
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

impl<'a> Serialize for DatumList<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(None)?;
        let mut iter = self.iter();
        while let Some(datum) = iter.next() {
            seq.serialize_element(&datum)?;
        }
        seq.end()
    }
}

/// A mapping from string keys to Datums
#[derive(Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct DatumMap<'a> {
    /// Points at the serialized datums, which should be sorted in key order
    data: &'a [u8],
}

impl<'a> Serialize for DatumMap<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        let mut iter = self.iter();
        while let Some((key, val)) = iter.next() {
            map.serialize_entry(&key, &val)?;
        }
        map.end()
    }
}

// All new tags MUST be added to the end of the enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum Tag {
    Null,
    False,
    True,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt32,
    Float32,
    Float64,
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
    UInt16,
    UInt64,
}

// --------------------------------------------------------------------------------
// reading data

/// Read a byte slice starting at byte `offset`.
///
/// Updates `offset` to point to the first byte after the end of the read region.
fn read_untagged_bytes<'a>(data: &'a [u8], offset: &mut usize) -> &'a [u8] {
    let len = u64::from_le_bytes(read_byte_array(data, offset));
    let len = usize::cast_from(len);
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
        Tag::BytesTiny | Tag::StringTiny => usize::from(read_byte(data, offset)),
        Tag::BytesShort | Tag::StringShort => {
            usize::from(u16::from_le_bytes(read_byte_array(data, offset)))
        }
        Tag::BytesLong | Tag::StringLong => {
            usize::cast_from(u32::from_le_bytes(read_byte_array(data, offset)))
        }
        Tag::BytesHuge | Tag::StringHuge => {
            usize::cast_from(u64::from_le_bytes(read_byte_array(data, offset)))
        }
        _ => unreachable!(),
    };
    let bytes = &data[*offset..(*offset + len)];
    *offset += len;
    match tag {
        Tag::BytesTiny | Tag::BytesShort | Tag::BytesLong | Tag::BytesHuge => Datum::Bytes(bytes),
        Tag::StringTiny | Tag::StringShort | Tag::StringLong | Tag::StringHuge => {
            Datum::String(str::from_utf8_unchecked(bytes))
        }
        _ => unreachable!(),
    }
}

fn read_byte(data: &[u8], offset: &mut usize) -> u8 {
    let byte = data[*offset];
    *offset += 1;
    byte
}

fn read_byte_array<const N: usize>(data: &[u8], offset: &mut usize) -> [u8; N] {
    let mut raw = [0; N];
    raw.copy_from_slice(&data[*offset..*offset + N]);
    *offset += N;
    raw
}

fn read_date(data: &[u8], offset: &mut usize) -> NaiveDate {
    let year = i32::from_le_bytes(read_byte_array(data, offset));
    let ordinal = u32::from_le_bytes(read_byte_array(data, offset));
    NaiveDate::from_yo(year, ordinal)
}

fn read_time(data: &[u8], offset: &mut usize) -> NaiveTime {
    let secs = u32::from_le_bytes(read_byte_array(data, offset));
    let nanos = u32::from_le_bytes(read_byte_array(data, offset));
    NaiveTime::from_num_seconds_from_midnight(secs, nanos)
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
    let tag = Tag::try_from_primitive(read_byte(data, offset)).expect("unknown row tag");
    match tag {
        Tag::Null => Datum::Null,
        Tag::False => Datum::False,
        Tag::True => Datum::True,
        Tag::Int16 => {
            let i = i16::from_le_bytes(read_byte_array(data, offset));
            Datum::Int16(i)
        }
        Tag::Int32 => {
            let i = i32::from_le_bytes(read_byte_array(data, offset));
            Datum::Int32(i)
        }
        Tag::Int64 => {
            let i = i64::from_le_bytes(read_byte_array(data, offset));
            Datum::Int64(i)
        }
        Tag::UInt8 => {
            let i = u8::from_le_bytes(read_byte_array(data, offset));
            Datum::UInt8(i)
        }
        Tag::UInt16 => {
            let i = u16::from_le_bytes(read_byte_array(data, offset));
            Datum::UInt16(i)
        }
        Tag::UInt32 => {
            let i = u32::from_le_bytes(read_byte_array(data, offset));
            Datum::UInt32(i)
        }
        Tag::UInt64 => {
            let i = u64::from_le_bytes(read_byte_array(data, offset));
            Datum::UInt64(i)
        }
        Tag::Float32 => {
            let f = f32::from_bits(u32::from_le_bytes(read_byte_array(data, offset)));
            Datum::Float32(OrderedFloat::from(f))
        }
        Tag::Float64 => {
            let f = f64::from_bits(u64::from_le_bytes(read_byte_array(data, offset)));
            Datum::Float64(OrderedFloat::from(f))
        }
        Tag::Date => Datum::Date(read_date(data, offset)),
        Tag::Time => Datum::Time(read_time(data, offset)),
        Tag::Timestamp => {
            let date = read_date(data, offset);
            let time = read_time(data, offset);
            Datum::Timestamp(date.and_time(time))
        }
        Tag::TimestampTz => {
            let date = read_date(data, offset);
            let time = read_time(data, offset);
            Datum::TimestampTz(DateTime::from_utc(date.and_time(time), Utc))
        }
        Tag::Interval => {
            let months = i32::from_le_bytes(read_byte_array(data, offset));
            let days = i32::from_le_bytes(read_byte_array(data, offset));
            let micros = i64::from_le_bytes(read_byte_array(data, offset));
            Datum::Interval(Interval {
                months,
                days,
                micros,
            })
        }
        Tag::BytesTiny
        | Tag::BytesShort
        | Tag::BytesLong
        | Tag::BytesHuge
        | Tag::StringTiny
        | Tag::StringShort
        | Tag::StringLong
        | Tag::StringHuge => read_lengthed_datum(data, offset, tag),
        Tag::Uuid => Datum::Uuid(Uuid::from_bytes(read_byte_array(data, offset))),
        Tag::Array => {
            // See the comment in `Row::push_array` for details on the encoding
            // of arrays.
            let ndims = read_byte(data, offset);
            let dims_size = usize::from(ndims) * size_of::<u64>() * 2;
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
            let digits = read_byte(data, offset).into();
            let exponent = read_byte(data, offset) as i8;
            let bits = read_byte(data, offset);

            let lsu_u16_len = Numeric::digits_to_lsu_elements_len(digits);
            let lsu_u8_len = lsu_u16_len * 2;
            let lsu_u8 = &data[*offset..(*offset + lsu_u8_len)];
            *offset += lsu_u8_len;

            // TODO: if we refactor the decimal library to accept the owned
            // array as a parameter to `from_raw_parts` below, we could likely
            // avoid a copy because it is exactly the value we want
            let mut lsu = [0; numeric::NUMERIC_DATUM_WIDTH_USIZE];
            for (i, c) in lsu_u8.chunks(2).enumerate() {
                lsu[i] = u16::from_le_bytes(c.try_into().unwrap());
            }

            let d = Numeric::from_raw_parts(digits, exponent.into(), bits, lsu);
            Datum::from(d)
        }
    }
}

// --------------------------------------------------------------------------------
// writing data

fn push_untagged_bytes<D>(data: &mut D, bytes: &[u8])
where
    D: Vector<u8>,
{
    let len = u64::cast_from(bytes.len());
    data.extend_from_slice(&len.to_le_bytes());
    data.extend_from_slice(bytes);
}

fn push_lengthed_bytes<D>(data: &mut D, bytes: &[u8], tag: Tag)
where
    D: Vector<u8>,
{
    match tag {
        Tag::BytesTiny | Tag::StringTiny => {
            let len = bytes.len() as u8;
            data.push(len);
        }
        Tag::BytesShort | Tag::StringShort => {
            let len = bytes.len() as u16;
            data.extend_from_slice(&len.to_le_bytes());
        }
        Tag::BytesLong | Tag::StringLong => {
            let len = bytes.len() as u32;
            data.extend_from_slice(&len.to_le_bytes());
        }
        Tag::BytesHuge | Tag::StringHuge => {
            let len = u64::cast_from(bytes.len());
            data.extend_from_slice(&len.to_le_bytes());
        }
        _ => unreachable!(),
    }
    data.extend_from_slice(bytes);
}

fn push_date<D>(data: &mut D, date: NaiveDate)
where
    D: Vector<u8>,
{
    data.extend_from_slice(&i32::to_le_bytes(date.year()));
    data.extend_from_slice(&u32::to_le_bytes(date.ordinal()));
}

fn push_time<D>(data: &mut D, time: NaiveTime)
where
    D: Vector<u8>,
{
    data.extend_from_slice(&u32::to_le_bytes(time.num_seconds_from_midnight()));
    data.extend_from_slice(&u32::to_le_bytes(time.nanosecond()));
}

fn push_datum<D>(data: &mut D, datum: Datum)
where
    D: Vector<u8>,
{
    match datum {
        Datum::Null => data.push(Tag::Null.into()),
        Datum::False => data.push(Tag::False.into()),
        Datum::True => data.push(Tag::True.into()),
        Datum::Int16(i) => {
            data.push(Tag::Int16.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::Int32(i) => {
            data.push(Tag::Int32.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::Int64(i) => {
            data.push(Tag::Int64.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::UInt8(i) => {
            data.push(Tag::UInt8.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::UInt16(i) => {
            data.push(Tag::UInt16.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::UInt32(i) => {
            data.push(Tag::UInt32.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::UInt64(i) => {
            data.push(Tag::UInt64.into());
            data.extend_from_slice(&i.to_le_bytes());
        }
        Datum::Float32(f) => {
            data.push(Tag::Float32.into());
            data.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Datum::Float64(f) => {
            data.push(Tag::Float64.into());
            data.extend_from_slice(&f.to_bits().to_le_bytes());
        }
        Datum::Date(d) => {
            data.push(Tag::Date.into());
            push_date(data, d);
        }
        Datum::Time(t) => {
            data.push(Tag::Time.into());
            push_time(data, t);
        }
        Datum::Timestamp(t) => {
            data.push(Tag::Timestamp.into());
            push_date(data, t.date());
            push_time(data, t.time());
        }
        Datum::TimestampTz(t) => {
            data.push(Tag::TimestampTz.into());
            push_date(data, t.date().naive_utc());
            push_time(data, t.time());
        }
        Datum::Interval(i) => {
            data.push(Tag::Interval.into());
            data.extend_from_slice(&i.months.to_le_bytes());
            data.extend_from_slice(&i.days.to_le_bytes());
            data.extend_from_slice(&i.micros.to_le_bytes());
        }
        Datum::Bytes(bytes) => {
            let tag = match bytes.len() {
                0..=255 => Tag::BytesTiny,
                256..=65535 => Tag::BytesShort,
                65536..=4294967295 => Tag::BytesLong,
                _ => Tag::BytesHuge,
            };
            data.push(tag.into());
            push_lengthed_bytes(data, bytes, tag);
        }
        Datum::String(string) => {
            let tag = match string.len() {
                0..=255 => Tag::StringTiny,
                256..=65535 => Tag::StringShort,
                65536..=4294967295 => Tag::StringLong,
                _ => Tag::StringHuge,
            };
            data.push(tag.into());
            push_lengthed_bytes(data, string.as_bytes(), tag);
        }
        Datum::Uuid(u) => {
            data.push(Tag::Uuid.into());
            data.extend_from_slice(u.as_bytes());
        }
        Datum::Array(array) => {
            // See the comment in `Row::push_array` for details on the encoding
            // of arrays.
            data.push(Tag::Array.into());
            data.push(array.dims.ndims());
            data.extend_from_slice(array.dims.data);
            push_untagged_bytes(data, &array.elements.data);
        }
        Datum::List(list) => {
            data.push(Tag::List.into());
            push_untagged_bytes(data, &list.data);
        }
        Datum::Map(dict) => {
            data.push(Tag::Dict.into());
            push_untagged_bytes(data, &dict.data);
        }
        Datum::JsonNull => data.push(Tag::JsonNull.into()),
        Datum::Dummy => data.push(Tag::Dummy.into()),
        Datum::Numeric(mut n) => {
            // Pseudo-canonical representation of decimal values with
            // insignificant zeroes trimmed. This compresses the number further
            // than `Numeric::trim` by removing all zeroes, and not only those in
            // the fractional component.
            numeric::cx_datum().reduce(&mut n.0);
            let (digits, exponent, bits, lsu) = n.0.to_raw_parts();
            data.push(Tag::Numeric.into());
            data.push(u8::try_from(digits).expect("digits to fit within u8; should not exceed 39"));
            data.push(
                i8::try_from(exponent).expect("exponent to fit within i8; should not exceed +/- 39")
                    as u8,
            );
            data.push(bits);

            let lsu = &lsu[..Numeric::digits_to_lsu_elements_len(digits)];

            // Little endian machines can take the lsu directly from u16 to u8.
            if cfg!(target_endian = "little") {
                // SAFETY: `lsu` (returned by `coefficient_units()`) is a `&[u16]`, so
                // each element can safely be transmuted into two `u8`s.
                let (prefix, lsu_bytes, suffix) = unsafe { lsu.align_to::<u8>() };
                // The `u8` aligned version of the `lsu` should have twice as many
                // elements as we expect for the `u16` version.
                soft_assert!(
                    lsu_bytes.len() == Numeric::digits_to_lsu_elements_len(digits) * 2,
                    "u8 version of numeric LSU contained the wrong number of elements; expected {}, but got {}",
                    Numeric::digits_to_lsu_elements_len(digits) * 2,
                    lsu_bytes.len()
                );
                // There should be no unaligned elements in the prefix or suffix.
                soft_assert!(prefix.is_empty() && suffix.is_empty());
                data.extend_from_slice(&lsu_bytes);
            } else {
                for u in lsu {
                    data.extend_from_slice(&u.to_le_bytes());
                }
            }
        }
    }
}

/// Return the number of bytes these Datums would use if packed as a Row.
pub fn row_size<'a, I>(a: I) -> usize
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // Using datums_size instead of a.data().len() here is safer because it will
    // return the size of the datums if they were packed into a Row. Although
    // a.data().len() happens to give the correct answer (and is faster), data()
    // is documented as for debugging only.
    let sz = datums_size(a);
    let size_of_row = std::mem::size_of::<Row>();
    // The Row struct attempts to inline data until it can't fit in the
    // preallocated size. Otherwise it spills to heap, and uses the Row to point
    // to that.
    if sz > Row::SIZE {
        sz + size_of_row
    } else {
        size_of_row
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
        Datum::Int16(_) => 1 + size_of::<i16>(),
        Datum::Int32(_) => 1 + size_of::<i32>(),
        Datum::Int64(_) => 1 + size_of::<i64>(),
        Datum::UInt8(_) => 1 + size_of::<u8>(),
        Datum::UInt16(_) => 1 + size_of::<u16>(),
        Datum::UInt32(_) => 1 + size_of::<u32>(),
        Datum::UInt64(_) => 1 + size_of::<u64>(),
        Datum::Float32(_) => 1 + size_of::<f32>(),
        Datum::Float64(_) => 1 + size_of::<f64>(),
        Datum::Date(_) => 1 + 8,
        Datum::Time(_) => 1 + 8,
        Datum::Timestamp(_) => 1 + 16,
        Datum::TimestampTz(_) => 1 + 16,
        Datum::Interval(_) => 1 + size_of::<i32>() + size_of::<i32>() + size_of::<i64>(),
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
        Datum::Uuid(_) => 1 + size_of::<uuid::Bytes>(),
        Datum::Array(array) => {
            1 + size_of::<u8>()
                + array.dims.data.len()
                + size_of::<u64>()
                + array.elements.data.len()
        }
        Datum::List(list) => 1 + size_of::<u64>() + list.data.len(),
        Datum::Map(dict) => 1 + size_of::<u64>() + dict.data.len(),
        Datum::JsonNull => 1,
        Datum::Dummy => 1,
        Datum::Numeric(d) => {
            let mut d = d.0.clone();
            // Values must be reduced to determine appropriate number of
            // coefficient units.
            numeric::cx_datum().reduce(&mut d);
            // 4 = 1 bit each for tag, digits, exponent, bits
            4 + (d.coefficient_units().len() * 2)
        }
    }
}

/// Number of bytes required by a sequence of datums.
///
/// This method can be used to right-size the allocation for a `Row`
/// before calling [`RowPacker::extend`].
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
    1 + size_of::<u64>() + datums_size(iter)
}

// --------------------------------------------------------------------------------
// public api

impl Row {
    /// Allocate an empty `Row` with a pre-allocated capacity.
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            data: SmallVec::with_capacity(cap),
        }
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

    /// Constructs a [`RowPacker`] that will pack datums into this row's
    /// allocation.
    ///
    /// This method clears the existing contents of the row, but retains the
    /// allocation.
    pub fn packer(&mut self) -> RowPacker<'_> {
        self.data.clear();
        RowPacker { row: self }
    }

    /// Take some `Datum`s and pack them into a `Row`.
    ///
    /// This method builds a `Row` by repeatedly increasing the backing
    /// allocation. If the contents of the iterator are known ahead of
    /// time, consider [`Row::with_capacity`] to right-size the allocation
    /// first, and then [`RowPacker::extend`] to populate it with `Datum`s.
    /// This avoids the repeated allocation resizing and copying.
    pub fn pack<'a, I, D>(iter: I) -> Row
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        let mut row = Row::default();
        row.packer().extend(iter);
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
        row.packer().try_extend(iter)?;
        Ok(row)
    }

    /// Pack a slice of `Datum`s into a `Row`.
    ///
    /// This method has the advantage over `pack` that it can determine the required
    /// allocation before packing the elements, ensuring only one allocation and no
    /// redundant copies required.
    pub fn pack_slice<'a>(slice: &[Datum<'a>]) -> Row {
        // Pre-allocate the needed number of bytes.
        let mut row = Row::with_capacity(datums_size(slice.iter()));
        row.packer().extend(slice.iter());
        row
    }
}

impl RowPacker<'_> {
    /// Constructs a row packer that will pack additional datums into the
    /// provided row.
    ///
    /// This function is intentionally somewhat inconvenient to call. You
    /// usually want to call [`Row::packer`] instead to start packing from
    /// scratch.
    pub fn for_existing_row(row: &mut Row) -> RowPacker {
        RowPacker { row }
    }

    /// Extend an existing `Row` with a `Datum`.
    #[inline]
    pub fn push<'a, D>(&mut self, datum: D)
    where
        D: Borrow<Datum<'a>>,
    {
        push_datum(&mut self.row.data, *datum.borrow())
    }

    /// Extend an existing `Row` with additional `Datum`s.
    #[inline]
    pub fn extend<'a, I, D>(&mut self, iter: I)
    where
        I: IntoIterator<Item = D>,
        D: Borrow<Datum<'a>>,
    {
        for datum in iter {
            push_datum(&mut self.row.data, *datum.borrow())
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
        self.row.data.extend(row.data.iter().copied());
    }

    /// Pushes a [`DatumList`] that is built from a closure.
    ///
    /// The supplied closure will be invoked once with a `Row` that can be used
    /// to populate the list. It is valid to call any method on the
    /// [`RowPacker`] except for [`RowPacker::clear`], [`RowPacker::truncate`],
    /// or [`RowPacker::truncate_datums`].
    ///
    /// Returns the value returned by the closure, if any.
    ///
    /// ```
    /// # use mz_repr::{Row, Datum};
    /// let mut row = Row::default();
    /// row.packer().push_list_with(|row| {
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
        F: FnOnce(&mut RowPacker) -> R,
    {
        self.row.data.push(Tag::List as u8);
        let start = self.row.data.len();
        // write a dummy len, will fix it up later
        self.row.data.extend_from_slice(&[0; size_of::<u64>()]);

        let out = f(self);

        let len = u64::cast_from(self.row.data.len() - start - size_of::<u64>());
        // fix up the len
        self.row.data[start..start + size_of::<u64>()].copy_from_slice(&len.to_le_bytes());

        out
    }

    /// Pushes a [`DatumMap`] that is built from a closure.
    ///
    /// The supplied closure will be invoked once with a `Row` that can be used
    /// to populate the dict.
    ///
    /// The closure **must** alternate pushing string keys and arbitrary values,
    /// otherwise reading the dict will cause a panic.
    ///
    /// The closure **must** push keys in ascending order, otherwise equality
    /// checks on the resulting `Row` may be wrong and reading the dict IN DEBUG
    /// MODE will cause a panic.
    ///
    /// The closure **must not** call [`RowPacker::clear`],
    /// [`RowPacker::truncate`], or [`RowPacker::truncate_datums`].
    ///
    /// # Example
    ///
    /// ```
    /// # use mz_repr::{Row, Datum};
    /// let mut row = Row::default();
    /// row.packer().push_dict_with(|row| {
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
        F: FnOnce(&mut RowPacker) -> R,
    {
        self.row.data.push(Tag::Dict as u8);
        let start = self.row.data.len();
        // write a dummy len, will fix it up later
        self.row.data.extend_from_slice(&[0; size_of::<u64>()]);

        let res = f(self);

        let len = u64::cast_from(self.row.data.len() - start - size_of::<u64>());
        // fix up the len
        self.row.data[start..start + size_of::<u64>()].copy_from_slice(&len.to_le_bytes());

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
        // u8    ndims
        // u64   dim_0 lower bound
        // u64   dim_0 length
        // ...
        // u64   dim_n lower bound
        // u64   dim_n length
        // u64   element data size in bytes
        // u8    element data, where elements are encoded in row-major order

        if dims.len() > usize::from(MAX_ARRAY_DIMENSIONS) {
            return Err(InvalidArrayError::TooManyDimensions(dims.len()));
        }

        let start = self.row.data.len();
        self.row.data.push(Tag::Array as u8);

        // Write dimension information.
        self.row
            .data
            .push(dims.len().try_into().expect("ndims verified to fit in u8"));
        for dim in dims {
            self.row
                .data
                .extend_from_slice(&u64::cast_from(dim.lower_bound).to_le_bytes());
            self.row
                .data
                .extend_from_slice(&u64::cast_from(dim.length).to_le_bytes());
        }

        // Write elements.
        let off = self.row.data.len();
        self.row.data.extend_from_slice(&[0; size_of::<u64>()]);
        let mut nelements = 0;
        for datum in iter {
            self.push(*datum.borrow());
            nelements += 1;
        }
        let len = u64::cast_from(self.row.data.len() - off - size_of::<u64>());
        self.row.data[off..off + size_of::<u64>()].copy_from_slice(&len.to_le_bytes());

        // Check that the number of elements written matches the dimension
        // information.
        let cardinality = match dims {
            [] => 0,
            dims => dims.iter().map(|d| d.length).product(),
        };
        if nelements != cardinality {
            self.row.data.truncate(start);
            return Err(InvalidArrayError::WrongCardinality {
                actual: nelements,
                expected: cardinality,
            });
        }

        Ok(())
    }

    /// Convenience function to push a `DatumList` from an iter of `Datum`s
    ///
    /// See [`RowPacker::push_dict_with`] if you need to be able to handle errors
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

    /// Clears the contents of the packer without de-allocating its backing memory.
    pub fn clear(&mut self) {
        self.row.data.clear();
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
        self.row.data.truncate(pos)
    }

    /// Truncates the underlying row to contain at most the first `n` datums.
    pub fn truncate_datums(&mut self, n: usize) {
        let mut iter = self.row.iter();
        for _ in iter.by_ref().take(n) {}
        let offset = iter.offset;
        // SAFETY: iterator offsets always lie on a datum boundary.
        unsafe { self.truncate(offset) }
    }
}

impl RowRef {
    /// Construct a `RowRef` from a byte slice.
    ///
    /// # Safety
    ///
    /// This method is unsafe because if the byte slice is not a valid
    /// row encoding, then unpacking its contents can cause undefined
    /// behavior.
    pub unsafe fn from_bytes_unchecked(data: &[u8]) -> &Self {
        // SAFETY: RowRef just wraps [u8], and data is &[u8],
        // therefore transmuting &[u8] to &RowRef is safe.
        std::mem::transmute(data)
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

    /// For debugging only
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn byte_len(&self) -> usize {
        self.data.len()
    }

    /// True iff there is no data in this Row
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl std::ops::Deref for Row {
    type Target = RowRef;

    fn deref(&self) -> &Self::Target {
        // SAFETY: self.data is a valid Row encoding since self is a Row
        unsafe { RowRef::from_bytes_unchecked(&*self.data) }
    }
}

impl<'a> IntoIterator for &'a RowRef {
    type Item = Datum<'a>;
    type IntoIter = DatumListIter<'a>;
    fn into_iter(self) -> DatumListIter<'a> {
        self.iter()
    }
}

impl fmt::Debug for RowRef {
    /// Debug representation using the internal datums
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("Row{")?;
        f.debug_list().entries(self.iter()).finish()?;
        f.write_str("}")
    }
}

impl fmt::Debug for Row {
    /// Debug representation using the internal datums
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl fmt::Display for RowRef {
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

impl fmt::Display for Row {
    /// Debug representation using the internal datums
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
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

impl<'a> Debug for DatumMap<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
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
            let key_tag = Tag::try_from_primitive(read_byte(self.data, &mut self.offset))
                .expect("unknown row tag");
            assert!(
                key_tag == Tag::StringTiny
                    || key_tag == Tag::StringShort
                    || key_tag == Tag::StringLong
                    || key_tag == Tag::StringHuge,
                "Dict keys must be strings, got {:?}",
                key_tag
            );
            let key =
                unsafe { read_lengthed_datum(self.data, &mut self.offset, key_tag).unwrap_str() };
            let val = unsafe { read_datum(self.data, &mut self.offset) };

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

            Some((key, val))
        }
    }
}

impl RowArena {
    pub fn new() -> Self {
        RowArena {
            inner: RefCell::new(vec![]),
        }
    }

    /// Take ownership of `bytes` for the lifetime of the arena.
    #[allow(clippy::transmute_ptr_to_ptr)]
    pub fn push_bytes<'a>(&'a self, bytes: Vec<u8>) -> &'a [u8] {
        let mut inner = self.inner.borrow_mut();
        inner.push(bytes);
        let owned_bytes = &inner[inner.len() - 1];
        unsafe {
            // This is safe because:
            //   * We only ever append to self.inner, so the byte vector
            //     will live as long as the arena.
            //   * We return a reference to the byte vector's contents, so it's
            //     okay if self.inner reallocates and moves the byte
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
        inner.push(row.data.into_vec());
        unsafe {
            // This is safe because:
            //   * We only ever append to self.inner, so the row data will live
            //     as long as the arena.
            //   * We force the row data into its own heap allocation--
            //     importantly, we do NOT store the SmallVec, which might be
            //     storing data inline--so it's okay if self.inner reallocates
            //     and moves the row.
            //   * We don't allow access to the byte vector itself, so it will
            //     never reallocate.
            let datum = read_datum(&inner[inner.len() - 1], &mut 0);
            transmute::<Datum<'_>, Datum<'a>>(datum)
        }
    }

    /// Convenience function to make a new `Row` containing a single datum, and
    /// take ownership of it for the lifetime of the arena
    ///
    /// ```
    /// # use mz_repr::{RowArena, Datum};
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
        let mut row = Row::default();
        f(&mut row.packer());
        self.push_unary_row(row)
    }

    /// Like [`RowArena::make_datum`], but the provided closure can return an error.
    pub fn try_make_datum<'a, F, E>(&'a self, f: F) -> Result<Datum<'a>, E>
    where
        F: FnOnce(&mut RowPacker) -> Result<(), E>,
    {
        let mut row = Row::default();
        f(&mut row.packer())?;
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
    use chrono::NaiveDateTime;

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
        let mut packer = row.packer();
        packer.push_dict_with(|row| {
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

            // When run under miri this catches undefined bytes written to data
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
            Datum::Int16(-21),
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
            Datum::Interval(Interval::new(0, 0, 1_012_312)),
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
        let mut packer = row.packer();
        packer
            .push_array(&[DIM], vec![Datum::Int32(1), Datum::Int32(2)])
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
        let mut packer = row.packer();
        packer
            .push_array(
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
        let res = row.packer().push_array(
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
        row.packer()
            .push_array(
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
        let res = row.packer().push_array(
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
        row.packer().push_dict_with(|row| {
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
            row.packer().push_dict_with(|row| {
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
            Datum::Int16(0),
            Datum::Int32(0),
            Datum::Int64(0),
            Datum::Float32(OrderedFloat(0.0)),
            Datum::Float64(OrderedFloat(0.0)),
            Datum::from(numeric::Numeric::from(0)),
            Datum::from(numeric::Numeric::from(1000)),
            Datum::from(numeric::Numeric::from(9999)),
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
        // nothing depends on this being exactly 32, we just want it to be an active decision if we
        // change it
        assert_eq!(std::mem::size_of::<super::Row>(), 32);
    }
}
