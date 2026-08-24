// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoding a row through a predicted class per column.
//!
//! [`super::read_datum`] dispatches over roughly thirty tag classes, which makes its body too
//! large for a caller to inline, so every datum costs a call returning a 48-byte value through a
//! hidden pointer. Selecting the arm from the column's class instead of its tag shrinks the match
//! to the classes below, which does inline. That is the whole mechanism: payload width is not
//! involved, and neither is batching or column-major traversal, both of which measured flat.
//!
//! The prediction is never trusted. Each arm checks that the tag really belongs to the predicted
//! class and returns `None` otherwise, so [`Prediction::decode`] falls back to the general decoder
//! for that datum and re-learns the column. A wrong prediction costs throughput and never
//! correctness, which is what lets the prediction be learned from the previous row rather than
//! plumbed down from a `RelationDesc`. The compute plan carries no column types, so learning is
//! not merely convenient, it is the only option that does not change the protocol.

// `Tag` is `#[repr(u8)]` and its discriminants are needed as constants. `Tag::X as u8` is the only
// way to name them.
#![allow(clippy::as_conversions)]

use ordered_float::OrderedFloat;

use crate::row::{Tag, read_date};
use crate::{Datum, RowRef};

/// The predicted class of a column.
///
/// One arm of [`decode_one`]'s match each. The set is deliberately small: it covers the column
/// types common enough to matter and sends everything else through [`DatumClass::Other`], because
/// the win comes from the match staying small enough to inline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatumClass {
    /// Nothing has been observed for this column yet. Always misses, so the first datum decodes
    /// generally and teaches the column its class.
    #[default]
    Unknown,
    /// Confirmed to be a type no fast arm covers. Terminal: it decodes generally forever rather
    /// than re-classifying on every row.
    Other,
    /// `Int32`, variable-length encoded.
    Int32,
    /// `Int64`, variable-length encoded.
    Int64,
    /// `UInt64`, variable-length encoded.
    UInt64,
    /// `Float64`, eight fixed bytes.
    Float64,
    /// `Bool`, no body.
    Bool,
    /// `String`, a length prefix then the bytes.
    Str,
    /// `Bytes`, a length prefix then the bytes.
    Bytes,
    /// `Date`, four fixed bytes.
    Date,
}

const NULL: u8 = Tag::Null as u8;
const FALSE: u8 = Tag::False as u8;
const TRUE: u8 = Tag::True as u8;
const F64: u8 = Tag::Float64 as u8;
const DATE: u8 = Tag::Date as u8;
const STR_TINY: u8 = Tag::StringTiny as u8;
const STR_HUGE: u8 = Tag::StringHuge as u8;
const BYTES_TINY: u8 = Tag::BytesTiny as u8;
const BYTES_HUGE: u8 = Tag::BytesHuge as u8;
const I32_POS: u8 = Tag::NonNegativeInt32_0 as u8;
const I32_NEG: u8 = Tag::NegativeInt32_0 as u8;
const I64_POS: u8 = Tag::NonNegativeInt64_0 as u8;
const I64_NEG: u8 = Tag::NegativeInt64_0 as u8;
const U64: u8 = Tag::UInt64_0 as u8;

/// Zero- or sign-extend the `len` bytes at `data[at]` into an `N`-byte little-endian array.
#[inline(always)]
fn extend<const N: usize>(data: &[u8], at: usize, len: usize, fill: u8) -> [u8; N] {
    let mut raw = [fill; N];
    raw[..len].copy_from_slice(&data[at..at + len]);
    raw
}

/// The byte count of a variable-length integer whose family starts at `base`, or `None` if `tag`
/// is not in that family.
///
/// `push_datum` writes `base + n` for a value needing `n` bytes, so the count is arithmetic on the
/// tag. `N` is the width of the integer the family encodes, which is also the widest encoding the
/// family has, so `base + N` is its last tag. Accepting a wider `delta` would claim the first tag
/// of the next family and then read a body longer than the family's integer.
#[inline(always)]
fn varint_len<const N: usize>(tag: u8, base: u8) -> Option<usize> {
    let delta = usize::from(tag.wrapping_sub(base));
    (delta <= N).then_some(delta)
}

/// The `N` little-endian bytes of the variable-length integer at `at`, and the next offset.
///
/// Returns `None` if `tag` is not in the family starting at `base`. `fill` extends the encoded
/// bytes to `N`, so it is `255` for a family of negative values and `0` otherwise.
///
/// Taking the family width and the destination width as one `N` is what keeps them in agreement:
/// the caller's `from_le_bytes` fixes `N`, so a family whose width disagrees does not compile.
///
/// # Panics
///
/// Panics unless `data[at]` is the tag of a validly encoded datum, whose body is then in bounds.
#[inline(always)]
fn varint<const N: usize>(
    data: &[u8],
    at: usize,
    tag: u8,
    base: u8,
    fill: u8,
) -> Option<([u8; N], usize)> {
    let len = varint_len::<N>(tag, base)?;
    Some((extend(data, at + 1, len, fill), at + 1 + len))
}

/// The length prefix of a length-prefixed datum, and the width of the prefix itself.
///
/// The four tags of each length-prefixed family are consecutive and ordered by prefix width, so
/// the width is the tag's distance from the family's first tag.
#[inline(always)]
fn prefixed_len(data: &[u8], at: usize, tag: u8, first: u8) -> (usize, usize) {
    let body = at + 1;
    match tag - first {
        0 => (usize::from(data[body]), 1),
        1 => (
            usize::from(u16::from_le_bytes(
                data[body..body + 2].try_into().expect("2 bytes"),
            )),
            2,
        ),
        2 => (
            usize::try_from(u32::from_le_bytes(
                data[body..body + 4].try_into().expect("4 bytes"),
            ))
            .expect("fits"),
            4,
        ),
        _ => (
            usize::try_from(u64::from_le_bytes(
                data[body..body + 8].try_into().expect("8 bytes"),
            ))
            .expect("fits"),
            8,
        ),
    }
}

/// Decode the datum at `at` assuming it has class `class`, returning it and the next offset.
///
/// Returns `None` when the tag does not belong to `class`, which is the caller's signal to fall
/// back and re-learn. [`DatumClass::Unknown`] always returns `None`, so an unseen column learns on
/// its first datum. [`DatumClass::Other`] never does, so a column of an uncovered type settles
/// rather than paying a classification on every row.
///
/// # Safety
///
/// `data` must hold a validly encoded row and `at` must be the start of one of its datums.
#[inline(always)]
unsafe fn decode_one<'a>(
    data: &'a [u8],
    at: usize,
    class: DatumClass,
) -> Option<(Datum<'a>, usize)> {
    let tag = data[at];
    // Any column can be nullable, so a `Null` tag is compatible with every class. One compare,
    // and well predicted for a column that is mostly null or mostly not.
    if tag == NULL {
        return Some((Datum::Null, at + 1));
    }
    match class {
        DatumClass::Int64 => {
            let (raw, next) = varint(data, at, tag, I64_POS, 0)
                .or_else(|| varint(data, at, tag, I64_NEG, 255))?;
            Some((Datum::Int64(i64::from_le_bytes(raw)), next))
        }
        DatumClass::Int32 => {
            let (raw, next) = varint(data, at, tag, I32_POS, 0)
                .or_else(|| varint(data, at, tag, I32_NEG, 255))?;
            Some((Datum::Int32(i32::from_le_bytes(raw)), next))
        }
        DatumClass::UInt64 => {
            let (raw, next) = varint(data, at, tag, U64, 0)?;
            Some((Datum::UInt64(u64::from_le_bytes(raw)), next))
        }
        DatumClass::Float64 => {
            if tag != F64 {
                return None;
            }
            let bits = u64::from_le_bytes(data[at + 1..at + 9].try_into().expect("8 bytes"));
            Some((
                Datum::Float64(OrderedFloat::from(f64::from_bits(bits))),
                at + 9,
            ))
        }
        DatumClass::Bool => match tag {
            FALSE => Some((Datum::False, at + 1)),
            TRUE => Some((Datum::True, at + 1)),
            _ => None,
        },
        DatumClass::Str => {
            if !(STR_TINY..=STR_HUGE).contains(&tag) {
                return None;
            }
            let (len, width) = prefixed_len(data, at, tag, STR_TINY);
            let from = at + 1 + width;
            // SAFETY: the bytes were written from a `str` under a `String` tag.
            let s = unsafe { std::str::from_utf8_unchecked(&data[from..from + len]) };
            Some((Datum::String(s), from + len))
        }
        DatumClass::Bytes => {
            if !(BYTES_TINY..=BYTES_HUGE).contains(&tag) {
                return None;
            }
            let (len, width) = prefixed_len(data, at, tag, BYTES_TINY);
            let from = at + 1 + width;
            Some((Datum::Bytes(&data[from..from + len]), from + len))
        }
        DatumClass::Date => {
            if tag != DATE {
                return None;
            }
            let mut body = &data[at + 1..];
            Some((Datum::Date(read_date(&mut body)), at + 5))
        }
        // Neither has a fast arm, so both defer to the caller's single general decode. Giving
        // either one an arm here that calls the general decoder measured 24% SLOWER than not
        // covering it at all, because the arm adds a dispatch and a tag compare in front of the
        // same work.
        DatumClass::Other | DatumClass::Unknown => None,
    }
}

/// The class of the datum whose tag is `tag`, or `None` if the tag says nothing about the column.
///
/// A `Null` tag says nothing, so a column whose first observed value is null keeps whatever
/// prediction it had.
fn classify(tag: u8) -> Option<DatumClass> {
    let class = match tag {
        NULL => return None,
        FALSE | TRUE => DatumClass::Bool,
        F64 => DatumClass::Float64,
        DATE => DatumClass::Date,
        STR_TINY..=STR_HUGE => DatumClass::Str,
        BYTES_TINY..=BYTES_HUGE => DatumClass::Bytes,
        _ if varint_len::<8>(tag, I64_POS).is_some() || varint_len::<8>(tag, I64_NEG).is_some() => {
            DatumClass::Int64
        }
        _ if varint_len::<4>(tag, I32_POS).is_some() || varint_len::<4>(tag, I32_NEG).is_some() => {
            DatumClass::Int32
        }
        _ if varint_len::<8>(tag, U64).is_some() => DatumClass::UInt64,
        _ => DatumClass::Other,
    };
    Some(class)
}

/// A per-column class prediction, learned from the rows it has decoded.
///
/// Owned by a long-lived decoder such as `DatumVec`, so the classes are learned once and reused.
#[derive(Debug, Default, Clone)]
pub struct Prediction {
    classes: Vec<DatumClass>,
    /// Counts of what the predictions actually did, so a test can tell a hit from a miss.
    ///
    /// Every datum decodes correctly whether or not the prediction is right, so without these a
    /// completely inert fast path is indistinguishable from a working one.
    #[cfg(test)]
    stats: Stats,
    /// Whether every column has settled on [`DatumClass::Other`].
    ///
    /// Only then can predicting not pay for itself, and the bookkeeping it needs measured 9%
    /// slower than simply decoding generally, so such a relation uses the general decoder verbatim.
    ///
    /// The condition is deliberately "every column has settled on `Other`" rather than "no column
    /// has a fast arm". A null teaches nothing, so a column can sit at [`DatumClass::Unknown`]
    /// after a row; treating that as nothing to gain would stop the learning loop from ever
    /// running again, and a relation whose fast columns happen to be null in its first row would
    /// decode generally for the life of the decoder.
    all_other: bool,
}

/// What [`Prediction::decode`] did, for tests.
#[cfg(test)]
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Stats {
    /// Datums a fast arm decoded.
    hits: usize,
    /// Datums that fell back, whether because the column was unseen, the prediction was wrong, or
    /// the column has no fast arm.
    misses: usize,
    /// Rows that skipped prediction altogether.
    general_rows: usize,
}

impl Prediction {
    /// A prediction that has seen nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// Decode every datum of `row`, appending to `out`.
    ///
    /// Corrects the prediction as it goes, so `out` is always exactly what
    /// [`super::read_datum`] would have produced.
    pub fn decode<'a>(&mut self, row: &'a RowRef, out: &mut Vec<Datum<'a>>) {
        let data = row.data();
        if self.all_other {
            // No column has a fast arm. Decode generally, and notice a change of shape so a new
            // schema is still learned.
            let before = out.len();
            let mut rest = data;
            while !rest.is_empty() {
                // SAFETY: `rest` points at a datum of a validly encoded row.
                out.push(unsafe { super::read_datum(&mut rest) });
            }
            #[cfg(test)]
            {
                self.stats.general_rows += 1;
            }
            if out.len() - before != self.classes.len() {
                // A new shape, so learn it from scratch.
                self.classes.clear();
                self.all_other = false;
            }
            return;
        }

        let classes = &mut self.classes;
        let arity_before = classes.len();
        let mut learned = false;
        let mut at = 0;
        let mut idx = 0;
        while at < data.len() {
            if idx == classes.len() {
                classes.push(DatumClass::Unknown);
            }
            let class = classes[idx];
            // A settled `Other` has no fast arm, so route it straight to the general decoder
            // rather than through `decode_one`'s match.
            if class == DatumClass::Other {
                let mut rest = &data[at..];
                // SAFETY: `data` is a validly encoded row and `at` is a datum boundary.
                out.push(unsafe { super::read_datum(&mut rest) });
                at = data.len() - rest.len();
                idx += 1;
                #[cfg(test)]
                {
                    self.stats.misses += 1;
                }
                continue;
            }
            // SAFETY: `data` is a validly encoded row and `at` is a datum boundary, maintained by
            // every arm returning the offset just past the datum it decoded.
            match unsafe { decode_one(data, at, class) } {
                Some((datum, next)) => {
                    out.push(datum);
                    at = next;
                    #[cfg(test)]
                    {
                        self.stats.hits += 1;
                    }
                }
                None => {
                    // The column has not been seen, or the prediction was wrong. Both decode
                    // generally this once and both are worth learning from. A `Null` tag teaches
                    // nothing, so a column whose first value is null stays unknown and tries again
                    // on the next row.
                    if let Some(class) = classify(data[at]) {
                        classes[idx] = class;
                        learned = true;
                    }
                    let mut rest = &data[at..];
                    // SAFETY: as above.
                    out.push(unsafe { super::read_datum(&mut rest) });
                    at = data.len() - rest.len();
                    #[cfg(test)]
                    {
                        self.stats.misses += 1;
                    }
                }
            }
            idx += 1;
        }
        classes.truncate(idx);
        // `all_other` can only turn on when a class changed, either because a column was learned
        // or because truncation dropped the last column that had a fast arm. It cannot turn off
        // here, since a decoder for which it is already on never reaches this loop. So the scan is
        // needed only in those two cases, which keeps it off the settled path entirely: once every
        // column is classified nothing is learned and the arity is stable.
        if learned || idx < arity_before {
            self.all_other = !classes.is_empty() && classes.iter().all(|c| *c == DatumClass::Other);
        }
    }

    /// The classes learned so far, for tests.
    #[cfg(test)]
    fn classes(&self) -> &[DatumClass] {
        &self.classes
    }

    /// What the predictions did so far, for tests.
    #[cfg(test)]
    fn stats(&self) -> &Stats {
        &self.stats
    }

    /// Forget the counts, so a test can assert about the rows after a warm-up.
    #[cfg(test)]
    fn reset_stats(&mut self) {
        self.stats = Stats::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Row;

    /// The class [`classify`] must answer for `tag`, stated independently of it.
    ///
    /// Exhaustive on purpose. [`classify`] ends in a catch-all, so a `Tag` variant added to a
    /// family it does not know would silently become [`DatumClass::Other`] and decode generally
    /// forever. Here the same addition fails to compile, which forces the decision to be made.
    ///
    /// The fixed-width integer tags are [`DatumClass::Other`] because `push_datum` no longer
    /// writes them, so predicting them would spend a compare on a tag that never arrives.
    fn expected_class(tag: Tag) -> Option<DatumClass> {
        use Tag::*;
        let class = match tag {
            // A null says nothing about the column.
            Null => return None,

            False | True => DatumClass::Bool,
            Float64 => DatumClass::Float64,
            Date => DatumClass::Date,
            StringTiny | StringShort | StringLong | StringHuge => DatumClass::Str,
            BytesTiny | BytesShort | BytesLong | BytesHuge => DatumClass::Bytes,

            NonNegativeInt32_0 | NonNegativeInt32_8 | NonNegativeInt32_16 | NonNegativeInt32_24
            | NonNegativeInt32_32 | NegativeInt32_0 | NegativeInt32_8 | NegativeInt32_16
            | NegativeInt32_24 | NegativeInt32_32 => DatumClass::Int32,

            NonNegativeInt64_0 | NonNegativeInt64_8 | NonNegativeInt64_16 | NonNegativeInt64_24
            | NonNegativeInt64_32 | NonNegativeInt64_40 | NonNegativeInt64_48
            | NonNegativeInt64_56 | NonNegativeInt64_64 | NegativeInt64_0 | NegativeInt64_8
            | NegativeInt64_16 | NegativeInt64_24 | NegativeInt64_32 | NegativeInt64_40
            | NegativeInt64_48 | NegativeInt64_56 | NegativeInt64_64 => DatumClass::Int64,

            UInt64_0 | UInt64_8 | UInt64_16 | UInt64_24 | UInt64_32 | UInt64_40 | UInt64_48
            | UInt64_56 | UInt64_64 => DatumClass::UInt64,

            // No fast arm. Listed rather than caught, so a new variant lands here as an error.
            Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float32 | Time
            | Timestamp | TimestampTz | Interval | Uuid | Array | ListTiny | ListShort
            | ListLong | ListHuge | Dict | JsonNull | Dummy | Numeric | MzTimestamp | Range
            | MzAclItem | AclItem | CheapTimestamp | CheapTimestampTz => DatumClass::Other,

            NonNegativeInt16_0 | NonNegativeInt16_8 | NonNegativeInt16_16 | NegativeInt16_0
            | NegativeInt16_8 | NegativeInt16_16 | UInt8_0 | UInt8_8 | UInt16_0 | UInt16_8
            | UInt16_16 | UInt32_0 | UInt32_8 | UInt32_16 | UInt32_24 | UInt32_32 => {
                DatumClass::Other
            }
        };
        Some(class)
    }

    /// Every tag the encoder can write must get the class `expected_class` names.
    ///
    /// `Tag` is `#[repr(u8)]` with `TryFromPrimitive`, so scanning the byte range enumerates the
    /// whole enum without a generator having to produce a datum of each type.
    #[mz_ore::test]
    fn classify_matches_every_tag() {
        let mut seen = 0;
        for byte in 0..=u8::MAX {
            let Ok(tag) = Tag::try_from(byte) else {
                continue;
            };
            seen += 1;
            assert_eq!(classify(byte), expected_class(tag), "tag {tag:?} ({byte})");
        }
        // The scan found the enum, not an empty range.
        assert!(seen > 90, "only {seen} tags enumerated");
    }

    /// The lengths `varint_len` computes by arithmetic must equal the ones `Tag` computes by name.
    ///
    /// These are the two independent statements of the same encoding: `predict` derives the body
    /// length from the tag's distance from its family's first tag, `Tag::actual_int_length`
    /// spells it out per variant. A family width that reached one tag too far would show up here
    /// as a length disagreement, or as two families claiming one tag.
    #[mz_ore::test]
    fn varint_len_agrees_with_tag() {
        for byte in 0..=u8::MAX {
            let Ok(tag) = Tag::try_from(byte) else {
                continue;
            };
            let claims = [
                varint_len::<8>(byte, I64_POS),
                varint_len::<8>(byte, I64_NEG),
                varint_len::<4>(byte, I32_POS),
                varint_len::<4>(byte, I32_NEG),
                varint_len::<8>(byte, U64),
            ];
            assert!(
                claims.iter().filter(|len| len.is_some()).count() <= 1,
                "tag {tag:?} claimed by more than one family"
            );
            if let Some(len) = claims.into_iter().flatten().next() {
                assert_eq!(
                    Some(len),
                    tag.actual_int_length(),
                    "tag {tag:?} body length"
                );
            }
        }
    }

    /// The predicting decoder must match `read_datum` on any row, from any starting prediction.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn matches_general_decoder() {
        use proptest::prelude::*;

        let strat = proptest::collection::vec(
            proptest::collection::vec(crate::scalar::arb_datum(true), 1..6),
            1..8,
        );
        proptest!(|(rows in strat)| {
            let packed: Vec<Row> = rows
                .iter()
                .map(|datums| Row::pack(datums.iter().map(Datum::from)))
                .collect();

            // Learning across a sequence of rows, which is how a `DatumVec` sees them.
            let mut prediction = Prediction::new();
            for row in &packed {
                let expected: Vec<_> = row.iter().collect();
                let mut got = Vec::new();
                prediction.decode(row, &mut got);
                prop_assert_eq!(&got, &expected);
            }

            // And from every fixed prediction, including deliberately wrong ones.
            for wrong in [
                DatumClass::Unknown,
                DatumClass::Other,
                DatumClass::Int64,
                DatumClass::Int32,
                DatumClass::UInt64,
                DatumClass::Float64,
                DatumClass::Bool,
                DatumClass::Str,
                DatumClass::Bytes,
                DatumClass::Date,
            ] {
                for row in &packed {
                    let expected: Vec<_> = row.iter().collect();
                    let mut prediction = Prediction {
                        classes: vec![wrong; expected.len()],
                        all_other: false,
                        stats: Stats::default(),
                    };
                    let mut got = Vec::new();
                    prediction.decode(row, &mut got);
                    prop_assert_eq!(&got, &expected, "prediction {:?}", wrong);
                }
            }
        });
    }

    /// The prediction must settle after one row and stay settled.
    #[mz_ore::test]
    fn prediction_settles() {
        let rows: Vec<Row> = (0..4)
            .map(|r| {
                let s = format!("v{r}");
                Row::pack([
                    Datum::Int64(i64::from(r)),
                    Datum::String(&s),
                    Datum::Float64(f64::from(r).into()),
                    Datum::True,
                ])
            })
            .collect();

        let mut prediction = Prediction::new();
        let mut out = Vec::new();
        prediction.decode(&rows[0], &mut out);
        assert_eq!(
            prediction.classes(),
            [
                DatumClass::Int64,
                DatumClass::Str,
                DatumClass::Float64,
                DatumClass::Bool
            ]
        );

        // Subsequent rows must not disturb it, and must actually take the fast path. Asserting
        // only on the classes would hold even if every arm missed, since a miss re-learns the same
        // class.
        prediction.reset_stats();
        for row in &rows[1..] {
            let before = prediction.classes().to_vec();
            out.clear();
            prediction.decode(row, &mut out);
            assert_eq!(prediction.classes(), before);
        }
        let stats = prediction.stats();
        assert_eq!(stats.misses, 0, "{stats:?}");
        assert_eq!(stats.general_rows, 0, "{stats:?}");
        assert_eq!(stats.hits, 4 * (rows.len() - 1), "{stats:?}");
    }

    /// A column whose first value is null must not be mispredicted by the null.
    #[mz_ore::test]
    fn null_teaches_nothing() {
        let first = Row::pack([Datum::Null, Datum::Int64(1)]);
        let second = Row::pack([Datum::String("x"), Datum::Int64(2)]);

        let mut prediction = Prediction::new();
        let mut out = Vec::new();
        prediction.decode(&first, &mut out);
        assert_eq!(out, [Datum::Null, Datum::Int64(1)]);

        out.clear();
        prediction.decode(&second, &mut out);
        assert_eq!(out, [Datum::String("x"), Datum::Int64(2)]);
        assert_eq!(prediction.classes()[0], DatumClass::Str);
    }

    /// A first row that classifies no column must not disable the prediction for good.
    ///
    /// A null teaches nothing, so a relation whose only fast-armed columns are null in its first
    /// row learns nothing from it. The prediction has to keep trying on later rows rather than
    /// settling on the general decoder.
    #[mz_ore::test]
    fn first_row_teaching_nothing_still_learns() {
        // Arity one, so the first row classifies nothing at all.
        let mut rows = vec![Row::pack([Datum::Null])];
        rows.extend((0..4).map(|r| Row::pack([Datum::Int64(i64::from(r))])));

        let mut prediction = Prediction::new();
        let mut out = Vec::new();
        for row in &rows[..2] {
            out.clear();
            prediction.decode(row, &mut out);
        }
        assert_eq!(prediction.classes(), [DatumClass::Int64]);

        prediction.reset_stats();
        for row in &rows[2..] {
            out.clear();
            prediction.decode(row, &mut out);
        }
        let stats = prediction.stats();
        assert_eq!(stats.misses, 0, "{stats:?}");
        assert_eq!(stats.general_rows, 0, "{stats:?}");
        assert_eq!(stats.hits, rows.len() - 2, "{stats:?}");
    }

    /// The same, where one column has no fast arm and the other is null in the first row.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn uncovered_column_does_not_mask_a_learnable_one() {
        let numeric = crate::adt::numeric::Numeric::from(3);
        let mut rows = vec![Row::pack([Datum::from(numeric), Datum::Null])];
        rows.extend((0..4).map(|_| Row::pack([Datum::from(numeric), Datum::Int64(5)])));

        let mut prediction = Prediction::new();
        let mut out = Vec::new();
        for row in &rows[..2] {
            out.clear();
            prediction.decode(row, &mut out);
        }
        assert_eq!(prediction.classes(), [DatumClass::Other, DatumClass::Int64]);

        prediction.reset_stats();
        for row in &rows[2..] {
            out.clear();
            prediction.decode(row, &mut out);
        }
        let stats = prediction.stats();
        assert_eq!(stats.general_rows, 0, "{stats:?}");
        // One hit for the `Int64` column and one miss for the uncovered one, per row.
        assert_eq!(stats.hits, rows.len() - 2, "{stats:?}");
        assert_eq!(stats.misses, rows.len() - 2, "{stats:?}");
    }

    /// A column that changes arity between rows must self-correct.
    #[mz_ore::test]
    fn arity_change_settles() {
        let wide = Row::pack([Datum::Int64(1), Datum::Int64(2), Datum::Int64(3)]);
        let narrow = Row::pack([Datum::String("a")]);

        let mut prediction = Prediction::new();
        let mut out = Vec::new();
        prediction.decode(&wide, &mut out);
        assert_eq!(prediction.classes().len(), 3);

        out.clear();
        prediction.decode(&narrow, &mut out);
        assert_eq!(out, [Datum::String("a")]);
        assert_eq!(prediction.classes().len(), 1);
    }
}
