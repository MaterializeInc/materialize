// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::type_name;
use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{self, Debug, Display};

use bitflags::bitflags;

use crate::row::DatumNested;
use crate::Datum;

bitflags! {
    pub(crate) struct Flags: u8 {
        const EMPTY = 1;
        const LB_INCLUSIVE = 1 << 1;
        const LB_INFINITE = 1 << 2;
        const UB_INCLUSIVE = 1 << 3;
        const UB_INFINITE = 1 << 4;
    }
}

/// A continuous set of domain values.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Range<'a> {
    /// None value represents empty range
    pub inner: Option<RangeInner<'a>>,
}

impl<'a> Display for Range<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.inner {
            None => f.write_str("empty"),
            Some(i) => Display::fmt(&i, f),
        }
    }
}

/// Trait alias for traits required for generic range function implementations.
pub trait RangeOps<'a>: Debug + Ord + PartialOrd + Eq + PartialEq + TryFrom<Datum<'a>> {}
impl<'a, T: Debug + Ord + PartialOrd + Eq + PartialEq + TryFrom<Datum<'a>>> RangeOps<'a> for T {}

impl<'a> Range<'a> {
    #[allow(dead_code)]
    fn contains<T: RangeOps<'a>>(&self, elem: &T) -> bool {
        match self.inner {
            None => false,
            Some(inner) => inner.lower.satisfied_by(elem) && inner.upper.satisfied_by(elem),
        }
    }
}

/// Holds the upper and lower `DatumRangeBound`s for non-empty ranges.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RangeInner<'a> {
    pub lower: RangeLowerBound<'a>,
    pub upper: RangeUpperBound<'a>,
}

impl<'a> Display for RangeInner<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.lower.inclusive { "[" } else { "(" })?;
        Display::fmt(&self.lower, f)?;
        f.write_str(",")?;
        Display::fmt(&self.upper, f)?;
        f.write_str(if self.upper.inclusive { "]" } else { ")" })
    }
}

impl<'a> Ord for RangeInner<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.lower
            .cmp(&other.lower)
            .then(self.upper.cmp(&other.upper))
    }
}

impl<'a> PartialOrd for RangeInner<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Represents a terminal point of a range.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RangeBound<'a, const UPPER: bool = false> {
    pub inclusive: bool,
    /// None value represents an infinite bound.
    pub bound: Option<DatumNested<'a>>,
}

impl<'a, const UPPER: bool> Display for RangeBound<'a, UPPER> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.bound {
            None => Ok(()),
            Some(bound) => Display::fmt(&bound.datum(), f),
        }
    }
}

impl<'a, const UPPER: bool> Ord for RangeBound<'a, UPPER> {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = match self.bound.cmp(&other.bound) {
            Ordering::Equal => {
                if self.inclusive == other.inclusive {
                    Ordering::Equal
                } else if self.inclusive {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            o => o,
        };
        if UPPER {
            ordering.reverse()
        } else {
            ordering
        }
    }
}

impl<'a, const UPPER: bool> PartialOrd for RangeBound<'a, UPPER> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A `RangeBound` that sorts correctly for use as a lower bound.
pub type RangeLowerBound<'a> = RangeBound<'a, false>;

/// A `RangeBound` that sorts correctly for use as an upper bound.
pub type RangeUpperBound<'a> = RangeBound<'a, true>;

impl<'a, const UPPER: bool> RangeBound<'a, UPPER> {
    /// Determines where `elem` lies in relation to the range bound.
    ///
    /// # Panics
    /// - If `self.bound.datum()` is not convertible to `T`.
    fn elem_cmp<T: RangeOps<'a>>(&self, elem: &T) -> Ordering {
        match self.bound.map(|bound| {
            <T>::try_from(bound.datum())
                .unwrap_or_else(|_| panic!("cannot take {} to {}", bound.datum(), type_name::<T>()))
        }) {
            None if UPPER => Ordering::Greater,
            None => Ordering::Less,
            Some(bound) => bound.cmp(elem),
        }
    }

    /// Does `elem` satisfy this bound?
    fn satisfied_by<T: RangeOps<'a>>(&self, elem: &T) -> bool {
        match self.elem_cmp(elem) {
            // Inclusive always satisfied with equality, regardless of upper or
            // lower.
            Ordering::Equal => self.inclusive,
            // Upper satisfied with values less than itself
            Ordering::Greater => UPPER,
            // Lower satisfied with values greater than itself
            Ordering::Less => !UPPER,
        }
    }
}

/// Describes the value passed in via `RangeBoundDesc`s.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd)]
pub enum RangeBoundDescValue<D> {
    Finite { value: D },
    Infinite,
}

impl<'a> From<Datum<'a>> for RangeBoundDescValue<Datum<'a>> {
    /// Treats `Datum::Null` as an infinite bound (appropriate for PG
    /// semantics), and all other `Datum`s as finite values.
    fn from(d: Datum<'a>) -> Self {
        match d {
            Datum::Null => RangeBoundDescValue::Infinite,
            value => RangeBoundDescValue::Finite { value },
        }
    }
}

/// Structures arguments for functions that construct ranges.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd)]
pub struct RangeBoundDesc<D> {
    pub inclusive: bool,
    pub value: RangeBoundDescValue<D>,
}

impl<'a> RangeBoundDesc<Datum<'a>> {
    /// Create a new `RangeBoundDesc` whose value is infinite if `d ==
    /// Datum::Null`, otherwise finite.
    pub fn new(d: Datum<'a>, inclusive: bool) -> RangeBoundDesc<Datum<'a>> {
        RangeBoundDesc {
            inclusive,
            value: d.into(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum RangeError {
    MisorderedRangeBounds,
}

impl Display for RangeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RangeError::MisorderedRangeBounds => {
                f.write_str("range lower bound must be less than or equal to range upper bound")
            }
        }
    }
}

impl Error for RangeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

// Required due to Proto decoding using string as its error type
impl From<RangeError> for String {
    fn from(e: RangeError) -> Self {
        e.to_string()
    }
}

#[cfg(test)]
mod tests {

    use super::Range;
    use crate::adt::range::RangeBoundDesc;
    use crate::{Datum, RowArena};

    // TODO: Once SQL supports this, the test can be moved into SLT.
    #[test]
    fn test_range_contains() {
        fn test_range_contains_inner(
            range: Range,
            contains: Option<i32>,
            does_not_contain: Option<i32>,
        ) {
            if let Some(el) = contains {
                assert!(range.contains(&el), "el {:?}, range {}", el, range);
            }

            if let Some(el) = does_not_contain {
                assert!(!range.contains(&el), "el {:?}, range {:?}", el, range);
            }
        }

        let arena = RowArena::new();
        for (lower, lower_inclusive, upper, upper_inclusive, contains, does_not_contain) in [
            (
                Datum::Null,
                true,
                Datum::Int32(1),
                false,
                Some(i32::MIN),
                Some(1),
            ),
            (Datum::Null, true, Datum::Int32(1), true, Some(1), Some(2)),
            (Datum::Null, true, Datum::Null, false, Some(i32::MAX), None),
            (Datum::Null, true, Datum::Null, true, Some(i32::MAX), None),
            (
                Datum::Null,
                false,
                Datum::Int32(1),
                false,
                Some(i32::MIN),
                Some(1),
            ),
            (Datum::Null, false, Datum::Int32(1), true, Some(1), Some(2)),
            (Datum::Null, false, Datum::Null, false, Some(i32::MAX), None),
            (Datum::Null, false, Datum::Null, true, Some(i32::MAX), None),
            (
                Datum::Int32(-1),
                true,
                Datum::Int32(1),
                false,
                Some(-1),
                Some(1),
            ),
            (
                Datum::Int32(-1),
                true,
                Datum::Int32(1),
                true,
                Some(1),
                Some(-2),
            ),
            (
                Datum::Int32(-1),
                false,
                Datum::Int32(1),
                false,
                Some(0),
                Some(-1),
            ),
            (
                Datum::Int32(-1),
                false,
                Datum::Int32(1),
                true,
                Some(1),
                Some(-1),
            ),
            (Datum::Int32(1), true, Datum::Null, false, Some(1), Some(-1)),
            (
                Datum::Int32(1),
                true,
                Datum::Null,
                true,
                Some(i32::MAX),
                Some(-1),
            ),
            (Datum::Int32(1), false, Datum::Null, false, Some(2), Some(1)),
            (
                Datum::Int32(1),
                false,
                Datum::Null,
                true,
                Some(i32::MAX),
                Some(1),
            ),
        ] {
            let range = arena.make_datum(|packer| {
                packer
                    .push_range(
                        RangeBoundDesc::new(lower, lower_inclusive),
                        RangeBoundDesc::new(upper, upper_inclusive),
                    )
                    .unwrap();
            });

            let range = match range {
                Datum::Range(inner) => inner,
                _ => unreachable!(),
            };

            test_range_contains_inner(range, contains, does_not_contain);
        }

        let range = arena.make_datum(|packer| {
            packer.push_empty_range();
        });

        let range = match range {
            Datum::Range(inner) => inner,
            _ => unreachable!(),
        };

        for el in i16::MIN..i16::MAX {
            test_range_contains_inner(range, None, Some(el.into()));
        }
    }
}
