// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
