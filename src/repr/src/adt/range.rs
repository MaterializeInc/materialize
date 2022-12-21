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
use std::hash::{Hash, Hasher};

use bitflags::bitflags;
use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use num_traits::CheckedAdd;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::scalar::DatumKind;
use crate::Datum;

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.range.rs"));

bitflags! {
    pub(crate) struct Flags: u8 {
        const EMPTY = 1;
        const LB_INCLUSIVE = 1 << 1;
        const LB_INFINITE = 1 << 2;
        const UB_INCLUSIVE = 1 << 3;
        const UB_INFINITE = 1 << 4;
    }
}

/// A range of values along the domain `D`.
///
/// `D` is generic to facilitate interoperating over multiple representation,
/// e.g. `Datum` and `mz_pgrepr::Value`. Because of the latter, we have to
/// "manually derive" traits over `Range`.
///
/// Also notable, is that `Datum`s themselves store ranges as
/// `Range<DatumNested<'a>>`, which lets us avoid unnecessary boxing of the
/// range's finite bounds, which are most often expressed as `Datum`.
pub struct Range<D> {
    /// None value represents empty range
    pub inner: Option<RangeInner<D>>,
}

impl<D: Display> Display for Range<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            None => f.write_str("empty"),
            Some(i) => i.fmt(f),
        }
    }
}

impl<D: Debug> Debug for Range<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Range").field("inner", &self.inner).finish()
    }
}

impl<D: Clone> Clone for Range<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<D: Copy> Copy for Range<D> {}

impl<D: PartialEq> PartialEq for Range<D> {
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

impl<D: Eq> Eq for Range<D> {}

impl<D: Ord + PartialOrd> PartialOrd for Range<D> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<D: Ord> Ord for Range<D> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<D: Hash> Hash for Range<D> {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.inner.hash(hasher)
    }
}

/// Trait alias for traits required for generic range function implementations.
pub trait RangeOps<'a>:
    Debug + Ord + PartialOrd + Eq + PartialEq + CheckedAdd + TryFrom<Datum<'a>> + Into<Datum<'a>>
where
    <Self as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
{
    fn step() -> Option<Self>;

    fn unwrap_datum(d: Datum<'a>) -> Self {
        <Self>::try_from(d)
            .unwrap_or_else(|_| panic!("cannot take {} to {}", d, type_name::<Self>()))
    }

    fn err_type_name() -> &'static str;
}

impl<'a> RangeOps<'a> for i32 {
    fn step() -> Option<i32> {
        Some(1)
    }

    fn err_type_name() -> &'static str {
        "integer"
    }
}

impl<'a> RangeOps<'a> for i64 {
    fn step() -> Option<i64> {
        Some(1)
    }

    fn err_type_name() -> &'static str {
        "bigint"
    }
}

// Totally generic range implementations.
impl<D> Range<D> {
    pub fn new(inner: Option<(RangeLowerBound<D>, RangeUpperBound<D>)>) -> Range<D> {
        Range {
            inner: inner.map(|(lower, upper)| RangeInner { lower, upper }),
        }
    }

    /// Converts `self` from having bounds of type `D` to type `O`, converting
    /// the current bounds using `conv`.
    // clippy's suggestion for avoiding redundant closures on `conv` moves `F`,
    // which does not (and in most cases cannot) implement Copy.
    #[allow(clippy::redundant_closure)]
    pub fn into_bounds<F, O>(self, conv: F) -> Range<O>
    where
        F: Fn(D) -> O,
    {
        Range {
            inner: self
                .inner
                .map(|RangeInner::<D> { lower, upper }| RangeInner::<O> {
                    lower: RangeLowerBound {
                        inclusive: lower.inclusive,
                        bound: lower.bound.map(|d| conv(d)),
                    },
                    upper: RangeUpperBound {
                        inclusive: upper.inclusive,
                        bound: upper.bound.map(|d| conv(d)),
                    },
                }),
        }
    }
}

// Range implementations meant to work with `Range<Datum>` and
// `Range<DatumNested>`.
impl<'a, B: Copy> Range<B>
where
    Datum<'a>: From<B>,
{
    pub fn contains<T: RangeOps<'a>>(&self, elem: &T) -> bool
    where
        <T as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    {
        match self.inner {
            None => false,
            Some(inner) => inner.lower.satisfied_by(elem) && inner.upper.satisfied_by(elem),
        }
    }
}

impl<'a> Range<Datum<'a>> {
    /// Canonicalize the range by PG's heuristics, which are:
    /// - Infinite bounds are always exclusive
    /// - If type has step:
    ///  - Exclusive lower bounds are rewritten as inclusive += step
    ///  - Inclusive lower bounds are rewritten as exclusive += step
    /// - Ranges are empty if lower >= upper after prev. step unless range type
    ///   does not have step and both bounds are inclusive
    ///
    /// # Panics
    /// - If the upper and lower bounds are finite and of different types.
    pub fn canonicalize(&mut self) -> Result<(), InvalidRangeError> {
        let (lower, upper) = match &mut self.inner {
            Some(inner) => (&mut inner.lower, &mut inner.upper),
            None => return Ok(()),
        };

        match (lower.bound, upper.bound) {
            (Some(l), Some(u)) => {
                assert_eq!(
                    DatumKind::from(l),
                    DatumKind::from(u),
                    "finite bounds must be of same type"
                );
                if l > u {
                    return Err(InvalidRangeError::MisorderedRangeBounds);
                }
            }
            _ => {}
        };

        lower.canonicalize()?;
        upper.canonicalize()?;

        // The only way that you have two inclusive bounds with equal value are
        // if type does not have step.
        if !(lower.inclusive && upper.inclusive)
            && lower.bound >= upper.bound
            // None is less than any Some, so only need to check this condition.
            && upper.bound.is_some()
        {
            // emtpy range
            self.inner = None
        }

        Ok(())
    }
}

/// Holds the upper and lower bounds for non-empty ranges.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RangeInner<B> {
    pub lower: RangeLowerBound<B>,
    pub upper: RangeUpperBound<B>,
}

impl<B: Display> Display for RangeInner<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.lower.inclusive { "[" } else { "(" })?;
        self.lower.fmt(f)?;
        f.write_str(",")?;
        Display::fmt(&self.upper, f)?;
        f.write_str(if self.upper.inclusive { "]" } else { ")" })
    }
}

impl<B: Ord> Ord for RangeInner<B> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.lower
            .cmp(&other.lower)
            .then(self.upper.cmp(&other.upper))
    }
}

impl<B: PartialOrd + Ord> PartialOrd for RangeInner<B> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Represents a terminal point of a range.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct RangeBound<B, const UPPER: bool = false> {
    pub inclusive: bool,
    /// None value represents an infinite bound.
    pub bound: Option<B>,
}

impl<const UPPER: bool, D: Display> Display for RangeBound<D, UPPER> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.bound {
            None => Ok(()),
            Some(bound) => bound.fmt(f),
        }
    }
}

impl<const UPPER: bool, D: Ord> Ord for RangeBound<D, UPPER> {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. Sort by bounds
        let mut cmp = self.bound.cmp(&other.bound);
        // 2. Infinite bounds vs. finite bounds are reversed for uppers.
        if UPPER && other.bound.is_none() ^ self.bound.is_none() {
            cmp = cmp.reverse();
        }
        // 3. Tie break by sorting by inclusivity, which is inverted between
        //    lowers and uppers.
        cmp.then(if self.inclusive == other.inclusive {
            Ordering::Equal
        } else if self.inclusive {
            if UPPER {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        } else if UPPER {
            Ordering::Less
        } else {
            Ordering::Greater
        })
    }
}

impl<const UPPER: bool, D: PartialOrd + Ord> PartialOrd for RangeBound<D, UPPER> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A `RangeBound` that sorts correctly for use as a lower bound.
pub type RangeLowerBound<B> = RangeBound<B, false>;

/// A `RangeBound` that sorts correctly for use as an upper bound.
pub type RangeUpperBound<B> = RangeBound<B, true>;

// Generic RangeBound implementations meant to work over `RangeBound<Datum,..>`
// and `RangeBound<DatumNested,..>`.
impl<'a, const UPPER: bool, B: Copy> RangeBound<B, UPPER>
where
    Datum<'a>: From<B>,
{
    /// Determines where `elem` lies in relation to the range bound.
    ///
    /// # Panics
    /// - If `self.bound.datum()` is not convertible to `T`.
    fn elem_cmp<T: RangeOps<'a>>(&self, elem: &T) -> Ordering
    where
        <T as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    {
        match self.bound.map(|bound| <T>::unwrap_datum(bound.into())) {
            None if UPPER => Ordering::Greater,
            None => Ordering::Less,
            Some(bound) => bound.cmp(elem),
        }
    }

    /// Does `elem` satisfy this bound?
    fn satisfied_by<T: RangeOps<'a>>(&self, elem: &T) -> bool
    where
        <T as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    {
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

impl<'a, const UPPER: bool> RangeBound<Datum<'a>, UPPER> {
    /// Create a new `RangeBound` whose value is "infinite" (i.e. None) if `d ==
    /// Datum::Null`, otherwise finite (i.e. Some).
    ///
    /// There is not a corresponding generic implementation of this because
    /// genericizing how to express infinite bounds is less clear.
    pub fn new(d: Datum<'a>, inclusive: bool) -> RangeBound<Datum<'a>, UPPER> {
        RangeBound {
            inclusive,
            bound: match d {
                Datum::Null => None,
                o => Some(o),
            },
        }
    }

    fn canonicalize(&mut self) -> Result<(), InvalidRangeError> {
        Ok(match self.bound {
            None => {
                self.inclusive = false;
            }
            Some(value) => match value {
                d @ Datum::Int32(_) => self.canonicalize_inner::<i32>(d)?,
                d @ Datum::Int64(_) => self.canonicalize_inner::<i64>(d)?,
                d => unreachable!("{d:?} not yet supported in ranges"),
            },
        })
    }

    fn canonicalize_inner<T: RangeOps<'a>>(&mut self, d: Datum<'a>) -> Result<(), InvalidRangeError>
    where
        <T as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    {
        if let Some(step) = T::step() {
            // Upper bounds must be exclusive, lower bounds inclusive
            if UPPER == self.inclusive {
                let cur = <T>::unwrap_datum(d);
                self.bound = Some(
                    cur.checked_add(&step)
                        .ok_or_else(|| {
                            InvalidRangeError::CanonicalizationOverflow(
                                T::err_type_name().to_string(),
                            )
                        })?
                        .into(),
                );
                self.inclusive = !UPPER;
            }
        }

        Ok(())
    }
}

#[derive(
    Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum InvalidRangeError {
    MisorderedRangeBounds,
    CanonicalizationOverflow(String),
}

impl Display for InvalidRangeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InvalidRangeError::MisorderedRangeBounds => {
                f.write_str("range lower bound must be less than or equal to range upper bound")
            }
            InvalidRangeError::CanonicalizationOverflow(t) => {
                write!(f, "{} out of range", t)
            }
        }
    }
}

impl Error for InvalidRangeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}

// Required due to Proto decoding using string as its error type
impl From<InvalidRangeError> for String {
    fn from(e: InvalidRangeError) -> Self {
        e.to_string()
    }
}

impl RustType<ProtoInvalidRangeError> for InvalidRangeError {
    fn into_proto(&self) -> ProtoInvalidRangeError {
        use proto_invalid_range_error::*;
        use Kind::*;
        let kind = match self {
            InvalidRangeError::MisorderedRangeBounds => MisorderedRangeBounds(()),
            InvalidRangeError::CanonicalizationOverflow(s) => CanonicalizationOverflow(s.clone()),
        };
        ProtoInvalidRangeError { kind: Some(kind) }
    }

    fn from_proto(proto: ProtoInvalidRangeError) -> Result<Self, TryFromProtoError> {
        use proto_invalid_range_error::Kind::*;
        match proto.kind {
            Some(kind) => match kind {
                MisorderedRangeBounds(()) => Ok(InvalidRangeError::MisorderedRangeBounds),
                CanonicalizationOverflow(s) => Ok(InvalidRangeError::CanonicalizationOverflow(s)),
            },
            None => Err(TryFromProtoError::missing_field(
                "`ProtoInvalidRangeError::kind`",
            )),
        }
    }
}
