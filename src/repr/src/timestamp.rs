// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::time::Duration;

use dec::TryFromDecimalError;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize, Serializer};

use crate::adt::numeric::Numeric;

/// System-wide timestamp type.
#[derive(
    Clone,
    // TODO: De-implement Copy, which is widely used.
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Arbitrary,
)]
#[repr(transparent)]
pub struct Timestamp {
    /// note no `pub`.
    internal: u64,
}

pub trait TimestampManipulation:
    timely::progress::Timestamp
    + timely::order::TotalOrder
    + differential_dataflow::lattice::Lattice
    + std::fmt::Debug
{
    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    fn step_forward(&self) -> Self;

    /// Advance a timestamp forward by the given `amount`. Panic if unable to do so.
    fn step_forward_by(&self, amount: &Self) -> Self;

    /// Retreat a timestamp by the least amount possible such that
    /// `ts.step_back().unwrap().less_than(ts)` is true. Return `None` if unable,
    /// which must only happen if the timestamp is `Timestamp::minimum()`.
    fn step_back(&self) -> Option<Self>;

    /// Return the maximum value for this timestamp.
    fn maximum() -> Self;
}

impl TimestampManipulation for Timestamp {
    fn step_forward(&self) -> Self {
        self.step_forward()
    }

    fn step_forward_by(&self, amount: &Self) -> Self {
        self.step_forward_by(amount)
    }

    fn step_back(&self) -> Option<Self> {
        self.step_back()
    }

    fn maximum() -> Self {
        Self::MAX
    }
}

impl Timestamp {
    pub const MAX: Self = Self { internal: u64::MAX };
    pub const MIN: Self = Self { internal: u64::MIN };

    pub const fn new(timestamp: u64) -> Self {
        Self {
            internal: timestamp,
        }
    }

    pub fn to_bytes(&self) -> [u8; 8] {
        self.internal.to_le_bytes()
    }

    pub fn from_bytes(bytes: [u8; 8]) -> Self {
        Self {
            internal: u64::from_le_bytes(bytes),
        }
    }

    pub fn saturating_sub<I: Into<Self>>(self, rhs: I) -> Self {
        Self {
            internal: self.internal.saturating_sub(rhs.into().internal),
        }
    }

    pub fn saturating_add<I: Into<Self>>(self, rhs: I) -> Self {
        Self {
            internal: self.internal.saturating_add(rhs.into().internal),
        }
    }

    pub fn saturating_mul<I: Into<Self>>(self, rhs: I) -> Self {
        Self {
            internal: self.internal.saturating_mul(rhs.into().internal),
        }
    }

    pub fn checked_add<I: Into<Self>>(self, rhs: I) -> Option<Self> {
        self.internal
            .checked_add(rhs.into().internal)
            .map(|internal| Self { internal })
    }

    pub fn checked_sub<I: Into<Self>>(self, rhs: I) -> Option<Self> {
        self.internal
            .checked_sub(rhs.into().internal)
            .map(|internal| Self { internal })
    }

    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    pub fn step_forward(&self) -> Self {
        match self.checked_add(1) {
            Some(ts) => ts,
            None => panic!("could not step forward"),
        }
    }

    /// Advance a timestamp forward by the given `amount`. Panic if unable to do so.
    pub fn step_forward_by(&self, amount: &Self) -> Self {
        match self.checked_add(*amount) {
            Some(ts) => ts,
            None => panic!("could not step {self} forward by {amount}"),
        }
    }

    /// Retreat a timestamp by the least amount possible such that
    /// `ts.step_back().unwrap().less_than(ts)` is true. Return `None` if unable,
    /// which must only happen if the timestamp is `Timestamp::minimum()`.
    pub fn step_back(&self) -> Option<Self> {
        self.checked_sub(1)
    }
}

impl From<u64> for Timestamp {
    fn from(internal: u64) -> Self {
        Self { internal }
    }
}

impl From<Timestamp> for u64 {
    fn from(ts: Timestamp) -> Self {
        ts.internal
    }
}

impl From<Timestamp> for u128 {
    fn from(ts: Timestamp) -> Self {
        u128::from(ts.internal)
    }
}

impl TryFrom<Timestamp> for i64 {
    type Error = TryFromIntError;

    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        value.internal.try_into()
    }
}

impl From<&Timestamp> for u64 {
    fn from(ts: &Timestamp) -> Self {
        ts.internal
    }
}

impl From<Timestamp> for Numeric {
    fn from(ts: Timestamp) -> Self {
        ts.internal.into()
    }
}

impl std::ops::Rem<Timestamp> for Timestamp {
    type Output = Timestamp;

    fn rem(self, rhs: Timestamp) -> Self::Output {
        Self {
            internal: self.internal % rhs.internal,
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.internal.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(Self {
            internal: u64::deserialize(deserializer)?,
        })
    }
}

impl timely::order::PartialOrder for Timestamp {
    fn less_equal(&self, other: &Self) -> bool {
        self.internal.less_equal(&other.internal)
    }
}

impl timely::order::TotalOrder for Timestamp {}

impl timely::progress::Timestamp for Timestamp {
    type Summary = Timestamp;

    fn minimum() -> Self {
        Self::MIN
    }
}

impl timely::progress::PathSummary<Timestamp> for Timestamp {
    #[inline]
    fn results_in(&self, src: &Timestamp) -> Option<Timestamp> {
        self.internal
            .checked_add(src.internal)
            .map(|internal| Self { internal })
    }
    #[inline]
    fn followed_by(&self, other: &Timestamp) -> Option<Timestamp> {
        self.internal
            .checked_add(other.internal)
            .map(|internal| Self { internal })
    }
}

impl timely::progress::timestamp::Refines<()> for Timestamp {
    fn to_inner(_: ()) -> Timestamp {
        Default::default()
    }
    fn to_outer(self) -> () {
        ()
    }
    fn summarize(_: <Timestamp as timely::progress::timestamp::Timestamp>::Summary) -> () {
        ()
    }
}

impl differential_dataflow::lattice::Lattice for Timestamp {
    #[inline]
    fn join(&self, other: &Self) -> Self {
        ::std::cmp::max(*self, *other)
    }
    #[inline]
    fn meet(&self, other: &Self) -> Self {
        ::std::cmp::min(*self, *other)
    }
}

impl mz_persist_types::Codec64 for Timestamp {
    fn codec_name() -> String {
        u64::codec_name()
    }

    fn encode(&self) -> [u8; std::mem::size_of::<Self>()] {
        self.internal.encode()
    }

    fn decode(buf: [u8; std::mem::size_of::<Self>()]) -> Self {
        Self {
            internal: u64::decode(buf),
        }
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.internal, f)
    }
}

impl std::fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.internal, f)
    }
}

impl std::str::FromStr for Timestamp {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            internal: s.parse().map_err(|_| "could not parse mz_timestamp")?,
        })
    }
}

impl TryFrom<Duration> for Timestamp {
    type Error = TryFromIntError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Ok(Self {
            internal: value.as_millis().try_into()?,
        })
    }
}

impl TryFrom<u128> for Timestamp {
    type Error = TryFromIntError;

    fn try_from(value: u128) -> Result<Self, Self::Error> {
        Ok(Self {
            internal: value.try_into()?,
        })
    }
}

impl TryFrom<i64> for Timestamp {
    type Error = TryFromIntError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Self {
            internal: value.try_into()?,
        })
    }
}

impl TryFrom<Numeric> for Timestamp {
    type Error = TryFromDecimalError;

    fn try_from(value: Numeric) -> Result<Self, Self::Error> {
        Ok(Self {
            internal: value.try_into()?,
        })
    }
}

impl columnation::Columnation for Timestamp {
    type InnerRegion = columnation::CloneRegion<Timestamp>;
}
