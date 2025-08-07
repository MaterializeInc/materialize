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
use mz_proto::{RustType, TryFromProtoError};
use mz_timely_util::temporal::BucketTimestamp;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize, Serializer};

use crate::adt::numeric::Numeric;
use crate::refresh_schedule::RefreshSchedule;
use crate::strconv::parse_timestamptz;

include!(concat!(env!("OUT_DIR"), "/mz_repr.timestamp.rs"));

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
    bytemuck::AnyBitPattern,
    bytemuck::NoUninit,
)]
#[repr(transparent)]
pub struct Timestamp {
    /// note no `pub`.
    internal: u64,
}

impl PartialEq<&Timestamp> for Timestamp {
    fn eq(&self, other: &&Timestamp) -> bool {
        self.eq(*other)
    }
}

impl PartialEq<Timestamp> for &Timestamp {
    fn eq(&self, other: &Timestamp) -> bool {
        self.internal.eq(&other.internal)
    }
}

impl RustType<ProtoTimestamp> for Timestamp {
    fn into_proto(&self) -> ProtoTimestamp {
        ProtoTimestamp {
            internal: self.into(),
        }
    }

    fn from_proto(proto: ProtoTimestamp) -> Result<Self, TryFromProtoError> {
        Ok(Timestamp::new(proto.internal))
    }
}

mod columnar_timestamp {
    use crate::Timestamp;
    use columnar::Columnar;
    use mz_ore::cast::CastFrom;

    /// A newtype wrapper for a vector of `Timestamp` values.
    #[derive(Clone, Copy, Default, Debug)]
    pub struct Timestamps<T>(T);
    impl<D, T: columnar::Push<D>> columnar::Push<D> for Timestamps<T> {
        #[inline(always)]
        fn push(&mut self, item: D) {
            self.0.push(item)
        }
    }
    impl<T: columnar::Clear> columnar::Clear for Timestamps<T> {
        #[inline(always)]
        fn clear(&mut self) {
            self.0.clear()
        }
    }
    impl<T: columnar::Len> columnar::Len for Timestamps<T> {
        #[inline(always)]
        fn len(&self) -> usize {
            self.0.len()
        }
    }
    impl<'a> columnar::Index for Timestamps<&'a [Timestamp]> {
        type Ref = Timestamp;

        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            self.0[index]
        }
    }

    impl Columnar for Timestamp {
        #[inline(always)]
        fn into_owned<'a>(other: columnar::Ref<'a, Self>) -> Self {
            other
        }
        type Container = Timestamps<Vec<Timestamp>>;
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: columnar::Ref<'a, Self>) -> columnar::Ref<'b, Self>
        where
            Self: 'a,
        {
            thing
        }
    }

    impl columnar::Container for Timestamps<Vec<Timestamp>> {
        type Ref<'a> = Timestamp;
        type Borrowed<'a>
            = Timestamps<&'a [Timestamp]>
        where
            Self: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Timestamps(self.0.as_slice())
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b>
        where
            Self: 'a,
        {
            Timestamps(item.0)
        }

        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b>
        where
            Self: 'a,
        {
            item
        }

        #[inline(always)]
        fn reserve_for<'a, I>(&mut self, selves: I)
        where
            Self: 'a,
            I: Iterator<Item = Self::Borrowed<'a>> + Clone,
        {
            self.0.reserve_for(selves.map(|s| s.0));
        }
    }

    impl columnar::HeapSize for Timestamp {}

    impl<T: columnar::HeapSize> columnar::HeapSize for Timestamps<T> {
        #[inline(always)]
        fn heap_size(&self) -> (usize, usize) {
            self.0.heap_size()
        }
    }

    impl<'a> columnar::AsBytes<'a> for Timestamps<&'a [Timestamp]> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item = (u64, &'a [u8])> {
            std::iter::once((
                u64::cast_from(align_of::<Timestamp>()),
                bytemuck::cast_slice(self.0),
            ))
        }
    }
    impl<'a> columnar::FromBytes<'a> for Timestamps<&'a [Timestamp]> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item = &'a [u8]>) -> Self {
            Timestamps(bytemuck::cast_slice(
                bytes.next().expect("Iterator exhausted prematurely"),
            ))
        }
    }
}

impl BucketTimestamp for Timestamp {
    fn advance_by_power_of_two(&self, exponent: u32) -> Option<Self> {
        let rhs = 1_u64.checked_shl(exponent)?;
        Some(self.internal.checked_add(rhs)?.into())
    }
}

pub trait TimestampManipulation:
    timely::progress::Timestamp
    + timely::order::TotalOrder
    + differential_dataflow::lattice::Lattice
    + std::fmt::Debug
    + mz_persist_types::StepForward
    + Sync
{
    /// Advance a timestamp by the least amount possible such that
    /// `ts.less_than(ts.step_forward())` is true. Panic if unable to do so.
    fn step_forward(&self) -> Self;

    /// Advance a timestamp forward by the given `amount`. Panic if unable to do so.
    fn step_forward_by(&self, amount: &Self) -> Self;

    /// Advance a timestamp forward by the given `amount`. Return `None` if unable to do so.
    fn try_step_forward_by(&self, amount: &Self) -> Option<Self>;

    /// Advance a timestamp by the least amount possible such that `ts.less_than(ts.step_forward())`
    /// is true. Return `None` if unable to do so.
    fn try_step_forward(&self) -> Option<Self>;

    /// Retreat a timestamp by the least amount possible such that
    /// `ts.step_back().unwrap().less_than(ts)` is true. Return `None` if unable,
    /// which must only happen if the timestamp is `Timestamp::minimum()`.
    fn step_back(&self) -> Option<Self>;

    /// Return the maximum value for this timestamp.
    fn maximum() -> Self;

    /// Rounds up the timestamp to the time of the next refresh according to the given schedule.
    /// Returns None if there is no next refresh.
    fn round_up(&self, schedule: &RefreshSchedule) -> Option<Self>;

    /// Rounds down `timestamp - 1` to the time of the previous refresh according to the given
    /// schedule.
    /// Returns None if there is no previous refresh.
    fn round_down_minus_1(&self, schedule: &RefreshSchedule) -> Option<Self>;
}

impl TimestampManipulation for Timestamp {
    fn step_forward(&self) -> Self {
        self.step_forward()
    }

    fn step_forward_by(&self, amount: &Self) -> Self {
        self.step_forward_by(amount)
    }

    fn try_step_forward(&self) -> Option<Self> {
        self.try_step_forward()
    }

    fn try_step_forward_by(&self, amount: &Self) -> Option<Self> {
        self.try_step_forward_by(amount)
    }

    fn step_back(&self) -> Option<Self> {
        self.step_back()
    }

    fn maximum() -> Self {
        Self::MAX
    }

    fn round_up(&self, schedule: &RefreshSchedule) -> Option<Self> {
        schedule.round_up_timestamp(*self)
    }

    fn round_down_minus_1(&self, schedule: &RefreshSchedule) -> Option<Self> {
        schedule.round_down_timestamp_m1(*self)
    }
}

impl mz_persist_types::StepForward for Timestamp {
    fn step_forward(&self) -> Self {
        self.step_forward()
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

    /// Advance a timestamp by the least amount possible such that `ts.less_than(ts.step_forward())`
    /// is true. Return `None` if unable to do so.
    pub fn try_step_forward(&self) -> Option<Self> {
        self.checked_add(1)
    }

    /// Advance a timestamp forward by the given `amount`. Return `None` if unable to do so.
    pub fn try_step_forward_by(&self, amount: &Self) -> Option<Self> {
        self.checked_add(*amount)
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

impl From<Timestamp> for Duration {
    fn from(ts: Timestamp) -> Self {
        Duration::from_millis(ts.internal)
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

impl timely::order::PartialOrder<&Timestamp> for Timestamp {
    fn less_equal(&self, other: &&Self) -> bool {
        self.internal.less_equal(&other.internal)
    }
}

impl timely::order::PartialOrder<Timestamp> for &Timestamp {
    fn less_equal(&self, other: &Timestamp) -> bool {
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

    fn encode(&self) -> [u8; 8] {
        self.internal.encode()
    }

    fn decode(buf: [u8; 8]) -> Self {
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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            internal: s
                .parse::<u64>()
                .map_err(|_| "could not parse as number of milliseconds since epoch".to_string())
                .or_else(|err_num_of_millis| {
                    parse_timestamptz(s)
                        .map_err(|parse_error| {
                            format!(
                                "{}; could not parse as date and time: {}",
                                err_num_of_millis, parse_error
                            )
                        })?
                        .timestamp_millis()
                        .try_into()
                        .map_err(|_| "out of range for mz_timestamp".to_string())
                })
                .map_err(|e: String| format!("could not parse mz_timestamp: {}", e))?,
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
    type InnerRegion = columnation::CopyRegion<Timestamp>;
}
