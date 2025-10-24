// Copyright 2019 The Rust Project Contributors
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Overflowing number types.

#[cfg(feature = "proptest")]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use std::ops::{Add, AddAssign, Div, Mul, Neg, Rem, Sub, SubAssign};

/// Overflowing number. Operations panic on overflow, even in release mode.
///
/// The `ore_overflowing_behavior` feature flag can be used to control the
/// overflow behavior:
/// * `panic`: panic on overflow (default when debug assertions are enabled).
/// * `soft_panic`: log a warning on overflow, or panic, depending on whether
///   soft assertions are enbaled.
/// * `ignore`: ignore overflow (default when debug assertions are disabled).
/// The default value is `panic` when `debug_assertions` are enabled, or `ignore` otherwise.
///
/// The non-aborting modes simply return the result of the operation, which can
/// include overflows.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "proptest", derive(Arbitrary))]
pub struct Overflowing<T>(T);

/// The behavior of the [`Overflowing`] type when an overflow occurs.
#[derive(Debug)]
pub enum OverflowingBehavior {
    /// Panic on overflow. Corresponds to the `panic` string.
    Panic,
    /// Soft panic on overflow. Corresponds to the `soft_panic` string.
    SoftPanic,
    /// Ignore overflow. Corresponds to the `ignore` string.
    Ignore,
}

impl std::str::FromStr for OverflowingBehavior {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            _ if s.eq_ignore_ascii_case("panic") => Ok(OverflowingBehavior::Panic),
            _ if s.eq_ignore_ascii_case("soft_panic") => Ok(OverflowingBehavior::SoftPanic),
            _ if s.eq_ignore_ascii_case("ignore") => Ok(OverflowingBehavior::Ignore),
            _ => Err(format!("Invalid OverflowingBehavior: {s}")),
        }
    }
}

/// Set the overflowing behavior for the process.
///
/// This function is thread-safe and can be used to change the behavior at runtime.
///
/// The default behavior is to ignore overflows.
pub fn set_behavior(behavior: OverflowingBehavior) {
    overflowing_support::set_overflowing_mode(behavior);
}

impl<T> Overflowing<T> {
    /// Returns the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Overflowing<T> {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(feature = "columnar")]
mod columnar {
    use crate::overflowing::Overflowing;
    use columnar::common::PushIndexAs;
    use columnar::{
        AsBytes, Borrow, Clear, Columnar, Container, FromBytes, Index, IndexAs, Len, Push,
    };
    use serde::{Deserialize, Serialize};
    use std::ops::Range;

    impl<T: Columnar + Copy + Send> Columnar for Overflowing<T>
    where
        for<'a> &'a [T]: AsBytes<'a> + FromBytes<'a>,
        Overflowing<T>: From<T>,
    {
        #[inline(always)]
        fn into_owned(other: columnar::Ref<'_, Self>) -> Self {
            other
        }
        type Container = Overflows<T>;
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(thing: columnar::Ref<'a, Self>) -> columnar::Ref<'b, Self>
        where
            Self: 'a,
        {
            thing
        }
    }

    /// Columnar container for [`Overflowing`].
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Overflows<T, TC = Vec<T>>(TC, std::marker::PhantomData<T>);

    impl<T, TC: Default> Default for Overflows<T, TC> {
        #[inline(always)]
        fn default() -> Self {
            Self(TC::default(), std::marker::PhantomData)
        }
    }

    impl<T: Columnar + Copy + Send, TC: PushIndexAs<T>> Borrow for Overflows<T, TC>
    where
        Overflowing<T>: From<T>,
    {
        type Ref<'a> = Overflowing<T>;
        type Borrowed<'a>
            = Overflows<T, TC::Borrowed<'a>>
        where
            Self: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Overflows(self.0.borrow(), std::marker::PhantomData)
        }
        #[inline(always)]
        fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b>
        where
            Self: 'a,
        {
            Overflows(TC::reborrow(item.0), std::marker::PhantomData)
        }

        #[inline(always)]
        fn reborrow_ref<'b, 'a: 'b>(item: Self::Ref<'a>) -> Self::Ref<'b>
        where
            Self: 'a,
        {
            item
        }
    }

    impl<T: Columnar + Copy + Send, TC: PushIndexAs<T>> Container for Overflows<T, TC>
    where
        Overflowing<T>: From<T>,
    {
        #[inline(always)]
        fn extend_from_self(&mut self, other: Self::Borrowed<'_>, range: Range<usize>) {
            self.0.extend_from_self(other.0, range);
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

    impl<'a, T: Copy, TC: AsBytes<'a>> AsBytes<'a> for Overflows<T, TC> {
        #[inline(always)]
        fn as_bytes(&self) -> impl Iterator<Item = (u64, &'a [u8])> {
            self.0.as_bytes()
        }
    }

    impl<'a, T: Copy, TC: FromBytes<'a>> FromBytes<'a> for Overflows<T, TC> {
        #[inline(always)]
        fn from_bytes(bytes: &mut impl Iterator<Item = &'a [u8]>) -> Self {
            Self(TC::from_bytes(bytes), std::marker::PhantomData)
        }
    }

    impl<T: Copy, TC: Len> Len for Overflows<T, TC> {
        #[inline(always)]
        fn len(&self) -> usize {
            self.0.len()
        }
    }

    impl<T: Copy, TC: Clear> Clear for Overflows<T, TC> {
        #[inline(always)]
        fn clear(&mut self) {
            self.0.clear();
        }
    }

    impl<T: Copy, TC: IndexAs<T>> Index for Overflows<T, TC>
    where
        Overflowing<T>: From<T>,
    {
        type Ref = Overflowing<T>;
        #[inline(always)]
        fn get(&self, index: usize) -> Self::Ref {
            self.0.index_as(index).into()
        }
    }

    impl<T: Copy, TC: for<'a> Push<&'a T>> Push<Overflowing<T>> for Overflows<T, TC> {
        #[inline(always)]
        fn push(&mut self, item: Overflowing<T>) {
            self.0.push(&item.0);
        }
    }

    impl<T: Copy, TC: Push<T>> Push<&Overflowing<T>> for Overflows<T, TC> {
        #[inline(always)]
        fn push(&mut self, item: &Overflowing<T>) {
            self.0.push(item.0);
        }
    }

    impl<T, TC: columnar::HeapSize> columnar::HeapSize for Overflows<T, TC> {
        #[inline(always)]
        fn heap_size(&self) -> (usize, usize) {
            self.0.heap_size()
        }
    }
}

macro_rules! impl_overflowing {
    ($t:ty) => {
        impl Overflowing<$t> {
            /// The value zero.
            pub const ZERO: Self = Self(0);
            /// The value one.
            pub const ONE: Self = Self(1);
            /// The minimum value.
            pub const MIN: Self = Self(<$t>::MIN);
            /// The maximum value.
            pub const MAX: Self = Self(<$t>::MAX);

            /// Checked addition. Returns `None` if overflow occurred.
            #[inline(always)]
            pub fn checked_add(self, rhs: Self) -> Option<Self> {
                self.0.checked_add(rhs.0).map(Self)
            }

            /// Wrapping addition.
            #[inline(always)]
            pub fn wrapping_add(self, rhs: Self) -> Self {
                Self(self.0.wrapping_add(rhs.0))
            }

            /// Checked multiplication. Returns `None` if overflow occurred.
            #[inline(always)]
            pub fn checked_mul(self, rhs: Self) -> Option<Self> {
                self.0.checked_mul(rhs.0).map(Self)
            }

            /// Wrapping multiplication.
            #[inline(always)]
            pub fn wrapping_mul(self, rhs: Self) -> Self {
                Self(self.0.wrapping_mul(rhs.0))
            }

            /// Returns `true` if the number is zero.
            pub fn is_zero(self) -> bool {
                self == Self::ZERO
            }
        }

        impl Add<Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn add(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_add(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} + {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl<'a> Add<&'a Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn add(self, rhs: &'a Self) -> Self::Output {
                match self.0.overflowing_add(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} + {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl AddAssign<Self> for Overflowing<$t> {
            #[inline(always)]
            fn add_assign(&mut self, rhs: Self) {
                *self = *self + rhs;
            }
        }

        impl AddAssign<&Self> for Overflowing<$t> {
            #[inline(always)]
            fn add_assign(&mut self, rhs: &Self) {
                *self = *self + *rhs;
            }
        }

        impl Div<Self> for Overflowing<$t> {
            type Output = Overflowing<<$t as Div>::Output>;

            #[inline(always)]
            fn div(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_div(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} / {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl Rem<Self> for Overflowing<$t> {
            type Output = Overflowing<<$t as Rem>::Output>;

            #[inline(always)]
            fn rem(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_rem(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} % {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl Sub<Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn sub(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_sub(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} - {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl<'a> Sub<&'a Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn sub(self, rhs: &'a Self) -> Self::Output {
                match self.0.overflowing_sub(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} - {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl SubAssign<Self> for Overflowing<$t> {
            #[inline(always)]
            fn sub_assign(&mut self, rhs: Self) {
                *self = *self - rhs;
            }
        }

        impl SubAssign<&Self> for Overflowing<$t> {
            #[inline(always)]
            fn sub_assign(&mut self, rhs: &Self) {
                *self = *self - *rhs;
            }
        }

        impl std::iter::Sum<Overflowing<$t>> for Overflowing<$t> {
            #[inline(always)]
            fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
                iter.fold(Self::ZERO, |a, b| a + b)
            }
        }

        impl<'a> std::iter::Sum<&'a Overflowing<$t>> for Overflowing<$t> {
            #[inline(always)]
            fn sum<I: Iterator<Item = &'a Self>>(iter: I) -> Self {
                iter.fold(Self::ZERO, |a, b| a + b)
            }
        }

        impl Mul for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn mul(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_mul(rhs.0) {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("{self} * {rhs}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        #[cfg(feature = "differential-dataflow")]
        impl differential_dataflow::difference::IsZero for Overflowing<$t> {
            #[inline(always)]
            fn is_zero(&self) -> bool {
                self.0.is_zero()
            }
        }

        #[cfg(feature = "differential-dataflow")]
        impl differential_dataflow::difference::Semigroup for Overflowing<$t> {
            #[inline(always)]
            fn plus_equals(&mut self, rhs: &Self) {
                *self += *rhs
            }
        }

        #[cfg(feature = "differential-dataflow")]
        impl differential_dataflow::difference::Monoid for Overflowing<$t> {
            #[inline(always)]
            fn zero() -> Self {
                Self::ZERO
            }
        }

        #[cfg(feature = "differential-dataflow")]
        impl differential_dataflow::difference::Multiply<Self> for Overflowing<$t> {
            type Output = Self;
            #[inline(always)]
            fn multiply(self, rhs: &Self) -> Self::Output {
                self * *rhs
            }
        }

        #[cfg(feature = "columnation")]
        impl columnation::Columnation for Overflowing<$t> {
            type InnerRegion = columnation::CopyRegion<Self>;
        }

        impl std::str::FromStr for Overflowing<$t> {
            type Err = <$t as std::str::FromStr>::Err;

            #[inline(always)]
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                <$t>::from_str(s).map(Self)
            }
        }

        impl std::hash::Hash for Overflowing<$t> {
            #[inline(always)]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }

        impl<T> crate::cast::CastFrom<T> for Overflowing<$t>
        where
            $t: crate::cast::CastFrom<T>,
        {
            #[inline(always)]
            fn cast_from(value: T) -> Self {
                Self(<$t>::cast_from(value))
            }
        }

        #[cfg(feature = "num-traits")]
        impl num_traits::identities::Zero for Overflowing<$t> {
            #[inline(always)]
            fn zero() -> Self {
                Self::ZERO
            }
            #[inline(always)]
            fn is_zero(&self) -> bool {
                self.0.is_zero()
            }
        }

        #[cfg(feature = "num-traits")]
        impl num_traits::identities::One for Overflowing<$t> {
            #[inline(always)]
            fn one() -> Self {
                Self::ONE
            }
        }

        #[cfg(feature = "num-traits")]
        impl num_traits::Num for Overflowing<$t> {
            type FromStrRadixErr = <$t as num_traits::Num>::FromStrRadixErr;

            #[inline(always)]
            fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
                <$t>::from_str_radix(str, radix).map(Self)
            }
        }
    };
}

macro_rules! impl_overflowing_from {
    ($t:ty, $($f:ty)+) => {
        $(
            impl From<$f> for Overflowing<$t> {
                #[inline(always)]
                fn from(value: $f) -> Self {
                    Self(value.into())
                }
            }
        )+
    };
}

macro_rules! impl_overflowing_from_overflowing {
    ($t:ty, $($f:ty)+) => {
        $(
            impl From<Overflowing<$f>> for Overflowing<$t> {
                #[inline(always)]
                fn from(value: Overflowing<$f>) -> Self {
                    Self(value.0.into())
                }
            }
        )+
    };
}

macro_rules! impl_overflowing_try_from {
    ($t:ty, $($f:ty)+) => {
        $(
            impl TryFrom<$f> for Overflowing<$t> {
                type Error = <$t as TryFrom<$f>>::Error;
                #[inline(always)]
                fn try_from(value: $f) -> Result<Self, Self::Error> {
                    <$t>::try_from(value).map(Self)
                }
            }

            impl TryFrom<Overflowing<$f>> for Overflowing<$t> {
                type Error = <$t as TryFrom<$f>>::Error;
                #[inline(always)]
                fn try_from(value: Overflowing<$f>) -> Result<Self, Self::Error> {
                    <$t>::try_from(value.0).map(Self)
                }
            }
        )+
    };
}

// Implement Overflowing for signed types.
macro_rules! impl_overflowing_signed {
    ($t:ty, $u:ty) => {
        impl Overflowing<$t> {
            /// The value minus one.
            pub const MINUS_ONE: Self = Self(-1);

            /// Returns the absolute value of the number.
            pub fn abs(self) -> Self {
                Self(self.0.abs())
            }

            /// Returns the absolute value of the number as an unsigned integer.
            #[inline(always)]
            pub fn unsigned_abs(self) -> $u {
                self.0.unsigned_abs()
            }

            /// Returns `true` if the number is positive and `false` if the number is zero
            /// or negative.
            ///
            /// # Examples
            ///
            /// ```
            /// # use mz_ore::Overflowing;
            /// assert!(!Overflowing::<i64>::from(-10i32).is_positive());
            /// assert!(Overflowing::<i64>::from(10i32).is_positive());
            /// ```
            pub fn is_positive(self) -> bool {
                self > Self::ZERO
            }

            /// Returns `true` if the number is negative and `false` if the number is zero
            /// or positive.
            ///
            /// # Examples
            ///
            /// ```
            /// # use mz_ore::Overflowing;
            /// assert!(Overflowing::<i64>::from(-10i32).is_negative());
            /// assert!(!Overflowing::<i64>::from(10i32).is_negative());
            /// ```
            pub fn is_negative(self) -> bool {
                self < Self::ZERO
            }
        }

        impl Neg for Overflowing<$t> {
            type Output = Overflowing<<$t as Neg>::Output>;

            #[inline(always)]
            fn neg(self) -> Self::Output {
                match self.0.overflowing_neg() {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("-{self}"))
                    }
                    (result, false) => Self(result),
                }
            }
        }

        impl Neg for &Overflowing<$t> {
            type Output = Overflowing<<$t as Neg>::Output>;

            #[inline(always)]
            fn neg(self) -> Self::Output {
                match self.0.overflowing_neg() {
                    (result, true) => {
                        overflowing_support::handle_overflow(result, format_args!("-{self}"))
                    }
                    (result, false) => Overflowing(result),
                }
            }
        }

        #[cfg(feature = "differential-dataflow")]
        impl differential_dataflow::difference::Abelian for Overflowing<$t> {
            #[inline(always)]
            fn negate(&mut self) {
                *self = -*self
            }
        }

        #[cfg(feature = "num-traits")]
        impl num_traits::sign::Signed for Overflowing<$t> {
            #[inline(always)]
            fn abs(&self) -> Self {
                Self(self.0.abs())
            }
            #[inline(always)]
            fn abs_sub(&self, other: &Self) -> Self {
                Self(self.0.abs_sub(&other.0))
            }
            #[inline(always)]
            fn signum(&self) -> Self {
                Self(self.0.signum())
            }
            #[inline(always)]
            fn is_positive(&self) -> bool {
                self.0.is_positive()
            }
            #[inline(always)]
            fn is_negative(&self) -> bool {
                self.0.is_negative()
            }
        }
    };
}

macro_rules! overflowing {
    ($t:ty, $($fit:ty)+, $($may_fit:ty)+ $(, $unsigned:ty)?) => {
        impl_overflowing!($t);
        impl_overflowing_from!($t, $($fit)+ $t);
        impl_overflowing_from_overflowing!($t, $($fit)+);
        impl_overflowing_try_from!($t, $($may_fit)+);
        $( impl_overflowing_signed!($t, $unsigned); )?
    };
}

// type, types that certainly fit, types that may fit, optional corresponding unsigned type
overflowing!(u8, bool, u16 u32 u64 u128 i8 i16 i32 i64 i128 isize usize);
overflowing!(u16, bool u8, u32 u64 u128 i8 i16 i32 i64 i128 isize usize);
overflowing!(u32, bool u8 u16, u64 u128 i8 i16 i32 i64 i128 isize usize);
overflowing!(u64, bool u8 u16 u32, u128 i8 i16 i32 i64 i128 isize usize);
overflowing!(u128, bool u8 u16 u32 u64, i8 i16 i32 i64 i128 isize usize);

overflowing!(i8, bool, u8 i16 u16 i32 u32 i64 u64 i128 u128 isize usize, u8);
overflowing!(i16, bool i8 u8, u16 i32 u32 i64 u64 i128 u128 isize usize, u16);
overflowing!(i32, bool i8 u8 i16 u16, u32 i64 u64 i128 u128 isize usize, u32);
overflowing!(i64, bool i8 u8 i16 u16 i32 u32, u64 i128 u128 isize usize, u64);
overflowing!(i128, bool i8 u8 i16 u16 i32 u32 i64 u64, u128 isize usize, u128);

mod overflowing_support {
    use std::sync::atomic::AtomicUsize;

    use crate::overflowing::OverflowingBehavior;

    /// Ignore overflow.
    const MODE_IGNORE: usize = 0;
    /// Soft assert on overflow.
    const MODE_SOFT_PANIC: usize = 1;
    /// Panic on overflow.
    const MODE_PANIC: usize = 2;

    static OVERFLOWING_MODE: AtomicUsize = AtomicUsize::new(MODE_IGNORE);

    /// Handles overflow for [`Overflowing`](super::Overflowing) numbers.
    #[track_caller]
    #[cold]
    pub(super) fn handle_overflow<T: Into<O>, O>(result: T, description: std::fmt::Arguments) -> O {
        let mode = OVERFLOWING_MODE.load(std::sync::atomic::Ordering::Relaxed);
        match mode {
            #[cfg(not(target_arch = "wasm32"))]
            MODE_SOFT_PANIC => crate::soft_panic_or_log!("Overflow: {description}"),
            // We cannot use the logging `soft_panic_or_log` in wasm, so we panic instead (soft
            // assertions are always enabled in wasm).
            #[cfg(target_arch = "wasm32")]
            MODE_SOFT_PANIC => panic!("Overflow: {description}"),
            MODE_PANIC => panic!("Overflow: {description}"),
            // MODE_IGNORE and all other (impossible) values
            _ => {}
        }
        result.into()
    }

    /// Set the overflowing mode.
    pub(crate) fn set_overflowing_mode(behavior: OverflowingBehavior) {
        let value = match behavior {
            OverflowingBehavior::Panic => MODE_PANIC,
            OverflowingBehavior::SoftPanic => MODE_SOFT_PANIC,
            OverflowingBehavior::Ignore => MODE_IGNORE,
        };
        OVERFLOWING_MODE.store(value, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[cfg(debug_assertions)]
    #[crate::test]
    #[should_panic]
    fn test_panicking_add() {
        set_behavior(OverflowingBehavior::Panic);
        let _ = Overflowing::<i8>::MAX + Overflowing::<i8>::ONE;
    }

    #[crate::test]
    fn test_wrapping_add() {
        let result = Overflowing::<i8>::MAX.wrapping_add(Overflowing::<i8>::ONE);
        assert_eq!(result, Overflowing::<i8>::MIN);
    }

    #[crate::test]
    fn test_checked_add() {
        let result = Overflowing::<i8>::MAX.checked_add(Overflowing::<i8>::ONE);
        assert_eq!(result, None);
    }
}
