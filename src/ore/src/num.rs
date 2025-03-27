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

//! Number utilities

use num::traits::bounds::UpperBounded;
use num::Signed;
#[cfg(feature = "proptest")]
use proptest::arbitrary::Arbitrary;
#[cfg(feature = "proptest")]
use proptest::strategy::{BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
#[cfg(feature = "proptest")]
use std::ops::Range;
use std::ops::{Add, AddAssign, Deref, Div, Mul, Neg, Rem, Sub, SubAssign};

/// A wrapper type which ensures a signed number is non-negative.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash, Ord, PartialOrd)]
#[repr(transparent)]
#[serde(transparent)]
pub struct NonNeg<T>(T)
where
    T: Signed + fmt::Display;

impl<T> NonNeg<T>
where
    T: Signed + fmt::Display,
{
    /// Returns the minimum value of the type.
    pub fn min() -> NonNeg<T> {
        NonNeg(T::zero())
    }

    /// Returns the maximum value of the type.
    pub fn max() -> NonNeg<T>
    where
        T: UpperBounded,
    {
        NonNeg(T::max_value())
    }

    /// Attempts to construct a `NonNeg` from its underlying type.
    ///
    /// Returns an error if `n` is negative.
    pub fn try_from(n: T) -> Result<NonNeg<T>, NonNegError> {
        match n.is_negative() {
            false => Ok(NonNeg(n)),
            true => Err(NonNegError),
        }
    }
}

impl<T> fmt::Display for NonNeg<T>
where
    T: Signed + fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> Deref for NonNeg<T>
where
    T: Signed + fmt::Display,
{
    type Target = T;

    fn deref(&self) -> &T {
        &self.0
    }
}

impl From<NonNeg<i64>> for u64 {
    fn from(n: NonNeg<i64>) -> u64 {
        u64::try_from(*n).expect("non-negative")
    }
}

#[cfg(target_pointer_width = "64")]
impl crate::cast::CastFrom<NonNeg<i64>> for usize {
    #[allow(clippy::as_conversions)]
    fn cast_from(from: NonNeg<i64>) -> usize {
        usize::cast_from(u64::from(from))
    }
}

#[cfg(feature = "proptest")]
impl<T> Arbitrary for NonNeg<T>
where
    T: Signed + UpperBounded + fmt::Display + fmt::Debug + Copy + 'static,
    Range<T>: Strategy<Value = T>,
{
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (*Self::min()..*Self::max()).prop_map(NonNeg).boxed()
    }
}

/// An error indicating the attempted construction of a `NonNeg` with a negative
/// number.
#[derive(Debug, Clone)]
pub struct NonNegError;

impl fmt::Display for NonNegError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("cannot construct NonNeg from negative number")
    }
}

impl Error for NonNegError {}

/// Overflowing number. Operations panic on overflow, even in release mode.
#[derive(Debug, Default, Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize, Deserialize)]
pub struct Overflowing<T>(T);

impl<T: fmt::Display> fmt::Display for Overflowing<T> {
    #[inline(always)]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T> Deref for Overflowing<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::borrow::Borrow<T> for Overflowing<T> {
    #[inline(always)]
    fn borrow(&self) -> &T {
        &self.0
    }
}

#[cfg(feature = "columnar")]
mod columnar {
    use crate::num::Overflowing;
    use columnar::common::index::CopyAs;
    use columnar::{AsBytes, Clear, Columnar, Container, FromBytes, Index, IndexAs, Len, Push};
    use serde::{Deserialize, Serialize};

    impl<T: Columnar + Copy + Send> Columnar for Overflowing<T>
    where
        Vec<T>: Container<T>,
        Overflowing<T>: From<T>,
        for<'a> <T as Columnar>::Ref<'a>: CopyAs<T>,
    {
        type Ref<'a> = Overflowing<T>;
        #[inline(always)]
        fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
            other
        }
        type Container = Overflows<T, Vec<T>>;
    }

    /// Columnar container for [`Overflowing`].
    #[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct Overflows<T, TC>(TC, std::marker::PhantomData<T>);

    impl<T, TC: Default> Default for Overflows<T, TC> {
        #[inline(always)]
        fn default() -> Self {
            Self(TC::default(), std::marker::PhantomData)
        }
    }

    impl<T: Columnar + Copy + Send, TC: Container<T>> Container<Overflowing<T>> for Overflows<T, TC>
    where
        Vec<T>: Container<T>,
        Overflowing<T>: From<T>,
        for<'a> <T as Columnar>::Ref<'a>: CopyAs<T>,
    {
        type Borrowed<'a>
            = Overflows<T, TC::Borrowed<'a>>
        where
            Self: 'a;
        #[inline(always)]
        fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
            Overflows(self.0.borrow(), std::marker::PhantomData)
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

    impl<T: Copy, TC: Push<T>> Push<Overflowing<T>> for Overflows<T, TC> {
        #[inline(always)]
        fn push(&mut self, item: Overflowing<T>) {
            self.0.push(*item);
        }
    }

    impl<T: Copy, TC: Push<T>> Push<&Overflowing<T>> for Overflows<T, TC> {
        #[inline(always)]
        fn push(&mut self, item: &Overflowing<T>) {
            self.0.push(**item);
        }
    }
}

macro_rules! impl_overflowing {
    ($t:ty, $($f:ty)*, $($fo:ty)*, $($cf:ty)*) => {
        impl Overflowing<$t> {
            /// The value zero.
            pub const ZERO: Self = Self(0);
            /// The value minus one.
            pub const MINUS_ONE: Self = Self(-1);
            /// The value one.
            pub const ONE: Self = Self(1);
            /// The minimum value.
            pub const MIN: Self = Self(<$t>::min_value());
            /// The maximum value.
            pub const MAX: Self = Self(<$t>::max_value());

            /// Returns the absolute value of the number.
            pub fn abs(self) -> Self {
                Self(self.0.abs())
            }

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
        }

        impl Add<Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn add(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_add(rhs.0) {
                    (_, true) => panic!("Overflow {self} + {rhs}"),
                    (result, false) => Self(result),
                }
            }
        }

        impl<'a> Add<&'a Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn add(self, rhs: &'a Self) -> Self::Output {
                match self.0.overflowing_add(rhs.0) {
                    (_, true) => panic!("Overflow {self} + {rhs}"),
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
                    (_, true) => panic!("Overflow {self} / {rhs}"),
                    (result, false) => Self(result),
                }
            }
        }

        impl Rem<Self> for Overflowing<$t> {
            type Output = Overflowing<<$t as Rem>::Output>;

            #[inline(always)]
            fn rem(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_rem(rhs.0) {
                    (_, true) => panic!("Overflow {self} % {rhs}"),
                    (result, false) => Self(result),
                }
            }
        }

        impl Sub<Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn sub(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_sub(rhs.0) {
                    (_, true) => panic!("Overflow {self} - {rhs}"),
                    (result, false) => Self(result),
                }
            }
        }

        impl<'a> Sub<&'a Self> for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn sub(self, rhs: &'a Self) -> Self::Output {
                match self.0.overflowing_sub(rhs.0) {
                    (_, true) => panic!("Overflow {self} - {rhs}"),
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
            fn sum<I: Iterator<Item=Self>>(iter: I) -> Self {
                iter.fold(
                    Self::ZERO,
                    |a, b| a + b,
                )
            }
        }

        impl<'a> std::iter::Sum<&'a Overflowing<$t>> for Overflowing<$t> {
            #[inline(always)]
            fn sum<I: Iterator<Item=&'a Self>>(iter: I) -> Self {
                iter.fold(
                    Self::ZERO,
                    |a, b| a + b,
                )
            }
        }

        impl Neg for Overflowing<$t> {
            type Output = Overflowing<<$t as Neg>::Output>;

            #[inline(always)]
            fn neg(self) -> Self::Output {
                match self.0.overflowing_neg() {
                    (_, true) => panic!("Overflow -{self}"),
                    (result, false) => Self(result),
                }
            }
        }

        impl Neg for &Overflowing<$t> {
            type Output = Overflowing<<$t as Neg>::Output>;

            #[inline(always)]
            fn neg(self) -> Self::Output {
                match self.0.overflowing_neg() {
                    (_, true) => panic!("Overflow -{self}"),
                    (result, false) => Overflowing(result),
                }
            }
        }

        impl Mul for Overflowing<$t> {
            type Output = Self;

            #[inline(always)]
            fn mul(self, rhs: Self) -> Self::Output {
                match self.0.overflowing_mul(rhs.0) {
                    (_, true) => panic!("Overflow {self} * {rhs}"),
                    (result, false) => Self(result),
                }
            }
        }

        $(
            impl From<$f> for Overflowing<$t> {
                #[inline(always)]
                fn from(value: $f) -> Self {
                    Self(value.into())
                }
            }
        )*

        $(
            impl From<Overflowing<$fo>> for Overflowing<$t> {
                #[inline(always)]
                fn from(value: Overflowing<$fo>) -> Self {
                    Self(value.0.into())
                }
            }
        )*

        $(
            impl crate::cast::CastFrom<$cf> for Overflowing<$t> {
                #[inline(always)]
                fn cast_from(value: $cf) -> Self {
                    Self(<$t>::cast_from(value))
                }
            }
        )*

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
        impl differential_dataflow::difference::Abelian for Overflowing<$t> {
            #[inline(always)]
            fn negate(&mut self) {
                *self = -*self
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

        impl TryFrom<usize> for Overflowing<$t> {
            type Error = <u32 as TryFrom<usize>>::Error;

            #[inline(always)]
            fn try_from(value: usize) -> Result<Self, Self::Error> {
                <$t>::try_from(value).map(Self)
            }
        }

        impl TryFrom<Overflowing<$t>> for usize {
            type Error = <usize as TryFrom<$t>>::Error;

            #[inline(always)]
            fn try_from(value: Overflowing<$t>) -> Result<Self, Self::Error> {
                Self::try_from(value.0)
            }
        }

        impl std::hash::Hash for Overflowing<$t> {
            #[inline(always)]
            fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
                self.0.hash(state);
            }
        }

        #[cfg(feature = "proptest")]
        impl Arbitrary for Overflowing<$t> {
            type Strategy = BoxedStrategy<Self>;
            type Parameters = ();

            fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
                (Self::MIN.0..=Self::MAX.0).prop_map(Overflowing).boxed()
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
macro_rules! impl_overflowing_signed {
    ($t:ty, $u:ty) => {
        impl Overflowing<$t> {
            /// Returns the absolute value of the number as an unsigned integer.
            #[inline(always)]
            pub fn unsigned_abs(self) -> $u {
                self.0.unsigned_abs()
            }
        }
    };
}

// TODO: Support unsigned integers.
impl_overflowing!(i8, bool i8, bool,);
impl_overflowing_signed!(i8, u8);
impl_overflowing!(i16, bool i8 u8 i16, bool i8 u8,);
impl_overflowing_signed!(i16, u16);
impl_overflowing!(i32, bool i8 u8 i16 u16 i32, bool i8 u8 i16 u16, );
impl_overflowing_signed!(i32, u32);
// N.B. We're including `isize` here because we know it's 64 bits on all platforms we support.
impl_overflowing!(i64, bool i8 u8 i16 u16 i32 u32 i64, bool i8 u8 i16 u16 i32 u32, isize);
impl_overflowing_signed!(i64, u64);
impl_overflowing!(i128, bool i8 u8 i16 u16 i32 u32 i64 u64 i128, bool i8 u8 i16 u16 i32 u32 i64 u64, isize usize);
impl_overflowing_signed!(i128, u128);
