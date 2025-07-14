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

//! Columnar utilities.

use columnar::{AsBytes, Clear, Columnar, Container, FromBytes, Index, Len, Push};
use proptest::prelude::{Arbitrary, BoxedStrategy, Strategy};
use serde::{Deserialize, Serialize};

/// A boxed type that implements `Columnar` and can be used in columnar storage.
#[derive(PartialEq, Eq, PartialOrd, Ord, Serialize, Hash)]
pub struct Boxed<T: ?Sized>(Box<T>);

impl<T: ?Sized> std::fmt::Debug for Boxed<T>
where
    Box<T>: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: std::fmt::Display> std::fmt::Display for Boxed<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Manual Deserialize implementation for `Boxed<T>`
impl<'de, T: ?Sized> Deserialize<'de> for Boxed<T>
where
    Box<T>: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let boxed: Box<T> = Deserialize::deserialize(deserializer)?;
        Ok(Boxed(boxed))
    }
}

impl<T: ?Sized> Clone for Boxed<T>
where
    Box<T>: Clone,
{
    fn clone(&self) -> Self {
        Boxed(self.0.clone())
    }

    fn clone_from(&mut self, other: &Self) {
        self.0.clone_from(&other.0);
    }
}

impl<T: ?Sized> Boxed<T> {
    /// Creates a new `Boxed` instance from a boxed value.
    pub fn new(value: T) -> Self
    where
        T: Sized,
    {
        Boxed(Box::new(value))
    }

    /// Creates a new `Boxed` instance from a boxed value.
    pub fn new_boxed(value: Box<T>) -> Self {
        Boxed(value)
    }

    /// Return a reference to the inner boxed value.
    pub fn inner(&self) -> &Box<T> {
        &self.0
    }

    /// Consumes the `Boxed` instance and returns the inner boxed value.
    pub fn into_inner(self) -> Box<T> {
        self.0
    }
}

impl<T: ?Sized> From<Box<T>> for Boxed<T> {
    fn from(value: Box<T>) -> Self {
        Boxed(value)
    }
}
//
// impl<T> From<T> for Boxed<T> {
//     fn from(value: T) -> Self {
//         Boxed::new(value)
//     }
// }

impl From<&str> for Boxed<str> {
    fn from(value: &str) -> Self {
        Boxed::new_boxed(Box::from(value))
    }
}

impl From<&String> for Boxed<str> {
    fn from(value: &String) -> Self {
        Boxed::new_boxed(Box::from(value.as_str()))
    }
}

impl From<String> for Boxed<str> {
    fn from(value: String) -> Self {
        Boxed::new_boxed(Box::from(&*value))
    }
}

impl<T: ?Sized> std::ops::Deref for Boxed<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Columnar> Columnar for Boxed<T> {
    type Ref<'a> = BoxedRef<T::Ref<'a>>;

    fn into_owned<'a>(other: Self::Ref<'a>) -> Self {
        Boxed(Box::new(T::into_owned(other.0)))
    }

    fn copy_from<'a>(&mut self, other: Self::Ref<'a>)
    where
        Self: Sized,
    {
        T::copy_from(&mut self.0, other.0);
    }

    type Container = BoxedContainer<T>;

    fn reborrow<'b, 'a: 'b>(thing: Self::Ref<'a>) -> Self::Ref<'b>
    where
        Self: 'a,
    {
        BoxedRef(T::reborrow(thing.0))
    }
}

/// A reference to a boxed type that implements `Columnar`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoxedRef<T>(T);

/// A container for
#[derive(Debug)]
pub struct BoxedContainer<T: Columnar>(T::Container);

impl<T: Columnar> Container<Boxed<T>> for BoxedContainer<T> {
    type Borrowed<'a>
        = BoxedRef<<T::Container as Container<T>>::Borrowed<'a>>
    where
        Self: 'a;

    fn borrow<'a>(&'a self) -> Self::Borrowed<'a> {
        BoxedRef(self.0.borrow())
    }

    fn reborrow<'b, 'a: 'b>(item: Self::Borrowed<'a>) -> Self::Borrowed<'b>
    where
        Self: 'a,
    {
        BoxedRef(<T::Container as Container<T>>::reborrow(item.0))
    }
}

impl<T: Columnar> Clone for BoxedContainer<T>
where
    T::Container: Clone,
{
    fn clone(&self) -> Self {
        BoxedContainer(self.0.clone())
    }

    fn clone_from(&mut self, other: &Self) {
        self.0.clone_from(&other.0);
    }
}

impl<T: Columnar> Default for BoxedContainer<T>
where
    T::Container: Default,
{
    fn default() -> Self {
        BoxedContainer(T::Container::default())
    }
}

impl<T: Columnar> Clear for BoxedContainer<T> {
    fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T: Clear> Clear for BoxedRef<T> {
    fn clear(&mut self) {
        self.0.clear();
    }
}

impl<T: Columnar> Len for BoxedContainer<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T: Len> Len for BoxedRef<T> {
    fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T: Index> Index for BoxedRef<T> {
    type Ref = BoxedRef<T::Ref>;

    fn get(&self, index: usize) -> Self::Ref {
        BoxedRef(self.0.get(index))
    }
}

impl<'a, T: FromBytes<'a>> FromBytes<'a> for BoxedRef<T> {
    fn from_bytes(bytes: &mut impl Iterator<Item = &'a [u8]>) -> Self {
        BoxedRef(T::from_bytes(bytes))
    }
}
impl<'a, T: AsBytes<'a>> AsBytes<'a> for BoxedRef<T> {
    fn as_bytes(&self) -> impl Iterator<Item = (u64, &'a [u8])> {
        self.0.as_bytes()
    }
}

impl<'a, T: Columnar> Push<BoxedRef<T::Ref<'a>>> for BoxedContainer<T> {
    fn push(&mut self, item: BoxedRef<T::Ref<'a>>) {
        self.0.push(item.0);
    }
}

impl<'a, T: Columnar> Push<&'a Boxed<T>> for BoxedContainer<T> {
    fn push(&mut self, item: &'a Boxed<T>) {
        self.0.push(&*item.0);
    }
}

#[cfg(feature = "proptest")]
impl<T> Arbitrary for Boxed<T>
where
    Box<T>: std::fmt::Debug + Arbitrary + 'static,
{
    type Parameters = <Box<T> as Arbitrary>::Parameters;
    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        Box::<T>::arbitrary_with(args).prop_map(Boxed).boxed()
    }
    type Strategy = BoxedStrategy<Self>;
}

impl Arbitrary for Boxed<str> {
    type Parameters = <Box<str> as Arbitrary>::Parameters;

    fn arbitrary_with(args: Self::Parameters) -> Self::Strategy {
        Box::<str>::arbitrary_with(args).prop_map(Boxed).boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
