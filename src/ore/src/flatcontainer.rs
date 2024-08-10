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

//! Flat container utilities

use flatcontainer::impls::deduplicate::ConsecutiveIndexPairs;
use flatcontainer::{OptionRegion, Push, Region, ReserveItems, StringRegion};
use serde::{Deserialize, Serialize};

pub use item::ItemRegion;
pub use offset::MzIndexOptimized;

/// Associate a type with a flat container region.
pub trait MzRegionPreference: 'static {
    /// The owned type of the container.
    type Owned;
    /// A region that can hold `Self`.
    type Region: MzRegion<Owned = Self::Owned>;
}

/// TODO
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MzIndex(usize);

impl std::ops::Deref for MzIndex {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// TODO
pub trait MzRegion:
    Region
    + Push<<Self as Region>::Owned>
    + for<'a> Push<&'a <Self as Region>::Owned>
    + for<'a> Push<<Self as Region>::ReadItem<'a>>
    + for<'a> ReserveItems<<Self as Region>::ReadItem<'a>>
    + Clone
    + 'static
{
}

impl<R> MzRegion for R where
    R: Region
        + Push<<R as Region>::Owned>
        + for<'a> Push<&'a <Self as Region>::Owned>
        + for<'a> Push<<Self as Region>::ReadItem<'a>>
        + for<'a> ReserveItems<<Self as Region>::ReadItem<'a>>
        + Clone
        + 'static
{
}

/// Opinion indicating that the contents of a collection should be stored in an
/// [`OwnedRegion`](flatcontainer::OwnedRegion). This is most useful to force types to a region
/// that doesn't copy individual elements to a nested region, like the
/// [`SliceRegion`](flatcontainer::SliceRegion) does.
#[derive(Debug)]
pub struct OwnedRegionOpinion<T>(std::marker::PhantomData<T>);

mod tuple {
    use flatcontainer::impls::tuple::*;
    use paste::paste;

    use crate::flatcontainer::MzRegionPreference;

    /// The macro creates the region implementation for tuples
    macro_rules! tuple_flatcontainer {
        ($($name:ident)+) => (paste! {
            impl<$($name: MzRegionPreference),*> MzRegionPreference for ($($name,)*) {
                type Owned = ($($name::Owned,)*);
                type Region = [<Tuple $($name)* Region >]<$($name::Region,)*>;
            }
        });
    }

    tuple_flatcontainer!(A);
    tuple_flatcontainer!(A B);
    tuple_flatcontainer!(A B C);
    tuple_flatcontainer!(A B C D);
    tuple_flatcontainer!(A B C D E);
}

mod copy {
    use flatcontainer::MirrorRegion;

    use crate::flatcontainer::MzRegionPreference;

    macro_rules! implement_for {
        ($index_type:ty) => {
            impl MzRegionPreference for $index_type {
                type Owned = Self;
                type Region = MirrorRegion<Self>;
            }
        };
    }

    implement_for!(());
    implement_for!(bool);
    implement_for!(char);

    implement_for!(u8);
    implement_for!(u16);
    implement_for!(u32);
    implement_for!(u64);
    implement_for!(u128);
    implement_for!(usize);

    implement_for!(i8);
    implement_for!(i16);
    implement_for!(i32);
    implement_for!(i64);
    implement_for!(i128);
    implement_for!(isize);

    implement_for!(f32);
    implement_for!(f64);

    implement_for!(std::num::Wrapping<i8>);
    implement_for!(std::num::Wrapping<i16>);
    implement_for!(std::num::Wrapping<i32>);
    implement_for!(std::num::Wrapping<i64>);
    implement_for!(std::num::Wrapping<i128>);
    implement_for!(std::num::Wrapping<isize>);

    implement_for!(std::time::Duration);
}

impl MzRegionPreference for String {
    type Owned = String;
    type Region = ConsecutiveIndexPairs<StringRegion>;
}

mod vec {
    use crate::flatcontainer::lgalloc::LgAllocOwnedRegion;
    use crate::flatcontainer::{MzRegionPreference, OwnedRegionOpinion};

    impl<T: Clone + 'static> MzRegionPreference for OwnedRegionOpinion<Vec<T>> {
        type Owned = Vec<T>;
        type Region = LgAllocOwnedRegion<T>;
    }
}

impl<T: MzRegionPreference> MzRegionPreference for Option<T> {
    type Owned = <OptionRegion<T::Region> as Region>::Owned;
    type Region = OptionRegion<T::Region>;
}

mod lgalloc {
    //! A region that stores slices of clone types in lgalloc

    use crate::flatcontainer::MzIndex;
    use crate::region::LgAllocVec;
    use flatcontainer::impls::index::{IndexContainer, IndexOptimized};
    use flatcontainer::impls::storage::Storage;
    use flatcontainer::{Push, PushIter, Region, ReserveItems};

    /// A container for owned types.
    ///
    /// The container can absorb any type, and stores an owned version of the type, similarly to what
    /// vectors do. We recommend using this container for copy types, but there is no restriction in
    /// the implementation, and in fact it can correctly store owned values, although any data owned
    /// by `T` is regular heap-allocated data, and not contained in regions.
    ///
    /// # Examples
    ///
    /// ```
    /// use flatcontainer::{Push, OwnedRegion, Region};
    /// let mut r = <OwnedRegion<_>>::default();
    ///
    /// let panagram_en = "The quick fox jumps over the lazy dog";
    /// let panagram_de = "Zwölf Boxkämpfer jagen Viktor quer über den großen Sylter Deich";
    ///
    /// let en_index = r.push(panagram_en.as_bytes());
    /// let de_index = r.push(panagram_de.as_bytes());
    ///
    /// assert_eq!(panagram_de.as_bytes(), r.index(de_index));
    /// assert_eq!(panagram_en.as_bytes(), r.index(en_index));
    /// ```
    #[derive(Debug)]
    pub struct LgAllocOwnedRegion<T> {
        slices: LgAllocVec<T>,
        offsets: IndexOptimized,
    }

    impl<T: Clone> Clone for LgAllocOwnedRegion<T> {
        #[inline]
        fn clone(&self) -> Self {
            Self {
                slices: self.slices.clone(),
                offsets: self.offsets.clone(),
            }
        }

        #[inline]
        fn clone_from(&mut self, source: &Self) {
            self.slices.clone_from(&source.slices);
            self.offsets.clone_from(&source.offsets);
        }
    }

    impl<T> Region for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        type Owned = <[T] as ToOwned>::Owned;
        type ReadItem<'a> = &'a [T] where Self: 'a;
        type Index = MzIndex;

        #[inline]
        fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
        where
            Self: 'a,
        {
            let mut this = Self {
                slices: LgAllocVec::with_capacity(regions.map(|r| r.slices.len()).sum()),
                offsets: IndexOptimized::default(),
            };
            this.offsets.push(0);
            this
        }

        #[inline]
        fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
            let start = self.offsets.index(*index);
            let end = self.offsets.index(*index + 1);
            &self.slices[start..end]
        }

        #[inline]
        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.slices.reserve(regions.map(|r| r.slices.len()).sum());
        }

        #[inline]
        fn clear(&mut self) {
            self.slices.clear();
            self.offsets.clear();
            self.offsets.push(0);
        }

        #[inline]
        fn heap_size<F: FnMut(usize, usize)>(&self, mut callback: F) {
            let size_of_t = std::mem::size_of::<T>();
            callback(
                self.slices.len() * size_of_t,
                self.slices.capacity() * size_of_t,
            );
        }

        #[inline]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>
        where
            Self: 'a,
        {
            item
        }
    }

    impl<T> Default for LgAllocOwnedRegion<T> {
        #[inline]
        fn default() -> Self {
            let mut this = Self {
                slices: LgAllocVec::default(),
                offsets: IndexOptimized::default(),
            };
            this.offsets.push(0);
            this
        }
    }

    impl<T: Clone, const N: usize> Push<&[T; N]> for LgAllocOwnedRegion<T> {
        #[inline]
        fn push(&mut self, item: &[T; N]) -> <LgAllocOwnedRegion<T> as Region>::Index {
            self.slices.extend_from_slice(item);
            self.offsets.push(self.slices.len());
            MzIndex(self.offsets.len() - 2)
        }
    }

    impl<T: Clone, const N: usize> Push<&&[T; N]> for LgAllocOwnedRegion<T> {
        #[inline]
        fn push(&mut self, item: &&[T; N]) -> <LgAllocOwnedRegion<T> as Region>::Index {
            self.push(*item)
        }
    }

    impl<'b, T: Clone, const N: usize> ReserveItems<&'b [T; N]> for LgAllocOwnedRegion<T> {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = &'b [T; N]> + Clone,
        {
            self.slices.reserve(items.map(|i| i.len()).sum());
        }
    }

    impl<T: Clone> Push<&[T]> for LgAllocOwnedRegion<T> {
        #[inline]
        fn push(&mut self, item: &[T]) -> <LgAllocOwnedRegion<T> as Region>::Index {
            self.slices.extend_from_slice(item);
            self.offsets.push(self.slices.len());
            MzIndex(self.offsets.len() - 2)
        }
    }

    impl<T: Clone> Push<&&[T]> for LgAllocOwnedRegion<T>
    where
        for<'a> Self: Push<&'a [T]>,
    {
        #[inline]
        fn push(&mut self, item: &&[T]) -> <LgAllocOwnedRegion<T> as Region>::Index {
            self.push(*item)
        }
    }

    impl<'b, T> ReserveItems<&'b [T]> for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = &'b [T]> + Clone,
        {
            self.slices.reserve(items.map(<[T]>::len).sum());
        }
    }

    impl<T> Push<Vec<T>> for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        #[inline]
        fn push(&mut self, mut item: Vec<T>) -> <LgAllocOwnedRegion<T> as Region>::Index {
            self.slices.append(&mut item);
            self.offsets.push(self.slices.len());
            MzIndex(self.offsets.len() - 2)
        }
    }

    impl<T: Clone> Push<&Vec<T>> for LgAllocOwnedRegion<T> {
        #[inline]
        fn push(&mut self, item: &Vec<T>) -> <LgAllocOwnedRegion<T> as Region>::Index {
            self.push(item.as_slice())
        }
    }

    impl<'a, T> ReserveItems<&'a Vec<T>> for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = &'a Vec<T>> + Clone,
        {
            self.reserve_items(items.map(Vec::as_slice));
        }
    }

    impl<T, J: IntoIterator<Item = T>> ReserveItems<PushIter<J>> for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = PushIter<J>> + Clone,
        {
            self.slices
                .reserve(items.flat_map(|i| i.0.into_iter()).count());
        }
    }

    #[cfg(test)]
    mod tests {
        use flatcontainer::{Push, Region, ReserveItems};

        use super::*;

        #[crate::test]
        fn test_copy_ref_ref_array() {
            let mut r = <LgAllocOwnedRegion<u8>>::default();
            ReserveItems::reserve_items(&mut r, std::iter::once(&[1; 4]));
            let index = r.push(&&[1; 4]);
            assert_eq!([1, 1, 1, 1], r.index(index));
        }

        #[crate::test]
        fn test_copy_vec() {
            let mut r = <LgAllocOwnedRegion<u8>>::default();
            ReserveItems::reserve_items(&mut r, std::iter::once(&vec![1; 4]));
            let index = r.push(&vec![1; 4]);
            assert_eq!([1, 1, 1, 1], r.index(index));
            let index = r.push(vec![2; 4]);
            assert_eq!([2, 2, 2, 2], r.index(index));
        }
    }
}

mod item {
    //! A region that stores indexes in lgalloc, converting indexes to [`MzIndex`].
    use flatcontainer::{Push, Region, ReserveItems};

    use crate::flatcontainer::MzIndex;
    use crate::region::LgAllocVec;

    /// A region that stores indexes in lgalloc.
    pub struct ItemRegion<R: Region> {
        inner: R,
        storage: LgAllocVec<R::Index>,
    }

    impl<R: Region> std::fmt::Debug for ItemRegion<R> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ItemRegion").finish_non_exhaustive()
        }
    }

    impl<R: Region + Clone> Clone for ItemRegion<R> {
        #[inline]
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                storage: self.storage.clone(),
            }
        }

        #[inline]
        fn clone_from(&mut self, source: &Self) {
            self.inner.clone_from(&source.inner);
            self.storage.clone_from(&source.storage);
        }
    }

    impl<R: Region> Default for ItemRegion<R> {
        #[inline]
        fn default() -> Self {
            Self {
                inner: R::default(),
                storage: LgAllocVec::default(),
            }
        }
    }

    impl<R: Region> Region for ItemRegion<R> {
        type Owned = R::Owned;
        type ReadItem<'a> = R::ReadItem<'a>
        where
            Self: 'a;
        type Index = MzIndex;

        #[inline]
        fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
        where
            Self: 'a,
        {
            Self {
                inner: R::merge_regions(regions.clone().map(|r| &r.inner)),
                storage: LgAllocVec::with_capacity(regions.map(|r| r.storage.len()).sum()),
            }
        }

        #[inline]
        fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
            self.inner.index(self.storage[*index])
        }

        #[inline]
        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.inner
                .reserve_regions(regions.clone().map(|r| &r.inner));
            self.storage.reserve(regions.map(|r| r.storage.len()).sum());
        }

        #[inline]
        fn clear(&mut self) {
            self.inner.clear();
            self.storage.clear();
        }

        #[inline]
        fn heap_size<F: FnMut(usize, usize)>(&self, mut callback: F) {
            self.inner.heap_size(&mut callback);
            self.storage.heap_size(callback);
        }

        #[inline]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>
        where
            Self: 'a,
        {
            R::reborrow(item)
        }
    }

    impl<R: Region + Push<T>, T> Push<T> for ItemRegion<R> {
        #[inline]
        fn push(&mut self, item: T) -> Self::Index {
            let index = self.inner.push(item);
            self.storage.push(index);
            MzIndex(self.storage.len() - 1)
        }
    }

    impl<R: Region + ReserveItems<T>, T> ReserveItems<T> for ItemRegion<R> {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = T> + Clone,
        {
            self.inner.reserve_items(items.clone());
            self.storage.reserve(items.count());
        }
    }

    #[cfg(feature = "differential")]
    mod differential {
        use differential_dataflow::trace::implementations::merge_batcher_flat::RegionUpdate;
        use differential_dataflow::trace::implementations::Update;

        use crate::flatcontainer::{ItemRegion, MzRegion};

        impl<UR> Update for ItemRegion<UR>
        where
            UR: Update + MzRegion,
            UR::Owned: Clone + Ord,
            for<'a> UR::ReadItem<'a>: Copy + Ord,
        {
            type Key = UR::Key;
            type Val = UR::Val;
            type Time = UR::Time;
            type Diff = UR::Diff;
        }

        impl<UR> RegionUpdate for ItemRegion<UR>
        where
            UR: RegionUpdate + MzRegion,
            for<'a> UR::ReadItem<'a>: Copy + Ord,
        {
            type Key<'a> = UR::Key<'a> where Self: 'a;
            type Val<'a> = UR::Val<'a> where Self: 'a;
            type Time<'a> = UR::Time<'a> where Self: 'a;
            type TimeOwned = UR::TimeOwned;
            type Diff<'a> = UR::Diff<'a> where Self: 'a;
            type DiffOwned = UR::DiffOwned;

            #[inline]
            fn into_parts<'a>(
                item: Self::ReadItem<'a>,
            ) -> (Self::Key<'a>, Self::Val<'a>, Self::Time<'a>, Self::Diff<'a>) {
                UR::into_parts(item)
            }

            #[inline]
            fn reborrow_key<'b, 'a: 'b>(item: Self::Key<'a>) -> Self::Key<'b>
            where
                Self: 'a,
            {
                UR::reborrow_key(item)
            }

            #[inline]
            fn reborrow_val<'b, 'a: 'b>(item: Self::Val<'a>) -> Self::Val<'b>
            where
                Self: 'a,
            {
                UR::reborrow_val(item)
            }

            #[inline]
            fn reborrow_time<'b, 'a: 'b>(item: Self::Time<'a>) -> Self::Time<'b>
            where
                Self: 'a,
            {
                UR::reborrow_time(item)
            }

            #[inline]
            fn reborrow_diff<'b, 'a: 'b>(item: Self::Diff<'a>) -> Self::Diff<'b>
            where
                Self: 'a,
            {
                UR::reborrow_diff(item)
            }
        }
    }
}

mod lgallocvec {
    //! A vector-like structure that stores its contents in lgalloc.

    use crate::flatcontainer::MzIndex;
    use crate::region::LgAllocVec;
    use flatcontainer::impls::index::IndexContainer;
    use flatcontainer::impls::storage::Storage;
    use flatcontainer::{Push, Region, ReserveItems};

    impl<T: Clone> Region for LgAllocVec<T> {
        type Owned = T;
        type ReadItem<'a> = &'a T where Self: 'a;
        type Index = MzIndex;

        #[inline]
        fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
        where
            Self: 'a,
        {
            Self::with_capacity(regions.map(LgAllocVec::len).sum())
        }

        #[inline]
        fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
            &self[*index]
        }

        #[inline]
        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.reserve(regions.map(LgAllocVec::len).sum());
        }

        #[inline]
        fn clear(&mut self) {
            self.clear();
        }

        #[inline]
        fn heap_size<F: FnMut(usize, usize)>(&self, mut callback: F) {
            let size_of_t = std::mem::size_of::<T>();
            callback(self.len() * size_of_t, self.capacity() * size_of_t);
        }

        #[inline]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>
        where
            Self: 'a,
        {
            item
        }
    }

    impl<T> Storage<T> for LgAllocVec<T> {
        #[inline]
        fn with_capacity(capacity: usize) -> Self {
            Self::with_capacity(capacity)
        }

        #[inline]
        fn reserve(&mut self, additional: usize) {
            self.reserve(additional);
        }

        #[inline]
        fn clear(&mut self) {
            self.clear();
        }

        #[inline]
        fn heap_size<F: FnMut(usize, usize)>(&self, callback: F) {
            self.heap_size(callback);
        }

        #[inline]
        fn len(&self) -> usize {
            self.len()
        }

        #[inline]
        fn is_empty(&self) -> bool {
            self.is_empty()
        }
    }

    impl<T: Copy> IndexContainer<T> for LgAllocVec<T> {
        type Iter<'a> = std::iter::Copied<std::slice::Iter<'a, T>>
        where
            Self: 'a;

        #[inline]
        fn index(&self, index: usize) -> T {
            self[index]
        }

        #[inline]
        fn push(&mut self, item: T) {
            self.push(item);
        }

        #[inline]
        fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
        where
            I::IntoIter: ExactSizeIterator,
        {
            for item in iter {
                self.push(item);
            }
        }

        #[inline]
        fn iter(&self) -> Self::Iter<'_> {
            self.iter().copied()
        }
    }

    impl<T: Clone> Push<T> for LgAllocVec<T> {
        #[inline]
        fn push(&mut self, item: T) -> Self::Index {
            self.push(item);
            MzIndex(self.len() - 1)
        }
    }

    impl<T: Clone> Push<&T> for LgAllocVec<T> {
        #[inline]
        fn push(&mut self, item: &T) -> Self::Index {
            self.push(item.clone());
            MzIndex(self.len() - 1)
        }
    }

    impl<T: Clone> Push<&&T> for LgAllocVec<T> {
        #[inline]
        fn push(&mut self, item: &&T) -> Self::Index {
            self.push((*item).clone());
            MzIndex(self.len() - 1)
        }
    }

    impl<T: Clone, D> ReserveItems<D> for LgAllocVec<T> {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = D> + Clone,
        {
            self.reserve(items.count());
        }
    }

    #[cfg(test)]
    mod tests {
        #[crate::test]
        fn vec() {
            use flatcontainer::{Push, Region, ReserveItems};

            use crate::region::LgAllocVec;

            let mut region = LgAllocVec::<u32>::default();
            let index = <_ as Push<_>>::push(&mut region, 42);
            assert_eq!(region.index(index), &42);

            let mut region = LgAllocVec::<u32>::default();
            let i0 = <_ as Push<_>>::push(&mut region, 42);
            let i1 = <_ as Push<_>>::push(&mut region, 43);
            let i2 = <_ as Push<_>>::push(&mut region, 44);
            region.reserve_items([1, 2, 3].iter());
            assert_eq!(region.index(i0), &42);
            assert_eq!(region.index(i1), &43);
            assert_eq!(region.index(i2), &44);
        }
    }
}

mod offset {
    use crate::flatcontainer::MzIndex;
    use flatcontainer::impls::index::{IndexContainer, IndexOptimized};
    use flatcontainer::impls::storage::Storage;

    /// TODO
    #[derive(Default, Clone, Debug)]
    pub struct MzIndexOptimized(IndexOptimized);

    impl Storage<MzIndex> for MzIndexOptimized {
        #[inline]
        fn with_capacity(capacity: usize) -> Self {
            Self(IndexOptimized::with_capacity(capacity))
        }

        #[inline]
        fn reserve(&mut self, additional: usize) {
            self.0.reserve(additional)
        }

        #[inline]
        fn clear(&mut self) {
            self.0.clear();
        }

        #[inline]
        fn heap_size<F: FnMut(usize, usize)>(&self, callback: F) {
            self.0.heap_size(callback);
        }

        #[inline]
        fn len(&self) -> usize {
            self.0.len()
        }

        #[inline]
        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    impl IndexContainer<MzIndex> for MzIndexOptimized {
        type Iter<'a> = MzOffsetOptimizedIter<<IndexOptimized as IndexContainer<usize>>::Iter<'a>>
        where
            Self: 'a;

        #[inline]
        fn index(&self, index: usize) -> MzIndex {
            MzIndex(self.0.index(index))
        }

        #[inline]
        fn push(&mut self, item: MzIndex) {
            self.0.push(item.0);
        }

        #[inline]
        fn extend<I: IntoIterator<Item = MzIndex>>(&mut self, iter: I)
        where
            I::IntoIter: ExactSizeIterator,
        {
            self.0.extend(iter.into_iter().map(|item| item.0));
        }

        #[inline]
        fn iter(&self) -> Self::Iter<'_> {
            MzOffsetOptimizedIter(self.0.iter())
        }
    }

    impl<'a> IntoIterator for &'a MzIndexOptimized {
        type Item = MzIndex;
        type IntoIter = MzOffsetOptimizedIter<<IndexOptimized as IndexContainer<usize>>::Iter<'a>>;

        fn into_iter(self) -> Self::IntoIter {
            self.iter()
        }
    }

    /// TODO
    #[derive(Clone, Copy, Debug)]
    pub struct MzOffsetOptimizedIter<I>(I);

    impl<I> Iterator for MzOffsetOptimizedIter<I>
    where
        I: Iterator<Item = usize>,
    {
        type Item = MzIndex;

        #[inline]
        fn next(&mut self) -> Option<Self::Item> {
            self.0.next().map(MzIndex)
        }
    }
}
