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
pub use offset::MzOffsetOptimized;
pub use tuple::*;

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
    Region<Index = MzIndex>
    + Push<<Self as Region>::Owned>
    + for<'a> Push<&'a <Self as Region>::Owned>
    + for<'a> Push<<Self as Region>::ReadItem<'a>>
    + for<'a> ReserveItems<<Self as Region>::ReadItem<'a>>
    + Clone
    + 'static
{
}

impl<R> MzRegion for R where
    R: Region<Index = MzIndex>
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
    use flatcontainer::{Index, Push, Region, ReserveItems};
    use paste::paste;

    use crate::flatcontainer::{MzIndex, MzRegion, MzRegionPreference};

    /// The macro creates the region implementation for tuples
    macro_rules! tuple_flatcontainer {
    ($($name:ident)+) => (
        paste! {
            impl<$($name: MzRegionPreference),*> MzRegionPreference for ($($name,)*) {
                type Owned = ($($name::Owned,)*);
                type Region = [<MzTuple $($name)* Region >]<$($name::Region,)*>;
            }

            /// A region for a tuple.
            #[allow(non_snake_case)]
            #[derive(Default, Debug)]
            #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
            pub struct [<MzTuple $($name)* Region >]<$($name),*> {
                $([<container $name>]: $name),*
            }

            #[allow(non_snake_case)]
            impl<$($name: MzRegion),*> Clone for [<MzTuple $($name)* Region>]<$($name),*>
            where
               $(<$name as Region>::Index: Index),*
            {
                fn clone(&self) -> Self {
                    Self {
                        $([<container $name>]: self.[<container $name>].clone(),)*
                    }
                }

                fn clone_from(&mut self, source: &Self) {
                    $(self.[<container $name>].clone_from(&source.[<container $name>]);)*
                }
            }

            #[allow(non_snake_case)]
            impl<$($name: MzRegion),*> Region for [<MzTuple $($name)* Region>]<$($name),*>
            where
               $(<$name as Region>::Index: Index),*
            {
                type Owned = ($($name::Owned,)*);
                type ReadItem<'a> = ($($name::ReadItem<'a>,)*) where Self: 'a;

                type Index = MzIndex;

                #[inline]
                fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
                where
                    Self: 'a,
                {
                    Self {
                        $([<container $name>]: $name::merge_regions(regions.clone().map(|r| &r.[<container $name>]))),*
                    }
                }

                #[inline] fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
                    (
                        $(self.[<container $name>].index(index),)*
                    )
                }

                #[inline(always)]
                fn reserve_regions<'a, It>(&mut self, regions: It)
                where
                    Self: 'a,
                    It: Iterator<Item = &'a Self> + Clone,
                {
                    $(self.[<container $name>].reserve_regions(regions.clone().map(|r| &r.[<container $name>]));)*
                }

                #[inline(always)]
                fn clear(&mut self) {
                    $(self.[<container $name>].clear();)*
                }

                #[inline]
                fn heap_size<Fn: FnMut(usize, usize)>(&self, mut callback: Fn) {
                    $(self.[<container $name>].heap_size(&mut callback);)*
                }

                #[inline]
                fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> where Self: 'a {
                    let ($($name,)*) = item;
                    (
                        $($name::reborrow($name),)*
                    )
                }
            }

            #[allow(non_camel_case_types)]
            #[allow(non_snake_case)]
            impl<$($name, [<$name _C>]: MzRegion ),*> Push<($($name,)*)> for [<MzTuple $($name)* Region>]<$([<$name _C>]),*>
            where
                $([<$name _C>]: Push<$name>),*
            {
                #[inline]
                fn push(&mut self, item: ($($name,)*))
                    -> <[<MzTuple $($name)* Region>]<$([<$name _C>]),*> as Region>::Index {
                    let ($($name,)*) = item;
                    $(let _index = self.[<container $name>].push($name);)*
                    _index
                }
            }

            #[allow(non_camel_case_types)]
            #[allow(non_snake_case)]
            impl<'a, $($name, [<$name _C>]),*> Push<&'a ($($name,)*)> for [<MzTuple $($name)* Region>]<$([<$name _C>]),*>
            where
                $([<$name _C>]: MzRegion + Push<&'a $name>),*
            {
                #[inline]
                fn push(&mut self, item: &'a ($($name,)*))
                    -> <[<MzTuple $($name)* Region>]<$([<$name _C>]),*> as Region>::Index {
                    let ($($name,)*) = item;
                    $(let _index = self.[<container $name>].push($name);)*
                    _index
                }
            }

            #[allow(non_camel_case_types)]
            #[allow(non_snake_case)]
            impl<'a, $($name, [<$name _C>]),*> ReserveItems<&'a ($($name,)*)> for [<MzTuple $($name)* Region>]<$([<$name _C>]),*>
            where
                $([<$name _C>]: MzRegion + ReserveItems<&'a $name>),*
            {
                #[inline]
                fn reserve_items<It>(&mut self, items: It)
                where
                    It: Iterator<Item = &'a ($($name,)*)> + Clone,
                {
                        tuple_flatcontainer!(reserve_items self items $($name)* @ 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31);
                }
            }

            #[allow(non_camel_case_types)]
            #[allow(non_snake_case)]
            impl<$($name, [<$name _C>]),*> ReserveItems<($($name,)*)> for [<MzTuple $($name)* Region>]<$([<$name _C>]),*>
            where
                $([<$name _C>]: MzRegion + ReserveItems<$name>),*
            {
                #[inline]
                fn reserve_items<It>(&mut self, items: It)
                where
                    It: Iterator<Item = ($($name,)*)> + Clone,
                {
                        tuple_flatcontainer!(reserve_items_owned self items $($name)* @ 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31);
                }
            }
        }
    );
    (reserve_items $self:ident $items:ident $name0:ident $($name:ident)* @ $num0:tt $($num:tt)*) => {
        paste! {
            $self.[<container $name0>].reserve_items($items.clone().map(|i| &i.$num0));
            tuple_flatcontainer!(reserve_items $self $items $($name)* @ $($num)*);
        }
    };
    (reserve_items $self:ident $items:ident @ $($num:tt)*) => {};
    (reserve_items_owned $self:ident $items:ident $name0:ident $($name:ident)* @ $num0:tt $($num:tt)*) => {
        paste! {
            $self.[<container $name0>].reserve_items($items.clone().map(|i| i.$num0));
            tuple_flatcontainer!(reserve_items_owned $self $items $($name)* @ $($num)*);
        }
    };
    (reserve_items_owned $self:ident $items:ident @ $($num:tt)*) => {};
}

    tuple_flatcontainer!(A);
    tuple_flatcontainer!(A B);
    tuple_flatcontainer!(A B C);
    tuple_flatcontainer!(A B C D);
    tuple_flatcontainer!(A B C D E);

    #[cfg(feature = "differential")]
    mod differential {
        use differential_dataflow::difference::Semigroup;
        use differential_dataflow::lattice::Lattice;
        use differential_dataflow::trace::implementations::merge_batcher_flat::RegionUpdate;
        use differential_dataflow::trace::implementations::Update;
        use timely::progress::Timestamp;

        use crate::flatcontainer::{MzRegion, MzTupleABCRegion, MzTupleABRegion};

        impl<KR, VR, TR, RR> Update for MzTupleABCRegion<MzTupleABRegion<KR, VR>, TR, RR>
        where
            KR: MzRegion,
            KR::Owned: Clone + Ord,
            for<'a> KR::ReadItem<'a>: Copy + Ord,
            VR: MzRegion,
            VR::Owned: Clone + Ord,
            for<'a> VR::ReadItem<'a>: Copy + Ord,
            TR: MzRegion,
            TR::Owned: Clone + Lattice + Ord + Timestamp,
            for<'a> TR::ReadItem<'a>: Copy + Ord,
            RR: MzRegion,
            RR::Owned: Clone + Ord + Semigroup,
            for<'a> RR::ReadItem<'a>: Copy + Ord,
        {
            type Key = KR::Owned;
            type Val = VR::Owned;
            type Time = TR::Owned;
            type Diff = RR::Owned;
        }

        impl<KR, VR, TR, RR> RegionUpdate for MzTupleABCRegion<MzTupleABRegion<KR, VR>, TR, RR>
        where
            KR: MzRegion,
            for<'a> KR::ReadItem<'a>: Copy + Ord,
            VR: MzRegion,
            for<'a> VR::ReadItem<'a>: Copy + Ord,
            TR: MzRegion,
            for<'a> TR::ReadItem<'a>: Copy + Ord,
            RR: MzRegion,
            for<'a> RR::ReadItem<'a>: Copy + Ord,
        {
            type Key<'a> = KR::ReadItem<'a> where Self: 'a;
            type Val<'a> = VR::ReadItem<'a> where Self: 'a;
            type Time<'a> = TR::ReadItem<'a> where Self: 'a;
            type TimeOwned = TR::Owned;
            type Diff<'a> = RR::ReadItem<'a> where Self: 'a;
            type DiffOwned = RR::Owned;

            fn into_parts<'a>(
                ((key, val), time, diff): Self::ReadItem<'a>,
            ) -> (Self::Key<'a>, Self::Val<'a>, Self::Time<'a>, Self::Diff<'a>) {
                (key, val, time, diff)
            }

            fn reborrow_key<'b, 'a: 'b>(item: Self::Key<'a>) -> Self::Key<'b>
            where
                Self: 'a,
            {
                KR::reborrow(item)
            }

            fn reborrow_val<'b, 'a: 'b>(item: Self::Val<'a>) -> Self::Val<'b>
            where
                Self: 'a,
            {
                VR::reborrow(item)
            }

            fn reborrow_time<'b, 'a: 'b>(item: Self::Time<'a>) -> Self::Time<'b>
            where
                Self: 'a,
            {
                TR::reborrow(item)
            }

            fn reborrow_diff<'b, 'a: 'b>(item: Self::Diff<'a>) -> Self::Diff<'b>
            where
                Self: 'a,
            {
                RR::reborrow(item)
            }
        }
    }
}

mod copy {
    use crate::region::LgAllocVec;

    use crate::flatcontainer::MzRegionPreference;

    macro_rules! implement_for {
        ($index_type:ty) => {
            impl MzRegionPreference for $index_type {
                type Owned = Self;
                type Region = LgAllocVec<Self>;
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
    type Region = ItemRegion<ConsecutiveIndexPairs<StringRegion>>;
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
    type Region = ItemRegion<OptionRegion<T::Region>>;
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
        fn clone(&self) -> Self {
            Self {
                slices: self.slices.clone(),
                offsets: self.offsets.clone(),
            }
        }

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

    /// TODO
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
        fn clone(&self) -> Self {
            Self {
                inner: self.inner.clone(),
                storage: self.storage.clone(),
            }
        }

        fn clone_from(&mut self, source: &Self) {
            self.inner.clone_from(&source.inner);
            self.storage.clone_from(&source.storage);
        }
    }

    impl<R: Region> Default for ItemRegion<R> {
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

        fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
        where
            Self: 'a,
        {
            Self {
                inner: R::merge_regions(regions.clone().map(|r| &r.inner)),
                storage: LgAllocVec::with_capacity(regions.map(|r| r.storage.len()).sum()),
            }
        }

        fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
            self.inner.index(self.storage[*index])
        }

        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.inner
                .reserve_regions(regions.clone().map(|r| &r.inner));
            self.storage.reserve(regions.map(|r| r.storage.len()).sum());
        }

        fn clear(&mut self) {
            self.inner.clear();
            self.storage.clear();
        }

        fn heap_size<F: FnMut(usize, usize)>(&self, mut callback: F) {
            self.inner.heap_size(&mut callback);
            self.storage.heap_size(callback);
        }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>
        where
            Self: 'a,
        {
            R::reborrow(item)
        }
    }

    impl<R: Region + Push<T>, T> Push<T> for ItemRegion<R> {
        fn push(&mut self, item: T) -> Self::Index {
            let index = self.inner.push(item);
            self.storage.push(index);
            MzIndex(self.storage.len() - 1)
        }
    }

    impl<R: Region + ReserveItems<T>, T> ReserveItems<T> for ItemRegion<R> {
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = T> + Clone,
        {
            self.inner.reserve_items(items.clone());
            self.storage.reserve(items.count());
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

        fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
        where
            Self: 'a,
        {
            Self::with_capacity(regions.map(LgAllocVec::len).sum())
        }

        fn index(&self, index: Self::Index) -> Self::ReadItem<'_> {
            &self[*index]
        }

        fn reserve_regions<'a, I>(&mut self, regions: I)
        where
            Self: 'a,
            I: Iterator<Item = &'a Self> + Clone,
        {
            self.reserve(regions.map(LgAllocVec::len).sum());
        }

        fn clear(&mut self) {
            self.clear();
        }

        fn heap_size<F: FnMut(usize, usize)>(&self, mut callback: F) {
            let size_of_t = std::mem::size_of::<T>();
            callback(self.len() * size_of_t, self.capacity() * size_of_t);
        }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>
        where
            Self: 'a,
        {
            item
        }
    }

    impl<T> Storage<T> for LgAllocVec<T> {
        fn with_capacity(capacity: usize) -> Self {
            Self::with_capacity(capacity)
        }

        fn reserve(&mut self, additional: usize) {
            self.reserve(additional);
        }

        fn clear(&mut self) {
            self.clear();
        }

        fn heap_size<F: FnMut(usize, usize)>(&self, callback: F) {
            self.heap_size(callback);
        }

        fn len(&self) -> usize {
            self.len()
        }

        fn is_empty(&self) -> bool {
            self.is_empty()
        }
    }

    impl<T: Copy> IndexContainer<T> for LgAllocVec<T> {
        type Iter<'a> = std::iter::Copied<std::slice::Iter<'a, T>>
        where
            Self: 'a;

        fn index(&self, index: usize) -> T {
            self[index]
        }

        fn push(&mut self, item: T) {
            self.push(item);
        }

        fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I)
        where
            I::IntoIter: ExactSizeIterator,
        {
            for item in iter {
                self.push(item);
            }
        }

        fn iter(&self) -> Self::Iter<'_> {
            self.iter().copied()
        }
    }

    impl<T: Clone> Push<T> for LgAllocVec<T> {
        fn push(&mut self, item: T) -> Self::Index {
            self.push(item);
            MzIndex(self.len() - 1)
        }
    }

    impl<T: Clone> Push<&T> for LgAllocVec<T> {
        fn push(&mut self, item: &T) -> Self::Index {
            self.push(item.clone());
            MzIndex(self.len() - 1)
        }
    }

    impl<T: Clone> Push<&&T> for LgAllocVec<T> {
        fn push(&mut self, item: &&T) -> Self::Index {
            self.push((*item).clone());
            MzIndex(self.len() - 1)
        }
    }

    impl<T: Clone, D> ReserveItems<D> for LgAllocVec<T> {
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
    pub struct MzOffsetOptimized(IndexOptimized);

    impl Storage<MzIndex> for MzOffsetOptimized {
        fn with_capacity(capacity: usize) -> Self {
            Self(IndexOptimized::with_capacity(capacity))
        }

        fn reserve(&mut self, additional: usize) {
            self.0.reserve(additional)
        }

        fn clear(&mut self) {
            self.0.clear();
        }

        fn heap_size<F: FnMut(usize, usize)>(&self, callback: F) {
            self.0.heap_size(callback);
        }

        fn len(&self) -> usize {
            self.0.len()
        }

        fn is_empty(&self) -> bool {
            self.0.is_empty()
        }
    }

    impl IndexContainer<MzIndex> for MzOffsetOptimized {
        type Iter<'a> = MzOffsetOptimizedIter<<IndexOptimized as IndexContainer<usize>>::Iter<'a>>
        where
            Self: 'a;

        fn index(&self, index: usize) -> MzIndex {
            MzIndex(self.0.index(index))
        }

        fn push(&mut self, item: MzIndex) {
            self.0.push(item.0);
        }

        fn extend<I: IntoIterator<Item = MzIndex>>(&mut self, iter: I)
        where
            I::IntoIter: ExactSizeIterator,
        {
            self.0.extend(iter.into_iter().map(|item| item.0));
        }

        fn iter(&self) -> Self::Iter<'_> {
            MzOffsetOptimizedIter(self.0.iter())
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

        fn next(&mut self) -> Option<Self::Item> {
            self.0.next().map(MzIndex)
        }
    }
}
