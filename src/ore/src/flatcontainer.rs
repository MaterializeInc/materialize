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

use flatcontainer::{Push, Region, ReserveItems};

/// Associate a type with a flat container region.
pub trait MzRegionPreference: 'static {
    /// The owned type of the container.
    type Owned;
    /// A region that can hold `Self`.
    type Region: for<'a> Region<Owned = Self::Owned>
        + Push<Self::Owned>
        + for<'a> Push<<Self::Region as Region>::ReadItem<'a>>
        + for<'a> ReserveItems<<Self::Region as Region>::ReadItem<'a>>;
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

    macro_rules! tuple_flatcontainer {
        ($($name:ident)+) => (
            paste! {
                impl<$($name: MzRegionPreference),*> MzRegionPreference for ($($name,)*) {
                    type Owned = ($($name::Owned,)*);
                    type Region = [<Tuple $($name)* Region >]<$($name::Region,)*>;
                }
            }
        )
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

mod vec {
    use crate::flatcontainer::lgalloc::LgAllocOwnedRegion;
    use crate::flatcontainer::{MzRegionPreference, OwnedRegionOpinion};

    impl<T: Clone + 'static> MzRegionPreference for OwnedRegionOpinion<Vec<T>> {
        type Owned = Vec<T>;
        type Region = LgAllocOwnedRegion<T>;
    }
}

impl<T: MzRegionPreference> MzRegionPreference for Option<T> {
    type Owned = <flatcontainer::OptionRegion<T::Region> as Region>::Owned;
    type Region = flatcontainer::OptionRegion<T::Region>;
}

mod lgalloc {
    //! A region that stores slices of clone types in lgalloc

    use crate::region::LgAllocVec;
    use flatcontainer::{CopyIter, Push, Region, ReserveItems};

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
    }

    impl<T: Clone> Clone for LgAllocOwnedRegion<T> {
        fn clone(&self) -> Self {
            Self {
                slices: self.slices.clone(),
            }
        }

        fn clone_from(&mut self, source: &Self) {
            self.slices.clone_from(&source.slices);
        }
    }

    impl<T> Region for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        type Owned = <[T] as ToOwned>::Owned;
        type ReadItem<'a> = &'a [T] where Self: 'a;
        type Index = (usize, usize);

        #[inline]
        fn merge_regions<'a>(regions: impl Iterator<Item = &'a Self> + Clone) -> Self
        where
            Self: 'a,
        {
            Self {
                slices: LgAllocVec::with_capacity(regions.map(|r| r.slices.len()).sum()),
            }
        }

        #[inline]
        fn index(&self, (start, end): Self::Index) -> Self::ReadItem<'_> {
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
            Self {
                slices: LgAllocVec::default(),
            }
        }
    }

    impl<T: Clone, const N: usize> Push<&[T; N]> for LgAllocOwnedRegion<T> {
        #[inline]
        fn push(&mut self, item: &[T; N]) -> <LgAllocOwnedRegion<T> as Region>::Index {
            let start = self.slices.len();
            self.slices.extend_from_slice(item);
            (start, self.slices.len())
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
            let start = self.slices.len();
            self.slices.extend_from_slice(item);
            (start, self.slices.len())
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
            let start = self.slices.len();
            self.slices.append(&mut item);
            (start, self.slices.len())
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

    impl<T, J: IntoIterator<Item = T>> ReserveItems<CopyIter<J>> for LgAllocOwnedRegion<T>
    where
        [T]: ToOwned,
    {
        #[inline]
        fn reserve_items<I>(&mut self, items: I)
        where
            I: Iterator<Item = CopyIter<J>> + Clone,
        {
            self.slices
                .reserve(items.flat_map(|i| i.0.into_iter()).count());
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::{CopyIter, Push, Region, ReserveItems};

        use super::*;

        #[test]
        fn test_copy_array() {
            let mut r = <LgAllocOwnedRegion<u8>>::default();
            r.reserve_items(std::iter::once(&[1; 4]));
            let index = r.push([1; 4]);
            assert_eq!([1, 1, 1, 1], r.index(index));
        }

        #[test]
        fn test_copy_ref_ref_array() {
            let mut r = <LgAllocOwnedRegion<u8>>::default();
            ReserveItems::reserve_items(&mut r, std::iter::once(&[1; 4]));
            let index = r.push(&&[1; 4]);
            assert_eq!([1, 1, 1, 1], r.index(index));
        }

        #[test]
        fn test_copy_vec() {
            let mut r = <LgAllocOwnedRegion<u8>>::default();
            ReserveItems::reserve_items(&mut r, std::iter::once(&vec![1; 4]));
            let index = r.push(&vec![1; 4]);
            assert_eq!([1, 1, 1, 1], r.index(index));
            let index = r.push(vec![2; 4]);
            assert_eq!([2, 2, 2, 2], r.index(index));
        }

        #[test]
        fn test_copy_iter() {
            let mut r = <LgAllocOwnedRegion<u8>>::default();
            r.reserve_items(std::iter::once(CopyIter(std::iter::repeat(1).take(4))));
            let index = r.push(CopyIter(std::iter::repeat(1).take(4)));
            assert_eq!([1, 1, 1, 1], r.index(index));
        }
    }
}
