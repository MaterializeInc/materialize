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
    use flatcontainer::OwnedRegion;

    use crate::flatcontainer::{MzRegionPreference, OwnedRegionOpinion};

    impl<T: Clone + 'static> MzRegionPreference for OwnedRegionOpinion<Vec<T>> {
        type Owned = Vec<T>;
        type Region = OwnedRegion<T>;
    }
}

impl<T: MzRegionPreference> MzRegionPreference for Option<T> {
    type Owned = <flatcontainer::OptionRegion<T::Region> as Region>::Owned;
    type Region = flatcontainer::OptionRegion<T::Region>;
}
