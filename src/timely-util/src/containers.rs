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

//! Reusable containers.

pub mod stack;

pub(crate) use alloc::alloc_aligned_zeroed;
pub use alloc::{enable_columnar_lgalloc, set_enable_columnar_lgalloc};
pub use provided_builder::ProvidedBuilder;

mod alloc {
    use mz_ore::region::Region;

    /// Allocate a region of memory with a capacity of at least `len` that is properly aligned
    /// and zeroed. The memory in Regions is always aligned to its content type.
    #[inline]
    pub(crate) fn alloc_aligned_zeroed<T: bytemuck::AnyBitPattern>(len: usize) -> Region<T> {
        if enable_columnar_lgalloc() {
            Region::new_auto_zeroed(len)
        } else {
            Region::new_heap_zeroed(len)
        }
    }

    thread_local! {
        static ENABLE_COLUMNAR_LGALLOC: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    }

    /// Returns `true` if columnar allocations should come from lgalloc.
    #[inline]
    pub fn enable_columnar_lgalloc() -> bool {
        ENABLE_COLUMNAR_LGALLOC.get()
    }

    /// Set whether columnar allocations should come from lgalloc. Applies to future allocations.
    pub fn set_enable_columnar_lgalloc(enabled: bool) {
        ENABLE_COLUMNAR_LGALLOC.set(enabled);
    }
}

mod provided_builder {
    use timely::Container;
    use timely::container::ContainerBuilder;

    /// A container builder that doesn't support pushing elements, and is only suitable for pushing
    /// whole containers at Timely sessions. See [`give_container`] for more information.
    ///
    ///  [`give_container`]: timely::dataflow::channels::pushers::buffer::Session::give_container
    pub struct ProvidedBuilder<C> {
        _marker: std::marker::PhantomData<C>,
    }

    impl<C> Default for ProvidedBuilder<C> {
        fn default() -> Self {
            Self {
                _marker: std::marker::PhantomData,
            }
        }
    }

    impl<C: Container + Clone + 'static> ContainerBuilder for ProvidedBuilder<C> {
        type Container = C;

        #[inline(always)]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            None
        }

        #[inline(always)]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            None
        }
    }
}
