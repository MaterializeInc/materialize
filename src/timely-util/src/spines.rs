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

//! Columnated arrangement spines for Materialize workloads.
//!
//! These types build on the `columnation` crate and `differential_dataflow`'s
//! arrangement layer to provide memory-efficient `Spine` / `Batcher` / `Builder`
//! aliases used by both `mz-compute` and `mz-storage` for arrangement output.
//!
//! Historically the `Col*` aliases lived in `differential_dataflow` itself
//! (`ColValSpine` / `ColKeySpine` / friends in
//! `trace::implementations::ord_neu`), but DD removed them in
//! [TimelyDataflow/differential-dataflow#715]. Materialize now owns them.
//!
//! [TimelyDataflow/differential-dataflow#715]:
//!     https://github.com/TimelyDataflow/differential-dataflow/pull/715

use std::rc::Rc;

use columnation::Columnation;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::ord_neu::{
    OrdKeyBatch, OrdKeyBuilder, OrdValBatch, OrdValBuilder,
};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::{Layout, Update};
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

use crate::columnation::{ColInternalMerger, ColumnationChunker, ColumnationStack};

pub use self::offset_opt::OffsetOptimized;

/// A spine for generic columnated keys and values.
pub type ColValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<MzStack<((K, V), T, R)>>>>;
/// A batcher for generic columnated keys and values.
pub type ColValBatcher<K, V, T, R> = KeyValBatcher<K, V, T, R>;
/// A builder for generic columnated keys and values.
pub type ColValBuilder<K, V, T, R> =
    RcBuilder<OrdValBuilder<MzStack<((K, V), T, R)>, ColumnationStack<((K, V), T, R)>>>;

/// A spine for generic columnated keys.
pub type ColKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<MzStack<((K, ()), T, R)>>>>;
/// A batcher for generic columnated keys.
pub type ColKeyBatcher<K, T, R> = KeyBatcher<K, T, R>;
/// A builder for generic columnated keys.
pub type ColKeyBuilder<K, T, R> =
    RcBuilder<OrdKeyBuilder<MzStack<((K, ()), T, R)>, ColumnationStack<((K, ()), T, R)>>>;

/// A consolidating batcher for columnated key-only updates.
pub type KeyBatcher<K, T, D> = KeyValBatcher<K, (), T, D>;
/// A consolidating batcher for columnated key-value updates.
pub type KeyValBatcher<K, V, T, D> = MergeBatcher<
    Vec<((K, V), T, D)>,
    ColumnationChunker<((K, V), T, D)>,
    ColInternalMerger<(K, V), T, D>,
>;

/// A [`Layout`] backed by chunked timely stacks (Materialize's columnated container).
pub struct MzStack<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for MzStack<U>
where
    U::Key: Columnation + 'static,
    U::Val: Columnation + 'static,
    U::Time: Columnation,
    U::Diff: Columnation,
{
    type KeyContainer = ColumnationStack<U::Key>;
    type ValContainer = ColumnationStack<U::Val>;
    type TimeContainer = ColumnationStack<U::Time>;
    type DiffContainer = ColumnationStack<U::Diff>;
    type OffsetContainer = OffsetOptimized;
}

mod offset_opt {
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::OffsetList;
    use timely::container::PushInto;

    enum OffsetStride {
        Empty,
        Zero,
        Striding(usize, usize),
        Saturated(usize, usize, usize),
    }

    impl OffsetStride {
        /// Accepts or rejects a newly pushed element.
        #[inline]
        fn push(&mut self, item: usize) -> bool {
            match self {
                OffsetStride::Empty => {
                    if item == 0 {
                        *self = OffsetStride::Zero;
                        true
                    } else {
                        false
                    }
                }
                OffsetStride::Zero => {
                    *self = OffsetStride::Striding(item, 2);
                    true
                }
                OffsetStride::Striding(stride, count) => {
                    if item == *stride * *count {
                        *count += 1;
                        true
                    } else if item == *stride * (*count - 1) {
                        *self = OffsetStride::Saturated(*stride, *count, 1);
                        true
                    } else {
                        false
                    }
                }
                OffsetStride::Saturated(stride, count, reps) => {
                    if item == *stride * (*count - 1) {
                        *reps += 1;
                        true
                    } else {
                        false
                    }
                }
            }
        }

        #[inline]
        fn index(&self, index: usize) -> usize {
            match self {
                OffsetStride::Empty => {
                    panic!("Empty OffsetStride")
                }
                OffsetStride::Zero => 0,
                OffsetStride::Striding(stride, _steps) => *stride * index,
                OffsetStride::Saturated(stride, steps, _reps) => {
                    if index < *steps {
                        *stride * index
                    } else {
                        *stride * (*steps - 1)
                    }
                }
            }
        }

        #[inline]
        fn len(&self) -> usize {
            match self {
                OffsetStride::Empty => 0,
                OffsetStride::Zero => 1,
                OffsetStride::Striding(_stride, steps) => *steps,
                OffsetStride::Saturated(_stride, steps, reps) => *steps + *reps,
            }
        }
    }

    /// A compact offset container that strides over equally-spaced offsets and
    /// spills the rest into an [`OffsetList`].
    pub struct OffsetOptimized {
        strided: OffsetStride,
        spilled: OffsetList,
    }

    impl BatchContainer for OffsetOptimized {
        type Owned = usize;
        type ReadItem<'a> = usize;

        #[inline]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned {
            item
        }

        #[inline]
        fn push_ref(&mut self, item: Self::ReadItem<'_>) {
            self.push_into(item)
        }

        #[inline]
        fn push_own(&mut self, item: &Self::Owned) {
            self.push_into(*item)
        }

        fn clear(&mut self) {
            self.strided = OffsetStride::Empty;
            self.spilled.clear();
        }

        fn with_capacity(_size: usize) -> Self {
            Self {
                strided: OffsetStride::Empty,
                spilled: OffsetList::with_capacity(0),
            }
        }

        fn merge_capacity(_cont1: &Self, _cont2: &Self) -> Self {
            Self {
                strided: OffsetStride::Empty,
                spilled: OffsetList::with_capacity(0),
            }
        }

        #[inline]
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
            item
        }

        #[inline]
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            if index < self.strided.len() {
                self.strided.index(index)
            } else {
                self.spilled.index(index - self.strided.len())
            }
        }

        #[inline]
        fn len(&self) -> usize {
            self.strided.len() + self.spilled.len()
        }
    }

    impl PushInto<usize> for OffsetOptimized {
        #[inline]
        fn push_into(&mut self, item: usize) {
            if !self.spilled.is_empty() {
                self.spilled.push(item);
            } else {
                let inserted = self.strided.push(item);
                if !inserted {
                    self.spilled.push(item);
                }
            }
        }
    }

    impl OffsetOptimized {
        /// Estimates the heap usage of the spilled portion of the offset list.
        pub fn heap_size(&self, callback: impl FnMut(usize, usize)) {
            offset_list_size(&self.spilled, callback);
        }
    }

    /// Helper to compute the size of an [`OffsetList`] in memory.
    #[inline]
    fn offset_list_size(data: &OffsetList, mut callback: impl FnMut(usize, usize)) {
        // Private `vec_size` because we should only use it where data isn't region-allocated.
        // `T: Copy` makes sure the implementation is correct even if types change!
        #[inline(always)]
        fn vec_size<T: Copy>(data: &Vec<T>, mut callback: impl FnMut(usize, usize)) {
            let size_of_t = std::mem::size_of::<T>();
            callback(data.len() * size_of_t, data.capacity() * size_of_t);
        }

        vec_size(&data.smol, &mut callback);
        vec_size(&data.chonk, callback);
    }
}
