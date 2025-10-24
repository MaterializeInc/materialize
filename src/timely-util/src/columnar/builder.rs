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

//! A container builder for columns.

use std::collections::VecDeque;

use columnar::bytes::{EncodeDecode, Indexed};
use columnar::{Clear, Columnar, Len, Push};
use timely::container::PushInto;
use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};

use crate::columnar::Column;

/// A container builder for `Column<C>`.
pub struct ColumnBuilder<C: Columnar> {
    /// Container that we're writing to.
    current: C::Container,
    /// Finished container that we presented to callers of extract/finish.
    ///
    /// We don't recycle the column because for extract, it's not typed, and after calls
    /// to finish it'll be `None`.
    finished: Option<Column<C>>,
    /// Completed containers pending to be sent.
    pending: VecDeque<Column<C>>,
}

impl<C: Columnar, T> PushInto<T> for ColumnBuilder<C>
where
    C::Container: Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        self.current.push(item);
        // If there is less than 10% slop with 2MB backing allocations, mint a container.
        use columnar::Borrow;
        let words = Indexed::length_in_words(&self.current.borrow());
        let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
        if round - words < round / 10 {
            /// Move the contents from `current` to an aligned allocation, and push it to `pending`.
            /// The contents must fit in `round` words (u64).
            #[cold]
            fn outlined_align<C>(
                current: &mut C::Container,
                round: usize,
                pending: &mut VecDeque<Column<C>>,
            ) where
                C: Columnar,
            {
                let mut alloc = crate::containers::alloc_aligned_zeroed(round);
                let writer = std::io::Cursor::new(bytemuck::cast_slice_mut(&mut alloc[..]));
                Indexed::write(writer, &current.borrow()).unwrap();
                pending.push_back(Column::Align(alloc));
                current.clear();
            }

            outlined_align(&mut self.current, round, &mut self.pending);
        }
    }
}

impl<C: Columnar> Default for ColumnBuilder<C> {
    #[inline]
    fn default() -> Self {
        ColumnBuilder {
            current: Default::default(),
            finished: None,
            pending: Default::default(),
        }
    }
}

impl<C: Columnar> ContainerBuilder for ColumnBuilder<C>
where
    C::Container: Clone,
{
    type Container = Column<C>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(container) = self.pending.pop_front() {
            self.finished = Some(container);
            self.finished.as_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            self.pending
                .push_back(Column::Typed(std::mem::take(&mut self.current)));
        }
        self.finished = self.pending.pop_front();
        self.finished.as_mut()
    }
}

impl<C: Columnar> LengthPreservingContainerBuilder for ColumnBuilder<C> where C::Container: Clone {}
