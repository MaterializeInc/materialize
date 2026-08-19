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

use columnar::{Clear, Columnar, Len, Push};
use timely::container::PushInto;
use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};

use crate::columnar::Column;
use crate::columnar::align_buffer::{AlignBuffer, Origin};

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
    /// The origin stamped on every buffer this builder mints.
    origin: Origin,
}

impl<C: Columnar> ColumnBuilder<C> {
    /// A builder that stamps its buffers with `origin` rather than the
    /// [`Origin::Ship`] that [`Default`] uses.
    ///
    /// For builders whose chunks are retained rather than sent, so their
    /// lifetimes do not land in the same metric series as bodies in flight on a
    /// dataflow edge. Timely constructs container builders through `Default`,
    /// which is why shipping is the default and retention is the opt-in.
    pub fn with_origin(origin: Origin) -> Self {
        ColumnBuilder {
            origin,
            ..Default::default()
        }
    }
}

impl<C: Columnar, T> PushInto<T> for ColumnBuilder<C>
where
    C::Container: Push<T>,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        self.current.push(item);
        // Mint a container once the serialized size reaches the ship threshold.
        use columnar::Borrow;
        if crate::columnar::at_serialized_capacity(&self.current.borrow()) {
            /// Move the contents from `current` into a fitting [`AlignBuffer`]
            /// and push it to `pending`.
            #[cold]
            fn outlined_align<C>(
                current: &mut C::Container,
                pending: &mut VecDeque<Column<C>>,
                origin: Origin,
            ) where
                C: Columnar,
            {
                use columnar::{Borrow, Len};
                let view = current.borrow();
                let buffer = AlignBuffer::encode(origin, view.len(), &view);
                pending.push_back(Column::Align(buffer));
                current.clear();
            }

            outlined_align(&mut self.current, &mut self.pending, self.origin);
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
            origin: Origin::Ship,
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
