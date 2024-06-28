// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Reusable containers.

use std::collections::VecDeque;

use timely::container::flatcontainer::{FlatStack, Push, Region};
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use timely::Container;

pub mod array;
pub mod stack;

/// A container builder that uses length and preferred capacity to chunk data. Preallocates the next
/// container based on the capacity of the previous one once a container is full.
///
/// Ideally, we'd have a `TryPush` trait that would fail if a push would cause a reallocation, but
/// we aren't there yet.
///
/// Maintains a single empty allocation between [`Self::push_into`] and [`Self::extract`], but not
/// across [`Self::finish`] to maintain a low memory footprint.
///
/// Maintains FIFO order.
#[derive(Default, Debug)]
pub struct PreallocatingCapacityContainerBuilder<C> {
    /// Container that we're writing to.
    current: C,
    /// Emtpy allocation.
    empty: Option<C>,
    /// Completed containers pending to be sent.
    pending: VecDeque<C>,
}

impl<T, R> PushInto<T> for PreallocatingCapacityContainerBuilder<FlatStack<R>>
where
    R: Region + Push<T> + Clone + 'static,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        if self.current.capacity() == 0 {
            self.current = self.empty.take().unwrap_or_default();
            // Protect against non-emptied containers.
            self.current.clear();
        }
        // Ensure capacity
        let preferred_capacity = FlatStack::<R>::preferred_capacity();
        if self.current.capacity() < preferred_capacity {
            self.current
                .reserve(preferred_capacity - self.current.len());
        }

        // Push item
        self.current.push(item);

        // Maybe flush
        if self.current.len() == self.current.capacity() {
            let pending = std::mem::take(&mut self.current);
            self.current = FlatStack::merge_capacity(std::iter::once(&pending));
            self.current
                .reserve(preferred_capacity.saturating_sub(self.current.len()));
            self.pending.push_back(pending);
        }
    }
}

impl<R> ContainerBuilder for PreallocatingCapacityContainerBuilder<FlatStack<R>>
where
    R: Region + Clone + 'static,
{
    type Container = FlatStack<R>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.empty = Some(self.pending.pop_front()?);
        self.empty.as_mut()
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            let pending = std::mem::take(&mut self.current);
            self.current = FlatStack::merge_capacity(std::iter::once(&pending));
            let preferred_capacity = FlatStack::<R>::preferred_capacity();
            self.current
                .reserve(preferred_capacity.saturating_sub(self.current.len()));
            self.pending.push_back(pending);
        }
        self.empty = self.pending.pop_front();
        self.empty.as_mut()
    }
}
