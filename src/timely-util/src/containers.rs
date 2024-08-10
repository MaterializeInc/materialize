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

use timely::container::flatcontainer::impls::index::IndexContainer;
use timely::container::flatcontainer::{FlatStack, Push, Region};
use timely::container::{CapacityContainer, ContainerBuilder, PushInto};
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
    current: Option<C>,
    /// Emtpy allocation.
    empty: Option<C>,
    /// Completed containers pending to be sent.
    pending: VecDeque<C>,
}

impl<T, R, S> PushInto<T> for PreallocatingCapacityContainerBuilder<FlatStack<R, S>>
where
    R: Region + Push<T> + Clone + 'static,
    S: IndexContainer<R::Index> + Clone + 'static,
    FlatStack<R, S>: CapacityContainer,
{
    #[inline]
    fn push_into(&mut self, item: T) {
        if self.current.is_none() {
            let mut empty = self.empty.take().unwrap_or_default();
            empty.clear();
            self.current = Some(empty);
        }

        let current = self.current.as_mut().unwrap();

        // Ensure capacity
        current.ensure_preferred_capacity();
        // Push item
        current.push(item);

        // Maybe flush
        if current.len() >= FlatStack::<R, S>::preferred_capacity() {
            let pending = std::mem::take(current);
            *current = FlatStack::merge_capacity(std::iter::once(&pending));
            self.pending.push_back(pending);
        }
    }
}

impl<R, S> ContainerBuilder for PreallocatingCapacityContainerBuilder<FlatStack<R, S>>
where
    R: Region + Clone + 'static,
    S: IndexContainer<R::Index> + Clone + 'static,
    FlatStack<R, S>: CapacityContainer,
{
    type Container = FlatStack<R, S>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.empty = Some(self.pending.pop_front()?);
        self.empty.as_mut()
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        let current = self.current.as_mut();
        if current.as_ref().map_or(false, |c| !c.is_empty()) {
            let current = current.unwrap();
            let pending = std::mem::take(current);
            *current = FlatStack::merge_capacity(std::iter::once(&pending));
            self.pending.push_back(pending);
        }
        self.empty = self.pending.pop_front();
        self.empty.as_mut()
    }
}
