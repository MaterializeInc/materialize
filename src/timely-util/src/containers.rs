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
use std::marker::PhantomData;
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use timely::Container;

pub mod array;
pub mod stack;

pub struct ConvertingContainerBuilder<CI, CO, I> {
    current: CO,
    outbound: VecDeque<CO>,
    empty: Vec<CO>,
    inner: I,
    _phantom: PhantomData<(CI, CO)>,
}

impl<CI, CO: Default, I> ConvertingContainerBuilder<CI, CO, I> {
    #[inline]
    pub fn new(inner: I) -> Self {
        Self {
            current: CO::default(),
            outbound: VecDeque::new(),
            empty: Vec::default(),
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<CI, CO: Default, I: Default> Default for ConvertingContainerBuilder<CI, CO, I> {
    #[inline]
    fn default() -> Self {
        Self::new(I::default())
    }
}

impl<CI, CO, I, D> PushInto<D> for ConvertingContainerBuilder<CI, CO, I>
where
    I: PushInto<D>,
{
    fn push_into(&mut self, item: D) {
        self.inner.push_into(item);
    }
}

impl<CI, CO, I> ConvertingContainerBuilder<CI, CO, I>
where
    CI: Container + 'static,
    CO: SizableContainer + Clone + for<'a> PushInto<CI::Item<'a>> + 'static,
    I: ContainerBuilder<Container = CI>,
{
    fn process(&mut self, mut logic: impl FnMut(&mut I) -> Option<&mut CI>) {
        while let Some(container) = logic(&mut self.inner) {
            for item in container.drain() {
                if self.current.at_capacity() {
                    if !self.current.is_empty() {
                        self.outbound.push_back(std::mem::take(&mut self.current));
                    }
                    self.current = CO::default();
                    self.current.ensure_capacity(&mut self.empty.pop());
                }
                self.current.push_into(item);
            }
        }
    }
}

impl<CI, CO, I> ContainerBuilder for ConvertingContainerBuilder<CI, CO, I>
where
    CI: Container + 'static,
    CO: SizableContainer + Clone + for<'a> PushInto<CI::Item<'a>> + 'static,
    I: ContainerBuilder<Container = CI>,
{
    type Container = CO;

    #[inline]
    fn extract(&mut self) -> Option<&mut CO> {
        self.process(|inner| inner.extract());
        if let Some(container) = self.outbound.pop_front() {
            self.empty.push(container);
            self.empty.last_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut CO> {
        self.process(|inner| inner.finish());
        if !self.current.is_empty() {
            self.outbound.push_back(std::mem::take(&mut self.current));
            self.empty.truncate(2);
        }
        self.extract()
    }
}
