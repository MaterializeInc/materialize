//! Organize streams of data into sorted chunks.

use std::collections::VecDeque;

use timely::Container;
use timely::container::{ContainerBuilder, DrainContainer, PushInto, SizableContainer};

use crate::consolidation::Consolidate;

/// Chunk a stream of containers into chains of vectors.
pub struct ContainerChunker<Output> {
    pending: Output,
    ready: VecDeque<Output>,
    empty: Output,
}

impl<Output: Default> Default for ContainerChunker<Output> {
    fn default() -> Self {
        Self {
            pending: Output::default(),
            ready: VecDeque::default(),
            empty: Output::default(),
        }
    }
}

impl<'a, Input, Output> PushInto<&'a mut Input> for ContainerChunker<Output>
where
    Input: DrainContainer,
    Output: Default
        + SizableContainer
        + Consolidate
        + PushInto<Input::Item<'a>>,
{
    fn push_into(&mut self, container: &'a mut Input) {
        self.pending.ensure_capacity(&mut None);

        for item in container.drain() {
            self.pending.push_into(item);
            if self.pending.at_capacity() {
                let starting_len = self.pending.len();
                self.pending.consolidate_into(&mut self.empty);
                std::mem::swap(&mut self.pending, &mut self.empty);
                self.empty.clear();
                if self.pending.len() > starting_len / 2 {
                    // Note that we're pushing non-full containers, which is a deviation from
                    // other implementation. The reason for this is that we cannot extract
                    // partial data from `this.pending`. We should revisit this in the future.
                    self.ready.push_back(std::mem::take(&mut self.pending));
                }
            }
        }
    }
}

impl<Output> ContainerBuilder for ContainerChunker<Output>
where
    Output: SizableContainer + Consolidate + Container,
{
    type Container = Output;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = ready;
            Some(&mut self.empty)
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.pending.is_empty() {
            self.pending.consolidate_into(&mut self.empty);
            std::mem::swap(&mut self.pending, &mut self.empty);
            self.empty.clear();
            if !self.pending.is_empty() {
                self.ready.push_back(std::mem::take(&mut self.pending));
            }
        }
        if let Some(ready) = self.ready.pop_front() {
            self.empty = ready;
            Some(&mut self.empty)
        } else {
            None
        }
    }
}
