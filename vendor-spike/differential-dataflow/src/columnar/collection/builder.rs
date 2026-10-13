//! `ValColBuilder`: the ContainerBuilder that feeds the dataflow input side.
//!
//! Accepts flat `(k, v, t, d)` tuples via `PushInto`; when the internal tuple
//! container reaches a threshold, sorts + forms a `RecordedUpdates` trie and
//! queues it. `finish` produces one final trie from any remaining tuples.

use std::collections::VecDeque;
use columnar::{Columnar, Clear, Len, Push};

use crate::columnar::layout::ColumnarUpdate as Update;
use crate::columnar::updates::UpdatesTyped;
use super::RecordedUpdates;

type TupleContainer<U> = <(<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff) as Columnar>::Container;

/// A container builder that produces `RecordedUpdates` (sorted, consolidated trie + record count).
pub struct ValBuilder<U: Update> {
    /// Container that we're writing to.
    current: TupleContainer<U>,
    /// Empty allocation.
    empty: Option<RecordedUpdates<U>>,
    /// Completed containers pending to be sent.
    pending: VecDeque<RecordedUpdates<U>>,
}

use timely::container::PushInto;
impl<T, U: Update> PushInto<T> for ValBuilder<U> where TupleContainer<U> : Push<T> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.current.push(item);
        if self.current.len() > crate::columnar::LINK_TARGET {
            use columnar::{Borrow, Index};
            let records = self.current.len();
            let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
            refs.sort_unstable();
            let updates = UpdatesTyped::form(refs.into_iter()).into();
            self.pending.push_back(RecordedUpdates { updates, records, consolidated: true });
            self.current.clear();
        }
    }
}

impl<U: Update> Default for ValBuilder<U> {
    fn default() -> Self {
        ValBuilder {
            current: Default::default(),
            empty: None,
            pending: Default::default(),
        }
    }
}

use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
impl<U: Update> ContainerBuilder for ValBuilder<U> {
    type Container = RecordedUpdates<U>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(container) = self.pending.pop_front() {
            self.empty = Some(container);
            self.empty.as_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Self::Container> {
        if !self.current.is_empty() {
            use columnar::{Borrow, Index};
            let records = self.current.len();
            let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
            refs.sort_unstable();
            let updates = UpdatesTyped::form(refs.into_iter()).into();
            self.pending.push_back(RecordedUpdates { updates, records, consolidated: true });
            self.current.clear();
        }
        self.empty = self.pending.pop_front();
        self.empty.as_mut()
    }
}

impl<U: Update> LengthPreservingContainerBuilder for ValBuilder<U> { }
