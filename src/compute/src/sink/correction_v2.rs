// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics};
use mz_repr::{Diff, Timestamp};
use timely::container::columnation::{Columnation, TimelyStack};
use timely::progress::Antichain;
use timely::PartialOrder;

const CHUNK_SIZE_BYTES: usize = 8 << 10;

/// chains:
///
/// 0: xxxxxxxx
/// 1: xxxx
/// 2: xx
/// 3: x
///
pub(super) struct Correction<D: Columnation> {
    chains: Vec<Chain<D>>,
    since: Antichain<Timestamp>,
    /// Global persist sink metrics.
    _metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    _worker_metrics: SinkWorkerMetrics,
}

impl<D: Clone + Ord + Columnation> Correction<D> {
    /// Construct a new `Correction` instance.
    pub fn new(metrics: SinkMetrics, worker_metrics: SinkWorkerMetrics) -> Self {
        Self {
            chains: Default::default(),
            since: Antichain::from_elem(Timestamp::MIN),
            _metrics: metrics,
            _worker_metrics: worker_metrics,
        }
    }

    /// Insert a batch of updates.
    pub fn insert(&mut self, mut updates: Vec<(D, Timestamp, Diff)>) {
        let Some(since_ts) = self.since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            return;
        };

        for (_, time, _) in &mut updates {
            *time = std::cmp::max(*time, *since_ts);
        }

        self.insert_inner(updates);
    }

    /// Insert a batch of updates, after negating their diffs.
    pub fn insert_negated(&mut self, mut updates: Vec<(D, Timestamp, Diff)>) {
        let Some(since_ts) = self.since.as_option() else {
            // If the since is the empty frontier, discard all updates.
            return;
        };

        for (_, time, diff) in &mut updates {
            *time = std::cmp::max(*time, *since_ts);
            *diff = -*diff;
        }

        self.insert_inner(updates);
    }

    fn insert_inner(&mut self, updates: Vec<(D, Timestamp, Diff)>) {
        // TODO(optimization): append to the last chain if all times in `updates` are greater than
        // all times in that chain

        let chain = Chain::from(updates);
        if !chain.is_empty() {
            self.chains.push(chain);
            self.restore_chain_invariant();
        }
    }

    fn restore_chain_invariant(&mut self) {
        let merge_needed = |chains: &[Chain<_>]| match chains {
            [.., prev, last] => last.len() >= prev.len() / 2,
            _ => false,
        };

        while merge_needed(&self.chains) {
            let a = self.chains.pop().unwrap();
            let b = self.chains.pop().unwrap();
            let merged = merge(a, b, &self.since);
            self.chains.push(merged);
        }
    }

    /// Return consolidated updates before the given `upper`.
    pub fn updates_before(
        &mut self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + '_ {
        // FIXME

        let mut iters = Vec::new();

        if let Some(since_ts) = self.since.as_option() {
            for chain in &self.chains {
                let upper = upper.clone();
                let chain_iter = chain
                    .iter()
                    .filter(move |(_, t, _)| !upper.less_equal(t))
                    .map(|(d, t, r)| (d.clone(), std::cmp::max(t, *since_ts), r));
                iters.push(chain_iter);
            }
        };

        iters.into_iter().flatten()
    }

    /// Return the current since frontier.
    pub fn since(&self) -> &Antichain<Timestamp> {
        &self.since
    }

    /// Advance the since frontier.
    ///
    /// # Panics
    ///
    /// Panics if the given `since` is less than the current since frontier.
    pub fn advance_since(&mut self, since: Antichain<Timestamp>) {
        assert!(PartialOrder::less_equal(&self.since, &since));
        self.since = since;
    }
}

struct Chain<D: Columnation> {
    chunks: Vec<Chunk<D>>,
}

impl<D: Columnation> Default for Chain<D> {
    fn default() -> Self {
        Self {
            chunks: Default::default(),
        }
    }
}

impl<D: Ord + Columnation> From<Vec<(D, Timestamp, Diff)>> for Chain<D> {
    fn from(mut updates: Vec<(D, Timestamp, Diff)>) -> Self {
        consolidate(&mut updates);

        let mut self_ = Self::default();
        while let Some(update) = updates.pop() {
            self_.push(update);
        }

        self_
    }
}

impl<D: Ord + Columnation> Chain<D> {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    fn len(&self) -> usize {
        self.chunks.len()
    }

    fn push(&mut self, update: (D, Timestamp, Diff)) {
        let (d, t, r) = update;
        self.push_ref((&d, t, r));
    }

    fn push_ref(&mut self, update: (&D, Timestamp, Diff)) {
        // Chains must remain consolidated and ordered by (time, data).
        debug_assert!(self
            .chunks
            .last()
            .and_then(|c| c.last())
            .map_or(true, |(d, t, _)| {
                let (d2, t2, _) = &update;
                (t, d) > (t2, d2)
            }));

        match self.chunks.last_mut() {
            Some(c) if c.capacity_left() => c.push_ref(update),
            Some(_) | None => {
                let mut chunk = Chunk::new();
                chunk.push_ref(update);
                self.chunks.push(chunk);
            }
        }
    }

    fn iter(&self) -> ChainIter<'_, D> {
        ChainIter {
            chunks: &self.chunks,
            chunk_index: 0,
        }
    }
}

struct ChainQueue<D: Columnation> {
    chunks: Vec<Chunk<D>>,
    chunk_index: usize,
}

impl<D: Columnation> From<Chain<D>> for ChainQueue<D> {
    fn from(chain: Chain<D>) -> Self {
        let mut chunks = chain.chunks;
        chunks.reverse();

        Self {
            chunks,
            chunk_index: 0,
        }
    }
}

impl<D: Columnation> ChainQueue<D> {
    fn head(&mut self) -> Option<(&D, Timestamp, Diff)> {
        loop {
            let chunk = self.chunks.last()?;
            if chunk.0.get(self.chunk_index).is_some() {
                break;
            }

            self.chunks.pop();
            self.chunk_index = 0;
        }

        let chunk = self.chunks.last().expect("known to exist");
        let (d, t, r) = &chunk.0[self.chunk_index];

        Some((d, *t, *r))
    }

    fn remove(&mut self) {
        self.chunk_index += 1;
    }

    fn iter(&self) -> ChainIter<'_, D> {
        ChainIter {
            chunks: &self.chunks,
            chunk_index: self.chunk_index,
        }
    }
}

struct ChainIter<'a, D: Columnation> {
    chunks: &'a [Chunk<D>],
    chunk_index: usize,
}

impl<'a, D: Columnation> Iterator for ChainIter<'a, D> {
    type Item = (&'a D, Timestamp, Diff);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (chunk, rest) = self.chunks.split_last()?;
            if let Some((d, t, r)) = chunk.0.get(self.chunk_index) {
                self.chunk_index += 1;
                return Some((d, *t, *r));
            }

            self.chunks = rest;
            self.chunk_index = 0;
        }
    }
}

struct Chunk<D: Columnation>(TimelyStack<(D, Timestamp, Diff)>);

impl<D: Columnation> Default for Chunk<D> {
    fn default() -> Self {
        let capacity = Self::capacity();
        Self(TimelyStack::with_capacity(capacity))
    }
}

impl<D: Columnation> Chunk<D> {
    fn capacity() -> usize {
        let size = std::mem::size_of::<(D, Timestamp, Diff)>();
        std::cmp::max(CHUNK_SIZE_BYTES / size, 1)
    }

    fn new() -> Self {
        let capacity = Self::capacity();
        Self(TimelyStack::with_capacity(capacity))
    }

    fn capacity_left(&self) -> bool {
        self.0.len() < Self::capacity()
    }

    fn last(&self) -> Option<&(D, Timestamp, Diff)> {
        self.0.last()
    }

    fn push_ref(&mut self, update: (&D, Timestamp, Diff)) {
        let (d, t, r) = update;
        self.0.copy_destructured(d, &t, &r);
    }
}

fn consolidate<D: Ord>(updates: &mut Vec<(D, Timestamp, Diff)>) {
    if updates.is_empty() {
        return;
    }

    let diff = |update: &(_, _, Diff)| update.2;

    updates.sort_unstable_by(|(d1, t1, _), (d2, t2, _)| (t1, d1).cmp(&(t2, d2)));

    let mut offset = 0;
    let mut accum = diff(&updates[0]);

    for idx in 1..updates.len() {
        let this = &updates[idx];
        let prev = &updates[idx - 1];
        if this.0 == prev.0 && this.1 == prev.1 {
            accum += diff(&updates[idx]);
        } else {
            if accum != 0 {
                updates.swap(offset, idx - 1);
                updates[offset].2 = accum;
                offset += 1;
            }
            accum = diff(&updates[idx]);
        }
    }

    if accum != 0 {
        let len = updates.len();
        updates.swap(offset, len - 1);
        updates[offset].2 = accum;
        offset += 1;
    }

    updates.truncate(offset);
}

fn merge<D: Clone + Ord + Columnation>(
    chain1: Chain<D>,
    chain2: Chain<D>,
    since: &Antichain<Timestamp>,
) -> Chain<D> {
    use std::cmp::Ordering::*;

    let Some(since_ts) = since.as_option() else {
        return Default::default();
    };

    let mut cursor1 = ChainQueue::from(chain1);
    let mut cursor2 = ChainQueue::from(chain2);

    let mut chain = Chain::default();

    // As long as update times are greater than `since`, we can do a merge join.
    while let (Some((d1, t1, r1)), Some((d2, t2, r2))) = (cursor1.head(), cursor2.head()) {
        if !since.less_than(&t1) && !since.less_than(&t2) {
            break;
        }

        match (t1, d1).cmp(&(t2, d2)) {
            Greater => {
                chain.push_ref((d1, t1, r1));
                cursor1.remove();
            }
            Less => {
                chain.push_ref((d2, t2, r2));
                cursor2.remove();
            }
            Equal => {
                let diff = r1 + r2;
                if diff != 0 {
                    chain.push_ref((d1, t1, diff));
                }
                cursor1.remove();
                cursor2.remove();
            }
        }
    }

    // Remaining updates are at times <= `since`, which leaves them potentially unconsolidated once
    // we advance them by the `since`. We must consolidate them before adding them to the output
    // chain, to uphold the chain invariants.

    let mut early_updates = Vec::new();
    let mut iter1 = cursor1.iter().peekable();
    let mut iter2 = cursor2.iter().peekable();

    // Still do a merge join when collecting updates into the consolidation buffer. We can hope
    // that some updates cancel out this way, saving us some memory.
    while let (Some((d1, t1, r1)), Some((d2, t2, r2))) = (iter1.peek(), iter2.peek()) {
        match (t1, d1).cmp(&(t2, d2)) {
            Greater => {
                early_updates.push((*d1, *since_ts, *r1));
                iter1.next();
            }
            Less => {
                early_updates.push((*d2, *since_ts, *r2));
                iter2.next();
            }
            Equal => {
                let diff = r1 + r2;
                if diff != 0 {
                    early_updates.push((*d1, *since_ts, diff));
                }
                iter1.next();
                iter2.next();
            }
        }
    }

    early_updates.extend(iter1.map(|(d, _, r)| (d, *since_ts, r)));
    early_updates.extend(iter2.map(|(d, _, r)| (d, *since_ts, r)));

    consolidate(&mut early_updates);

    while let Some(update) = early_updates.pop() {
        chain.push_ref(update);
    }

    chain
}
