// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::rc::Rc;

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

impl<D: Clone + Ord + Columnation + 'static> Correction<D> {
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
            let merged = merge_chains(a, b, &self.since);
            self.chains.push(merged);
        }
    }

    /// Return consolidated updates before the given `upper`.
    pub fn updates_before<'a>(
        &'a self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + 'a {
        let upper_ts = upper.as_option().copied();

        let mut cursors = Vec::with_capacity(self.chains.len());
        for chain in &self.chains {
            let mut cursor = chain.cursor();
            cursor.upper_ts = upper_ts;
            cursors.push(cursor);
        }

        Merger::new(cursors, &self.since)
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

        let mut chain = Self::default();
        for update in updates {
            chain.push(update);
        }

        chain
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
        debug_assert!({
            let last = self.chunks.last().map(|c| c.last());
            last.map_or(true, |(d1, t1, _)| {
                let (d2, t2, _) = &update;
                (t1, d1) < (t2, d2)
            })
        });

        match self.chunks.last_mut() {
            Some(c) if c.capacity_left() => c.push_ref(update),
            Some(_) | None => {
                let mut chunk = Chunk::new();
                chunk.push_ref(update);
                self.chunks.push(chunk.into());
            }
        }
    }

    fn cursor(&self) -> ChainRefCursor<'_, D> {
        ChainRefCursor::new(&self.chunks)
    }

    fn into_rc_cursor(self) -> ChainRcCursor<D> {
        let chunks = self.chunks.into_iter().map(Rc::new).collect();
        ChainRcCursor::new(chunks)
    }
}

type ChainRefCursor<'a, D> = ChainCursor<&'a [Chunk<D>]>;
type ChainRcCursor<D> = ChainCursor<VecDeque<Rc<Chunk<D>>>>;

#[derive(Clone)]
struct ChainCursor<C> {
    chunks: C,
    chunk_index: usize,
    limit: Option<usize>,
    since_ts: Option<Timestamp>,
    upper_ts: Option<Timestamp>,
}

impl<C: ChunkList> ChainCursor<C> {
    fn new(chunks: C) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            limit: None,
            since_ts: None,
            upper_ts: None,
        }
    }

    fn get(&self) -> Option<(&C::Data, Timestamp, Diff)> {
        let chunk = self.chunks.first()?;
        let (d, t, r) = &chunk.0[self.chunk_index];
        let t = self.since_ts.map_or(*t, |since| std::cmp::max(*t, since));

        if self.upper_ts.map_or(false, |upper| upper <= t) {
            // FIXME: drop all remaining chunks
            return None;
        }

        Some((d, t, *r))
    }

    fn step(&mut self) {
        if let Some(limit) = &mut self.limit {
            *limit = limit.saturating_sub(1);
            if *limit == 0 {
                self.chunks.clear();
                return;
            }
        }

        self.chunk_index += 1;

        loop {
            let Some(chunk) = self.chunks.first() else {
                return; // no chunks left
            };
            if chunk.len() > self.chunk_index {
                return; // valid chunk index
            }
            self.skip_chunk();
        }
    }

    fn skip_chunk(&mut self) {
        self.chunks.remove_first();
        self.chunk_index = 0;
    }

    fn skip_time(&mut self, time: Timestamp) -> usize {
        let mut skip_count = 0;

        while let Some(chunk) = self.chunks.first() {
            if chunk.last().1 <= time {
                skip_count += chunk.len() - self.chunk_index;
                self.skip_chunk();
            } else {
                break;
            }
        }

        if let Some(chunk) = self.chunks.first() {
            while chunk.0[self.chunk_index].1 <= time {
                self.chunk_index += 1;
                skip_count += 1;
            }
        }

        skip_count
    }

    fn split_by_time(self, up_to: Option<Timestamp>) -> (Vec<Self>, Self) {
        let mut splits = Vec::new();
        let mut cursor = self;

        while let Some((_, time, _)) = cursor.get() {
            if up_to.map_or(false, |max| time > max) {
                break;
            }

            let mut current = cursor.clone();
            let skipped = cursor.skip_time(time);
            current.limit = Some(skipped);
            splits.push(current);
        }

        (splits, cursor)
    }
}

trait ChunkList: Clone {
    type Data: Columnation;

    fn first(&self) -> Option<&Chunk<Self::Data>>;
    fn remove_first(&mut self);
    fn clear(&mut self);
}

impl<D: Columnation> ChunkList for &[Chunk<D>] {
    type Data = D;

    fn first(&self) -> Option<&Chunk<Self::Data>> {
        self.get(0)
    }

    fn remove_first(&mut self) {
        if let Some((_, rest)) = self.split_first() {
            *self = rest;
        }
    }

    fn clear(&mut self) {
        *self = &[];
    }
}

impl<D: Columnation> ChunkList for VecDeque<Rc<Chunk<D>>> {
    type Data = D;

    fn first(&self) -> Option<&Chunk<Self::Data>> {
        self.front().map(AsRef::as_ref)
    }

    fn remove_first(&mut self) {
        self.pop_front();
    }

    fn clear(&mut self) {
        self.clear();
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

    fn len(&self) -> usize {
        self.0.len()
    }

    fn capacity_left(&self) -> bool {
        self.len() < Self::capacity()
    }

    fn last(&self) -> &(D, Timestamp, Diff) {
        self.0.last().expect("chunks are never empty")
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

fn merge_chains<D>(chain1: Chain<D>, chain2: Chain<D>, since: &Antichain<Timestamp>) -> Chain<D>
where
    D: Ord + Columnation + 'static,
{
    let cursors = vec![chain1.into_rc_cursor(), chain2.into_rc_cursor()];
    let mut merger = Merger::new(cursors, since);
    let mut chain = Chain::default();
    while let Some(result) = merger.next_ref() {
        if let Some(update) = result {
            chain.push_ref(update);
        }
    }

    chain
}

struct Merger<C> {
    stages: Vec<Vec<ChainCursor<C>>>,
    cursors_to_step: Vec<usize>,
    cursors_to_remove: Vec<usize>,
}

impl<C> Default for Merger<C> {
    fn default() -> Self {
        Self {
            stages: Default::default(),
            cursors_to_step: Default::default(),
            cursors_to_remove: Default::default(),
        }
    }
}

impl<C> Merger<C>
where
    C: ChunkList<Data: Ord>,
{
    fn new(cursors: Vec<ChainCursor<C>>, since: &Antichain<Timestamp>) -> Self {
        let Some(&since_ts) = since.as_option() else {
            return Default::default();
        };

        let mut early = Vec::with_capacity(cursors.len());
        let mut late = Vec::with_capacity(cursors.len());
        for cursor in cursors {
            let up_to = Some(since_ts);
            let (before, after) = cursor.split_by_time(up_to);
            early.extend(before);
            late.push(after);
        }

        for cursor in &mut early {
            cursor.since_ts = Some(since_ts);
        }

        Self {
            stages: vec![late, early],
            ..Default::default()
        }
    }

    fn cleanup(&mut self) {
        let Some(cursors) = self.stages.last_mut() else {
            return;
        };

        for idx in self.cursors_to_step.drain(..) {
            cursors[idx].step();
        }
        for idx in self.cursors_to_remove.drain(..).rev() {
            cursors.swap_remove(idx);
        }

        if cursors.is_empty() {
            self.stages.pop();
        }
    }

    fn next_ref(&mut self) -> Option<Option<(&C::Data, Timestamp, Diff)>> {
        self.cleanup();

        let cursors = self.stages.last_mut()?;

        let mut min_key_diff = None;
        for (idx, cursor) in cursors.iter().enumerate() {
            let Some(update) = cursor.get() else {
                self.cursors_to_remove.push(idx);
                continue;
            };

            let (d, t, r) = update;
            let (new_key, new_diff) = ((t, d), r);

            let Some((min_key, min_diff)) = &mut min_key_diff else {
                min_key_diff = Some((new_key, new_diff));
                self.cursors_to_step.push(idx);
                continue;
            };

            use std::cmp::Ordering::*;
            match new_key.cmp(&min_key) {
                Greater => (),
                Less => {
                    min_key_diff = Some((new_key, new_diff));
                    self.cursors_to_step.clear();
                    self.cursors_to_step.push(idx);
                }
                Equal => {
                    *min_diff += new_diff;
                    self.cursors_to_step.push(idx);
                }
            }
        }

        let result = match min_key_diff {
            Some(((t, d), r)) if r != 0 => Some((d, t, r)),
            _ => None,
        };
        Some(result)
    }
}

impl<C> Iterator for Merger<C>
where
    C: ChunkList<Data: Clone + Ord>,
{
    type Item = (C::Data, Timestamp, Diff);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((d, t, r)) = self.next_ref()? {
                return Some((d.clone(), t, r));
            }
        }
    }
}

fn merge_cursors<C, F>(cursors: Vec<ChainCursor<C>>, since: &Antichain<Timestamp>, mut result_fn: F)
where
    C: ChunkList<Data: Ord>,
    F: FnMut((&C::Data, Timestamp, Diff)),
{
    let Some(&since_ts) = since.as_option() else {
        return;
    };

    let mut early = Vec::with_capacity(cursors.len());
    let mut late = Vec::with_capacity(cursors.len());
    for cursor in cursors {
        let up_to = Some(since_ts);
        let (before, after) = cursor.split_by_time(up_to);
        early.extend(before);
        late.push(after);
    }

    for cursor in &mut early {
        cursor.since_ts = Some(since_ts);
    }

    merge_cursors_inner(early, &mut result_fn);
    merge_cursors_inner(late, &mut result_fn);
}

fn merge_cursors_inner<C, F>(mut cursors: Vec<ChainCursor<C>>, mut result_fn: F)
where
    C: ChunkList<Data: Ord>,
    F: FnMut((&C::Data, Timestamp, Diff)),
{
    let mut to_step = Vec::new();
    let mut to_remove = Vec::new();

    while !cursors.is_empty() {
        let mut min_key_diff = None;
        for (idx, cursor) in cursors.iter().enumerate() {
            let Some(update) = cursor.get() else {
                to_remove.push(idx);
                continue;
            };

            let (d, t, r) = update;
            let (new_key, new_diff) = ((t, d), r);

            let Some((min_key, diff)) = &mut min_key_diff else {
                min_key_diff = Some((new_key, new_diff));
                to_step.push(idx);
                continue;
            };

            use std::cmp::Ordering::*;
            match new_key.cmp(min_key) {
                Greater => (),
                Less => {
                    min_key_diff = Some((new_key, new_diff));
                    to_step.clear();
                    to_step.push(idx);
                }
                Equal => {
                    *diff += new_diff;
                    to_step.push(idx);
                }
            }
        }

        if let Some(((t, d), r)) = min_key_diff {
            if r != 0 {
                result_fn((d, t, r));
            }
        }

        for idx in to_step.drain(..) {
            cursors[idx].step();
        }
        for idx in to_remove.drain(..).rev() {
            cursors.swap_remove(idx);
        }
    }
}
