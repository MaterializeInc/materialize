// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt;
use std::rc::Rc;

use differential_dataflow::trace::implementations::BatchContainer;
use mz_persist_client::metrics::{SinkMetrics, SinkWorkerMetrics};
use mz_repr::{Diff, Timestamp};
use mz_timely_util::containers::stack::StackWrapper;
use timely::container::columnation::Columnation;
use timely::progress::Antichain;
use timely::{Container, PartialOrder};

const CHUNK_SIZE_BYTES: usize = 8 << 10;

pub trait Data: differential_dataflow::Data + Columnation {}
impl<D: differential_dataflow::Data + Columnation> Data for D {}

#[derive(Debug)]
pub(super) struct CorrectionV2<D: Data> {
    /// Chains containing sorted updates.
    chains: Vec<Chain<D>>,
    /// The frontier by which all contained times are advanced.
    since: Antichain<Timestamp>,
    /// Global persist sink metrics.
    _metrics: SinkMetrics,
    /// Per-worker persist sink metrics.
    _worker_metrics: SinkWorkerMetrics,
}

impl<D: Data> CorrectionV2<D> {
    /// Construct a new [`CorrectionV2`] instance.
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

    /// Insert a batch of updates.
    ///
    /// All times are expected to be >= the `since`.
    fn insert_inner(&mut self, mut updates: Vec<(D, Timestamp, Diff)>) {
        debug_assert!(updates.iter().all(|(_, t, _)| self.since.less_equal(t)));

        consolidate(&mut updates);

        let first_update = match updates.first() {
            Some((d, t, r)) => (d, *t, *r),
            None => return,
        };

        // Optimization: If all items in `updates` sort after all items in the last chain, we can
        // append them to the last chain directly instead of constructing a new chain.
        let chain = match self.chains.last_mut() {
            Some(chain) if chain.can_accept(first_update) => chain,
            _ => {
                self.chains.push(Chain::default());
                self.chains.last_mut().unwrap()
            }
        };

        chain.extend(updates);

        // Restore the chain invariant.
        let merge_needed = |chains: &[Chain<_>]| match chains {
            [.., prev, last] => last.len() >= prev.len() / 3,
            _ => false,
        };

        while merge_needed(&self.chains) {
            let a = self.chains.pop().unwrap();
            let b = self.chains.pop().unwrap();
            let merged = merge_chains(vec![a, b], &self.since);
            self.chains.push(merged);
        }
    }

    /// Return consolidated updates before the given `upper`.
    pub fn updates_before<'a>(
        &'a mut self,
        upper: &Antichain<Timestamp>,
    ) -> impl Iterator<Item = (D, Timestamp, Diff)> + 'a {
        self.consolidate_before(upper);

        // There are either zero updates before `upper`, or a single chain that has them all.
        let merged = self
            .chains
            .iter()
            .find(|c| c.last().map_or(false, |(_, t, _)| !upper.less_equal(&t)));

        merged.map(|c| c.iter()).into_iter().flatten()
    }

    /// Consolidate all updates before the given `upper`.
    ///
    /// Once this method returns, `chains` either contains no updates before `upper`, or contains
    /// a single chain that contains exactly all updates before `upper`.
    fn consolidate_before(&mut self, upper: &Antichain<Timestamp>) {
        if self.chains.is_empty() {
            return;
        }

        let chains = std::mem::take(&mut self.chains);
        let (merged, remains) = merge_chains_up_to(chains, &self.since, upper);

        self.chains = remains;
        if !merged.is_empty() {
            self.chains.push(merged);
        }

        self.chains.sort_unstable_by_key(|c| c.len());
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

    /// Consolidate all updates at the current `since`.
    pub fn consolidate_at_since(&mut self) {
        let upper_ts = self.since.as_option().and_then(|t| t.try_step_forward());
        if let Some(upper_ts) = upper_ts {
            let upper = Antichain::from_elem(upper_ts);
            self.consolidate_before(&upper);
        }
    }
}

#[derive(Debug)]
struct Chain<D: Data> {
    chunks: Vec<Chunk<D>>,
}

impl<D: Data> Default for Chain<D> {
    fn default() -> Self {
        Self {
            chunks: Default::default(),
        }
    }
}

impl<D: Data> Chain<D> {
    fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    fn len(&self) -> usize {
        self.chunks.len()
    }

    fn push<DT: Borrow<D>>(&mut self, update: (DT, Timestamp, Diff)) {
        let (d, t, r) = update;
        let update = (d.borrow(), t, r);

        debug_assert!(self.can_accept(update));

        match self.chunks.last_mut() {
            Some(c) if c.capacity_left() => c.push(update),
            Some(_) | None => {
                let mut chunk = Chunk::default();
                chunk.push(update);
                self.push_chunk(chunk);
            }
        }
    }

    fn push_chunk(&mut self, chunk: Chunk<D>) {
        self.chunks.push(chunk);
    }

    fn push_cursor(&mut self, cursor: Cursor<D>) {
        let mut rest = Some(cursor);
        while let Some(cursor) = rest.take() {
            let update = cursor.get();
            self.push(update);
            rest = cursor.step();
        }
    }

    fn can_accept(&self, update: (&D, Timestamp, Diff)) -> bool {
        self.last().map_or(true, |(dc, tc, _)| {
            let (d, t, _) = update;
            (tc, dc) < (t, d)
        })
    }

    fn last(&self) -> Option<(&D, Timestamp, Diff)> {
        self.chunks.last().map(|c| c.last())
    }

    fn into_cursor(self) -> Option<Cursor<D>> {
        let chunks = self.chunks.into_iter().map(Rc::new).collect();
        Cursor::new(chunks)
    }

    fn iter(&self) -> impl Iterator<Item = (D, Timestamp, Diff)> + '_ {
        self.chunks
            .iter()
            .flat_map(|c| c.0.iter().map(|(d, t, r)| (d.clone(), *t, *r)))
    }
}

impl<D: Data> Extend<(D, Timestamp, Diff)> for Chain<D> {
    fn extend<I: IntoIterator<Item = (D, Timestamp, Diff)>>(&mut self, iter: I) {
        for update in iter {
            self.push(update);
        }
    }
}

/// Invariant: Produced updates are ordered and consolidated.
/// Invariant: Cursor is not empty.
#[derive(Clone, Debug)]
struct Cursor<D: Data> {
    chunks: VecDeque<Rc<Chunk<D>>>,
    chunk_offset: usize,
    limit: Option<usize>,
    overwrite_ts: Option<Timestamp>,
}

impl<D: Data> Cursor<D> {
    fn new(chunks: VecDeque<Rc<Chunk<D>>>) -> Option<Self> {
        if chunks.is_empty() {
            return None;
        }

        Some(Self {
            chunks,
            chunk_offset: 0,
            limit: None,
            overwrite_ts: None,
        })
    }

    fn set_limit(mut self, limit: usize) -> Option<Self> {
        if limit == 0 {
            return None;
        }

        // Release chunks made unreachable by the limit.
        let mut count = 0;
        let mut idx = 0;
        let mut offset = self.chunk_offset;
        while idx < self.chunks.len() && count < limit {
            let chunk = &self.chunks[idx];
            count += chunk.len() - offset;
            idx += 1;
            offset = 0;
        }
        self.chunks.truncate(idx);

        if count > limit {
            self.limit = Some(limit);
        }

        Some(self)
    }

    fn get(&self) -> (&D, Timestamp, Diff) {
        let chunk = self.get_chunk();
        let (d, t, r) = chunk.index(self.chunk_offset);
        let t = self.overwrite_ts.unwrap_or(t);
        (d, t, r)
    }

    fn get_chunk(&self) -> &Chunk<D> {
        &self.chunks[0]
    }

    fn step(mut self) -> Option<Self> {
        if self.chunk_offset == self.get_chunk().len() - 1 {
            return self.skip_chunk().map(|(c, _)| c);
        }

        self.chunk_offset += 1;

        if let Some(limit) = &mut self.limit {
            *limit -= 1;
            if *limit == 0 {
                return None;
            }
        }

        Some(self)
    }

    fn skip_chunk(mut self) -> Option<(Self, usize)> {
        let chunk = self.chunks.pop_front().expect("cursor invariant");

        if self.chunks.is_empty() {
            return None;
        }

        let skipped = chunk.len() - self.chunk_offset;
        self.chunk_offset = 0;

        if let Some(limit) = &mut self.limit {
            if skipped >= *limit {
                return None;
            }
            *limit -= skipped;
        }

        Some((self, skipped))
    }

    fn skip_time(mut self, time: Timestamp) -> Option<(Self, usize)> {
        if self.overwrite_ts.map_or(false, |ts| ts <= time) {
            return None;
        } else if self.get().1 > time {
            return Some((self, 0));
        }

        let mut skipped = 0;

        let new_offset = loop {
            let chunk = self.get_chunk();
            if let Some(index) = chunk.find_time_greater_than(time) {
                break index;
            }

            let (cursor, count) = self.skip_chunk()?;
            self = cursor;
            skipped += count;
        };

        skipped += new_offset - self.chunk_offset;
        self.chunk_offset = new_offset;

        Some((self, skipped))
    }

    fn advance_by(mut self, since_ts: Timestamp) -> Vec<Self> {
        if let Some(ts) = self.overwrite_ts {
            if ts < since_ts {
                self.overwrite_ts = Some(since_ts);
            }
            return vec![self];
        }

        let mut splits = Vec::new();
        let mut remaining = Some(self);

        while let Some(cursor) = remaining.take() {
            let (_, time, _) = cursor.get();
            if time >= since_ts {
                splits.push(cursor);
                break;
            }

            let mut current = cursor.clone();
            if let Some((cursor, skipped)) = cursor.skip_time(time) {
                remaining = Some(cursor);
                current = current.set_limit(skipped).expect("skipped at least 1");
            }
            current.overwrite_ts = Some(since_ts);
            splits.push(current);
        }

        splits
    }

    fn split_at_time(self, time: Timestamp) -> (Option<Self>, Option<Self>) {
        let Some(skip_ts) = time.step_back() else {
            return (None, Some(self));
        };

        let before = self.clone();
        match self.skip_time(skip_ts) {
            Some((beyond, skipped)) => (before.set_limit(skipped), Some(beyond)),
            None => (Some(before), None),
        }
    }

    fn try_unwrap(self) -> Result<Chain<D>, (&'static str, Self)> {
        if self.limit.is_some() {
            return Err(("cursor with limit", self));
        }
        if self.overwrite_ts.is_some() {
            return Err(("cursor with overwrite_ts", self));
        }
        if self.chunks.iter().any(|c| Rc::strong_count(c) != 1) {
            return Err(("cursor on shared chunks", self));
        }

        let mut chain = Chain::default();
        let mut remaining = Some(self);

        while let Some(cursor) = remaining.take() {
            if cursor.chunk_offset == 0 {
                remaining = Some(cursor);
                break;
            }
            let update = cursor.get();
            chain.push(update);
            remaining = cursor.step();
        }

        if let Some(cursor) = remaining {
            for chunk in cursor.chunks {
                let chunk = Rc::into_inner(chunk).expect("checked above");
                chain.push_chunk(chunk);
            }
        }

        Ok(chain)
    }
}

impl<D: Data> From<Cursor<D>> for Chain<D> {
    fn from(cursor: Cursor<D>) -> Self {
        match cursor.try_unwrap() {
            Ok(chain) => chain,
            Err((_, cursor)) => {
                let mut chain = Chain::default();
                chain.push_cursor(cursor);
                chain
            }
        }
    }
}

struct Chunk<D: Data>(StackWrapper<(D, Timestamp, Diff)>);

impl<D: Data> Default for Chunk<D> {
    fn default() -> Self {
        let capacity = Self::capacity();
        Self(StackWrapper::with_capacity(capacity))
    }
}

impl<D: Data> fmt::Debug for Chunk<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Chunk(<{}>)", self.len())
    }
}

impl<D: Data> Chunk<D> {
    fn capacity() -> usize {
        let size = std::mem::size_of::<(D, Timestamp, Diff)>();
        std::cmp::max(CHUNK_SIZE_BYTES / size, 1)
    }

    fn len(&self) -> usize {
        Container::len(&self.0)
    }

    fn capacity_left(&self) -> bool {
        self.len() < Self::capacity()
    }

    fn index(&self, idx: usize) -> (&D, Timestamp, Diff) {
        let (d, t, r) = self.0.index(idx);
        (d, *t, *r)
    }

    fn last(&self) -> (&D, Timestamp, Diff) {
        self.index(self.len() - 1)
    }

    fn push<DT: Borrow<D>>(&mut self, update: (DT, Timestamp, Diff)) {
        let (d, t, r) = update;
        self.0.copy_destructured(d.borrow(), &t, &r);
    }

    fn find_time_greater_than(&self, time: Timestamp) -> Option<usize> {
        if self.last().1 <= time {
            return None;
        }

        let mut lower = 0;
        let mut upper = self.len();
        while lower < upper {
            let idx = (lower + upper) / 2;
            if self.index(idx).1 > time {
                upper = idx;
            } else {
                lower = idx + 1;
            }
        }

        Some(lower)
    }
}

fn consolidate<D: Data>(updates: &mut Vec<(D, Timestamp, Diff)>) {
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

fn merge_chains<D: Data>(chains: Vec<Chain<D>>, since: &Antichain<Timestamp>) -> Chain<D> {
    let Some(&since_ts) = since.as_option() else {
        return Chain::default();
    };

    let mut to_merge = Vec::new();
    for chain in chains {
        if let Some(cursor) = chain.into_cursor() {
            let mut runs = cursor.advance_by(since_ts);
            to_merge.append(&mut runs);
        }
    }

    merge_cursors(to_merge)
}

fn merge_chains_up_to<D: Data>(
    chains: Vec<Chain<D>>,
    since: &Antichain<Timestamp>,
    upper: &Antichain<Timestamp>,
) -> (Chain<D>, Vec<Chain<D>>) {
    let Some(&since_ts) = since.as_option() else {
        return (Chain::default(), Vec::new());
    };
    let Some(&upper_ts) = upper.as_option() else {
        let merged = merge_chains(chains, since);
        return (merged, Vec::new());
    };

    assert!(since_ts < upper_ts);

    let mut to_merge = Vec::new();
    let mut remains = Vec::new();
    for chain in chains {
        if let Some(cursor) = chain.into_cursor() {
            let mut runs = cursor.advance_by(since_ts);
            if let Some(last) = runs.pop() {
                let (before, beyond) = last.split_at_time(upper_ts);
                before.map(|c| runs.push(c));
                beyond.map(|c| remains.push(c));
            }
            to_merge.append(&mut runs);
        }
    }

    let merged = merge_cursors(to_merge);
    let remains = remains
        .into_iter()
        .map(|c| c.try_unwrap().expect("unwrapable by construction"))
        .collect();

    (merged, remains)
}

fn merge_cursors<D: Data>(cursors: Vec<Cursor<D>>) -> Chain<D> {
    match cursors.len() {
        0 => Chain::default(),
        1 => {
            let [cur] = cursors.try_into().unwrap();
            Chain::from(cur)
        }
        _ => merge_many(cursors),
    }
}

fn merge_many<D: Data>(cursors: Vec<Cursor<D>>) -> Chain<D> {
    let mut heap = MergeHeap::from_iter(cursors);
    let mut merged = Chain::default();
    while let Some(cursor1) = heap.pop() {
        let (data, time, mut diff) = cursor1.get();

        while let Some((cursor2, r)) = heap.pop_equal(data, time) {
            diff += r;
            if let Some(cursor2) = cursor2.step() {
                heap.push(cursor2);
            }
        }

        if diff != 0 {
            merged.push((data, time, diff));
        }
        if let Some(cursor1) = cursor1.step() {
            heap.push(cursor1);
        }
    }

    merged
}

struct MergeHeap<D: Data>(BinaryHeap<MergeHead<D>>);

impl<D: Data> FromIterator<Cursor<D>> for MergeHeap<D> {
    fn from_iter<I: IntoIterator<Item = Cursor<D>>>(cursors: I) -> Self {
        let inner = cursors.into_iter().map(MergeHead).collect();
        Self(inner)
    }
}

impl<D: Data> MergeHeap<D> {
    fn pop(&mut self) -> Option<Cursor<D>> {
        self.0.pop().map(|MergeHead(c)| c)
    }

    fn pop_equal(&mut self, data: &D, time: Timestamp) -> Option<(Cursor<D>, Diff)> {
        let MergeHead(cursor) = self.0.peek()?;
        let (d, t, r) = cursor.get();
        if d == data && t == time {
            let cursor = self.pop().expect("checked above");
            Some((cursor, r))
        } else {
            None
        }
    }

    fn push(&mut self, cursor: Cursor<D>) {
        self.0.push(MergeHead(cursor));
    }
}

struct MergeHead<D: Data>(Cursor<D>);

impl<D: Data> PartialEq for MergeHead<D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<D: Data> Eq for MergeHead<D> {}

impl<D: Data> PartialOrd for MergeHead<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<D: Data> Ord for MergeHead<D> {
    fn cmp(&self, other: &Self) -> Ordering {
        let (d1, t1, _) = self.0.get();
        let (d2, t2, _) = other.0.get();
        (t1, d1).cmp(&(t2, d2)).reverse()
    }
}
