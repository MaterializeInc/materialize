// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::ops::Range;

use differential_dataflow::difference::Multiply;
use differential_dataflow::lattice::Lattice;

use super::{Batch, RowHandle};

/// One bounded step of maximum-metadata selection.
#[derive(Debug, Eq, PartialEq)]
pub enum SelectionStep {
    /// Index of the winning record in the source batch.
    Selected(usize),
    /// The step exhausted its comparison budget.
    Yield,
    /// All keys were processed.
    Done,
}

/// Select maximum metadata per exact key without accessing payloads.
///
/// Equal metadata keeps the first record. Consumers must ensure ties are
/// interchangeable or provide an ordering that includes their tie-break rule.
/// A consumer can group by `(key, time)` to select source order at each time.
pub struct LatestCursor<'a, K, M> {
    batch: &'a Batch<K, M>,
    next: usize,
    best: Option<usize>,
}

impl<'a, K: Ord, M: Ord> LatestCursor<'a, K, M> {
    /// Begin selection over a sorted batch.
    pub fn new(batch: &'a Batch<K, M>) -> Self {
        Self {
            batch,
            next: 0,
            best: None,
        }
    }

    /// Inspect at most `fuel` records and return at most one winner.
    pub fn step(&mut self, fuel: usize) -> SelectionStep {
        let records = self.batch.records();
        for _ in 0..fuel {
            let Some(record) = records.get(self.next) else {
                return self
                    .best
                    .take()
                    .map(SelectionStep::Selected)
                    .unwrap_or(SelectionStep::Done);
            };
            let index = self.next;
            self.next += 1;
            match self.best {
                None => self.best = Some(index),
                Some(best) if records[best].key != record.key => {
                    self.best = Some(index);
                    return SelectionStep::Selected(best);
                }
                Some(best) if record.metadata > records[best].metadata => self.best = Some(index),
                Some(_) => {}
            }
        }
        SelectionStep::Yield
    }
}

/// A pair of payload locators with joined time and multiplied difference.
#[derive(Debug, Eq, PartialEq)]
pub struct JoinMatch<T, R> {
    /// Position in the left index, for accessing its exact key and metadata.
    pub left_index: usize,
    /// Position in the right index, for accessing its exact key and metadata.
    pub right_index: usize,
    /// Left payload, or an operator-defined payload-free value.
    pub left: Option<RowHandle>,
    /// Right payload, or an operator-defined payload-free value.
    pub right: Option<RowHandle>,
    /// Lattice join of the input timestamps.
    pub time: T,
    /// Product of the input differences.
    pub diff: R,
}

/// One bounded equijoin step, allowing output backpressure between matches.
#[derive(Debug, Eq, PartialEq)]
pub enum JoinStep<T, R> {
    /// A match whose payloads can be requested together.
    Match(JoinMatch<T, R>),
    /// The step exhausted its key-comparison budget.
    Yield,
    /// Both inputs have no further matching keys.
    Done,
}

/// Equijoin two immutable batches, retaining constant cursor state for large groups.
///
/// This is a batch cross product, not an incremental trace-join driver. The
/// caller is responsible for scheduling each batch pair once and holding time
/// capabilities until its matches are published. Input differences need not be
/// positive, and output is not consolidated.
pub struct JoinCursor<'a, K, T, R0, R1> {
    left: &'a Batch<K, (T, R0)>,
    right: &'a Batch<K, (T, R1)>,
    left_next: usize,
    right_next: usize,
    group: Option<(Range<usize>, Range<usize>)>,
    pair: (usize, usize),
}

impl<'a, K: Ord, T: Lattice + Clone, R0: Multiply<R1> + Clone, R1> JoinCursor<'a, K, T, R0, R1> {
    /// Start matching exact keys in two sorted batches.
    pub fn new(left: &'a Batch<K, (T, R0)>, right: &'a Batch<K, (T, R1)>) -> Self {
        Self {
            left,
            right,
            left_next: 0,
            right_next: 0,
            group: None,
            pair: (0, 0),
        }
    }

    /// Compare at most `fuel` key pairs or emit one match from the active group.
    ///
    /// Locating a group's end uses a binary search over resident index keys.
    pub fn step(&mut self, fuel: usize) -> JoinStep<T, R0::Output> {
        let left = self.left.records();
        let right = self.right.records();
        for _ in 0..fuel {
            if let Some((left_group, right_group)) = &self.group {
                let l = &left[self.pair.0];
                let r = &right[self.pair.1];
                let matched = JoinMatch {
                    left_index: self.pair.0,
                    right_index: self.pair.1,
                    left: l.row,
                    right: r.row,
                    time: l.metadata.0.join(&r.metadata.0),
                    diff: l.metadata.1.clone().multiply(&r.metadata.1),
                };
                self.pair.1 += 1;
                if self.pair.1 == right_group.end {
                    self.pair.1 = right_group.start;
                    self.pair.0 += 1;
                    if self.pair.0 == left_group.end {
                        self.group = None;
                    }
                }
                return JoinStep::Match(matched);
            }
            let (Some(l), Some(r)) = (left.get(self.left_next), right.get(self.right_next)) else {
                return JoinStep::Done;
            };
            match l.key.cmp(&r.key) {
                Ordering::Less => self.left_next += 1,
                Ordering::Greater => self.right_next += 1,
                Ordering::Equal => {
                    let left_end = self.left_next
                        + left[self.left_next..].partition_point(|record| record.key == l.key);
                    let right_end = self.right_next
                        + right[self.right_next..].partition_point(|record| record.key == r.key);
                    self.pair = (self.left_next, self.right_next);
                    self.group = Some((self.left_next..left_end, self.right_next..right_end));
                    self.left_next = left_end;
                    self.right_next = right_end;
                }
            }
        }
        JoinStep::Yield
    }
}
