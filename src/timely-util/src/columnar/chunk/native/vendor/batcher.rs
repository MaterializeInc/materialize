// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See LICENSE in this directory.

//! A `Batcher` implementation based on merge sort.
//!
//! The `MergeBatcher` requires a "merger" that implements the [`Merger`] trait, which provides
//! hooks for manipulating sorted "chains" of chunks as needed by the merge batcher: merging
//! chunks and also splitting them apart based on time.
//!
//! Raw input containers are fed to the batcher via [`Batcher::insert`], which chunks them with
//! its `Chu` before merging: forming sorted, consolidated chunks is the first stage of the
//! batcher's own work rather than something a caller arranges.

use std::task::{Context, Poll, ready};

use timely::container::{ContainerBuilder, PushInto};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Timestamp, frontier::Antichain};

use differential_dataflow::logging::{BatcherEvent, Logger};

/// Creates batches from chunks of sorted, consolidated tuples.
///
/// Chunking input is `Chu`'s business, merging chunks is `M`'s, and sealing the extracted chain
/// into a batch is `S`'s; the batcher's own work is the geometric ladder of chains and the
/// carve-by-frontier.
pub struct MergeBatcher<Chu, M: Merger, S> {
    /// Melds input containers into sorted, consolidated chunks.
    chunker: Chu,
    /// Sorted, consolidated chains, each paired with its cached summed update count.
    ///
    /// The cached count is the chain's *merge weight*: the geometric ladder weighs
    /// chains by updates, not chunk counts, since regrading decouples the two. A
    /// chain is immutable until merged, so the weight is computed once at push.
    ///
    /// Do not push/pop directly but use the corresponding functions ([`Self::chain_push`]/[`Self::chain_pop`]).
    chains: Vec<(usize, Vec<M::Chunk>)>,
    /// Stash of empty chunks, recycled through the merging process.
    stash: Vec<M::Chunk>,
    /// Merges consolidated chunks, and extracts the subset of an update chain that lies in an interval of time.
    merger: M,
    /// The lower-bound frontier of the data, after the last call to extract.
    frontier: Antichain<M::Time>,
    /// Logger for size accounting.
    logger: Option<Logger>,
    /// Timely operator ID.
    operator_id: usize,
    /// Seals each extracted chain into a batch.
    sealer: std::marker::PhantomData<S>,
    inserting: bool,
    merging: Option<(Vec<M::Chunk>, Vec<M::Chunk>, Vec<M::Chunk>)>,
    extracting: Option<(
        Antichain<M::Time>,
        Vec<M::Chunk>,
        Vec<M::Chunk>,
        Vec<M::Chunk>,
    )>,
}

impl<C, Chu, M, S> Batcher<C> for MergeBatcher<Chu, M, S>
where
    M: Merger<Time: Timestamp>,
    Chu: ContainerBuilder<Container = M::Chunk> + for<'a> PushInto<&'a mut C>,
    S: Sealer<M::Chunk>,
{
    type Time = M::Time;
    type Output = S::Output;

    fn insert(&mut self, container: &mut C) {
        self.chunker.push_into(container);
        while let Some(chunk) = self.chunker.extract().map(std::mem::take) {
            self.insert_chain(vec![chunk]);
        }
    }

    // Extraction means finding those updates with times not greater or equal to any time in
    // `upper`. All updates must have time greater or equal to the previously used `upper`, by
    // assumption that after extracting from a batcher we receive no more updates with times not
    // greater or equal to `upper`.
    fn extract<'a>(
        &'a mut self,
        upper: AntichainRef<'_, M::Time>,
    ) -> (Option<S::Output>, AntichainRef<'a, M::Time>) {
        // Flush whatever the chunker is still accumulating: a partial final chunk would
        // otherwise never reach the merge ladder.
        while let Some(chunk) = self.chunker.finish().map(std::mem::take) {
            self.insert_chain(vec![chunk]);
        }

        // Merge all remaining chains into a single chain.
        while self.chains.len() > 1 {
            let list1 = self.chain_pop().unwrap();
            let list2 = self.chain_pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.chain_push(merged);
        }
        let merged = self.chain_pop().unwrap_or_default();

        // Extract readied data.
        let mut kept = Vec::new();
        let mut readied = Vec::new();
        self.frontier.clear();

        self.merger.extract(
            merged,
            upper,
            &mut self.frontier,
            &mut readied,
            &mut kept,
            &mut self.stash,
        );

        if !kept.is_empty() {
            self.chain_push(kept);
        }

        self.stash.clear();

        (S::seal(&mut readied), self.frontier.borrow())
    }
    fn poll_insert(&mut self, container: &mut C, cx: &mut Context<'_>) -> Poll<()> {
        assert!(self.extracting.is_none(), "cannot insert during extraction");
        if !self.inserting {
            self.chunker.push_into(container);
            self.inserting = true;
        }
        ready!(self.poll_ladder(false, cx));
        while let Some(chunk) = self.chunker.extract().map(std::mem::take) {
            if M::len(&chunk) > 0 {
                self.chain_push(vec![chunk]);
            }
            ready!(self.poll_ladder(false, cx));
        }
        self.inserting = false;
        Poll::Ready(())
    }

    fn poll_extract(
        &mut self,
        upper: AntichainRef<'_, M::Time>,
        cx: &mut Context<'_>,
    ) -> Poll<(Option<S::Output>, Antichain<M::Time>)> {
        assert!(!self.inserting, "finish insertion before extracting");
        if self.extracting.is_none() {
            ready!(self.poll_ladder(false, cx));
            while let Some(chunk) = self.chunker.finish().map(std::mem::take) {
                if M::len(&chunk) > 0 {
                    self.chain_push(vec![chunk]);
                }
                ready!(self.poll_ladder(false, cx));
            }
            ready!(self.poll_ladder(true, cx));
            let merged = self.chain_pop().unwrap_or_default();
            self.frontier.clear();
            self.extracting = Some((upper.to_owned(), merged, Vec::new(), Vec::new()));
        }
        let (saved, merged, readied, kept) = self.extracting.as_mut().unwrap();
        assert_eq!(
            saved.borrow(),
            upper,
            "extraction frontier changed while pending"
        );
        ready!(self.merger.poll_extract(
            merged,
            saved.borrow(),
            &mut self.frontier,
            readied,
            kept,
            &mut self.stash,
            cx
        ));
        let (_, _, mut readied, kept) = self.extracting.take().unwrap();
        if !kept.is_empty() {
            self.chain_push(kept);
        }
        self.stash.clear();
        Poll::Ready((S::seal(&mut readied), self.frontier.clone()))
    }
}

impl<Chu: Default, M: Merger, S> MergeBatcher<Chu, M, S> {
    /// Allocates a new empty batcher.
    ///
    /// The logger and operator identifier are used to report the batcher's memory footprint,
    /// attributed to the operator that owns it.
    pub fn new(logger: Option<Logger>, operator_id: usize) -> Self {
        Self {
            logger,
            operator_id,
            merger: M::default(),
            chunker: Chu::default(),
            chains: Vec::new(),
            stash: Vec::new(),
            frontier: Antichain::new(),
            sealer: std::marker::PhantomData,
            inserting: false,
            merging: None,
            extracting: None,
        }
    }
}

impl<Chu, M: Merger, S> MergeBatcher<Chu, M, S> {
    fn poll_ladder(&mut self, all: bool, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            if self.merging.is_none() {
                let n = self.chains.len();
                if n < 2 || (!all && self.chains[n - 1].0 < self.chains[n - 2].0 / 2) {
                    return Poll::Ready(());
                }
                let a = self.chain_pop().unwrap();
                let b = self.chain_pop().unwrap();
                self.merging = Some((a, b, Vec::new()));
            }
            let (a, b, output) = self.merging.as_mut().unwrap();
            ready!(self.merger.poll_merge(a, b, output, &mut self.stash, cx));
            let (_, _, output) = self.merging.take().unwrap();
            self.chain_push(output);
        }
    }

    /// Insert a chain and maintain chain properties: Chains are geometrically sized
    /// (by summed updates) and ordered by decreasing update weight.
    fn insert_chain(&mut self, chain: Vec<M::Chunk>) {
        if !chain.is_empty() {
            self.chain_push(chain);
            while self.chains.len() > 1
                && (self.chains[self.chains.len() - 1].0
                    >= self.chains[self.chains.len() - 2].0 / 2)
            {
                let list1 = self.chain_pop().unwrap();
                let list2 = self.chain_pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.chain_push(merged);
            }
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(&mut self, list1: Vec<M::Chunk>, list2: Vec<M::Chunk>) -> Vec<M::Chunk> {
        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        self.merger
            .merge(list1, list2, &mut output, &mut self.stash);

        output
    }

    /// Pop a chain and account size changes.
    #[inline]
    fn chain_pop(&mut self) -> Option<Vec<M::Chunk>> {
        let (_weight, chain) = self.chains.pop()?;
        self.account(chain.iter().map(Self::record), -1);
        Some(chain)
    }

    /// Push a chain and account size changes.
    ///
    /// Caches the chain's summed update count alongside it for the ladder.
    #[inline]
    fn chain_push(&mut self, chain: Vec<M::Chunk>) {
        let weight = chain.iter().map(M::len).sum();
        self.account(chain.iter().map(Self::record), 1);
        self.chains.push((weight, chain));
    }

    /// The `(records, size, capacity, allocations)` logger tuple for one chunk,
    /// assembled from the two focused `Merger` methods.
    #[inline]
    fn record(chunk: &M::Chunk) -> (usize, usize, usize, usize) {
        let (size, capacity, allocations) = M::allocation(chunk);
        (M::len(chunk), size, capacity, allocations)
    }

    /// Account size changes. Only performs work if a logger exists.
    ///
    /// Calculate the size based on the iterator passed along, with each attribute
    /// multiplied by `diff`. Usually, one wants to pass 1 or -1 as the diff.
    #[inline]
    fn account<I: IntoIterator<Item = (usize, usize, usize, usize)>>(&self, items: I, diff: isize) {
        if let Some(logger) = &self.logger {
            let (mut records, mut size, mut capacity, mut allocations) =
                (0isize, 0isize, 0isize, 0isize);
            for (records_, size_, capacity_, allocations_) in items {
                records = records.saturating_add_unsigned(records_);
                size = size.saturating_add_unsigned(size_);
                capacity = capacity.saturating_add_unsigned(capacity_);
                allocations = allocations.saturating_add_unsigned(allocations_);
            }
            logger.log(BatcherEvent {
                operator: self.operator_id,
                records_diff: records * diff,
                size_diff: size * diff,
                capacity_diff: capacity * diff,
                allocations_diff: allocations * diff,
            })
        }
    }
}

impl<Chu, M: Merger, S> Drop for MergeBatcher<Chu, M, S> {
    fn drop(&mut self) {
        // Cleanup chain to retract accounting information.
        while self.chain_pop().is_some() {}
    }
}

/// A trait to describe interesting moments in a merge batcher.
pub trait Merger: Default {
    /// The internal representation of chunks of data.
    type Chunk: Default;
    /// The type of time in frontiers to extract updates.
    type Time;
    /// Merge chains into an output chain.
    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    );
    /// Extract ready updates based on the `upper` frontier.
    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    );

    /// Poll a chain merge, consuming input allocations at most once.
    fn poll_merge(
        &mut self,
        a: &mut Vec<Self::Chunk>,
        b: &mut Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
        _cx: &mut Context<'_>,
    ) -> Poll<()> {
        self.merge(std::mem::take(a), std::mem::take(b), output, stash);
        Poll::Ready(())
    }
    /// Poll extraction with stable queues and frontier until completion.
    fn poll_extract(
        &mut self,
        input: &mut Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        readied: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
        _cx: &mut Context<'_>,
    ) -> Poll<()> {
        self.extract(std::mem::take(input), upper, frontier, readied, kept, stash);
        Poll::Ready(())
    }

    /// The number of updates in a chunk.
    ///
    /// Drives the geometric ladder (chains are weighed by summed updates, not chunk
    /// counts, since regrading decouples the two) and the `records` field of the
    /// size logger.
    fn len(chunk: &Self::Chunk) -> usize;

    /// Backing-allocation figures for a chunk: `(size, capacity, allocations)`, for
    /// the size logger's memory telemetry.
    ///
    /// Defaults to zero — most chunk types do not track this. Override to report
    /// real figures (e.g. Materialize's memory accounting).
    fn allocation(_chunk: &Self::Chunk) -> (usize, usize, usize) {
        (0, 0, 0)
    }
}

/// Forms a batch from a whole chain of updates at once.
///
/// Named rather than a bare `fn(&mut Vec<C>) -> Option<B>` so that implementors can name the
/// batch they produce. There is no receiver: the chain goes in and the batch comes out, leaving
/// nowhere for an update to be retained.
pub trait Sealer<C> {
    /// Output batch type.
    type Output;

    /// Builds a batch from a chain of updates.
    ///
    /// This method relies on the chain only containing updates greater or equal to the lower frontier,
    /// and not greater or equal to the upper frontier, of the interval the caller means to describe.
    /// Chains must also be sorted and consolidated.
    ///
    /// Having the whole chain in hand, an implementor can size itself before it fills.
    fn seal(chain: &mut Vec<C>) -> Option<Self::Output>;
}

/// A type capable of accepting containers of updates, and carving them out by time as batches.
///
/// Updates are accepted as `C0`, the containers that arrive on the dataflow edge, and released as
/// `Output`, whatever the implementor means by a batch. The two need not agree: an implementor
/// staging updates in a form of its own can release that form directly, and one whose batch is a
/// sequence of chunks names a sequence as its output.
///
/// The implementor determines the meaning of extraction by a frontier; it is not required to be by
/// antichain partial order.
pub trait Batcher<C0> {
    /// The timestamps by which updates are carved out.
    type Time;
    /// The batches released by extraction.
    type Output;

    /// Takes the updates in `container`, leaving it in an undefined state.
    ///
    /// The implementor decides whether to claim the container's allocation or to drain it and
    /// leave the allocation with the caller, who is free to reuse the container either way.
    fn insert(&mut self, container: &mut C0);
    /// Extracts the updates `upper` unblocks as a batch, and lower bounds the times of those retained.
    ///
    /// What `upper` unblocks is the implementor's to decide. It can be based on the antichain up
    /// set, or it can be based on the total order of times (as used in delta join constructions).
    /// Absent a batch, `upper` unblocked no updates.
    ///
    /// The reported lower bound should accurately reflect the times of all accepted updates that
    /// have not been extracted. Over approximation can result in stalling dataflows, and under
    /// approximation is simply incorrect.
    fn extract<'a>(
        &'a mut self,
        upper: AntichainRef<'_, Self::Time>,
    ) -> (Option<Self::Output>, AntichainRef<'a, Self::Time>);
    /// Poll insertion, retaining the same container until completion.
    fn poll_insert(
        &mut self,
        container: &mut C0,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        self.insert(container);
        std::task::Poll::Ready(())
    }
    /// Poll extraction at a fixed frontier, retaining capabilities until ready.
    fn poll_extract(
        &mut self,
        upper: AntichainRef<'_, Self::Time>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<(Option<Self::Output>, Antichain<Self::Time>)>
    where
        Self::Time: Clone,
    {
        let (batch, retained) = self.extract(upper);
        std::task::Poll::Ready((batch, retained.to_owned()))
    }
}
