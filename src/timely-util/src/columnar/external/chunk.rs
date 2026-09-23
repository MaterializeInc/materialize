// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Column chunks whose records reference out-of-line blobs.
//!
//! [`ExternalChunk`] pairs a [`ColumnChunk`] with the [`BlockSet`] owning the
//! blocks its records reference. Every trace and batcher operation delegates
//! to the column chunk unchanged: records order by blob digest, so nothing in
//! a merge needs the bytes. Operations that produce new chunks give them the
//! union of their inputs' owners, which is always sufficient. `settle`, which
//! every committed chunk passes through, trims each output's owners to the
//! blocks its records reference, so blocks referenced by no surviving record
//! are released.

use std::collections::VecDeque;
use std::marker::PhantomData;

use columnar::{BorrowedOf, Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk;
use timely::Accountable;
use timely::container::{ContainerBuilder, PushInto};
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use super::{BlobWriter, BlockSet};
use crate::columnar::Column;
use crate::columnar::batcher::ColumnChunker;
use crate::columnar::chunk::ColumnChunk;
use crate::columnar::unload::UnloadChunk;

/// Names the blobs a record references.
pub trait BlobLayout: 'static {
    /// The record's data, holding one or more [`Blob`](super::Blob)s.
    type Data: Columnar;
    /// The record's difference.
    type Diff: Columnar;

    /// Appends the block of every blob `data` and `diff` reference. Omitting
    /// one lets its block be freed while the record points at it.
    fn blocks(
        data: columnar::Ref<'_, Self::Data>,
        diff: columnar::Ref<'_, Self::Diff>,
        out: &mut Vec<u64>,
    );
}

/// A column chunk plus the blocks its records reference.
pub struct ExternalChunk<L: BlobLayout, T: Columnar> {
    inner: ColumnChunk<L::Data, T, L::Diff>,
    owners: BlockSet,
    /// Whether `owners` names exactly the referenced blocks.
    exact: bool,
    layout: PhantomData<L>,
}

impl<L: BlobLayout, T: Columnar> Clone for ExternalChunk<L, T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            owners: self.owners.clone(),
            exact: self.exact,
            layout: PhantomData,
        }
    }
}

impl<L: BlobLayout, T: Columnar> Default for ExternalChunk<L, T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            owners: BlockSet::default(),
            exact: true,
            layout: PhantomData,
        }
    }
}

impl<L: BlobLayout, T: Columnar> Accountable for ExternalChunk<L, T> {
    fn record_count(&self) -> i64 {
        self.inner.record_count()
    }
}

impl<L: BlobLayout, T: Columnar> ExternalChunk<L, T> {
    /// The blocks this chunk's records may reference.
    pub fn owners(&self) -> &BlockSet {
        &self.owners
    }

    /// Wraps sorted, consolidated, non-empty `column`, whose blobs `owners`
    /// must cover.
    pub fn from_column(column: Column<(L::Data, T, L::Diff)>, owners: BlockSet) -> Self {
        Self {
            inner: ColumnChunk::from_column(column),
            owners,
            exact: false,
            layout: PhantomData,
        }
    }

    /// The records, without reading any blob.
    pub fn column(&self) -> Column<(L::Data, T, L::Diff)> {
        self.inner.clone().into_column()
    }

    /// As [`ExternalChunk::column`], reading spilled records off the calling
    /// thread.
    pub async fn column_async(&self) -> Column<(L::Data, T, L::Diff)> {
        self.inner.clone().into_column_async().await
    }

    fn wrap_all(
        inner: VecDeque<ColumnChunk<L::Data, T, L::Diff>>,
        owners: &BlockSet,
        out: &mut VecDeque<Self>,
    ) {
        out.extend(inner.into_iter().map(|inner| Self {
            inner,
            owners: owners.clone(),
            exact: false,
            layout: PhantomData,
        }));
    }

    /// Unwraps `input`, returning the union of its owners.
    fn unwrap_all(
        input: &mut VecDeque<Self>,
    ) -> (VecDeque<ColumnChunk<L::Data, T, L::Diff>>, BlockSet) {
        let owners = BlockSet::union(input.iter().map(|c| &c.owners));
        (input.drain(..).map(|c| c.inner).collect(), owners)
    }

    /// Restores `remaining` to the front of `input`, preserving order.
    fn restore(
        remaining: VecDeque<ColumnChunk<L::Data, T, L::Diff>>,
        owners: &BlockSet,
        input: &mut VecDeque<Self>,
    ) {
        let mut wrapped = VecDeque::new();
        Self::wrap_all(remaining, owners, &mut wrapped);
        for chunk in wrapped.into_iter().rev() {
            input.push_front(chunk);
        }
    }

    /// Trims `owners` to the blocks the records reference.
    ///
    /// Reads the chunk's records, which for a chunk `settle` just committed are
    /// normally still resident in the pool.
    fn narrow(&mut self) {
        if self.exact {
            return;
        }
        let mut ids = Vec::new();
        self.inner.with_column(|column| {
            let view = column.borrow();
            for index in 0..view.len() {
                let (data, _, diff) = view.get(index);
                L::blocks(data, diff, &mut ids);
            }
        });
        ids.sort_unstable();
        ids.dedup();
        self.owners = self.owners.restrict(&ids);
        self.exact = true;
    }
}

impl<L, T> Chunk for ExternalChunk<L, T>
where
    L: BlobLayout,
    for<'a> columnar::Ref<'a, L::Data>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    L::Diff: Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, L::Diff>>,
{
    type Time = T;
    const TARGET: usize = ColumnChunk::<L::Data, T, L::Diff>::TARGET;

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn merge(left: &mut VecDeque<Self>, right: &mut VecDeque<Self>, output: &mut VecDeque<Self>) {
        let a = left.pop_front().expect("nonempty merge input");
        let b = right.pop_front().expect("nonempty merge input");
        let merged = BlockSet::union([&a.owners, &b.owners]);
        let (a_owners, b_owners) = (a.owners, b.owners);
        let mut a = VecDeque::from([a.inner]);
        let mut b = VecDeque::from([b.inner]);
        let mut out = VecDeque::new();
        ColumnChunk::merge(&mut a, &mut b, &mut out);
        Self::restore(a, &a_owners, left);
        Self::restore(b, &b_owners, right);
        Self::wrap_all(out, &merged, output);
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(chunk) = input.pop_front() else {
            return;
        };
        let mut inner = VecDeque::from([chunk.inner]);
        let (mut kept, mut shipped) = (VecDeque::new(), VecDeque::new());
        ColumnChunk::extract(&mut inner, frontier, residual, &mut kept, &mut shipped);
        Self::restore(inner, &chunk.owners, input);
        Self::wrap_all(kept, &chunk.owners, keep);
        Self::wrap_all(shipped, &chunk.owners, ship);
    }

    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        output: &mut VecDeque<Self>,
    ) {
        let (mut inner, owners) = Self::unwrap_all(input);
        let mut out = VecDeque::new();
        ColumnChunk::advance(&mut inner, frontier, done, &mut out);
        Self::restore(inner, &owners, input);
        Self::wrap_all(out, &owners, output);
    }

    fn settle(input: &mut VecDeque<Self>, done: bool, output: &mut VecDeque<Self>) {
        let (mut inner, owners) = Self::unwrap_all(input);
        let mut out = VecDeque::new();
        ColumnChunk::settle(&mut inner, done, &mut out);
        Self::restore(inner, &owners, input);
        let start = output.len();
        Self::wrap_all(out, &owners, output);
        for chunk in output.iter_mut().skip(start) {
            chunk.narrow();
        }
    }
}

/// Probe results copied out of external chunks, with the owners that keep
/// their blobs resolvable after the source batches drop.
pub struct ExternalStaging<U: Columnar> {
    /// The matching records.
    pub updates: U::Container,
    /// Owners covering every blob in `updates`.
    pub owners: Vec<BlockSet>,
}

impl<U: Columnar> Default for ExternalStaging<U> {
    fn default() -> Self {
        Self {
            updates: Default::default(),
            owners: Vec::new(),
        }
    }
}

impl<L, K, V, T> UnloadChunk for ExternalChunk<L, T>
where
    L: BlobLayout<Data = (K, V)>,
    K: Columnar,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    V: Columnar,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    L::Diff: Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, L::Diff>>,
{
    type Staging = ExternalStaging<((K, V), T, L::Diff)>;
    type Probes<'a> = BorrowedOf<'a, K>;

    fn probe_count(probes: Self::Probes<'_>) -> usize {
        probes.len()
    }

    fn locate(&self, probes: Self::Probes<'_>, probe_index: usize) -> std::cmp::Ordering {
        self.inner.locate(probes, probe_index)
    }

    fn extract_into(
        &self,
        probes: Self::Probes<'_>,
        probe_index: &mut usize,
        staging: &mut Self::Staging,
    ) {
        let before = staging.updates.len();
        self.inner
            .extract_into(probes, probe_index, &mut staging.updates);
        if staging.updates.len() > before {
            staging.owners.push(self.owners.clone());
        }
    }

    async fn extract_into_async(
        &self,
        probes: Self::Probes<'_>,
        probe_index: &mut usize,
        staging: &mut Self::Staging,
    ) {
        let before = staging.updates.len();
        self.inner
            .extract_into_async(probes, probe_index, &mut staging.updates)
            .await;
        if staging.updates.len() > before {
            staging.owners.push(self.owners.clone());
        }
    }

    fn fetch_into(&self, staging: &mut Self::Staging) {
        let before = staging.updates.len();
        self.inner.fetch_into(&mut staging.updates);
        if staging.updates.len() > before {
            staging.owners.push(self.owners.clone());
        }
    }
}

/// Moves one column of a record type out of line.
pub trait Externalize: 'static {
    /// The data of an incoming record, holding its bytes inline.
    type Input: Columnar;
    /// The layout of the outgoing record.
    type Layout: BlobLayout;

    /// The outgoing data for `input`, with its bytes appended to `writer`.
    fn externalize(
        input: columnar::Ref<'_, Self::Input>,
        writer: &mut BlobWriter,
    ) -> <Self::Layout as BlobLayout>::Data;
}

/// A chunker for `arrange_core` that externalizes incoming records, then
/// sorts and consolidates them into [`ExternalChunk`]s.
///
/// The writer's open block persists across inputs, so small inputs share
/// blocks instead of each allocating one.
pub struct ExternalChunker<X: Externalize, T: Columnar> {
    writer: BlobWriter,
    inner: ColumnChunker<(
        <X::Layout as BlobLayout>::Data,
        T,
        <X::Layout as BlobLayout>::Diff,
    )>,
    ready: VecDeque<ExternalChunk<X::Layout, T>>,
    staged: ExternalChunk<X::Layout, T>,
}

impl<X: Externalize, T: Columnar> Default for ExternalChunker<X, T> {
    fn default() -> Self {
        Self {
            writer: BlobWriter::default(),
            inner: Default::default(),
            ready: VecDeque::new(),
            staged: Default::default(),
        }
    }
}

impl<'c, X, T> PushInto<&'c mut Column<(X::Input, T, <X::Layout as BlobLayout>::Diff)>>
    for ExternalChunker<X, T>
where
    X: Externalize,
    T: Columnar,
    ColumnChunker<(
        <X::Layout as BlobLayout>::Data,
        T,
        <X::Layout as BlobLayout>::Diff,
    )>: for<'a> PushInto<
            &'a mut Column<(
                <X::Layout as BlobLayout>::Data,
                T,
                <X::Layout as BlobLayout>::Diff,
            )>,
        > + ContainerBuilder<
            Container = Column<(
                <X::Layout as BlobLayout>::Data,
                T,
                <X::Layout as BlobLayout>::Diff,
            )>,
        >,
{
    fn push_into(&mut self, input: &'c mut Column<(X::Input, T, <X::Layout as BlobLayout>::Diff)>) {
        let mut external: Column<(
            <X::Layout as BlobLayout>::Data,
            T,
            <X::Layout as BlobLayout>::Diff,
        )> = Default::default();
        {
            let view = input.borrow();
            for index in 0..view.len() {
                let (data, time, diff) = view.get(index);
                let data = X::externalize(data, &mut self.writer);
                external.push_into(&(
                    data,
                    T::into_owned(time),
                    <<X::Layout as BlobLayout>::Diff as Columnar>::into_owned(diff),
                ));
            }
        }
        let owners = self.writer.take_owners();
        self.inner.push_into(&mut external);
        let mut bounded = VecDeque::new();
        while let Some(column) = self.inner.extract() {
            ColumnChunk::push_bounded(std::mem::take(column), 0, &mut bounded);
        }
        ExternalChunk::wrap_all(bounded, &owners, &mut self.ready);
    }
}

impl<X, T> ContainerBuilder for ExternalChunker<X, T>
where
    X: Externalize,
    T: Columnar + 'static,
{
    type Container = ExternalChunk<X::Layout, T>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.staged = self.ready.pop_front()?;
        Some(&mut self.staged)
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Blob, Resolver};
    use super::*;

    struct Layout;
    impl BlobLayout for Layout {
        type Data = (u64, Blob);
        type Diff = i64;
        fn blocks((_, blob): columnar::Ref<'_, Self::Data>, _: &i64, out: &mut Vec<u64>) {
            out.push(Blob::into_owned(blob).block());
        }
    }

    struct Text;
    impl Externalize for Text {
        type Input = (u64, Vec<u8>);
        type Layout = Layout;
        fn externalize(
            (key, bytes): columnar::Ref<'_, Self::Input>,
            writer: &mut BlobWriter,
        ) -> (u64, Blob) {
            let bytes: Vec<u8> = <Vec<u8> as Columnar>::into_owned(bytes);
            (*key, writer.push(&bytes))
        }
    }

    type Chunker = ExternalChunker<Text, u64>;
    type Ext = ExternalChunk<Layout, u64>;

    fn chunks(chunker: &mut Chunker, updates: &[(u64, &[u8], u64, i64)]) -> VecDeque<Ext> {
        let mut column: Column<((u64, Vec<u8>), u64, i64)> = Default::default();
        for (key, bytes, time, diff) in updates {
            column.push_into(&((*key, bytes.to_vec()), *time, *diff));
        }
        chunker.push_into(&mut column);
        let mut out = VecDeque::new();
        while let Some(chunk) = chunker.extract() {
            out.push_back(std::mem::take(chunk));
        }
        out
    }

    fn settled(mut input: VecDeque<Ext>) -> VecDeque<Ext> {
        let mut out = VecDeque::new();
        Ext::settle(&mut input, true, &mut out);
        out
    }

    fn records(chunks: &VecDeque<Ext>) -> Vec<(u64, u128, u64, i64)> {
        let mut out = Vec::new();
        for chunk in chunks {
            let column = chunk.column();
            let view = column.borrow();
            for index in 0..view.len() {
                let ((key, blob), time, diff) = view.get(index);
                out.push((*key, Blob::into_owned(blob).digest(), *time, *diff));
            }
        }
        out
    }

    #[mz_ore::test]
    fn copies_of_equal_bytes_cancel_without_reading_them() {
        // Two chunkers write the same bytes to different blocks, as a value
        // and its retraction do when the retraction is rebuilt from a copy.
        let (mut a, mut b) = (Chunker::default(), Chunker::default());
        let mut left = chunks(&mut a, &[(1, b"one", 0, 1), (2, b"two", 0, 1)]);
        let mut right = chunks(&mut b, &[(1, b"one", 0, -1)]);
        let mut merged = VecDeque::new();
        while !left.is_empty() && !right.is_empty() {
            Ext::merge(&mut left, &mut right, &mut merged);
        }
        merged.extend(left);
        merged.extend(right);
        let merged = settled(merged);
        assert_eq!(
            records(&merged),
            vec![(2, super::super::digest(b"two"), 0, 1)]
        );
    }

    /// A block opened by a large batch fills during a later, much smaller
    /// batch. The merge batcher keeps the two in separate chains, so the
    /// smaller batch's chunks reach the next merge without the larger batch's
    /// owners and must own the carried-over block themselves.
    #[mz_ore::test]
    fn chains_merged_without_the_opening_batch_still_own_its_block() {
        use differential_dataflow::trace::Batcher as _;
        use differential_dataflow::trace::chunk::ChunkBatcher;

        let mut chunker = Chunker::default();
        let mut batcher = ChunkBatcher::<Ext>::new(None, 0);
        let half = vec![1u8; 5_000];
        let wide = vec![2u8; 150_000];
        let rounds: [(u64, u64, &[u8]); 3] =
            [(0, 200, &half), (200, 10, &wide), (210, 10, b"small")];
        for (first, count, bytes) in rounds {
            let mut column: Column<((u64, Vec<u8>), u64, i64)> = Default::default();
            for key in first..first + count {
                let mut value = key.to_le_bytes().to_vec();
                value.extend_from_slice(bytes);
                column.push_into(&((key, value), 0, 1));
            }
            chunker.push_into(&mut column);
            while let Some(chunk) = chunker.extract() {
                batcher.push_into(std::mem::take(chunk));
            }
        }
        let (sealed, _) = batcher.seal(Antichain::new());
        let owners = BlockSet::union(sealed.iter().map(|c| c.owners()));
        assert!(owners.len() >= 2);
        assert_eq!(records(&VecDeque::from(sealed)).len(), 220);
    }

    #[mz_ore::test]
    fn settle_releases_blocks_no_record_references() {
        let mut a = Chunker::default();
        let big = vec![3u8; super::super::BLOCK_BYTES];
        let first = chunks(&mut a, &[(1, &big, 0, 1)]);
        let second = chunks(&mut a, &[(1, &big, 0, -1), (2, b"kept", 0, 1)]);
        drop(a);
        let (mut left, mut right) = (first, second);
        let mut merged = VecDeque::new();
        while !left.is_empty() && !right.is_empty() {
            Ext::merge(&mut left, &mut right, &mut merged);
        }
        merged.extend(left);
        merged.extend(right);
        let before = BlockSet::union(merged.iter().map(|c| c.owners()));
        let merged = settled(merged);
        let after = BlockSet::union(merged.iter().map(|c| c.owners()));
        assert!(after.len() < before.len(), "{before:?} -> {after:?}");
        let column = merged[0].column();
        let view = column.borrow();
        let ((_, blob), _, _) = view.get(0);
        let blob = Blob::into_owned(blob);
        let mut resolver = Resolver::default();
        resolver.load_sync([&blob], std::slice::from_ref(&after));
        resolver.with(&blob, |bytes| assert_eq!(bytes, b"kept"));
    }
}
