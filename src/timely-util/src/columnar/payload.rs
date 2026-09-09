// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Columnar metadata chunks with independently owned pool-backed payloads.
//!
//! [`PayloadLayout`] locates handles inside operator-defined data and differences.
//! [`PayloadChunk`] delegates ordering, consolidation, and spilling to
//! [`ColumnChunk`], then retains the blocks referenced by the resulting metadata.
//! Rewrites can read metadata but never need to decode the external payloads.
//!
//! [`PayloadStaging`] carries ownership alongside copied probe results so callers
//! can release a trace batch before reading its rows. Locators are physical
//! addresses, so the operator must supply its own logical equality policy.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::rc::Rc;

use columnar::{Columnar, Index, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::columnar::Column;
use crate::columnar::chunk::ColumnChunk;
use crate::out_of_core::{Manifest, RowHandle};

/// Identifies every external payload referenced by a metadata update.
pub trait PayloadLayout: 'static {
    /// Columnar data used for ordering and consolidation.
    type Data: Columnar;
    /// Columnar difference used for consolidation.
    type Diff: Columnar;
    /// Append every external locator referenced by this update, including those
    /// carried in the difference. Duplicates are allowed. Omitting a locator can
    /// retire its block while the update still refers to it.
    fn visit(
        data: columnar::Ref<'_, Self::Data>,
        diff: columnar::Ref<'_, Self::Diff>,
        rows: &mut Vec<RowHandle>,
    );
}

/// A metadata chunk whose manifest keeps every referenced payload block alive.
pub struct PayloadChunk<P: PayloadLayout, T: Columnar> {
    metadata: ColumnChunk<P::Data, T, P::Diff>,
    manifest: Rc<Manifest>,
    marker: PhantomData<P>,
}

impl<P: PayloadLayout, T: Columnar> Clone for PayloadChunk<P, T> {
    fn clone(&self) -> Self {
        Self {
            metadata: self.metadata.clone(),
            manifest: Rc::clone(&self.manifest),
            marker: PhantomData,
        }
    }
}

impl<P: PayloadLayout, T: Columnar> PayloadChunk<P, T> {
    /// The ownership needed to resolve this chunk's locators.
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Resident ordering bounds, without reading metadata or payload bodies.
    pub fn data_span(&self) -> (columnar::Ref<'_, P::Data>, columnar::Ref<'_, P::Data>) {
        self.metadata.data_span()
    }

    /// Read metadata without decoding payloads.
    pub fn metadata(&self) -> Column<(P::Data, T, P::Diff)> {
        self.metadata.clone().into_column()
    }

    /// Read metadata asynchronously without decoding payloads.
    pub async fn metadata_async(&self) -> Column<(P::Data, T, P::Diff)> {
        self.metadata.clone().into_column_async().await
    }
}

impl<P, T> PayloadChunk<P, T>
where
    P: PayloadLayout,
    for<'a> columnar::Ref<'a, P::Data>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    P::Diff: Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, P::Diff>>,
{
    /// Attach ownership to sorted, consolidated metadata.
    ///
    /// Panics if `owners` does not cover every locator reported by the layout.
    pub fn new(metadata: Column<(P::Data, T, P::Diff)>, owners: &[&Manifest]) -> Self {
        Self::attach(ColumnChunk::from_column(metadata), owners)
    }

    fn attach(metadata: ColumnChunk<P::Data, T, P::Diff>, owners: &[&Manifest]) -> Self {
        let mut rows = Vec::new();
        metadata.with_column(|column| {
            let view = column.borrow();
            for index in 0..view.len() {
                let (data, _, diff) = view.get(index);
                P::visit(data, diff, &mut rows);
            }
        });
        let manifest = Manifest::retain(rows, owners.iter().copied())
            .expect("metadata must retain payload ownership");
        Self {
            metadata,
            manifest: Rc::new(manifest),
            marker: PhantomData,
        }
    }

    fn detach_metadata(
        input: &mut VecDeque<Self>,
        owners: &mut Vec<Rc<Manifest>>,
    ) -> VecDeque<ColumnChunk<P::Data, T, P::Diff>> {
        input
            .drain(..)
            .map(|chunk| {
                owners.push(chunk.manifest);
                chunk.metadata
            })
            .collect()
    }

    fn attach_metadata(
        input: VecDeque<ColumnChunk<P::Data, T, P::Diff>>,
        owners: &[Rc<Manifest>],
        output: &mut VecDeque<Self>,
    ) {
        let owners: Vec<_> = owners.iter().map(|owner| &**owner).collect();
        output.extend(input.into_iter().map(|chunk| Self::attach(chunk, &owners)));
    }
}

impl<P, T> Chunk for PayloadChunk<P, T>
where
    P: PayloadLayout,
    for<'a> columnar::Ref<'a, P::Data>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    P::Diff: Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, P::Diff>>,
{
    type Time = T;
    const TARGET: usize = ColumnChunk::<P::Data, T, P::Diff>::TARGET;
    fn len(&self) -> usize {
        self.metadata.len()
    }

    fn merge(left: &mut VecDeque<Self>, right: &mut VecDeque<Self>, output: &mut VecDeque<Self>) {
        let a = left.pop_front().expect("nonempty merge input");
        let b = right.pop_front().expect("nonempty merge input");
        let owners = vec![a.manifest, b.manifest];
        let mut a = VecDeque::from([a.metadata]);
        let mut b = VecDeque::from([b.metadata]);
        let mut out = VecDeque::new();
        ColumnChunk::merge(&mut a, &mut b, &mut out);
        let mut remaining = VecDeque::new();
        Self::attach_metadata(a, &owners, &mut remaining);
        for chunk in remaining.into_iter().rev() {
            left.push_front(chunk);
        }
        let mut remaining = VecDeque::new();
        Self::attach_metadata(b, &owners, &mut remaining);
        for chunk in remaining.into_iter().rev() {
            right.push_front(chunk);
        }
        Self::attach_metadata(out, &owners, output);
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
        let owners = vec![chunk.manifest];
        let mut columns = VecDeque::from([chunk.metadata]);
        let (mut kept, mut shipped) = (VecDeque::new(), VecDeque::new());
        ColumnChunk::extract(&mut columns, frontier, residual, &mut kept, &mut shipped);
        assert!(columns.is_empty());
        Self::attach_metadata(kept, &owners, keep);
        Self::attach_metadata(shipped, &owners, ship);
    }

    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        output: &mut VecDeque<Self>,
    ) {
        let mut owners = Vec::new();
        let mut columns = Self::detach_metadata(input, &mut owners);
        let mut out = VecDeque::new();
        ColumnChunk::advance(&mut columns, frontier, done, &mut out);
        Self::attach_metadata(columns, &owners, input);
        Self::attach_metadata(out, &owners, output);
    }

    fn settle(input: &mut VecDeque<Self>, done: bool, output: &mut VecDeque<Self>) {
        let mut owners = Vec::new();
        let mut columns = Self::detach_metadata(input, &mut owners);
        let mut out = VecDeque::new();
        ColumnChunk::settle(&mut columns, done, &mut out);
        Self::attach_metadata(columns, &owners, input);
        Self::attach_metadata(out, &owners, output);
    }
}

impl<P: PayloadLayout, T: Columnar> Default for PayloadChunk<P, T> {
    fn default() -> Self {
        Self {
            metadata: Default::default(),
            manifest: Rc::new(Manifest::default()),
            marker: PhantomData,
        }
    }
}

impl<P: PayloadLayout, T: Columnar> timely::Accountable for PayloadChunk<P, T> {
    fn record_count(&self) -> i64 {
        self.metadata.record_count()
    }
}

/// Pass already sorted payload chunks to an arrangement without losing owners.
pub struct PayloadChunker<P: PayloadLayout, T: Columnar> {
    ready: VecDeque<PayloadChunk<P, T>>,
    staged: PayloadChunk<P, T>,
}
impl<P: PayloadLayout, T: Columnar> Default for PayloadChunker<P, T> {
    fn default() -> Self {
        Self {
            ready: VecDeque::new(),
            staged: Default::default(),
        }
    }
}
impl<P: PayloadLayout, T: Columnar> timely::container::PushInto<&mut Vec<PayloadChunk<P, T>>>
    for PayloadChunker<P, T>
{
    fn push_into(&mut self, input: &mut Vec<PayloadChunk<P, T>>) {
        self.ready.extend(input.drain(..));
    }
}
impl<P: PayloadLayout, T: Columnar> timely::container::ContainerBuilder for PayloadChunker<P, T> {
    type Container = PayloadChunk<P, T>;
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.staged = self.ready.pop_front()?;
        Some(&mut self.staged)
    }
    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

/// Copied metadata and the owners needed to keep its external payloads valid.
pub struct PayloadStaging<U: Columnar> {
    /// Matching updates, retaining their stored times and differences.
    pub updates: U::Container,
    /// Chunk manifests that own the locators in `updates`.
    pub owners: Vec<Rc<Manifest>>,
}
impl<U: Columnar> Default for PayloadStaging<U> {
    fn default() -> Self {
        Self {
            updates: Default::default(),
            owners: Vec::new(),
        }
    }
}

impl<P, K, V, T> crate::columnar::unload::UnloadChunk for PayloadChunk<P, T>
where
    P: PayloadLayout<Data = (K, V)>,
    K: Columnar,
    V: Columnar,
    for<'a> columnar::Ref<'a, K>: Copy + Ord,
    for<'a> columnar::Ref<'a, V>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    P::Diff: Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, P::Diff>>,
{
    type Staging = PayloadStaging<((K, V), T, P::Diff)>;
    type Probes<'a> =
        <ColumnChunk<(K, V), T, P::Diff> as crate::columnar::unload::UnloadChunk>::Probes<'a>;
    fn probe_count(probes: Self::Probes<'_>) -> usize {
        ColumnChunk::<(K, V), T, P::Diff>::probe_count(probes)
    }
    fn locate(&self, probes: Self::Probes<'_>, index: usize) -> std::cmp::Ordering {
        self.metadata.locate(probes, index)
    }
    fn extract_into(
        &self,
        probes: Self::Probes<'_>,
        index: &mut usize,
        staging: &mut Self::Staging,
    ) {
        self.metadata
            .extract_into(probes, index, &mut staging.updates);
        staging.owners.push(Rc::clone(&self.manifest));
    }
    async fn extract_into_async(
        &self,
        probes: Self::Probes<'_>,
        index: &mut usize,
        staging: &mut Self::Staging,
    ) {
        self.metadata
            .extract_into_async(probes, index, &mut staging.updates)
            .await;
        staging.owners.push(Rc::clone(&self.manifest));
    }
    fn fetch_into(&self, staging: &mut Self::Staging) {
        self.metadata.fetch_into(&mut staging.updates);
        staging.owners.push(Rc::clone(&self.manifest));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::out_of_core::{PayloadInterner, Store};
    use timely::container::PushInto;

    struct Layout;
    impl PayloadLayout for Layout {
        type Data = (u64, RowHandle);
        type Diff = i64;
        fn visit((_, row): columnar::Ref<'_, Self::Data>, _: &i64, rows: &mut Vec<RowHandle>) {
            rows.push(RowHandle::into_owned(row));
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn metadata_compaction_cancels_rows_and_releases_payloads_without_reads() {
        let pool = mz_ore::pool::Pool::new().unwrap();
        pool.set_budget(0);
        let store = Store::new(pool.clone(), 128, 256, 1, &mz_ore::pool::IDENTITY_CODEC).unwrap();
        let mut interner = PayloadInterner::new(store.clone());
        let (ids, owner) = interner.intern(&[vec![1; 96], vec![2; 96]]).await.unwrap();
        let make = |updates: &[((u64, RowHandle), u64, i64)]| {
            let mut column = Column::default();
            for update in updates {
                column.push_into(update);
            }
            PayloadChunk::<Layout, u64>::new(column, &[&owner])
        };
        let mut a = VecDeque::from([make(&[((0, ids[0]), 0, 1), ((1, ids[1]), 0, 1)])]);
        let mut b = VecDeque::from([make(&[((0, ids[0]), 0, -1)])]);
        drop(owner);
        let reads = store.stats().read_jobs;
        let mut output = VecDeque::new();
        PayloadChunk::merge(&mut a, &mut b, &mut output);
        output.extend(a);
        output.extend(b);
        let mut advanced = VecDeque::new();
        PayloadChunk::advance(
            &mut output,
            Antichain::from_elem(2).borrow(),
            true,
            &mut advanced,
        );
        assert_eq!(advanced.iter().map(Chunk::len).sum::<usize>(), 1);
        assert_eq!(
            pool.stats().live_chunks,
            1,
            "canceled row's block must retire"
        );
        assert_eq!(
            store.stats().read_jobs,
            reads,
            "metadata rewrites must not fetch payloads"
        );
        let chunk = advanced.pop_front().unwrap();
        let lease = store
            .prepare_read([(chunk.manifest(), ids[1])])
            .unwrap()
            .read()
            .await;
        assert_eq!(lease.get(ids[1]).unwrap(), &[2; 96]);
        drop(lease);
        drop(chunk);
        assert_eq!(pool.stats().live_chunks, 0);
    }
}
