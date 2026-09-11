// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Chunk operations whose logical data order can differ from locator order.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::rc::Rc;

use columnar::{Borrow, Columnar, Container, Index, Len, Push};
use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

use super::{PayloadChunk, PayloadLayout};
use crate::columnar::Column;
use crate::columnar::chunk::ColumnChunk;
use crate::out_of_core::{Manifest, PayloadComparator};

impl<P, T> PayloadChunk<P, T>
where
    P: PayloadLayout,
    for<'a> columnar::Ref<'a, P::Data>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    P::Diff: Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, P::Diff>>,
{
    /// Sort and consolidate one resident column by the layout's logical order.
    /// Scratch grows with this column, so callers must bound their input batches.
    pub fn consolidate(
        column: Column<(P::Data, T, P::Diff)>,
        owners: &[&Manifest],
    ) -> Column<(P::Data, T, P::Diff)> {
        let view = column.borrow();
        let mut cache = PayloadComparator::default();
        let mut order: Vec<_> = (0..view.len()).collect();
        order.sort_by(|a, b| {
            let (a, ta, _) = view.get(*a);
            let (b, tb, _) = view.get(*b);
            P::compare(a, b, owners, &mut cache).then_with(|| ta.cmp(&tb))
        });
        let mut result = <(P::Data, T, P::Diff) as Columnar>::Container::default();
        let mut indices = order.into_iter().peekable();
        while let Some(index) = indices.next() {
            let (data, time, diff) = view.get(index);
            let mut diff = P::Diff::into_owned(diff);
            while let Some(next) = indices.peek() {
                let (next_data, next_time, next_diff) = view.get(*next);
                if time != next_time
                    || P::compare(data, next_data, owners, &mut cache) != Ordering::Equal
                {
                    break;
                }
                diff.plus_equals(&next_diff);
                indices.next();
            }
            if !diff.is_zero() {
                result.0.push(data);
                result.1.push(time);
                result.2.push(&diff);
            }
        }
        Column::Typed(result)
    }

    fn publish(
        column: Column<(P::Data, T, P::Diff)>,
        depth: u8,
        owners: &[&Manifest],
        output: &mut VecDeque<Self>,
    ) {
        let mut pieces = VecDeque::new();
        ColumnChunk::push_bounded(column, depth, &mut pieces);
        output.extend(pieces.into_iter().map(|piece| Self::attach(piece, owners)));
    }

    pub(super) fn merge_logical(
        left: &mut VecDeque<Self>,
        right: &mut VecDeque<Self>,
        output: &mut VecDeque<Self>,
    ) {
        let a = left.front().expect("nonempty input");
        let b = right.front().expect("nonempty input");
        let owners = [a.manifest(), b.manifest()];
        let mut cache = PayloadComparator::default();
        if P::compare(a.data_span().1, b.data_span().0, &owners, &mut cache) == Ordering::Less {
            let mut chunk = left.pop_front().expect("observed front");
            chunk.metadata = chunk.metadata.survive_merge();
            output.push_back(chunk);
            return;
        }
        if P::compare(b.data_span().1, a.data_span().0, &owners, &mut cache) == Ordering::Less {
            let mut chunk = right.pop_front().expect("observed front");
            chunk.metadata = chunk.metadata.survive_merge();
            output.push_back(chunk);
            return;
        }
        let a = left.pop_front().expect("observed front");
        let b = right.pop_front().expect("observed front");
        let depths = [a.metadata.depth(), b.metadata.depth()];
        let owners = [a.manifest(), b.manifest()];
        let ca = a.metadata();
        let cb = b.metadata();
        let (va, vb) = (ca.borrow(), cb.borrow());
        let (mut i, mut j) = (0, 0);
        let mut result = <(P::Data, T, P::Diff) as Columnar>::Container::default();
        // Stop when either front ends. Its next chunk can continue the same
        // logical value, so emitting the other front's tail would miss cancellation.
        while i < va.len() && j < vb.len() {
            let (da, ta, ra) = va.get(i);
            let (db, tb, rb) = vb.get(j);
            let comparison = P::compare(da, db, &owners, &mut cache).then_with(|| ta.cmp(&tb));
            let (data, time, diff) = match comparison {
                Ordering::Less => {
                    i += 1;
                    (da, ta, P::Diff::into_owned(ra))
                }
                Ordering::Greater => {
                    j += 1;
                    (db, tb, P::Diff::into_owned(rb))
                }
                Ordering::Equal => {
                    i += 1;
                    j += 1;
                    let mut diff = P::Diff::into_owned(ra);
                    diff.plus_equals(&rb);
                    (da, ta, diff)
                }
            };
            if !diff.is_zero() {
                result.0.push(data);
                result.1.push(time);
                result.2.push(&diff);
            }
        }
        Self::publish(
            Column::Typed(result),
            depths[0].max(depths[1]).saturating_add(1),
            &owners,
            output,
        );
        for (view, index, depth, queue) in [(va, i, depths[0], left), (vb, j, depths[1], right)] {
            if index < view.len() {
                let mut rest = <(P::Data, T, P::Diff) as Columnar>::Container::default();
                rest.extend_from_self(view, index..view.len());
                queue.push_front(Self::attach(
                    ColumnChunk::Resident(Rc::new(Column::Typed(rest)), depth),
                    &owners,
                ));
            }
        }
    }

    pub(super) fn advance_logical(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        output: &mut VecDeque<Self>,
    ) {
        let mut columns = <(P::Data, T, P::Diff) as Columnar>::Container::default();
        let mut owners = Vec::new();
        let mut depth = 0;
        for chunk in input.drain(..) {
            depth = depth.max(chunk.metadata.depth());
            let column = chunk.metadata();
            let view = column.borrow();
            columns.extend_from_self(view, 0..view.len());
            owners.push(chunk.manifest);
        }
        let owners: Vec<_> = owners.iter().map(|owner| &**owner).collect();
        let view = columns.borrow();
        if view.is_empty() {
            return;
        }
        let mut cache = PayloadComparator::default();
        let mut canonical = <(P::Data, T, P::Diff) as Columnar>::Container::default();
        let mut representative = view.get(0).0;
        for index in 0..view.len() {
            let (data, time, diff) = view.get(index);
            if P::compare(representative, data, &owners, &mut cache) != Ordering::Equal {
                representative = data;
            }
            canonical.0.push(representative);
            canonical.1.push(time);
            canonical.2.push(diff);
        }
        drop(cache);
        // One representative per adjacent logical group lets native advancement
        // identify equal data without reading payloads. It sorts only times within
        // each group, preserving the layout's logical data order.
        let mut columns = VecDeque::from([ColumnChunk::Resident(
            Rc::new(Column::Typed(canonical)),
            depth,
        )]);
        let mut advanced = VecDeque::new();
        ColumnChunk::advance(&mut columns, frontier, done, &mut advanced);
        input.extend(
            columns
                .into_iter()
                .map(|column| Self::attach(column, &owners)),
        );
        output.extend(
            advanced
                .into_iter()
                .map(|column| Self::attach(column, &owners)),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_ore::pool::{IDENTITY_CODEC, Pool};
    use timely::container::PushInto;
    use timely::order::Product;

    use super::*;
    use crate::out_of_core::{PayloadRef, RowHandle, Store};
    use timely::progress::Antichain;

    struct Logical;
    impl PayloadLayout for Logical {
        type Data = (u64, RowHandle);
        type Diff = i64;
        // All values for a key deliberately share a fingerprint.
        const COMPARE_PAYLOADS: bool = true;
        fn compare<'a>(
            (ka, a): columnar::Ref<'a, Self::Data>,
            (kb, b): columnar::Ref<'a, Self::Data>,
            owners: &[&Manifest],
            cache: &mut PayloadComparator,
        ) -> Ordering {
            ka.cmp(kb).then_with(|| {
                cache
                    .compare(
                        PayloadRef::External(RowHandle::into_owned(a)),
                        PayloadRef::External(RowHandle::into_owned(b)),
                        owners,
                    )
                    .unwrap()
            })
        }
        fn visit((_, row): columnar::Ref<'_, Self::Data>, _: &i64, rows: &mut Vec<RowHandle>) {
            rows.push(RowHandle::into_owned(row));
        }
    }

    fn make<T>(store: &Store, updates: &[(u64, u64, T, i64)]) -> PayloadChunk<Logical, T>
    where
        T: Columnar + Default + Timestamp + Lattice,
        for<'a> columnar::Ref<'a, T>: Copy + Ord,
    {
        let mut builder = store.builder();
        let mut column = Column::default();
        for (key, value, time, diff) in updates {
            let mut bytes = [0; 96];
            bytes[..8].copy_from_slice(&value.to_be_bytes());
            let row = builder.push(&bytes).unwrap();
            column.push_into(&((*key, row), time.clone(), *diff));
        }
        let owner = builder.finish();
        let column = PayloadChunk::<Logical, T>::consolidate(column, &[&owner]);
        PayloadChunk::new(column, &[&owner])
    }

    fn merge<T>(
        mut a: VecDeque<PayloadChunk<Logical, T>>,
        mut b: VecDeque<PayloadChunk<Logical, T>>,
    ) -> VecDeque<PayloadChunk<Logical, T>>
    where
        T: Columnar + Default + Timestamp + Lattice,
        for<'a> columnar::Ref<'a, T>: Copy + Ord,
    {
        let mut output = VecDeque::new();
        while !a.is_empty() && !b.is_empty() {
            PayloadChunk::merge(&mut a, &mut b, &mut output);
        }
        output.extend(a);
        output.extend(b);
        output
    }

    #[mz_ore::test]
    fn independent_locations_cancel_across_chunk_boundaries() {
        let pool = Pool::new().unwrap();
        pool.set_budget(0);
        let store = Store::new(pool.clone(), 128, 256, 1, &IDENTITY_CODEC).unwrap();
        let a = VecDeque::from([
            make(&store, &[(0, 3, 0u64, 1)]),
            make(&store, &[(0, 4, 0, 1), (1, 9, 0, 1)]),
        ]);
        let b = VecDeque::from([
            make(&store, &[(0, 3, 0, -1), (0, 4, 0, -1)]),
            make(&store, &[(1, 9, 0, -1)]),
        ]);
        let inserts = pool.stats().inserts;
        assert!(merge(a, b).is_empty());
        assert_eq!(
            pool.stats().inserts,
            inserts,
            "merging must not rewrite payloads"
        );
        assert_eq!(pool.stats().live_chunks, 0);
    }

    #[mz_ore::test]
    fn cancellation_reads_spilled_metadata_and_releases_all_blocks() {
        let pool = Pool::new().unwrap();
        pool.set_budget(0);
        pool.set_rss_target(1 << 20);
        crate::columnar::chunk::with_spill_override(pool.clone(), || {
            let store = Store::new(
                pool.clone(),
                4096,
                8192,
                1,
                &crate::columnar::chunk::LZ4_CODEC,
            )
            .unwrap();
            let positives: Vec<_> = (0..4096).map(|value| (0, value, 0u64, 1)).collect();
            let negatives: Vec<_> = positives
                .iter()
                .map(|(key, value, time, _)| (*key, *value, *time, -1))
                .collect();
            let mut a = VecDeque::new();
            let mut b = VecDeque::new();
            PayloadChunk::settle(
                &mut VecDeque::from([make(&store, &positives)]),
                true,
                &mut a,
            );
            PayloadChunk::settle(
                &mut VecDeque::from([make(&store, &negatives)]),
                true,
                &mut b,
            );
            assert!(a.iter().chain(&b).all(|chunk| chunk.metadata.is_spilled()));
            let inserts = pool.stats().inserts;
            assert!(merge(a, b).is_empty());
            assert_eq!(pool.stats().inserts, inserts);
            assert_eq!(pool.stats().live_chunks, 0);
        });
    }

    #[mz_ore::test(tokio::test)]
    async fn colliding_values_survive_and_advance_matches_reference() {
        let pool = Pool::new().unwrap();
        pool.set_budget(0);
        let store = Store::new(pool.clone(), 128, 256, 1, &IDENTITY_CODEC).unwrap();
        let mut actual = VecDeque::new();
        let mut expected = BTreeMap::new();
        let frontier = Antichain::from(vec![Product::new(3u64, 0u64), Product::new(0, 3)]);
        for run in 0..12 {
            let updates: Vec<_> = (0..70)
                .map(|index| {
                    let key = index % 3;
                    let value = (index * 7 + run) % 19;
                    let mut time = Product::new(index % 5, (index + run) % 5);
                    let original = time;
                    time.advance_by(frontier.borrow());
                    let diff = if (index + run) % 3 == 0 { -1 } else { 1 };
                    *expected.entry((key, value, time)).or_insert(0) += diff;
                    (key, value, original, diff)
                })
                .collect();
            actual = merge(actual, VecDeque::from([make(&store, &updates)]));
        }
        expected.retain(|_, diff| *diff != 0);
        let mut carry = VecDeque::new();
        let mut advanced = VecDeque::new();
        for chunk in actual {
            carry.push_back(chunk);
            PayloadChunk::advance(&mut carry, frontier.borrow(), false, &mut advanced);
        }
        PayloadChunk::advance(&mut carry, frontier.borrow(), true, &mut advanced);
        let mut decoded = BTreeMap::new();
        for chunk in advanced {
            let column = chunk.metadata();
            let view = column.borrow();
            for index in 0..view.len() {
                let ((key, row), time, diff) = view.get(index);
                let row = RowHandle::into_owned(row);
                let lease = store
                    .prepare_read([(chunk.manifest(), row)])
                    .unwrap()
                    .read()
                    .await;
                let value = u64::from_be_bytes(lease.get(row).unwrap()[..8].try_into().unwrap());
                assert!(
                    decoded
                        .insert((*key, value, Product::into_owned(time)), *diff)
                        .is_none()
                );
            }
        }
        assert_eq!(decoded, expected);
        assert_eq!(pool.stats().live_chunks, 0);
    }
}
