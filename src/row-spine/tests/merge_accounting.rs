// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Heap accounting over a spine has to include its in-progress merges.
//!
//! A spine mid-merge holds three allocations at the layer being merged: the two input batches and
//! the merge's partially assembled output. Only the inputs are reachable through
//! `TraceReader::map_batches`, and the output's containers are allocated at their full merged
//! capacity the moment the merge begins, so an accounting built on `map_batches` alone understates
//! a merging arrangement's resident bytes by close to a factor of two.
//!
//! This drives a real `RowRowSpine` into a state with a merge in flight and checks that
//! `Spine::map_mergers` reaches the output and that the output is a large share of the total.
//! `mz_compute`'s `ArrangementSize` operator sums exactly these two contributions.

use std::rc::Rc;

use differential_dataflow::trace::{Builder, Description, Trace, TraceReader};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_row_spine::{RowRowBuilder, RowRowSpine};
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, Timestamp as _};

type Spine = RowRowSpine<Timestamp, Diff>;
type SpineBatch = <Spine as TraceReader>::Batch;
type SpineBuilder = RowRowBuilder<Timestamp, Diff>;

/// Heap size, capacity, and allocation count of a key/value storage's containers.
///
/// Mirrors `mz_compute::extensions::arrange`'s accounting, which is what this test guards.
macro_rules! val_storage_heap_size {
    ($storage:expr) => {{
        let storage = $storage;
        let (mut size, mut capacity, mut allocations) = (0usize, 0usize, 0usize);
        let mut callback = |siz, cap| {
            size += siz;
            capacity += cap;
            allocations += usize::from(cap > 0);
        };
        storage.keys.heap_size(&mut callback);
        storage.vals.offs.heap_size(&mut callback);
        storage.vals.vals.heap_size(&mut callback);
        storage.upds.offs.heap_size(&mut callback);
        storage.upds.times.heap_size(&mut callback);
        storage.upds.diffs.heap_size(&mut callback);
        (size, capacity, allocations)
    }};
}

/// Seals one batch covering `[time, time + 1)`, holding `keys` distinct key/value pairs.
fn batch(time: u64, keys: std::ops::Range<u64>) -> SpineBatch {
    let mut chunk = <SpineBuilder as Builder>::Input::default();
    for k in keys {
        let key = Row::pack_slice(&[Datum::UInt64(k)]);
        let val = Row::pack_slice(&[Datum::UInt64(k), Datum::String("padding padding padding")]);
        chunk.copy(&((key, val), Timestamp::from(time), Diff::ONE));
    }
    <SpineBuilder as Builder>::seal(
        &mut vec![chunk],
        Description::new(
            Antichain::from_elem(Timestamp::from(time)),
            Antichain::from_elem(Timestamp::from(time + 1)),
            Antichain::from_elem(Timestamp::minimum()),
        ),
    )
}

/// The bytes a spine's batches account for, and the bytes only its mergers account for.
fn measure(spine: &Spine) -> (usize, usize) {
    let mut batch_capacity = 0;
    spine.map_batches(|batch| {
        let (_, capacity, _) = val_storage_heap_size!(&batch.storage);
        batch_capacity += capacity;
    });
    let mut merger_capacity = 0;
    spine.map_mergers(|merger| {
        let (_, capacity, _) = val_storage_heap_size!(merger.inner().result());
        merger_capacity += capacity;
    });
    (batch_capacity, merger_capacity)
}

#[mz_ore::test]
fn in_progress_merges_hold_bytes_no_batch_accounts_for() {
    let info = OperatorInfo::new(0, 0, Rc::from(&[0][..]));
    let mut spine: Spine = Trace::new(info, None, None);

    // Enough batches that the spine has layers of several different scales mid-merge at once.
    let per_batch = 4_000;
    let mut peak: Option<(usize, usize)> = None;
    for step in 0..32u64 {
        let lo = step * per_batch;
        spine.insert(batch(step, lo..lo + per_batch));
        spine.set_physical_compaction(Antichain::from_elem(Timestamp::from(step + 1)).borrow());
        spine.set_logical_compaction(Antichain::from_elem(Timestamp::from(step + 1)).borrow());

        let (batches, mergers) = measure(&spine);
        if peak.is_none_or(|(_, best)| mergers > best) {
            peak = Some((batches, mergers));
        }
    }

    let (batches, mergers) = peak.expect("at least one observation");
    assert!(
        mergers > 0,
        "no in-progress merge observed; the test never reached the state it means to cover"
    );
    // The share is not a tight bound, only evidence that what `map_batches` misses is a first-order
    // term rather than bookkeeping noise. In practice a top-level merge roughly doubles the total.
    assert!(
        mergers * 4 > batches,
        "merger bytes {mergers} are under a quarter of batch bytes {batches}; \
         either merge_capacity stopped allocating up front or the measurement is wrong"
    );
}
