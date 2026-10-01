// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::chunk::vec::VecChunk;
use differential_dataflow::trace::{
    Batch, BatchReader, Description, ExertionLogic, Trace, TraceReader,
};
use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::Antichain;

use super::Spine;

type TestBatch = Rc<ChunkBatch<VecChunk<u64, u64, u64, i64>>>;

/// Rows at level 18 in the spine's doubling geometry. Lifting a batch into it
/// takes several hundred policy grants, far more than the funded turns below
/// supply, so an unmodified spine and a funded one end in different shapes.
const LARGE_ROWS: u64 = 1 << 18;
const SMALL_ROWS: u64 = 16;
const TURNS_PER_INSERT: usize = 10_000;

/// The cluster exertion policy without its proportionality bound: continue an
/// active merge, otherwise ask for effort whenever a non-empty layer exists
/// below the largest.
fn policy() -> ExertionLogic {
    Arc::new(|layers| {
        if layers.iter().any(|(_, count, _)| *count > 1) {
            return Some(1000);
        }
        let non_empty = layers.iter().filter(|(_, _, len)| *len > 0).count();
        (non_empty > 1).then_some(1000)
    })
}

fn spine() -> Spine<TestBatch> {
    let info = OperatorInfo::new(0, 0, Rc::from(&[0usize][..]));
    let mut spine = <Spine<TestBatch> as Trace>::new(info, None, None);
    spine.set_exert_logic(policy());
    spine
}

fn batch(time: u64, keys: std::ops::Range<u64>) -> TestBatch {
    let description = Description::new(
        Antichain::from_elem(time),
        Antichain::from_elem(time + 1),
        Antichain::from_elem(0),
    );
    if keys.is_empty() {
        return Rc::new(ChunkBatch::empty(
            Antichain::from_elem(time),
            Antichain::from_elem(time + 1),
        ));
    }
    let mut chunk = VecChunk::default();
    for key in keys {
        chunk.push_into(((key, 0u64), time, 1i64));
    }
    Rc::new(ChunkBatch::new(vec![chunk], description))
}

/// Row counts of the non-empty batches, largest layer first. Empty batches,
/// such as the one `close` inserts, carry no updates and are ignored.
fn shape(spine: &Spine<TestBatch>) -> Vec<usize> {
    let mut lens = Vec::new();
    spine.map_batches(|batch| {
        if batch.len() > 0 {
            lens.push(batch.len());
        }
    });
    lens
}

/// Insert a batch and, as the upsert operator does as persist advances, let
/// the spine merge everything below the new upper.
fn insert(spine: &mut Spine<TestBatch>, batch: TestBatch) {
    let upper = batch.upper().clone();
    spine.insert(batch);
    spine.set_physical_compaction(upper.borrow());
}

fn exert(spine: &mut Spine<TestBatch>, turns: usize) {
    for _ in 0..turns {
        spine.exert();
    }
}

/// One large batch followed by many small ones, with far more scheduling turns
/// than inserted updates between them.
fn hydrate(spine: &mut Spine<TestBatch>) -> u64 {
    insert(spine, batch(0, 0..LARGE_ROWS));
    exert(spine, TURNS_PER_INSERT);
    let mut next_key = LARGE_ROWS;
    for time in 1..=16 {
        insert(spine, batch(time, next_key..next_key + SMALL_ROWS));
        next_key += SMALL_ROWS;
        exert(spine, TURNS_PER_INSERT);
    }
    next_key
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn funded_exertion_stops_when_inserted_updates_are_spent() {
    let mut spine = spine();
    let total = hydrate(&mut spine);

    let lens = shape(&spine);
    assert_eq!(lens.iter().sum::<usize>(), usize::try_from(total).unwrap());
    assert!(
        lens.len() > 1,
        "small batches were all consolidated into the largest: {lens:?}"
    );
    // The large batch's own credit paid for lifting a few of the first small
    // batches into it. A 16-row insert funds far less than one lift, so the
    // later ones stay in the small layers.
    let largest = *lens.iter().max().unwrap();
    assert!(
        largest < usize::try_from(LARGE_ROWS + 8 * SMALL_ROWS).unwrap(),
        "small inserts kept funding lifts into the largest batch: {lens:?}"
    );

    // With the funding spent, further turns must not change the trace.
    exert(&mut spine, TURNS_PER_INSERT);
    assert_eq!(shape(&spine), lens, "unfunded turns changed the trace");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn closed_input_reduces_without_bound() {
    let mut spine = spine();
    let total = hydrate(&mut spine);
    assert!(shape(&spine).len() > 1);

    spine.close();
    spine.set_physical_compaction(Antichain::new().borrow());
    let mut turns = 0;
    while shape(&spine).len() > 1 {
        spine.exert();
        turns += 1;
        assert!(
            turns < 1_000_000,
            "closed spine did not reduce to one batch"
        );
    }
    assert_eq!(shape(&spine), vec![usize::try_from(total).unwrap()]);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn frontier_advances_fund_consolidation_of_a_quiet_input() {
    let mut spine = spine();
    let total = hydrate(&mut spine);
    let before = shape(&spine);
    assert!(before.len() > 1);

    // Empty batches advance the frontier without adding updates. Each funds a
    // bounded number of allowances, and enough of them reduce the trace.
    let mut time = 17;
    while shape(&spine).len() > 1 {
        insert(&mut spine, batch(time, 0..0));
        exert(&mut spine, 1000);
        time += 1;
        assert!(
            time < 2000,
            "frontier advances did not fund consolidation: {:?}",
            shape(&spine)
        );
    }
    assert_eq!(shape(&spine), vec![usize::try_from(total).unwrap()]);
}

/// Once the large batch's credit is spent, small inserts fund only a few
/// allowances each, far fewer than lifting data into the large layer takes.
/// Such a lift starts a merge that holds the large batch and its partial output
/// until done, so turns must finish it whether or not they are funded.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn unfunded_turns_finish_merges_in_progress() {
    let merging = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::clone(&merging);
    let inner = policy();
    let mut spine = spine();
    spine.set_exert_logic(Arc::new(move |layers| {
        *observed.lock().expect("policy lock") = layers
            .iter()
            .filter(|(_, count, len)| *count > 1 && *len > 0)
            .copied()
            .collect();
        inner(layers)
    }));

    insert(&mut spine, batch(0, 0..LARGE_ROWS));
    exert(&mut spine, TURNS_PER_INSERT);
    let mut next_key = LARGE_ROWS;
    for time in 1..=160 {
        insert(&mut spine, batch(time, next_key..next_key + SMALL_ROWS));
        next_key += SMALL_ROWS;
        exert(&mut spine, TURNS_PER_INSERT);
        let stalled = merging.lock().expect("policy lock").clone();
        assert!(
            stalled.is_empty(),
            "merges still in progress after insert {time}: {stalled:?}"
        );
    }
}
