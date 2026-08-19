// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Edge paging: serialized bodies live in the buffer pool until borrowed.
//!
//! Its own test binary because both pieces of state are process-global and
//! have no teardown: `apply_pool_config` installs a pool singleton for the life
//! of the process, and the paging gate is a static. A sibling test minting
//! bodies would land in the same pool and race the gate, so everything here
//! runs in one test function.

use columnar::{Borrow, Push};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::align_buffer::{AlignBuffer, Origin, set_edge_paging_enabled};
use mz_timely_util::pool_config::{PoolPagerConfig, apply_pool_config};
use timely::Accountable;
use timely::dataflow::channels::ContainerBytes;

/// Records per body. Each `(u64, u64)` serializes to 16 bytes, so this clears
/// the 64 KiB paging floor several times over without reaching the ~2 MiB ship
/// threshold, which keeps the test about paging rather than about shipping.
const RECORDS: u64 = 20_000;

/// A body too small to be worth a pool slot, under the 64 KiB floor.
const TINY_RECORDS: u64 = 8;

/// Builds a container of `records` `(u64, u64)` pairs.
fn container(records: u64) -> <(u64, u64) as columnar::Columnar>::Container {
    let mut c = <(u64, u64) as columnar::Columnar>::Container::default();
    for i in 0..records {
        c.push(&(i, i));
    }
    c
}

/// Encodes `records` rows, stamped as an edge body.
fn encode(records: u64) -> AlignBuffer {
    let c = container(records);
    let view = c.borrow();
    AlignBuffer::encode(Origin::Ship, usize::try_from(records).unwrap(), &view)
}

/// Live chunks in the process pool, or 0 before one is installed.
fn pool_live_chunks() -> u64 {
    mz_timely_util::pool_config::active_pool()
        .map(|p| p.stats().live_chunks)
        .unwrap_or(0)
}

/// The decoded `(u64, u64)` pairs a column holds.
fn decode(column: &Column<(u64, u64)>) -> Vec<(u64, u64)> {
    use columnar::{Index, Len};
    let view = column.borrow();
    (0..view.len())
        .map(|i| {
            let (a, b) = view.get(i);
            (*a, *b)
        })
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: foreign function calls (mmap, madvise)
fn edge_paging() {
    // Reference encoding, gate off. Everything below is compared against this,
    // so paging is held to producing byte-identical bodies.
    set_edge_paging_enabled(false);
    let heap = encode(RECORDS);
    assert!(!heap.is_paged(), "gate off must leave the body on the heap");
    let heap_words = heap.as_words().to_vec();
    let heap_rows = decode(&Column::Align(heap));

    // A gate with no pool installed is still inert: paging must never be a
    // half-configured state that silently drops the body somewhere else.
    set_edge_paging_enabled(true);
    assert!(
        !encode(RECORDS).is_paged(),
        "no pool installed means no paging, whatever the gate says",
    );

    let installed = apply_pool_config(PoolPagerConfig {
        budget_bytes: 1 << 30,
        spill_threads: 0,
        eager_backing: false,
        rss_target_bytes: 0,
    });
    assert!(installed, "pool reservation expected to succeed in tests");

    // Below the floor, a body stays on the heap even with pool and gate ready.
    assert!(
        !encode(TINY_RECORDS).is_paged(),
        "a body under the size-class floor is not worth a slot",
    );

    let paged = encode(RECORDS);
    assert!(paged.is_paged(), "gate plus pool must page the body");

    // The point of the design: the metadata every hot path needs is resident,
    // so none of it drags the body back out of the pool. Timely asks for
    // `record_count` at both push and pull, and materializing there would undo
    // paging before the body ever sat in a queue.
    assert_eq!(
        paged.len(),
        heap_words.len(),
        "word count without a copy-out"
    );
    assert_eq!(paged.records(), Some(usize::try_from(RECORDS).unwrap()));
    assert!(!paged.is_empty());
    assert!(paged.is_paged(), "reading metadata must not materialize");

    let column = Column::Align(paged);
    assert_eq!(column.record_count(), i64::try_from(RECORDS).unwrap());
    assert_eq!(column.length_in_bytes(), heap_words.len() * 8);
    assert!(!column.is_empty());
    let Column::Align(ref still) = column else {
        unreachable!("constructed as Align")
    };
    assert!(
        still.is_paged(),
        "record_count, length_in_bytes and is_empty must all stay off the body",
    );

    // Borrowing is what pays for the copy, and it must reproduce the body
    // exactly.
    assert_eq!(decode(&column), heap_rows, "paged body decodes identically");
    let Column::Align(ref materialized) = column else {
        unreachable!("constructed as Align")
    };
    assert!(!materialized.is_paged(), "a borrow materializes the body");
    assert_eq!(materialized.as_words(), &heap_words[..]);
    assert_eq!(
        materialized.records(),
        Some(usize::try_from(RECORDS).unwrap()),
        "materializing keeps the resident record count",
    );

    // Materializing frees the pool chunk rather than holding the body in both
    // places: a run where every body is borrowed would otherwise cost more
    // memory than not paging at all.
    let before = pool_live_chunks();
    let transient = encode(RECORDS);
    assert!(transient.is_paged());
    assert_eq!(
        pool_live_chunks(),
        before + 1,
        "a paged body holds exactly one chunk",
    );
    let _ = transient.as_words();
    assert!(!transient.is_paged());
    assert_eq!(
        pool_live_chunks(),
        before,
        "materializing must release the chunk, not keep a second copy",
    );
    drop(transient);

    // A clone of a paged body is a heap body with the same contents: the pool
    // hands out no second owner, so the clone pays the copy.
    let fresh = encode(RECORDS);
    assert!(fresh.is_paged());
    let clone = fresh.clone();
    assert_eq!(clone.as_words(), &heap_words[..]);
    assert_eq!(clone.records(), fresh.records());

    // `into_words` yields the same bytes from either state.
    assert_eq!(encode(RECORDS).into_words(), heap_words);

    set_edge_paging_enabled(false);
}
