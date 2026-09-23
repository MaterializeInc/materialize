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
use mz_ore::cast::CastLossy;
use mz_ore::metrics::MetricsRegistry;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::align_buffer::{
    AlignBuffer, Origin, metrics, set_edge_paging_enabled, stashed_capacity,
};
use mz_timely_util::pool_config::{PoolPagerConfig, apply_pool_config};
use timely::Accountable;
use timely::bytes::arc::BytesMut;
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

/// The `name` gauge for `origin`.
fn gauge(registry: &MetricsRegistry, name: &str, origin: &str) -> f64 {
    let families = registry.gather();
    let family = families
        .iter()
        .find(|f| f.name() == name)
        .unwrap_or_else(|| panic!("{name} is registered"));
    let metric = family
        .get_metric()
        .iter()
        .find(|m| {
            m.get_label()
                .iter()
                .any(|l| l.name() == "origin" && l.value() == origin)
        })
        .unwrap_or_else(|| panic!("{name} has an {origin} series"));
    metric.get_gauge().value()
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
    // The heap bodies dropped above leave the slot empty: only a materialized
    // copy is retired, so with paging inert nothing parks in it.
    assert_eq!(
        stashed_capacity(),
        None,
        "a heap body must not retire its buffer"
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

    // The buffer a materialized body leaves behind is retired to this thread's
    // slot and refilled by the next materialization, so a steady stream of
    // paged bodies allocates once rather than once per body. Per thread and
    // capacity one, never per operator: a per-builder buffer would be held by
    // every idle operator on every worker.
    let a = encode(RECORDS);
    let _ = a.as_words();
    drop(a);
    let retired = stashed_capacity();
    assert!(
        retired.is_some_and(|c| c >= heap_words.len()),
        "a materialized body retires its buffer, got {retired:?}",
    );
    let b = encode(RECORDS);
    assert!(b.is_paged());
    assert_eq!(
        stashed_capacity(),
        retired,
        "an unmaterialized body must not disturb the slot",
    );
    assert_eq!(b.as_words(), &heap_words[..]);
    assert_eq!(
        stashed_capacity(),
        None,
        "materializing must consume the retired buffer, not allocate afresh",
    );
    drop(b);

    // Dropping a body nobody borrowed frees its chunk without a copy-out.
    let before = pool_live_chunks();
    drop(encode(RECORDS));
    assert_eq!(
        pool_live_chunks(),
        before,
        "an unborrowed drop frees the chunk"
    );

    // A fan-out clone shares the chunk, so the body stays in the pool until
    // the last consumer reaches it. Timely clones a body once per extra
    // consumer at push time, and a clone that copied out there would undo
    // paging for every fan-out edge.
    let before = pool_live_chunks();
    let first = encode(RECORDS);
    let second = first.clone();
    assert_eq!(pool_live_chunks(), before + 1, "clones share one chunk");
    assert!(
        first.is_paged() && second.is_paged(),
        "cloning copies nothing out"
    );
    assert_eq!(second.records(), first.records());
    assert_eq!(first.as_words(), &heap_words[..]);
    assert!(
        second.is_paged(),
        "one consumer's copy-out leaves the other paged"
    );
    assert_eq!(
        pool_live_chunks(),
        before + 1,
        "a holder that is not the last reads the chunk and leaves it",
    );
    drop(first);
    assert_eq!(
        pool_live_chunks(),
        before + 1,
        "the remaining holder keeps it"
    );
    assert_eq!(second.as_words(), &heap_words[..]);
    assert_eq!(
        pool_live_chunks(),
        before,
        "the last holder takes the chunk"
    );
    drop(second);

    // The chunk is freed when the last holder drops, whichever order the
    // holders read and drop in.
    let first = encode(RECORDS);
    let second = first.clone();
    assert_eq!(first.as_words(), &heap_words[..]);
    assert_eq!(second.as_words(), &heap_words[..]);
    assert_eq!(pool_live_chunks(), before + 1, "both read while shared");
    drop(first);
    drop(second);
    assert_eq!(pool_live_chunks(), before, "the last drop frees the chunk");

    // A clone made after the last holder took the chunk copies its words.
    let taken = encode(RECORDS);
    let _ = taken.as_words();
    let late = taken.clone();
    assert!(!late.is_paged());
    assert_eq!(late.as_words(), &heap_words[..]);
    drop((taken, late));
    assert_eq!(pool_live_chunks(), before);

    // Two threads borrowing one body for the first time see the same words,
    // and the body is copied out once.
    let shared = encode(RECORDS);
    std::thread::scope(|scope| {
        let readers: Vec<_> = (0..2)
            .map(|_| scope.spawn(|| shared.as_words().to_vec()))
            .collect();
        for reader in readers {
            assert_eq!(reader.join().unwrap(), heap_words);
        }
    });
    assert!(!shared.is_paged());
    drop(shared);
    assert_eq!(pool_live_chunks(), before);

    // Serializing a paged column for another process yields the same bytes a
    // heap column would.
    let column = Column::<(u64, u64)>::Align(encode(RECORDS));
    let mut bytes = Vec::new();
    column.into_bytes(&mut bytes);
    assert_eq!(bytes.len(), column.length_in_bytes());
    let received = Column::<(u64, u64)>::from_bytes(BytesMut::from(bytes).freeze());
    assert_eq!(decode(&received), heap_rows, "a paged column round-trips");
    drop(column);
    assert_eq!(pool_live_chunks(), before);

    // `into_words` yields the same bytes from either state, and from a shared
    // body.
    assert_eq!(encode(RECORDS).into_words(), heap_words);
    let first = encode(RECORDS);
    let second = first.clone();
    assert_eq!(first.into_words(), heap_words);
    assert_eq!(pool_live_chunks(), before + 1);
    assert_eq!(second.into_words(), heap_words);
    assert_eq!(pool_live_chunks(), before);

    // Recording charges a paged body's count but not its bytes, which the
    // pool reports, and charges each consumer's copy as an `unpage` buffer
    // for as long as the consumer holds it.
    let registry = MetricsRegistry::new();
    metrics::register(&registry);
    metrics::set_tracking_enabled(true);
    let inflight = |name: &str, origin: &str| gauge(&registry, name, origin);
    let ship_count = inflight("mz_column_align_buffer_inflight_count", "ship");
    let ship_bytes = inflight("mz_column_align_buffer_inflight_bytes", "ship");
    let unpage_bytes = inflight("mz_column_align_buffer_inflight_bytes", "unpage");
    let first = encode(RECORDS);
    let second = first.clone();
    assert_eq!(
        inflight("mz_column_align_buffer_inflight_count", "ship"),
        ship_count + 2.0,
        "a paged body and its shared clone are each in flight",
    );
    assert_eq!(
        inflight("mz_column_align_buffer_inflight_bytes", "ship"),
        ship_bytes,
        "paged bytes belong to the pool's ledger",
    );
    let _ = first.as_words();
    let copied = inflight("mz_column_align_buffer_inflight_bytes", "unpage") - unpage_bytes;
    assert!(
        copied >= f64::cast_lossy(heap_words.len() * 8),
        "a consumer's copy is charged while it lives, got {copied}",
    );
    drop((first, second));
    assert_eq!(
        inflight("mz_column_align_buffer_inflight_bytes", "unpage"),
        unpage_bytes,
        "dropping the copy credits it back",
    );
    assert_eq!(
        inflight("mz_column_align_buffer_inflight_count", "ship"),
        ship_count,
    );
    metrics::set_tracking_enabled(false);

    set_edge_paging_enabled(false);
}
