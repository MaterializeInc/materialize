// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Pool-level tests of file mode: demotion, reads, and drop deferral.

use std::rc::Rc;

use itertools::Itertools;

use super::*;
use crate::pool::extent::TEST_CODEC;

/// Words that fill a 64 KiB class exactly.
const SMALL: usize = (64 << 10) / 8;

fn file_pool(budget: usize, capacity: u64) -> (tempfile::TempDir, Pool) {
    let dir = crate::pool::file::disk_tempdir();
    let pool = Pool::with_backend_and_class_capacity(
        ExtentBackend::File {
            dir: dir.path().to_owned(),
            capacity_bytes: Some(capacity),
        },
        64 << 20,
    )
    .expect("file pool");
    pool.set_budget(budget);
    (dir, pool)
}

fn payload(words: usize, seed: u64) -> Vec<u64> {
    (0..u64::cast_from(words))
        .map(|i| seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(i))
        .collect()
}

fn insert_with_codec(pool: &Pool, data: &[u64], codec: &'static dyn ExtentCodec) -> ChunkHandle {
    pool.insert_with(data.len(), ChunkHints::default(), codec, |dst| {
        dst.copy_from_slice(data)
    })
}

fn insert(pool: &Pool, data: &[u64]) -> ChunkHandle {
    insert_with_codec(pool, data, &TEST_CODEC)
}

fn read(handle: &ChunkHandle) -> Vec<u64> {
    let mut out = Vec::new();
    handle.read_into(&mut out);
    out
}

fn read_admit(handle: &ChunkHandle) -> Vec<u64> {
    let mut out = Vec::new();
    handle.read_into_admit(&mut out);
    out
}

/// Arms a one-shot hook that runs inside the next demotion on this thread,
/// after the chunk lock is released and before the file write.
fn arm_demotion_hook(hook: impl FnOnce() + 'static) {
    DEMOTION_HOOK.with(|cell| *cell.borrow_mut() = Some(Box::new(hook)));
}

fn assert_drained(pool: &Pool) {
    let stats = pool.stats();
    assert_eq!(stats.extent_resident_bytes, 0, "no extent bytes in RAM");
    assert_eq!(stats.extent_file_bytes, 0, "no extent bytes on file");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn evicted_chunk_demotes_and_reads_back() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    assert_ne!(pool.backend_kind(), BackendKind::Swap);
    let orig = payload(SMALL, 1);
    let handle = insert(&pool, &orig);
    // The RSS target defaults to zero, so the eviction's enforcement pass
    // demotes the extent immediately.
    pool.evict(&handle);
    assert_eq!(handle.residency(), Residency::Evicted);
    let stats = pool.stats();
    assert_eq!(stats.extent_file_writes, 1);
    assert_eq!(stats.extent_file_writes_inline, 1);
    assert_eq!(stats.extent_pageouts, 1);
    assert_eq!(stats.extent_resident_bytes, 0);
    assert!(stats.extent_file_bytes > 0);
    assert!(stats.extent_file_capacity_bytes > 0);
    assert_eq!(read(&handle), orig);
    assert_eq!(
        handle.residency(),
        Residency::Evicted,
        "plain reads do not admit"
    );
    let stats = pool.stats();
    assert_eq!(stats.extent_file_reads, 1);
    assert!(stats.extent_file_read_bytes > 0);
    assert_eq!(stats.extent_file_read_latency.iter().sum::<u64>(), 1);
    assert_eq!(stats.extent_resident_bytes, 0, "file reads revive nothing");
    drop(handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn freed_before_demotion_elides_write() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    pool.set_rss_target(1 << 30);
    let handle = insert(&pool, &payload(SMALL, 2));
    pool.evict(&handle);
    assert_eq!(handle.residency(), Residency::Evicted);
    assert!(pool.stats().extent_resident_bytes > 0);
    drop(handle);
    let stats = pool.stats();
    assert_eq!(stats.extent_demotions_elided, 1);
    assert_eq!(stats.extent_file_writes, 0);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn free_during_demotion_returns_both_slots_once() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let store = pool.file_store().expect("file mode");
    let handle = insert(&pool, &payload(SMALL, 3));
    let probe = Arc::clone(&handle.meta);
    let arena = Arc::clone(&pool.0.extent_arena);
    assert_eq!(arena.slots_in_use(), 0);
    let hook_arena = Arc::clone(&arena);
    arm_demotion_hook(move || {
        assert_eq!(hook_arena.slots_in_use(), 1, "the demoting extent's slot");
        drop(handle);
        assert_eq!(hook_arena.slots_in_use(), 1, "the free left the slot alone");
    });
    // Re-borrow through the meta: the handle itself moved into the hook.
    {
        let mut state = probe.state();
        probe.pool.evict_locked(&probe, &mut state);
    }
    probe.pool.enforce_or_defer_compressed_cap();
    let stats = pool.stats();
    assert_eq!(stats.frees, 1, "the hook dropped the handle mid-demotion");
    assert_eq!(
        stats.extent_file_writes, 1,
        "the write ran before the commit"
    );
    assert!(
        stats.extent_file_write_bytes_compressed > 0,
        "write bytes count the same writes as `extent_file_writes`"
    );
    assert_eq!(
        stats.extent_pageouts, 0,
        "a freed chunk's demotion never commits"
    );
    assert_eq!(stats.extent_demotions_elided, 0, "the write happened");
    assert_eq!(stats.extent_resident_bytes, 0);
    assert_eq!(stats.extent_unreclaimable_bytes, 0);
    assert_eq!(pool.0.extent_residents.load(Ordering::Relaxed), 0);
    assert_eq!(stats.extent_file_bytes, 0);
    assert!(
        probe.state().extent.is_none(),
        "the demoter dropped the extent"
    );
    assert_eq!(arena.slots_in_use(), 0, "the arena slot returned");
    drop(probe);
    let allocated = store.allocated_bytes();
    assert!(allocated > 0, "the freed file slot keeps its blocks");

    // The next demotion of the same class reuses the returned file slot.
    let again = insert(&pool, &payload(SMALL, 3));
    pool.evict(&again);
    assert_eq!(pool.stats().extent_file_writes, 2);
    assert!(again.file_slot().is_some(), "demoted");
    assert_eq!(store.allocated_bytes(), allocated, "no new blocks");
    assert_eq!(read(&again), payload(SMALL, 3));
    drop(again);
    assert_drained(&pool);
}

/// Evicts `handle`'s chunk and demotes it while `during` runs between the
/// demotion's unlock and its write.
fn demote_with_hook(pool: &Pool, handle: &Rc<ChunkHandle>, during: fn(&ChunkHandle)) {
    let in_hook = Rc::clone(handle);
    arm_demotion_hook(move || {
        assert!(in_hook.file_slot().is_none(), "the hook runs mid-demotion");
        during(&in_hook);
    });
    pool.evict(handle);
    assert!(handle.file_slot().is_some(), "the demotion committed");
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn plain_read_during_demotion() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let handle = Rc::new(insert(&pool, &payload(SMALL, 9)));
    demote_with_hook(&pool, &handle, |handle| {
        assert_eq!(read(handle), payload(SMALL, 9));
        let mut out = Vec::new();
        handle.read_range_into(13..113, &mut out);
        assert_eq!(out, &payload(SMALL, 9)[13..113]);
        assert_eq!(handle.residency(), Residency::Evicted);
    });
    let stats = pool.stats();
    assert_eq!(handle.residency(), Residency::Evicted);
    assert_eq!(stats.extent_file_reads, 0, "the reads decoded the arena");
    assert_eq!(stats.extent_pageouts, 1);
    assert_eq!(stats.extent_resident_bytes, 0);
    assert_eq!(read(&handle), payload(SMALL, 9));
    assert_eq!(pool.stats().extent_file_reads, 1);
    drop(Rc::into_inner(handle).expect("sole owner"));
    assert_eq!(pool.0.extent_arena.slots_in_use(), 0);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn admitting_read_during_demotion() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let handle = Rc::new(insert(&pool, &payload(SMALL, 10)));
    demote_with_hook(&pool, &handle, |handle| {
        assert_eq!(read_admit(handle), payload(SMALL, 10));
        assert_eq!(handle.residency(), Residency::BackedResident);
    });
    // The commit moved the admitted chunk's extent to file.
    let stats = pool.stats();
    assert_eq!(handle.residency(), Residency::BackedResident);
    assert_eq!(stats.extent_pageouts, 1);
    assert_eq!(stats.extent_resident_bytes, 0);
    assert_eq!(stats.extent_file_reads, 0);
    assert_eq!(read(&handle), payload(SMALL, 10), "served from the slot");
    // Evicting the backed chunk releases its slot without a new write.
    pool.evict(&handle);
    let after = pool.stats();
    assert_eq!(handle.residency(), Residency::Evicted);
    assert_eq!(after.evictions_cheap, stats.evictions_cheap + 1);
    assert_eq!(after.extent_file_writes, 1);
    assert_eq!(read(&handle), payload(SMALL, 10));
    drop(Rc::into_inner(handle).expect("sole owner"));
    assert_eq!(pool.0.extent_arena.slots_in_use(), 0);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn admitting_read_of_file_extent_backs_the_chunk() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let orig = payload(SMALL, 4);
    let handle = insert(&pool, &orig);
    pool.evict(&handle);
    assert!(handle.file_slot().is_some(), "demoted");
    assert_eq!(read_admit(&handle), orig);
    assert_eq!(handle.residency(), Residency::BackedResident);
    assert!(handle.file_slot().is_some(), "the extent stays on file");
    assert_eq!(pool.stats().extent_resident_bytes, 0);
    let before = pool.stats();
    pool.evict(&handle);
    let after = pool.stats();
    assert_eq!(handle.residency(), Residency::Evicted);
    assert_eq!(after.evictions_cheap, before.evictions_cheap + 1);
    assert_eq!(after.extent_file_writes, before.extent_file_writes);
    assert_eq!(after.extent_bytes_written, before.extent_bytes_written);
    assert_eq!(read(&handle), orig);
    drop(handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn repeat_reads_are_counted() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let orig = payload(SMALL, 5);
    let handle = insert(&pool, &orig);
    pool.evict(&handle);
    assert_eq!(read(&handle), orig);
    assert_eq!(read(&handle), orig);
    let stats = pool.stats();
    assert_eq!(stats.extent_file_reads, 2);
    assert_eq!(stats.extent_file_repeat_reads, 1);
    drop(handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn write_bytes_split_by_codec() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let identity = insert_with_codec(&pool, &payload(SMALL, 6), &IDENTITY_CODEC);
    let compressed = insert(&pool, &payload(SMALL, 7));
    pool.evict(&identity);
    let stats = pool.stats();
    assert!(stats.extent_file_write_bytes_identity > 0);
    assert_eq!(stats.extent_file_write_bytes_compressed, 0);
    pool.evict(&compressed);
    let stats = pool.stats();
    assert!(stats.extent_file_write_bytes_compressed > 0);
    assert_eq!(stats.extent_file_writes, 2);
    assert_eq!(read(&identity), payload(SMALL, 6));
    assert_eq!(read(&compressed), payload(SMALL, 7));
    drop(identity);
    drop(compressed);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn ranged_read_from_file_matches_full_read() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let orig = payload(SMALL, 33);
    let handle = insert(&pool, &orig);
    let ranges = [
        (0usize, 7usize),
        (13, 100),
        (SMALL - 9, 9),
        (0, SMALL),
        (5, 0),
    ];
    let check = |label: &str| {
        for (start, len) in ranges {
            let mut out = Vec::new();
            handle.read_range_into(start..start + len, &mut out);
            assert_eq!(
                out,
                &orig[start..start + len],
                "{label} range ({start}, {len})"
            );
        }
    };
    pool.evict(&handle);
    assert!(handle.file_slot().is_some(), "demoted");
    check("file");
    assert_eq!(
        handle.residency(),
        Residency::Evicted,
        "plain ranged reads do not admit"
    );
    let mut out = Vec::new();
    handle.read_range_into_admit(3..19, &mut out);
    assert_eq!(out, &orig[3..19]);
    assert_eq!(handle.residency(), Residency::BackedResident);
    check("backed");
    drop(handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn swap_pool_reports_swap_backend() {
    assert_eq!(Pool::new().expect("pool").backend_kind(), BackendKind::Swap);
    let pool = Pool::with_class_capacity(64 << 20).expect("pool");
    assert_eq!(pool.backend_kind(), BackendKind::Swap);
    pool.set_budget(256 << 20);
    pool.set_rss_target(1 << 30);
    let orig = payload(SMALL, 8);
    let handle = insert(&pool, &orig);
    pool.evict(&handle);
    assert_eq!(read(&handle), orig);
    pool.set_rss_target(0);
    assert_eq!(read(&handle), orig);
    drop(handle);
    let stats = pool.stats();
    assert_eq!(stats.extent_file_bytes, 0);
    assert_eq!(stats.extent_file_capacity_bytes, 0);
    assert_eq!(stats.extent_file_writes, 0);
    assert_eq!(stats.extent_file_writes_inline, 0);
    assert_eq!(stats.extent_file_write_bytes_identity, 0);
    assert_eq!(stats.extent_file_write_bytes_compressed, 0);
    assert_eq!(stats.extent_file_reads, 0);
    assert_eq!(stats.extent_file_read_bytes, 0);
    assert_eq!(stats.extent_file_repeat_reads, 0);
    assert_eq!(stats.extent_file_full, 0);
    assert_eq!(stats.extent_file_write_errors, 0);
    assert_eq!(stats.extent_file_holes_punched_bytes, 0);
    assert_eq!(stats.extent_file_read_latency, [0; 13]);
    assert_drained(&pool);
}

/// Words of a constant, which compress into the smallest file class.
fn flat(words: usize, seed: u64) -> Vec<u64> {
    vec![seed; words]
}

/// The file class size `data` occupies once stored through `codec`.
fn stored_class_size(data: &[u64], codec: &'static dyn ExtentCodec) -> usize {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    pool.set_rss_target(1 << 30);
    let handle = insert_with_codec(&pool, data, codec);
    pool.evict(&handle);
    let comp_len = handle
        .meta
        .state()
        .extent
        .as_ref()
        .expect("evicted chunk has an extent")
        .comp_len();
    let store = pool.file_store().expect("file mode");
    store.class_size(store.class_for(comp_len).expect("a class fits"))
}

fn is_arena(handle: &ChunkHandle) -> bool {
    handle
        .meta
        .state()
        .extent
        .as_ref()
        .is_some_and(|extent| extent.is_reclaimable_arena())
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn full_store_keeps_extents_in_ram_and_resumes() {
    let (a, b) = (payload(SMALL, 11), payload(SMALL, 12));
    let slot = stored_class_size(&a, &TEST_CODEC);
    assert_eq!(stored_class_size(&b, &TEST_CODEC), slot, "same class");
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(slot));
    let first = insert(&pool, &a);
    let second = insert(&pool, &b);
    pool.evict(&first);
    assert!(first.file_slot().is_some(), "the only slot holds the first");
    pool.evict(&second);
    assert!(second.file_slot().is_none(), "no slot for the second");
    assert!(is_arena(&second));
    let stats = pool.stats();
    assert!(stats.extent_resident_bytes > 0, "the second stays in RAM");
    assert!(stats.extent_file_full >= 1);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));
    assert_eq!(read(&first), a);
    assert_eq!(read(&second), b);

    drop(first);
    assert!(
        !pool.0.full_hint.load(Ordering::Relaxed),
        "a returned file slot clears the hint"
    );
    pool.enforce_compressed();
    assert!(
        second.file_slot().is_some(),
        "the freed slot took the second"
    );
    assert_eq!(pool.stats().extent_resident_bytes, 0);
    assert_eq!(read(&second), b);
    drop(second);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn full_store_does_not_spin_backstop() {
    let slot = stored_class_size(&flat(SMALL, 0), &TEST_CODEC);
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(slot));
    let filler = insert(&pool, &flat(SMALL, 0));
    pool.evict(&filler);
    assert!(
        filler.file_slot().is_some(),
        "the filler takes the only slot"
    );
    let full = pool.stats().extent_file_full;
    let probes = demotion_probes();
    const INSERTS: u64 = 1000;
    let handles: Vec<_> = (1..=INSERTS)
        .map(|seed| {
            let handle = insert(&pool, &flat(SMALL, seed));
            pool.evict(&handle);
            handle
        })
        .collect();
    // The first pass probes once, stops, and sets the hint. Every later
    // insert and eviction skips the pass while the hint holds.
    assert_eq!(pool.stats().extent_file_full - full, 1);
    assert_eq!(demotion_probes() - probes, 1);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));
    assert_eq!(pool.stats().extent_file_writes, 1, "only the filler");
    for (seed, handle) in (1..=INSERTS).zip_eq(&handles) {
        assert!(handle.file_slot().is_none());
        assert_eq!(read(handle), flat(SMALL, seed));
    }
    drop(handles);
    drop(filler);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn write_error_keeps_extent_readable() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let orig = payload(SMALL, 13);
    let handle = insert(&pool, &orig);
    file::fault::fail_next(file::fault::Op::Write, libc::EIO);
    pool.evict(&handle);
    assert!(handle.file_slot().is_none(), "the demotion aborted");
    assert!(is_arena(&handle), "the extent stays in the arena");
    let stats = pool.stats();
    assert_eq!(stats.extent_file_write_errors, 1);
    assert_eq!(stats.extent_file_writes, 0);
    assert_eq!(stats.extent_pageouts, 0);
    assert!(stats.extent_resident_bytes > 0);
    assert_eq!(stats.extent_file_bytes, 0);
    assert_eq!(read(&handle), orig);
    let store = pool.file_store().expect("file mode");
    assert!(store.writes_disabled());

    // Inline passes find writes disabled and write nothing.
    let more: Vec<_> = (0..4)
        .map(|seed| {
            let handle = insert(&pool, &payload(SMALL, 100 + seed));
            pool.evict(&handle);
            handle
        })
        .collect();
    let stats = pool.stats();
    assert_eq!(stats.extent_file_writes, 0, "writes stay disabled");
    assert_eq!(stats.extent_file_write_errors, 1);
    assert!(more.iter().all(is_arena));

    // With spill threads present, the inline backstop stays off.
    pool.fake_spill_threads();
    let full = stats.extent_file_full;
    let probes = demotion_probes();
    let last = insert(&pool, &payload(SMALL, 200));
    pool.evict(&last);
    assert_eq!(pool.stats().extent_file_full, full, "no inline pass");
    assert_eq!(demotion_probes(), probes, "no inline pass");
    assert_eq!(read(&last), payload(SMALL, 200));
    assert_eq!(read(&handle), orig);
    drop(last);
    drop(more);
    drop(handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[should_panic(expected = "checksum")]
fn corrupt_extent_panics_on_read() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let handle = insert(&pool, &payload(SMALL, 14));
    pool.evict(&handle);
    assert!(handle.file_slot().is_some(), "demoted");
    handle.corrupt_file_extent();
    read(&handle);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
#[should_panic(expected = "short read")]
fn short_read_panics() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let handle = insert(&pool, &payload(SMALL, 15));
    pool.evict(&handle);
    assert!(handle.file_slot().is_some(), "demoted");
    file::fault::fail_next(file::fault::Op::Read, 0);
    read(&handle);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn unplaceable_head_does_not_block_smaller_class() {
    let large = payload(SMALL, 16);
    let small = flat(SMALL, 17);
    let small_slot = stored_class_size(&small, &TEST_CODEC);
    assert!(stored_class_size(&large, &IDENTITY_CODEC) > small_slot);
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(small_slot));
    pool.set_rss_target(1 << 30);
    let large_handle = insert_with_codec(&pool, &large, &IDENTITY_CODEC);
    let small_handle = insert(&pool, &small);
    pool.evict(&large_handle);
    pool.evict(&small_handle);
    assert_eq!(pool.stats().extent_pageouts, 0);

    // The large extent heads the queue, and its class cannot fit.
    pool.set_rss_target(0);
    assert!(large_handle.file_slot().is_none(), "no room for the large");
    assert!(small_handle.file_slot().is_some(), "the small one demoted");
    let stats = pool.stats();
    assert_eq!(stats.extent_pageouts, 1);
    assert_eq!(stats.extent_file_full, 1, "the pass refused the large");
    assert!(
        !pool.0.full_hint.load(Ordering::Relaxed),
        "the pass demoted something"
    );

    // No class can allocate now, so the next pass stops at once.
    let probes = demotion_probes();
    pool.enforce_compressed();
    assert_eq!(demotion_probes() - probes, 1);
    assert_eq!(pool.stats().extent_file_full, 2);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));
    assert_eq!(read(&large_handle), large);
    assert_eq!(read(&small_handle), small);
    drop(large_handle);
    drop(small_handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn inline_pass_stops_at_twice_the_cap() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    pool.set_rss_target(1 << 30);
    const CHUNKS: u64 = 8;
    let handles: Vec<_> = (0..CHUNKS)
        .map(|seed| {
            let handle = insert(&pool, &flat(SMALL, seed));
            pool.evict(&handle);
            handle
        })
        .collect();
    let resident = pool.stats().extent_resident_bytes;
    let extent = resident / CHUNKS;
    assert_eq!(extent * CHUNKS, resident, "equal extents");
    let floor = (1 << 30) - pool.0.compressed_cap();
    let cap = extent + extent / 2;

    pool.fake_spill_threads();
    pool.set_rss_target(usize::cast_from(floor + cap));
    assert_eq!(pool.0.compressed_cap(), cap);
    let stats = pool.stats();
    assert_eq!(
        stats.extent_resident_bytes,
        3 * extent,
        "an inline pass stops at twice the cap"
    );
    assert_eq!(stats.extent_file_writes, CHUNKS - 3);
    assert_eq!(stats.extent_file_writes_inline, CHUNKS - 3);

    pool.0.enforce_compressed_cap(Pass::Background);
    let stats = pool.stats();
    assert_eq!(
        stats.extent_resident_bytes, extent,
        "a spill thread's pass goes down to the cap"
    );
    assert_eq!(stats.extent_file_writes, CHUNKS - 1);
    assert_eq!(stats.extent_file_writes_inline, CHUNKS - 3);
    for (seed, handle) in (0..CHUNKS).zip_eq(&handles) {
        assert_eq!(read(handle), flat(SMALL, seed));
    }
    drop(handles);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn full_store_defers_backstop_to_spill_threads() {
    let slot = stored_class_size(&flat(SMALL, 0), &TEST_CODEC);
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(slot));
    let filler = insert(&pool, &flat(SMALL, 0));
    pool.evict(&filler);
    assert!(
        filler.file_slot().is_some(),
        "the filler takes the only slot"
    );
    pool.fake_spill_threads();

    // The first over-threshold eviction runs the backstop and finds the
    // store full.
    let first = insert(&pool, &flat(SMALL, 1));
    pool.evict(&first);
    assert!(first.file_slot().is_none());
    assert_eq!(pool.stats().extent_file_full, 1);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));

    // Further callers leave the full store to the spill threads.
    let probes = demotion_probes();
    let more: Vec<_> = (2..10)
        .map(|seed| {
            let handle = insert(&pool, &flat(SMALL, seed));
            pool.evict(&handle);
            handle
        })
        .collect();
    assert_eq!(demotion_probes(), probes, "no inline pass while full");
    assert_eq!(pool.stats().extent_file_full, 1);

    // A returned file slot re-arms the backstop.
    drop(filler);
    let last = insert(&pool, &flat(SMALL, 10));
    pool.evict(&last);
    assert!(demotion_probes() > probes, "the backstop ran");
    assert_eq!(pool.stats().extent_file_writes, 2);
    assert_eq!(read(&first), flat(SMALL, 1));
    drop(first);
    drop(more);
    drop(last);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn write_error_with_read_in_flight_keeps_extent_in_arena() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let orig = payload(SMALL, 18);
    let handle = Rc::new(insert(&pool, &orig));
    let in_hook = Rc::clone(&handle);
    let expected = orig.clone();
    arm_demotion_hook(move || {
        assert!(in_hook.file_slot().is_none(), "the hook runs mid-demotion");
        assert_eq!(read(&in_hook), expected);
        assert_eq!(in_hook.residency(), Residency::Evicted);
    });
    file::fault::fail_next(file::fault::Op::Write, libc::EIO);
    pool.evict(&handle);
    assert!(handle.file_slot().is_none(), "the demotion aborted");
    assert!(is_arena(&handle), "the extent returned to the arena");
    let stats = pool.stats();
    assert_eq!(stats.extent_file_write_errors, 1);
    assert_eq!(stats.extent_pageouts, 0);
    assert_eq!(stats.extent_file_bytes, 0);
    assert!(stats.extent_resident_bytes > 0);
    assert_eq!(read(&handle), orig);
    assert_eq!(pool.stats().extent_file_reads, 0, "served from the arena");
    drop(Rc::into_inner(handle).expect("sole owner"));
    assert_eq!(pool.0.extent_arena.slots_in_use(), 0);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn partly_full_store_probes_once_per_class() {
    let small = flat(SMALL, 19);
    let small_slot = stored_class_size(&small, &TEST_CODEC);
    let large: Vec<_> = (0..16).map(|seed| payload(SMALL, 300 + seed)).collect();
    assert!(stored_class_size(&large[0], &IDENTITY_CODEC) > small_slot);
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(small_slot));

    // Leave a warm slot of the small class, which nothing queued uses.
    let warm = insert(&pool, &small);
    pool.evict(&warm);
    assert!(warm.file_slot().is_some());
    drop(warm);
    let store = pool.file_store().expect("file mode");
    assert!(store.can_alloc_any(), "the warm slot can take its class");

    pool.set_rss_target(1 << 30);
    let handles: Vec<_> = large
        .iter()
        .map(|data| {
            let handle = insert_with_codec(&pool, data, &IDENTITY_CODEC);
            pool.evict(&handle);
            handle
        })
        .collect();
    assert_eq!(pool.extent_queue_len(), large.len());

    let probes = demotion_probes();
    pool.set_rss_target(0);
    assert_eq!(
        demotion_probes() - probes,
        1,
        "one probe for the one refused class, not one per entry"
    );
    let stats = pool.stats();
    assert_eq!(stats.extent_file_full, 1);
    assert_eq!(stats.extent_pageouts, 1, "only the warm-slot chunk");
    assert!(pool.0.full_hint.load(Ordering::Relaxed), "nothing demoted");
    assert_eq!(pool.extent_queue_len(), large.len(), "every entry kept");

    // Inline callers skip the pass while the hint holds.
    let probes = demotion_probes();
    let extra = insert_with_codec(&pool, &payload(SMALL, 400), &IDENTITY_CODEC);
    pool.evict(&extra);
    assert_eq!(demotion_probes(), probes, "no inline pass");

    // A spill thread retries only once the retry deadline passes.
    let mut retry = std::time::Instant::now() + std::time::Duration::from_secs(3600);
    for _ in 0..10 {
        pool.0.spill_trim(&mut retry);
    }
    assert_eq!(demotion_probes(), probes, "wakeups run no pass");
    let mut retry = std::time::Instant::now();
    pool.0.spill_trim(&mut retry);
    assert_eq!(demotion_probes() - probes, 1, "the retry probes once");
    assert!(
        retry > std::time::Instant::now(),
        "the next retry is deferred"
    );
    pool.0.spill_trim(&mut retry);
    assert_eq!(demotion_probes() - probes, 1);

    for (data, handle) in large.iter().zip_eq(&handles) {
        assert_eq!(&read(handle), data);
    }
    drop(handles);
    drop(extra);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn spill_jobs_skip_the_pass_while_full() {
    let filler_data = flat(SMALL, 23);
    let slot = stored_class_size(&filler_data, &TEST_CODEC);
    let large = payload(SMALL, 600);
    assert!(stored_class_size(&large, &IDENTITY_CODEC) > slot);
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(slot));
    let filler = insert(&pool, &filler_data);
    pool.evict(&filler);
    assert!(
        filler.file_slot().is_some(),
        "the filler takes the only slot"
    );
    let refused = insert_with_codec(&pool, &large, &IDENTITY_CODEC);
    pool.evict(&refused);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));
    let full = pool.stats().extent_file_full;

    pool.enable_spill_without_threads();
    let probes = demotion_probes();
    let evicted: Vec<_> = (0..4)
        .map(|seed| {
            let handle = insert_with_codec(&pool, &payload(SMALL, 610 + seed), &IDENTITY_CODEC);
            pool.evict(&handle);
            assert_eq!(handle.residency(), Residency::WriteInFlight);
            handle
        })
        .collect();
    let backed = insert_with_codec(&pool, &payload(SMALL, 620), &IDENTITY_CODEC);
    let mut jobs = 0;
    while pool.spill_step() {
        jobs += 1;
    }
    assert_eq!(jobs, evicted.len());
    assert!(pool.back_step(), "the unbacked chunk is backed");
    assert_eq!(backed.residency(), Residency::BackedResident);
    assert!(evicted.iter().all(is_arena));
    assert_eq!(demotion_probes(), probes, "spill jobs run no pass");
    assert_eq!(pool.stats().extent_file_full, full);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));

    for (seed, handle) in (610..614).zip_eq(&evicted) {
        assert_eq!(read(handle), payload(SMALL, seed));
    }
    assert_eq!(read(&backed), payload(SMALL, 620));
    assert_eq!(read(&refused), large);
    drop(evicted);
    drop(backed);
    drop(refused);
    drop(filler);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn placeable_enqueue_clears_hint_without_spill_threads() {
    let small = flat(SMALL, 21);
    let small_slot = stored_class_size(&small, &TEST_CODEC);
    let large: Vec<_> = (0..4).map(|seed| payload(SMALL, 500 + seed)).collect();
    assert!(stored_class_size(&large[0], &IDENTITY_CODEC) > small_slot);
    let (_dir, pool) = file_pool(256 << 20, u64::cast_from(small_slot));

    // Leave a warm slot of the small class.
    let warm = insert(&pool, &flat(SMALL, 20));
    pool.evict(&warm);
    assert!(warm.file_slot().is_some());
    drop(warm);

    // A large-only queue refuses every entry and sets the hint.
    pool.set_rss_target(1 << 30);
    let handles: Vec<_> = large
        .iter()
        .map(|data| {
            let handle = insert_with_codec(&pool, data, &IDENTITY_CODEC);
            pool.evict(&handle);
            handle
        })
        .collect();
    pool.set_rss_target(0);
    assert!(pool.0.full_hint.load(Ordering::Relaxed));
    let frees = pool.stats().frees;

    // A small extent fits the warm slot: its enqueue clears the hint, and
    // the eviction's inline pass demotes it.
    let handle = insert(&pool, &small);
    assert!(pool.0.full_hint.load(Ordering::Relaxed), "insert keeps it");
    pool.evict(&handle);
    assert!(handle.file_slot().is_some(), "the small extent demoted");
    assert_eq!(pool.stats().frees, frees, "no file extent was freed");
    assert!(handles.iter().all(|handle| handle.file_slot().is_none()));
    assert_eq!(read(&handle), small);
    for (data, handle) in large.iter().zip_eq(&handles) {
        assert_eq!(&read(handle), data);
    }
    drop(handles);
    drop(handle);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn write_enospc_refuses_without_disabling_writes() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let orig = payload(SMALL, 24);
    let handle = insert(&pool, &orig);
    file::fault::fail_next(file::fault::Op::Write, libc::ENOSPC);
    pool.evict(&handle);
    assert!(handle.file_slot().is_none(), "the demotion aborted");
    assert!(is_arena(&handle), "the extent stays in the arena");
    let stats = pool.stats();
    assert_eq!(stats.extent_file_write_errors, 0);
    assert_eq!(stats.extent_file_writes, 0);
    assert_eq!(stats.extent_file_full, 1);
    assert_eq!(stats.extent_file_bytes, 0);
    assert!(
        stats.extent_file_holes_punched_bytes > 0,
        "the slot returned"
    );
    let store = pool.file_store().expect("file mode");
    assert!(!store.writes_disabled());
    assert_eq!(store.allocated_bytes(), 0);
    assert_eq!(store.capacity_bytes(), 0, "capacity latched");
    assert!(
        pool.0.full_hint.load(Ordering::Relaxed),
        "no class can be placed"
    );
    assert_eq!(read(&handle), orig);
    drop(handle);
    assert_eq!(pool.0.extent_arena.slots_in_use(), 0);
    assert_drained(&pool);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)]
fn free_during_enospc_write_returns_the_file_slot_once() {
    let (_dir, pool) = file_pool(256 << 20, 64 << 20);
    let store = pool.file_store().expect("file mode");
    let handle = insert(&pool, &payload(SMALL, 25));
    let probe = Arc::clone(&handle.meta);
    arm_demotion_hook(move || drop(handle));
    file::fault::fail_next(file::fault::Op::Write, libc::ENOSPC);
    {
        let mut state = probe.state();
        probe.pool.evict_locked(&probe, &mut state);
    }
    probe.pool.enforce_or_defer_compressed_cap();
    assert_eq!(pool.stats().frees, 1, "the hook dropped the handle");
    assert!(probe.state().extent.is_none(), "the demoter dropped it");
    drop(probe);
    assert_eq!(store.slots_in_use(), 0);
    assert_eq!(store.allocated_bytes(), 0, "the punched slot is cold");
    assert_eq!(pool.0.extent_arena.slots_in_use(), 0);
    assert_drained(&pool);
}

/// Races demotions and their commits, frees during writes, file reads, and
/// admissions across worker threads, an enforcer, and two spill threads
/// doing eviction, eager backing, and compressed-tier trims. Every read is
/// verified, and the pool must drain to zero in both stores.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn concurrent_file_mode_churn() {
    const WORKERS: u64 = 3;
    const ROUNDS: u64 = 600;
    // Four small chunks of budget keep every insert evicting. The store
    // holds fewer extents than the workers keep alive, so demotions also
    // meet a full store.
    let (_dir, pool) = file_pool(4 * (64 << 10), 2 << 20);
    pool.set_spill_threads(2);
    pool.set_eager_backing(true);
    let store = pool.file_store().expect("file mode");
    let shared: Arc<Vec<(Vec<u64>, ChunkHandle)>> = Arc::new(
        (0..8u64)
            .map(|seed| {
                let data = payload(SMALL, 900 + seed);
                let handle = insert(&pool, &data);
                (data, handle)
            })
            .collect(),
    );
    let done = Arc::new(AtomicBool::new(false));
    let enforcer = {
        let pool = pool.clone();
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut round = 0u64;
            while !done.load(Ordering::Relaxed) {
                pool.enforce_budget();
                // Alternate between a tier that holds a few extents and none,
                // so extents both linger in the arena and demote.
                let target = if round / 8 % 2 == 0 { 8 << 20 } else { 0 };
                pool.set_rss_target(target);
                pool.enforce_compressed();
                round += 1;
            }
        })
    };
    let workers: Vec<_> = (0..WORKERS)
        .map(|t| {
            let pool = pool.clone();
            let shared = Arc::clone(&shared);
            std::thread::spawn(move || {
                let mut local: VecDeque<(Vec<u64>, ChunkHandle)> = VecDeque::new();
                for round in 0..ROUNDS {
                    let seed = t * 10_000 + round;
                    let codec: &'static dyn ExtentCodec = if round % 2 == 0 {
                        &TEST_CODEC
                    } else {
                        &IDENTITY_CODEC
                    };
                    let data = payload(SMALL, seed);
                    let handle = insert_with_codec(&pool, &data, codec);
                    if round % 3 == 0 {
                        pool.evict(&handle);
                    }
                    if round % 5 == 1 {
                        // Freed while queued for a spill thread, or while its
                        // extent waits in the arena or is being written.
                        pool.evict(&handle);
                        drop(handle);
                        continue;
                    }
                    local.push_back((data, handle));
                    let index = usize::cast_from(round * 7) % local.len();
                    let (data, handle) = &local[index];
                    if round % 4 == 0 {
                        assert_eq!(&read_admit(handle), data);
                    } else {
                        assert_eq!(&read(handle), data);
                    }
                    let (data, handle) = &shared[usize::cast_from(round + t) % shared.len()];
                    if round % 5 == 0 {
                        assert_eq!(&read_admit(handle), data);
                    } else {
                        assert_eq!(&read(handle), data);
                    }
                    // Drop the oldest, which is likely mid-demotion or on
                    // file by now.
                    if local.len() > 16 {
                        local.pop_front();
                    }
                }
                for (data, handle) in &local {
                    assert_eq!(&read(handle), data);
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("worker thread panicked");
    }
    done.store(true, Ordering::Relaxed);
    enforcer.join().expect("enforcer thread panicked");
    pool.quiesce_spill();
    pool.join_spill_threads();
    // With every thread stopped, the live extents' file bytes are the
    // counter, per the invariant on `note_extent_resident`.
    let live_file_bytes: usize = shared
        .iter()
        .filter_map(|(_, handle)| handle.meta.state().extent.as_ref()?.file_bytes())
        .sum();
    assert_eq!(
        pool.stats().extent_file_bytes,
        u64::cast_from(live_file_bytes)
    );
    assert_eq!(
        pool.stats().extent_file_allocated_bytes,
        store.allocated_bytes()
    );
    for (data, handle) in shared.iter() {
        assert_eq!(&read(handle), data);
    }
    drop(shared);

    let stats = pool.stats();
    assert!(stats.extent_file_writes > 0, "demotions ran");
    assert!(stats.extent_file_reads > 0, "file reads ran");
    assert_eq!(stats.extent_file_write_errors, 0);
    assert!(!store.writes_disabled());
    assert_eq!(stats.live_chunks, 0);
    assert_eq!(stats.resident_bytes, 0);
    assert_drained(&pool);
    assert_eq!(pool.0.extent_arena.slots_in_use(), 0);
    assert_eq!(store.slots_in_use(), 0);
}
