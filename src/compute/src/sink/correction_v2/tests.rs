// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::Metrics;
use mz_repr::{Diff, Timestamp};

use super::*;
use crate::sink::correction::CorrectionV1;

#[mz_ore::test]
fn chain_builder_update_count_matches_items() {
    let mut builder = ChainBuilder::<i64>::default();
    for i in 0..10_u64 {
        let d = i64::try_from(i).expect("fits");
        builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
    }
    let chain = builder.finish();
    assert_eq!(chain.update_count, chain.iter().count());
}

/// Push enough updates to cross at least one `mint()` boundary, forcing the
/// `Align` encode -> `from_bytes` decode roundtrip (the spilling path this data
/// structure exists to support), and assert `iter()` roundtrips values, order,
/// and diffs across the spill boundary.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow: crossing the ~2 MiB mint boundary needs ~200k updates
fn chain_builder_roundtrips_across_mint_boundary() {
    // A single `mint()` fires near the ~2 MiB (`SHIP_WORDS`) serialized boundary. With
    // three 8-byte columns per update that's tens of thousands of updates; pushing 200k
    // comfortably forces multiple mints.
    let count = 200_000_u64;

    let mut builder = ChainBuilder::<i64>::default();
    for i in 0..count {
        let d = i64::try_from(i).expect("fits");
        builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
    }
    let chain = builder.finish();

    // Crossing the mint boundary must have produced more than one chunk; otherwise the spill
    // path (each minted chunk is paged out and read back through the pager) wouldn't be
    // exercised. The chunk payload itself is now behind the pager (see [`Chunk`]), so we
    // assert on chunk count rather than inspecting the column variant directly.
    assert!(
        chain.chunks.len() > 1,
        "expected multiple minted chunks, got {} chunk(s): {:?}",
        chain.chunks.len(),
        chain.chunks,
    );

    // `iter()` must roundtrip every update, in order, with correct diffs.
    assert_eq!(chain.update_count, usize::try_from(count).expect("fits"));
    let mut expected = 0_u64;
    for (d, t, r) in chain.iter() {
        assert_eq!(d, i64::try_from(expected).expect("fits"));
        assert_eq!(t, Timestamp::new(expected));
        assert_eq!(r, Diff::ONE);
        expected += 1;
    }
    assert_eq!(expected, count);
}

impl DataBytes for String {
    fn data_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.len()
    }
}

impl DataBytes for i64 {
    fn data_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

/// A k-way merge over chains of several chunks each consolidates across all of them, with
/// its rounds ending wherever any input crosses a chunk boundary.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri
fn merge_many_consolidates_across_chunks() {
    // Large enough that the sparsest chain still spans several chunks.
    let count = 600_000_i64;
    // Chain `k` holds the updates of `inputs[k]`, sorted by (time, data).
    let inputs: [Vec<(i64, Timestamp, Diff)>; 3] = [
        (0..count)
            .map(|d| (d, Timestamp::new(0), Diff::ONE))
            .collect(),
        (0..count)
            .filter(|d| d % 2 == 0)
            .map(|d| (d, Timestamp::new(0), Diff::ONE))
            .collect(),
        (0..count)
            .filter(|d| d % 3 == 0)
            .map(|d| (d, Timestamp::new(0), -Diff::ONE))
            .collect(),
    ];

    let mut expected = std::collections::BTreeMap::new();
    for (d, _, r) in inputs.iter().flatten() {
        *expected.entry(*d).or_insert(Diff::ZERO) += *r;
    }
    expected.retain(|_, r| *r != Diff::ZERO);

    let cursors = inputs
        .iter()
        .map(|updates| {
            let mut builder = ChainBuilder::<i64>::default();
            for update in updates {
                builder.push_owned(update);
            }
            let chain = builder.finish();
            assert!(chain.chunks.len() > 1, "expected multiple minted chunks");
            chain.into_cursor().expect("non-empty")
        })
        .collect();
    let merged = merge_many(cursors, 0);

    let actual: Vec<_> = merged.iter().map(|(d, _, r)| (d, r)).collect();
    let expected: Vec<_> = expected.into_iter().collect();
    assert_eq!(actual, expected);
}

fn sink_metrics() -> SinkMetrics {
    let registry = MetricsRegistry::new();
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
    metrics.sink.clone()
}

/// Insert single updates of the given payload width until the stage ships, returning how
/// many updates that took.
fn updates_until_ship(width: usize, chunk_size: usize) -> usize {
    let metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        metrics.clone(),
        metrics.for_worker(0),
        None,
        3.0,
        chunk_size,
    );
    for i in 0..chunk_size {
        let datum = format!("{i:0width$}");
        v2.insert(&mut vec![(datum, Timestamp::from(0), Diff::ONE)]);
        if v2.stage.data.is_empty() {
            assert_eq!(v2.stage.bytes, 0, "a shipped stage holds no bytes");
            return i + 1;
        }
    }
    panic!("the stage never shipped");
}

/// The stage ships by bytes, heap included, so wider rows ship after fewer updates.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri
fn stage_ships_by_bytes() {
    let chunk_size = 16 * 1024;
    let narrow = updates_until_ship(8, chunk_size);
    let wide = updates_until_ship(1024, chunk_size);

    let per_update = |width: usize| {
        std::mem::size_of::<String>() + width + std::mem::size_of::<(Timestamp, Diff)>()
    };
    assert_eq!(narrow, chunk_size.div_ceil(per_update(8)));
    assert_eq!(wide, chunk_size.div_ceil(per_update(1024)));
    assert!(wide < narrow);
}

/// Run the same stepwise-drain workload through `CorrectionV1` and `CorrectionV2` and assert
/// that they emit the same updates at every step.
///
/// Models the `write_batches` operator catching up through many distinct timestamps: the
/// desired input runs ahead, batches are written one timestamp at a time, and written updates
/// come back negated through the persist feedback.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn equivalence_with_v1() {
    let sink_metrics = sink_metrics();

    let mut v1 = CorrectionV1::<String>::new(sink_metrics.clone(), sink_metrics.for_worker(0), 1);
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    let num_ts = 50;
    let keys = 4;

    // Upsert-style input: every timestamp updates each key, retracting the previous value.
    let batch = |t: u64| -> Vec<(String, Timestamp, Diff)> {
        (0..keys)
            .flat_map(|k| {
                let addition = (format!("{k}-{t}"), Timestamp::from(t), Diff::ONE);
                let retraction = t
                    .checked_sub(1)
                    .map(|p| (format!("{k}-{p}"), Timestamp::from(t), -Diff::ONE));
                std::iter::once(addition).chain(retraction)
            })
            .collect()
    };

    // Pre-fill both with all batches, like a catch-up where the input runs ahead.
    for t in 0..num_ts {
        v1.insert(&mut batch(t));
        v2.insert(&mut batch(t));
    }

    // Drain stepwise, with persist feedback, comparing emissions.
    for t in 0..num_ts {
        let upper = Antichain::from_elem(Timestamp::from(t + 1));

        let mut out1: Vec<_> = v1.updates_before(&upper).collect();
        let mut out2: Vec<_> = v2.updates_before(&upper).collect();
        out1.sort();
        out2.sort();
        assert_eq!(out1, out2, "diverged at t={t}");

        v1.insert_negated(&mut out1.clone());
        v2.insert_negated(&mut out2);
        v1.advance_since(upper.clone());
        v2.advance_since(upper);
    }

    // Compare the final state at the since.
    let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
    v1.consolidate_at_since();
    v2.consolidate_at_since();
    let mut out1: Vec<_> = v1.updates_before(&upper).collect();
    let mut out2: Vec<_> = v2.updates_before(&upper).collect();
    out1.sort();
    out2.sort();
    assert_eq!(out1, out2);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri, and the sort and merge it checks have no unsafe code
fn sort_advanced_consolidates_across_runs() {
    // Each time carries an insertion and, at a later time, its retraction, so with runs of
    // a few updates the pair lands in different runs and only the merge can cancel it.
    let since_ts = Timestamp::from(100);
    let mut updates = Vec::new();
    for i in 0..50_i64 {
        let t = u64::try_from(i).expect("fits");
        updates.push((i, Timestamp::from(t), Diff::ONE));
        updates.push((i, Timestamp::from(t + 50), -Diff::ONE));
        updates.push((i, Timestamp::from(t), Diff::ONE));
    }
    // Times beyond the since keep their own time.
    updates.push((7, Timestamp::from(200), Diff::ONE));
    let merged = sort_advanced(chains_of(&updates, 9), since_ts, 4);

    let mut expected: Vec<_> = updates
        .into_iter()
        .map(|(d, t, r)| (d, std::cmp::max(t, since_ts), r))
        .collect();
    consolidate(&mut expected);
    assert_eq!(merged.iter().collect::<Vec<_>>(), expected);
    assert_eq!(
        merged.update_count, 51,
        "50 surviving insertions and the late update"
    );
}

/// Builds one chain per `per_chain` updates, each sorted and consolidated on its own.
fn chains_of(updates: &[(i64, Timestamp, Diff)], per_chain: usize) -> Vec<Chain<i64>> {
    updates
        .chunks(per_chain)
        .map(|part| {
            let mut part = part.to_vec();
            consolidate(&mut part);
            mint_run(&mut part)
        })
        .collect()
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri, and the sort and merge it checks have no unsafe code
fn sort_advanced_matches_one_sort() {
    // Insertions spread over many times, a share of them retracted at a later time, so runs
    // both survive consolidation and cancel across runs at several merge levels.
    let since_ts = Timestamp::from(500);
    let mut updates = Vec::new();
    for i in 0..2_000_i64 {
        let t = u64::try_from(i % 997).expect("fits");
        updates.push((i % 300, Timestamp::from(t), Diff::ONE));
        if i % 3 == 0 {
            updates.push((i % 300, Timestamp::from(t + 3), -Diff::ONE));
        }
    }
    let mut expected: Vec<_> = updates
        .iter()
        .map(|(d, t, r)| (*d, std::cmp::max(*t, since_ts), *r))
        .collect();
    consolidate(&mut expected);

    for run_len in [1, 2, 7, 64, 100_000] {
        let merged = sort_advanced(chains_of(&updates, 37), since_ts, run_len);
        assert_eq!(
            merged.iter().collect::<Vec<_>>(),
            expected,
            "run_len {run_len}"
        );
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // the pool's mapped regions are unsupported under miri
fn sort_advanced_matches_one_sort_through_pool() {
    // Enough updates that input chains and runs mint chunks large enough to spill, so the sort
    // reads spilled input and merges spilled runs.
    let since_ts = Timestamp::from(5_000);
    let mut updates = Vec::new();
    for i in 0..400_000_i64 {
        let t = u64::try_from(i % 9_973).expect("fits");
        updates.push((i % 50_000, Timestamp::from(t), Diff::ONE));
        if i % 3 == 0 {
            updates.push((i % 50_000, Timestamp::from(t + 7), -Diff::ONE));
        }
    }
    let mut expected: Vec<_> = updates
        .iter()
        .map(|(d, t, r)| (*d, std::cmp::max(*t, since_ts), *r))
        .collect();
    consolidate(&mut expected);

    with_spill_pool(|| {
        let chains = chains_of(&updates, 100_000);
        for chain in &chains {
            assert_all_spilled(chain);
        }
        let merged = sort_advanced(chains, since_ts, 1 << 12);
        assert_eq!(merged.iter().collect::<Vec<_>>(), expected);
    });
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri, and the sort and merge it checks have no unsafe code
fn sort_advanced_cancels_to_nothing() {
    let since_ts = Timestamp::from(10);
    let updates: Vec<_> = (0..100_i64)
        .flat_map(|i| {
            let t = Timestamp::from(u64::try_from(i % 10).expect("fits"));
            [
                (i, t, Diff::ONE),
                (i, t.max(Timestamp::from(5)), -Diff::ONE),
            ]
        })
        .collect();
    let merged = sort_advanced(chains_of(&updates, 13), since_ts, 8);
    assert!(merged.is_empty(), "every insertion meets its retraction");
}

/// A since jump across many distinct buffered timestamps must collapse them onto the since.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn since_jump() {
    let sink_metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    let num_ts = 100;
    for t in 0..num_ts {
        v2.insert(&mut vec![
            (format!("a-{t}"), Timestamp::from(t), Diff::ONE),
            (format!("a-{t}"), Timestamp::from(t), -Diff::ONE),
            (format!("b-{t}"), Timestamp::from(t), Diff::ONE),
        ]);
    }

    v2.advance_since(Antichain::from_elem(Timestamp::from(num_ts)));
    v2.consolidate_at_since();

    let upper = Antichain::from_elem(Timestamp::from(num_ts + 1));
    let out: Vec<_> = v2.updates_before(&upper).collect();
    assert_eq!(out.len(), usize::try_from(num_ts).unwrap());
    assert!(
        out.iter()
            .all(|(_, t, r)| *t == Timestamp::from(num_ts) && *r == Diff::ONE)
    );
}

/// The maintained record, size, and allocation totals must equal a walk over the chunks.
///
/// Guards the delta accounting in [`Chain::push_chunk`] and [`Stage::account_since`]: a
/// site that grows a chain or the stage without adjusting the totals shows up here.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri, and the bookkeeping it checks has no unsafe code
fn metrics_totals_match_walk() {
    let sink_metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    let num_ts = 200_u64;
    for t in 0..num_ts {
        v2.insert(&mut vec![
            (format!("a-{t}"), Timestamp::from(t), Diff::ONE),
            (format!("b-{t}"), Timestamp::from(t), Diff::ONE),
        ]);
    }

    // Drain part of the buffer, so chains sit in `emitted` and `pending_low` as well as in
    // the bucket chain.
    let upper = Antichain::from_elem(Timestamp::from(num_ts / 2));
    let drained = v2.updates_before(&upper).count();
    assert_eq!(drained, usize::try_from(num_ts).expect("fits"));

    // Advancing the since and consolidating there exercises the peel, split, and merge paths,
    // each of which retires and mints chains.
    v2.advance_since(Antichain::from_elem(Timestamp::from(num_ts / 2)));
    v2.consolidate_at_since();
    v2.insert(&mut vec![(
        "late".to_owned(),
        Timestamp::from(num_ts),
        Diff::ONE,
    )]);

    let mut chains: Vec<&Chain<String>> = vec![&v2.emitted];
    chains.extend(&v2.pending_low);
    for bucket in v2.chain.buckets() {
        chains.extend(&bucket.chains);
    }

    let mut size = v2.stage.bytes;
    let mut records = v2.stage.data.len();
    let mut allocations = usize::from(v2.stage.data.capacity() > 0);
    for chain in chains {
        size += chain.chunks.iter().map(Chunk::size).sum::<usize>();
        records += chain.chunks.iter().map(Chunk::len).sum::<usize>();
        allocations += chain.chunks.len();
    }

    assert!(size > 0, "workload must leave chunks behind");
    assert_eq!(v2.prev_size.size, size);
    assert_eq!(v2.prev_size.capacity, size);
    assert_eq!(v2.prev_size.allocations, allocations);
    assert_eq!(v2.prev_update_count, records);
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri
fn bucket_merges_deepen_and_reads_reset_depth() {
    let sink_metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    // Many stage flushes at one time land in one bucket, whose chain merges deepen.
    let mut i = 0;
    let deepest = loop {
        v2.insert(&mut vec![(
            format!("{i:08}"),
            Timestamp::from(0),
            Diff::ONE,
        )]);
        i += 1;
        let chains: Vec<&Chain<String>> = v2.chain.buckets().flat_map(|b| &b.chains).collect();
        for chain in &chains {
            let max = chain.chunks.iter().map(|c| c.depth).max().unwrap_or(0);
            assert_eq!(chain.depth, max, "a chain's depth is its deepest chunk's");
        }
        let deepest = chains.iter().map(|c| c.depth).max().unwrap_or(0);
        if deepest >= 2 && chains.len() >= 2 {
            break deepest;
        }
        assert!(i < 1_000_000, "bucket merges never deepened");
    };
    assert!(deepest >= 2);

    let upper = Antichain::from_elem(Timestamp::from(1));
    let read = v2.updates_before(&upper).count();
    assert_eq!(read, i);
    assert!(
        v2.emitted.chunks.iter().all(|c| c.depth == 0),
        "a read writes the youngest generation",
    );
}

/// Every chain announced to introspection is retired by the time the buffer drops, the
/// stage's included, whether or not the stage still holds updates.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // slow under Miri
fn logged_chains_balance_on_drop() {
    use crate::sink::correction::LoggingEvent;

    for staged in [false, true] {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sink_metrics = sink_metrics();
        let mut v2 = CorrectionV2::<String>::new(
            sink_metrics.clone(),
            sink_metrics.for_worker(0),
            Some(ChannelLogging::new(tx)),
            3.0,
            // One byte ships every insert, one mebibyte ships none of them.
            if staged { 1 << 20 } else { 1 },
        );
        for t in 0..100_u64 {
            v2.insert(&mut vec![(format!("{t}"), Timestamp::from(t), Diff::ONE)]);
        }
        assert_eq!(v2.stage.data.is_empty(), !staged);
        drop(v2);

        let (mut batches, mut records) = (0_isize, 0_isize);
        while let Ok(event) = rx.try_recv() {
            match event {
                LoggingEvent::ChainCreated(n) => {
                    batches += 1;
                    records += isize::try_from(n).expect("fits");
                }
                LoggingEvent::ChainDropped(n) => {
                    batches -= 1;
                    records -= isize::try_from(n).expect("fits");
                }
                _ => {}
            }
        }
        assert_eq!((batches, records), (0, 0), "staged={staged}");
    }
}

/// Reads must not observe updates at or beyond their `upper`, even when the `upper` is not
/// beyond the `since`.
#[mz_ore::test]
// Columnation regions are not Stacked Borrows compliant: later pushes invalidate the
// provenance of previously stored items under Miri.
#[cfg_attr(miri, ignore)]
fn upper_not_beyond_since() {
    let sink_metrics = sink_metrics();
    let mut v2 = CorrectionV2::<String>::new(
        sink_metrics.clone(),
        sink_metrics.for_worker(0),
        None,
        3.0,
        8 * 1024,
    );

    v2.insert(&mut vec![(
        "a".to_owned(),
        Timestamp::from(5_u64),
        Diff::ONE,
    )]);
    v2.advance_since(Antichain::from_elem(Timestamp::from(10_u64)));

    // The update logically lives at time 10 now, so a read before 7 must be empty.
    let upper = Antichain::from_elem(Timestamp::from(7_u64));
    assert_eq!(v2.updates_before(&upper).count(), 0);

    // A read before 11 must emit it, advanced to the since.
    let upper = Antichain::from_elem(Timestamp::from(11_u64));
    let out: Vec<_> = v2.updates_before(&upper).collect();
    assert_eq!(
        out,
        vec![("a".to_owned(), Timestamp::from(10_u64), Diff::ONE)]
    );
}

/// One pool shared by every test in the module. A pool reserves a large slab of address
/// space, so one per test exhausts the VM map under parallel test threads.
fn test_pool() -> mz_ore::pool::Pool {
    static POOL: std::sync::OnceLock<mz_ore::pool::Pool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| mz_ore::pool::Pool::new().expect("pool reservation"))
        .clone()
}

/// Route this thread's chunk spills through the test pool for the duration of `f`.
///
/// Without an override no pool is installed and every chunk stays resident, so the tests
/// would not exercise [`Chunk::body`]'s read-back at all. The override is thread-scoped,
/// so concurrently running tests do not race on it.
fn with_spill_pool<R>(f: impl FnOnce() -> R) -> R {
    chunk::set_spill_override(Some(test_pool()));
    let result = f();
    chunk::set_spill_override(None);
    result
}

/// Assert every chunk in the chain went to the pool, so a read must fetch it back.
fn assert_all_spilled<D: Data>(chain: &Chain<D>) {
    assert!(
        chain
            .chunks
            .iter()
            .all(|c| c.pooled.lock().expect("not poisoned").is_some()),
        "expected every chunk to spill, got {:?}",
        chain.chunks,
    );
}

/// Build a chain crossing the mint boundary while every chunk spills, then assert `iter()`
/// (the read path behind `updates_before`) reads each chunk back, roundtrips values, order,
/// and diffs, and leaves every body in the pool.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // the pool's mapped regions are unsupported under miri
fn iter_roundtrips_through_pool() {
    let count = 200_000_u64;
    with_spill_pool(|| {
        let mut builder = ChainBuilder::<i64>::default();
        for i in 0..count {
            let d = i64::try_from(i).expect("fits");
            builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
        }
        let chain = builder.finish();
        assert!(chain.chunks.len() > 1, "expected multiple minted chunks");
        assert_eq!(chain.update_count, usize::try_from(count).expect("fits"));
        assert_all_spilled(&chain);

        let mut expected = 0_u64;
        for (d, t, r) in chain.iter() {
            assert_eq!(d, i64::try_from(expected).expect("fits"));
            assert_eq!(t, Timestamp::new(expected));
            assert_eq!(r, Diff::ONE);
            expected += 1;
        }
        assert_eq!(expected, count);
        // The read is scoped: every body is still in the pool and none was materialized.
        assert_all_spilled(&chain);
        assert!(chain.chunks.iter().all(|c| c.resident.get().is_none()));
    });
}

/// Drive a [`Cursor`] over a spilled, multi-chunk chain to completion (the access pattern
/// merges use). Each step reads the front chunk back via [`Chunk::body`]; assert the
/// cursor yields every update in order.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // the pool's mapped regions are unsupported under miri
fn cursor_steps_through_pool() {
    let count = 200_000_u64;
    with_spill_pool(|| {
        let mut builder = ChainBuilder::<i64>::default();
        for i in 0..count {
            let d = i64::try_from(i).expect("fits");
            builder.push_owned(&(d, Timestamp::new(i), Diff::ONE));
        }
        let chain = builder.finish();
        assert!(chain.chunks.len() > 1, "expected multiple minted chunks");
        assert_all_spilled(&chain);

        let mut rest = chain.into_cursor();
        let mut expected = 0_u64;
        while let Some(cursor) = rest.take() {
            let (d, t, r) = cursor.get();
            assert_eq!(i64::into_owned(d), i64::try_from(expected).expect("fits"));
            assert_eq!(t, Timestamp::new(expected));
            assert_eq!(r, Diff::ONE);
            expected += 1;
            rest = cursor.step();
        }
        assert_eq!(expected, count);
    });
}
