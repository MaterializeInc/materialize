// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_alloc_default as _;
use std::sync::mpsc::Receiver;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use differential_dataflow::{
    ExchangeData,
    input::{Input, InputSession},
};

use mz_ore::Overflowing;
use mz_ore::cast::CastFrom;
use mz_timely_util::capture::PusherCapture;
use mz_timely_util::order::Partitioned;
use mz_timely_util::reclock::reclock;
use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::vec::UnorderedInput;
use timely::dataflow::operators::vec::unordered_input::UnorderedHandle;
use timely::dataflow::operators::{ActivateCapability, Capture};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};
use timely::worker::Worker;

type Diff = Overflowing<i64>;
type FromTime = Partitioned<u64, u64>;
type IntoTime = u64;
type BindingHandle<FromTime> = InputSession<IntoTime, FromTime, Diff>;
type DataHandle<D, FromTime> = (
    UnorderedHandle<FromTime, (D, FromTime, Diff)>,
    ActivateCapability<FromTime>,
);
type ReclockedStream<D> = Receiver<Event<IntoTime, Vec<(D, IntoTime, Diff)>>>;

fn harness<FromTime, D, F, R>(as_of: Antichain<IntoTime>, test_logic: F) -> R
where
    FromTime: Timestamp + Refines<()>,
    D: ExchangeData,
    F: FnOnce(
            &mut Worker,
            BindingHandle<FromTime>,
            DataHandle<D, FromTime>,
            ReclockedStream<D>,
        ) -> R
        + Send
        + Sync
        + 'static,
    R: Send + 'static,
{
    timely::execute_directly(move |worker| {
        let (bindings, data, data_cap, reclocked) = worker.dataflow::<(), _, _>(|scope| {
            let (bindings, data_pusher, reclocked) =
                scope.scoped::<IntoTime, _, _>("IntoScope", move |scope| {
                    let (binding_handle, binding_collection) = scope.new_collection();
                    let (data_pusher, reclocked_collection) = reclock(binding_collection, as_of);
                    let reclocked_capture = reclocked_collection.inner.capture();
                    (binding_handle, data_pusher, reclocked_capture)
                });

            let (data, data_cap) = scope.scoped::<FromTime, _, _>("FromScope", move |scope| {
                let ((handle, cap), data) = scope.new_unordered_input();
                data.capture_into(PusherCapture(data_pusher));
                (handle, cap)
            });

            (bindings, data, data_cap, reclocked)
        });

        test_logic(worker, bindings, (data, data_cap), reclocked)
    })
}

fn step(worker: &mut Worker) {
    for _ in 0..4 {
        worker.step();
    }
}

fn bench_reclock_simple(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_reclock_simple_group");

    for n in [100_u64, 1_000_u64, 10_000_u64] {
        group.bench_with_input(BenchmarkId::new("bench_reclock_simple", n), &n, |b, d| {
            b.iter(|| {
                let upto = *d;
                let downgrade_interval = upto / 3;
                let data_interval = upto / 10;
                harness::<FromTime, u64, _, _>(
                    Antichain::from_elem(0),
                    move |worker, mut bindings, (mut data, mut data_cap), _| {
                        for ts in 0..upto {
                            if ts > 0 {
                                bindings.update_at(
                                    Partitioned::new_singleton(0, ts - 1),
                                    ts,
                                    Diff::MINUS_ONE,
                                );
                                if ts.is_multiple_of(downgrade_interval) {
                                    data_cap.downgrade(&Partitioned::new_singleton(0, ts - 1));
                                }
                                if ts.is_multiple_of(data_interval) {
                                    data.activate().session(&data_cap).give((
                                        ts,
                                        Partitioned::new_singleton(0, ts),
                                        Diff::ONE,
                                    ));
                                }
                            }
                            bindings.update_at(Partitioned::new_singleton(0, ts), ts, Diff::ONE);
                            bindings.advance_to(ts + 1);
                            bindings.flush();
                            step(worker);
                        }
                    },
                )
            })
        });
    }
}

/// The partition that the bindings `mint` creates advance, and that reclocked updates belong to.
const PID: u64 = 1;

/// The shape of the bindings [`mint`] creates.
#[derive(Clone, Copy)]
enum Shape {
    /// Partition `PID` alone.
    Partition,
    /// A GTID set with `uuids` server UUIDs and ranges filling the gaps around them. Only the
    /// UUID at `PID` advances, the way a source with idle UUIDs from passive masters or past
    /// failovers does.
    Gtid { uuids: u64 },
}

/// Inserts the elements of `shape` that never advance, at time 0.
fn mint_idle(bindings: &mut BindingHandle<FromTime>, shape: Shape) {
    let Shape::Gtid { uuids } = shape else {
        return;
    };
    // The UUIDs sit at the odd partitions from `PID` up, and the ranges at the partitions
    // between them.
    for pid in (PID..).step_by(2).take(usize::cast_from(uuids)) {
        bindings.update_at(Partitioned::new_range(pid - 1, pid - 1, 0), 0, Diff::ONE);
        if pid != PID {
            bindings.update_at(Partitioned::new_singleton(pid, 1), 0, Diff::ONE);
        }
    }
    let last = PID + 2 * uuids - 1;
    bindings.update_at(Partitioned::new_range(last, u64::MAX, 0), 0, Diff::ONE);
}

/// Mints the binding at time `t`, which covers the offsets of partition `PID` below `t`.
fn mint_binding(bindings: &mut BindingHandle<FromTime>, t: u64) {
    if t > 0 {
        bindings.update_at(Partitioned::new_singleton(PID, t - 1), t, Diff::MINUS_ONE);
    }
    bindings.update_at(Partitioned::new_singleton(PID, t), t, Diff::ONE);
}

/// Mints bindings of `shape` at times `0..=count`. Offset `o` of `PID` reclocks to `o + 1`.
fn mint(bindings: &mut BindingHandle<FromTime>, count: u64, shape: Shape) {
    mint_idle(bindings, shape);
    for t in 0..=count {
        mint_binding(bindings, t);
    }
    bindings.advance_to(count + 1);
    bindings.flush();
}

/// The binding counts most benchmarks of a pinned since run at.
const COUNTS: &[u64] = &[1, 10, 100, 1_000, 10_000, 100_000, 1_000_000];

/// Times one activation of the reclock operator over each of `counts` bindings of `shape`, while
/// `data_cap` holds the since at the as_of the way a snapshotting export does. Each activation
/// reclocks one update at each offset `batch` returns. Setup runs outside the timed loop.
fn bench_pinned_since(
    c: &mut Criterion,
    group: &str,
    counts: &[u64],
    shape: Shape,
    batch: fn(u64) -> Vec<u64>,
) {
    let mut group = c.benchmark_group(group);
    group.sample_size(10);
    for &count in counts {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_custom(|iters| {
                harness::<FromTime, u64, _, _>(
                    Antichain::from_elem(0),
                    move |worker, mut bindings, (mut data, data_cap), reclocked| {
                        mint(&mut bindings, count, shape);
                        step(worker);
                        let offsets = batch(count);
                        let mut activate = || {
                            data.activate().session(&data_cap).give_iterator(
                                offsets.iter().enumerate().map(|(d, offset)| {
                                    (
                                        u64::cast_from(d),
                                        Partitioned::new_singleton(PID, *offset),
                                        Diff::ONE,
                                    )
                                }),
                            );
                            step(worker);
                            reclocked
                                .try_iter()
                                .map(|event| match event {
                                    Event::Messages(_, updates) => updates.len(),
                                    Event::Progress(_) => 0,
                                })
                                .sum::<usize>()
                        };
                        // Every update must reclock to a binding, or the benchmark measures
                        // deferred updates instead.
                        assert_eq!(activate(), offsets.len());

                        let start = Instant::now();
                        for _ in 0..iters {
                            activate();
                        }
                        start.elapsed()
                    },
                )
            })
        });
    }
}

/// Replicated updates reclock to the newest binding, which is where a snapshotting export's
/// updates land.
fn bench_reclock_pinned_since(c: &mut Criterion) {
    bench_pinned_since(
        c,
        "reclock_pinned_since",
        COUNTS,
        Shape::Partition,
        |count| vec![count - 1; 1_000],
    );
}

/// One update per binding, the way a backlog reclocks while replication catches up. No binding
/// can be skipped, so this checks that the index doesn't slow down the walk.
fn bench_reclock_dense(c: &mut Criterion) {
    bench_pinned_since(c, "reclock_dense", COUNTS, Shape::Partition, |count| {
        (0..count).collect()
    });
}

/// Returns about 1,000 offsets spread evenly over `count` bindings.
fn sparse_offsets(count: u64) -> Vec<u64> {
    (0..count)
        .step_by(usize::cast_from(std::cmp::max(count / 1_000, 1)))
        .collect()
}

/// Updates spread over the bindings, the way updates that map into the middle of the trace do.
/// Each lookup lands past the next binding, so it searches the trace.
fn bench_reclock_sparse(c: &mut Criterion) {
    bench_pinned_since(
        c,
        "reclock_sparse",
        COUNTS,
        Shape::Partition,
        sparse_offsets,
    );
}

/// [`bench_reclock_sparse`] over bindings shaped like a GTID set with one server UUID.
fn bench_reclock_sparse_gtid(c: &mut Criterion) {
    bench_pinned_since(
        c,
        "reclock_sparse_gtid",
        COUNTS,
        Shape::Gtid { uuids: 1 },
        sparse_offsets,
    );
}

/// [`bench_reclock_sparse_gtid`] with idle UUIDs, which widen every binding without changing.
/// The counts stop at 100,000 because each binding holds every UUID.
fn bench_reclock_sparse_gtid_idle(c: &mut Criterion) {
    for uuids in [3, 10, 20] {
        bench_pinned_since(
            c,
            &format!("reclock_sparse_gtid_{uuids}_uuids"),
            &[1_000, 10_000, 100_000],
            Shape::Gtid { uuids },
            sparse_offsets,
        );
    }
}

/// Times minting one binding per iteration over `parts` partitions that all advance at every
/// binding, the way a busy multi-partition source's bindings do. Each binding retracts and
/// inserts an element per partition, which stresses maintaining the trace's accumulated binding.
fn bench_reclock_wide(c: &mut Criterion) {
    let mut group = c.benchmark_group("reclock_wide");
    group.sample_size(10);
    for parts in [16_u64, 64, 256] {
        group.bench_with_input(BenchmarkId::from_parameter(parts), &parts, |b, &parts| {
            b.iter_custom(|iters| {
                harness::<FromTime, u64, _, _>(
                    Antichain::from_elem(0),
                    // Holding `_data` keeps the source frontier, and so the since, at the minimum.
                    move |worker, mut bindings, _data, _| {
                        let start = Instant::now();
                        for t in 0..iters {
                            for pid in 0..parts {
                                if t > 0 {
                                    bindings.update_at(
                                        Partitioned::new_singleton(pid, t - 1),
                                        t,
                                        Diff::MINUS_ONE,
                                    );
                                }
                                bindings.update_at(
                                    Partitioned::new_singleton(pid, t),
                                    t,
                                    Diff::ONE,
                                );
                            }
                            bindings.advance_to(t + 1);
                            bindings.flush();
                            step(worker);
                        }
                        start.elapsed()
                    },
                )
            })
        });
    }
}

/// Times minting one binding per iteration over a GTID set whose idle UUIDs widen every binding
/// while one UUID advances. The since stays at the minimum, so every binding stays in the trace.
fn bench_reclock_wide_idle(c: &mut Criterion) {
    let mut group = c.benchmark_group("reclock_wide_idle");
    group.sample_size(10);
    for uuids in [3_u64, 10, 20] {
        group.bench_with_input(BenchmarkId::from_parameter(uuids), &uuids, |b, &uuids| {
            b.iter_custom(|iters| {
                harness::<FromTime, u64, _, _>(
                    Antichain::from_elem(0),
                    // Holding `_data` keeps the source frontier, and so the since, at the minimum.
                    move |worker, mut bindings, _data, _| {
                        mint_idle(&mut bindings, Shape::Gtid { uuids });
                        mint_binding(&mut bindings, 0);
                        bindings.advance_to(1);
                        bindings.flush();
                        step(worker);
                        let start = Instant::now();
                        for t in 1..=iters {
                            mint_binding(&mut bindings, t);
                            bindings.advance_to(t + 1);
                            bindings.flush();
                            step(worker);
                        }
                        start.elapsed()
                    },
                )
            })
        });
    }
}

criterion_group!(
    benches,
    bench_reclock_simple,
    bench_reclock_pinned_since,
    bench_reclock_dense,
    bench_reclock_sparse,
    bench_reclock_sparse_gtid,
    bench_reclock_sparse_gtid_idle,
    bench_reclock_wide,
    bench_reclock_wide_idle
);
criterion_main!(benches);
