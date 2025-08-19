// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::mpsc::Receiver;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use differential_dataflow::{
    ExchangeData,
    input::{Input, InputSession},
};
use mz_ore::Overflowing;
use mz_timely_util::{capture::PusherCapture, order::Partitioned, reclock::reclock};
use timely::{
    communication::allocator::Thread,
    dataflow::{
        Scope,
        operators::{
            ActivateCapability, Capture, UnorderedInput, capture::Event,
            unordered_input::UnorderedHandle,
        },
    },
    progress::{Antichain, Timestamp, timestamp::Refines},
    worker::Worker,
};

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
            &mut Worker<Thread>,
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
                    let (data_pusher, reclocked_collection) = reclock(&binding_collection, as_of);
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

fn step(worker: &mut Worker<Thread>) {
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
                                    data.session(data_cap.clone()).give((
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

criterion_group!(benches, bench_reclock_simple);
criterion_main!(benches);
