// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{criterion_group, criterion_main, Criterion};
use mz_ore::id_gen::{IdAllocator, IdAllocatorInner, IdAllocatorInnerBitSet};

fn bench_id_gen(c: &mut Criterion) {
    bench_id_allocators(c, 0, 1_000);
    bench_id_allocators(c, 100_000, 1);
    bench_id_allocators(c, 1_000, 1_000);
    bench_id_allocators(c, 0, 100_000);
    bench_id_allocators(c, 50_000, 50_000);
}

// Creates an allocator with permanent initial_conns. Then makes bench_conns and drops those in a
// loop.
fn bench_id_allocators(c: &mut Criterion, initial_conns: usize, bench_conns: usize) {
    bench_id_allocator::<IdAllocatorInnerBitSet>(c, initial_conns, bench_conns);
}

fn bench_id_allocator<A: IdAllocatorInner>(
    c: &mut Criterion,
    initial_conns: usize,
    bench_conns: usize,
) {
    let name = format!("id_allocator_{}_{initial_conns}_{bench_conns}", A::NAME);
    c.bench_function(&name, |b| {
        let allocator = IdAllocator::<A>::new(1, 1 << 20, 0);
        let permanent = (0..initial_conns)
            .map(|_| allocator.alloc().unwrap())
            .collect::<Vec<_>>();
        let mut tmp = Vec::with_capacity(bench_conns);
        b.iter(|| {
            for _ in 0..bench_conns {
                tmp.push(allocator.alloc().unwrap());
            }
            tmp.clear();
        });
        drop(permanent);
    });
}

criterion_group!(benches, bench_id_gen);
criterion_main!(benches);
