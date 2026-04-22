// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks for arranging Row×Row data via the factorized `RowRowSpine`.
//!
//! Each iteration runs a timely worker with a single dataflow:
//!   input → arrange_core → drop arrangement.
//! Measures wall time to push `n` updates and let the worker settle.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use differential_dataflow::input::{Input, InputSession};
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::{Batch, Batcher, Builder, Trace, TraceReader};
use mz_compute::typedefs::{RowRowBatcher, RowRowBuilder, RowRowSpine};
use mz_repr::{Datum, Diff, Row, Timestamp};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Probe;

/// Generate `n` `(Row, Row, Timestamp, Diff)` updates with `n_keys` distinct keys
/// and `n_vals` distinct values.
fn generate_row_data(n: usize, n_keys: usize, n_vals: usize) -> Vec<((Row, Row), Timestamp, Diff)> {
    let mut data = Vec::with_capacity(n);
    let mut key_buf = Row::default();
    let mut val_buf = Row::default();
    for i in 0..n {
        let k = (i % n_keys) as i64;
        let v = (i % n_vals) as i64;
        key_buf.packer().push(Datum::Int64(k));
        val_buf.packer().push(Datum::Int64(v));
        data.push((
            (key_buf.clone(), val_buf.clone()),
            Timestamp::from((i % 10) as u64),
            Diff::ONE,
        ));
    }
    data
}

/// Run a dataflow: input → arrange → drop. Waits for worker to settle.
fn run_arrange<Ba, Bu, Tr>(data: &[((Row, Row), Timestamp, Diff)], name: &str)
where
    Ba: Batcher<Input = Vec<((Row, Row), Timestamp, Diff)>, Time = Timestamp> + 'static,
    Bu: Builder<Time = Timestamp, Input = Ba::Output, Output = Tr::Batch>,
    Tr: Trace<Time = Timestamp> + TraceReader + 'static,
    Tr::Batch: Batch,
{
    let data_owned = data.to_vec();
    let name_owned = name.to_string();
    timely::execute_directly(move |worker| {
        let (mut input, probe) = worker.dataflow::<Timestamp, _, _>(|scope| {
            let (input, collection): (InputSession<Timestamp, (Row, Row), Diff>, _) =
                scope.new_collection();
            let arranged = arrange_core::<_, Ba, Bu, Tr>(
                collection.inner,
                Exchange::new(|_: &((Row, Row), Timestamp, Diff)| 0u64),
                &name_owned,
            );
            let (probe, _stream) = arranged.stream.probe();
            (input, probe)
        });
        // Push all data at time 0, then advance to close the frontier.
        for ((k, v), _t, d) in data_owned {
            input.update((k, v), d);
        }
        let closed = Timestamp::from(100u64);
        input.advance_to(closed);
        input.flush();
        while probe.less_than(&closed) {
            worker.step();
        }
    });
}

fn bench_rowrow_arrange(c: &mut Criterion) {
    let mut group = c.benchmark_group("compute/arrange_row");
    // Third config: high dedup — few distinct keys, many updates per key.
    // Exposes the structural win of the trie-aware batcher over the flat baseline.
    for (n, nk, nv) in [
        (10_000, 100, 1_000),
        (100_000, 1_000, 10_000),
        (100_000, 10, 100),
    ] {
        let data = generate_row_data(n, nk, nv);
        let label = format!("n={n}/k={nk}/v={nv}");

        group.throughput(Throughput::Elements(n as u64));
        group.bench_function(BenchmarkId::new("row_row_spine", &label), |b| {
            b.iter(|| {
                run_arrange::<
                    RowRowBatcher<Timestamp, Diff>,
                    RowRowBuilder<Timestamp, Diff>,
                    RowRowSpine<Timestamp, Diff>,
                >(&data, "BenchRowRow");
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_rowrow_arrange);
criterion_main!(benches);
