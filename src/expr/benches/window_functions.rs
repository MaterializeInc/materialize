// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hint::black_box;

use chrono::DateTime;
use criterion::{criterion_group, criterion_main, Criterion};
use mz_expr::ColumnOrder;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena};
use rand::distributions::{Distribution, Uniform};

/// Microbenchmark to test an important part of window function evaluation.
///
/// Run with
/// ```
/// cargo bench window_function_benches
/// ```
/// (It's good to go into this directory, to avoid building also all other things in release mode.)
///
/// A similar end-to-end test:
/// ```
/// CREATE SOURCE counter FROM LOAD GENERATOR COUNTER;
///
/// create table t3(t timestamptz, d1 int, d2 text, d3 int, d4 int, d5 int, d6 text);
///
/// insert into t3
/// select generate_series(1,300000)::mz_timestamp::timestamptz, 5, 'aaa', 7, 8, 9, 'bbb';
///
/// create view v1 as
/// select counter::mz_timestamp::timestamptz as t, 5 as d1, 'aaa' as d2, 7 as d3, 8 as d4, 9 as d5, 'bbb' as d6 from counter
/// union all
/// select t, d1, d2, EXTRACT(MILLISECOND FROM t) * 91 % 1223 as d3, d4, d5, d6 from t3;
///
/// create index v1_ind on v1(t);
///
/// create view v2 as
/// select t, d1, d2, lag(t) over (order by t) as r
/// from v1;
///
/// create index v2_ind on v2(r);
/// ```
/// and then vary the size of the `generate_series`, and watch whether `v2_ind` is able to keep up.
fn order_aggregate_datums_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_function_benches");

    let scale = 1000000;

    group.bench_function("order_aggregate_datums", |b| {
        let mut rng = rand::thread_rng();
        let temp_storage = RowArena::new();

        let order_by = vec![ColumnOrder {
            column: 0,
            desc: false,
            nulls_last: false,
        }];

        // Simulate a lag/lead, e.g.
        // row(
        //   row(
        //     row(#0, #1, #2, #3), // original row
        //     row(#1, 1, null) // args to lag/lead
        //   ),
        //   <order_by_col_1>,
        // )
        let distr = Uniform::new(0, 1000000000);
        let mut datums = Vec::with_capacity(scale);
        for _i in 0..scale {
            datums.push(temp_storage.make_datum(|packer| {
                let orig_row_and_args = temp_storage.make_datum(|packer| {
                    packer.push(Datum::Int32(3));
                    packer.push(Datum::Int32(545577777));
                    packer.push(Datum::Int32(123456789));
                    packer.push(Datum::String("aaaaaaaaaa"));
                });

                // An early version had non-random stuff here, but surprisingly this is much faster
                // to sort, so it's not representative.
                //let order_by_col_1 = Datum::Int32((scale - i) as i32);
                // This is faster, probably because Int32 is easier to decode than a Timestamp.
                //let order_by_col_1 = Datum::Int32(distr.sample(&mut rng));
                // So, we generate a random timestamp. Timestamps are a common thing to order by in
                // window functions.
                let order_by_col_1 = Datum::TimestampTz(
                    CheckedTimestamp::from_timestamplike(
                        DateTime::from_timestamp_millis(distr.sample(&mut rng)).unwrap(),
                    )
                    .unwrap(),
                );

                packer.push_list_with(|packer| {
                    packer.push(orig_row_and_args);
                    packer.push(order_by_col_1);
                });
            }));
        }

        b.iter(|| {
            black_box(mz_expr::order_aggregate_datums_exported_for_benchmarking(
                black_box(datums.clone()),
                black_box(&order_by),
            ))
            // Do something with the result to force the iterator.
            .for_each(|d| assert!(!d.is_null()));
        })
    });
}

criterion_group!(window_function_benches, order_aggregate_datums_benchmark);
criterion_main!(window_function_benches);
