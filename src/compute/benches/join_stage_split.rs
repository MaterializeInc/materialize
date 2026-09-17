// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Where a fallible join stage should split its `Result`s.
//!
//! A linear-join stage whose closure can error produces `Result<Row, _>`, because
//! `LinearJoinSpec::render` has one output. Two ways to get from there to a row column
//! plus an error `Vec`:
//!
//! * **demux**: build `Vec<(Result<Row, E>, T, Diff)>`, then walk it in a separate
//!   operator, matching each record and pushing the rows into a `ColumnBuilder`.
//! * **split**: route on the variant as each record is pushed, so the halves are
//!   already apart and the operator downstream moves containers.
//!
//! Both do the same per-record work once (one match, one row-byte copy). The question
//! is what the `Vec` hop costs: a move per record, plus a second pass over the batch.
//! This measures that difference and nothing else, so the numbers are an upper bound on
//! what the dataflow change can win, not a prediction for a query.
//!
//! Every builder in both arms releases after every push, the way `Session::give` does.
//! Any builder left to accumulate holds that much of the input live at once, which no
//! operator does, and whichever arm has the un-drained builder loses by a wide and
//! entirely fictional margin.
//!
//! Swept over the error rate, because that decides how much of the work each half does,
//! and a healthy dataflow sits at zero.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::containers::split::{Split, SplitBuilder};
use timely::Accountable;
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};

/// Stands in for `DataflowErrorSer`, which is private to the crate and is itself a
/// `Vec<u8>` newtype, so the container work is the same.
type ErrPayload = Vec<u8>;

type Item = (Result<Row, ErrPayload>, Timestamp, Diff);

/// Rows wide enough that a row-byte copy is not lost in the noise, which is the cost
/// both arms pay and neither avoids.
fn input(count: usize, err_in: usize) -> Vec<Item> {
    let err: ErrPayload = b"division by zero".to_vec();
    (0..count)
        .map(|i| {
            let data = if err_in != 0 && i % err_in == 0 {
                Err(err.clone())
            } else {
                let mut row = Row::default();
                row.packer().extend([
                    Datum::UInt64(u64::try_from(i).unwrap()),
                    Datum::String("a moderately sized string value"),
                    Datum::UInt64(u64::try_from(i * 7).unwrap()),
                ]);
                Ok(row)
            };
            (
                data,
                Timestamp::from(u64::try_from(i % 16).unwrap()),
                Diff::ONE,
            )
        })
        .collect()
}

/// Today: a `Vec` of `Result`s, then a second pass that matches and encodes.
fn demux(items: &[Item]) -> (i64, i64) {
    let mut staging = CapacityContainerBuilder::<Vec<Item>>::default();
    let mut oks = ColumnBuilder::<(Row, Timestamp, Diff)>::default();
    let mut errs = CapacityContainerBuilder::<Vec<(ErrPayload, Timestamp, Diff)>>::default();

    let (mut ok_count, mut err_count) = (0, 0);

    fn split_batch(
        batch: &mut Vec<Item>,
        oks: &mut ColumnBuilder<(Row, Timestamp, Diff)>,
        errs: &mut CapacityContainerBuilder<Vec<(ErrPayload, Timestamp, Diff)>>,
        ok_count: &mut i64,
        err_count: &mut i64,
    ) {
        for (data, time, diff) in batch.drain(..) {
            match data {
                Ok(row) => {
                    oks.push_into((&row, &time, &diff));
                    while let Some(batch) = oks.extract() {
                        *ok_count += batch.record_count();
                    }
                }
                Err(err) => {
                    errs.push_into((err, time, diff));
                    while let Some(batch) = errs.extract() {
                        *err_count += batch.record_count();
                    }
                }
            }
        }
    }

    for item in items {
        staging.push_into(item.clone());
        while let Some(batch) = staging.extract() {
            split_batch(batch, &mut oks, &mut errs, &mut ok_count, &mut err_count);
        }
    }
    while let Some(batch) = staging.finish() {
        split_batch(batch, &mut oks, &mut errs, &mut ok_count, &mut err_count);
    }
    let (ok_rest, err_rest) = drain(&mut oks, &mut errs);
    (ok_count + ok_rest, err_count + err_rest)
}

/// Proposed: route on push, so the halves never share a container.
fn split(items: &[Item]) -> (i64, i64) {
    type Builder = SplitBuilder<
        ColumnBuilder<(Row, Timestamp, Diff)>,
        CapacityContainerBuilder<Vec<(ErrPayload, Timestamp, Diff)>>,
    >;
    let mut builder = Builder::default();
    let (mut ok_count, mut err_count) = (0, 0);
    for item in items {
        builder.push_into(item.clone());
        while let Some(Split { ok, err }) = builder.extract() {
            ok_count += ok.record_count();
            err_count += err.record_count();
        }
    }
    while let Some(Split { ok, err }) = builder.finish() {
        ok_count += ok.record_count();
        err_count += err.record_count();
    }
    (ok_count, err_count)
}

/// Releases whatever the two halves still hold once the input is exhausted.
fn drain(
    oks: &mut ColumnBuilder<(Row, Timestamp, Diff)>,
    errs: &mut CapacityContainerBuilder<Vec<(ErrPayload, Timestamp, Diff)>>,
) -> (i64, i64) {
    let (mut ok_count, mut err_count) = (0, 0);
    while let Some(batch) = oks.extract() {
        ok_count += batch.record_count();
    }
    while let Some(batch) = oks.finish() {
        ok_count += batch.record_count();
    }
    while let Some(batch) = errs.extract() {
        err_count += batch.record_count();
    }
    while let Some(batch) = errs.finish() {
        err_count += batch.record_count();
    }
    (ok_count, err_count)
}

fn bench(c: &mut Criterion) {
    let count = 256 << 10;
    let mut group = c.benchmark_group("join_stage_split");
    group.throughput(Throughput::Elements(u64::try_from(count).unwrap()));

    // `0` is no errors at all, the healthy case; the rest are one error every N records.
    for err_in in [0, 1024, 16] {
        let items = input(count, err_in);
        let label = if err_in == 0 {
            "no_errors".to_string()
        } else {
            format!("one_error_in_{err_in}")
        };
        group.bench_with_input(BenchmarkId::new("demux", &label), &items, |b, items| {
            b.iter(|| std::hint::black_box(demux(items)))
        });
        group.bench_with_input(BenchmarkId::new("split", &label), &items, |b, items| {
            b.iter(|| std::hint::black_box(split(items)))
        });
    }
    group.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
