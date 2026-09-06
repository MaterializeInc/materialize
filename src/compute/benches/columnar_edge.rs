// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Micro-benchmarks for the columnar dataflow edge operators.
//!
//! Covers the render-layer operators that carry `Column<(Row, T, Diff)>` between
//! Plan nodes, and the row-based operators they replace. Companion to
//! [`columnar_merge_batcher_row`](`columnar_merge_batcher_row.rs`), which
//! measures the batcher substrate underneath arrange sites rather than the edge
//! itself.
//!
//! Every case builds a single-worker dataflow, feeds one batch of `n` updates,
//! and runs the worker to quiescence. Dataflow construction and input feeding
//! are inside the timed region, so `edge/baseline` measures that floor with no
//! operator under test and every other case should be read against it.
//! Throughput is reported per input record.
//!
//! The pairs to compare:
//! - `negate/columnar` against `negate/row`
//! - `consolidate/columnar` against `consolidate/row`
//!
//! and the conversion leaves, which the row-based rendering did not pay at all:
//! - `edge/encode` is one `VecToColumnar`, what a persist import or a
//!   row-serializing node's output leaf costs.
//! - `edge/encode_decode` adds one `ColumnarToVec`; subtract `edge/encode` for
//!   the decode alone, which is what a sink pays.
//! - `edge/letrec_iteration` is encode, decode, encode, the conversions a
//!   `LetRec` binding crosses per iteration.

use std::collections::BTreeMap;
use std::hint::black_box;

use criterion::measurement::WallTime;
use criterion::{
    BenchmarkGroup, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use differential_dataflow::AsCollection;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use itertools::Itertools;
use mz_compute::bench::{
    CollectionEdge, columnar_consolidate, columnar_negate, columnar_to_vec, concat_many,
    vec_to_columnar,
};
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_timely_util::columnation::ColInternalMerger;
use mz_timely_util::operator::CollectionExt;
use timely::Container;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Input;
use timely::dataflow::operators::generic::Operator;

/// The batcher `consolidate_named` takes, spelled without reaching into
/// `mz_compute`'s private `typedefs`.
type RowKeyBatcher = MergeBatcher<ColInternalMerger<(Row, ()), Timestamp, Diff>>;

type Update = (Row, Timestamp, Diff);

/// Row shape of the benchmark input.
///
/// Row width drives what the edge actually costs: encoding copies row bytes, so
/// it scales with width, while decoding allocates one `Row` per record and is
/// closer to width-independent. Measuring both ends keeps a change that trades
/// one against the other visible.
#[derive(Clone, Copy)]
enum Shape {
    /// Two integer columns, near the per-record floor.
    Narrow,
    /// Four integers and four short strings, where byte copying dominates.
    Wide,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Narrow => "narrow",
            Shape::Wide => "wide",
        }
    }

    fn row(self, i: u64) -> Row {
        let i = i64::try_from(i % (1 << 40)).expect("bounded above");
        match self {
            Shape::Narrow => Row::pack_slice(&[Datum::Int64(i), Datum::Int64(i * 7)]),
            Shape::Wide => {
                let s0 = format!("value-{i}");
                let s1 = format!("{:016x}", i);
                Row::pack_slice(&[
                    Datum::Int64(i),
                    Datum::Int64(i * 7),
                    Datum::Int64(i * 13),
                    Datum::Int64(i * 31),
                    Datum::String(&s0),
                    Datum::String(&s1),
                    Datum::String("constant"),
                    Datum::Null,
                ])
            }
        }
    }
}

/// How much of the input cancels or accumulates when consolidated.
#[derive(Clone, Copy)]
enum Density {
    /// Every record distinct, so consolidation only sorts.
    Distinct,
    /// Each row repeated four times at one time, half of them retractions, so
    /// consolidation cancels three quarters of the input.
    Duplicated,
}

impl Density {
    fn name(self) -> &'static str {
        match self {
            Density::Distinct => "distinct",
            Density::Duplicated => "duplicated",
        }
    }
}

/// Builds `n` updates spread over four timestamps.
///
/// Times are assigned round-robin rather than in runs, so a consolidating
/// operator sees interleaved times and cannot shortcut on a sorted input.
fn updates(shape: Shape, density: Density, n: u64) -> Vec<Update> {
    let distinct = match density {
        Density::Distinct => n,
        Density::Duplicated => n / 4,
    };
    (0..n)
        .map(|i| {
            let row = shape.row(i % distinct.max(1));
            let time = Timestamp::from(i % 4);
            let diff = match density {
                Density::Distinct => Diff::ONE,
                // Alternate sign so the repeats cancel rather than accumulate.
                Density::Duplicated if i % 2 == 0 => Diff::ONE,
                Density::Duplicated => -Diff::ONE,
            };
            (row, time, diff)
        })
        .collect()
}

/// Checks that `updates` built the input the case names promise.
///
/// A silent drift here (a density that stops cancelling, a shape that stops
/// being distinct) would not fail anything, it would just change what the
/// numbers mean, so the benchmark checks its own input once per run.
fn check_input(density: Density, input: &[Update]) {
    let mut consolidated = BTreeMap::new();
    for (row, time, diff) in input {
        *consolidated.entry((row, time)).or_insert(Diff::ZERO) += *diff;
    }
    consolidated.retain(|_, diff| *diff != Diff::ZERO);
    let survivors = consolidated.len();
    match density {
        Density::Distinct => assert_eq!(
            survivors,
            input.len(),
            "distinct input should not consolidate away"
        ),
        Density::Duplicated => assert!(
            survivors * 2 < input.len(),
            "duplicated input should mostly cancel, {survivors} of {} survived",
            input.len()
        ),
    }
}

/// Consumes a stream so the operators upstream are not measuring into a void,
/// without decoding or otherwise touching the records.
fn drain<C: Container>(stream: Stream<'_, Timestamp, C>) {
    stream.sink(Pipeline, "BenchDrain", |(input, _frontier)| {
        input.for_each(|_cap, container| {
            black_box(container);
        });
    });
}

/// The operator arrangement under test.
#[derive(Clone, Copy)]
enum Case {
    /// Input straight to the drain. The floor every other case sits on.
    Baseline,
    /// One `VecToColumnar`.
    Encode,
    /// `VecToColumnar` then `ColumnarToVec`.
    EncodeDecode,
    /// The conversions a `LetRec` binding crosses per iteration.
    LetRecIteration,
    NegateColumnar,
    NegateRow,
    ConsolidateColumnar,
    ConsolidateRow,
    /// Four columnar edges concatenated, as a `Union` with four inputs.
    ConcatMany,
}

impl Case {
    /// How many dataflow inputs this case feeds.
    fn inputs(self) -> usize {
        match self {
            Case::ConcatMany => 4,
            _ => 1,
        }
    }

    fn id(self) -> &'static str {
        match self {
            Case::Baseline => "edge/baseline",
            Case::Encode => "edge/encode",
            Case::EncodeDecode => "edge/encode_decode",
            Case::LetRecIteration => "edge/letrec_iteration",
            Case::NegateColumnar => "negate/columnar",
            Case::NegateRow => "negate/row",
            Case::ConsolidateColumnar => "consolidate/columnar",
            Case::ConsolidateRow => "consolidate/row",
            Case::ConcatMany => "concat_many/columnar",
        }
    }
}

/// Builds and runs one dataflow over `input`, to quiescence.
///
/// `Union` is the one case with more than one input, so the input is split
/// across `case.inputs()` handles rather than teeing one. A tee would clone the
/// records and put that cost, not concatenation's, into the measurement.
fn run(case: Case, input: Vec<Update>) {
    timely::execute_directly(move |worker| {
        let mut handles = worker.dataflow::<Timestamp, _, _>(|scope| {
            let mut handles = Vec::with_capacity(case.inputs());
            let mut collections = Vec::with_capacity(case.inputs());
            for _ in 0..case.inputs() {
                let (handle, stream) = scope.new_input::<Vec<Update>>();
                handles.push(handle);
                collections.push(stream.as_collection());
            }
            let collection = collections.remove(0);

            match case {
                Case::Baseline => drain(collection.inner),
                Case::Encode => drain(vec_to_columnar(collection).inner),
                Case::EncodeDecode => drain(columnar_to_vec(vec_to_columnar(collection)).inner),
                Case::LetRecIteration => {
                    // The read edge a `Get` sees, decoded for the `Vec`-internal
                    // consolidate that feeds the variable, then re-encoded for
                    // the next iteration's readers.
                    let edge = vec_to_columnar(collection);
                    let value = columnar_to_vec(edge);
                    drain(vec_to_columnar(value).inner)
                }
                Case::NegateColumnar => drain(columnar_negate(vec_to_columnar(collection)).inner),
                Case::NegateRow => drain(collection.negate().inner),
                Case::ConsolidateColumnar => drain(
                    columnar_consolidate(vec_to_columnar(collection), "BenchConsolidation").inner,
                ),
                Case::ConsolidateRow => drain(
                    CollectionExt::consolidate_named::<RowKeyBatcher>(
                        collection,
                        "BenchConsolidation",
                    )
                    .inner,
                ),
                Case::ConcatMany => {
                    let edges: Vec<CollectionEdge<'_, Timestamp>> = std::iter::once(collection)
                        .chain(collections)
                        .map(vec_to_columnar)
                        .collect();
                    drain(concat_many(scope.clone(), edges).inner)
                }
            }

            handles
        });

        // Split the input evenly across the handles, so total record count is
        // the same whatever the input count and throughput stays comparable.
        let per_handle = input.len().div_ceil(handles.len());
        for (handle, chunk) in handles.iter_mut().zip_eq(input.chunks(per_handle)) {
            handle.send_batch(&mut chunk.to_vec());
        }
        for handle in &mut handles {
            handle.advance_to(Timestamp::from(4_u64));
        }
        // The handles hold capabilities, so the dataflow only drains and retires
        // once they are gone. Without this the step loop below never ends.
        drop(handles);
        while worker.step() {}
    });
}

/// Short wall-clock configuration.
///
/// The signal here is the ratio between arrangements of the same input, not a
/// tight confidence interval on any one of them, so this trades statistical
/// rigor for turnaround.
fn configure(group: &mut BenchmarkGroup<WallTime>) {
    group
        .sample_size(10)
        .warm_up_time(std::time::Duration::from_millis(500))
        .measurement_time(std::time::Duration::from_secs(3));
}

fn bench_edge(c: &mut Criterion) {
    let n: u64 = 200_000;

    // Conversion and negate cases: density does not change their work, so run
    // them on distinct input only.
    for shape in [Shape::Narrow, Shape::Wide] {
        let input = updates(shape, Density::Distinct, n);
        check_input(Density::Distinct, &input);
        let mut group = c.benchmark_group(format!("columnar_edge/{}", shape.name()));
        configure(&mut group);
        group.throughput(Throughput::Elements(n));
        for case in [
            Case::Baseline,
            Case::Encode,
            Case::EncodeDecode,
            Case::LetRecIteration,
            Case::NegateColumnar,
            Case::NegateRow,
            Case::ConcatMany,
        ] {
            group.bench_with_input(BenchmarkId::new(case.id(), n), &input, |b, input| {
                b.iter(|| run(case, input.clone()))
            });
        }
        group.finish();
    }

    // Consolidation is the one place the cancellation rate changes the work, and
    // the one place worker routing matters, so it gets both densities.
    for shape in [Shape::Narrow, Shape::Wide] {
        for density in [Density::Distinct, Density::Duplicated] {
            let input = updates(shape, density, n);
            check_input(density, &input);
            let mut group =
                c.benchmark_group(format!("columnar_edge/{}/{}", shape.name(), density.name()));
            configure(&mut group);
            group.throughput(Throughput::Elements(n));
            for case in [Case::ConsolidateColumnar, Case::ConsolidateRow] {
                group.bench_with_input(BenchmarkId::new(case.id(), n), &input, |b, input| {
                    b.iter(|| run(case, input.clone()))
                });
            }
            group.finish();
        }
    }
}

criterion_group!(benches, bench_edge);
criterion_main!(benches);
