// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Codec cost on paged dataflow-edge bodies.
//!
//! A paged edge body pays its codec when the pool evicts it (encode) and when
//! a consumer reads it back evicted (decode). This measures both directions
//! for lz4 and identity on bodies a `ColumnBuilder` ships, and prints the lz4
//! compression ratio of each shape, so the CPU can be weighed against the
//! bytes it saves. Swap I/O and pool bookkeeping are not measured here.
//!
//! Lives in `mz-compute` because it needs `mz_repr::Row`'s columnar container.
//!
//! Shapes:
//!
//! * `lineitem`: a TPC-H lineitem-like row of integer keys, a float price, low
//!   cardinality flag strings, and a comment drawn from a small vocabulary.
//! * `ints`: two sequential `Int64` columns, a compressible extreme.
//! * `random`: a 32-byte random `Bytes` datum, an incompressible extreme.

use mz_alloc_default as _;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::pool::{ExtentCodec, IDENTITY_CODEC};
use mz_repr::{Datum, Row};
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::chunk::LZ4_CODEC;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use std::hint::black_box;
use timely::container::{ContainerBuilder, PushInto};

type Update = (Row, u64, i64);

const SHIP_MODES: [&str; 7] = ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];
const FLAGS: [&str; 3] = ["A", "N", "R"];
const WORDS: [&str; 16] = [
    "furiously",
    "carefully",
    "quickly",
    "slyly",
    "regular",
    "final",
    "express",
    "pending",
    "ironic",
    "bold",
    "deposits",
    "requests",
    "accounts",
    "packages",
    "theodolites",
    "pinto beans",
];

fn lineitem(i: u64, rng: &mut StdRng) -> Row {
    let mut comment = String::new();
    for _ in 0..rng.random_range(2..6) {
        if !comment.is_empty() {
            comment.push(' ');
        }
        comment.push_str(WORDS[rng.random_range(0..WORDS.len())]);
    }
    let mut row = Row::default();
    row.packer().extend([
        Datum::Int64(i64::try_from(i / 4).expect("key fits i64")),
        Datum::Int64(rng.random_range(0..200_000)),
        Datum::Int32(rng.random_range(1..51)),
        Datum::Float64(rng.random_range(900.0..105_000.0f64).into()),
        Datum::String(FLAGS[rng.random_range(0..FLAGS.len())]),
        Datum::String(SHIP_MODES[rng.random_range(0..SHIP_MODES.len())]),
        Datum::String(&comment),
    ]);
    row
}

fn ints(i: u64, _rng: &mut StdRng) -> Row {
    let i = i64::try_from(i).expect("key fits i64");
    Row::pack_slice(&[Datum::Int64(i), Datum::Int64(i * 3)])
}

fn random(_i: u64, rng: &mut StdRng) -> Row {
    let bytes: [u8; 32] = rng.random();
    Row::pack_slice(&[Datum::Bytes(&bytes)])
}

/// The serialized words of the first body a `ColumnBuilder` ships for rows
/// from `make`, all at one time with diff one, as during hydration.
fn shipped_body(make: fn(u64, &mut StdRng) -> Row) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(0);
    let mut builder = ColumnBuilder::<Update>::default();
    for i in 0.. {
        builder.push_into(&(make(i, &mut rng), 0u64, 1i64));
        if let Some(column) = builder.extract() {
            let Column::Align(body) = column else {
                panic!("a shipped body is serialized");
            };
            return body.as_words().to_vec();
        }
    }
    unreachable!("the builder ships before the counter wraps")
}

fn bench_codecs(c: &mut Criterion) {
    let shapes: [(&str, fn(u64, &mut StdRng) -> Row); 3] =
        [("lineitem", lineitem), ("ints", ints), ("random", random)];
    let codecs: [(&str, &'static dyn ExtentCodec); 2] =
        [("lz4", &LZ4_CODEC), ("identity", &IDENTITY_CODEC)];
    for (shape, make) in shapes {
        let words = shipped_body(make);
        let body: Vec<u8> = words.iter().flat_map(|w| w.to_ne_bytes()).collect();
        let body = body.as_slice();
        let mut stored = Vec::new();
        LZ4_CODEC.encode(body, &mut stored);
        eprintln!(
            "edge_codec/{shape}: body {} bytes, lz4 {} bytes, ratio {:.2}",
            body.len(),
            stored.len(),
            f64::cast_lossy(body.len()) / f64::cast_lossy(stored.len()),
        );

        let mut group = c.benchmark_group(format!("edge_codec/{shape}"));
        group.throughput(Throughput::Bytes(u64::cast_from(body.len())));
        for (name, codec) in codecs {
            let mut stored = Vec::new();
            group.bench_function(format!("{name}/encode"), |b| {
                b.iter(|| codec.encode(black_box(body), &mut stored))
            });
            codec.encode(body, &mut stored);
            let mut decoded = vec![0u8; body.len()];
            group.bench_function(format!("{name}/decode"), |b| {
                b.iter(|| codec.decode(black_box(&stored), &mut decoded))
            });
            assert_eq!(decoded, body, "{name} round-trips");
        }
        group.finish();
    }
}

criterion_group!(benches, bench_codecs);
criterion_main!(benches);
