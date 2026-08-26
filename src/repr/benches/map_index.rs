// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ground truth for the `DatumMap` in-map index, on the real `Row` encoding.
//!
//! Measures the two lookup strategies that exist in the tree, `DatumMap::get`
//! (binary search over the index) and a linear scan over `iter()`, plus the
//! pack cost, over a corpus large enough that the maps do not sit in L2. The
//! numbers calibrate the standalone layout probe that explores index designs
//! this crate does not implement.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_repr::{Datum, Row};

/// Total corpus bytes per configuration. Above this machine's L2 so that the
/// lookups pay a realistic share of memory traffic.
const CORPUS_BYTES: usize = 32 << 20;

fn keys_for(n: usize, style: &str) -> Vec<String> {
    let mut keys: Vec<String> = match style {
        "typical" => (0..n).map(|i| format!("field_name_{i:03}")).collect(),
        "prefixed" => (0..n)
            .map(|i| format!("com.example.service.metrics.dimension_{i:03}"))
            .collect(),
        _ => unreachable!("unknown key style"),
    };
    keys.sort();
    keys
}

fn map_row(keys: &[String]) -> Row {
    let mut row = Row::default();
    row.packer().push_dict_with(|packer| {
        for (i, k) in keys.iter().enumerate() {
            packer.push(Datum::String(k));
            packer.push(Datum::Int64(i64::try_from(i).unwrap()));
        }
    });
    row
}

/// The probe set of a "JSON to columns" projection: four present keys spread
/// over the map plus one miss.
fn probes(keys: &[String]) -> Vec<String> {
    let n = keys.len();
    vec![
        keys[0].clone(),
        keys[n / 3].clone(),
        keys[n * 2 / 3].clone(),
        keys[n - 1].clone(),
        "not_a_present_key_at_all".to_string(),
    ]
}

fn bench_map_index(c: &mut Criterion) {
    for style in ["typical", "prefixed"] {
        for n in [3usize, 8, 16, 32, 50, 100, 250, 500] {
            let keys = keys_for(n, style);
            let probes = probes(&keys);
            let one = map_row(&keys);
            let row_bytes = one.byte_len();
            let rows = (CORPUS_BYTES / row_bytes).max(64);
            let corpus: Vec<Row> = (0..rows).map(|_| map_row(&keys)).collect();

            let mut group = c.benchmark_group(format!("map_index/{style}/n={n}"));
            group.throughput(Throughput::Elements(
                u64::try_from(corpus.len() * probes.len()).unwrap(),
            ));

            group.bench_function("get_indexed", |b| {
                b.iter(|| {
                    let mut hits = 0u64;
                    for row in &corpus {
                        let datum = row.unpack_first();
                        let map = datum.unwrap_map();
                        for p in &probes {
                            if black_box(map.get(black_box(p.as_str()))).is_some() {
                                hits += 1;
                            }
                        }
                    }
                    black_box(hits)
                })
            });

            group.bench_function("get_linear_scan", |b| {
                b.iter(|| {
                    let mut hits = 0u64;
                    for row in &corpus {
                        let datum = row.unpack_first();
                        let map = datum.unwrap_map();
                        for p in &probes {
                            let found = map
                                .iter()
                                .find(|(k, _v)| *k == black_box(p.as_str()))
                                .map(|(_k, v)| v);
                            if black_box(found).is_some() {
                                hits += 1;
                            }
                        }
                    }
                    black_box(hits)
                })
            });

            group.finish();

            let mut group = c.benchmark_group(format!("map_pack/{style}/n={n}"));
            group.throughput(Throughput::Elements(1));
            group.bench_function("push_dict_with", |b| {
                let mut row = Row::default();
                b.iter(|| {
                    let mut packer = row.packer();
                    packer.push_dict_with(|packer| {
                        for (i, k) in keys.iter().enumerate() {
                            packer.push(Datum::String(k));
                            packer.push(Datum::Int64(i64::try_from(i).unwrap()));
                        }
                    });
                    black_box(row.byte_len())
                })
            });
            group.bench_function("push_indexed_dict_with", |b| {
                let mut row = Row::default();
                b.iter(|| {
                    let mut packer = row.packer();
                    packer.push_indexed_dict_with(|builder| {
                        for (i, k) in keys.iter().enumerate() {
                            builder.push_entry(k, |packer| {
                                packer.push(Datum::Int64(i64::try_from(i).unwrap()))
                            });
                        }
                    });
                    black_box(row.byte_len())
                })
            });
            group.finish();

            // Payload accounting, so the standalone probe's byte model can be
            // checked against the real encoding.
            eprintln!("SIZE {style} n={n} row_bytes={row_bytes} corpus_rows={rows}");
        }
    }
}

criterion_group!(benches, bench_map_index);
criterion_main!(benches);
