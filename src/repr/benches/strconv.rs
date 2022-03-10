// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use mz_repr::strconv;

fn bench_parse_float32(c: &mut Criterion) {
    for s in &["-3.0", "9.7", "NaN", "inFiNiTy"] {
        c.bench_with_input(BenchmarkId::new("parse_float32", s), s, |b, s| {
            b.iter(|| strconv::parse_float32(s).unwrap())
        });
    }
}

fn bench_parse_numeric(c: &mut Criterion) {
    for s in &["-135412353251", "1.030340E11"] {
        c.bench_with_input(BenchmarkId::new("parse_numeric", s), s, |b, s| {
            b.iter(|| strconv::parse_numeric(s).unwrap())
        });
    }
}

fn bench_parse_jsonb(c: &mut Criterion) {
    let input = include_str!("testdata/twitter.json");
    c.bench_function("parse_jsonb", |b| {
        b.iter(|| black_box(strconv::parse_jsonb(input).unwrap()))
    });
}

fn bench_format_list_simple(c: &mut Criterion) {
    let mut rng = StdRng::from_seed([0; 32]);
    let list: Vec<i32> = (0..(1 << 12)).map(|_| rng.gen()).collect();
    c.bench_function("format_list simple", |b| {
        b.iter(|| {
            let mut buf = String::new();
            strconv::format_list(&mut buf, black_box(&list), |lw, i| {
                Ok::<_, ()>(strconv::format_int32(lw.nonnull_buffer(), *i))
            })
            .unwrap()
        })
    });
}

fn bench_format_list_nested(c: &mut Criterion) {
    let mut rng = StdRng::from_seed([0; 32]);
    const STRINGS: &[&str] = &[
        "NULL",
        "Po1bcC3mQWeYrMh6XaAM3ibM9CDDOoZK",
        r#""Elementary, my dear Watson," said Sherlock."#,
        "14VyaJllwQiPHRO2aNBo7p3P4v8cTLVB",
    ];
    let list: Vec<Vec<Vec<String>>> = (0..8)
        .map(|_| {
            (0..rng.gen_range(0..16))
                .map(|_| {
                    (1..rng.gen_range(0..16))
                        .map(|_| STRINGS.choose(&mut rng).unwrap())
                        .map(|s| (*s).to_owned())
                        .collect()
                })
                .collect()
        })
        .collect();

    c.bench_function("format_list nested", |b| {
        b.iter(|| {
            let mut buf = String::new();
            strconv::format_list(&mut buf, black_box(&list), |lw, list| {
                strconv::format_list(lw.nonnull_buffer(), list, |lw, list| {
                    strconv::format_list(lw.nonnull_buffer(), list, |lw, s| {
                        Ok::<_, ()>(strconv::format_string(lw.nonnull_buffer(), s))
                    })
                })
            })
            .unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_format_list_simple,
    bench_format_list_nested,
    bench_parse_numeric,
    bench_parse_float32,
    bench_parse_jsonb
);
criterion_main!(benches);
