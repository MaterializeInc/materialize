// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use mz_repr::strconv;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

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
