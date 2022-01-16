// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use expr::like_pattern;

// SMART, by Shel Silverstein
const POEM: &[&str] = &[
    "My dad gave me one dollar bill",
    "'Cause I'm his smartest son,",
    "And I swapped it for two shiny quarters",
    "'Cause two is more than one!",
    //
    "And then I took the quarters",
    "And traded them to Lou",
    "For three dimes -- I guess he don't know",
    "That three is more than two!",
    //
    "Just then, along came old blind Bates",
    "And just 'cause he can't see",
    "He gave me four nickels for my three dimes,",
    "And four is more then three!",
    //
    "And I took the nickels to Hiram Coombs",
    "Down at the seed-feed store,",
    "And the fool gave me five pennies for them,",
    "And five is more than four!",
    //
    "And then I went and showed my dad,",
    "And he got red in the cheeks",
    "And closed his eye and shook his head--",
    "Too proud of me to speak!",
];

fn search_poem(needle: &like_pattern::Matcher) {
    for i in 0..POEM.len() {
        needle.is_match(black_box(POEM[i]));
    }
}

fn bench_op<F>(c: &mut Criterion, name: &str, mut compile_fn: F)
where
    F: FnMut(&str) -> like_pattern::Matcher,
{
    let mut group = c.benchmark_group("like_pattern");

    // Test how long it takes to compile a pattern.
    group.bench_function(format!("{}_compile", name), |b| {
        b.iter(|| compile_fn(black_box("W_rdle%fun%")))
    });

    // Test some search scenarios:
    let mut matcher = compile_fn("And");
    group.bench_function(format!("{}_search_literal", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("And%");
    group.bench_function(format!("{}_search_starts_with", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%is%");
    group.bench_function(format!("{}_search_contains", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%is%more%");
    group.bench_function(format!("{}_search_contains2", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%is%more%than%");
    group.bench_function(format!("{}_search_contains3", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%!");
    group.bench_function(format!("{}_search_ends_with", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%e%e%e%e%e%e?");
    group.bench_function(format!("{}_search_expensive", name), |b| {
        b.iter(|| matcher.is_match(black_box("wheeeeeeeeeeeeeeeeeeeeee!")))
    });
}

pub fn bench_ilike(c: &mut Criterion) {
    bench_op(c, "ilike", |pattern| {
        like_pattern::compile(pattern, true, '\\').unwrap()
    });
}

pub fn bench_like(c: &mut Criterion) {
    bench_op(c, "like", |pattern| {
        like_pattern::compile(pattern, false, '\\').unwrap()
    });
}

criterion_group!(benches, bench_like, bench_ilike);
criterion_main!(benches);
