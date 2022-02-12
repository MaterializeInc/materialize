// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use mz_expr::like_pattern;

// Ozymandias, by Percy Bysshe Shelley
// written in 1818, in the public domain.
const POEM: &[&str] = &[
    "I met a traveller from an antique land,",
    "Who said -- Two vast and trunkless legs of stone",
    "Stand in the desert... Near them, on the sand,",
    "Half sunk a shattered visage lies, whose frown,",
    "And wrinkled lip, and sneer of cold command,",
    "Tell that its sculptor well those passions read",
    "Which yet survive, stamped on these lifeless things,",
    "The hand that mocked them, and the heart that fed;",
    "And on the pedestal, these words appear:",
    "My name is Ozymandias, King of Kings;",
    "Look on my Works, ye Mighty, and despair!",
    "Nothing beside remains. Round the decay",
    "Of that colossal Wreck, boundless and bare",
    "The lone and level sands stretch far away.",
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
        b.iter(|| compile_fn(black_box("a%b_c%d_e")))
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
    matcher = compile_fn("%and%");
    group.bench_function(format!("{}_search_contains", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%and%the%");
    group.bench_function(format!("{}_search_contains2", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%and%the%th%");
    group.bench_function(format!("{}_search_contains3", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%!");
    group.bench_function(format!("{}_search_ends_with", name), |b| {
        b.iter(|| search_poem(&matcher))
    });
    matcher = compile_fn("%e%e%e%e%e?");
    group.bench_function(format!("{}_search_adversarial", name), |b| {
        b.iter(|| matcher.is_match(black_box("wheeeeeeeeeeeeeeeeeeeeee!")))
    });
}

pub fn bench_ilike(c: &mut Criterion) {
    bench_op(c, "ilike", |pattern| {
        like_pattern::compile(pattern, true, like_pattern::EscapeBehavior::default()).unwrap()
    });
}

pub fn bench_like(c: &mut Criterion) {
    bench_op(c, "like", |pattern| {
        like_pattern::compile(pattern, false, like_pattern::EscapeBehavior::default()).unwrap()
    });
}

criterion_group!(benches, bench_like, bench_ilike);
criterion_main!(benches);
