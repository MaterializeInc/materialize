// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Guest functions used by the `mz-wasm-udf` tests.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use arrow_udf::function;

#[function("gcd(int, int) -> int")]
fn gcd(mut a: i32, mut b: i32) -> i32 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a.abs()
}

#[function("safe_div(bigint, bigint) -> bigint")]
fn safe_div(a: i64, b: i64) -> Result<i64, String> {
    a.checked_div(b).ok_or_else(|| "division by zero".into())
}

#[function("nullable_or_zero(int) -> int")]
fn nullable_or_zero(a: Option<i32>) -> i32 {
    a.unwrap_or(0)
}

#[function("reverse(string) -> string")]
fn reverse(s: &str) -> String {
    s.chars().rev().collect()
}

/// Jaro-Winkler similarity over Unicode scalar values, with the standard
/// prefix scale of 0.1 and a maximum prefix length of 4.
#[function("jaro_winkler(string, string) -> float64")]
fn jaro_winkler(a: &str, b: &str) -> f64 {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.is_empty() && b.is_empty() {
        return 1.0;
    }
    if a.is_empty() || b.is_empty() {
        return 0.0;
    }

    let window = (a.len().max(b.len()) / 2).saturating_sub(1);
    let mut a_matched = vec![false; a.len()];
    let mut b_matched = vec![false; b.len()];
    let mut matches = 0usize;
    for (i, ca) in a.iter().enumerate() {
        let lo = i.saturating_sub(window);
        let hi = (i + window + 1).min(b.len());
        for j in lo..hi {
            if !b_matched[j] && b[j] == *ca {
                a_matched[i] = true;
                b_matched[j] = true;
                matches += 1;
                break;
            }
        }
    }
    if matches == 0 {
        return 0.0;
    }

    let a_seq = a.iter().zip(&a_matched).filter(|(_, m)| **m).map(|(c, _)| c);
    let b_seq = b.iter().zip(&b_matched).filter(|(_, m)| **m).map(|(c, _)| c);
    let transpositions = a_seq.zip(b_seq).filter(|(x, y)| x != y).count() / 2;

    let m = matches as f64;
    let jaro = (m / a.len() as f64 + m / b.len() as f64 + (m - transpositions as f64) / m) / 3.0;
    let prefix = a.iter().zip(&b).take(4).take_while(|(x, y)| x == y).count();
    jaro + prefix as f64 * 0.1 * (1.0 - jaro)
}

#[function("byte_len(binary) -> int")]
fn byte_len(b: &[u8]) -> i32 {
    i32::try_from(b.len()).unwrap_or(i32::MAX)
}

#[function("halve(float64) -> float64")]
fn halve(f: f64) -> f64 {
    f / 2.0
}

#[function("is_even(bigint) -> boolean")]
fn is_even(i: i64) -> bool {
    i % 2 == 0
}

/// Panics on negative input, which aborts the guest.
#[function("trap_on_negative(int) -> int")]
fn trap_on_negative(i: i32) -> i32 {
    assert!(i >= 0, "negative input");
    i
}

/// Loops for `n` iterations, to exercise fuel limits.
#[function("spin(bigint) -> bigint")]
fn spin(n: i64) -> i64 {
    let mut acc: i64 = 0;
    for i in 0..n {
        acc = std::hint::black_box(acc.wrapping_add(i));
    }
    acc
}

/// Allocates `n` MiB and touches every page, to exercise memory limits.
#[function("alloc_mib(int) -> int")]
fn alloc_mib(n: i32) -> i32 {
    let len = usize::try_from(n).unwrap_or(0) * 1024 * 1024;
    let mut v = vec![0u8; len];
    for i in (0..len).step_by(4096) {
        v[i] = 1;
    }
    std::hint::black_box(&v);
    n
}

static COUNTER: AtomicI64 = AtomicI64::new(0);

/// Returns how many rows this instance has seen, violating the per-row
/// contract.
#[function("stateful(int) -> bigint")]
fn stateful(_i: i32) -> i64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Uses a default-hashed `HashMap`, which seeds itself through `random_get`.
#[function("distinct_chars(string) -> int")]
fn distinct_chars(s: &str) -> i32 {
    let mut seen = HashMap::new();
    for c in s.chars() {
        *seen.entry(c).or_insert(0) += 1;
    }
    i32::try_from(seen.len()).unwrap_or(i32::MAX)
}

/// Reads the wall clock, which the host freezes at the Unix epoch.
#[function("now_secs() -> bigint")]
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(-1)
}

/// Writes to stdout, which the host captures.
#[function("shout(string) -> string")]
fn shout(s: &str) -> String {
    println!("shouting {s}");
    s.to_uppercase()
}
