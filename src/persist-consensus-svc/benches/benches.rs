// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Criterion benchmarks for envelope encryption (AES-256-GCM) overhead.

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use mz_ore::hint::black_box;
use mz_persist_consensus_svc::crypto::{decrypt_with_key, encrypt_with_dek, parse_envelope};

fn test_key() -> [u8; 32] {
    [0x42u8; 32]
}

fn test_wrapped_dek() -> Vec<u8> {
    vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE]
}

fn random_payload(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    aws_lc_rs::rand::fill(&mut buf).expect("failed to generate random payload");
    buf
}

const SIZES: &[(usize, &str)] = &[
    (256, "256B"),
    (4 * 1024, "4KiB"),
    (64 * 1024, "64KiB"),
    (1024 * 1024, "1MiB"),
];

fn bench_crypto(c: &mut Criterion) {
    let key = test_key();
    let wrapped = test_wrapped_dek();

    // Encrypt benchmarks
    {
        let mut group = c.benchmark_group("crypto/encrypt");
        for &(size, label) in SIZES {
            let payload = random_payload(size);
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_function(label, |b| {
                b.iter(|| {
                    black_box(encrypt_with_dek(&key, &wrapped, &payload).unwrap());
                });
            });
        }
        group.finish();
    }

    // Decrypt benchmarks
    {
        let mut group = c.benchmark_group("crypto/decrypt");
        for &(size, label) in SIZES {
            let payload = random_payload(size);
            let encrypted = encrypt_with_dek(&key, &wrapped, &payload).unwrap();
            let (_, nonce_ct) = parse_envelope(&encrypted).unwrap();
            let nonce_ct = nonce_ct.to_vec();
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_function(label, |b| {
                b.iter(|| {
                    black_box(decrypt_with_key(&key, &nonce_ct).unwrap());
                });
            });
        }
        group.finish();
    }

    // Roundtrip benchmarks (encrypt -> parse -> decrypt)
    {
        let mut group = c.benchmark_group("crypto/roundtrip");
        for &(size, label) in SIZES {
            let payload = random_payload(size);
            group.throughput(Throughput::Bytes(size as u64));
            group.bench_function(label, |b| {
                b.iter(|| {
                    let encrypted = encrypt_with_dek(&key, &wrapped, &payload).unwrap();
                    let (_, nonce_ct) = parse_envelope(&encrypted).unwrap();
                    black_box(decrypt_with_key(&key, nonce_ct).unwrap());
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, bench_crypto);
criterion_main!(benches);
