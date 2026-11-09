// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use mz_ore::cast::CastLossy;

use super::BucketScheme;

fn scheme(b: u32) -> BucketScheme {
    BucketScheme::new(b).expect("valid bucket count")
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

#[mz_ore::test]
fn bucket_scheme_validates_per_octave() {
    for b in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
        assert_eq!(scheme(b).per_octave(), b);
    }
    for b in [0, 3, 5, 6, 257, 512] {
        assert!(BucketScheme::new(b).is_none(), "{b}");
    }
}

#[mz_ore::test]
fn bucket_boundaries() {
    let s = scheme(4);
    assert_eq!(s.bucket(ms(1)), 0);
    assert_eq!(s.bucket(ms(2)), 4);
    assert_eq!(s.bucket(ms(1024)), 40);
    assert_eq!(s.bucket(ms(1025)), 41);
    for x in 1..10_000u64 {
        let k = s.bucket(ms(x));
        let x = f64::cast_lossy(x);
        assert!(s.upper_bound_ms(k - 1) < x, "{x} above bucket {k}");
        assert!(x <= s.upper_bound_ms(k), "{x} below bucket {k}");
    }
}

#[mz_ore::test]
fn bucket_exact_at_powers_of_two() {
    for b in [1, 2, 4, 8, 16, 64, 256] {
        let s = scheme(b);
        let bi = i32::try_from(b).unwrap();
        for e in 0..64i32 {
            let x = 1u64 << e;
            assert_eq!(s.bucket(ms(x)), e * bi, "b={b} 2^{e}");
            if e > 0 {
                // One past the bound is in the next bucket or, for a coarse scheme, a later one
                // of the same octave.
                assert!(s.bucket(ms(x + 1)) > e * bi, "b={b} 2^{e}+1");
                assert!(s.bucket(ms(x + 1)) <= (e + 1) * bi, "b={b} 2^{e}+1");
            }
            if e > 1 {
                // Just below 2^e, the top of the octave: the bucket is at most 2^e's.
                assert!(s.bucket(ms(x - 1)) <= e * bi, "b={b} 2^{e}-1");
                assert!(s.bucket(ms(x - 1)) > (e - 1) * bi, "b={b} 2^{e}-1");
            }
        }
        assert_eq!(s.bucket(ms(u64::MAX)), 64 * bi);
    }
}

#[mz_ore::test]
fn bucket_bounds_are_inclusive_above() {
    for b in [1, 2, 4, 8] {
        let s = scheme(b);
        assert_eq!(s.bucket(ms(0)), 0, "b={b}");
        assert_eq!(s.bucket(ms(1)), 0, "b={b}");
        assert_eq!(s.bucket(ms(2)), i32::try_from(b).unwrap(), "b={b}");
        for k in 1..=64 {
            let bound = s.upper_bound_ms(k);
            // Only bounds that are integers above 1 ms are checked, the others have no integer
            // staleness that sits exactly on the bound.
            if bound > 1.0 && bound == bound.floor() {
                assert_eq!(s.bucket(ms(u64::cast_lossy(bound))), k, "b={b} k={k}");
            }
        }
    }
}

#[mz_ore::test]
fn bucket_rounds_up_to_whole_milliseconds() {
    let s = scheme(4);
    // 1.5 ms is above the 1 ms bound of bucket 0, so it lands with 2 ms.
    assert_eq!(s.bucket(Duration::from_micros(1500)), s.bucket(ms(2)));
    assert_eq!(s.bucket(Duration::from_micros(1001)), s.bucket(ms(2)));
    assert_eq!(s.bucket(Duration::from_nanos(1)), 0);
    assert_eq!(s.bucket(Duration::MAX), s.bucket(ms(u64::MAX)));
}
