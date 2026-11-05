// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Helpers for tests that install the tracker as the global allocator.
//!
//! Such tests share one process-wide tracker, so each identifies its own
//! sites by symbolizing stacks for a uniquely named function.

use crate::{FreeKind, Snapshot, Space, set_sample_interval};

/// Whether any frame of `stack` symbolizes to a name containing `needle`.
pub fn stack_mentions(stack: &[usize], needle: &str) -> bool {
    stack.iter().any(|&ip| {
        let mut found = false;
        backtrace::resolve(std::ptr::without_provenance_mut(ip), |sym| {
            if let Some(name) = sym.name() {
                found |= name.to_string().contains(needle);
            }
        });
        found
    })
}

/// Estimated live bytes in `space` allocated by a stack mentioning `needle`.
pub fn live_bytes(snapshot: &Snapshot, space: Space, needle: &str) -> f64 {
    snapshot
        .live
        .iter()
        .filter(|s| s.space == space)
        .filter(|s| stack_mentions(&snapshot.stacks[usize::try_from(s.stack).unwrap()], needle))
        .map(|s| s.bytes)
        .sum()
}

/// Estimated bytes in `space` allocated by a stack mentioning `alloc_needle`
/// and deallocated by one mentioning `free_needle`.
pub fn freed_bytes(
    snapshot: &Snapshot,
    space: Space,
    alloc_needle: &str,
    free_needle: &str,
) -> f64 {
    snapshot
        .freed
        .iter()
        .filter(|s| s.space == space && s.kind == FreeKind::Dealloc)
        .filter(|s| {
            stack_mentions(
                &snapshot.stacks[usize::try_from(s.alloc_stack).unwrap()],
                alloc_needle,
            ) && stack_mentions(
                &snapshot.stacks[usize::try_from(s.free_stack).unwrap()],
                free_needle,
            )
        })
        .map(|s| s.bytes)
        .sum()
}

/// Samples every allocation of at least 1 MiB with weight 1 on this thread,
/// which makes the estimates exact up to small unrelated allocations.
pub fn sample_every_mib() {
    set_sample_interval(4096);
    // The thread picks up the interval at its next sample, and a countdown
    // drawn at the default interval can skip a 1 MiB allocation. One 64 MiB
    // allocation consumes any such countdown.
    drop(std::hint::black_box(vec![0u8; 64 << 20]));
}

/// Asserts that `actual` is within 1% of `expected`.
pub fn assert_close(actual: f64, expected: usize, what: &str) {
    let expected = f64::from(u32::try_from(expected).expect("test sizes fit u32"));
    assert!(
        (actual - expected).abs() <= expected * 0.01,
        "{what}: {actual} bytes, expected about {expected}",
    );
}
