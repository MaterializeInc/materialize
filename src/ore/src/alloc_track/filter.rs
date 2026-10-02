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

//! A sparse, lock-free filter over the addresses of live samples.
//!
//! Every free consults the filter, so a negative answer must be cheap and
//! false negatives impossible. The filter is a two-level radix table: a
//! static root of leaf pointers, one per 256 MiB of address space, and
//! lazily allocated leaves holding one counter per 4 KiB page. A page's
//! counter is the number of live samples starting in it. Frees in address
//! ranges without samples stop at a null root entry, and frees elsewhere
//! cost a second load whose cache line covers 256 KiB of neighbouring
//! addresses. Leaves are never freed: they cost 64 KiB per 256 MiB of
//! sampled address space.

use std::alloc::Layout;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

const GRANULE_SHIFT: u32 = 12;
const LEAF_SHIFT: u32 = 28;
const LEAF_LEN: usize = 1 << (LEAF_SHIFT - GRANULE_SHIFT);
/// Root entries covering a 48-bit address space. Addresses beyond it are
/// not filtered: they always answer "maybe".
const ROOT_LEN: usize = 1 << (48 - LEAF_SHIFT);

struct Leaf([AtomicU8; LEAF_LEN]);

/// Zero-initialized, so it lives in `.bss` and costs no physical memory
/// until a leaf is published.
static ROOT: [AtomicPtr<Leaf>; ROOT_LEN] =
    [const { AtomicPtr::new(std::ptr::null_mut()) }; ROOT_LEN];

fn split(addr: usize) -> (usize, usize) {
    (addr >> LEAF_SHIFT, (addr >> GRANULE_SHIFT) & (LEAF_LEN - 1))
}

/// Whether a live sample may start at `addr`. Never false for an address
/// passed to [`insert`] and not yet to [`remove`].
#[inline]
pub(super) fn maybe_contains(addr: usize) -> bool {
    let (root, index) = split(addr);
    let Some(entry) = ROOT.get(root) else {
        return true;
    };
    let leaf = entry.load(Ordering::Acquire);
    if leaf.is_null() {
        return false;
    }
    // SAFETY: a published leaf is never freed or moved.
    let leaf = unsafe { &*leaf };
    leaf.0[index].load(Ordering::Relaxed) != 0
}

/// Counts a live sample at `addr`. Returns false, counting nothing, when no
/// leaf could be allocated; the caller must then not record the sample.
pub(super) fn insert(addr: usize) -> bool {
    let (root, index) = split(addr);
    let Some(entry) = ROOT.get(root) else {
        return true;
    };
    let mut leaf = entry.load(Ordering::Acquire);
    if leaf.is_null() {
        // SAFETY: `Leaf` has nonzero size.
        let new = unsafe { std::alloc::alloc_zeroed(Layout::new::<Leaf>()) }.cast::<Leaf>();
        if new.is_null() {
            return false;
        }
        match entry.compare_exchange(
            std::ptr::null_mut(),
            new,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => leaf = new,
            Err(existing) => {
                // SAFETY: `new` came from `alloc_zeroed` with this layout
                // and was never published.
                unsafe { std::alloc::dealloc(new.cast(), Layout::new::<Leaf>()) };
                leaf = existing;
            }
        }
    }
    // SAFETY: a published leaf is never freed or moved, and an all-zero
    // `Leaf` is a valid array of zero counters.
    let leaf = unsafe { &*leaf };
    // A saturated counter sticks: its page answers "maybe" forever, which
    // costs registry lookups but never a false negative.
    let _ = leaf.0[index].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
        (c != u8::MAX).then(|| c + 1)
    });
    true
}

/// Uncounts a live sample at `addr` previously counted by [`insert`].
pub(super) fn remove(addr: usize) {
    let (root, index) = split(addr);
    let Some(entry) = ROOT.get(root) else {
        return;
    };
    let leaf = entry.load(Ordering::Acquire);
    if leaf.is_null() {
        return;
    }
    // SAFETY: a published leaf is never freed or moved.
    let leaf = unsafe { &*leaf };
    let _ = leaf.0[index].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
        (c != 0 && c != u8::MAX).then(|| c - 1)
    });
}
