// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The C `malloc` family, forwarding to the allocator [`allocator`] selects
//! and reporting to the allocation tracker as [`Space::CHeap`].
//!
//! The workspace builds jemalloc with its `_rjem_` symbol prefix and mimalloc
//! without overriding `malloc`, so these definitions in the executable are
//! what glibc's dynamic linker binds every `malloc` call to, including those
//! inside glibc and other shared libraries.
//! The set is the one glibc's manual lists for replacing `malloc`, plus
//! `reallocarray`, the C23 sized frees, and the `__libc_*` aliases that some
//! code calls directly. Memory from any of these
//! functions must be freed by `free` here, so a missing export would hand
//! glibc's allocator a pointer it did not allocate.

use std::ffi::{c_int, c_void};

#[cfg(feature = "mimalloc")]
use libmimalloc_sys as mi;
use mz_alloc_track::{FreeKind, Space, on_alloc, on_free};
#[cfg(feature = "jemalloc")]
use tikv_jemalloc_sys as je;

use crate::{Allocator, allocator};

/// Evaluates the expression for the selected allocator.
macro_rules! dispatch {
    (jemalloc => $je:expr, mimalloc => $mi:expr $(,)?) => {
        match allocator() {
            #[cfg(feature = "jemalloc")]
            Some(Allocator::Jemalloc) => $je,
            #[cfg(feature = "mimalloc")]
            Some(Allocator::Mimalloc) => $mi,
            _ => unreachable!("an allocator is compiled in"),
        }
    };
}

/// The smallest alignment `posix_memalign` accepts.
const MIN_ALIGN: usize = std::mem::size_of::<*mut c_void>();

/// The kernel's page size, which `valloc` and `pvalloc` align to.
fn page_size() -> usize {
    // SAFETY: `sysconf` has no preconditions and does not allocate.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).unwrap_or(4096)
}

#[inline]
fn track(ptr: *mut c_void, size: usize) -> *mut c_void {
    if !ptr.is_null() {
        on_alloc(ptr.cast(), size, Space::CHeap);
    }
    ptr
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn malloc(size: usize) -> *mut c_void {
    // SAFETY: forwarded under the caller's contract.
    track(
        unsafe { dispatch!(jemalloc => je::malloc(size), mimalloc => mi::mi_malloc(size)) },
        size,
    )
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn calloc(number: usize, size: usize) -> *mut c_void {
    // SAFETY: forwarded under the caller's contract.
    let ptr = unsafe {
        dispatch!(
            jemalloc => je::calloc(number, size),
            mimalloc => mi::mi_calloc(number, size),
        )
    };
    // A successful `calloc` implies the product does not overflow.
    track(ptr, number.wrapping_mul(size))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    // Ending the old allocation first is required by `on_free`'s contract.
    // If the reallocation fails, the old allocation survives untracked,
    // which loses one sample and nothing else.
    if !ptr.is_null() {
        on_free(ptr.cast(), FreeKind::Realloc);
    }
    // SAFETY: forwarded under the caller's contract.
    let new = unsafe {
        dispatch!(
            jemalloc => je::realloc(ptr, size),
            mimalloc => mi::mi_realloc(ptr, size),
        )
    };
    track(new, size)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn reallocarray(ptr: *mut c_void, number: usize, size: usize) -> *mut c_void {
    match number.checked_mul(size) {
        // SAFETY: forwarded under the caller's contract.
        Some(total) => unsafe { realloc(ptr, total) },
        None => {
            // SAFETY: `errno` is thread-local and always writable.
            unsafe { *libc::__errno_location() = libc::ENOMEM };
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free(ptr: *mut c_void) {
    if !ptr.is_null() {
        on_free(ptr.cast(), FreeKind::Dealloc);
    }
    // SAFETY: forwarded under the caller's contract.
    unsafe { dispatch!(jemalloc => je::free(ptr), mimalloc => mi::mi_free(ptr)) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_sized(ptr: *mut c_void, _size: usize) {
    // SAFETY: forwarded under the caller's contract.
    unsafe { free(ptr) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_aligned_sized(ptr: *mut c_void, _alignment: usize, _size: usize) {
    // SAFETY: forwarded under the caller's contract.
    unsafe { free(ptr) }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn posix_memalign(
    out: *mut *mut c_void,
    alignment: usize,
    size: usize,
) -> c_int {
    // SAFETY: forwarded under the caller's contract.
    let rc = unsafe {
        dispatch!(
            jemalloc => je::posix_memalign(out, alignment, size),
            mimalloc => mi::mi_posix_memalign(out, alignment, size),
        )
    };
    if rc == 0 {
        // SAFETY: on success, `posix_memalign` stored the allocation in `out`.
        track(unsafe { *out }, size);
    }
    rc
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn aligned_alloc(alignment: usize, size: usize) -> *mut c_void {
    // SAFETY: forwarded under the caller's contract.
    let ptr = unsafe {
        dispatch!(
            jemalloc => je::aligned_alloc(alignment, size),
            mimalloc => mi::mi_aligned_alloc(alignment, size),
        )
    };
    track(ptr, size)
}

/// Allocates `size` bytes aligned to `alignment`, or returns null with
/// `errno` set like glibc's `memalign`.
fn memalign_impl(alignment: usize, size: usize) -> *mut c_void {
    let mut out = std::ptr::null_mut();
    // SAFETY: `out` is a valid destination, and an alignment raised to at
    // least `MIN_ALIGN` meets `posix_memalign`'s requirement whenever the
    // caller's alignment is a power of two. Other alignments fail with
    // `EINVAL`, which glibc's `memalign` reports the same way.
    let rc = unsafe { posix_memalign(&mut out, alignment.max(MIN_ALIGN), size) };
    if rc != 0 {
        // SAFETY: `errno` is thread-local and always writable.
        unsafe { *libc::__errno_location() = rc };
        return std::ptr::null_mut();
    }
    out
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn memalign(alignment: usize, size: usize) -> *mut c_void {
    memalign_impl(alignment, size)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn valloc(size: usize) -> *mut c_void {
    memalign_impl(page_size(), size)
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn pvalloc(size: usize) -> *mut c_void {
    let page = page_size();
    match size.max(1).checked_next_multiple_of(page) {
        Some(size) => memalign_impl(page, size),
        None => {
            // SAFETY: `errno` is thread-local and always writable.
            unsafe { *libc::__errno_location() = libc::ENOMEM };
            std::ptr::null_mut()
        }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn malloc_usable_size(ptr: *mut c_void) -> usize {
    // SAFETY: forwarded under the caller's contract.
    unsafe {
        dispatch!(
            jemalloc => je::malloc_usable_size(ptr),
            mimalloc => mi::mi_usable_size(ptr),
        )
    }
}

/// Defines `__libc_<name>` as an alias of `<name>`, for code that calls
/// glibc's internal entry points directly.
macro_rules! libc_alias {
    ($alias:ident => $target:ident($($arg:ident: $ty:ty),*) -> $ret:ty) => {
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn $alias($($arg: $ty),*) -> $ret {
            // SAFETY: forwarded under the caller's contract.
            unsafe { $target($($arg),*) }
        }
    };
}

libc_alias!(__libc_malloc => malloc(size: usize) -> *mut c_void);
libc_alias!(__libc_calloc => calloc(number: usize, size: usize) -> *mut c_void);
libc_alias!(__libc_realloc => realloc(ptr: *mut c_void, size: usize) -> *mut c_void);
libc_alias!(__libc_free => free(ptr: *mut c_void) -> ());
libc_alias!(__libc_memalign => memalign(alignment: usize, size: usize) -> *mut c_void);
libc_alias!(__libc_valloc => valloc(size: usize) -> *mut c_void);
libc_alias!(__libc_pvalloc => pvalloc(size: usize) -> *mut c_void);
libc_alias!(__posix_memalign => posix_memalign(out: *mut *mut c_void, alignment: usize, size: usize) -> c_int);
