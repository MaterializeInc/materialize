// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The `malloc` family that follows this executable's definitions in symbol
//! lookup order: a preloaded library's, such as heaptrack's, or else glibc's.
//!
//! While a thread runs one of these functions, the allocations it makes
//! reenter this executable's `malloc` family, for example when heaptrack
//! records a call. Those allocations belong to the next allocator and skip
//! the tracker, see [`inside`].
//!
//! The functions are resolved with `dlsym(RTLD_NEXT, ..)` on first use.
//! `dlsym` can allocate, which reenters this module before resolution
//! finishes. Allocations made while resolution is in progress, on any thread,
//! come from a static bootstrap arena, whose memory is never reused.

use std::alloc::{GlobalAlloc, Layout};
use std::ffi::{CStr, c_int, c_void};
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};

use mz_alloc_track::AllocatorHooks;

/// The alignment glibc's `malloc` guarantees on 64-bit targets. It is 16 on
/// 32-bit x86 as well, so this underestimates there, which is safe.
const MALLOC_ALIGN: usize = 2 * std::mem::size_of::<usize>();

const UNRESOLVED: u8 = 0;
const RESOLVING: u8 = 1;
const RESOLVED: u8 = 2;

static STATE: AtomicU8 = AtomicU8::new(UNRESOLVED);

static MALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static CALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static REALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static FREE: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static POSIX_MEMALIGN: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static ALIGNED_ALLOC: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());
static MALLOC_USABLE_SIZE: AtomicPtr<c_void> = AtomicPtr::new(std::ptr::null_mut());

static SYMBOLS: [(&AtomicPtr<c_void>, &CStr); 7] = [
    (&MALLOC, c"malloc"),
    (&CALLOC, c"calloc"),
    (&REALLOC, c"realloc"),
    (&FREE, c"free"),
    (&POSIX_MEMALIGN, c"posix_memalign"),
    (&ALIGNED_ALLOC, c"aligned_alloc"),
    (&MALLOC_USABLE_SIZE, c"malloc_usable_size"),
];

std::thread_local! {
    static INSIDE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Whether this thread is inside one of the next allocator's functions.
///
/// NOTE: Tracking such reentrant calls deadlocks. heaptrack holds its lock
/// while it allocates, and the tracker allocates while it holds its registry
/// lock, so a thread waiting for the registry inside heaptrack and a thread
/// waiting for heaptrack inside the registry block each other.
#[inline]
pub(crate) fn inside() -> bool {
    INSIDE.try_with(|inside| inside.get()).unwrap_or(false)
}

/// Runs `f`, a call into the next allocator, with [`inside`] set.
#[inline]
fn enter<R>(f: impl FnOnce() -> R) -> R {
    // A const-initialized `Cell` has no destructor, so the access succeeds
    // on any thread at any time, including during thread exit.
    let prev = INSIDE
        .try_with(|inside| inside.replace(true))
        .unwrap_or(false);
    let result = f();
    let _ = INSIDE.try_with(|inside| inside.set(prev));
    result
}

/// Whether the functions are resolved, resolving them if no thread has
/// started to. `false` means a resolution is in progress, either on this
/// thread inside `dlsym` or on another thread.
#[inline]
fn resolved() -> bool {
    match STATE.load(Ordering::Acquire) {
        RESOLVED => true,
        UNRESOLVED => resolve(),
        _ => false,
    }
}

#[cold]
fn resolve() -> bool {
    if STATE
        .compare_exchange(UNRESOLVED, RESOLVING, Ordering::Acquire, Ordering::Relaxed)
        .is_err()
    {
        return STATE.load(Ordering::Acquire) == RESOLVED;
    }
    for (slot, name) in &SYMBOLS {
        // SAFETY: `name` is NUL-terminated, and `RTLD_NEXT` is a valid
        // pseudo-handle in a dynamically linked executable.
        let f = unsafe { libc::dlsym(libc::RTLD_NEXT, name.as_ptr()) };
        if f.is_null() {
            // Every process links glibc, which defines all of these, so a
            // missing symbol means the process is not what this module
            // assumes, and there is no allocator to fall back to.
            // SAFETY: `abort` has no preconditions.
            unsafe { libc::abort() };
        }
        slot.store(f, Ordering::Relaxed);
    }
    STATE.store(RESOLVED, Ordering::Release);
    true
}

/// Reads a resolved function pointer.
///
/// # Safety
///
/// [`resolved`] must have returned `true`, and `F` must be the C signature
/// of the function `slot` holds.
#[inline]
unsafe fn load<F: Copy>(slot: &AtomicPtr<c_void>) -> F {
    let ptr = slot.load(Ordering::Relaxed);
    // SAFETY: the caller's contract, and function pointers have the size of
    // data pointers on every target glibc supports.
    unsafe { std::mem::transmute_copy::<*mut c_void, F>(&ptr) }
}

/// # Safety
///
/// The contract of C's `malloc`.
pub(crate) unsafe fn malloc(size: usize) -> *mut c_void {
    if !resolved() {
        return bootstrap::alloc(size, MALLOC_ALIGN);
    }
    // SAFETY: resolved, and `MALLOC` holds `malloc`.
    let f: unsafe extern "C" fn(usize) -> *mut c_void = unsafe { load(&MALLOC) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(size) })
}

/// # Safety
///
/// The contract of C's `calloc`.
pub(crate) unsafe fn calloc(number: usize, size: usize) -> *mut c_void {
    if !resolved() {
        // The arena starts zeroed and is never reused.
        return match number.checked_mul(size) {
            Some(total) => bootstrap::alloc(total, MALLOC_ALIGN),
            None => std::ptr::null_mut(),
        };
    }
    // SAFETY: resolved, and `CALLOC` holds `calloc`.
    let f: unsafe extern "C" fn(usize, usize) -> *mut c_void = unsafe { load(&CALLOC) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(number, size) })
}

/// # Safety
///
/// The contract of C's `realloc`.
pub(crate) unsafe fn realloc(ptr: *mut c_void, size: usize) -> *mut c_void {
    if bootstrap::contains(ptr) {
        // SAFETY: forwarded under the caller's contract.
        let new = unsafe { malloc(size) };
        if !new.is_null() {
            // SAFETY: `ptr` is an arena block of `bootstrap::size(ptr)`
            // bytes, `new` has room for `size`, and the two are distinct
            // allocations.
            unsafe {
                let len = bootstrap::size(ptr).min(size);
                std::ptr::copy_nonoverlapping(ptr.cast::<u8>(), new.cast::<u8>(), len);
            }
        }
        return new;
    }
    if !resolved() {
        if ptr.is_null() {
            return bootstrap::alloc(size, MALLOC_ALIGN);
        }
        // A pointer outside the arena came from the resolved `malloc`, so
        // resolution finished before it was handed out.
        unreachable!("a non-arena pointer before resolution");
    }
    // SAFETY: resolved, and `REALLOC` holds `realloc`.
    let f: unsafe extern "C" fn(*mut c_void, usize) -> *mut c_void = unsafe { load(&REALLOC) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(ptr, size) })
}

/// # Safety
///
/// The contract of C's `free`.
pub(crate) unsafe fn free(ptr: *mut c_void) {
    // Arena blocks are never reused. A pointer outside the arena implies
    // that resolution finished, see `realloc`.
    if ptr.is_null() || bootstrap::contains(ptr) || !resolved() {
        return;
    }
    // SAFETY: resolved, and `FREE` holds `free`.
    let f: unsafe extern "C" fn(*mut c_void) = unsafe { load(&FREE) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(ptr) })
}

/// # Safety
///
/// The contract of C's `posix_memalign`.
pub(crate) unsafe fn posix_memalign(out: *mut *mut c_void, alignment: usize, size: usize) -> c_int {
    if !resolved() {
        if !alignment.is_power_of_two() || alignment % std::mem::size_of::<*mut c_void>() != 0 {
            return libc::EINVAL;
        }
        let ptr = bootstrap::alloc(size, alignment);
        if ptr.is_null() {
            return libc::ENOMEM;
        }
        // SAFETY: the caller's contract makes `out` writable.
        unsafe { *out = ptr };
        return 0;
    }
    // SAFETY: resolved, and `POSIX_MEMALIGN` holds `posix_memalign`.
    let f: unsafe extern "C" fn(*mut *mut c_void, usize, usize) -> c_int =
        unsafe { load(&POSIX_MEMALIGN) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(out, alignment, size) })
}

/// # Safety
///
/// The contract of C's `aligned_alloc`.
pub(crate) unsafe fn aligned_alloc(alignment: usize, size: usize) -> *mut c_void {
    if !resolved() {
        if !alignment.is_power_of_two() {
            return std::ptr::null_mut();
        }
        return bootstrap::alloc(size, alignment);
    }
    // SAFETY: resolved, and `ALIGNED_ALLOC` holds `aligned_alloc`.
    let f: unsafe extern "C" fn(usize, usize) -> *mut c_void = unsafe { load(&ALIGNED_ALLOC) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(alignment, size) })
}

/// # Safety
///
/// The contract of glibc's `malloc_usable_size`.
pub(crate) unsafe fn malloc_usable_size(ptr: *mut c_void) -> usize {
    if bootstrap::contains(ptr) {
        // SAFETY: `ptr` is an arena block.
        return unsafe { bootstrap::size(ptr) };
    }
    if ptr.is_null() || !resolved() {
        return 0;
    }
    // SAFETY: resolved, and `MALLOC_USABLE_SIZE` holds `malloc_usable_size`.
    let f: unsafe extern "C" fn(*mut c_void) -> usize = unsafe { load(&MALLOC_USABLE_SIZE) };
    // SAFETY: the caller's contract.
    enter(|| unsafe { f(ptr) })
}

/// Allocates with the next `malloc` family, following the same rules as
/// `std::alloc::System` on glibc. Its allocations bypass this executable's
/// `malloc` definitions and so are not reported as C allocations.
#[derive(Debug)]
pub(crate) struct NextAlloc;

impl NextAlloc {
    /// # Safety
    ///
    /// `layout` has a non-zero size.
    unsafe fn aligned(layout: Layout) -> *mut u8 {
        let mut out = std::ptr::null_mut();
        let alignment = layout.align().max(std::mem::size_of::<*mut c_void>());
        // SAFETY: `out` is writable, and `alignment` is a power of two and
        // a multiple of the pointer size.
        let rc = unsafe { posix_memalign(&mut out, alignment, layout.size()) };
        if rc == 0 {
            out.cast()
        } else {
            std::ptr::null_mut()
        }
    }

    fn malloc_aligns(align: usize, size: usize) -> bool {
        align <= MALLOC_ALIGN && align <= size
    }
}

// SAFETY: every allocation comes from, and returns to, the next `malloc`
// family, which returns memory aligned as requested or null.
unsafe impl GlobalAlloc for NextAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if Self::malloc_aligns(layout.align(), layout.size()) {
            // SAFETY: `GlobalAlloc` callers pass a non-zero size.
            unsafe { malloc(layout.size()).cast() }
        } else {
            // SAFETY: as above.
            unsafe { Self::aligned(layout) }
        }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if Self::malloc_aligns(layout.align(), layout.size()) {
            // SAFETY: `GlobalAlloc` callers pass a non-zero size.
            unsafe { calloc(layout.size(), 1).cast() }
        } else {
            // SAFETY: as above.
            let ptr = unsafe { Self::aligned(layout) };
            if !ptr.is_null() {
                // SAFETY: `ptr` holds `layout.size()` bytes.
                unsafe { std::ptr::write_bytes(ptr, 0, layout.size()) };
            }
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, _layout: Layout) {
        // SAFETY: `ptr` came from this allocator.
        unsafe { free(ptr.cast()) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if Self::malloc_aligns(layout.align(), new_size) {
            // SAFETY: `ptr` came from this allocator, and `new_size` is
            // non-zero by the caller's contract.
            return unsafe { realloc(ptr.cast(), new_size).cast() };
        }
        // SAFETY: the caller's contract makes `new_size` a valid size for
        // `layout.align()`.
        let new_layout = unsafe { Layout::from_size_align_unchecked(new_size, layout.align()) };
        // SAFETY: `new_layout` has a non-zero size.
        let new = unsafe { self.alloc(new_layout) };
        if !new.is_null() {
            // SAFETY: both blocks hold at least the copied length, and they
            // are distinct allocations. `ptr` came from this allocator.
            unsafe {
                std::ptr::copy_nonoverlapping(ptr, new, layout.size().min(new_size));
                self.dealloc(ptr, layout);
            }
        }
        new
    }
}

/// Introspection hooks for the system allocator. The statistics are glibc's,
/// which miss the heap of a preloaded allocator that does not forward to it.
pub(crate) const SYSTEM_HOOKS: AllocatorHooks = AllocatorHooks {
    name: "system",
    stats,
    collect,
};

fn stats() -> String {
    // SAFETY: `mallinfo2` has no preconditions.
    let info = unsafe { libc::mallinfo2() };
    format!(
        "glibc malloc: {} bytes in arenas, {} bytes in mmaped chunks, {} bytes in use, {} bytes free\n",
        info.arena, info.hblkhd, info.uordblks, info.fordblks,
    )
}

fn collect() {
    // SAFETY: `malloc_trim` has no preconditions.
    unsafe { libc::malloc_trim(0) };
}

/// The arena that serves allocations while the functions are resolved.
mod bootstrap {
    use std::cell::UnsafeCell;
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::MALLOC_ALIGN;

    /// Observed `dlsym` allocations are a few hundred bytes.
    const CAPACITY: usize = 64 << 10;
    /// The arena's alignment, the largest block alignment it serves.
    const ALIGN: usize = 4096;
    /// Each block is preceded by its size.
    const HEADER: usize = std::mem::size_of::<usize>();

    #[repr(C, align(4096))]
    struct Arena(UnsafeCell<[u8; CAPACITY]>);

    // SAFETY: `alloc` hands out each byte range to exactly one caller.
    unsafe impl Sync for Arena {}

    static ARENA: Arena = Arena(UnsafeCell::new([0; CAPACITY]));
    static USED: AtomicUsize = AtomicUsize::new(0);

    fn base() -> *mut u8 {
        ARENA.0.get().cast()
    }

    /// A zeroed block of `size` bytes aligned to `align`, or null if the
    /// arena is exhausted or `align` exceeds its alignment.
    pub(super) fn alloc(size: usize, align: usize) -> *mut c_void {
        let align = align.max(MALLOC_ALIGN);
        if align > ALIGN {
            return std::ptr::null_mut();
        }
        let mut used = USED.load(Ordering::Relaxed);
        loop {
            let Some(start) = used
                .checked_add(HEADER)
                .and_then(|start| start.checked_next_multiple_of(align))
            else {
                return std::ptr::null_mut();
            };
            let Some(end) = start.checked_add(size).filter(|&end| end <= CAPACITY) else {
                return std::ptr::null_mut();
            };
            match USED.compare_exchange_weak(used, end, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => {
                    // SAFETY: `[start - HEADER, end)` lies within the arena
                    // and belongs to this call alone, and `start` is a
                    // multiple of `align` from a base aligned to `ALIGN`.
                    unsafe {
                        let block = base().add(start);
                        block.sub(HEADER).cast::<usize>().write_unaligned(size);
                        return block.cast();
                    }
                }
                Err(current) => used = current,
            }
        }
    }

    pub(super) fn contains(ptr: *mut c_void) -> bool {
        let base = base().addr();
        (base..base + CAPACITY).contains(&ptr.addr())
    }

    /// The size `ptr` was allocated with.
    ///
    /// # Safety
    ///
    /// `ptr` must have been returned by [`alloc`].
    pub(super) unsafe fn size(ptr: *mut c_void) -> usize {
        // SAFETY: `alloc` wrote the size just before the block.
        unsafe {
            ptr.cast::<u8>()
                .sub(HEADER)
                .cast::<usize>()
                .read_unaligned()
        }
    }
}
