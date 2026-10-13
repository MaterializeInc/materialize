// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A deterministic implementation of WASI preview1.
//!
//! Guests get a process with no arguments, no environment, no files, a clock
//! frozen at the Unix epoch, and a fixed pseudorandom stream. stdout and
//! stderr are captured into a bounded buffer. Every function in
//! [`mz_wasm_udf_abi::wasi::FUNCTIONS`] is linked, so any module that passes
//! validation instantiates.
//!
//! Host functions take pointers and lengths from the guest. Each one checks
//! them against the guest's memory before touching anything, and works on
//! guest memory in place, so host allocation stays bounded by the memory
//! limit no matter what a guest passes. Host work is charged as fuel, see
//! [`charge`].

use std::fmt;
use std::ops::Range;

use itertools::Itertools;
use mz_ore::cast::CastFrom;
use wasmtime::{Caller, Extern, Linker, Memory, Trap};

use crate::call::StoreState;

/// How many bytes of stdout and stderr a call keeps, combined.
pub const OUTPUT_CAPACITY: usize = 4096;

const ESUCCESS: i32 = 0;
const EBADF: i32 = 8;
const EFAULT: i32 = 21;
const EINVAL: i32 = 28;
const ENOSYS: i32 = 52;
const ENOTSUP: i32 = 58;

const FILETYPE_CHARACTER_DEVICE: u8 = 2;
const EVENTTYPE_CLOCK: u8 = 0;

/// The error a guest raises by calling `proc_exit`.
#[derive(Debug)]
pub struct ProcExit(pub i32);

impl fmt::Display for ProcExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "guest exited with code {}", self.0)
    }
}

impl std::error::Error for ProcExit {}

/// The fixed-seed pseudorandom stream behind `random_get` (SplitMix64).
///
/// Its quality only matters for hash seeding. What matters is that every
/// instance produces the same sequence.
#[derive(Debug)]
pub struct DeterministicRng(u64);

impl Default for DeterministicRng {
    fn default() -> Self {
        DeterministicRng(0x4d61_7465_7269_616c)
    }
}

impl DeterministicRng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next_u64().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

fn memory(caller: &mut Caller<'_, StoreState>) -> Option<Memory> {
    match caller.get_export(mz_wasm_udf_abi::MEMORY_EXPORT) {
        Some(Extern::Memory(memory)) => Some(memory),
        _ => None,
    }
}

fn offset(ptr: i32) -> usize {
    // Guest pointers are unsigned 32-bit offsets.
    usize::try_from(ptr.cast_unsigned()).expect("u32 fits in usize")
}

fn write_bytes(caller: &mut Caller<'_, StoreState>, ptr: i32, bytes: &[u8]) -> Result<(), i32> {
    let memory = memory(caller).ok_or(EFAULT)?;
    memory.write(caller, offset(ptr), bytes).map_err(|_| EFAULT)
}

fn write_u32(caller: &mut Caller<'_, StoreState>, ptr: i32, value: u32) -> Result<(), i32> {
    write_bytes(caller, ptr, &value.to_le_bytes())
}

fn write_u64(caller: &mut Caller<'_, StoreState>, ptr: i32, value: u64) -> Result<(), i32> {
    write_bytes(caller, ptr, &value.to_le_bytes())
}

/// The `len` bytes of guest memory at `ptr`, if they are in bounds.
fn range(
    caller: &Caller<'_, StoreState>,
    memory: Memory,
    ptr: i32,
    len: usize,
) -> Option<Range<usize>> {
    let start = offset(ptr);
    let end = start.checked_add(len)?;
    (end <= memory.data_size(caller)).then_some(start..end)
}

/// Charges one unit of fuel per byte of guest memory a host function
/// processes. Without it, host loops would be CPU time that the fuel limit
/// does not see. Running out raises the same trap as guest code running out.
fn charge(caller: &mut Caller<'_, StoreState>, bytes: usize) -> wasmtime::Result<()> {
    let cost = u64::cast_from(bytes);
    let fuel = caller.get_fuel()?;
    let Some(remaining) = fuel.checked_sub(cost) else {
        caller.set_fuel(0)?;
        return Err(Trap::OutOfFuel.into());
    };
    caller.set_fuel(remaining)
}

fn errno(result: Result<(), i32>) -> i32 {
    match result {
        Ok(()) => ESUCCESS,
        Err(e) => e,
    }
}

/// Reports zero arguments or environment variables.
fn empty_sizes(caller: &mut Caller<'_, StoreState>, count: i32, size: i32) -> i32 {
    errno(write_u32(caller, count, 0).and_then(|()| write_u32(caller, size, 0)))
}

/// Captures writes to stdout and stderr. Bytes past [`OUTPUT_CAPACITY`] are
/// reported as written but neither copied nor charged.
fn fd_write(
    caller: &mut Caller<'_, StoreState>,
    fd: i32,
    iovs: i32,
    iovs_len: i32,
    nwritten: i32,
) -> wasmtime::Result<i32> {
    const IOVEC_SIZE: usize = 8;
    if fd != 1 && fd != 2 {
        return Ok(EBADF);
    }
    let Some(memory) = memory(caller) else {
        return Ok(EFAULT);
    };
    let Ok(iovs_len) = usize::try_from(iovs_len) else {
        return Ok(EINVAL);
    };
    let Some(iovecs) = iovs_len
        .checked_mul(IOVEC_SIZE)
        .and_then(|len| range(caller, memory, iovs, len))
    else {
        return Ok(EFAULT);
    };
    charge(caller, iovecs.len())?;
    let mut written: u32 = 0;
    for at in iovecs.step_by(IOVEC_SIZE) {
        let data = memory.data(&*caller);
        let ptr = i32::from_le_bytes(data[at..at + 4].try_into().expect("4 bytes"));
        let len = u32::from_le_bytes(data[at + 4..at + 8].try_into().expect("4 bytes"));
        let Some(bytes) = range(caller, memory, ptr, usize::cast_from(len)) else {
            return Ok(EFAULT);
        };
        let room = OUTPUT_CAPACITY.saturating_sub(caller.data().output_len());
        let captured = bytes.start..bytes.start + bytes.len().min(room);
        charge(caller, captured.len())?;
        let (data, state) = memory.data_and_store_mut(&mut *caller);
        state.capture_output(&data[captured]);
        written = written.saturating_add(len);
    }
    Ok(errno(write_u32(caller, nwritten, written)))
}

/// Answers every subscription immediately: clock subscriptions as elapsed,
/// file descriptor subscriptions with `EBADF`.
fn poll_oneoff(
    caller: &mut Caller<'_, StoreState>,
    subs: i32,
    events: i32,
    nsubs: i32,
    nevents: i32,
) -> wasmtime::Result<i32> {
    const SUBSCRIPTION_SIZE: usize = 48;
    const EVENT_SIZE: usize = 32;
    let Some(memory) = memory(caller) else {
        return Ok(EFAULT);
    };
    let Ok(n) = u32::try_from(nsubs) else {
        return Ok(EINVAL);
    };
    let n_usize = usize::cast_from(n);
    let regions = (
        n_usize
            .checked_mul(SUBSCRIPTION_SIZE)
            .and_then(|len| range(caller, memory, subs, len)),
        n_usize
            .checked_mul(EVENT_SIZE)
            .and_then(|len| range(caller, memory, events, len)),
    );
    let (Some(subs), Some(events)) = regions else {
        return Ok(EFAULT);
    };
    charge(caller, subs.len() + events.len())?;
    let data = memory.data_mut(&mut *caller);
    for (sub, at) in subs
        .step_by(SUBSCRIPTION_SIZE)
        .zip_eq(events.step_by(EVENT_SIZE))
    {
        let mut event = [0u8; EVENT_SIZE];
        event[0..8].copy_from_slice(&data[sub..sub + 8]);
        let tag = data[sub + 8];
        let error: u16 = if tag == EVENTTYPE_CLOCK {
            0
        } else {
            u16::try_from(EBADF).expect("errno fits in u16")
        };
        event[8..10].copy_from_slice(&error.to_le_bytes());
        event[10] = tag;
        data[at..at + EVENT_SIZE].copy_from_slice(&event);
    }
    Ok(errno(write_u32(caller, nevents, n)))
}

/// Fills guest memory from the deterministic stream.
fn random_get(caller: &mut Caller<'_, StoreState>, buf: i32, len: i32) -> wasmtime::Result<i32> {
    let Some(memory) = memory(caller) else {
        return Ok(EFAULT);
    };
    let Ok(len) = usize::try_from(len) else {
        return Ok(EINVAL);
    };
    let Some(bytes) = range(caller, memory, buf, len) else {
        return Ok(EFAULT);
    };
    charge(caller, bytes.len())?;
    let (data, state) = memory.data_and_store_mut(&mut *caller);
    state.rng.fill(&mut data[bytes]);
    Ok(ESUCCESS)
}

/// Adds every WASI preview1 function to `linker`.
pub fn add_to_linker(linker: &mut Linker<StoreState>) -> wasmtime::Result<()> {
    use mz_wasm_udf_abi::wasi::MODULE as M;

    linker.func_wrap(
        M,
        "args_get",
        |_: Caller<'_, StoreState>, _: i32, _: i32| ESUCCESS,
    )?;
    linker.func_wrap(
        M,
        "args_sizes_get",
        |mut c: Caller<'_, StoreState>, count: i32, size: i32| empty_sizes(&mut c, count, size),
    )?;
    linker.func_wrap(
        M,
        "environ_get",
        |_: Caller<'_, StoreState>, _: i32, _: i32| ESUCCESS,
    )?;
    linker.func_wrap(
        M,
        "environ_sizes_get",
        |mut c: Caller<'_, StoreState>, count: i32, size: i32| empty_sizes(&mut c, count, size),
    )?;
    linker.func_wrap(
        M,
        "clock_res_get",
        |mut c: Caller<'_, StoreState>, _id: i32, out: i32| errno(write_u64(&mut c, out, 1_000)),
    )?;
    linker.func_wrap(
        M,
        "clock_time_get",
        |mut c: Caller<'_, StoreState>, _id: i32, _precision: i64, out: i32| {
            errno(write_u64(&mut c, out, 0))
        },
    )?;
    linker.func_wrap(
        M,
        "fd_advise",
        |_: Caller<'_, StoreState>, _: i32, _: i64, _: i64, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_allocate",
        |_: Caller<'_, StoreState>, _: i32, _: i64, _: i64| EBADF,
    )?;
    linker.func_wrap(M, "fd_close", |_: Caller<'_, StoreState>, _: i32| EBADF)?;
    linker.func_wrap(M, "fd_datasync", |_: Caller<'_, StoreState>, _: i32| EBADF)?;
    linker.func_wrap(
        M,
        "fd_fdstat_get",
        |mut c: Caller<'_, StoreState>, fd: i32, out: i32| {
            if !(0..=2).contains(&fd) {
                return EBADF;
            }
            // filetype, padding, flags, padding, rights_base, rights_inheriting.
            let mut stat = [0u8; 24];
            stat[0] = FILETYPE_CHARACTER_DEVICE;
            errno(write_bytes(&mut c, out, &stat))
        },
    )?;
    linker.func_wrap(
        M,
        "fd_fdstat_set_flags",
        |_: Caller<'_, StoreState>, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_fdstat_set_rights",
        |_: Caller<'_, StoreState>, _: i32, _: i64, _: i64| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_filestat_get",
        |_: Caller<'_, StoreState>, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_filestat_set_size",
        |_: Caller<'_, StoreState>, _: i32, _: i64| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_filestat_set_times",
        |_: Caller<'_, StoreState>, _: i32, _: i64, _: i64, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_pread",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i64, _: i32| EBADF,
    )?;
    // `EBADF` from `fd_prestat_get` tells the guest there are no preopened
    // directories.
    linker.func_wrap(
        M,
        "fd_prestat_get",
        |_: Caller<'_, StoreState>, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_prestat_dir_name",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_pwrite",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i64, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_read",
        |mut c: Caller<'_, StoreState>, fd: i32, _iovs: i32, _iovs_len: i32, nread: i32| {
            // stdin is empty.
            if fd != 0 {
                return EBADF;
            }
            errno(write_u32(&mut c, nread, 0))
        },
    )?;
    linker.func_wrap(
        M,
        "fd_readdir",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i64, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_renumber",
        |_: Caller<'_, StoreState>, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "fd_seek",
        |_: Caller<'_, StoreState>, _: i32, _: i64, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(M, "fd_sync", |_: Caller<'_, StoreState>, _: i32| EBADF)?;
    linker.func_wrap(M, "fd_tell", |_: Caller<'_, StoreState>, _: i32, _: i32| {
        EBADF
    })?;
    linker.func_wrap(
        M,
        "fd_write",
        |mut c: Caller<'_, StoreState>, fd: i32, iovs: i32, iovs_len: i32, nwritten: i32| {
            fd_write(&mut c, fd, iovs, iovs_len, nwritten)
        },
    )?;
    linker.func_wrap(
        M,
        "path_create_directory",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_filestat_get",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_filestat_set_times",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i64, _: i64, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_link",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_open",
        |_: Caller<'_, StoreState>,
         _: i32,
         _: i32,
         _: i32,
         _: i32,
         _: i32,
         _: i64,
         _: i64,
         _: i32,
         _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_readlink",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_remove_directory",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_rename",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_symlink",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "path_unlink_file",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32| EBADF,
    )?;
    linker.func_wrap(
        M,
        "poll_oneoff",
        |mut c: Caller<'_, StoreState>, subs: i32, events: i32, nsubs: i32, nevents: i32| {
            poll_oneoff(&mut c, subs, events, nsubs, nevents)
        },
    )?;
    linker.func_wrap(
        M,
        "proc_exit",
        |_: Caller<'_, StoreState>, code: i32| -> wasmtime::Result<()> {
            Err(wasmtime::Error::new(ProcExit(code)))
        },
    )?;
    linker.func_wrap(M, "proc_raise", |_: Caller<'_, StoreState>, _: i32| ENOSYS)?;
    linker.func_wrap(M, "sched_yield", |_: Caller<'_, StoreState>| ESUCCESS)?;
    linker.func_wrap(
        M,
        "random_get",
        |mut c: Caller<'_, StoreState>, buf: i32, len: i32| random_get(&mut c, buf, len),
    )?;
    linker.func_wrap(
        M,
        "sock_accept",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32| ENOTSUP,
    )?;
    linker.func_wrap(
        M,
        "sock_recv",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32, _: i32| ENOTSUP,
    )?;
    linker.func_wrap(
        M,
        "sock_send",
        |_: Caller<'_, StoreState>, _: i32, _: i32, _: i32, _: i32, _: i32| ENOTSUP,
    )?;
    linker.func_wrap(
        M,
        "sock_shutdown",
        |_: Caller<'_, StoreState>, _: i32, _: i32| ENOTSUP,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use wasmtime::ExternType;

    use super::*;
    use crate::engine::ENGINE;

    #[mz_ore::test]
    fn links_exactly_the_abi_function_list() {
        let mut linker = Linker::new(&ENGINE);
        add_to_linker(&mut linker).unwrap();
        let mut store = wasmtime::Store::new(&ENGINE, StoreState::new(0));
        let mut linked: Vec<_> = linker
            .iter(&mut store)
            .map(|(module, name, item)| {
                assert_eq!(module, mz_wasm_udf_abi::wasi::MODULE);
                (name.to_string(), item)
            })
            .collect();
        linked.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected: Vec<_> = mz_wasm_udf_abi::wasi::FUNCTIONS.to_vec();
        expected.sort_by_key(|(name, _, _)| *name);
        assert_eq!(linked.len(), expected.len());

        for ((name, item), (expected_name, params, results)) in linked.iter().zip_eq(expected) {
            assert_eq!(name, expected_name);
            let ExternType::Func(ty) = item.ty(&mut store) else {
                panic!("{name} is not a function");
            };
            let convert = |t: &mz_wasm_udf_abi::wasi::ValType| match t {
                mz_wasm_udf_abi::wasi::ValType::I32 => "i32",
                mz_wasm_udf_abi::wasi::ValType::I64 => "i64",
            };
            let actual_params: Vec<String> = ty.params().map(|p| p.to_string()).collect();
            let actual_results: Vec<String> = ty.results().map(|r| r.to_string()).collect();
            assert_eq!(
                actual_params,
                params.iter().map(convert).collect::<Vec<_>>(),
                "{name} params"
            );
            assert_eq!(
                actual_results,
                results.iter().map(convert).collect::<Vec<_>>(),
                "{name} results"
            );
        }
    }

    #[mz_ore::test]
    fn rng_is_deterministic() {
        let mut a = DeterministicRng::default();
        let mut b = DeterministicRng::default();
        let (mut x, mut y) = ([0u8; 13], [0u8; 13]);
        a.fill(&mut x);
        b.fill(&mut y);
        assert_eq!(x, y);
    }
}
