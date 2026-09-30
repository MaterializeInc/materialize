// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The wasmtime engine configuration.
//!
//! Every process that evaluates WebAssembly functions uses this one
//! configuration, so a module produces the same results and errors in a
//! cluster replica as in the `mz` CLI.

use std::num::NonZeroUsize;
use std::sync::LazyLock;

use wasmtime::{Config, Engine, WasmBacktraceDetails};

/// The maximum native stack a guest call may use. Stack overflow traps at a
/// depth that depends only on the module and the compiler version, so it is
/// deterministic across replicas of one cluster.
pub const MAX_WASM_STACK: usize = 512 << 10;

/// How many frames a trap's backtrace, and so its error message, includes.
pub const MAX_BACKTRACE_FRAMES: usize = 16;

/// The process-wide engine.
pub static ENGINE: LazyLock<Engine> =
    LazyLock::new(|| Engine::new(&config()).expect("valid engine configuration"));

/// The engine configuration. It enables exactly the Wasm features that
/// `mz_wasm_udf_abi::ModuleInfo::parse` accepts, and closes every source of
/// nondeterminism in the remaining feature set.
fn config() -> Config {
    let mut config = Config::new();
    config
        // Fuel is the only execution bound. It is deterministic, unlike epoch
        // interruption, so exhausting it can be an ordinary query error.
        .consume_fuel(true)
        .epoch_interruption(false)
        // NaN bit patterns are otherwise platform-dependent.
        .cranelift_nan_canonicalization(true)
        // Threads and exceptions are also off: this crate does not enable the
        // wasmtime `threads` and `gc` features that implement them.
        .wasm_relaxed_simd(false)
        .wasm_shared_everything_threads(false)
        .wasm_memory64(false)
        .wasm_gc(false)
        .wasm_stack_switching(false)
        .wasm_custom_page_sizes(false)
        .max_wasm_stack(MAX_WASM_STACK)
        // Each batch instantiates the module afresh, which copy-on-write
        // memory images keep cheap.
        .memory_init_cow(true)
        .wasm_backtrace_max_frames(NonZeroUsize::new(MAX_BACKTRACE_FRAMES))
        // Backtraces name functions from the module's name section and cite
        // code offsets, both properties of the binary. DWARF symbolication is
        // left to the CLI.
        .wasm_backtrace_details(WasmBacktraceDetails::Disable);
    config
}
