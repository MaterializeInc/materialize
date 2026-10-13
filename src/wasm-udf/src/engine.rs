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

/// The engine configuration. It accepts every module that
/// `mz_wasm_udf_abi::ModuleInfo::parse` accepts, and closes every source of
/// nondeterminism in that feature set.
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
        .wasm_multi_memory(false)
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
    // Generated code depends on the CPU features the compiler may use, and
    // with it the frame sizes that decide how deep a guest can recurse before
    // its stack overflows. Naming the target turns off inference from the
    // host, so every replica compiles for the same fixed baseline, whatever
    // hardware it runs on.
    if let Some(target) = HOST_TARGET {
        config.target(target).expect("the host target is supported");
        for flag in BASELINE_FLAGS {
            // SAFETY: the engine checks at creation that the host supports
            // each enabled feature, and refuses to start otherwise.
            unsafe {
                config.cranelift_flag_enable(flag);
            }
        }
    }
    config
}

/// The target triple of the host, for the platforms replicas run on.
const HOST_TARGET: Option<&str> = if cfg!(all(target_arch = "x86_64", target_os = "linux")) {
    Some("x86_64-unknown-linux-gnu")
} else if cfg!(all(target_arch = "aarch64", target_os = "linux")) {
    Some("aarch64-unknown-linux-gnu")
} else if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
    Some("aarch64-apple-darwin")
} else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
    Some("x86_64-apple-darwin")
} else {
    None
};

/// The CPU features compiled code may use beyond the target's defaults: on
/// x86-64 the x86-64-v2 level, which WebAssembly SIMD needs and every x86 CPU
/// a replica runs on provides. aarch64 targets need none beyond their
/// defaults.
const BASELINE_FLAGS: &[&str] = if cfg!(target_arch = "x86_64") {
    &[
        "has_sse3",
        "has_ssse3",
        "has_sse41",
        "has_sse42",
        "has_popcnt",
    ]
} else {
    &[]
};

#[cfg(test)]
mod tests {
    use wasmparser::Validator;
    use wasmtime::Module;

    use super::*;

    /// A module whose only content is one table of `ref_type` with one
    /// element.
    fn table_module(ref_type: u8) -> Vec<u8> {
        let mut bytes = b"\0asm\x01\0\0\0".to_vec();
        // Table section: 4 bytes, one table, element type, limits min 1.
        bytes.extend_from_slice(&[0x04, 0x04, 0x01, ref_type, 0x00, 0x01]);
        bytes
    }

    fn validator_accepts(bytes: &[u8]) -> bool {
        Validator::new_with_features(mz_wasm_udf_abi::features())
            .validate_all(bytes)
            .is_ok()
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    fn engine_accepts_what_validation_accepts() {
        const FUNCREF: u8 = 0x70;
        const EXTERNREF: u8 = 0x6f;
        let funcref = table_module(FUNCREF);
        assert!(validator_accepts(&funcref));
        assert!(Module::new(&ENGINE, &funcref).is_ok());

        // The engine is built without GC support, so validation must reject
        // `externref` rather than let a module through that no replica can
        // compile.
        let externref = table_module(EXTERNREF);
        assert!(Module::new(&ENGINE, &externref).is_err());
        assert!(!validator_accepts(&externref));
    }

    /// Bulk memory operations must cost fuel in proportion to their length,
    /// or a guest could loop `memory.fill` over its whole memory for one unit
    /// of fuel per iteration.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    fn bulk_memory_costs_fuel_per_byte() {
        // (module (memory 1) (func (export "fill")
        //   (memory.fill (i32.const 0) (i32.const 0) (i32.const 65536))))
        let bytes: &[u8] = &[
            0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // header
            0x01, 0x04, 0x01, 0x60, 0x00, 0x00, // type: () -> ()
            0x03, 0x02, 0x01, 0x00, // func 0 has type 0
            0x05, 0x03, 0x01, 0x00, 0x01, // memory min 1 page
            0x07, 0x08, 0x01, 0x04, b'f', b'i', b'l', b'l', 0x00, 0x00, // export
            0x0a, 0x0f, 0x01, 0x0d, 0x00, // code: 1 body, 13 bytes, no locals
            0x41, 0x00, 0x41, 0x00, // i32.const 0, i32.const 0
            0x41, 0x80, 0x80, 0x04, // i32.const 65536
            0xfc, 0x0b, 0x00, // memory.fill 0
            0x0b, // end
        ];
        let module = Module::new(&ENGINE, bytes).expect("valid module");
        let mut store = wasmtime::Store::new(&ENGINE, ());
        store.set_fuel(1 << 30).expect("fuel is enabled");
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).expect("instantiates");
        let fill = instance
            .get_typed_func::<(), ()>(&mut store, "fill")
            .expect("export");
        fill.call(&mut store, ()).expect("fills");
        let consumed = (1 << 30) - store.get_fuel().expect("fuel is enabled");
        assert!(
            consumed >= 65536,
            "memory.fill of 64 KiB cost {consumed} fuel"
        );
    }
}
