// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! jq programs, run in a WebAssembly sandbox with bounded resources.
//!
//! Programs compile and run on jaq inside `mz-jq-guest`, which this crate's
//! build script compiles to WebAssembly and embeds. Each [`Program`] owns one
//! instance of that module, with its own linear memory, and every call into it
//! is bounded in instructions, wasm stack, and memory. Exceeding a bound is an
//! error for that call, never a host crash. All bounds are deterministic
//! except the stack, whose use per call depends on the host architecture.

use std::sync::{LazyLock, Mutex};

use wasmtime::{Config, Engine, Instance, Memory, Module, ResourceLimiter, Store, Trap, TypedFunc};

/// The most instructions a program may execute to compile.
const COMPILE_FUEL: u64 = 1_000_000_000;

/// The most instructions a program may execute per input value.
// TODO: Tune with evaluation benchmarks. At about 1.3 ns per unit, this
// allows about 1.3 s per value.
const EVAL_FUEL: u64 = 1_000_000_000;

/// The most host stack, in bytes, that wasm code may use per call.
///
/// This must stay well above the guest's own stack (`STACK_SIZE` in
/// `mz-jq-guest`'s build script, 256 KiB), so that deep programs exhaust the
/// guest's stack first. That limit is the same on every architecture, while
/// host stack use per wasm frame is not. Measured on aarch64, a call uses
/// under twice as much host stack as guest stack.
const MAX_WASM_STACK: usize = 1 << 20;

/// The most linear memory, in bytes, a program's instance may grow to.
const MAX_MEMORY: usize = 256 << 20;

/// Instances whose memory has grown past this many bytes are discarded after
/// a call, because wasm memory never shrinks.
const RETAIN_MEMORY: usize = 16 << 20;

/// Return codes of the guest's `compile` and `eval`, as `mz_jq_guest::status`
/// defines them. Any other code is an error.
mod status {
    pub const NONE: u32 = 0;
    pub const VALUE: u32 = 1;
}

static WASM: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/mz_jq_guest.wasm"));

/// The engine and compiled module. Compiling the module takes about two
/// seconds on first use in an optimized build.
static MODULE: LazyLock<(Engine, Module)> = LazyLock::new(|| {
    let mut config = Config::new();
    config
        .consume_fuel(true)
        .cranelift_nan_canonicalization(true)
        .max_wasm_stack(MAX_WASM_STACK)
        // Unused, as calls are synchronous, but must not be below the above.
        .async_stack_size(MAX_WASM_STACK);
    let engine = Engine::new(&config).expect("valid wasmtime config");
    let module = Module::new(&engine, WASM).expect("valid mz-jq-guest module");
    (engine, module)
});

/// A compiled jq program.
pub struct Program {
    source: String,
    /// `None` after a call left the instance unusable or too large. The next
    /// call instantiates and compiles again.
    guest: Mutex<Option<Guest>>,
}

impl Program {
    /// Compiles `source`, or describes why it does not compile.
    pub fn compile(source: &str) -> Result<Program, String> {
        let guest = Guest::new(source)?;
        Ok(Program {
            source: source.to_owned(),
            guest: Mutex::new(Some(guest)),
        })
    }

    /// Applies the program to the JSON text `input`.
    ///
    /// Returns the program's only output, as jq renders it, or `None` if it
    /// produced none. The rendering is not JSON for values like NaN or byte
    /// strings.
    pub fn eval(&self, input: &str) -> Result<Option<String>, String> {
        let mut guard = self.guest.lock().expect("lock poisoned");
        let guest = match &mut *guard {
            Some(guest) => guest,
            None => guard.insert(Guest::new(&self.source)?),
        };
        let eval = guest.eval.clone();
        let result = guest.call(eval, input.as_bytes(), EVAL_FUEL);
        let discard = match &result {
            Err(CallError::Trap(..)) => true,
            _ => guest.memory.data_size(&guest.store) > RETAIN_MEMORY,
        };
        if discard {
            *guard = None;
        }
        match result? {
            (status::NONE, _) => Ok(None),
            (status::VALUE, output) => Ok(Some(output)),
            (_, error) => Err(error),
        }
    }
}

/// An instance of the guest module with a compiled program.
struct Guest {
    store: Store<Limits>,
    memory: Memory,
    alloc: TypedFunc<u32, u32>,
    eval: TypedFunc<(u32, u32), u32>,
    output_ptr: TypedFunc<(), u32>,
    output_len: TypedFunc<(), u32>,
}

impl Guest {
    fn new(source: &str) -> Result<Guest, CallError> {
        let (engine, module) = &*MODULE;
        let mut store = Store::new(engine, Limits);
        store.limiter(|limits| limits);
        // Instantiation runs the module's initializers, which consume fuel.
        store.set_fuel(COMPILE_FUEL).map_err(CallError::trap)?;
        let instance = Instance::new(&mut store, module, &[]).map_err(CallError::trap)?;
        let compile = export(&instance, &mut store, "compile");
        let mut guest = Guest {
            memory: instance
                .get_memory(&mut store, "memory")
                .expect("mz-jq-guest memory"),
            alloc: export(&instance, &mut store, "alloc"),
            eval: export(&instance, &mut store, "eval"),
            output_ptr: export(&instance, &mut store, "output_ptr"),
            output_len: export(&instance, &mut store, "output_len"),
            store,
        };
        match guest.call(compile, source.as_bytes(), COMPILE_FUEL)? {
            (status::NONE, _) => Ok(guest),
            (_, error) => Err(CallError::Other(error)),
        }
    }

    /// Calls `func` with `arg` and at most `fuel` instructions, returning its
    /// status and output.
    fn call(
        &mut self,
        func: TypedFunc<(u32, u32), u32>,
        arg: &[u8],
        fuel: u64,
    ) -> Result<(u32, String), CallError> {
        // Wasm runs on the calling thread's stack, which must have room for
        // the wasm stack limit and wasmtime's own frames.
        stacker::maybe_grow(
            MAX_WASM_STACK + (256 << 10),
            MAX_WASM_STACK + (1 << 20),
            || {
                let len = u32::try_from(arg.len())
                    .map_err(|_| CallError::Other("argument too large".into()))?;
                self.store.set_fuel(fuel).map_err(CallError::trap)?;
                let ptr = self
                    .alloc
                    .call(&mut self.store, len)
                    .map_err(CallError::trap)?;
                self.memory
                    .write(&mut self.store, usize::try_from(ptr).unwrap(), arg)
                    .map_err(|e| CallError::Other(e.to_string()))?;
                let status = func
                    .call(&mut self.store, (ptr, len))
                    .map_err(CallError::trap)?;
                let ptr = self
                    .output_ptr
                    .call(&mut self.store, ())
                    .map_err(CallError::trap)?;
                let len = self
                    .output_len
                    .call(&mut self.store, ())
                    .map_err(CallError::trap)?;
                let mut output = vec![0; usize::try_from(len).unwrap()];
                self.memory
                    .read(&self.store, usize::try_from(ptr).unwrap(), &mut output)
                    .map_err(|e| CallError::Trap(None, e.to_string()))?;
                let output =
                    String::from_utf8(output).map_err(|e| CallError::Trap(None, e.to_string()))?;
                Ok((status, output))
            },
        )
    }
}

fn export<P, R>(instance: &Instance, store: &mut Store<Limits>, name: &str) -> TypedFunc<P, R>
where
    P: wasmtime::WasmParams,
    R: wasmtime::WasmResults,
{
    instance
        .get_typed_func(store, name)
        .expect("mz-jq-guest export")
}

enum CallError {
    /// The instance trapped, or its state can no longer be trusted.
    Trap(Option<Trap>, String),
    Other(String),
}

impl CallError {
    fn trap(e: wasmtime::Error) -> CallError {
        CallError::Trap(e.downcast_ref::<Trap>().copied(), trap_message(e))
    }
}

impl From<CallError> for String {
    fn from(e: CallError) -> String {
        match e {
            CallError::Trap(_, msg) | CallError::Other(msg) => msg,
        }
    }
}

fn trap_message(e: wasmtime::Error) -> String {
    if e.is::<MemoryLimitExceeded>() {
        return format!(
            "program exceeded its memory limit of {} MiB",
            MAX_MEMORY >> 20
        );
    }
    match e.downcast_ref::<Trap>() {
        Some(Trap::OutOfFuel) => "program exceeded its evaluation budget".into(),
        // NOTE: The guest's Rust stack sits below its data at the bottom of
        // linear memory, so overflowing it is an out-of-bounds access, not a
        // write over data. `jq_deep_programs_exhaust_guest_stack` checks that
        // deep programs land here and not on `Trap::StackOverflow`.
        Some(Trap::StackOverflow | Trap::MemoryOutOfBounds) => {
            "program exceeded its stack limit".into()
        }
        Some(trap) => format!("program failed: {trap}"),
        None => format!("program failed: {e}"),
    }
}

/// Fails `memory.grow` past [`MAX_MEMORY`] with a trap, so that the guest
/// never observes the failure.
struct Limits;

#[derive(Debug)]
struct MemoryLimitExceeded;

impl std::fmt::Display for MemoryLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("memory limit exceeded")
    }
}

impl std::error::Error for MemoryLimitExceeded {}

impl ResourceLimiter for Limits {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if desired > MAX_MEMORY {
            return Err(MemoryLimitExceeded.into());
        }
        Ok(true)
    }

    /// Allows only creating tables at their declared size. The guest's
    /// table holds its function pointers and never grows.
    fn table_growing(
        &mut self,
        current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(current == 0)
    }
}

#[cfg(test)]
mod tests;
