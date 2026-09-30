// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compiled modules and single guest calls.

use std::fmt;

use mz_expr::func::{WasmErrorKind, WasmLimits};
use mz_ore::cast::CastFrom;
use mz_wasm_udf_abi::{ALLOC_EXPORT, AbiError, INITIALIZE_EXPORT, MEMORY_EXPORT, ModuleInfo};
use wasmtime::{InstancePre, Linker, Module, ResourceLimiter, Store, Trap, WasmBacktrace};

use crate::engine::{ENGINE, MAX_BACKTRACE_FRAMES};
use crate::wasi::{self, DeterministicRng, OUTPUT_CAPACITY, ProcExit};

/// The upper bound on table growth. Growth beyond it fails with -1, which
/// the guest observes deterministically.
const MAX_TABLE_ELEMENTS: usize = 1 << 20;

/// Per-store state: resource accounting and the WASI context.
pub struct StoreState {
    limiter: Limiter,
    pub(crate) rng: DeterministicRng,
    output: Vec<u8>,
}

impl StoreState {
    pub(crate) fn new(memory_limit: usize) -> Self {
        StoreState {
            limiter: Limiter {
                limit: memory_limit,
                peak: 0,
            },
            rng: DeterministicRng::default(),
            output: Vec::new(),
        }
    }

    /// Records guest stdout or stderr output, up to [`OUTPUT_CAPACITY`].
    pub(crate) fn capture_output(&mut self, bytes: &[u8]) {
        let room = OUTPUT_CAPACITY.saturating_sub(self.output.len());
        self.output
            .extend_from_slice(&bytes[..bytes.len().min(room)]);
    }
}

struct Limiter {
    limit: usize,
    peak: usize,
}

/// The error a memory growth beyond the limit raises.
#[derive(Debug)]
struct MemoryLimitExceeded;

impl fmt::Display for MemoryLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("memory limit exceeded")
    }
}

impl std::error::Error for MemoryLimitExceeded {}

impl ResourceLimiter for Limiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // Failing the call, rather than returning -1 from `memory.grow`,
        // makes the limit a call failure that bisection can attribute to
        // rows, instead of an allocation failure each guest handles its own
        // way.
        if desired > self.limit {
            return Err(wasmtime::Error::new(MemoryLimitExceeded));
        }
        self.peak = self.peak.max(desired);
        Ok(true)
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(desired <= MAX_TABLE_ELEMENTS)
    }
}

/// A validated, compiled guest module.
pub struct CompiledModule {
    info: ModuleInfo,
    pre: InstancePre<StoreState>,
}

impl fmt::Debug for CompiledModule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompiledModule")
            .field("info", &self.info)
            .finish_non_exhaustive()
    }
}

/// A module that could not be prepared for execution.
#[derive(Debug, thiserror::Error)]
pub enum CompileError {
    #[error(transparent)]
    Abi(#[from] AbiError),
    #[error("failed to compile module: {0}")]
    Compile(String),
}

impl CompiledModule {
    /// Validates and compiles `bytes`.
    pub fn compile(bytes: &[u8]) -> Result<Self, CompileError> {
        let info = ModuleInfo::parse(bytes)?;
        let module =
            Module::new(&ENGINE, bytes).map_err(|e| CompileError::Compile(format!("{e:#}")))?;
        let mut linker = Linker::new(&ENGINE);
        wasi::add_to_linker(&mut linker).map_err(|e| CompileError::Compile(format!("{e:#}")))?;
        let pre = linker
            .instantiate_pre(&module)
            .map_err(|e| CompileError::Compile(format!("{e:#}")))?;
        Ok(CompiledModule { info, pre })
    }

    pub fn info(&self) -> &ModuleInfo {
        &self.info
    }

    /// Calls `export` once, in a fresh instance, with an Arrow IPC file as
    /// input.
    pub fn call(&self, export: &str, input: &[u8], limits: WasmLimits) -> CallReport {
        let memory_limit = usize::try_from(limits.memory_bytes).unwrap_or(usize::MAX);
        let mut store = Store::new(&ENGINE, StoreState::new(memory_limit));
        store.limiter(|state| &mut state.limiter);
        store
            .set_fuel(limits.fuel)
            .expect("the engine enables fuel");

        let result = self.call_in(&mut store, export, input);
        let fuel_consumed = limits.fuel - store.get_fuel().expect("the engine enables fuel");
        let state = store.into_data();
        CallReport {
            result: result.map_err(|e| CallError::classify(&e)).and_then(|r| r),
            fuel_consumed,
            peak_memory: state.limiter.peak,
            output: state.output,
        }
    }

    fn call_in(
        &self,
        store: &mut Store<StoreState>,
        export: &str,
        input: &[u8],
    ) -> wasmtime::Result<Result<Vec<u8>, CallError>> {
        let instance = self.pre.instantiate(&mut *store)?;
        if instance.get_func(&mut *store, INITIALIZE_EXPORT).is_some() {
            instance
                .get_typed_func::<(), ()>(&mut *store, INITIALIZE_EXPORT)?
                .call(&mut *store, ())?;
        }
        let alloc = instance.get_typed_func::<(u32, u32), u32>(&mut *store, ALLOC_EXPORT)?;
        let func = instance.get_typed_func::<(u32, u32, u32), i32>(&mut *store, export)?;
        let memory = instance
            .get_memory(&mut *store, MEMORY_EXPORT)
            .expect("validated export");

        let protocol = |msg: &str| Ok(Err(CallError::Protocol(msg.to_string())));
        let Some(len) = u32::try_from(input.len()).ok() else {
            return protocol("input batch exceeds 4 GiB");
        };
        let Some(alloc_len) = len.checked_add(8) else {
            return protocol("input batch exceeds 4 GiB");
        };
        let ptr = alloc.call(&mut *store, (alloc_len, 4))?;
        if ptr == 0 {
            return protocol("guest failed to allocate the input buffer");
        }
        let in_ptr = ptr + 8;
        memory.write(&mut *store, usize::cast_from(in_ptr), input)?;

        let code = func.call(&mut *store, (in_ptr, len, ptr))?;

        // Output buffers are never freed: the instance is discarded after
        // this call.
        let data = memory.data(&*store);
        let read_u32 = |at: u32| -> Option<u32> {
            let at = usize::cast_from(at);
            Some(u32::from_le_bytes(data.get(at..at + 4)?.try_into().ok()?))
        };
        let (Some(out_ptr), Some(out_len)) = (read_u32(ptr), read_u32(ptr + 4)) else {
            return protocol("output slice out of bounds");
        };
        let (start, end) = (
            usize::cast_from(out_ptr),
            usize::cast_from(out_ptr) + usize::cast_from(out_len),
        );
        let Some(out) = data.get(start..end) else {
            return protocol("output slice out of bounds");
        };
        Ok(match code {
            0 => Ok(out.to_vec()),
            _ => Err(CallError::Failed(String::from_utf8_lossy(out).into_owned())),
        })
    }
}

/// The outcome of one guest call, with its resource usage.
#[derive(Debug)]
pub struct CallReport {
    /// The output Arrow IPC file, or why the call failed.
    pub result: Result<Vec<u8>, CallError>,
    pub fuel_consumed: u64,
    /// The largest linear memory size the guest grew to, in bytes.
    pub peak_memory: usize,
    /// Captured stdout and stderr, truncated to [`OUTPUT_CAPACITY`].
    pub output: Vec<u8>,
}

/// Why a whole guest call failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CallError {
    Trap(String),
    OutOfFuel,
    MemoryLimit,
    /// The guest returned -1 with a message.
    Failed(String),
    /// The guest's input or output did not follow the ABI.
    Protocol(String),
}

impl CallError {
    fn classify(error: &wasmtime::Error) -> CallError {
        if error.downcast_ref::<MemoryLimitExceeded>().is_some() {
            return CallError::MemoryLimit;
        }
        if let Some(exit) = error.downcast_ref::<ProcExit>() {
            return CallError::Trap(exit.to_string());
        }
        let Some(trap) = error.downcast_ref::<Trap>() else {
            return CallError::Trap(format!("{error:#}"));
        };
        if *trap == Trap::OutOfFuel {
            return CallError::OutOfFuel;
        }
        let mut message = trap.to_string();
        if let Some(backtrace) = error.downcast_ref::<WasmBacktrace>() {
            for frame in backtrace.frames().iter().take(MAX_BACKTRACE_FRAMES) {
                let name = frame
                    .func_name()
                    .map(|n| n.to_string())
                    .unwrap_or_else(|| format!("<function {}>", frame.func_index()));
                match frame.func_offset() {
                    Some(offset) => message.push_str(&format!("\n    at {name} (+{offset:#x})")),
                    None => message.push_str(&format!("\n    at {name}")),
                }
            }
        }
        CallError::Trap(message)
    }

    pub fn kind(&self) -> WasmErrorKind {
        match self {
            CallError::Trap(_) => WasmErrorKind::Trap,
            CallError::OutOfFuel => WasmErrorKind::OutOfFuel,
            CallError::MemoryLimit => WasmErrorKind::MemoryLimit,
            CallError::Failed(_) | CallError::Protocol(_) => WasmErrorKind::CallFailed,
        }
    }

    /// A deterministic description of the failure. `output` is the guest's
    /// captured output, which for a panicking Rust guest holds the panic
    /// message.
    pub fn message(&self, output: &[u8]) -> String {
        let mut message = match self {
            CallError::Trap(m) | CallError::Failed(m) | CallError::Protocol(m) => m.clone(),
            CallError::OutOfFuel => "the call exhausted its fuel".into(),
            CallError::MemoryLimit => "the guest exceeded its memory limit".into(),
        };
        if matches!(self, CallError::Trap(_)) && !output.is_empty() {
            let output = String::from_utf8_lossy(output);
            message = format!("{}\n{message}", output.trim_end());
        }
        message
    }
}
