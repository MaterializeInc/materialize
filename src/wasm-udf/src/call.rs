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
use wasmtime::{
    CallHook, InstancePre, Linker, Module, ResourceLimiter, Store, StoreContextMut, Trap,
    WasmBacktrace,
};

use crate::engine::{ENGINE, MAX_BACKTRACE_FRAMES, MAX_WASM_STACK};
use crate::wasi::{self, DeterministicRng, OUTPUT_CAPACITY, ProcExit};

/// The upper bound on the elements of all of an instance's tables combined.
const MAX_TABLE_ELEMENTS: usize = 1 << 20;

/// Native stack a guest call keeps free beyond [`MAX_WASM_STACK`], for the
/// host frames of wasmtime, WASI calls, and trap handling. Wasmtime bounds
/// guest stack use relative to the stack pointer at entry, so this headroom
/// is what keeps deep guest recursion a trap rather than a native overflow.
const HOST_STACK_HEADROOM: usize = 256 << 10;

/// The size of the stack segment a guest call switches to when the current
/// thread lacks [`MAX_WASM_STACK`] plus [`HOST_STACK_HEADROOM`].
const CALL_STACK_SIZE: usize = MAX_WASM_STACK + (1 << 20);

/// The largest message a failed call keeps from the guest. Messages become
/// persisted errors, so a guest must not be able to make them large.
pub(crate) const MAX_MESSAGE_BYTES: usize = 4 << 10;

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
                memory: 0,
                peak: 0,
                table_elements: 0,
            },
            rng: DeterministicRng::default(),
            output: Vec::new(),
        }
    }

    pub(crate) fn output_len(&self) -> usize {
        self.output.len()
    }

    /// Records guest stdout or stderr output, up to [`OUTPUT_CAPACITY`].
    pub(crate) fn capture_output(&mut self, bytes: &[u8]) {
        let room = OUTPUT_CAPACITY.saturating_sub(self.output.len());
        self.output
            .extend_from_slice(&bytes[..bytes.len().min(room)]);
    }
}

/// Bounds an instance's linear memories and tables, in aggregate.
///
/// Wasmtime calls the `*_growing` hooks both when an instance creates a
/// memory or table and when the guest grows one, so the running totals cover
/// initial sizes too.
///
/// Every refusal here depends only on the module and the limits, so the
/// guest observes the same outcome on every replica. A growth the hook
/// approves can still fail if the host cannot allocate. That is not a
/// property of the module, so the `*_grow_failed` hooks raise
/// [`HostAllocationFailed`], which halts the process rather than letting the
/// guest see `-1` and compute a replica-dependent result.
struct Limiter {
    /// The limit on all linear memory combined, in bytes.
    limit: usize,
    memory: usize,
    peak: usize,
    table_elements: usize,
}

/// The host failed to allocate memory the limiter had approved.
#[derive(Debug)]
struct HostAllocationFailed(String);

impl fmt::Display for HostAllocationFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "host allocation failed: {}", self.0)
    }
}

impl std::error::Error for HostAllocationFailed {}

/// The error a memory growth beyond the limit raises.
#[derive(Debug)]
struct MemoryLimitExceeded;

impl fmt::Display for MemoryLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("memory limit exceeded")
    }
}

impl std::error::Error for MemoryLimitExceeded {}

/// The error a table growth beyond [`MAX_TABLE_ELEMENTS`] raises. Like
/// [`MemoryLimitExceeded`], it fails the call rather than returning -1, so
/// that a refusal while instantiating carries a type `CallError::classify`
/// recognizes as the guest's doing.
#[derive(Debug)]
struct TableLimitExceeded;

impl fmt::Display for TableLimitExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("table limit exceeded")
    }
}

impl std::error::Error for TableLimitExceeded {}

impl ResourceLimiter for Limiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        // The module's own declared maximum is a property of the binary, so
        // refusing with -1 is deterministic.
        if maximum.is_some_and(|max| desired > max) {
            return Ok(false);
        }
        let delta = desired.saturating_sub(current);
        let total = self.memory.saturating_add(delta);
        // Failing the call, rather than returning -1 from `memory.grow`,
        // makes the limit a call failure that bisection can attribute to
        // rows, instead of an allocation failure each guest handles its own
        // way.
        if total > self.limit {
            return Err(wasmtime::Error::new(MemoryLimitExceeded));
        }
        self.memory = total;
        self.peak = self.peak.max(total);
        Ok(true)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        Err(wasmtime::Error::new(HostAllocationFailed(format!(
            "{error:#}"
        ))))
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        if maximum.is_some_and(|max| desired > max) {
            return Ok(false);
        }
        let delta = desired.saturating_sub(current);
        let total = self.table_elements.saturating_add(delta);
        if total > MAX_TABLE_ELEMENTS {
            return Err(wasmtime::Error::new(TableLimitExceeded));
        }
        self.table_elements = total;
        Ok(true)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        Err(wasmtime::Error::new(HostAllocationFailed(format!(
            "{error:#}"
        ))))
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
    ///
    /// A replica caches the outcome and reports a compile error for every row
    /// that calls the module, so only failures that are properties of the
    /// module become a [`CompileError`]. Any other failure halts the process.
    pub fn compile(bytes: &[u8]) -> Result<Self, CompileError> {
        let info = ModuleInfo::parse(bytes)?;
        let module = Module::new(&ENGINE, bytes).map_err(|error| {
            // NOTE: Code generation limits in cranelift are also properties
            // of the module, but wasmtime does not expose their type. A
            // module that hits one halts every replica that installs it until
            // the module is dropped.
            if error
                .chain()
                .any(|cause| cause.is::<wasmparser::BinaryReaderError>())
            {
                CompileError::Compile(format!("{error:#}"))
            } else {
                mz_ore::halt!("WebAssembly host failure while compiling a module: {error:#}")
            }
        })?;
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

    /// The bytes of the compiled artifact: native code, data and metadata.
    pub fn compiled_size(&self) -> usize {
        let range = self.pre.module().image_range();
        range.end.addr().saturating_sub(range.start.addr())
    }

    /// Calls `export` once, in a fresh instance, with an Arrow IPC file as
    /// input.
    pub fn call(&self, export: &str, input: &[u8], limits: WasmLimits) -> CallReport {
        let memory_limit = usize::try_from(limits.memory_bytes).unwrap_or(usize::MAX);
        let mut store = Store::new(&ENGINE, StoreState::new(memory_limit));
        store.limiter(|state| &mut state.limiter);
        store.call_hook(charge_host_call);
        store
            .set_fuel(limits.fuel)
            .expect("the engine enables fuel");

        // Wasmtime bounds guest stack use relative to the stack pointer at
        // entry, so the call needs that much native stack plus headroom.
        let result = stacker::maybe_grow(
            MAX_WASM_STACK + HOST_STACK_HEADROOM,
            CALL_STACK_SIZE,
            || self.call_in(&mut store, export, input),
        );
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
        let protocol = |msg: &str| Ok(Err(CallError::Protocol(msg.to_string())));
        let Ok(alloc) = instance.get_typed_func::<(u32, u32), u32>(&mut *store, ALLOC_EXPORT)
        else {
            return protocol("the alloc export has the wrong type");
        };
        let Ok(func) = instance.get_typed_func::<(u32, u32, u32), i32>(&mut *store, export) else {
            return protocol("the function export has the wrong type");
        };
        let memory = instance
            .get_memory(&mut *store, MEMORY_EXPORT)
            .expect("validated export");

        let Some(len) = u32::try_from(input.len()).ok() else {
            return protocol("input batch exceeds 4 GiB");
        };
        let Some(alloc_len) = len.checked_add(8) else {
            return protocol("input batch exceeds 4 GiB");
        };
        // The first 8 bytes of the allocation receive the output slice, and
        // the input follows them.
        let ptr = alloc.call(&mut *store, (alloc_len, 4))?;
        if ptr == 0 {
            return protocol("guest failed to allocate the input buffer");
        }
        let allocation = usize::cast_from(ptr)..usize::cast_from(ptr) + usize::cast_from(alloc_len);
        if allocation.end > memory.data_size(&*store) {
            return protocol("the input buffer is out of bounds");
        }
        let in_ptr = ptr + 8;
        memory.data_mut(&mut *store)[allocation.start + 8..allocation.end].copy_from_slice(input);

        let code = func.call(&mut *store, (in_ptr, len, ptr))?;

        // Output buffers are never freed: the instance is discarded after
        // this call.
        let data = memory.data(&*store);
        let read_u32 = |at: usize| -> Option<u32> {
            Some(u32::from_le_bytes(
                data.get(at..at.checked_add(4)?)?.try_into().ok()?,
            ))
        };
        let (Some(out_ptr), Some(out_len)) =
            (read_u32(allocation.start), read_u32(allocation.start + 4))
        else {
            return protocol("output slice out of bounds");
        };
        let start = usize::cast_from(out_ptr);
        let Some(out) = start
            .checked_add(usize::cast_from(out_len))
            .and_then(|end| data.get(start..end))
        else {
            return protocol("output slice out of bounds");
        };
        Ok(match code {
            0 => Ok(out.to_vec()),
            _ => Err(CallError::Failed(truncated_message(out))),
        })
    }
}

/// The outcome of one guest call, with its resource usage.
#[derive(Debug)]
pub struct CallReport {
    /// The output Arrow IPC file, or why the call failed.
    pub result: Result<Vec<u8>, CallError>,
    pub fuel_consumed: u64,
    /// The most linear memory the instance held at once, in bytes.
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

/// The fuel every call from the guest into a host function costs, on top of
/// what the host function charges for the bytes it processes. Crossing into
/// the host costs about as much as this many guest instructions.
const HOST_CALL_FUEL: u64 = 100;

/// Charges [`HOST_CALL_FUEL`] for each call into the host.
fn charge_host_call(
    mut store: StoreContextMut<'_, StoreState>,
    hook: CallHook,
) -> wasmtime::Result<()> {
    if !matches!(hook, CallHook::CallingHost) {
        return Ok(());
    }
    match store.get_fuel()?.checked_sub(HOST_CALL_FUEL) {
        Some(remaining) => store.set_fuel(remaining),
        None => {
            store.set_fuel(0)?;
            Err(Trap::OutOfFuel.into())
        }
    }
}

/// `bytes` as UTF-8, cut to at most [`MAX_MESSAGE_BYTES`] bytes plus a
/// truncation marker.
pub(crate) fn truncated_message(bytes: &[u8]) -> String {
    let prefix = &bytes[..bytes.len().min(MAX_MESSAGE_BYTES)];
    let lossy = String::from_utf8_lossy(prefix);
    // Replacement characters can make the decoded text longer than its bytes.
    let mut message = truncated_str(&lossy, MAX_MESSAGE_BYTES);
    if bytes.len() > MAX_MESSAGE_BYTES && !message.ends_with(TRUNCATED) {
        message.push_str(TRUNCATED);
    }
    message
}

/// `s`, cut on a character boundary to at most `max` bytes plus a truncation
/// marker.
pub(crate) fn truncated_str(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{TRUNCATED}", &s[..end])
}

/// Appended to a message that was cut short.
const TRUNCATED: &str = " (truncated)";

/// The most bytes of a function name a trap backtrace shows per frame. Names
/// come from the module's name section, which the guest controls.
const MAX_FRAME_NAME_BYTES: usize = 256;

impl CallError {
    /// Classifies an error a call raised.
    ///
    /// A call's outcome is persisted, so it must depend only on the module,
    /// its input and its limits. Only errors known to come from the guest
    /// become a [`CallError`]. Anything else is the host failing, and the
    /// honest outcome then is none at all: halt, restart, and evaluate again.
    fn classify(error: &wasmtime::Error) -> CallError {
        if error.downcast_ref::<MemoryLimitExceeded>().is_some()
            || error.downcast_ref::<TableLimitExceeded>().is_some()
        {
            return CallError::MemoryLimit;
        }
        if let Some(exit) = error.downcast_ref::<ProcExit>() {
            return CallError::Trap(exit.to_string());
        }
        let Some(trap) = error.downcast_ref::<Trap>() else {
            mz_ore::halt!("WebAssembly host failure: {error:#}");
        };
        if *trap == Trap::OutOfFuel {
            return CallError::OutOfFuel;
        }
        let mut message = trap.to_string();
        if let Some(backtrace) = error.downcast_ref::<WasmBacktrace>() {
            for frame in backtrace.frames().iter().take(MAX_BACKTRACE_FRAMES) {
                let name = frame
                    .func_name()
                    .map(|n| truncated_str(n, MAX_FRAME_NAME_BYTES))
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
            let output = truncated_message(output);
            message = format!("{}\n{message}", output.trim_end());
        }
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: usize = 64 << 10;

    fn limiter(limit: usize) -> Limiter {
        StoreState::new(limit).limiter
    }

    #[mz_ore::test]
    fn memory_limit_is_aggregate() {
        let mut l = limiter(4 * PAGE);
        assert!(l.memory_growing(0, 2 * PAGE, None).unwrap());
        assert!(l.memory_growing(0, 2 * PAGE, None).unwrap());
        let err = l.memory_growing(0, PAGE, None).unwrap_err();
        assert!(err.downcast_ref::<MemoryLimitExceeded>().is_some());
        assert_eq!(l.peak, 4 * PAGE);
    }

    #[mz_ore::test]
    fn failed_growth_is_a_host_failure() {
        let mut l = limiter(4 * PAGE);
        let err = l
            .memory_grow_failed(wasmtime::Error::msg("mmap failed"))
            .unwrap_err();
        assert!(err.downcast_ref::<HostAllocationFailed>().is_some());
    }

    #[mz_ore::test]
    fn declared_maximum_refuses_without_counting() {
        let mut l = limiter(4 * PAGE);
        assert!(!l.memory_growing(0, 3 * PAGE, Some(2 * PAGE)).unwrap());
        assert!(l.memory_growing(0, 4 * PAGE, None).unwrap());
    }

    #[mz_ore::test]
    fn table_limit_is_aggregate() {
        let mut l = limiter(0);
        assert!(l.table_growing(0, MAX_TABLE_ELEMENTS / 2, None).unwrap());
        assert!(l.table_growing(0, MAX_TABLE_ELEMENTS / 2, None).unwrap());
        let err = l.table_growing(0, 1, None).unwrap_err();
        assert!(err.downcast_ref::<TableLimitExceeded>().is_some());
    }

    #[mz_ore::test]
    fn guest_messages_are_truncated() {
        let long = vec![b'x'; MAX_MESSAGE_BYTES + 1];
        let message = truncated_message(&long);
        assert!(message.ends_with(" (truncated)"));
        assert_eq!(message.len(), MAX_MESSAGE_BYTES + " (truncated)".len());
        assert_eq!(truncated_message(b"short"), "short");
    }
}
