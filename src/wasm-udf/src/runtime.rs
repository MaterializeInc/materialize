// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The process-wide module cache and [`WasmRuntime`] implementation.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use mz_expr::EvalError;
use mz_expr::func::{
    WasmErrorKind, WasmFunc, WasmInvoker, WasmModuleHash, WasmRuntime, install_wasm_runtime,
};

use crate::call::{CompileError, CompiledModule};
use crate::codec::ValueType;
use crate::invoker::{BatchConfig, Invoker};

/// A module's compilation outcome, set once compilation finishes. Failures
/// are kept so that every function bound to the module reports the same
/// error.
type ModuleCell = Arc<OnceLock<Result<Arc<CompiledModule>, String>>>;

/// The memory the runtime spends keeping modules compiled after their last
/// install is undone, as [`Installed::footprint`] estimates it. A query that
/// is the only user of a module on a cluster installs and drops the module
/// around every peek; keeping it compiled saves a wasmtime compilation per
/// query.
const RETAINED_BYTES: usize = 256 << 20;

/// The most modules the runtime keeps compiled after their last install is
/// undone, whatever their size.
const RETAINED_MODULES: usize = 64;

/// The memory a compiled module costs beyond its bytes and compiled artifact:
/// its code mapping, type and metadata tables, and the instantiation
/// template.
const MODULE_OVERHEAD_BYTES: usize = 1 << 20;

/// A known module, and how many installs it has outstanding.
///
/// Every worker of a replica processes each install and drop command, in its
/// own order relative to the other workers. Counting installs means a drop
/// from one worker cannot evict a module another worker has already
/// reinstalled: the module stays until every install has been matched by a
/// drop. A module with no installs is retained, compiled, until
/// [`RETAINED_BYTES`] forces it out.
#[derive(Debug)]
struct Installed {
    cell: ModuleCell,
    installs: usize,
    /// The length of the module's bytes.
    bytes: usize,
    /// When the last install was undone, on the runtime's release clock, if
    /// the module has no installs.
    released: Option<u64>,
}

impl Installed {
    /// The memory this module holds: its bytes, its compiled artifact once
    /// compilation finishes, and a fixed overhead.
    fn footprint(&self) -> usize {
        let compiled = match self.cell.get() {
            Some(Ok(module)) => module.compiled_size(),
            _ => 0,
        };
        self.bytes
            .saturating_add(compiled)
            .saturating_add(MODULE_OVERHEAD_BYTES)
    }
}

#[derive(Debug, Default)]
struct Modules {
    by_hash: BTreeMap<WasmModuleHash, Installed>,
    /// Ticks once per release, ordering retained modules for eviction.
    release_clock: u64,
}

impl Modules {
    /// Evicts the least recently released modules until the retained ones
    /// fit in [`RETAINED_BYTES`] and [`RETAINED_MODULES`].
    fn evict(&mut self) {
        loop {
            let (count, bytes) = self
                .by_hash
                .values()
                .filter(|m| m.released.is_some())
                .fold((0, 0usize), |(count, bytes), m| {
                    (count + 1, bytes.saturating_add(m.footprint()))
                });
            if count <= RETAINED_MODULES && bytes <= RETAINED_BYTES {
                return;
            }
            let oldest = self
                .by_hash
                .iter()
                .filter_map(|(hash, m)| Some((m.released?, *hash)))
                .min()
                .map(|(_, hash)| hash)
                .expect("retained bytes are positive");
            self.by_hash.remove(&oldest);
        }
    }
}

/// Compiled modules by content hash, and the batch configuration their
/// invokers share.
#[derive(Debug, Default)]
pub struct Runtime {
    modules: Mutex<Modules>,
    batch: Arc<BatchConfig>,
}

static RUNTIME: LazyLock<Arc<Runtime>> = LazyLock::new(|| {
    let runtime = Arc::new(Runtime::default());
    let installed: Arc<Runtime> = Arc::clone(&runtime);
    if install_wasm_runtime(installed).is_err() {
        panic!("a different WebAssembly runtime is already installed");
    }
    runtime
});

impl Runtime {
    /// The process-wide runtime, which is installed as `mz_expr`'s
    /// WebAssembly runtime on first use.
    pub fn global() -> &'static Arc<Runtime> {
        &RUNTIME
    }

    /// Installs the module with `hash`, compiling `bytes` on a background
    /// thread if it is not installed already, and returns without waiting.
    ///
    /// Binding a function to the module waits for compilation to finish, so
    /// installing never stalls a worker that does not call the module. If
    /// `bytes` do not form a valid guest module or do not hash to `hash`,
    /// functions bound to the module fail with that error.
    pub fn install(&self, hash: WasmModuleHash, bytes: Arc<[u8]>) {
        let mut modules = self.modules.lock().expect("lock poisoned");
        if let Some(installed) = modules.by_hash.get_mut(&hash) {
            installed.installs += 1;
            installed.released = None;
            return;
        }
        let cell = ModuleCell::default();
        modules.by_hash.insert(
            hash,
            Installed {
                cell: Arc::clone(&cell),
                installs: 1,
                bytes: bytes.len(),
                released: None,
            },
        );
        drop(modules);
        std::thread::Builder::new()
            .name(format!("wasm-compile-{hash}"))
            .spawn(move || {
                // `compile` halts on host failures. A panic is a bug in the
                // compiler, not a property of the module, so it must not
                // become every row's error either.
                let compiled = match mz_ore::panic::catch_unwind_str(|| compile(hash, &bytes)) {
                    Ok(compiled) => compiled,
                    Err(panic) => {
                        mz_ore::halt!("WebAssembly module {hash} panicked the compiler: {panic}")
                    }
                };
                let compiled = compiled.map_err(|e| {
                    tracing::warn!(%hash, "WebAssembly module failed to compile: {e}");
                    e.to_string()
                });
                let _ = cell.set(compiled);
            })
            .expect("can spawn a thread");
    }

    /// Undoes one [`Self::install`] of the module with `hash`. Once every
    /// install is undone, the compiled module is retained for reuse until
    /// [`RETAINED_BYTES`] forces it out. Functions already bound to it keep
    /// it alive until they are dropped.
    pub fn uninstall(&self, hash: &WasmModuleHash) {
        let mut modules = self.modules.lock().expect("lock poisoned");
        modules.release_clock += 1;
        let now = modules.release_clock;
        if let Some(installed) = modules.by_hash.get_mut(hash) {
            installed.installs -= 1;
            if installed.installs == 0 {
                installed.released = Some(now);
                modules.evict();
            }
        }
    }

    /// Holds the installed module with `hash` installed for as long as the
    /// returned pin lives, or returns `None` if it is not installed.
    ///
    /// Work that can outlive the command that installed its modules, such as
    /// peek evaluation off the worker thread that keeps running briefly after
    /// the peek is cancelled, holds pins on the modules it calls, so a
    /// function it binds late still finds its module.
    pub fn pin(&self, hash: WasmModuleHash) -> Option<ModulePin> {
        let mut modules = self.modules.lock().expect("lock poisoned");
        let installed = modules.by_hash.get_mut(&hash)?;
        if installed.installs == 0 {
            return None;
        }
        installed.installs += 1;
        Some(ModulePin { hash })
    }

    /// Whether the module with `hash` is installed and compiled successfully,
    /// waiting for compilation to finish.
    pub fn contains(&self, hash: &WasmModuleHash) -> bool {
        self.module(hash).is_some_and(|m| m.is_ok())
    }

    /// The compiled module with `hash`, waiting for compilation to finish, or
    /// `None` if it is not installed.
    fn module(&self, hash: &WasmModuleHash) -> Option<Result<Arc<CompiledModule>, String>> {
        let cell = self
            .modules
            .lock()
            .expect("lock poisoned")
            .by_hash
            .get(hash)
            .filter(|installed| installed.installs > 0)
            .map(|installed| Arc::clone(&installed.cell))?;
        Some(cell.wait().clone())
    }

    pub fn batch_config(&self) -> &BatchConfig {
        &self.batch
    }

    /// Builds an invoker for `func` against `module`.
    pub fn invoker(
        &self,
        module: Arc<CompiledModule>,
        func: &WasmFunc,
    ) -> Result<Invoker, EvalError> {
        let error = |message: String| EvalError::WasmFunction {
            name: func.name.clone().into(),
            kind: WasmErrorKind::CallFailed,
            message: message.into(),
        };
        let value_type = |t: &mz_repr::SqlScalarType| {
            ValueType::new(t.clone()).map_err(|e| error(e.to_string()))
        };
        let args = func
            .arg_types
            .iter()
            .map(value_type)
            .collect::<Result<Vec<_>, _>>()?;
        let ret = value_type(&func.return_type)?;
        let exported = module
            .info()
            .functions
            .iter()
            .any(|sig| mz_wasm_udf_abi::function_export_name(sig) == func.export);
        if !exported {
            return Err(error(format!("module does not export {}", func.export)));
        }
        Ok(Invoker::new(
            func.name.clone(),
            module,
            func.export.clone(),
            args,
            ret,
            func.limits,
            Arc::clone(&self.batch),
        ))
    }
}

/// An install of a module that lasts as long as this value, from
/// [`Runtime::pin`].
#[derive(Debug)]
#[must_use]
pub struct ModulePin {
    hash: WasmModuleHash,
}

impl Drop for ModulePin {
    fn drop(&mut self) {
        Runtime::global().uninstall(&self.hash);
    }
}

/// Compiles `bytes` and checks that they hash to `hash`.
fn compile(hash: WasmModuleHash, bytes: &[u8]) -> Result<Arc<CompiledModule>, CompileError> {
    let module = CompiledModule::compile(bytes)?;
    // The controller hashes the bytes it sends, so a mismatch is corruption,
    // not a property of the module, and must not become every row's error.
    if module.info().hash != hash.0 {
        mz_ore::halt!(
            "WebAssembly module bytes hash to {}, expected {hash}",
            WasmModuleHash(module.info().hash)
        );
    }
    Ok(Arc::new(module))
}

impl WasmRuntime for Runtime {
    fn bind(&self, func: &WasmFunc) -> Result<Arc<dyn WasmInvoker>, EvalError> {
        match self.module(&func.module) {
            Some(Ok(module)) => Ok(Arc::new(self.invoker(module, func)?)),
            Some(Err(message)) => Err(EvalError::WasmFunction {
                name: func.name.clone().into(),
                kind: WasmErrorKind::CallFailed,
                message: format!("module failed to compile: {message}").into(),
            }),
            // The compute controller installs a module before any dataflow or
            // peek that calls it. An error here would be persisted although
            // it depends on command timing; a restarted replica reconciles
            // and receives every install it needs.
            None => mz_ore::halt!(
                "WebAssembly module {} for function {} is not installed",
                func.module,
                func.name
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn module(bytes: usize, installs: usize, released: Option<u64>) -> Installed {
        Installed {
            cell: ModuleCell::default(),
            installs,
            bytes,
            released,
        }
    }

    #[mz_ore::test]
    fn eviction_keeps_installed_and_recently_released_modules() {
        // Each footprint is exactly half the budget.
        let half = RETAINED_BYTES / 2 - MODULE_OVERHEAD_BYTES;
        let mut modules = Modules::default();
        modules
            .by_hash
            .insert(WasmModuleHash([1; 32]), module(RETAINED_BYTES, 1, None));
        modules
            .by_hash
            .insert(WasmModuleHash([2; 32]), module(half, 0, Some(1)));
        modules
            .by_hash
            .insert(WasmModuleHash([3; 32]), module(half, 0, Some(2)));
        modules.evict();
        assert_eq!(modules.by_hash.len(), 3, "retained modules fit exactly");

        modules
            .by_hash
            .insert(WasmModuleHash([4; 32]), module(1, 0, Some(3)));
        modules.evict();
        let hashes: Vec<_> = modules.by_hash.keys().map(|h| h.0[0]).collect();
        assert_eq!(hashes, vec![1, 3, 4], "the least recently released goes");
    }

    #[mz_ore::test]
    fn eviction_caps_the_number_of_retained_modules() {
        let mut modules = Modules::default();
        for i in 0..=RETAINED_MODULES {
            let n = u8::try_from(i).unwrap();
            modules.by_hash.insert(
                WasmModuleHash([n; 32]),
                module(1, 0, Some(i.try_into().unwrap())),
            );
        }
        modules.evict();
        assert_eq!(modules.by_hash.len(), RETAINED_MODULES);
        assert!(!modules.by_hash.contains_key(&WasmModuleHash([0; 32])));
    }
}
