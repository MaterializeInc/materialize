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

/// A module's compilation outcome. Failures are kept so that every function
/// bound to the module reports the same error.
type ModuleCell = Arc<OnceLock<Result<Arc<CompiledModule>, String>>>;

/// Compiled modules by content hash, and the batch configuration their
/// invokers share.
#[derive(Debug, Default)]
pub struct Runtime {
    // TODO: evict modules no installed dataflow or pending peek references.
    modules: Mutex<BTreeMap<WasmModuleHash, ModuleCell>>,
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

    /// Compiles `bytes` and caches the result under `hash`.
    ///
    /// The first call for a hash compiles; concurrent and later calls wait
    /// for and return its outcome. Errors if `bytes` do not form a valid
    /// guest module or do not hash to `hash`, and functions bound to the
    /// module then fail with that error.
    pub fn install(&self, hash: WasmModuleHash, bytes: &[u8]) -> Result<(), CompileError> {
        let cell = Arc::clone(
            self.modules
                .lock()
                .expect("lock poisoned")
                .entry(hash)
                .or_default(),
        );
        let mut error = None;
        cell.get_or_init(|| {
            let compiled = CompiledModule::compile(bytes).and_then(|module| {
                if module.info().hash == hash.0 {
                    Ok(Arc::new(module))
                } else {
                    Err(CompileError::Compile(format!(
                        "module bytes hash to {}, expected {hash}",
                        WasmModuleHash(module.info().hash)
                    )))
                }
            });
            compiled.map_err(|e| {
                let message = e.to_string();
                error = Some(e);
                message
            })
        });
        match error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Whether a module with `hash` compiled successfully.
    pub fn contains(&self, hash: &WasmModuleHash) -> bool {
        self.module(hash).is_some_and(|m| m.is_ok())
    }

    fn module(&self, hash: &WasmModuleHash) -> Option<Result<Arc<CompiledModule>, String>> {
        let cell = self
            .modules
            .lock()
            .expect("lock poisoned")
            .get(hash)
            .cloned()?;
        cell.get().cloned()
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

impl WasmRuntime for Runtime {
    fn bind(&self, func: &WasmFunc) -> Result<Arc<dyn WasmInvoker>, EvalError> {
        match self.module(&func.module) {
            Some(Ok(module)) => Ok(Arc::new(self.invoker(module, func)?)),
            Some(Err(message)) => Err(EvalError::WasmFunction {
                name: func.name.clone().into(),
                kind: WasmErrorKind::CallFailed,
                message: format!("module failed to compile: {message}").into(),
            }),
            None => Err(EvalError::Internal(
                format!(
                    "WebAssembly module {} for function {} is not installed",
                    func.module, func.name
                )
                .into(),
            )),
        }
    }
}
