// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A deterministic WebAssembly runtime for arrow-udf scalar functions.
//!
//! This is the host side of `mz_wasm_udf_abi`. It evaluates the
//! `mz_expr::func::WasmFunc` calls in dataflows and peeks, and backs the
//! `mz udf` CLI, so a function behaves identically in both. The pieces:
//!
//! * [`engine`]: the one wasmtime configuration every process uses.
//! * `wasi`: a deterministic WASI preview1 implementation.
//! * [`call`]: compiled modules and single calls in fresh instances.
//! * [`codec`]: datums to and from the guest's Arrow batches.
//! * [`invoker`]: batched calls with per-row semantics.
//! * [`runtime`]: the process-wide module cache that `mz_expr` binds
//!   functions through.

pub mod call;
pub mod codec;
pub mod engine;
pub mod invoker;
pub mod runtime;
mod wasi;

pub use call::{CallError, CallReport, CompileError, CompiledModule};
pub use invoker::{BatchConfig, CallObserver, Invoker};
pub use runtime::Runtime;
