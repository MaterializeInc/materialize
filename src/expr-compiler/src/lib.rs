// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WASM-compiled scalar expression evaluation.
//!
//! This crate compiles a subset of [`mz_expr::MirScalarExpr`] trees into WebAssembly
//! modules that operate on columnar data, replacing the tree-walking interpreter
//! with batch-oriented compiled evaluation.
//!
//! # Architecture
//!
//! 1. **Analysis** ([`analyze`]): determines whether an expression tree is compilable.
//! 2. **Code generation** ([`codegen`]): emits a WASM module via `wasm-encoder`.
//! 3. **Engine** ([`engine`]): compiles and instantiates the module via `wasmtime`.
//! 4. **Columnar buffers** ([`columnar`]): converts between row-oriented and columnar data.

pub mod analyze;
pub mod codegen;
pub mod columnar;
pub mod engine;
pub mod eval;
