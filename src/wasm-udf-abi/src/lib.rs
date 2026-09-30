// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The guest side of Materialize's WebAssembly user-defined functions.
//!
//! Guests implement the [arrow-udf] 3.x ABI: a `wasm32-wasip1` core module that
//! exports one `arrowudf_<symbol>` function per signature, where `<symbol>` is
//! the signature string (`name(arg,...)->ret`) in a symbol-safe base64. This
//! crate knows how to name signatures, how SQL types map onto arrow-udf type
//! names, and how to inspect a module without compiling it. It deliberately
//! has no dependency on a Wasm runtime, so that the SQL planner can validate
//! `CREATE FUNCTION` without linking one.
//!
//! [arrow-udf]: https://github.com/arrow-udf/arrow-udf

mod module;
mod sig;
mod types;
pub mod wasi;

pub use module::{AbiError, Import, ModuleInfo};
pub use sig::{ScalarSignature, decode_symbol, encode_symbol, function_export_name};
pub use types::{UdfType, UnsupportedType};

/// The arrow-udf ABI major version this crate implements.
pub const ABI_MAJOR_VERSION: u8 = 3;

/// The fuel one call gets unless a function sets `FUEL`. A limit applies to a
/// whole batched call, so this leaves about 6K instructions per row at the
/// default maximum batch of 16K rows, and heavier functions get smaller
/// batches.
pub const DEFAULT_FUEL: u64 = 100_000_000;

/// The memory limit of one call unless a function sets `MEMORY`. Sized for
/// the input and output buffers of a full batch plus working memory.
pub const DEFAULT_MEMORY_BYTES: u64 = 64 << 20;

/// The guest export that allocates `len` bytes aligned to `align`:
/// `(len: i32, align: i32) -> i32`.
pub const ALLOC_EXPORT: &str = "alloc";

/// The guest export that frees an allocation: `(ptr: i32, len: i32, align: i32)`.
pub const DEALLOC_EXPORT: &str = "dealloc";

/// The guest's linear memory.
pub const MEMORY_EXPORT: &str = "memory";

/// The WASI reactor initializer, called once per instance before any other
/// export if the module defines it.
pub const INITIALIZE_EXPORT: &str = "_initialize";

/// The name of the optional per-row error column in a function's output batch.
pub const ERROR_COLUMN: &str = "error";

/// The Arrow extension type name arrow-udf uses to mark `Utf8` columns that
/// carry decimal text.
pub const DECIMAL_EXTENSION: &str = "arrowudf.decimal";

/// The Arrow extension type name arrow-udf uses to mark `Utf8` columns that
/// carry JSON text.
pub const JSON_EXTENSION: &str = "arrowudf.json";

/// The Arrow field metadata key that names a field's extension type.
pub const EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
