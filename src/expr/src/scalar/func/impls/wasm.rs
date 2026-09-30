// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Calls to user-defined WebAssembly functions.
//!
//! A [`WasmFunc`] names a module by content hash and a guest export by
//! signature. It does not carry the module or a runtime: this crate is linked
//! by environmentd, which must never execute guest code. Processes that do
//! evaluate WebAssembly functions (clusterd, the `mz` CLI) install a
//! [`WasmRuntime`] with [`install_wasm_runtime`], and each `WasmFunc` binds
//! itself to a [`WasmInvoker`] from that runtime on first use.

use std::fmt;
use std::sync::{Arc, OnceLock};

use mz_proto::{RustType, TryFromProtoError};
use mz_repr::{Datum, RowArena, SqlColumnType, SqlScalarType};
use serde::{Deserialize, Serialize};

use crate::scalar::ProtoWasmErrorKind;
use crate::scalar::func::variadic::LazyVariadicFunc;
use crate::{Eval, EvalError};

/// The SHA-256 of a WebAssembly module's bytes.
#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct WasmModuleHash(pub [u8; 32]);

impl fmt::Display for WasmModuleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for WasmModuleHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WasmModuleHash({self})")
    }
}

/// The bytes of a WebAssembly module, shared rather than copied.
#[derive(Clone, PartialEq, Eq)]
pub struct WasmModuleBytes(pub Arc<[u8]>);

impl fmt::Debug for WasmModuleBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WasmModuleBytes({} bytes)", self.0.len())
    }
}

impl Serialize for WasmModuleBytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for WasmModuleBytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        Ok(WasmModuleBytes(Arc::from(bytes.into_vec())))
    }
}

/// Resource limits that apply to every guest call, whatever its batch size.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct WasmLimits {
    /// Wasmtime fuel available to one call.
    pub fuel: u64,
    /// The maximum size of the guest's linear memory, in bytes.
    pub memory_bytes: u64,
}

/// A call to an exported function of a WebAssembly module.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
pub struct WasmFunc {
    /// The function's fully qualified SQL name at plan time, for display and
    /// error messages only.
    pub name: String,
    pub module: WasmModuleHash,
    /// The arrow-udf export name that implements the call.
    pub export: String,
    pub arg_types: Vec<SqlScalarType>,
    pub return_type: SqlScalarType,
    /// Whether a `NULL` argument yields `NULL` without calling the guest.
    pub strict: bool,
    pub limits: WasmLimits,
    #[serde(skip)]
    pub invoker: InvokerCell,
}

impl WasmFunc {
    /// Calls the function on each row of `args` and appends one result per
    /// row to `out`.
    ///
    /// Rows with a `NULL` argument yield `NULL` without reaching the guest
    /// when the function is strict.
    pub fn call_batch<'a>(
        &self,
        args: &[&[Datum<'a>]],
        arena: &'a RowArena,
        out: &mut Vec<Result<Datum<'a>, EvalError>>,
    ) {
        let invoker = match self.invoker() {
            Ok(invoker) => invoker,
            Err(e) => {
                out.extend(args.iter().map(|_| Err(e.clone())));
                return;
            }
        };
        if !self.strict || args.iter().all(|row| !row.iter().any(Datum::is_null)) {
            invoker.call_batch(args, arena, out);
            return;
        }
        let live: Vec<&[Datum<'a>]> = args
            .iter()
            .copied()
            .filter(|row| !row.iter().any(Datum::is_null))
            .collect();
        let mut results = Vec::with_capacity(live.len());
        invoker.call_batch(&live, arena, &mut results);
        let mut results = results.into_iter();
        for row in args {
            if row.iter().any(Datum::is_null) {
                out.push(Ok(Datum::Null));
            } else {
                out.push(results.next().expect("one result per live row"));
            }
        }
    }

    fn invoker(&self) -> Result<&Arc<dyn WasmInvoker>, EvalError> {
        if let Some(invoker) = self.invoker.0.get() {
            return Ok(invoker);
        }
        let runtime = WASM_RUNTIME.get().ok_or_else(|| {
            EvalError::Internal(
                format!(
                    "WebAssembly function {} evaluated without a runtime",
                    self.name
                )
                .into(),
            )
        })?;
        let invoker = runtime.bind(self)?;
        Ok(self.invoker.0.get_or_init(|| invoker))
    }
}

impl LazyVariadicFunc for WasmFunc {
    fn eval<'a>(
        &'a self,
        datums: &[Datum<'a>],
        temp_storage: &'a RowArena,
        exprs: &'a [impl Eval],
    ) -> Result<Datum<'a>, EvalError> {
        let args = exprs
            .iter()
            .map(|e| e.eval(datums, temp_storage))
            .collect::<Result<Vec<_>, _>>()?;
        let mut out = Vec::with_capacity(1);
        self.call_batch(&[&args], temp_storage, &mut out);
        out.pop().expect("one result per row")
    }

    fn output_type(&self, _input_types: &[SqlColumnType]) -> SqlColumnType {
        self.return_type.clone().nullable(true)
    }

    fn propagates_nulls(&self) -> bool {
        self.strict
    }

    fn introduces_nulls(&self) -> bool {
        true
    }

    fn could_error(&self) -> bool {
        true
    }
}

impl fmt::Display for WasmFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}

/// Executes one bound WebAssembly function.
pub trait WasmInvoker: Send + Sync + fmt::Debug {
    /// Calls the function on each row of `args` and appends one result per
    /// row to `out`, in order.
    ///
    /// The result for a row must not depend on the other rows in the batch:
    /// it is the result of calling the function on that row alone.
    fn call_batch<'a>(
        &self,
        args: &[&[Datum<'a>]],
        arena: &'a RowArena,
        out: &mut Vec<Result<Datum<'a>, EvalError>>,
    );
}

/// Binds [`WasmFunc`]s to invokers.
pub trait WasmRuntime: Send + Sync {
    /// Returns an invoker for `func`. Errors if `func`'s module is not
    /// available to this process.
    fn bind(&self, func: &WasmFunc) -> Result<Arc<dyn WasmInvoker>, EvalError>;
}

static WASM_RUNTIME: OnceLock<Arc<dyn WasmRuntime>> = OnceLock::new();

/// Installs the process-wide runtime that evaluates WebAssembly functions.
///
/// Returns the runtime back if one is already installed. Processes that never
/// install one evaluate every WebAssembly call to an internal error.
pub fn install_wasm_runtime(runtime: Arc<dyn WasmRuntime>) -> Result<(), Arc<dyn WasmRuntime>> {
    WASM_RUNTIME.set(runtime)
}

/// A lazily bound invoker.
///
/// It is not part of a [`WasmFunc`]'s identity: every cell compares equal,
/// hashes to nothing, and is skipped by serde.
#[derive(Clone, Default)]
pub struct InvokerCell(OnceLock<Arc<dyn WasmInvoker>>);

impl fmt::Debug for InvokerCell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(if self.0.get().is_some() {
            "InvokerCell(bound)"
        } else {
            "InvokerCell(unbound)"
        })
    }
}

impl PartialEq for InvokerCell {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for InvokerCell {}

impl PartialOrd for InvokerCell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InvokerCell {
    fn cmp(&self, _other: &Self) -> std::cmp::Ordering {
        std::cmp::Ordering::Equal
    }
}

impl std::hash::Hash for InvokerCell {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

// A panic while binding leaves the cell unset, and invokers hold no state
// that a panic mid-call could leave inconsistent: each call runs in a fresh
// instance.
impl std::panic::UnwindSafe for InvokerCell {}
impl std::panic::RefUnwindSafe for InvokerCell {}

/// How a WebAssembly function call failed.
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize
)]
#[cfg_attr(any(test, feature = "proptest"), derive(proptest_derive::Arbitrary))]
pub enum WasmErrorKind {
    /// The guest returned an error for the row.
    Guest,
    /// The guest trapped.
    Trap,
    /// The call exhausted its fuel.
    OutOfFuel,
    /// The call exceeded its memory limit.
    MemoryLimit,
    /// The guest reported a failure for the whole call.
    CallFailed,
    /// A value could not be converted to or from the guest's representation.
    Conversion,
}

impl RustType<i32> for WasmErrorKind {
    fn into_proto(&self) -> i32 {
        let kind = match self {
            WasmErrorKind::Guest => ProtoWasmErrorKind::Guest,
            WasmErrorKind::Trap => ProtoWasmErrorKind::Trap,
            WasmErrorKind::OutOfFuel => ProtoWasmErrorKind::OutOfFuel,
            WasmErrorKind::MemoryLimit => ProtoWasmErrorKind::MemoryLimit,
            WasmErrorKind::CallFailed => ProtoWasmErrorKind::CallFailed,
            WasmErrorKind::Conversion => ProtoWasmErrorKind::Conversion,
        };
        kind.into()
    }

    fn from_proto(proto: i32) -> Result<Self, TryFromProtoError> {
        match ProtoWasmErrorKind::try_from(proto) {
            Ok(ProtoWasmErrorKind::Guest) => Ok(WasmErrorKind::Guest),
            Ok(ProtoWasmErrorKind::Trap) => Ok(WasmErrorKind::Trap),
            Ok(ProtoWasmErrorKind::OutOfFuel) => Ok(WasmErrorKind::OutOfFuel),
            Ok(ProtoWasmErrorKind::MemoryLimit) => Ok(WasmErrorKind::MemoryLimit),
            Ok(ProtoWasmErrorKind::CallFailed) => Ok(WasmErrorKind::CallFailed),
            Ok(ProtoWasmErrorKind::Conversion) => Ok(WasmErrorKind::Conversion),
            Ok(ProtoWasmErrorKind::Unspecified) | Err(_) => Err(
                TryFromProtoError::unknown_enum_variant(format!("ProtoWasmErrorKind::{proto}")),
            ),
        }
    }
}

impl fmt::Display for WasmErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WasmErrorKind::Guest => "error",
            WasmErrorKind::Trap => "trap",
            WasmErrorKind::OutOfFuel => "fuel exhausted",
            WasmErrorKind::MemoryLimit => "memory limit exceeded",
            WasmErrorKind::CallFailed => "call failed",
            WasmErrorKind::Conversion => "conversion error",
        })
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use mz_repr::{Datum, ReprScalarType, SqlScalarType};
    use proptest::prelude::*;

    use super::*;
    use crate::{MirScalarExpr, VariadicFunc};

    fn call(strict: bool, args: Vec<MirScalarExpr>) -> MirScalarExpr {
        MirScalarExpr::CallVariadic {
            func: VariadicFunc::Wasm(WasmFunc {
                name: "f".into(),
                module: WasmModuleHash([7; 32]),
                export: "f".into(),
                arg_types: vec![SqlScalarType::Int64; args.len()],
                return_type: SqlScalarType::Int64,
                strict,
                limits: WasmLimits {
                    fuel: 1,
                    memory_bytes: 1,
                },
                invoker: InvokerCell::default(),
            }),
            exprs: args,
        }
    }

    #[mz_ore::test]
    fn literal_calls_are_not_folded() {
        let literal = MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64);
        let mut expr = call(false, vec![literal]);
        let before = expr.clone();
        expr.reduce(&[]);
        assert_eq!(expr, before);
        assert!(expr.contains_unfoldable());
    }

    #[mz_ore::test]
    fn strict_calls_fold_null_arguments() {
        let null = MirScalarExpr::literal_null(ReprScalarType::Int64);
        let mut expr = call(true, vec![null]);
        expr.reduce(&[]);
        assert!(expr.is_literal_null(), "{expr:?}");
    }

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn wasm_error_kind_protobuf_roundtrip(kind in any::<WasmErrorKind>()) {
            let actual = protobuf_roundtrip::<_, i32>(&kind);
            prop_assert!(actual.is_ok());
            prop_assert_eq!(actual.unwrap(), kind);
        }
    }
}
