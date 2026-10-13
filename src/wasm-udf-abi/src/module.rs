// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Inspection and validation of guest modules without compiling them.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};
use wasmparser::types::EntityType;
use wasmparser::{CompositeInnerType, FuncType, Validator, WasmFeatures};

use crate::sig::{FUNCTION_PREFIX, ScalarSignature, TYPE_PREFIX, decode_symbol};
use crate::{ABI_VERSION, ALLOC_EXPORT, DEALLOC_EXPORT, INITIALIZE_EXPORT, MEMORY_EXPORT};
use crate::{wasi, wasi::ValType};

const VERSION_PREFIX: &str = "ARROWUDF_VERSION_";

/// A module import.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Import {
    pub module: String,
    pub name: String,
}

/// What a guest module exports and imports, gathered by [`ModuleInfo::parse`].
#[derive(Clone, Debug)]
pub struct ModuleInfo {
    /// The arrow-udf ABI version, `(major, minor)`.
    pub abi_version: (u8, u8),
    /// Every exported function signature, decoded from export names.
    pub functions: BTreeSet<String>,
    /// Every declared struct type, from `arrowudt_` exports, by name.
    pub types: BTreeMap<String, String>,
    pub imports: Vec<Import>,
    /// The SHA-256 of the module bytes.
    pub hash: [u8; 32],
    pub size: usize,
}

impl ModuleInfo {
    /// Validates `bytes` as a guest module and gathers its exports and
    /// imports.
    ///
    /// A module that passes validates under the feature set the runtime
    /// enables, implements a supported ABI version, and imports only WASI
    /// preview1 functions with their standard signatures, so the runtime can
    /// always link it.
    pub fn parse(bytes: &[u8]) -> Result<Self, AbiError> {
        let types = Validator::new_with_features(features())
            .validate_all(bytes)
            .map_err(|e| AbiError::Invalid(e.message().to_string()))?;
        let types = types.as_ref();
        let func_type = |entity: EntityType| -> Option<&FuncType> {
            match entity {
                EntityType::Func(id) | EntityType::FuncExact(id) => {
                    match &types[id].composite_type.inner {
                        CompositeInnerType::Func(f) => Some(f),
                        _ => None,
                    }
                }
                _ => None,
            }
        };

        let mut abi_version = None;
        let mut functions = BTreeSet::new();
        let mut struct_types = BTreeMap::new();
        let mut exports = BTreeMap::new();
        for (name, entity) in types.core_exports().expect("core module") {
            exports.insert(name, entity);
            if let Some(version) = name.strip_prefix(VERSION_PREFIX) {
                abi_version = Some(parse_version(version).ok_or_else(|| AbiError::BadExport {
                    name: name.into(),
                    reason: "malformed ABI version".into(),
                })?);
            } else if let Some(symbol) = name.strip_prefix(FUNCTION_PREFIX) {
                let signature = decode_symbol(symbol).ok_or_else(|| AbiError::BadExport {
                    name: name.into(),
                    reason: "malformed signature symbol".into(),
                })?;
                if !func_type(entity).is_some_and(|t| is_sig(t, &[I32, I32, I32], &[I32])) {
                    return Err(AbiError::BadExport {
                        name: signature,
                        reason: "expected (i32, i32, i32) -> i32".into(),
                    });
                }
                functions.insert(signature);
            } else if let Some(symbol) = name.strip_prefix(TYPE_PREFIX) {
                let decl = decode_symbol(symbol).ok_or_else(|| AbiError::BadExport {
                    name: name.into(),
                    reason: "malformed type symbol".into(),
                })?;
                let (type_name, fields) =
                    decl.split_once('=').ok_or_else(|| AbiError::BadExport {
                        name: decl.clone(),
                        reason: "malformed type declaration".into(),
                    })?;
                struct_types.insert(type_name.to_string(), fields.to_string());
            }
        }

        let abi_version = abi_version.ok_or(AbiError::MissingVersion)?;
        if abi_version != ABI_VERSION {
            return Err(AbiError::UnsupportedVersion(abi_version.0, abi_version.1));
        }

        let required: [(&'static str, &[ValType], &[ValType]); 2] = [
            (ALLOC_EXPORT, &[I32, I32], &[I32]),
            (DEALLOC_EXPORT, &[I32, I32, I32], &[]),
        ];
        for (name, params, results) in required {
            let entity = exports.get(name).ok_or(AbiError::MissingExport(name))?;
            if !func_type(*entity).is_some_and(|t| is_sig(t, params, results)) {
                return Err(AbiError::BadExport {
                    name: name.into(),
                    reason: "unexpected signature".into(),
                });
            }
        }
        match exports.get(MEMORY_EXPORT) {
            Some(EntityType::Memory(_)) => {}
            Some(_) => {
                return Err(AbiError::BadExport {
                    name: MEMORY_EXPORT.into(),
                    reason: "not a memory".into(),
                });
            }
            None => return Err(AbiError::MissingExport(MEMORY_EXPORT)),
        }
        if let Some(entity) = exports.get(INITIALIZE_EXPORT) {
            if !func_type(*entity).is_some_and(|t| is_sig(t, &[], &[])) {
                return Err(AbiError::BadExport {
                    name: INITIALIZE_EXPORT.into(),
                    reason: "expected () -> ()".into(),
                });
            }
        }

        let mut imports = Vec::new();
        for (module, name, entity) in types.core_imports().expect("core module") {
            let import = Import {
                module: module.into(),
                name: name.into(),
            };
            let expected = (module == wasi::MODULE)
                .then(|| wasi::function(name))
                .flatten();
            let Some((_, params, results)) = expected else {
                return Err(AbiError::UnsupportedImport(import));
            };
            if !func_type(entity).is_some_and(|t| is_sig(t, params, results)) {
                return Err(AbiError::ImportSignatureMismatch(import));
            }
            imports.push(import);
        }
        imports.sort();

        Ok(ModuleInfo {
            abi_version,
            functions,
            types: struct_types,
            imports,
            hash: module_hash(bytes),
            size: bytes.len(),
        })
    }

    /// Returns the export that implements `sig`, or an error listing the
    /// signatures the module does export.
    pub fn scalar_export(&self, sig: &ScalarSignature) -> Result<String, AbiError> {
        if self.functions.contains(&sig.to_string()) {
            Ok(sig.export_name())
        } else {
            Err(AbiError::NoSuchFunction {
                signature: sig.to_string(),
                available: self.functions.iter().cloned().collect(),
            })
        }
    }

    /// Imports whose host implementation is deterministic but differs from a
    /// real system, with a description of what the host does instead.
    pub fn determinized_imports(&self) -> impl Iterator<Item = (&Import, &'static str)> {
        self.imports.iter().filter_map(|import| {
            wasi::DETERMINIZED
                .iter()
                .find(|(name, _)| *name == import.name)
                .map(|(_, what)| (import, *what))
        })
    }
}

use ValType::I32;

fn is_sig(t: &FuncType, params: &[ValType], results: &[ValType]) -> bool {
    fn eq(actual: &[wasmparser::ValType], expected: &[ValType]) -> bool {
        let expected = expected.iter().map(|e| match e {
            ValType::I32 => wasmparser::ValType::I32,
            ValType::I64 => wasmparser::ValType::I64,
        });
        actual.iter().copied().eq(expected)
    }
    eq(t.params(), params) && eq(t.results(), results)
}

fn parse_version(version: &str) -> Option<(u8, u8)> {
    let (major, minor) = version.split_once('_')?;
    Some((major.parse().ok()?, minor.parse().ok()?))
}

/// The SHA-256 of a module's bytes, which identifies it everywhere it is
/// installed.
pub fn module_hash(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// The Wasm features guest modules may use: the features the Rust
/// `wasm32-wasip1` target enables by default, plus fixed-width SIMD.
///
/// This is an allowlist, so a feature a newer wasmparser adds stays rejected
/// until it is reviewed here. The runtime's engine must accept every module
/// these features allow; `mz_wasm_udf`'s tests check that for the features
/// whose engine support depends on wasmtime's build configuration. Left out
/// deliberately: threads and relaxed SIMD, which are nondeterministic;
/// multiple memories, each of which reserves host address space the memory
/// limit does not bound; and `externref` and GC types, which this wasmtime
/// build does not support.
pub fn features() -> WasmFeatures {
    WasmFeatures::FLOATS
        | WasmFeatures::MUTABLE_GLOBAL
        | WasmFeatures::SATURATING_FLOAT_TO_INT
        | WasmFeatures::SIGN_EXTENSION
        | WasmFeatures::REFERENCE_TYPES
        | WasmFeatures::MULTI_VALUE
        | WasmFeatures::BULK_MEMORY
        | WasmFeatures::SIMD
}

/// A module that cannot be used as a guest.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AbiError {
    #[error("invalid WebAssembly module: {0}")]
    Invalid(String),
    #[error("module does not export an arrow-udf ABI version")]
    MissingVersion,
    #[error("unsupported arrow-udf ABI version {0}.{1}, expected {major}.{minor}", major = ABI_VERSION.0, minor = ABI_VERSION.1)]
    UnsupportedVersion(u8, u8),
    #[error("module does not export `{0}`")]
    MissingExport(&'static str),
    #[error("export `{name}`: {reason}")]
    BadExport { name: String, reason: String },
    #[error(
        "module imports {}.{}, but only {} functions are provided",
        .0.module, .0.name, wasi::MODULE
    )]
    UnsupportedImport(Import),
    #[error("import {}.{} has a non-standard signature", .0.module, .0.name)]
    ImportSignatureMismatch(Import),
    #[error(
        "module does not export {signature}; it exports: {}",
        if available.is_empty() { "nothing".to_string() } else { available.join(", ") }
    )]
    NoSuchFunction {
        signature: String,
        available: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn rejects_garbage() {
        assert!(matches!(
            ModuleInfo::parse(b"not wasm"),
            Err(AbiError::Invalid(_))
        ));
    }

    #[mz_ore::test]
    fn rejects_multiple_memories() {
        // The module header, then a memory section declaring two memories
        // of one page each.
        let bytes = b"\0asm\x01\0\0\0\x05\x05\x02\x00\x01\x00\x01";
        let err = ModuleInfo::parse(bytes).unwrap_err();
        assert!(
            matches!(&err, AbiError::Invalid(m) if m.contains("multiple memories")),
            "{err}"
        );
    }

    #[mz_ore::test]
    fn rejects_other_abi_versions() {
        // The module header, a memory section with one memory, and an export
        // section exporting it as the ABI 3.1 version marker.
        let mut bytes = b"\0asm\x01\0\0\0\x05\x03\x01\x00\x01\x07\x18\x01\x14".to_vec();
        bytes.extend_from_slice(b"ARROWUDF_VERSION_3_1\x02\x00");
        assert_eq!(
            ModuleInfo::parse(&bytes).unwrap_err(),
            AbiError::UnsupportedVersion(3, 1)
        );
    }

    #[mz_ore::test]
    fn version_parsing() {
        assert_eq!(parse_version("3_0"), Some((3, 0)));
        assert_eq!(parse_version("3"), None);
        assert_eq!(parse_version("x_1"), None);
    }
}
