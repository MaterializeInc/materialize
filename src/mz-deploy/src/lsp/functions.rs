// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Registry of Materialize SQL functions for LSP completion and hover.
//!
//! Built at first use from the canonical builtin registries in
//! [`mz_sql::func`] so every overload the planner supports is automatically
//! discoverable. Descriptions are not sourced from upstream — the LSP only
//! shows kind, name, and synthesized signature strings.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_sql::func::{
    Func, FuncImplCatalogDetails, INFORMATION_SCHEMA_BUILTINS, MZ_CATALOG_BUILTINS,
    MZ_INTERNAL_BUILTINS, PG_CATALOG_BUILTINS,
};

/// The kind of a SQL function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    /// A scalar function that returns a single value per input row.
    Scalar,
    /// An aggregate function that combines multiple rows into one result.
    Aggregate,
    /// A window function that computes values across related rows.
    Window,
    /// A table function that returns a set of rows.
    Table,
}

impl FunctionKind {
    fn from_func(func: &Func) -> Self {
        match func {
            Func::Scalar(_) => FunctionKind::Scalar,
            Func::Aggregate(_) => FunctionKind::Aggregate,
            Func::Table(_) => FunctionKind::Table,
            Func::ScalarWindow(_) | Func::ValueWindow(_) => FunctionKind::Window,
        }
    }
}

/// Metadata about a single SQL function, collapsing all overloads for a name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionInfo {
    /// The function name (lowercase).
    pub name: String,
    /// One entry per overload, e.g. `abs(int2) -> int2`.
    pub signatures: Vec<String>,
    /// Whether this is a scalar, aggregate, window, or table function. When a
    /// name appears in multiple registries, the first-seen kind wins.
    pub kind: FunctionKind,
}

/// All documented Materialize SQL functions, sorted alphabetically by name.
///
/// Merged across `pg_catalog`, `mz_catalog`, `information_schema`, and
/// `mz_internal`. When the same name appears in multiple registries,
/// overloads are appended and the first-seen `kind` is retained (name
/// collisions across registries with different kinds are not expected in
/// practice).
pub static FUNCTIONS: LazyLock<Vec<FunctionInfo>> = LazyLock::new(build_functions);

fn build_functions() -> Vec<FunctionInfo> {
    let registries: &[&LazyLock<BTreeMap<&'static str, Func>>] = &[
        &PG_CATALOG_BUILTINS,
        &MZ_CATALOG_BUILTINS,
        &INFORMATION_SCHEMA_BUILTINS,
        &MZ_INTERNAL_BUILTINS,
    ];

    let mut by_name: BTreeMap<String, FunctionInfo> = BTreeMap::new();
    for registry in registries {
        for (name, func) in registry.iter() {
            let kind = FunctionKind::from_func(func);
            let entry = by_name
                .entry((*name).to_string())
                .or_insert_with(|| FunctionInfo {
                    name: (*name).to_string(),
                    signatures: Vec::new(),
                    kind,
                });
            for details in func.func_impls() {
                entry.signatures.push(format_signature(name, &details));
            }
        }
    }

    by_name.into_values().collect()
}

fn format_signature(name: &str, details: &FuncImplCatalogDetails) -> String {
    let mut args: Vec<String> = details.arg_typs.iter().map(|t| (*t).to_string()).collect();
    if let Some(variadic) = details.variadic_typ {
        args.push(format!("VARIADIC {variadic}"));
    }
    let args_str = args.join(", ");
    let return_str = match (details.return_typ, details.return_is_set) {
        (Some(t), true) => format!(" -> setof {t}"),
        (Some(t), false) => format!(" -> {t}"),
        (None, _) => String::new(),
    };
    format!("{name}({args_str}){return_str}")
}

/// Look up a function by exact name (case-insensitive).
///
/// Returns the merged entry containing every overload across registries.
pub fn lookup(name: &str) -> Option<&'static FunctionInfo> {
    let name_lower = name.to_lowercase();
    FUNCTIONS.iter().find(|f| f.name == name_lower)
}

/// Return all functions whose name starts with the given prefix (case-insensitive).
pub fn search_prefix(prefix: &str) -> impl Iterator<Item = &'static FunctionInfo> {
    let prefix_lower = prefix.to_lowercase();
    FUNCTIONS
        .iter()
        .filter(move |f| f.name.starts_with(&prefix_lower))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_finds_core_scalar() {
        let f = lookup("abs").expect("abs should exist");
        assert_eq!(f.name, "abs");
        assert_eq!(f.kind, FunctionKind::Scalar);
        assert!(
            !f.signatures.is_empty(),
            "abs should have at least one overload"
        );
        assert!(
            f.signatures.iter().all(|s| s.starts_with("abs(")),
            "every signature should start with the function name, got {:?}",
            f.signatures,
        );
    }

    #[test]
    fn lookup_is_case_insensitive() {
        assert!(lookup("ABS").is_some());
        assert!(lookup("Abs").is_some());
    }

    #[test]
    fn lookup_classifies_aggregate() {
        let f = lookup("sum").expect("sum should exist");
        assert_eq!(f.kind, FunctionKind::Aggregate);
    }

    #[test]
    fn lookup_classifies_table() {
        let f = lookup("generate_series").expect("generate_series should exist");
        assert_eq!(f.kind, FunctionKind::Table);
    }

    #[test]
    fn lookup_classifies_window() {
        let f = lookup("row_number").expect("row_number should exist");
        assert_eq!(f.kind, FunctionKind::Window);
    }

    #[test]
    fn lookup_finds_materialize_specific() {
        assert!(
            lookup("mz_now").is_some(),
            "mz_now missing — MZ_CATALOG_BUILTINS not wired in?"
        );
    }

    #[test]
    fn lookup_unknown_returns_none() {
        assert!(lookup("this_function_does_not_exist").is_none());
    }

    #[test]
    fn search_prefix_returns_sorted_unique_names() {
        let names: Vec<&str> = search_prefix("arr").map(|f| f.name.as_str()).collect();
        assert!(!names.is_empty(), "expected at least one 'arr*' function");
        assert!(
            names.iter().all(|n| n.starts_with("arr")),
            "non-matching name in prefix results: {:?}",
            names,
        );
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted, "prefix results not sorted: {:?}", names);
        sorted.dedup();
        assert_eq!(names.len(), sorted.len(), "duplicate names: {:?}", names);
    }

    #[test]
    fn signature_format_includes_return_type() {
        let f = lookup("lower").expect("lower should exist");
        assert!(
            f.signatures.iter().any(|s| s.contains(" -> ")),
            "expected ' -> ' in at least one signature, got {:?}",
            f.signatures,
        );
    }
}
