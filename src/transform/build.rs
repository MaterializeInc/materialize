// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compile-time code generation for the eqsat rewrite rules.
//!
//! `src/eqsat/rules/relational.rewrite` and `src/eqsat/rules/scalar.rewrite`
//! are the single source of truth for the equality-saturation rewrite rules,
//! split by the sort (relational vs. scalar) of the rule's left-hand-side
//! root. This build script parses both with a [`chumsky`] grammar, concatenates
//! the rule lists, and emits Rust source into `$OUT_DIR/eqsat_rules.rs`, which
//! `src/eqsat/rules.rs` includes. Nothing parses the rule files at run time:
//! the generated code is the rule engine.
//!
//! The AST types ([`Rule`], [`Pat`], [`Tmpl`], …) are shared with the crate by
//! `include!`ing `src/eqsat/dsl.rs` below, so the grammar, the codegen, and the
//! run-time engine all agree on one definition.

use std::env;
use std::fs;
use std::path::Path;

// The rewrite DSL AST, shared verbatim with the crate (`crate::eqsat::dsl`).
// Pointing a module at the real source keeps a single definition of the AST
// that the grammar below, the codegen, and the run-time engine all agree on.
#[allow(dead_code)]
#[path = "src/eqsat/dsl.rs"]
mod dsl;

#[path = "build/codegen.rs"]
mod codegen;
#[path = "build/grammar.rs"]
mod grammar;

fn main() {
    let manifest = env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR set by cargo");
    let rules_path = Path::new(&manifest).join("src/eqsat/rules/relational.rewrite");
    println!("cargo:rerun-if-changed={}", rules_path.display());
    println!("cargo:rerun-if-changed=src/eqsat/dsl.rs");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=build/codegen.rs");
    println!("cargo:rerun-if-changed=build/grammar.rs");

    let src = fs::read_to_string(&rules_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", rules_path.display()));

    let rules = grammar::parse(&src).unwrap_or_else(|errs| {
        let mut msg = format!("failed to parse {}:\n", rules_path.display());
        for e in errs {
            msg.push_str(&format!("  {e}\n"));
        }
        panic!("{msg}");
    });

    let scalar_path = Path::new(&manifest).join("src/eqsat/rules/scalar.rewrite");
    println!("cargo:rerun-if-changed={}", scalar_path.display());
    let scalar_src = fs::read_to_string(&scalar_path)
        .unwrap_or_else(|e| panic!("reading {}: {e}", scalar_path.display()));
    let scalar_rules = grammar::parse(&scalar_src).unwrap_or_else(|errs| {
        let mut msg = format!("failed to parse {}:\n", scalar_path.display());
        for e in errs {
            msg.push_str(&format!("  {e}\n"));
        }
        panic!("{msg}");
    });

    let mut rules = rules;
    rules.extend(scalar_rules);

    let generated = codegen::emit(&rules);

    let out_dir = env::var("OUT_DIR").expect("OUT_DIR set by cargo");
    let out_path = Path::new(&out_dir).join("eqsat_rules.rs");
    fs::write(&out_path, generated)
        .unwrap_or_else(|e| panic!("writing {}: {e}", out_path.display()));
}
