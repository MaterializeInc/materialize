// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use crate::{QuotedString, ToBazelDefinition};

/// A single rule in [`rules_rust`](http://bazelbuild.github.io/rules_rust/flatten.html).
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rule {
    RustLibrary,
    RustProcMacro,
    RustTest,
    RustDocTest,
    RustBinary,
    CargoBuildScript,
    // TODO(parkmycar): Include these rules. The tricky part is they need to
    // get imported from the crates_universe repository that is created.
    // Aliases,
    // AllCrateDeps,
}

impl Rule {
    pub fn module(&self) -> Module {
        match self {
            Rule::RustLibrary
            | Rule::RustProcMacro
            | Rule::RustTest
            | Rule::RustDocTest
            | Rule::RustBinary => Module::Rust,
            Rule::CargoBuildScript => Module::Cargo,
        }
    }
}

impl ToBazelDefinition for Rule {
    fn format(&self, writer: &mut dyn std::fmt::Write) -> Result<(), std::fmt::Error> {
        let s = match self {
            Rule::RustLibrary => "rust_library",
            Rule::RustProcMacro => "rust_proc_macro",
            Rule::RustTest => "rust_test",
            Rule::RustDocTest => "rust_doc_test",
            Rule::RustBinary => "rust_binary",
            Rule::CargoBuildScript => "cargo_build_script",
        };
        let s = QuotedString::new(s);
        s.format(writer)
    }
}

/// Module within [`rules_rust`](http://bazelbuild.github.io/rules_rust) that we import [`Rule`]s
/// from.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Module {
    Rust,
    Cargo,
}

impl ToBazelDefinition for Module {
    fn format(&self, writer: &mut dyn std::fmt::Write) -> Result<(), std::fmt::Error> {
        let s = match self {
            Module::Rust => "rust",
            Module::Cargo => "cargo",
        };
        write!(writer, "{s}")
    }
}

#[derive(Debug)]
pub struct LoadStatement {
    module: Module,
    rules: BTreeSet<Rule>,
}

impl ToBazelDefinition for LoadStatement {
    fn format(&self, writer: &mut dyn std::fmt::Write) -> Result<(), std::fmt::Error> {
        write!(
            writer,
            "load(\"@rules_rust//{}:defs.bzl\"",
            self.module.to_bazel_definition()
        )?;
        for rule in &self.rules {
            write!(writer, ", {}", rule.to_bazel_definition())?;
        }
        write!(writer, ")")?;

        Ok(())
    }
}

impl From<(Module, Vec<Rule>)> for LoadStatement {
    fn from(value: (Module, Vec<Rule>)) -> Self {
        let rules = value.1.into_iter().collect();
        LoadStatement {
            module: value.0,
            rules,
        }
    }
}
