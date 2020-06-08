// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Auto-generation of the `visit` and `visit_mut` modules.
//!
//! The SQL AST is sufficiently complicated that maintaining an AST visitor by
//! hand is onerous. This build script parses the crate's `ast` module and
//! automatically generates the `visit` and `visit_mut` modules based on the
//! structs and enums in the AST.
//!
//! The approach is modeled after the approach used by the [`syn`] crate.
//! See: <https://github.com/dtolnay/syn/tree/master/codegen>
//!
//! Note that the desired structure of the `Visit` and `VisitMut` traits
//! precludes the use of a custom derive procedural macro. We need to consider
//! the entire AST at once to build the `Visit` and `VisitMut` traits, and
//! derive macros only allow you to see one struct at a time.
//!
//! The build script is split into modules with the hope that the `parse` and
//! `ir` modules can be reused in other crates that have similar automatic code
//! generation needs.

use std::env;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};

mod gen;
mod ir;
mod parse;

const AST_MOD: &str = "src/ast/mod.rs";

// TODO(benesch): it might be cleaner to instead include only the types
// reachable from `Statement`.
const IGNORED_TYPES: &[&str] = &[
    "AstFormatter",
    "DisplaySeparated",
    "EscapeSingleQuoteString",
    "FormatMode",
    "ValueError",
];

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").context("Cannot read OUT_DIR env var")?);
    let items = parse::parse_mod(AST_MOD)?;
    let ir = ir::analyze(&items, IGNORED_TYPES)?;
    let visit = gen::gen_visit(&ir);
    let visit_mut = gen::gen_visit_mut(&ir);
    fs::write(out_dir.join("visit.rs"), visit.to_string())?;
    fs::write(out_dir.join("visit_mut.rs"), visit_mut.to_string())?;
    Ok(())
}
