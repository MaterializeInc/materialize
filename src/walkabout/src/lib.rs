// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Visitor generation for Rust structs and enums.
//!
//! Usage documentation is a work in progress, but for an example of the
//! generated visitor, see the [`sqlparser::ast::visit`] module.
//!
//! Some of our ASTs, which we represent with a tree of Rust structs and enums,
//! are sufficiently complicated that maintaining a visitor by hand is onerous.
//! This crate provides a generalizable framework for parsing Rust struct and
//! enum definitions from source code and automatically generating tree
//! traversal ("visitor") code.
//!
//! Note that the desired structure of the `Visit` and `VisitMut` traits
//! precludes the use of a custom derive procedural macro. We need to consider
//! the entire AST at once to build the `Visit` and `VisitMut` traits, and
//! derive macros only allow you to see one struct at a time.
//!
//! The design of the visitors is modeled after the visitors provided by the
//! [`syn`] crate. See: <https://github.com/dtolnay/syn/tree/master/codegen>
//!
//! The name of this crate is an homage to CockroachDB's
//! [Go package of the same name][crdb-walkabout].
//!
//! [`sqlparser::ast::visit`]: ../sql_parser/ast/visit/
//! [crdb-walkabout]: https://github.com/cockroachdb/walkabout

use std::path::Path;

use anyhow::Result;

mod gen;
mod parse;

pub mod ir;

pub use gen::gen_visit;
pub use gen::gen_visit_mut;

/// Loads type definitions from the specified module.
///
/// Returns an intermediate representation (IR) that can be fed to the
/// generation functions, like [`gen_visit`].
///
/// Note that parsing a Rust module is complicated. While most of the heavy
/// lifting is performed by [`syn`], syn does not understand the various options
/// for laying out a crate—and there are many attributes and edition settings
/// that control how modules can be laid out on the file system. This function
/// does not attempt to be fully general and only handles the file layout
/// currently required by Materialize.
///
/// Analyzing Rust types is also complicated. This function only handles basic
/// Rust containers, like [`Option`] and [`Vec`]. It does, however, endeavor to
/// produce understandable error messages when it encounters a type it does not
/// know how to handle.
pub fn load<P>(path: P) -> Result<ir::Ir>
where
    P: AsRef<Path>,
{
    let items = parse::parse_mod(path)?;
    ir::analyze(&items)
}
