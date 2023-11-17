// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(unknown_lints)]
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![allow(clippy::drain_collect)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
#![warn(clippy::large_futures)]
// END LINT CONFIG

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

pub use gen::{gen_fold, gen_visit, gen_visit_mut};

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
