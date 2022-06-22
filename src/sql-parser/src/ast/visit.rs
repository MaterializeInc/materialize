// Copyright Syn Developers.
// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// This file is derived from the syn project, available at
// https://github.com/dtolnay/syn. It was incorporated
// directly into Materialize on June 8, 2020.
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

//! Traversal of an immutable AST.
//!
//! Each method of the [`Visit`] trait is a hook that can be overridden to
//! customize the behavior when visiting the corresponding type of node. By
//! default, every method recursively visits the substructure of the input
//! by invoking the right visitor method of each of its fields.
//!
//! ```
//! # use mz_sql_parser::ast::{Expr, Function, FunctionArgs, UnresolvedObjectName, WindowSpec, Raw, AstInfo};
//! #
//! pub trait Visit<'ast, T: AstInfo> {
//!     /* ... */
//!
//!     fn visit_function(&mut self, node: &'ast Function<T>) {
//!         visit_function(self, node);
//!     }
//!
//!     /* ... */
//!     # fn visit_unresolved_object_name(&mut self, node: &'ast UnresolvedObjectName);
//!     # fn visit_function_args(&mut self, node: &'ast FunctionArgs<T>);
//!     # fn visit_expr(&mut self, node: &'ast Expr<T>);
//!     # fn visit_window_spec(&mut self, node: &'ast WindowSpec<T>);
//! }
//!
//! pub fn visit_function<'ast, V, T: AstInfo>(visitor: &mut V, node: &'ast Function<T>)
//! where
//!     V: Visit<'ast, T> + ?Sized,
//! {
//!     visitor.visit_unresolved_object_name(&node.name);
//!     visitor.visit_function_args(&node.args);
//!     if let Some(filter) = &node.filter {
//!         visitor.visit_expr(&*filter);
//!     }
//!     if let Some(over) = &node.over {
//!         visitor.visit_window_spec(over);
//!     }
//! }
//! ```
//!
//! See also the [`visit_mut`] module for traversing mutable ASTs.
//!
//! # Examples
//!
//! This visitor will count the number of subqueries in a SQL statement.
//!
//! ```
//! use std::error::Error;
//!
//! use mz_sql_parser::ast::{AstInfo, Query, Raw};
//! use mz_sql_parser::ast::visit::{self, Visit};
//!
//! struct SubqueryCounter {
//!     count: usize,
//! }
//!
//! impl<'ast> Visit<'ast, Raw> for SubqueryCounter {
//!     fn visit_query(&mut self, query: &'ast Query<Raw>) {
//!         self.count += 1;
//!
//!         // Delegate to the default implementation to visit any nested
//!         // subqueries. Placing this call at the end of the method results
//!         // in a pre-order traversal. Place it at the beginning for a
//!         // post-order traversal instead.
//!         visit::visit_query(self, query);
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let sql = "SELECT (SELECT 1) FROM (SELECT 1) WHERE EXISTS (SELECT (SELECT 1))";
//!     let stmts = mz_sql_parser::parser::parse_statements(sql.into())?;
//!
//!     let mut counter = SubqueryCounter { count: 0 };
//!     for stmt in &stmts {
//!         counter.visit_statement(stmt);
//!     }
//!     assert_eq!(counter.count, 5);
//!     Ok(())
//! }
//! ```
//!
//! The `'ast` lifetime on the input references means that the syntax tree
//! outlives the complete recursive visit call, so the visitor is allowed to
//! hold on to references into the syntax tree.
//!
//! ```
//! use std::error::Error;
//!
//! use mz_sql_parser::ast::{Ident, Raw, AstInfo, RawObjectName};
//! use mz_sql_parser::ast::visit::{self, Visit};
//!
//! struct IdentCollector<'ast> {
//!     idents: Vec<&'ast Ident>,
//! }
//!
//! impl<'ast> Visit<'ast, Raw> for IdentCollector<'ast> {
//!     fn visit_ident(&mut self, node: &'ast Ident) {
//!         self.idents.push(node);
//!         visit::visit_ident(self, node);
//!     }
//!     fn visit_object_name(&mut self, name: &'ast <Raw as AstInfo>::ObjectName) {
//!         match name {
//!             RawObjectName::Name(n) | RawObjectName::Id(_, n) => {
//!                 for node in &n.0 {
//!                     self.idents.push(node);
//!                     visit::visit_ident(self, node);
//!                 }
//!             }
//!         }
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let sql = "SELECT a FROM b.c WHERE 1 + d(e)";
//!     let stmts = mz_sql_parser::parser::parse_statements(sql.into())?;
//!
//!     let mut collector = IdentCollector { idents: vec![] };
//!     for stmt in &stmts {
//!         collector.visit_statement(stmt);
//!     }
//!     assert_eq!(collector.idents, &[
//!         &Ident::new("a"), &Ident::new("b"), &Ident::new("c"),
//!         &Ident::new("d"), &Ident::new("e"),
//!     ]);
//!     Ok(())
//! }
//! ```
//!
//! The [`VisitNode`] trait is implemented for every node in the AST and can be
//! used to write generic functions that apply a `Visit` implementation to any
//! node in the AST.
//!
//! # Implementation notes
//!
//! This module is automatically generated by the crate's build script. Changes
//! to the AST will be automatically propagated to the visitor.
//!
//! This approach to AST visitors is inspired by the [`syn`] crate. These
//! module docs are directly derived from the [`syn::visit`] module docs.
//!
//! [`syn`]: https://docs.rs/syn/1.*/syn/index.html
//! [`syn::visit`]: https://docs.rs/syn/1.*/syn/visit/index.html

#![allow(clippy::all)]
#![allow(unused_variables)]

use super::*;

include!(concat!(env!("OUT_DIR"), "/visit.rs"));
