// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPARQL 1.1 parser for Materialize.
//!
//! This crate provides a hand-rolled recursive-descent parser for SPARQL 1.1
//! queries. It produces an AST (defined in [`ast`]) that the SPARQL planner
//! (in the `mz-sparql` crate) translates into Materialize's HIR.
//!
//! # Usage
//!
//! ```ignore
//! use mz_sparql_parser::parser::parse;
//!
//! let query = parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")?;
//! ```
//!
//! # Crate structure
//!
//! - [`ast`]: SPARQL AST types (`SparqlQuery`, `GroupGraphPattern`, etc.)
//! - [`lexer`]: Tokenizer producing [`lexer::Token`]s from a query string
//! - [`parser`]: Recursive-descent parser producing [`ast::SparqlQuery`]

pub mod ast;
pub mod lexer;
pub mod parser;
