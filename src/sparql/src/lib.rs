// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SPARQL 1.1 query planner for Materialize.
//!
//! This crate translates a parsed SPARQL query ([`mz_sparql_parser::ast::SparqlQuery`])
//! into a [`mz_sql::plan::HirRelationExpr`], which is the high-level intermediate
//! representation used by the Materialize SQL planner. The HIR is subsequently
//! lowered to MIR and optimized by the existing pipeline.
//!
//! # Architecture
//!
//! The SPARQL planner assumes a single "quad table" with schema
//! `(subject TEXT, predicate TEXT, object TEXT, graph TEXT)`. Each SPARQL triple
//! pattern compiles to a scan of this table with filters on bound positions.
//! Multi-pattern BGPs produce inner joins with equality on shared variables.
//!
//! The planner is built incrementally:
//! - Prompt 7: BGP planning (this)
//! - Prompt 8: FILTER, OPTIONAL, UNION, MINUS
//! - Prompt 9: BIND, VALUES, expressions, type coercions
//! - Prompt 10: SELECT projection, aggregates, solution modifiers
//! - Prompt 11: CONSTRUCT, ASK, DESCRIBE
//! - Prompt 12: Property paths with LetRec

pub mod plan;
