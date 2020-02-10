// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A driver for sqllogictest, a SQL correctness testing framework.
//!
//! sqllogictest is developed as part of the SQLite project.
//! Details can be found on the SQLite website:
//! <https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki>
//!
//! This crate implements a parser and runner for sqllogictest files.
//! The parser is generic, but the runner is specific to Materialize.

pub mod ast;
pub mod fuzz;
pub mod parser;
pub mod runner;
pub mod util;
