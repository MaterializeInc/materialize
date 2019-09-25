// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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

mod postgres;
