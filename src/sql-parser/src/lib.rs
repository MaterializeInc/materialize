// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. All rights reserved.
//
// This file is derived from the sqlparser-rs project, available at
// https://github.com/andygrove/sqlparser-rs. It was incorporated
// directly into Materialize on December 21, 2019.
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

//! SQL parser.
//!
//! This crate provides an SQL lexer and parser for Materialize's dialect of
//! SQL.
//!
//! ```
//! use sql_parser::parser;
//!
//! let sql = "SELECT a, b, 123, myfunc(b) \
//!            FROM table_1 \
//!            WHERE a > b AND b < 100 \
//!            ORDER BY a DESC, b";
//!
//! let ast = parser::parse_statements(sql.to_string()).unwrap();
//! println!("AST: {:?}", ast);
//! ```

pub mod ast;
pub mod parser;

mod keywords;
mod tokenizer;
