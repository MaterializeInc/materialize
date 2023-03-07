// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL parsing.

pub use mz_sql_parser::parser::parse_statements as parse;
pub use mz_sql_parser::parser::parse_statements_with_limit as parse_with_limit;
