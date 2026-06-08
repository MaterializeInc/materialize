// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! IR representation of a parsed `EXECUTE UNIT TEST` statement.
//!
//! The compiler stores the parsed test definition on the project graph;
//! validation and SQL lowering happen later in the test runner (see
//! `cli::commands::test::lower`).

/// Represents a parsed unit test definition.
#[derive(Debug, Clone)]
pub struct UnitTest {
    /// Name of the test (e.g., "test_flippers")
    pub name: String,
    /// Fully qualified name of the target view being tested
    pub target_view: String,
    /// Optional timestamp for mz_now() during test execution
    pub at_time: Option<String>,
    /// Mock views to create for dependencies
    pub mocks: Vec<MockView>,
    /// Expected results definition
    pub expected: ExpectedResult,
}

impl UnitTest {
    /// Convert an ExecuteUnitTestStatement from the AST into a UnitTest.
    pub fn from_execute_statement(
        stmt: &mz_sql_parser::ast::ExecuteUnitTestStatement<mz_sql_parser::ast::Raw>,
    ) -> Self {
        use mz_sql_parser::ast::display::{AstDisplay, FormatMode};

        let name = stmt.name.to_string();
        let target_view = stmt.target.to_ast_string(FormatMode::Simple);

        let at_time = stmt
            .at_time
            .as_ref()
            .map(|expr| expr.to_ast_string(FormatMode::Simple));

        let mocks = stmt
            .mocks
            .iter()
            .map(|mock| {
                let fqn = mock.name.to_ast_string(FormatMode::Simple);
                let columns = mock
                    .columns
                    .iter()
                    .map(|col| {
                        (
                            col.name.to_string(),
                            col.data_type.to_ast_string(FormatMode::Simple),
                        )
                    })
                    .collect();
                let query = mock.query.to_ast_string(FormatMode::Simple);
                MockView {
                    fqn,
                    columns,
                    query,
                }
            })
            .collect();

        let expected = ExpectedResult {
            columns: stmt
                .expected
                .columns
                .iter()
                .map(|col| {
                    (
                        col.name.to_string(),
                        col.data_type.to_ast_string(FormatMode::Simple),
                    )
                })
                .collect(),
            query: stmt.expected.query.to_ast_string(FormatMode::Simple),
        };

        UnitTest {
            name,
            target_view,
            at_time,
            mocks,
            expected,
        }
    }
}

/// A mock view definition that replaces a real dependency.
#[derive(Debug, Clone)]
pub struct MockView {
    /// Fully qualified name (e.g., "materialize.public.flipper_activity")
    pub fqn: String,
    /// Column definitions as (name, type) pairs
    pub columns: Vec<(String, String)>,
    /// SQL query body (the part after AS)
    pub query: String,
}

/// Expected results for the test.
#[derive(Debug, Clone)]
pub struct ExpectedResult {
    /// Column definitions as (name, type) pairs
    pub columns: Vec<(String, String)>,
    /// SQL query body (the part after AS)
    pub query: String,
}
