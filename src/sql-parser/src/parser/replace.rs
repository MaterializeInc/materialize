// Copyright 2018 sqlparser-rs contributors. All rights reserved.
// Copyright Materialize, Inc. and contributors. All rights reserved.
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

//! SQL Parser for REPLACE statements.

use mz_sql_lexer::keywords::*;
use mz_sql_lexer::lexer::Token;

use crate::ast::*;
use crate::parser::IsOptional::Optional;
use crate::parser::{Parser, ParserError, ParserStatementError, ParserStatementErrorMapper};

impl<'a> Parser<'a> {
    /// Parse a SQL CREATE REPLACEMENT statement
    pub(super) fn parse_create_replace(&mut self) -> Result<Statement<Raw>, ParserStatementError> {
        self.expect_one_of_keywords(&[REPLACE, REPLACEMENT])
            .map_no_statement_parser_err()?;
        let name = self.parse_item_name().map_no_statement_parser_err()?;
        self.expect_keyword(FOR).map_no_statement_parser_err()?;
        if self.peek_keywords(&[MATERIALIZED, VIEW]) {
            self.parse_replace_materialized_view(name)
                .map_parser_err(StatementKind::CreateReplacementMaterializedView)
        } else {
            let expected_msg = { "MATERIALIZED VIEW after REPLACEMENT .. FOR" };
            self.expected(self.peek_pos(), expected_msg, self.peek_token())
                .map_no_statement_parser_err()
        }
    }

    fn parse_replace_materialized_view(
        &mut self,
        name: UnresolvedItemName,
    ) -> Result<Statement<Raw>, ParserError> {
        self.expect_keywords(&[MATERIALIZED, VIEW])?;

        let target_name = self.parse_raw_name()?;
        let columns = self.parse_parenthesized_column_list(Optional)?;
        let in_cluster = self.parse_optional_in_cluster()?;

        let with_options = if self.parse_keyword(WITH) {
            self.expect_token(&Token::LParen)?;
            let options = self.parse_comma_separated(Parser::parse_materialized_view_option)?;
            self.expect_token(&Token::RParen)?;
            options
        } else {
            vec![]
        };

        self.expect_keyword(AS)?;
        let query = self.parse_query()?;
        let as_of = self.parse_optional_internal_as_of()?;

        Ok(Statement::CreateReplacementMaterializedView(
            CreateReplacementMaterializedViewStatement {
                name,
                target_name,
                columns,
                in_cluster,
                query,
                as_of,
                with_options,
            },
        ))
    }
}
