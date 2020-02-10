// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! PreparedStatements maintain in-progress state during a session
//!
//! In postgres there are two ways to construct prepared statements:
//!
//! * Via an explicit, user-provided `PREPARE <name> AS <sql>` sql statement
//! * As part of the postgres frontend/backend protocol, where prepared statements are
//!   created implicitly by client libraries on behalf of users
//!
//! We do not currently actually support explicit prepared statements, all prepared
//! statements come in via the postgres wire protocol as [`Parse`/`Bind` messages][m] in
//! the [extended query flow][eqf].
//!
//! What that means is that there is a multi-step dance to use prepared statements:
//!
//! 1. Receive a `Parse` message. `Parse` messages included a _name_ for the prepared
//!    statement, in addition to some other possible metadata.
//! 2. After validation, we stash the statement in the [`crate::session::Session`]
//!    associated with the current user's session.
//! 3. The client issues a `Bind` message, which provides a name for a portal, and
//!    associates that name with a previously-named prepared statement. This is the point
//!    at which all possible parameters are associated with the statement, there are no
//!    longer any free variables permited.
//! 4. The client issues an `Execute` message with the name of a portal, causing that
//!    portal to actually start scanning and returning results.
//!
//! The upshot of this is that we need to store arbitrary named Sessions and Portals
//! inside individual Sessions, and this module provides the types that are responsible
//! for maintaining the state provided by the various parts of the dance.
//!
//! [eqf]: https://www.postgresql.org/docs/12/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
//! [m]: https://www.postgresql.org/docs/12/protocol-message-formats.html#Parse

use crate::Params;
use repr::{RelationDesc, Row};

/// A prepared statement.
#[derive(Debug)]
pub struct PreparedStatement {
    sql: Option<sql_parser::ast::Statement>,
    desc: Option<RelationDesc>,
    param_types: Vec<pgrepr::Type>,
}

impl PreparedStatement {
    /// Constructs a new `PreparedStatement`.
    pub fn new(
        sql: Option<sql_parser::ast::Statement>,
        desc: Option<RelationDesc>,
        param_types: Vec<pgrepr::Type>,
    ) -> PreparedStatement {
        PreparedStatement {
            sql,
            desc,
            param_types,
        }
    }

    /// Returns the raw SQL string associated with this prepared statement,
    /// if the prepared statement was not the empty query.
    pub fn sql(&self) -> Option<&sql_parser::ast::Statement> {
        self.sql.as_ref()
    }

    /// Returns the type of the rows that will be returned by this prepared
    /// statement, if this prepared statement will return rows at all.
    pub fn desc(&self) -> Option<&RelationDesc> {
        self.desc.as_ref()
    }

    /// Returns the types of any parameters in this prepared statement.
    pub fn param_types(&self) -> &[pgrepr::Type] {
        &self.param_types
    }

    /// Reports the number of columns in the statement's result set, or zero if
    /// the statement does not return rows.
    pub fn result_width(&self) -> usize {
        self.desc
            .as_ref()
            .map(|desc| desc.typ().column_types.len())
            .unwrap_or(0)
    }
}

/// A portal represents the execution state of a running or runnable query.
#[derive(Debug)]
pub struct Portal {
    /// The name of the prepared statement that is bound to this portal.
    pub statement_name: String,
    /// The bound values for the parameters in the prepared statement, if any.
    pub parameters: Params,
    /// The desired output format for each column in the result set.
    pub result_formats: Vec<pgrepr::Format>,
    /// The rows that have yet to be delivered to the client, if the portal is
    /// partially executed.
    pub remaining_rows: Option<Vec<Row>>,
}

impl Portal {
    pub fn set_remaining_rows(&mut self, rows: Vec<Row>) {
        self.remaining_rows = Some(rows);
    }
}
