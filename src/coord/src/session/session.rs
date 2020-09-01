// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! All the state that is associated with a specific session.
//!
//! The primary docs for this module are in the parent [`sql::session`](super)
//! module docs. This module exists to make the top level more clear in terms of
//! what is actually exported vs. required internally.
//!
//! Client connections each get a new [`Session`], which is composed of
//! [`Var`](var::Var)s and the elements of prepared statements.

use std::collections::HashMap;

use anyhow::bail;

use repr::{Datum, Row, ScalarType};
use sql::plan::Params;

use crate::session::statement::{Portal, PreparedStatement};
use crate::session::transaction::TransactionStatus;
use crate::session::var::{ServerVar, SessionVar, Var};

const APPLICATION_NAME: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("application_name"),
    value: "",
    description: "Sets the application name to be reported in statistics and logs (PostgreSQL).",
};

const CLIENT_ENCODING: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("client_encoding"),
    value: "UTF8",
    description: "Sets the client's character set encoding (PostgreSQL).",
};

const DATABASE: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("database"),
    value: "materialize",
    description: "Sets the current database (CockroachDB).",
};

const DATE_STYLE: ServerVar<&str> = ServerVar {
    // DateStyle has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("DateStyle"),
    value: "ISO, MDY",
    description: "Sets the display format for date and time values (PostgreSQL).",
};

const EXTRA_FLOAT_DIGITS: ServerVar<&i32> = ServerVar {
    name: unicase::Ascii::new("extra_float_digits"),
    value: &3,
    description: "Adjusts the number of digits displayed for floating-point values (PostgreSQL).",
};

const INTEGER_DATETIMES: ServerVar<&bool> = ServerVar {
    name: unicase::Ascii::new("integer_datetimes"),
    value: &true,
    description: "Reports whether the server uses 64-bit-integer dates and times (PostgreSQL).",
};

const SEARCH_PATH: ServerVar<&[&str]> = ServerVar {
    name: unicase::Ascii::new("search_path"),
    value: &["mz_catalog", "pg_catalog", "public", "mz_temp"],
    description:
        "Sets the schema search order for names that are not schema-qualified (PostgreSQL).",
};

const SERVER_VERSION: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("server_version"),
    // Pretend to be Postgres v9.5.0, which is also what CockroachDB pretends to
    // be. Too new and some clients will emit a "server too new" warning. Too
    // old and some clients will fall back to legacy code paths. v9.5.0
    // empirically seems to be a good compromise.
    value: "9.5.0",
    description: "Shows the server version (PostgreSQL).",
};

const SQL_SAFE_UPDATES: ServerVar<&bool> = ServerVar {
    name: unicase::Ascii::new("sql_safe_updates"),
    value: &false,
    description: "Prohibits SQL statements that may be overly destructive (CockroachDB).",
};

const TIMEZONE: ServerVar<&str> = ServerVar {
    // TimeZone has nonstandard capitalization for historical reasons.
    name: unicase::Ascii::new("TimeZone"),
    value: "UTC",
    description: "Sets the time zone for displaying and interpreting time stamps (PostgreSQL).",
};

const TRANSACTION_ISOLATION: ServerVar<&str> = ServerVar {
    name: unicase::Ascii::new("transaction_isolation"),
    value: "serializable",
    description: "Sets the current transaction's isolation level (PostgreSQL).",
};

const DUMMY_CONNECTION_ID: u32 = 0;

/// A `Session` holds SQL state that is attached to a session.
#[derive(Debug)]
pub struct Session {
    application_name: SessionVar<str>,
    client_encoding: ServerVar<&'static str>,
    database: SessionVar<str>,
    date_style: ServerVar<&'static str>,
    extra_float_digits: SessionVar<i32>,
    integer_datetimes: ServerVar<&'static bool>,
    search_path: ServerVar<&'static [&'static str]>,
    server_version: ServerVar<&'static str>,
    sql_safe_updates: SessionVar<bool>,
    timezone: ServerVar<&'static str>,
    transaction_isolation: ServerVar<&'static str>,
    conn_id: u32,
    /// The current state of the the session's transaction
    transaction: TransactionStatus,
    /// A map from statement names to SQL queries
    prepared_statements: HashMap<String, PreparedStatement>,
    /// Portals associated with the current session
    ///
    /// Portals are primarily a way to retrieve the results for a query with all
    /// parameters bound.
    portals: HashMap<String, Portal>,
}

impl Session {
    /// Given a connection id, provides a new session with default values.
    pub fn new(conn_id: u32) -> Session {
        assert_ne!(conn_id, DUMMY_CONNECTION_ID);
        Session {
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: CLIENT_ENCODING,
            database: SessionVar::new(&DATABASE),
            date_style: DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            integer_datetimes: INTEGER_DATETIMES,
            search_path: SEARCH_PATH,
            server_version: SERVER_VERSION,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            timezone: TIMEZONE,
            transaction_isolation: TRANSACTION_ISOLATION,
            conn_id,
            transaction: TransactionStatus::Idle,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    /// Returns a Session using a DUMMY_CONNECTION_ID.
    /// NOTE: Keep this in sync with ::new()
    pub fn dummy() -> Session {
        Session {
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: CLIENT_ENCODING,
            database: SessionVar::new(&DATABASE),
            date_style: DATE_STYLE,
            extra_float_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            integer_datetimes: INTEGER_DATETIMES,
            search_path: SEARCH_PATH,
            server_version: SERVER_VERSION,
            sql_safe_updates: SessionVar::new(&SQL_SAFE_UPDATES),
            timezone: TIMEZONE,
            transaction_isolation: TRANSACTION_ISOLATION,
            conn_id: DUMMY_CONNECTION_ID,
            transaction: TransactionStatus::Idle,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
        }
    }

    /// Returns the connection id of a session
    pub fn conn_id(&self) -> u32 {
        self.conn_id
    }

    /// Returns all configuration parameters and their current values for this
    /// session.
    pub fn vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.application_name,
            &self.client_encoding,
            &self.database,
            &self.date_style,
            &self.extra_float_digits,
            &self.integer_datetimes,
            &self.search_path,
            &self.server_version,
            &self.sql_safe_updates,
            &self.timezone,
            &self.transaction_isolation,
        ]
    }

    /// Returns the configuration parameters (and their current values for this
    /// session) that are expected to be sent to the client when a new
    /// connection is established or when their value changes.
    pub fn notify_vars(&self) -> Vec<&dyn Var> {
        vec![
            &self.application_name,
            &self.client_encoding,
            &self.date_style,
            &self.server_version,
            &self.integer_datetimes,
        ]
    }

    /// Returns a [`Var`] representing the configuration parameter with the
    /// specified name.
    ///
    /// Configuration parameters are matched case insensitively. If no such
    /// configuration parameter exists, `get` returns an error.
    ///
    /// Note that if `name` is known at compile time, you should instead use the
    /// named accessor to access the variable with its true Rust type. For
    /// example, `self.get("sql_safe_updates").value()` returns the string
    /// `"true"` or `"false"`, while `self.sql_safe_updates()` returns a bool.
    pub fn get(&self, name: &str) -> Result<&dyn Var, anyhow::Error> {
        if name == APPLICATION_NAME.name {
            Ok(&self.application_name)
        } else if name == CLIENT_ENCODING.name {
            Ok(&self.client_encoding)
        } else if name == DATABASE.name {
            Ok(&self.database)
        } else if name == DATE_STYLE.name {
            Ok(&self.date_style)
        } else if name == EXTRA_FLOAT_DIGITS.name {
            Ok(&self.extra_float_digits)
        } else if name == INTEGER_DATETIMES.name {
            Ok(&self.integer_datetimes)
        } else if name == SEARCH_PATH.name {
            Ok(&self.search_path)
        } else if name == SERVER_VERSION.name {
            Ok(&self.server_version)
        } else if name == SQL_SAFE_UPDATES.name {
            Ok(&self.sql_safe_updates)
        } else if name == TIMEZONE.name {
            Ok(&self.timezone)
        } else if name == TRANSACTION_ISOLATION.name {
            Ok(&self.transaction_isolation)
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Sets the configuration parameter named `name` to the value represented
    /// by `value`.
    ///
    /// Like with [`Session::get`], configuration parameters are matched case
    /// insensitively. If `value` is not valid, as determined by the underlying
    /// configuration parameter, or if the named configuration parameter does
    /// not exist, an error is returned.
    pub fn set(&mut self, name: &str, value: &str) -> Result<(), anyhow::Error> {
        if name == APPLICATION_NAME.name {
            self.application_name.set(value)
        } else if name == CLIENT_ENCODING.name {
            bail!("parameter {} is read only", CLIENT_ENCODING.name);
        } else if name == DATABASE.name {
            self.database.set(value)
        } else if name == DATE_STYLE.name {
            bail!("parameter {} is read only", DATE_STYLE.name);
        } else if name == EXTRA_FLOAT_DIGITS.name {
            self.extra_float_digits.set(value)
        } else if name == INTEGER_DATETIMES.name {
            bail!("parameter {} is read only", INTEGER_DATETIMES.name);
        } else if name == SEARCH_PATH.name {
            bail!("parameter {} is read only", SEARCH_PATH.name);
        } else if name == SERVER_VERSION.name {
            bail!("parameter {} is read only", SERVER_VERSION.name);
        } else if name == SQL_SAFE_UPDATES.name {
            self.sql_safe_updates.set(value)
        } else if name == TIMEZONE.name {
            if unicase::Ascii::new(value) != TIMEZONE.value {
                bail!(
                    "parameter {} can only be set to {}",
                    TIMEZONE.name,
                    TIMEZONE.value
                );
            } else {
                Ok(())
            }
        } else if name == TRANSACTION_ISOLATION.name {
            bail!("parameter {} is read only", TRANSACTION_ISOLATION.name);
        } else {
            bail!("unknown parameter: {}", name)
        }
    }

    /// Returns the value of the `application_name` configuration parameter.
    pub fn application_name(&self) -> &str {
        self.application_name.value()
    }

    /// Returns the value of the `client_encoding` configuration parameter.
    pub fn client_encoding(&self) -> &'static str {
        self.client_encoding.value
    }

    /// Returns the value of the `DateStyle` configuration parameter.
    pub fn date_style(&self) -> &'static str {
        self.date_style.value
    }

    /// Returns the value of the `database` configuration parameter.
    pub fn database(&self) -> &str {
        self.database.value()
    }

    /// Returns the value of the `extra_float_digits` configuration parameter.
    pub fn extra_float_digits(&self) -> i32 {
        *self.extra_float_digits.value()
    }

    /// Returns the value of the `integer_datetimes` configuration parameter.
    pub fn integer_datetimes(&self) -> bool {
        *self.integer_datetimes.value
    }

    /// Returns the value of the `search_path` configuration parameter.
    pub fn search_path(&self) -> &'static [&'static str] {
        self.search_path.value
    }

    /// Returns the value of the `server_version` configuration parameter.
    pub fn server_version(&self) -> &'static str {
        self.server_version.value
    }

    /// Returns the value of the `sql_safe_updates` configuration parameter.
    pub fn sql_safe_updates(&self) -> bool {
        *self.sql_safe_updates.value()
    }

    /// Returns the value of the `timezone` configuration parameter.
    pub fn timezone(&self) -> &'static str {
        self.timezone.value
    }

    /// Returns the value of the `transaction_isolation` configuration
    /// parameter.
    pub fn transaction_isolation(&self) -> &'static str {
        self.transaction_isolation.value
    }

    /// Put the session into a transaction
    ///
    /// This does not nest, it just keeps us in a transaction even if we were already in
    /// one.
    pub fn start_transaction(&mut self) {
        self.transaction = TransactionStatus::InTransaction;
    }

    /// Take the session out of a transaction
    ///
    /// This is fine to do even if we are not in a transaction
    pub fn end_transaction(&mut self) {
        self.transaction = TransactionStatus::Idle;
    }

    /// If the session is currenlty in a transaction, mark it failed
    ///
    /// Does nothing in other cases
    pub fn fail_transaction(&mut self) {
        if self.transaction == TransactionStatus::InTransaction {
            self.transaction = TransactionStatus::Failed;
        }
    }

    /// Get the current transaction status of the session
    pub fn transaction(&self) -> &TransactionStatus {
        &self.transaction
    }

    /// Ensure that the given prepared statement is present in this session
    pub fn set_prepared_statement(&mut self, name: String, statement: PreparedStatement) {
        self.prepared_statements.insert(name, statement);
    }

    /// Removes the prepared statement associated with `name`. It is not an
    /// error if no such statement exists.
    pub fn remove_prepared_statement(&mut self, name: &str) {
        let _ = self.prepared_statements.remove(name);
    }

    /// Retrieve the prepared statement in this session associated with `name`
    pub fn get_prepared_statement(&self, name: &str) -> Option<&PreparedStatement> {
        self.prepared_statements.get(name)
    }

    /// Given a portal name, get the associated prepared statement
    pub fn get_prepared_statement_for_portal(
        &self,
        portal_name: &str,
    ) -> Option<&PreparedStatement> {
        self.portals
            .get(portal_name)
            .and_then(|portal| self.prepared_statements.get(&portal.statement_name))
    }

    /// Ensure that the given portal exists
    ///
    /// **Errors** if the statement name has not be set
    pub fn set_portal<'a>(
        &mut self,
        portal_name: String,
        statement_name: String,
        params: Vec<(Datum<'a>, ScalarType)>,
        result_formats: Vec<pgrepr::Format>,
    ) -> Result<(), anyhow::Error> {
        if !self.prepared_statements.contains_key(&statement_name) {
            bail!(
                "statement does not exist for portal creation: \
                 statement={:?} portal={:?}",
                statement_name,
                portal_name
            );
        }

        self.portals.insert(
            portal_name,
            Portal {
                statement_name,
                parameters: Params {
                    datums: Row::pack(params.iter().map(|(d, _t)| d)),
                    types: params.into_iter().map(|(_d, t)| t).collect(),
                },
                result_formats: result_formats.into_iter().map(Into::into).collect(),
                remaining_rows: None,
            },
        );
        Ok(())
    }

    /// Remove the portal, doing nothing if the portal does not exist
    pub fn remove_portal(&mut self, portal_name: &str) {
        let _ = self.portals.remove(portal_name);
    }

    /// Retrieve a portal by name
    pub fn get_portal(&self, portal_name: &str) -> Option<&Portal> {
        self.portals.get(portal_name)
    }

    /// Get a portal for mutation
    pub fn get_portal_mut(&mut self, portal_name: &str) -> Option<&mut Portal> {
        self.portals.get_mut(portal_name)
    }
}
