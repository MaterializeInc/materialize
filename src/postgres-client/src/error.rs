// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! [PostgresClient](crate::PostgresClient)-related errors.

use std::fmt;

/// An error coming from Postgres.
#[derive(Debug)]
pub enum PostgresError {
    /// A determinate error from Postgres.
    Determinate(anyhow::Error),
    /// An indeterminate error from Postgres.
    Indeterminate(anyhow::Error),
}

impl std::fmt::Display for PostgresError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PostgresError::Determinate(x) => std::fmt::Display::fmt(x, f),
            PostgresError::Indeterminate(x) => std::fmt::Display::fmt(x, f),
        }
    }
}

impl std::error::Error for PostgresError {}

/// An impl of PartialEq purely for convenience in tests and debug assertions.
#[cfg(any(test, debug_assertions))]
impl PartialEq for PostgresError {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl From<anyhow::Error> for PostgresError {
    fn from(inner: anyhow::Error) -> Self {
        PostgresError::Indeterminate(inner)
    }
}

impl From<std::io::Error> for PostgresError {
    fn from(x: std::io::Error) -> Self {
        PostgresError::Indeterminate(anyhow::Error::new(x))
    }
}

impl From<deadpool_postgres::tokio_postgres::Error> for PostgresError {
    fn from(e: deadpool_postgres::tokio_postgres::Error) -> Self {
        let code = match e.as_db_error().map(|x| x.code()) {
            Some(x) => x,
            None => return PostgresError::Indeterminate(anyhow::Error::new(e)),
        };
        match code {
            // Feel free to add more things to this allowlist as we encounter
            // them as long as you're certain they're determinate.
            &deadpool_postgres::tokio_postgres::error::SqlState::T_R_SERIALIZATION_FAILURE => {
                PostgresError::Determinate(anyhow::Error::new(e))
            }
            _ => PostgresError::Indeterminate(anyhow::Error::new(e)),
        }
    }
}

impl From<deadpool_postgres::PoolError> for PostgresError {
    fn from(x: deadpool_postgres::PoolError) -> Self {
        match x {
            // We have logic for turning a postgres Error into an ExternalError,
            // so use it.
            deadpool_postgres::PoolError::Backend(x) => PostgresError::from(x),
            x => PostgresError::Indeterminate(anyhow::Error::new(x)),
        }
    }
}

impl From<tokio::task::JoinError> for PostgresError {
    fn from(x: tokio::task::JoinError) -> Self {
        PostgresError::Indeterminate(anyhow::Error::new(x))
    }
}
