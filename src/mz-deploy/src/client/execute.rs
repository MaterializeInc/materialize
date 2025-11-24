//! SQL execution API for running queries and statements.
//!
//! This module provides a thin wrapper around the tokio-postgres client
//! for executing SQL statements and queries. It converts low-level database
//! errors into the application's ConnectionError type.
//!
//! # Usage
//!
//! The ExecuteApi is typically accessed through the Client's `pg_client()` method:
//!
//! ```no_run
//! # async fn example(client: &mz_deploy::client::Client) -> Result<(), mz_deploy::client::ConnectionError> {
//! let execute = client.pg_client();
//! let rows = execute.query("SELECT * FROM mz_tables", &[]).await?;
//! # Ok(())
//! # }
//! ```

use super::connection::ConnectionError;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client as PgClient, Row, ToStatement};

/// API for executing SQL statements and queries.
///
/// This struct wraps a tokio-postgres client and provides methods for
/// executing SQL with proper error handling.
pub struct ExecuteApi<'a> {
    client: &'a PgClient,
}

impl<'a> ExecuteApi<'a> {
    /// Create a new execute API wrapping a PostgreSQL client.
    pub fn new(client: &PgClient) -> ExecuteApi<'_> {
        ExecuteApi { client }
    }

    /// Execute a SQL statement that doesn't return rows.
    ///
    /// Returns the number of rows affected by the statement.
    ///
    /// # Arguments
    /// * `statement` - SQL statement to execute (can be a string or prepared statement)
    /// * `params` - Parameters to bind to the statement
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use mz_deploy::client::{Client, ConnectionError};
    /// # async fn example(client: &Client) -> Result<(), ConnectionError> {
    /// // Execute a CREATE TABLE statement
    /// client.pg_client().execute("CREATE TABLE foo (id INT)", &[]).await?;
    ///
    /// // Execute with parameters
    /// let name = "bar";
    /// client.pg_client().execute("DROP VIEW IF EXISTS $1", &[&name]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .execute(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    ///
    /// # Arguments
    /// * `statement` - SQL query to execute (can be a string or prepared statement)
    /// * `params` - Parameters to bind to the query
    ///
    /// # Returns
    /// A vector of rows returned by the query. Each row can be accessed by column
    /// name or index.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use mz_deploy::client::{Client, ConnectionError};
    /// # async fn example(client: &Client) -> Result<(), ConnectionError> {
    /// // Query all databases
    /// let rows = client.pg_client().query("SELECT name FROM mz_databases", &[]).await?;
    /// for row in rows {
    ///     let name: String = row.get("name");
    ///     println!("Database: {}", name);
    /// }
    ///
    /// // Query with parameters
    /// let db_name = "materialize";
    /// let rows = client.pg_client().query(
    ///     "SELECT * FROM mz_schemas WHERE database_id = (SELECT id FROM mz_databases WHERE name = $1)",
    ///     &[&db_name]
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }
}
