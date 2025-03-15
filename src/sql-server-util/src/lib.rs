// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::borrow::Cow;
use std::future::IntoFuture;

use anyhow::Context;
use derivative::Derivative;
use futures::future::BoxFuture;
use futures::FutureExt;
use smallvec::SmallVec;
use tiberius::ToSql;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub mod config;
pub mod desc;
pub mod inspect;

pub use config::Config;

use crate::config::TunnelConfig;

/// Higher level wrapper around a [`tiberius::Client`] that models transaction
/// management like other database clients.
///
/// When creating a [`Client`] we return a [`Connection`] which implements [`std::future::Future`]
/// and must be polled for queries to make progress. Internally a [`Client`] holds the sending side
/// of a channel and the [`Connection`] receives query requests to run. This enables us to
/// introduce a [`Transaction`] type that when dropped will cause the `TRANSACTION` in the
/// connected SQL Server instance to get rolled back.
#[derive(Debug)]
pub struct Client {
    tx: UnboundedSender<Request>,
}
// While a Client could implement Clone, it's not obvious how multiple Clients
// using the same SQL Server connection would interact, so ban it for now.
static_assertions::assert_not_impl_all!(Client: Clone);

impl Client {
    /// Connect to the specified SQL Server instance, returning a [`Client`]
    /// that can be used to query it and a [`Connection`] that must be polled
    /// to send and receive results.
    ///
    /// TODO(sql_server2): Maybe return a `ClientBuilder` here that implements
    /// IntoFuture and does the default good thing of moving the `Connection`
    /// into a tokio task? And a `.raw()` option that will instead return both
    /// the Client and Connection for manual polling.
    pub async fn connect(config: Config) -> Result<(Self, Connection), SqlServerError> {
        // Setup our tunnelling and return any resources that need to be kept
        // alive for the duration of the connection.
        let (tcp, resources): (_, Option<Box<dyn Any + Send + Sync>>) = match &config.tunnel {
            TunnelConfig::Direct => {
                let tcp = TcpStream::connect(config.inner.get_addr())
                    .await
                    .context("direct")?;
                (tcp, None)
            }
            TunnelConfig::Ssh {
                config: ssh_config,
                manager,
                timeout,
                host,
                port,
            } => {
                // N.B. If this tunnel is dropped it will close so we need to
                // keep it alive for the duration of the connection.
                let tunnel = manager
                    .connect(ssh_config.clone(), &host, *port, *timeout, config.in_task)
                    .await?;
                let tcp = TcpStream::connect(tunnel.local_addr())
                    .await
                    .context("ssh tunnel")?;

                (tcp, Some(Box::new(tunnel)))
            }
            TunnelConfig::AwsPrivatelink { connection_id: _ } => {
                // TODO(sql_server1): Getting this right is tricky because
                // there is some subtle logic with hostname validation.
                return Err(SqlServerError::Generic(anyhow::anyhow!(
                    "TODO(sql_server1): Support PrivateLink connections"
                )));
            }
        };

        tcp.set_nodelay(true)?;
        Self::connect_raw(config, tcp, resources).await
    }

    pub async fn connect_raw(
        config: Config,
        tcp: tokio::net::TcpStream,
        resources: Option<Box<dyn Any + Send + Sync>>,
    ) -> Result<(Self, Connection), SqlServerError> {
        let client = tiberius::Client::connect(config.inner, tcp.compat_write()).await?;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        // TODO(sql_server2): Add a lot more logging here like the Postgres and MySQL clients have.

        Ok((
            Client { tx },
            Connection {
                rx,
                client,
                resources,
            },
        ))
    }

    /// Executes SQL statements in SQL Server, returning the number of rows effected.
    ///
    /// Passthrough method for [`tiberius::Client::execute`].
    ///
    /// Note: The returned [`Future`] does not need to be awaited for the query
    /// to be sent.
    ///
    /// [`Future`]: std::future::Future
    pub async fn execute<'a>(
        &mut self,
        query: impl Into<Cow<'a, str>>,
        params: &[&dyn ToSql],
    ) -> Result<SmallVec<[u64; 1]>, SqlServerError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let params = params
            .iter()
            .map(|p| OwnedColumnData::from(p.to_sql()))
            .collect();
        let kind = RequestKind::Execute {
            query: query.into().to_string(),
            params,
        };
        self.tx
            .send(Request { tx, kind })
            .context("sending request")?;

        let response = rx.await.context("channel")??;
        match response {
            Response::Execute { rows_affected } => Ok(rows_affected),
            other @ Response::Rows(_) => Err(SqlServerError::ProgrammingError(format!(
                "expected Response::Execute, got {other:?}"
            ))),
        }
    }

    /// Executes SQL statements in SQL Server, returning the resulting rows.
    ///
    /// Passthrough method for [`tiberius::Client::query`].
    ///
    /// Note: The returned [`Future`] does not need to be awaited for the query
    /// to be sent.
    ///
    /// [`Future`]: std::future::Future
    pub async fn query<'a>(
        &mut self,
        query: impl Into<Cow<'a, str>>,
        params: &[&dyn tiberius::ToSql],
    ) -> Result<SmallVec<[tiberius::Row; 1]>, SqlServerError> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        let params = params
            .iter()
            .map(|p| OwnedColumnData::from(p.to_sql()))
            .collect();
        let kind = RequestKind::Query {
            query: query.into().to_string(),
            params,
        };
        self.tx
            .send(Request { tx, kind })
            .context("sending request")?;

        let response = rx.await.context("channel")??;
        match response {
            Response::Rows(rows) => Ok(rows),
            other @ Response::Execute { .. } => Err(SqlServerError::ProgrammingError(format!(
                "expected Response::Rows, got {other:?}"
            ))),
        }
    }

    /// Executes multiple queries, delimited with `;` and return multiple
    /// result sets; one for each query.
    ///
    /// Passthrough method for [`tiberius::Client::simple_query`].
    ///
    /// Note: The returned [`Future`] does not need to be awaited for the query
    /// to be sent.
    ///
    /// [`Future`]: std::future::Future
    pub async fn simple_query<'a>(
        &mut self,
        query: impl Into<Cow<'a, str>>,
    ) -> Result<SmallVec<[tiberius::Row; 1]>, SqlServerError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let kind = RequestKind::SimpleQuery {
            query: query.into().to_string(),
        };
        self.tx
            .send(Request { tx, kind })
            .context("sending request")?;

        let response = rx.await.context("channel")??;
        match response {
            Response::Rows(rows) => Ok(rows),
            other @ Response::Execute { .. } => Err(SqlServerError::ProgrammingError(format!(
                "expected Response::Rows, got {other:?}"
            ))),
        }
    }

    /// Starts a transaction which is automatically rolled back on drop.
    ///
    /// To commit or rollback the transaction, see [`Transaction::commit`] and
    /// [`Transaction::rollback`] respectively.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, SqlServerError> {
        Transaction::new(self).await
    }

    /// Sets the transaction isolation level for the current session.
    pub async fn set_transaction_isolation(
        &mut self,
        level: TransactionIsolationLevel,
    ) -> Result<(), SqlServerError> {
        let query = format!("SET TRANSACTION ISOLATION LEVEL {}", level.as_str());
        self.simple_query(query).await?;
        Ok(())
    }

    /// Returns the current transaction isolation level for the current session.
    pub async fn get_transaction_isolation(
        &mut self,
    ) -> Result<TransactionIsolationLevel, SqlServerError> {
        const QUERY: &str = "SELECT transaction_isolation_level FROM sys.dm_exec_sessions where session_id = @@SPID;";
        let rows = self.simple_query(QUERY).await?;
        match &rows[..] {
            [row] => {
                let val: i16 = row
                    .try_get(0)
                    .context("getting 0th column")?
                    .ok_or_else(|| anyhow::anyhow!("no 0th column?"))?;
                let level = TransactionIsolationLevel::try_from_sql_server(val)?;
                Ok(level)
            }
            other => Err(SqlServerError::InvariantViolated(format!(
                "expected one row, got {other:?}"
            ))),
        }
    }
}

#[derive(Debug)]
pub struct Transaction<'a> {
    client: &'a mut Client,
}

impl<'a> Transaction<'a> {
    async fn new(client: &'a mut Client) -> Result<Self, SqlServerError> {
        let results = client
            .simple_query("BEGIN TRANSACTION")
            .await
            .context("begin")?;
        if !results.is_empty() {
            Err(SqlServerError::InvariantViolated(format!(
                "expected empty result from BEGIN TRANSACTION. Got: {results:?}"
            )))
        } else {
            Ok(Transaction { client })
        }
    }

    /// See [`Client::execute`].
    pub async fn execute<'q>(
        &mut self,
        query: impl Into<Cow<'q, str>>,
        params: &[&dyn ToSql],
    ) -> Result<SmallVec<[u64; 1]>, SqlServerError> {
        self.client.execute(query, params).await
    }

    /// See [`Client::query`].
    pub async fn query<'q>(
        &mut self,
        query: impl Into<Cow<'q, str>>,
        params: &[&dyn tiberius::ToSql],
    ) -> Result<SmallVec<[tiberius::Row; 1]>, SqlServerError> {
        self.client.query(query, params).await
    }

    /// See [`Client::simple_query`].
    pub async fn simple_query<'q>(
        &mut self,
        query: impl Into<Cow<'q, str>>,
    ) -> Result<SmallVec<[tiberius::Row; 1]>, SqlServerError> {
        self.client.simple_query(query).await
    }

    /// Rollback the [`Transaction`].
    pub async fn rollback(self) -> Result<(), SqlServerError> {
        static ROLLBACK_QUERY: &str = "ROLLBACK TRANSACTION";
        self.client.execute(ROLLBACK_QUERY, &[]).await?;
        Ok(())
    }

    /// Commit the [`Transaction`].
    pub async fn commit(self) -> Result<(), SqlServerError> {
        static COMMIT_QUERY: &str = "COMMIT TRANSACTION";
        self.client.execute(COMMIT_QUERY, &[]).await?;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Internally the query is synchronously sent down a channel, and the response is what
        // we await. In other words, we don't need to `.await` here for the query to be run.
        let _fut = self.client.simple_query("ROLLBACK TRANSACTION");
    }
}

/// Transaction isolation levels defined by Microsoft's SQL Server.
///
/// See: <https://learn.microsoft.com/en-us/sql/t-sql/statements/set-transaction-isolation-level-transact-sql>
#[derive(Debug, PartialEq, Eq)]
pub enum TransactionIsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Snapshot,
    Serializable,
}

impl TransactionIsolationLevel {
    /// Return the string representation of a transaction isolation level.
    fn as_str(&self) -> &'static str {
        match self {
            TransactionIsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            TransactionIsolationLevel::ReadCommitted => "READ COMMITTED",
            TransactionIsolationLevel::RepeatableRead => "REPEATABLE READ",
            TransactionIsolationLevel::Snapshot => "SNAPSHOT",
            TransactionIsolationLevel::Serializable => "SERIALIZABLE",
        }
    }

    /// Try to parse a [`TransactionIsolationLevel`] from the value returned from SQL Server.
    fn try_from_sql_server(val: i16) -> Result<TransactionIsolationLevel, anyhow::Error> {
        let level = match val {
            1 => TransactionIsolationLevel::ReadUncommitted,
            2 => TransactionIsolationLevel::ReadCommitted,
            3 => TransactionIsolationLevel::RepeatableRead,
            4 => TransactionIsolationLevel::Serializable,
            5 => TransactionIsolationLevel::Snapshot,
            x => anyhow::bail!("unknown level {x}"),
        };
        Ok(level)
    }
}

#[derive(Debug)]
enum Response {
    Execute { rows_affected: SmallVec<[u64; 1]> },
    Rows(SmallVec<[tiberius::Row; 1]>),
}

#[derive(Debug)]
struct Request {
    tx: oneshot::Sender<Result<Response, SqlServerError>>,
    kind: RequestKind,
}

#[derive(Derivative)]
#[derivative(Debug)]
enum RequestKind {
    Execute {
        query: String,
        #[derivative(Debug = "ignore")]
        params: SmallVec<[OwnedColumnData; 4]>,
    },
    Query {
        query: String,
        #[derivative(Debug = "ignore")]
        params: SmallVec<[OwnedColumnData; 4]>,
    },
    SimpleQuery {
        query: String,
    },
}

pub struct Connection {
    /// Other end of the channel that [`Client`] holds.
    rx: UnboundedReceiver<Request>,
    /// Actual client that we use to send requests.
    client: tiberius::Client<Compat<TcpStream>>,
    /// Resources (e.g. SSH tunnel) that need to be held open for the life of this connection.
    resources: Option<Box<dyn Any + Send + Sync>>,
}

impl Connection {
    async fn run(mut self) {
        while let Some(Request { tx, kind }) = self.rx.recv().await {
            tracing::debug!(?kind, "processing SQL Server query");
            let response = Connection::handle_request(&mut self.client, kind).await;

            // We don't care if our listener for this query has gone away.
            let _ = tx.send(response);
        }
        tracing::debug!("channel closed, SQL Server InnerClient shutting down");
    }

    async fn handle_request(
        client: &mut tiberius::Client<Compat<TcpStream>>,
        kind: RequestKind,
    ) -> Result<Response, SqlServerError> {
        match kind {
            RequestKind::Execute { query, params } => {
                #[allow(clippy::as_conversions)]
                let params: SmallVec<[&dyn ToSql; 4]> =
                    params.iter().map(|x| x as &dyn ToSql).collect();
                let result = client.execute(query, &params[..]).await?;

                match result.rows_affected() {
                    [] => Err(SqlServerError::InvariantViolated(
                        "got empty response".into(),
                    )),
                    rows_affected => Ok(Response::Execute {
                        rows_affected: rows_affected.into(),
                    }),
                }
            }
            RequestKind::Query { query, params } => {
                #[allow(clippy::as_conversions)]
                let params: SmallVec<[&dyn ToSql; 4]> =
                    params.iter().map(|x| x as &dyn ToSql).collect();
                let result = client.query(query, params.as_slice()).await?;

                let mut results = result.into_results().await.context("into results")?;
                if results.is_empty() {
                    Err(SqlServerError::InvariantViolated(
                        "got empty response".into(),
                    ))
                } else if results.len() == 1 {
                    // TODO(sql_server3): Don't use `into_results()` above, instead directly
                    // push onto a SmallVec to avoid the heap allocations.
                    let rows = results.pop().expect("checked len").into();
                    Ok(Response::Rows(rows))
                } else {
                    Err(SqlServerError::ProgrammingError(format!(
                        "Query only supports 1 statement, got {}",
                        results.len()
                    )))
                }
            }
            RequestKind::SimpleQuery { query } => {
                let result = client.simple_query(query).await?;

                let mut results = result.into_results().await.context("into results")?;
                if results.is_empty() {
                    Err(SqlServerError::InvariantViolated(
                        "got empty response".into(),
                    ))
                } else if results.len() == 1 {
                    // TODO(sql_server3): Don't use `into_results()` above, instead directly
                    // push onto a SmallVec to avoid the heap allocations.
                    let rows = results.pop().expect("checked len").into();
                    Ok(Response::Rows(rows))
                } else {
                    Err(SqlServerError::ProgrammingError(format!(
                        "Simple query only supports 1 statement, got {}",
                        results.len()
                    )))
                }
            }
        }
    }
}

impl IntoFuture for Connection {
    type Output = ();
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        self.run().boxed()
    }
}

/// Owned version of [`tiberius::ColumnData`] that can be more easily sent
/// across threads or through a channel.
#[derive(Debug)]
enum OwnedColumnData {
    U8(Option<u8>),
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    Bit(Option<bool>),
    String(Option<String>),
    Guid(Option<uuid::Uuid>),
    Binary(Option<Vec<u8>>),
    Numeric(Option<tiberius::numeric::Numeric>),
    Xml(Option<tiberius::xml::XmlData>),
    DateTime(Option<tiberius::time::DateTime>),
    SmallDateTime(Option<tiberius::time::SmallDateTime>),
    Time(Option<tiberius::time::Time>),
    Date(Option<tiberius::time::Date>),
    DateTime2(Option<tiberius::time::DateTime2>),
    DateTimeOffset(Option<tiberius::time::DateTimeOffset>),
}

impl<'a> From<tiberius::ColumnData<'a>> for OwnedColumnData {
    fn from(value: tiberius::ColumnData<'a>) -> Self {
        match value {
            tiberius::ColumnData::U8(inner) => OwnedColumnData::U8(inner),
            tiberius::ColumnData::I16(inner) => OwnedColumnData::I16(inner),
            tiberius::ColumnData::I32(inner) => OwnedColumnData::I32(inner),
            tiberius::ColumnData::I64(inner) => OwnedColumnData::I64(inner),
            tiberius::ColumnData::F32(inner) => OwnedColumnData::F32(inner),
            tiberius::ColumnData::F64(inner) => OwnedColumnData::F64(inner),
            tiberius::ColumnData::Bit(inner) => OwnedColumnData::Bit(inner),
            tiberius::ColumnData::String(inner) => {
                OwnedColumnData::String(inner.map(|s| s.to_string()))
            }
            tiberius::ColumnData::Guid(inner) => OwnedColumnData::Guid(inner),
            tiberius::ColumnData::Binary(inner) => {
                OwnedColumnData::Binary(inner.map(|b| b.to_vec()))
            }
            tiberius::ColumnData::Numeric(inner) => OwnedColumnData::Numeric(inner),
            tiberius::ColumnData::Xml(inner) => OwnedColumnData::Xml(inner.map(|x| x.into_owned())),
            tiberius::ColumnData::DateTime(inner) => OwnedColumnData::DateTime(inner),
            tiberius::ColumnData::SmallDateTime(inner) => OwnedColumnData::SmallDateTime(inner),
            tiberius::ColumnData::Time(inner) => OwnedColumnData::Time(inner),
            tiberius::ColumnData::Date(inner) => OwnedColumnData::Date(inner),
            tiberius::ColumnData::DateTime2(inner) => OwnedColumnData::DateTime2(inner),
            tiberius::ColumnData::DateTimeOffset(inner) => OwnedColumnData::DateTimeOffset(inner),
        }
    }
}

impl tiberius::ToSql for OwnedColumnData {
    fn to_sql(&self) -> tiberius::ColumnData<'_> {
        match self {
            OwnedColumnData::U8(inner) => tiberius::ColumnData::U8(*inner),
            OwnedColumnData::I16(inner) => tiberius::ColumnData::I16(*inner),
            OwnedColumnData::I32(inner) => tiberius::ColumnData::I32(*inner),
            OwnedColumnData::I64(inner) => tiberius::ColumnData::I64(*inner),
            OwnedColumnData::F32(inner) => tiberius::ColumnData::F32(*inner),
            OwnedColumnData::F64(inner) => tiberius::ColumnData::F64(*inner),
            OwnedColumnData::Bit(inner) => tiberius::ColumnData::Bit(*inner),
            OwnedColumnData::String(inner) => {
                tiberius::ColumnData::String(inner.as_deref().map(Cow::Borrowed))
            }
            OwnedColumnData::Guid(inner) => tiberius::ColumnData::Guid(*inner),
            OwnedColumnData::Binary(inner) => {
                tiberius::ColumnData::Binary(inner.as_deref().map(Cow::Borrowed))
            }
            OwnedColumnData::Numeric(inner) => tiberius::ColumnData::Numeric(*inner),
            OwnedColumnData::Xml(inner) => {
                tiberius::ColumnData::Xml(inner.as_ref().map(Cow::Borrowed))
            }
            OwnedColumnData::DateTime(inner) => tiberius::ColumnData::DateTime(*inner),
            OwnedColumnData::SmallDateTime(inner) => tiberius::ColumnData::SmallDateTime(*inner),
            OwnedColumnData::Time(inner) => tiberius::ColumnData::Time(*inner),
            OwnedColumnData::Date(inner) => tiberius::ColumnData::Date(*inner),
            OwnedColumnData::DateTime2(inner) => tiberius::ColumnData::DateTime2(*inner),
            OwnedColumnData::DateTimeOffset(inner) => tiberius::ColumnData::DateTimeOffset(*inner),
        }
    }
}

impl<'a, T: tiberius::ToSql> From<&'a T> for OwnedColumnData {
    fn from(value: &'a T) -> Self {
        OwnedColumnData::from(value.to_sql())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SqlServerError {
    #[error(transparent)]
    SqlServer(#[from] tiberius::error::Error),
    #[error("expected column '{0}' to be present")]
    MissingColumn(&'static str),
    #[error("'{column_type}' on column '{column_name}' is not supported")]
    UnsupportedDataType {
        column_name: String,
        column_type: String,
    },
    #[error("sql server client encountered I/O error: {0}")]
    IO(#[from] tokio::io::Error),
    #[error("invalid SQL Server system setting '{name}'. Expected '{expected}'. Got '{actual}'.")]
    InvalidSystemSetting {
        name: String,
        expected: String,
        actual: String,
    },
    #[error("invariant was violated: {0}")]
    InvariantViolated(String),
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    #[error("programming error! {0}")]
    ProgrammingError(String),
}
