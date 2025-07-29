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
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Context;
use derivative::Derivative;
use futures::future::BoxFuture;
use futures::{FutureExt, Stream, StreamExt, TryStreamExt};
use mz_ore::netio::DUMMY_DNS_PORT;
use mz_ore::result::ResultExt;
use mz_repr::ScalarType;
use smallvec::{SmallVec, smallvec};
use tiberius::ToSql;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

pub mod cdc;
pub mod config;
pub mod desc;
pub mod inspect;

pub use config::Config;
pub use desc::{ProtoSqlServerColumnDesc, ProtoSqlServerTableDesc};

use crate::config::TunnelConfig;
use crate::desc::SqlServerColumnDecodeType;

/// Higher level wrapper around a [`tiberius::Client`] that models transaction
/// management like other database clients.
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
    pub async fn connect(config: Config) -> Result<Self, SqlServerError> {
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
                    .connect(ssh_config.clone(), host, *port, *timeout, config.in_task)
                    .await?;
                let tcp = TcpStream::connect(tunnel.local_addr())
                    .await
                    .context("ssh tunnel")?;

                (tcp, Some(Box::new(tunnel)))
            }
            TunnelConfig::AwsPrivatelink {
                connection_id,
                port,
            } => {
                let privatelink_host = mz_cloud_resources::vpc_endpoint_name(*connection_id);
                let mut privatelink_addrs =
                    tokio::net::lookup_host((privatelink_host.clone(), DUMMY_DNS_PORT)).await?;

                let Some(mut addr) = privatelink_addrs.next() else {
                    return Err(SqlServerError::InvariantViolated(format!(
                        "aws privatelink: no addresses found for host {:?}",
                        privatelink_host
                    )));
                };

                addr.set_port(port.clone());

                let tcp = TcpStream::connect(addr)
                    .await
                    .context(format!("aws privatelink {:?}", addr))?;

                (tcp, None)
            }
        };

        tcp.set_nodelay(true)?;

        let (client, connection) = Self::connect_raw(config, tcp, resources).await?;
        mz_ore::task::spawn(|| "sql-server-client-connection", async move {
            connection.await
        });

        Ok(client)
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
                _resources: resources,
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
            other @ Response::Rows(_) | other @ Response::RowStream { .. } => {
                Err(SqlServerError::ProgrammingError(format!(
                    "expected Response::Execute, got {other:?}"
                )))
            }
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
            other @ Response::Execute { .. } | other @ Response::RowStream { .. } => Err(
                SqlServerError::ProgrammingError(format!("expected Response::Rows, got {other:?}")),
            ),
        }
    }

    /// Executes SQL statements in SQL Server, returning a [`Stream`] of
    /// resulting rows.
    ///
    /// Passthrough method for [`tiberius::Client::query`].
    pub fn query_streaming<'c, 'q, Q>(
        &'c mut self,
        query: Q,
        params: &[&dyn tiberius::ToSql],
    ) -> impl Stream<Item = Result<tiberius::Row, SqlServerError>> + Send + use<'c, Q>
    where
        Q: Into<Cow<'q, str>>,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let params = params
            .iter()
            .map(|p| OwnedColumnData::from(p.to_sql()))
            .collect();
        let kind = RequestKind::QueryStreamed {
            query: query.into().to_string(),
            params,
        };

        // Make our initial request which will return a Stream of Rows.
        let request_future = async move {
            self.tx
                .send(Request { tx, kind })
                .context("sending request")?;

            let response = rx.await.context("channel")??;
            match response {
                Response::RowStream { stream } => {
                    Ok(tokio_stream::wrappers::ReceiverStream::new(stream))
                }
                other @ Response::Execute { .. } | other @ Response::Rows(_) => {
                    Err(SqlServerError::ProgrammingError(format!(
                        "expected Response::Rows, got {other:?}"
                    )))
                }
            }
        };

        // "flatten" our initial request into the returned stream.
        futures::stream::once(request_future).try_flatten()
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
            other @ Response::Execute { .. } | other @ Response::RowStream { .. } => Err(
                SqlServerError::ProgrammingError(format!("expected Response::Rows, got {other:?}")),
            ),
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

    /// Return a [`CdcStream`] that can be used to track changes for the specified
    /// `capture_instances`.
    ///
    /// [`CdcStream`]: crate::cdc::CdcStream
    pub fn cdc<I>(&mut self, capture_instances: I) -> crate::cdc::CdcStream<'_>
    where
        I: IntoIterator,
        I::Item: Into<Arc<str>>,
    {
        let instances = capture_instances
            .into_iter()
            .map(|i| (i.into(), None))
            .collect();
        crate::cdc::CdcStream::new(self, instances)
    }
}

/// A stream of [`tiberius::Row`]s.
pub type RowStream<'a> =
    Pin<Box<dyn Stream<Item = Result<tiberius::Row, SqlServerError>> + Send + 'a>>;

#[derive(Debug)]
pub struct Transaction<'a> {
    client: &'a mut Client,
    closed: bool,
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
            Ok(Transaction {
                client,
                closed: false,
            })
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

    /// See [`Client::query_streaming`]
    pub fn query_streaming<'c, 'q, Q>(
        &'c mut self,
        query: Q,
        params: &[&dyn tiberius::ToSql],
    ) -> impl Stream<Item = Result<tiberius::Row, SqlServerError>> + Send + use<'c, Q>
    where
        Q: Into<Cow<'q, str>>,
    {
        self.client.query_streaming(query, params)
    }

    /// See [`Client::simple_query`].
    pub async fn simple_query<'q>(
        &mut self,
        query: impl Into<Cow<'q, str>>,
    ) -> Result<SmallVec<[tiberius::Row; 1]>, SqlServerError> {
        self.client.simple_query(query).await
    }

    /// Rollback the [`Transaction`].
    pub async fn rollback(mut self) -> Result<(), SqlServerError> {
        static ROLLBACK_QUERY: &str = "ROLLBACK TRANSACTION";
        // N.B. Mark closed _before_ running the query. This prevents us from
        // double closing the transaction if this query itself fails.
        self.closed = true;
        self.client.simple_query(ROLLBACK_QUERY).await?;
        Ok(())
    }

    /// Commit the [`Transaction`].
    pub async fn commit(mut self) -> Result<(), SqlServerError> {
        static COMMIT_QUERY: &str = "COMMIT TRANSACTION";
        // N.B. Mark closed _before_ running the query. This prevents us from
        // double closing the transaction if this query itself fails.
        self.closed = true;
        self.client.simple_query(COMMIT_QUERY).await?;
        Ok(())
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        // Internally the query is synchronously sent down a channel, and the response is what
        // we await. In other words, we don't need to `.await` here for the query to be run.
        if !self.closed {
            let _fut = self.client.simple_query("ROLLBACK TRANSACTION");
        }
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

#[derive(Derivative)]
#[derivative(Debug)]
enum Response {
    Execute {
        rows_affected: SmallVec<[u64; 1]>,
    },
    Rows(SmallVec<[tiberius::Row; 1]>),
    RowStream {
        #[derivative(Debug = "ignore")]
        stream: tokio::sync::mpsc::Receiver<Result<tiberius::Row, SqlServerError>>,
    },
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
    QueryStreamed {
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
    _resources: Option<Box<dyn Any + Send + Sync>>,
}

impl Connection {
    async fn run(mut self) {
        while let Some(Request { tx, kind }) = self.rx.recv().await {
            tracing::trace!(?kind, "processing SQL Server query");
            let result = Connection::handle_request(&mut self.client, kind).await;
            let (response, maybe_extra_work) = match result {
                Ok((response, work)) => (Ok(response), work),
                Err(err) => (Err(err), None),
            };

            // We don't care if our listener for this query has gone away.
            let _ = tx.send(response);

            // After we handle a request there might still be something in-flight
            // that we need to continue driving, e.g. when the response is a
            // Stream of Rows.
            if let Some(extra_work) = maybe_extra_work {
                extra_work.await;
            }
        }
        tracing::debug!("channel closed, SQL Server InnerClient shutting down");
    }

    async fn handle_request<'c>(
        client: &'c mut tiberius::Client<Compat<TcpStream>>,
        kind: RequestKind,
    ) -> Result<(Response, Option<BoxFuture<'c, ()>>), SqlServerError> {
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
                    rows_affected => {
                        let response = Response::Execute {
                            rows_affected: rows_affected.into(),
                        };
                        Ok((response, None))
                    }
                }
            }
            RequestKind::Query { query, params } => {
                #[allow(clippy::as_conversions)]
                let params: SmallVec<[&dyn ToSql; 4]> =
                    params.iter().map(|x| x as &dyn ToSql).collect();
                let result = client.query(query, params.as_slice()).await?;

                let mut results = result.into_results().await.context("into results")?;
                if results.is_empty() {
                    Ok((Response::Rows(smallvec![]), None))
                } else if results.len() == 1 {
                    // TODO(sql_server3): Don't use `into_results()` above, instead directly
                    // push onto a SmallVec to avoid the heap allocations.
                    let rows = results.pop().expect("checked len").into();
                    Ok((Response::Rows(rows), None))
                } else {
                    Err(SqlServerError::ProgrammingError(format!(
                        "Query only supports 1 statement, got {}",
                        results.len()
                    )))
                }
            }
            RequestKind::QueryStreamed { query, params } => {
                #[allow(clippy::as_conversions)]
                let params: SmallVec<[&dyn ToSql; 4]> =
                    params.iter().map(|x| x as &dyn ToSql).collect();
                let result = client.query(query, params.as_slice()).await?;

                // ~~ Rust Lifetimes ~~
                //
                // What's going on here, why do we have some extra channel and
                // this 'work' future?
                //
                // Remember, we run the actual `tiberius::Client` in a separate
                // `tokio::task` and the `mz::Client` sends query requests via
                // a channel, this allows us to "automatically" manage
                // transactions.
                //
                // But the returned `QueryStream` from a `tiberius::Client` has
                // a lifetime associated with said client running in this
                // separate task. Thus we cannot send the `QueryStream` back to
                // the `mz::Client` because the lifetime of these two clients
                // is not linked at all. The fix is to create a separate owned
                // channel and return the receiving end, while this work future
                // pulls events off the `QueryStream` and sends them over the
                // channel we just returned.
                let (tx, rx) = tokio::sync::mpsc::channel(256);
                let work = Box::pin(async move {
                    let mut stream = result.into_row_stream();
                    while let Some(result) = stream.next().await {
                        if let Err(err) = tx.send(result.err_into()).await {
                            tracing::warn!(?err, "SQL Server row stream receiver went away");
                        }
                    }
                    tracing::info!("SQL Server row stream complete");
                });

                Ok((Response::RowStream { stream: rx }, Some(work)))
            }
            RequestKind::SimpleQuery { query } => {
                let result = client.simple_query(query).await?;

                let mut results = result.into_results().await.context("into results")?;
                if results.is_empty() {
                    Ok((Response::Rows(smallvec![]), None))
                } else if results.len() == 1 {
                    // TODO(sql_server3): Don't use `into_results()` above, instead directly
                    // push onto a SmallVec to avoid the heap allocations.
                    let rows = results.pop().expect("checked len").into();
                    Ok((Response::Rows(rows), None))
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
    #[error(transparent)]
    CdcError(#[from] crate::cdc::CdcError),
    #[error("expected column '{0}' to be present")]
    MissingColumn(&'static str),
    #[error("sql server client encountered I/O error: {0}")]
    IO(#[from] tokio::io::Error),
    #[error("found invalid data in the column '{column_name}': {error}")]
    InvalidData { column_name: String, error: String },
    #[error("got back a null value when querying for the LSN")]
    NullLsn,
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

/// Errors returned from decoding SQL Server rows.
///
/// **PLEASE READ**
///
/// The string representation of this error type is **durably stored** in a source and thus this
/// error type needs to be **stable** across releases. For example, if in v11 of Materialize we
/// fail to decode `Row(["foo bar"])` from SQL Server, we will record the error in the source's
/// Persist shard. If in v12 of Materialize the user deletes the `Row(["foo bar"])` from their
/// upstream instance, we need to perfectly retract the error we previously committed.
///
/// This means be **very** careful when changing this type.
#[derive(Debug, thiserror::Error)]
pub enum SqlServerDecodeError {
    #[error("column '{column_name}' was invalid when getting as type '{as_type}'")]
    InvalidColumn {
        column_name: String,
        as_type: &'static str,
    },
    #[error("found invalid data in the column '{column_name}': {error}")]
    InvalidData { column_name: String, error: String },
    #[error("can't decode {sql_server_type:?} as {mz_type:?}")]
    Unsupported {
        sql_server_type: SqlServerColumnDecodeType,
        mz_type: ScalarType,
    },
}

impl SqlServerDecodeError {
    fn invalid_timestamp(name: &str, error: mz_repr::adt::timestamp::TimestampError) -> Self {
        // These error messages need to remain stable, do not change them.
        let error = match error {
            mz_repr::adt::timestamp::TimestampError::OutOfRange => "out of range",
        };
        SqlServerDecodeError::InvalidData {
            column_name: name.to_string(),
            error: error.to_string(),
        }
    }

    fn invalid_date(name: &str, error: mz_repr::adt::date::DateError) -> Self {
        // These error messages need to remain stable, do not change them.
        let error = match error {
            mz_repr::adt::date::DateError::OutOfRange => "out of range",
        };
        SqlServerDecodeError::InvalidData {
            column_name: name.to_string(),
            error: error.to_string(),
        }
    }

    fn invalid_char(name: &str, expected_chars: usize, found_chars: usize) -> Self {
        SqlServerDecodeError::InvalidData {
            column_name: name.to_string(),
            error: format!("expected {expected_chars} chars found {found_chars}"),
        }
    }

    fn invalid_varchar(name: &str, max_chars: usize, found_chars: usize) -> Self {
        SqlServerDecodeError::InvalidData {
            column_name: name.to_string(),
            error: format!("expected max {max_chars} chars found {found_chars}"),
        }
    }

    fn invalid_column(name: &str, as_type: &'static str) -> Self {
        SqlServerDecodeError::InvalidColumn {
            column_name: name.to_string(),
            as_type,
        }
    }
}
