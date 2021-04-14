// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::StreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;

use postgres_protocol::message::backend::{
    LogicalReplicationMessage, ReplicationMessage, Tuple, TupleData,
};
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::config::{Config, ReplicationMode};
use tokio_postgres::replication::LogicalReplicationStream;
use tokio_postgres::types::{PgLsn, Type as PgType};
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use uuid::Uuid;

use crate::source::{SimpleSource, SourceError, Timestamper};
use dataflow_types::PostgresSourceConnector;
use postgres_util::TableInfo;
use repr::{Datum, Row, RowArena};

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSourceReader {
    connector: PostgresSourceConnector,
    /// Our cursor into the WAL
    lsn: PgLsn,
    // TODO(petrosagg): move this in the catalog to clean it up on DROP SOURCE
    slot_name: String,
}

impl PostgresSourceReader {
    /// Constructs a new instance
    pub fn new(connector: PostgresSourceConnector) -> Self {
        let slot_name = Uuid::new_v4().to_string().replace('-', "");
        Self {
            connector,
            lsn: 0.into(),
            slot_name,
        }
    }

    /// Starts a replication connection to the upstream database
    async fn connect_replication(&self) -> Result<Client, anyhow::Error> {
        let mut config: Config = self.connector.conn.parse()?;

        let (client, conn) = config
            .replication_mode(ReplicationMode::Logical)
            .connect(NoTls)
            .await?;
        tokio::spawn(conn);
        Ok(client)
    }

    /// Creates the replication slot and produces the initial snapshot of the data
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot(
        &mut self,
        client: &Client,
        table_info: &TableInfo,
        timestamper: &Timestamper,
    ) -> Result<(), anyhow::Error> {
        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        let slot_query = format!(
            r#"CREATE_REPLICATION_SLOT {:?} TEMPORARY LOGICAL "pgoutput" USE_SNAPSHOT"#,
            &self.slot_name
        );
        let slot_row = client
            .simple_query(&slot_query)
            .await?
            .into_iter()
            .next()
            .and_then(|msg| match msg {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .ok_or_else(|| anyhow!("empty result after creating replication slot"))?;

        // Store the lsn at which we will need to start the replication stream from
        self.lsn = slot_row
            .get("consistent_point")
            .ok_or_else(|| anyhow!("missing expected column: `consistent_point`"))?
            .parse()
            .or_else(|_| Err(anyhow!("invalid lsn")))?;

        let copy_query = format!(
            r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
            &self.connector.namespace, &self.connector.table
        );

        let stream = client.copy_out_simple(&copy_query).await?;

        let scalar_types = table_info
            .schema
            .iter()
            .map(|c| c.scalar_type.clone())
            .collect_vec();

        let rows = BinaryCopyOutStream::new(stream, &scalar_types);
        tokio::pin!(rows);

        let snapshot_tx = timestamper.start_tx().await;
        while let Some(row) = rows.next().await.transpose()? {
            let mut mz_row = Row::default();
            for (i, ty) in table_info.schema.iter().enumerate() {
                let datum: Datum = match ty.scalar_type {
                    PgType::BOOL => row.get::<Option<bool>>(i).into(),
                    PgType::INT4 => row.get::<Option<i32>>(i).into(),
                    PgType::INT8 => row.get::<Option<i64>>(i).into(),
                    PgType::FLOAT4 => row.get::<Option<f32>>(i).into(),
                    PgType::FLOAT8 => row.get::<Option<f64>>(i).into(),
                    PgType::DATE => row.get::<Option<NaiveDate>>(i).into(),
                    PgType::TIME => row.get::<Option<NaiveTime>>(i).into(),
                    PgType::TIMESTAMP => row.get::<Option<NaiveDateTime>>(i).into(),
                    PgType::TEXT => row.get::<Option<&str>>(i).into(),
                    PgType::UUID => row.get::<Option<Uuid>>(i).into(),
                    ref other => bail!("Unsupported data type {:?}", other),
                };

                if !ty.nullable && datum.is_null() {
                    bail!("Got null value in non nullable column");
                }
                mz_row.push(datum);
            }

            snapshot_tx.insert(mz_row).await?;
        }

        client.simple_query("COMMIT;").await?;
        Ok(())
    }

    /// Converts a Tuple received in the replication stream into a Row instance. The logical
    /// replication protocol doesn't use the binary encoding for column values so contrary to the
    /// initial snapshot here we need to parse the textual form of each column.
    fn row_from_tuple(&mut self, tuple: &Tuple) -> Result<Row, anyhow::Error> {
        let mut row = Row::default();
        let arena = RowArena::new();

        for (val, cast_expr) in tuple
            .tuple_data()
            .iter()
            .zip(self.connector.cast_exprs.iter())
        {
            let datum = match val {
                TupleData::Null => Datum::Null,
                TupleData::UnchangedToast => bail!("Unsupported TOAST value"),
                TupleData::Text(b) => {
                    let txt_datum: Datum = std::str::from_utf8(&b)?.into();
                    cast_expr.eval(&[txt_datum], &arena)?
                }
            };
            row.push(datum);
        }

        Ok(row)
    }

    async fn produce_replication(
        &mut self,
        client: &Client,
        table_info: &TableInfo,
        timestamper: &Timestamper,
    ) -> Result<(), anyhow::Error> {
        let query = format!(
            r#"START_REPLICATION SLOT "{name}" LOGICAL {lsn}
              ("proto_version" '1', "publication_names" '{publication}')"#,
            name = &self.slot_name,
            lsn = self.lsn.to_string(),
            publication = self.connector.publication
        );
        let copy_stream = client.copy_both_simple(&query).await?;

        let stream = LogicalReplicationStream::new(copy_stream);
        tokio::pin!(stream);

        let mut tx = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(ReplicationMessage::XLogData(xlog_data)) => {
                    use LogicalReplicationMessage::*;

                    match xlog_data.data() {
                        Begin(_) => {
                            tx = Some(timestamper.start_tx().await);
                        }
                        // The replication stream might include data from other tables in the
                        // publication, so we ignore them
                        Insert(event) if event.rel_id() != table_info.rel_id => continue,
                        Update(event) if event.rel_id() != table_info.rel_id => continue,
                        Delete(event) if event.rel_id() != table_info.rel_id => continue,
                        Insert(insert) => {
                            let tx = tx.as_mut().ok_or_else(|| {
                                anyhow!("got row event without a prior BEGIN event")
                            })?;
                            let row = self.row_from_tuple(insert.tuple())?;
                            tx.insert(row).await?;
                        }
                        Update(update) => {
                            let tx = tx.as_mut().ok_or_else(|| {
                                anyhow!("got row event without a prior BEGIN event")
                            })?;
                            let old_row = self.row_from_tuple(update.old_tuple().unwrap())?;
                            tx.delete(old_row).await?;

                            let new_row = self.row_from_tuple(update.new_tuple())?;
                            tx.insert(new_row).await?;
                        }
                        Delete(delete) => {
                            let tx = tx.as_mut().ok_or_else(|| {
                                anyhow!("got row event without a prior BEGIN event")
                            })?;
                            let row = self.row_from_tuple(delete.old_tuple().unwrap())?;
                            tx.delete(row).await?;
                        }
                        Commit(commit) => {
                            self.lsn = commit.commit_lsn().into();
                            // Release the lease
                            tx = None;
                        }
                        Origin(_) | Relation(_) | Type(_) => (),
                        Truncate(_) => bail!("source table got truncated"),
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        _ => bail!("unexpected logical replication message"),
                    }
                }
                // The upstream will periodically send keepalive messages to:
                //    a) keep the TCP connection alive
                //    b) get feedback about the state of replication in the replica
                // The upstream database can dictate if a reply MUST be given by setting the reply
                // field to 1
                Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                    if keepalive.reply() == 1 {
                        let ts: i64 = PG_EPOCH
                            .elapsed()
                            .expect("system clock set earlier than year 2000!")
                            .as_micros()
                            .try_into()
                            .expect("software more than 200k years old, consider updating");

                        // Here we report that we've seen everything up until `self.lsn`, which is
                        // the lsn of that commit message we received. However, there could be
                        // transactions that we have not seen yet (perhaps because they are not
                        // committed yet) that started way earlier than self.lsn. Therefore, it's
                        // unclear how to calculate a safe lower bound value to report back to the
                        // server. The answer to this question will allow writing retry logic for
                        // the replication phase.
                        stream
                            .as_mut()
                            .standby_status_update(self.lsn, self.lsn, self.lsn, ts, 0)
                            .await?;
                    }
                }
                Err(err) => return Err(err.into()),
                // The enum is marked non_exaustive, better be conservative
                _ => bail!("Unexpected replication message"),
            }
        }
        Ok(())
    }
}

#[async_trait]
impl SimpleSource for PostgresSourceReader {
    /// The top-level control of the state machine and retry logic
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), SourceError> {
        // TODO(petrosagg): retry on recoverable errors
        let table_info = postgres_util::table_info(
            &self.connector.conn,
            &self.connector.namespace,
            &self.connector.table,
        )
        .await
        .map_err(|e| SourceError::FileIO(e.to_string()))?;

        let client = self
            .connect_replication()
            .await
            .map_err(|e| SourceError::FileIO(e.to_string()))?;

        // The initial snapshot has no easy way of retrying it in case of connection failures
        self.produce_snapshot(&client, &table_info, timestamper)
            .await
            .map_err(|e| SourceError::FileIO(e.to_string()))?;

        // TODO(petrosagg): implement retry logic for the replication phase. This is tricky because
        // in order to do it right we need to specify an LSN that will not skip any data but the
        // logical decode stream is not in LSN order and it's unclear what LSN we should use
        self.produce_replication(&client, &table_info, timestamper)
            .await
            .map_err(|e| SourceError::FileIO(e.to_string()))?;

        Ok(())
    }
}
