use std::convert::TryInto;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::StreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use postgres_protocol::message::backend::ReplicationMessage;
use postgres_protocol::replication::pgoutput::{
    LogicalReplicationMessage, PgOutput, Tuple, TupleData,
};
use tokio_postgres::binary_copy::BinaryCopyOutStream;
use tokio_postgres::replication_client::{ReplicationClient, SnapshotMode};
use tokio_postgres::types::{PgLsn, Type as PgType};
use tokio_postgres::{connect_replication, NoTls, ReplicationMode};
use uuid::Uuid;

use crate::source::{SimpleSource, Timestamper};
use dataflow_types::PostgresSourceConnector;
use repr::{Datum, Row, RowArena, RowPacker};

lazy_static! {
    /// Postgres epoch is 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSimpleSource {
    connector: PostgresSourceConnector,
    packer: RowPacker,
    plugin: PgOutput,
    /// Our cursor into the WAL
    lsn: PgLsn,
    // TODO(petrosagg): move this in the catalog to clean it up on DROP SOURCE
    slot_name: String,
}

/// The schema of a single column
struct PgColumn {
    scalar_type: PgType,
    nullable: bool,
}

/// Information about the remote table
struct TableInfo {
    /// The relation id of the table. This is identical to its OID
    rel_id: u32,
    /// The datatype and nullability of each column, in order
    schema: Vec<PgColumn>,
}

impl PostgresSimpleSource {
    /// Constructs a new instance
    pub fn new(connector: PostgresSourceConnector) -> Self {
        let publication = connector.publication.clone();
        let slot_name = Uuid::new_v4().to_string().replace('-', "");
        Self {
            connector,
            packer: RowPacker::new(),
            plugin: PgOutput::new(vec![publication]),
            lsn: 0.into(),
            slot_name,
        }
    }

    /// Uses a normal postgres connection (i.e not in replication mode) to get the column types of
    /// the remote table as well as the relation id that is used to filter the replication stream
    async fn table_info(&self) -> Result<TableInfo, anyhow::Error> {
        let conninfo = &self.connector.conn;

        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await?;
        tokio::spawn(connection);

        let namespace = &self.connector.namespace;
        let table = &self.connector.table;
        let rel_id: u32 = client
            .query(
                "SELECT c.oid
                    FROM pg_catalog.pg_class c
                    INNER JOIN pg_catalog.pg_namespace n
                          ON (c.relnamespace = n.oid)
                    WHERE n.nspname = $1
                      AND c.relname = $2;",
                &[&namespace, &table],
            )
            .await?
            .get(0)
            .ok_or(anyhow!("table not found in the upstream catalog"))?
            .get(0);

        // Get the column type info
        let schema = client
            .query(
                "SELECT a.atttypid, a.attnotnull
                    FROM pg_catalog.pg_attribute a
                    WHERE a.attnum > 0::pg_catalog.int2
                      AND NOT a.attisdropped
                      AND a.attrelid = $1
                    ORDER BY a.attnum",
                &[&rel_id],
            )
            .await?
            .into_iter()
            .map(|row| {
                let oid = row.get(0);
                let scalar_type =
                    PgType::from_oid(oid).ok_or_else(|| anyhow!("unknown type OID: {}", oid))?;
                let nullable = !row.get::<_, bool>(1);
                Ok(PgColumn {
                    scalar_type,
                    nullable,
                })
            })
            .collect::<Result<Vec<_>, anyhow::Error>>()?;

        Ok(TableInfo { rel_id, schema })
    }

    /// Starts a replication connection to the upstream database
    async fn connect_replication(&self) -> Result<ReplicationClient, anyhow::Error> {
        let conninfo = &self.connector.conn;
        let (client, conn) =
            connect_replication(&conninfo, NoTls, ReplicationMode::Logical).await?;
        tokio::spawn(conn);
        Ok(client)
    }

    /// Creates the replication slot and produces the initial snapshot of the data
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot(
        &mut self,
        table_info: &TableInfo,
        timestamper: &Timestamper,
    ) -> Result<(), anyhow::Error> {
        let mut client = self.connect_replication().await?;

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await?;

        // TODO(petrosagg): Before attempting to create the slot we need to check if it exists and
        // remove it if it does. This is because the only way of entering a transaction that is
        // synchronized with a replication slot is during its creation
        let slot = client
            .create_logical_replication_slot(
                &self.slot_name,
                false,
                &self.plugin,
                Some(SnapshotMode::UseSnapshot),
            )
            .await?;

        // Store the lsn at which we will need to start the replication stream from
        self.lsn = slot.consistent_point();

        let copy_query = format!(
            r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
            &self.connector.namespace, &self.connector.table
        );

        let stream = client.copy_out(&copy_query).await?;

        let scalar_types = table_info
            .schema
            .iter()
            .map(|c| c.scalar_type.clone())
            .collect_vec();
        let rows = BinaryCopyOutStream::new(stream, &scalar_types);
        tokio::pin!(rows);

        let snapshot_tx = timestamper.start_tx().await;
        while let Some(row) = rows.next().await.transpose()? {
            self.packer.clear();
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
                self.packer.push(datum);
            }

            let row = self.packer.finish_and_reuse();
            snapshot_tx.insert(row).await?;
        }

        client.simple_query("COMMIT;").await?;
        Ok(())
    }

    /// Converts a Tuple received in the replication stream into a Row instance. The logical
    /// replication protocol doesn't use the binary encoding for column values so contrary to the
    /// initial snapshot here we need to parse the textual form of each column.
    fn row_from_tuple(&mut self, tuple: &Tuple) -> Result<Row, anyhow::Error> {
        self.packer.clear();
        let arena = RowArena::new();

        for (val, cast_expr) in tuple
            .tuple_data()
            .iter()
            .zip(self.connector.cast_exprs.iter())
        {
            let datum = match val {
                TupleData::Null => Datum::Null,
                TupleData::Toast => bail!("Unsupported TOAST value"),
                TupleData::Text(b) => {
                    let txt_datum: Datum = std::str::from_utf8(&b)?.into();
                    cast_expr.eval(&[txt_datum], &arena)?
                }
            };
            self.packer.push(datum);
        }

        Ok(self.packer.finish_and_reuse())
    }

    async fn produce_replication(
        &mut self,
        table_info: &TableInfo,
        timestamper: &Timestamper,
    ) -> Result<(), anyhow::Error> {
        let mut client = self.connect_replication().await?;

        let mut stream = client
            .start_logical_replication(&self.slot_name, self.lsn, &self.plugin)
            .await?;

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
                            let tx = tx
                                .as_mut()
                                .ok_or(anyhow!("got row event without a prior BEGIN event"))?;
                            let row = self.row_from_tuple(insert.tuple())?;
                            tx.insert(row).await?;
                        }
                        Update(update) => {
                            let tx = tx
                                .as_mut()
                                .ok_or(anyhow!("got row event without a prior BEGIN event"))?;
                            let old_row = self.row_from_tuple(update.old_tuple().unwrap())?;
                            tx.delete(old_row).await?;

                            let new_row = self.row_from_tuple(update.new_tuple())?;
                            tx.insert(new_row).await?;
                        }
                        Delete(delete) => {
                            let tx = tx
                                .as_mut()
                                .ok_or(anyhow!("got row event without a prior BEGIN event"))?;
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
impl SimpleSource for PostgresSimpleSource {
    /// The top-level control of the state machine and retry logic
    async fn start(mut self, timestamper: &Timestamper) -> Result<(), anyhow::Error> {
        // TODO(petrosagg): retry on recoverable errors
        let table_info = self.table_info().await?;

        // The initial snapshot has no easy way of retrying it in case of connection failures
        self.produce_snapshot(&table_info, timestamper).await?;

        // TODO(petrosagg): implement retry logic for the replication phase. This is tricky because
        // in order to do it right we need to specify an LSN that will not skip any data but the
        // logical decode stream is not in LSN order and it's unclear what the lower bound is
        self.produce_replication(&table_info, timestamper).await?;

        Ok(())
    }
}
