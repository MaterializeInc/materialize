use std::convert::TryInto;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use futures::stream::BoxStream;
use futures::{pin_mut, StreamExt};
use genawaiter::sync::{Co, Gen};
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

use crate::source::{SimpleSource, StreamItem, Timestamper};
use dataflow_types::PostgresSourceConnector;
use expr::parse_datum;
use repr::{Datum, Row, RowPacker, ScalarType};

lazy_static! {
    /// Postgres epoch starts at 2000-01-01T00:00:00Z
    static ref PG_EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(946_684_800);
}

/// Information required to sync data from Postgres
pub struct PostgresSimpleSource {
    connector: PostgresSourceConnector,
    packer: RowPacker,
    plugin: PgOutput,
}

struct PgColumn {
    scalar_type: PgType,
    nullable: bool,
}

impl PostgresSimpleSource {
    /// Constructs a new instance
    pub fn new(connector: PostgresSourceConnector) -> Self {
        let publication = connector.publication.clone();
        Self {
            connector,
            packer: RowPacker::new(),
            plugin: PgOutput::new(vec![publication]),
        }
    }

    /// Uses a normal postgres connection (i.e not in replication mode) to get the column types of
    /// the remote table as well as the relation id that is used to filter the replication stream
    async fn table_info(&self) -> (u32, Vec<PgColumn>) {
        let conninfo = &self.connector.conn;

        let (client, connection) = tokio_postgres::connect(&conninfo, NoTls).await.unwrap();

        // TODO communicate errors back into the stream
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

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
            .await
            .unwrap()
            .get(0)
            .unwrap()
            .get(0);

        // Get the column type info
        let col_types = client
            .query(
                "SELECT a.atttypid, a.attnotnull
                    FROM pg_catalog.pg_attribute a
                    WHERE a.attnum > 0::pg_catalog.int2
                      AND NOT a.attisdropped
                      AND a.attrelid = $1
                    ORDER BY a.attnum",
                &[&rel_id],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| PgColumn {
                scalar_type: PgType::from_oid(row.get(0)).unwrap(),
                nullable: !row.get::<_, bool>(1),
            })
            .collect_vec();

        (rel_id, col_types)
    }

    /// Creates the replication slot and streams the initial snapshot of the data using the
    /// coroutine handle.
    ///
    /// After the initial snapshot has been produced it returns the name of the created slot and
    /// the LSN at which we should start the replication stream at.
    async fn produce_snapshot(
        &mut self,
        client: &mut ReplicationClient,
        col_types: &[PgColumn],
        timestamper: &Timestamper,
        co: &mut Co<StreamItem>,
    ) -> (String, PgLsn) {
        // TODO: find better naming scheme. Postgres doesn't like dashes
        let slot_name = Uuid::new_v4().to_string().replace('-', "");

        // Start a transaction and immediatelly create a replication slot with the USE SNAPSHOT
        // directive. This makes the starting point of the slot and the snapshot of the transaction
        // identical.
        //
        // TODO: The slot should be permanent and destroyed on Drop. This way we can support
        // reconnects
        client
            .simple_query("BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ;")
            .await
            .unwrap();
        let slot = client
            .create_logical_replication_slot(
                &slot_name,
                true,
                &self.plugin,
                Some(SnapshotMode::UseSnapshot),
            )
            .await
            .unwrap();

        let cur_lsn = slot.consistent_point();

        // Initial data copy
        let copy_query = format!(
            r#"COPY "{}"."{}" TO STDOUT WITH (FORMAT binary);"#,
            &self.connector.namespace, &self.connector.table
        );

        let stream = client.copy_out(&copy_query).await.unwrap();

        let scalar_types = col_types
            .iter()
            .map(|c| c.scalar_type.clone())
            .collect_vec();
        let rows = BinaryCopyOutStream::new(stream, &scalar_types);
        pin_mut!(rows);

        let snapshot_ts = timestamper.lease().await;

        // TODO: handle Err case
        while let Some(Ok(row)) = rows.next().await {
            self.packer.clear();
            for (i, ty) in col_types.iter().enumerate() {
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
                    ref other => todo!("Unsupported data type {:?}", other),
                };

                if !ty.nullable && datum.is_null() {
                    // TODO turn this into a stream error
                    panic!("null value in non nullable column");
                }
                self.packer.push(datum);
            }

            let row = self.packer.finish_and_reuse();
            let event = Ok((row, *snapshot_ts, 1));

            co.yield_(event).await;
        }

        client.simple_query("COMMIT;").await.unwrap();

        (slot_name, cur_lsn)
    }

    /// Converts a Tuple received in the replication stream into a Row instance. The logical
    /// replication protocol doesn't use the binary encoding for column values so here we need to
    /// parse the textual form of each column.
    fn row_from_tuple(&mut self, tuple: &Tuple, col_types: &[PgColumn]) -> Row {
        self.packer.clear();

        for (val, ty) in tuple.tuple_data().iter().zip(col_types.iter()) {
            let datum = match val {
                TupleData::Null => Datum::Null,
                TupleData::Toast => Datum::Null, // FIXME
                TupleData::Text(b) => {
                    let s = std::str::from_utf8(&b).unwrap();
                    match ty.scalar_type {
                        PgType::BOOL => parse_datum(s, ScalarType::Bool).unwrap(),
                        PgType::INT4 => parse_datum(s, ScalarType::Int32).unwrap(),
                        PgType::INT8 => parse_datum(s, ScalarType::Int64).unwrap(),
                        PgType::FLOAT4 => parse_datum(s, ScalarType::Float32).unwrap(),
                        PgType::FLOAT8 => parse_datum(s, ScalarType::Float64).unwrap(),
                        PgType::DATE => parse_datum(s, ScalarType::Date).unwrap(),
                        PgType::TIME => parse_datum(s, ScalarType::Time).unwrap(),
                        PgType::TIMESTAMP => parse_datum(s, ScalarType::Timestamp).unwrap(),
                        PgType::TEXT => parse_datum(s, ScalarType::String).unwrap(),
                        PgType::UUID => parse_datum(s, ScalarType::Uuid).unwrap(),
                        ref other => todo!("Unsupported data type {:?}", other),
                    }
                }
            };
            self.packer.push(datum);
        }

        self.packer.finish_and_reuse()
    }

    /// Converts this instance into a Generator. The generator encodes an async state machine that
    /// initially produces the initial snapshot of the table and then atomically switches to the
    /// replication stream starting where the snapshot left of
    async fn into_generator(mut self, timestamper: Timestamper, mut co: Co<StreamItem>) {
        let (rel_id, col_types) = self.table_info().await;

        let conninfo = &self.connector.conn;
        let (mut client, conn) = connect_replication(&conninfo, NoTls, ReplicationMode::Logical)
            .await
            .unwrap();

        // TODO communicate errors back into the stream
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("connection error: {}", e);
            }
        });

        let (slot_name, mut cur_lsn) = self
            .produce_snapshot(&mut client, &col_types, &timestamper, &mut co)
            .await;

        // Start the replication stream at the exact point that the snapshot ended (cur_lsn)
        let mut stream = client
            .start_logical_replication(&slot_name, cur_lsn, &self.plugin)
            .await
            .unwrap();

        let mut time_lease = None;
        while let Some(item) = stream.next().await {
            match item {
                Ok(ReplicationMessage::XLogData(xlog_data)) => {
                    use LogicalReplicationMessage::*;

                    match xlog_data.data() {
                        Begin(_) => {
                            // Lease a tx time
                            time_lease = Some(timestamper.lease().await);
                        }
                        Insert(insert) if insert.rel_id() == rel_id => {
                            let time = time_lease
                                .as_deref()
                                .cloned()
                                .expect("got row event without a prior BEGIN event");
                            let row = self.row_from_tuple(insert.tuple(), &col_types);
                            co.yield_(Ok((row, time, 1))).await;
                        }
                        Update(update) if update.rel_id() == rel_id => {
                            let time = time_lease
                                .as_deref()
                                .cloned()
                                .expect("got row event without a prior BEGIN event");
                            let old_row =
                                self.row_from_tuple(update.old_tuple().unwrap(), &col_types);
                            co.yield_(Ok((old_row, time, -1))).await;

                            let new_row = self.row_from_tuple(update.new_tuple(), &col_types);
                            co.yield_(Ok((new_row, time, 1))).await;
                        }
                        Delete(delete) if delete.rel_id() == rel_id => {
                            let time = time_lease
                                .as_deref()
                                .cloned()
                                .expect("got row event without a prior BEGIN event");
                            let row = self.row_from_tuple(delete.old_tuple().unwrap(), &col_types);
                            co.yield_(Ok((row, time, -1))).await;
                        }
                        Commit(commit) => {
                            cur_lsn = commit.commit_lsn().into();
                            // Release the lease
                            time_lease = None;
                        }
                        Origin(_) | Relation(_) | Type(_) => (),
                        // TODO turn this into a stream error
                        Truncate(_) => panic!("source table got truncated"),
                        // The enum is marked as non_exaustive. Better to be conservative here in
                        // case a new message is relevant to the semantics of our source
                        // TODO: turn into a stream error
                        _ => panic!("unexpected logical replication message"),
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
                            .unwrap();

                        // Since we don't have source persistence we can report that we've safely
                        // consumed everything until cur_lsn
                        stream
                            .as_mut()
                            .standby_status_update(cur_lsn, cur_lsn, cur_lsn, ts, 0)
                            .await
                            .unwrap();
                    }
                }
                // The enum is marked non_exaustive, better be conservative
                // TODO: turn into a stream error
                _ => panic!("Unexpected replication message"),
            }
        }
    }
}

impl SimpleSource for PostgresSimpleSource {
    fn into_stream(self, timestamper: Timestamper) -> BoxStream<'static, StreamItem> {
        Box::pin(Gen::new(move |co| self.into_generator(timestamper, co)))
    }
}
