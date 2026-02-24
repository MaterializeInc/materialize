// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::error::Error;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use futures::{StreamExt, TryStreamExt};
use mz_postgres_util::{Sql, execute, query_one, sql};
use prost::bytes::{BufMut, BytesMut};
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_postgres::types::{Format, IsNull, ToSql, Type, to_sql_checked};

use crate::crypto::AsyncAesDecrypter;
use crate::destination::{
    ColumnMetadata, FIVETRAN_SYSTEM_COLUMN_DELETE, FIVETRAN_SYSTEM_COLUMN_SYNCED, config,
};
use crate::error::{Context, OpError, OpErrorKind};
use crate::fivetran_sdk::{Compression, Encryption, Table, TruncateRequest, WriteBatchRequest};
use crate::utils::{self, AsyncCsvReaderTableAdapter};

pub async fn handle_truncate_table(request: TruncateRequest) -> Result<(), OpError> {
    let delete_before = {
        let utc_delete_before = request
            .utc_delete_before
            .ok_or(OpErrorKind::FieldMissing("utc_delete_before"))?;
        let secs = u64::try_from(utc_delete_before.seconds).map_err(|_| {
            OpErrorKind::InvariantViolated(
                "\"utc_delete_before.seconds\" field out of range".into(),
            )
        })?;
        let nanos = u32::try_from(utc_delete_before.nanos).map_err(|_| {
            OpErrorKind::InvariantViolated("\"utc_delete_before.nanos\" field out of range".into())
        })?;

        SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)
    };

    let (_dbname, client) = config::connect(request.configuration).await?;

    let exists: bool = query_one(
        &client,
        sql!(
            r#"SELECT EXISTS(
                SELECT 1 FROM mz_tables t
                LEFT JOIN mz_schemas s
                ON t.schema_id = s.id
                WHERE s.name = $1 AND t.name = $2
            )"#
        ),
        &[&request.schema_name, &request.table_name],
    )
    .await
    .map(|row| row.get(0))
    .context("checking existence")?;

    // Truncates can happen at any point in time, even if the table hasn't been created yet. We
    // want to no-op in this case.
    if !exists {
        return Ok(());
    }

    let query = match request.soft {
        None => sql!(
            "DELETE FROM {}.{} WHERE {} < $1",
            Sql::ident(&request.schema_name),
            Sql::ident(&request.table_name),
            Sql::ident(&request.synced_column),
        ),
        Some(soft) => sql!(
            "UPDATE {}.{} SET {} = true WHERE {} < $1",
            Sql::ident(&request.schema_name),
            Sql::ident(&request.table_name),
            Sql::ident(&soft.deleted_column),
            Sql::ident(&request.synced_column),
        ),
    };
    execute(&client, query, &[&delete_before])
        .await
        .context("truncating")?;

    Ok(())
}

pub async fn handle_write_batch(request: WriteBatchRequest) -> Result<(), OpError> {
    let table = request.table.ok_or(OpErrorKind::FieldMissing("table"))?;
    if !table.columns.iter().any(|c| c.primary_key) {
        let err = OpErrorKind::InvariantViolated("table has no primary key columns".into());
        return Err(err.into());
    }

    let file_params = request
        .file_params
        .ok_or(OpErrorKind::FieldMissing("file_params"))?;

    let file_config = FileConfig {
        compression: match file_params.compression() {
            Compression::Off => FileCompression::None,
            Compression::Zstd => FileCompression::Zstd,
            Compression::Gzip => FileCompression::Gzip,
        },
        aes_encryption_keys: match file_params.encryption() {
            Encryption::None => None,
            Encryption::Aes => Some(request.keys),
        },
        null_string: file_params.null_string,
        unmodified_string: file_params.unmodified_string,
    };

    let (dbname, client) = config::connect(request.configuration).await?;

    // Note: This ordering of operations is important! Fivetran expects that we run "replace",
    // "update", and then "delete" ops.
    //
    // Note: This isn't part of their documentation but was mentioned in a private conversation.

    replace_files(
        &dbname,
        &request.schema_name,
        &table,
        &file_config,
        &client,
        &request.replace_files,
    )
    .await
    .context("replace files")?;

    update_files(
        &dbname,
        &request.schema_name,
        &table,
        &file_config,
        &client,
        &request.update_files,
    )
    .await
    .context("update files")?;

    delete_files(
        &dbname,
        &request.schema_name,
        &table,
        &file_config,
        &client,
        &request.delete_files,
    )
    .await
    .context("delete files")?;

    Ok(())
}

#[derive(Debug, Clone)]
struct FileConfig {
    compression: FileCompression,
    aes_encryption_keys: Option<BTreeMap<String, Vec<u8>>>,
    null_string: String,
    unmodified_string: String,
}

#[derive(Debug, Clone, Copy)]
enum FileCompression {
    None,
    Gzip,
    Zstd,
}

type AsyncFileReader = Pin<Box<dyn AsyncRead + Send>>;

async fn load_file(file_config: &FileConfig, path: &str) -> Result<AsyncFileReader, OpError> {
    let mut file = File::open(path).await?;

    // Handle encryption.
    let file: Pin<Box<dyn AsyncRead + Send>> = match &file_config.aes_encryption_keys {
        None => Box::pin(file),
        Some(aes_encryption_keys) => {
            // Ensure we have an AES key.
            let aes_key = aes_encryption_keys
                .get(path)
                .ok_or(OpErrorKind::FieldMissing("aes key"))?;

            // The initialization vector is stored in the first 16 bytes of the
            // file.
            let mut iv = [0; 16];
            file.read_exact(&mut iv).await?;

            let decrypter = AsyncAesDecrypter::new(file, aes_key, &iv)?;
            Box::pin(decrypter)
        }
    };

    // Handle compression.
    let file = BufReader::new(file);
    let file: Pin<Box<dyn AsyncRead + Send>> = match file_config.compression {
        FileCompression::None => Box::pin(file),
        FileCompression::Gzip => Box::pin(GzipDecoder::new(file)),
        FileCompression::Zstd => Box::pin(ZstdDecoder::new(file)),
    };

    Ok(file)
}

/// `DELETE` and then `INSERT` for all of the records in `replace_files` based on primary key.
///
/// TODO(benesch): the `DELETE` and `INSERT` are not issued transactionally,
/// so they present as a retraction at one timestamp followed by an insertion
/// at another, rather than presenting as a single update at a single
/// timestamp.
async fn replace_files(
    database: &str,
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    replace_files: &[String],
) -> Result<(), OpError> {
    // Bail early if there isn't any work to do.
    if replace_files.is_empty() {
        return Ok(());
    }

    // Copy into a temporary table, which we then merge into the destination.
    let (qualified_temp_table_name, columns, guard) =
        get_scratch_table(database, schema, table, client).await?;
    let row_count = copy_files(
        file_config,
        replace_files,
        client,
        table,
        &qualified_temp_table_name,
    )
    .await
    .context("replace_files")?;

    // Bail early if there isn't any work to do.
    if row_count == 0 {
        return Ok(());
    }

    let qualified_table_name = sql!("{}.{}", Sql::ident(schema), Sql::ident(&table.name));

    // First delete all of the matching rows.
    let matching_cols = Sql::join(
        columns
            .iter()
            .filter(|col| col.is_primary)
            .map(|col| col.ident.clone()),
        ",",
    );
    let delete_stmt = sql!(
        "DELETE FROM {} WHERE ({}) IN (SELECT {} FROM {})",
        qualified_table_name.clone(),
        matching_cols.clone(),
        matching_cols,
        qualified_temp_table_name.clone(),
    );
    let rows_changed = execute(client, delete_stmt, &[]).await?;
    tracing::info!(rows_changed, "deleted rows from {qualified_table_name}");

    // Then re-insert rows.
    let all_cols = Sql::join(columns.iter().map(|col| col.ident.clone()), ",");
    let insert_stmt = sql!(
        "INSERT INTO {} ({}) SELECT {} FROM {}",
        qualified_table_name.clone(),
        all_cols.clone(),
        all_cols,
        qualified_temp_table_name.clone(),
    );
    let rows_changed = execute(client, insert_stmt, &[]).await?;
    tracing::info!(rows_changed, "inserted rows to {qualified_table_name}");

    // Clear out our scratch table.
    guard.clear().await.context("clearing guard")?;

    Ok(())
}

/// For each record in all `update_files`, `UPDATE` all columns that are not the "unmodified
/// string" to their new values provided in the record, based on primary key columns.
async fn update_files(
    _database: &str,
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    update_files: &[String],
) -> Result<(), OpError> {
    // TODO(benesch): this is hideously inefficient.

    let mut assignments: Vec<Sql> = vec![];
    let mut filters: Vec<Sql> = vec![];

    for (i, column) in table.columns.iter().enumerate() {
        let name = Sql::ident(&column.name);
        let param = Sql::param(i + 1);
        if column.primary_key {
            filters.push(sql!("{} = {}", name, param));
        } else {
            assignments.push(sql!(
                "{} = CASE {}::text WHEN {} THEN {} ELSE {}::{} END",
                name.clone(),
                param.clone(),
                Sql::literal(&file_config.unmodified_string),
                name,
                param,
                Sql::new(utils::to_materialize_type(column.r#type())?),
            ));
        }
    }

    let update_stmt = sql!(
        "UPDATE {}.{} SET {} WHERE {}",
        Sql::ident(schema),
        Sql::ident(&table.name),
        Sql::join(assignments, ","),
        Sql::join(filters, " AND "),
    );

    let update_stmt = client
        .prepare(update_stmt.as_str())
        .await
        .context("preparing update statement")?;

    for path in update_files {
        let file = load_file(file_config, path)
            .await
            .with_context(|| format!("loading update file {path}"))?;

        update_file(file_config, client, &update_stmt, table, file)
            .await
            .with_context(|| format!("handling update file {path}"))?;
    }
    Ok(())
}

async fn update_file(
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    update_stmt: &tokio_postgres::Statement,
    table: &Table,
    file: AsyncFileReader,
) -> Result<(), OpError> {
    // Map the column order from the CSV to the order of the table.
    let adapted_stream = AsyncCsvReaderTableAdapter::new(file, table)
        .await?
        .into_stream();
    let mut adapted_stream = std::pin::pin!(adapted_stream);

    while let Some(record) = adapted_stream.try_next().await? {
        let params = record.iter().map(|value| TextFormatter {
            value,
            null_string: &file_config.null_string,
        });
        client.execute_raw(update_stmt, params).await?;
    }
    Ok(())
}

/// "Soft deletes" rows by setting the [`FIVETRAN_SYSTEM_COLUMN_SYNCED`] to `true`, based on all
/// primary keys.
async fn delete_files(
    database: &str,
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    delete_files: &[String],
) -> Result<(), OpError> {
    // TODO(parkmycar): Make sure table exists.
    // TODO(parkmycar): Retry transient errors.

    // Bail early if there is no work to do.
    if delete_files.is_empty() {
        return Ok(());
    }

    // Copy into a temporary table, which we then merge into the destination.
    let (qualified_temp_table_name, columns, guard) =
        get_scratch_table(database, schema, table, client).await?;
    let row_count = copy_files(
        file_config,
        delete_files,
        client,
        table,
        &qualified_temp_table_name,
    )
    .await?;

    // Skip the update if there are no rows to delete!
    if row_count == 0 {
        return Ok(());
    }

    // Mark rows as deleted by "merging" our temporary table into the destination table.
    //
    // HACKY: We want to update the "_fivetran_synced" column for all of the rows we marked as
    // deleted, but don't have a way to read from the temp table that would allow this in an
    // `UPDATE` statement.
    let synced_time_stmt = sql!(
        "SELECT MAX({}) FROM {}",
        Sql::ident(FIVETRAN_SYSTEM_COLUMN_SYNCED),
        qualified_temp_table_name.clone(),
    );
    let synced_time_row = query_one(client, synced_time_stmt, &[])
        .await
        .context("get MAX _fivetran_synced")?;
    let synced_time: SystemTime = synced_time_row.try_get(0)?;

    let qualified_table_name = sql!("{}.{}", Sql::ident(schema), Sql::ident(&table.name));
    let matching_cols = Sql::join(
        columns
            .iter()
            .filter(|col| col.is_primary)
            .map(|col| col.ident.clone()),
        ",",
    );
    let merge_stmt = sql!(
        "UPDATE {} SET {} = true, {} = $1 WHERE ({}) IN (SELECT {} FROM {})",
        qualified_table_name.clone(),
        Sql::ident(FIVETRAN_SYSTEM_COLUMN_DELETE),
        Sql::ident(FIVETRAN_SYSTEM_COLUMN_SYNCED),
        matching_cols.clone(),
        matching_cols,
        qualified_temp_table_name.clone(),
    );
    let total_count = execute(client, merge_stmt, &[&synced_time])
        .await
        .context("update deletes")?;
    tracing::info!(?total_count, "altered rows in {qualified_table_name}");

    // Clear our scratch table.
    guard.clear().await.context("clearing guard")?;

    Ok(())
}

#[must_use = "Need to clear the scratch table once you're done using it."]
struct ScratchTableGuard<'a> {
    client: &'a tokio_postgres::Client,
    qualified_name: Sql,
}

impl<'a> ScratchTableGuard<'a> {
    /// Deletes all the rows from the associated scratch table.
    async fn clear(self) -> Result<(), OpError> {
        let rows_cleared = execute(
            self.client,
            sql!("DELETE FROM {}", self.qualified_name.clone()),
            &[],
        )
        .await
        .map_err(OpErrorKind::from)
        .context("scratch table guard")?;
        tracing::info!(?rows_cleared, table_name = %self.qualified_name, "guard cleared table");

        Ok(())
    }
}

/// Gets the existing "scratch" table that we can copy data into, or creates one if it doesn't
/// exist.
async fn get_scratch_table<'a>(
    database: &str,
    schema: &str,
    table: &Table,
    client: &'a tokio_postgres::Client,
) -> Result<(Sql, Vec<ColumnMetadata>, ScratchTableGuard<'a>), OpError> {
    static SCRATCH_TABLE_SCHEMA: &str = "_mz_fivetran_scratch";

    execute(
        client,
        sql!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            Sql::ident(SCRATCH_TABLE_SCHEMA)
        ),
        &[],
    )
    .await
    .map_err(OpErrorKind::from)
    .context("creating scratch schema")?;

    // To make sure the table name is unique, and under the Materialize identifier limits, we name
    // the scratch table with a hash.
    let mut hasher = Sha256::new();
    hasher.update(&format!("{database}.{schema}.{}", table.name));
    let scratch_table_name = format!("{:x}", hasher.finalize());

    let qualified_scratch_table_name = sql!(
        "{}.{}",
        Sql::ident(SCRATCH_TABLE_SCHEMA),
        Sql::ident(&scratch_table_name)
    );

    let columns = table
        .columns
        .iter()
        .map(ColumnMetadata::try_from)
        .collect::<Result<Vec<_>, OpError>>()?;

    let create_scratch_table = || async {
        let defs = Sql::join(columns.iter().map(|col| col.to_column_def()), ",");
        execute(
            client,
            sql!(
                "CREATE TABLE {} ({})",
                qualified_scratch_table_name.clone(),
                defs
            ),
            &[],
        )
        .await
        .map_err(OpErrorKind::from)
        .context("creating scratch table")?;

        // Leave a COMMENT on the scratch table for debug-ability.
        let comment = format!(
            "Fivetran scratch table for {database}.{schema}.{}",
            table.name
        );
        execute(
            client,
            sql!(
                "COMMENT ON TABLE {} IS {}",
                qualified_scratch_table_name.clone(),
                Sql::literal(&comment)
            ),
            &[],
        )
        .await
        .map_err(OpErrorKind::from)
        .context("comment scratch table")?;

        Ok::<_, OpError>(())
    };

    // Check if the scratch table already exists.
    let existing_scratch_table =
        super::ddl::describe_table(client, database, SCRATCH_TABLE_SCHEMA, &scratch_table_name)
            .await
            .context("describe table")?;

    match existing_scratch_table {
        None => {
            tracing::info!(
                %schema,
                ?table,
                %qualified_scratch_table_name,
                "creating new scratch table",
            );
            create_scratch_table().await.context("create new table")?;
        }
        Some(scratch) => {
            // Make sure the scratch table is compatible with the destination table.
            let table_columns: BTreeMap<_, _> = table
                .columns
                .iter()
                .enumerate()
                .map(|(pos, col)| (&col.name, (col.r#type, &col.params, pos)))
                .collect();
            let scratch_columns: BTreeMap<_, _> = scratch
                .columns
                .iter()
                .enumerate()
                .map(|(pos, col)| (&col.name, (col.r#type, &col.params, pos)))
                .collect();

            if table_columns != scratch_columns {
                tracing::warn!(
                    %schema,
                    ?table,
                    ?scratch,
                    %qualified_scratch_table_name,
                    "recreate scratch table",
                );

                execute(
                    client,
                    sql!("DROP TABLE {}", qualified_scratch_table_name.clone()),
                    &[],
                )
                .await
                .map_err(OpErrorKind::from)
                .context("dropping scratch table")?;

                create_scratch_table().await.context("recreate table")?;
            } else {
                tracing::info!(
                    %schema,
                    ?table,
                    %qualified_scratch_table_name,
                    "clear and reuse scratch table",
                );

                let rows_cleared = execute(
                    client,
                    sql!("DELETE FROM {}", qualified_scratch_table_name.clone()),
                    &[],
                )
                .await
                .map_err(OpErrorKind::from)
                .context("clearing scratch table")?;
                tracing::info!(?rows_cleared, %qualified_scratch_table_name, "cleared table");
            }
        }
    }

    // Verify that our table is empty.
    let rows: i64 = query_one(
        client,
        sql!(
            "SELECT COUNT(*) FROM {}",
            qualified_scratch_table_name.clone()
        ),
        &[],
    )
    .await
    .map(|row| row.get(0))
    .map_err(OpErrorKind::from)
    .context("validate scratch table")?;
    if rows != 0 {
        return Err(OpErrorKind::InvariantViolated(format!(
            "scratch table had non-zero number of rows: {rows}"
        ))
        .into());
    }

    let guard = ScratchTableGuard {
        client,
        qualified_name: qualified_scratch_table_name.clone(),
    };

    Ok((qualified_scratch_table_name, columns, guard))
}

/// Copies a CSV file into a temporary table using the `COPY FROM` protocol.
///
/// It is assumed that the provided CSV files have the same number of columns as the referenced
/// table.
async fn copy_files(
    file_config: &FileConfig,
    files: &[String],
    client: &tokio_postgres::Client,
    table: &Table,
    temporary_table: &Sql,
) -> Result<u64, OpError> {
    let mut total_row_count = 0;

    // Stream the files into the COPY FROM sink.
    for path in files {
        tracing::info!(?path, "starting copy");

        // Create a Sink which we can stream the CSV files into.
        let copy_in_stmt = sql!(
            "COPY {} FROM STDIN WITH (FORMAT CSV, HEADER false, NULL {})",
            temporary_table.clone(),
            Sql::literal(&file_config.null_string),
        );
        let sink = client.copy_in(copy_in_stmt.as_str()).await?;
        let mut sink = std::pin::pin!(sink);

        {
            // Create a CSV Writer that will serialize ByteRecords into the COPY FROM sink.
            let sink_writer = utils::CopyIntoAsyncWrite::new(sink.as_mut());
            let mut csv_sink = csv_async::AsyncWriterBuilder::new()
                .has_headers(false)
                .create_writer(sink_writer);

            // Open the CSV file, returning an AsyncReader.
            let file = load_file(file_config, path)
                .await
                .with_context(|| format!("loading delete file {path}"))?;
            // Map the column order from the CSV to the order of the table.
            let adapter = AsyncCsvReaderTableAdapter::new(file, table)
                .await
                .context("creating mapping adapter")?;

            // Write all of the ByteRecords into the sink.
            let mut record_stream = adapter.into_stream();
            while let Some(maybe_record) = record_stream.next().await {
                let record = maybe_record?;
                csv_sink
                    .write_byte_record(&record)
                    .await
                    .context("writing record")?;
                csv_sink.flush().await.context("flushing record")?;
            }
        }

        let row_count = sink.as_mut().finish().await.context("closing sink")?;
        tracing::info!(?path, row_count, "copied rows into {temporary_table}");

        total_row_count += row_count;
    }

    Ok(total_row_count)
}

#[derive(Debug)]
struct TextFormatter<'a> {
    value: &'a [u8],
    null_string: &'a str,
}

impl<'a> ToSql for TextFormatter<'a> {
    fn to_sql(&self, _: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        if self.value == self.null_string.as_bytes() {
            Ok(IsNull::Yes)
        } else {
            out.put_slice(self.value);
            Ok(IsNull::No)
        }
    }

    fn accepts(_: &Type) -> bool {
        true
    }

    to_sql_checked!();

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }
}
