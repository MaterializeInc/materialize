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
use itertools::Itertools;
use mz_sql_parser::ast::{Ident, UnresolvedItemName};
use postgres_protocol::escape;
use prost::bytes::{BufMut, BytesMut};
use sha2::{Digest, Sha256};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};

use crate::crypto::AsyncAesDecrypter;
use crate::destination::{
    config, ColumnMetadata, FIVETRAN_SYSTEM_COLUMN_DELETE, FIVETRAN_SYSTEM_COLUMN_SYNCED,
};
use crate::error::{Context, OpError, OpErrorKind};
use crate::fivetran_sdk::write_batch_request::FileParams;
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

    let exists_stmt = r#"
        SELECT EXISTS(
            SELECT 1 FROM mz_tables t
            LEFT JOIN mz_schemas s
            ON t.schema_id = s.id
            WHERE s.name = $1 AND t.name = $2
        )"#
    .to_string();
    let exists: bool = client
        .query_one(&exists_stmt, &[&request.schema_name, &request.table_name])
        .await
        .map(|row| row.get(0))
        .context("checking existence")?;

    // Truncates can happen at any point in time, even if the table hasn't been created yet. We
    // want to no-op in this case.
    if !exists {
        return Ok(());
    }

    let sql = match request.soft {
        None => format!(
            "DELETE FROM {}.{} WHERE {} < $1",
            escape::escape_identifier(&request.schema_name),
            escape::escape_identifier(&request.table_name),
            escape::escape_identifier(&request.synced_column),
        ),
        Some(soft) => format!(
            "UPDATE {}.{} SET {} = true WHERE {} < $1",
            escape::escape_identifier(&request.schema_name),
            escape::escape_identifier(&request.table_name),
            escape::escape_identifier(&soft.deleted_column),
            escape::escape_identifier(&request.synced_column),
        ),
    };
    client
        .execute(&sql, &[&delete_before])
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

    let FileParams::Csv(csv_file_params) = request
        .file_params
        .ok_or(OpErrorKind::FieldMissing("file_params"))?;

    let file_config = FileConfig {
        compression: match csv_file_params.compression() {
            Compression::Off => FileCompression::None,
            Compression::Zstd => FileCompression::Zstd,
            Compression::Gzip => FileCompression::Gzip,
        },
        aes_encryption_keys: match csv_file_params.encryption() {
            Encryption::None => None,
            Encryption::Aes => Some(request.keys),
        },
        null_string: csv_file_params.null_string,
        unmodified_string: csv_file_params.unmodified_string,
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

    let qualified_table_name = format!(
        "{}.{}",
        escape::escape_identifier(schema),
        escape::escape_identifier(&table.name)
    );

    // First delete all of the matching rows.
    let matching_cols = columns.iter().filter(|col| col.is_primary);
    let delete_stmt = format!(
        r#"
        DELETE FROM {qualified_table_name}
        WHERE ({cols}) IN (
            SELECT {cols}
            FROM {qualified_temp_table_name}
        )"#,
        cols = matching_cols.map(|col| &col.escaped_name).join(","),
    );
    let rows_changed = client.execute(&delete_stmt, &[]).await?;
    tracing::info!(rows_changed, "deleted rows from {qualified_table_name}");

    // Then re-insert rows.
    let insert_stmt = format!(
        r#"
        INSERT INTO {qualified_table_name} ({cols})
        SELECT {cols} FROM {qualified_temp_table_name}
        "#,
        cols = columns.iter().map(|col| &col.escaped_name).join(","),
    );
    let rows_changed = client.execute(&insert_stmt, &[]).await?;
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

    let mut assignments = vec![];
    let mut filters = vec![];

    for (i, column) in table.columns.iter().enumerate() {
        if column.primary_key {
            filters.push(format!(
                "{} = ${}",
                escape::escape_identifier(&column.name),
                i + 1
            ));
        } else {
            assignments.push(format!(
                "{name} = CASE ${p}::text WHEN {unmodified_string} THEN {name} ELSE ${p}::{ty} END",
                name = escape::escape_identifier(&column.name),
                p = i + 1,
                unmodified_string = escape::escape_literal(&file_config.unmodified_string),
                ty = utils::to_materialize_type(column.r#type())?,
            ));
        }
    }

    let update_stmt = format!(
        "UPDATE {}.{} SET {} WHERE {}",
        escape::escape_identifier(schema),
        escape::escape_identifier(&table.name),
        assignments.join(","),
        filters.join(" AND "),
    );

    let update_stmt = client
        .prepare(&update_stmt)
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
    let synced_time_stmt = format!(
        "SELECT MAX({synced_col}) FROM {qualified_temp_table_name}",
        synced_col = escape::escape_identifier(FIVETRAN_SYSTEM_COLUMN_SYNCED)
    );
    let synced_time: SystemTime = client
        .query_one(&synced_time_stmt, &[])
        .await
        .and_then(|row| row.try_get(0))
        .context("get MAX _fivetran_synced")?;

    let qualified_table_name =
        UnresolvedItemName::qualified(&[Ident::new(schema)?, Ident::new(&table.name)?]);
    let matching_cols = columns.iter().filter(|col| col.is_primary);
    let merge_stmt = format!(
        r#"
        UPDATE {qualified_table_name}
        SET {deleted_col} = true, {synced_col} = $1
        WHERE ({cols}) IN (
            SELECT {cols}
            FROM {qualified_temp_table_name}
        )"#,
        deleted_col = escape::escape_identifier(FIVETRAN_SYSTEM_COLUMN_DELETE),
        synced_col = escape::escape_identifier(FIVETRAN_SYSTEM_COLUMN_SYNCED),
        cols = matching_cols.map(|col| &col.escaped_name).join(","),
    );
    let total_count = client
        .execute(&merge_stmt, &[&synced_time])
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
    qualified_name: UnresolvedItemName,
}

impl<'a> ScratchTableGuard<'a> {
    /// Deletes all the rows from the associated scratch table.
    async fn clear(self) -> Result<(), OpError> {
        let clear_table_stmt = format!("DELETE FROM {}", self.qualified_name);
        let rows_cleared = self
            .client
            .execute(&clear_table_stmt, &[])
            .await
            .map_err(OpErrorKind::TemporaryResource)
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
) -> Result<
    (
        UnresolvedItemName,
        Vec<ColumnMetadata>,
        ScratchTableGuard<'a>,
    ),
    OpError,
> {
    static SCRATCH_TABLE_SCHEMA: &str = "_mz_fivetran_scratch";

    let create_schema_stmt = format!("CREATE SCHEMA IF NOT EXISTS {SCRATCH_TABLE_SCHEMA}");
    client
        .execute(&create_schema_stmt, &[])
        .await
        .map_err(OpErrorKind::TemporaryResource)
        .context("creating scratch schema")?;

    // To make sure the table name is unique, and under the Materialize identifier limits, we name
    // the scratch table with a hash.
    let mut hasher = Sha256::new();
    hasher.update(&format!("{database}.{schema}.{}", table.name));
    let scratch_table_name = format!("{:x}", hasher.finalize());

    let qualified_scratch_table_name = UnresolvedItemName::qualified(&[
        Ident::new(SCRATCH_TABLE_SCHEMA).context("scratch schema")?,
        Ident::new(&scratch_table_name).context("scratch table_name")?,
    ]);

    let columns = table
        .columns
        .iter()
        .map(ColumnMetadata::try_from)
        .collect::<Result<Vec<_>, OpError>>()?;

    let create_scratch_table = || async {
        let defs = columns.iter().map(|col| col.to_column_def()).join(",");
        let create_table_stmt = format!("CREATE TABLE {qualified_scratch_table_name} ({defs})");
        client
            .execute(&create_table_stmt, &[])
            .await
            .map_err(OpErrorKind::TemporaryResource)
            .context("creating scratch table")?;

        // Leave a COMMENT on the scratch table for debug-ability.
        let comment = format!(
            "Fivetran scratch table for {database}.{schema}.{}",
            table.name
        );
        let comment_stmt = format!(
            "COMMENT ON TABLE {qualified_scratch_table_name} IS {comment}",
            comment = escape::escape_literal(&comment)
        );
        client
            .execute(&comment_stmt, &[])
            .await
            .map_err(OpErrorKind::TemporaryResource)
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
                .map(|(pos, col)| (&col.name, (col.r#type, &col.decimal, pos)))
                .collect();
            let scratch_columns: BTreeMap<_, _> = scratch
                .columns
                .iter()
                .enumerate()
                .map(|(pos, col)| (&col.name, (col.r#type, &col.decimal, pos)))
                .collect();

            if table_columns != scratch_columns {
                tracing::warn!(
                    %schema,
                    ?table,
                    ?scratch,
                    %qualified_scratch_table_name,
                    "recreate scratch table",
                );

                let drop_table_stmt = format!("DROP TABLE {qualified_scratch_table_name}");
                client
                    .execute(&drop_table_stmt, &[])
                    .await
                    .map_err(OpErrorKind::TemporaryResource)
                    .context("dropping scratch table")?;

                create_scratch_table().await.context("recreate table")?;
            } else {
                tracing::info!(
                    %schema,
                    ?table,
                    %qualified_scratch_table_name,
                    "clear and reuse scratch table",
                );

                let clear_table_stmt = format!("DELETE FROM {qualified_scratch_table_name}");
                let rows_cleared = client
                    .execute(&clear_table_stmt, &[])
                    .await
                    .map_err(OpErrorKind::TemporaryResource)
                    .context("clearing scratch table")?;
                tracing::info!(?rows_cleared, %qualified_scratch_table_name, "cleared table");
            }
        }
    }

    // Verify that our table is empty.
    let count_stmt = format!("SELECT COUNT(*) FROM {qualified_scratch_table_name}");
    let rows: i64 = client
        .query_one(&count_stmt, &[])
        .await
        .map(|row| row.get(0))
        .map_err(OpErrorKind::TemporaryResource)
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
    temporary_table: &UnresolvedItemName,
) -> Result<u64, OpError> {
    let mut total_row_count = 0;

    // Stream the files into the COPY FROM sink.
    for path in files {
        tracing::info!(?path, "starting copy");

        // Create a Sink which we can stream the CSV files into.
        let copy_in_stmt = format!(
            "COPY {temporary_table} FROM STDIN WITH (FORMAT CSV, HEADER false, NULL {null_value})",
            null_value = escape::escape_literal(&file_config.null_string),
        );
        let sink = client.copy_in(&copy_in_stmt).await?;
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
