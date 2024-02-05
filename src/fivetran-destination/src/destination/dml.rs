// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::error::Error;
use std::pin::Pin;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context};
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use futures::{SinkExt, TryStreamExt};
use itertools::Itertools;
use mz_ore::error::ErrorExt;
use mz_sql_parser::ast::{Ident, UnresolvedItemName};
use postgres_protocol::escape;
use prost::bytes::{BufMut, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};
use tokio_util::io::ReaderStream;

use crate::crypto::AsyncAesDecrypter;
use crate::destination::config;
use crate::fivetran_sdk::write_batch_request::FileParams;
use crate::fivetran_sdk::{
    Compression, Encryption, Table, TruncateRequest, TruncateResponse, WriteBatchRequest,
    WriteBatchResponse,
};
use crate::utils;

/// Tracks if a row has been "soft deleted" if this column to true.
const FIVETRAN_SYSTEM_COLUMN_DELETE: &str = "_fivetran_deleted";
/// Tracks the last time this Row was modified by Fivetran.
const FIVETRAN_SYSTEM_COLUMN_SYNCED: &str = "_fivetran_synced";
/// Fivetran will synthesize a primary key column when one doesn't exist.
const FIVETRAN_SYSTEM_COLUMN_ID: &str = "_fivetran_id";

pub async fn handle_truncate_request(
    request: TruncateRequest,
) -> Result<TruncateResponse, anyhow::Error> {
    use crate::fivetran_sdk::truncate_response::Response;

    let response = match truncate_table(request).await {
        Ok(()) => Response::Success(true),
        Err(e) => Response::Failure(e.display_with_causes().to_string()),
    };
    Ok(TruncateResponse {
        response: Some(response),
    })
}

pub async fn handle_write_batch_request(
    request: WriteBatchRequest,
) -> Result<WriteBatchResponse, anyhow::Error> {
    use crate::fivetran_sdk::write_batch_response::Response;

    let response = match write_batch(request).await {
        Ok(()) => Response::Success(true),
        Err(e) => Response::Failure(e.display_with_causes().to_string()),
    };
    Ok(WriteBatchResponse {
        response: Some(response),
    })
}

async fn truncate_table(request: TruncateRequest) -> Result<(), anyhow::Error> {
    let delete_before = {
        let Some(utc_delete_before) = request.utc_delete_before else {
            bail!("internal error: TruncateRequest missing \"utc_delete_before\" field");
        };

        let secs = u64::try_from(utc_delete_before.seconds).map_err(|_| {
            anyhow!(
                "internal error: TruncateRequest \"utc_delete_before.seconds\" field out of range"
            )
        })?;
        let nanos = u32::try_from(utc_delete_before.nanos).map_err(|_| {
            anyhow!(
                "internal error: TruncateRequest \"utc_delete_before.nanos\" field out of range"
            )
        })?;

        SystemTime::UNIX_EPOCH + Duration::new(secs, nanos)
    };

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

    let (_dbname, client) = config::connect(request.configuration).await?;
    client.execute(&sql, &[&delete_before]).await?;
    Ok(())
}

async fn write_batch(request: WriteBatchRequest) -> Result<(), anyhow::Error> {
    let Some(table) = request.table else {
        bail!("internal error: WriteBatchRequest missing \"table\" field");
    };

    if !table.columns.iter().any(|c| c.primary_key) {
        bail!("table has no primary key columns");
    }

    let Some(FileParams::Csv(csv_file_params)) = request.file_params else {
        bail!("internal error: WriteBatchRequest missing \"file_params\" field");
    };

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

    let (_dbname, client) = config::connect(request.configuration).await?;

    // Note: This ordering of operations is important! Fivetran expects that we run "replace",
    // "update", and then "delete" ops.
    //
    // Note: This isn't part of their documentation but was mentioned in a private conversation.

    replace_files(
        &request.schema_name,
        &table,
        &file_config,
        &client,
        &request.replace_files,
    )
    .await?;

    update_files(
        &request.schema_name,
        &table,
        &file_config,
        &client,
        &request.update_files,
    )
    .await?;

    delete_files(
        &request.schema_name,
        &table,
        &file_config,
        &client,
        &request.delete_files,
    )
    .await?;

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
type AsyncCsvReader = csv_async::AsyncReader<Pin<Box<dyn AsyncRead + Send>>>;

async fn load_file(file_config: &FileConfig, path: &str) -> Result<AsyncFileReader, anyhow::Error> {
    let mut file = File::open(path)
        .await
        .context("internal error: opening file")?;

    // Handle encryption.
    let file: Pin<Box<dyn AsyncRead + Send>> = match &file_config.aes_encryption_keys {
        None => Box::pin(file),
        Some(aes_encryption_keys) => {
            // Ensure we have an AES key.
            let Some(aes_key) = aes_encryption_keys.get(path) else {
                bail!("internal error: aes key missing");
            };

            // The initialization vector is stored in the first 16 bytes of the
            // file.
            let mut iv = [0; 16];
            file.read_exact(&mut iv)
                .await
                .context("internal error: reading initialization vector")?;

            let decrypter = AsyncAesDecrypter::new(file, aes_key, &iv)
                .context("internal error: constructing AES decrypter")?;
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
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    replace_files: &[String],
) -> Result<(), anyhow::Error> {
    // Bail early if there isn't any work to do.
    if replace_files.is_empty() {
        return Ok(());
    }

    // Copy into a temporary table, which we then merge into the destination.
    let (qualified_temp_table_name, columns) = create_temporary_table(table, client).await?;
    let row_count = copy_files(
        file_config,
        replace_files,
        client,
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
        cols = matching_cols.map(|col| &col.name).join(","),
    );
    let rows_changed = client.execute(&delete_stmt, &[]).await?;
    tracing::info!(rows_changed, "deleted rows from {qualified_table_name}");

    // Then re-insert rows.
    let insert_stmt = format!(
        r#"
        INSERT INTO {qualified_table_name} ({cols})
        SELECT {cols} FROM {qualified_temp_table_name}
        "#,
        cols = columns.iter().map(|col| &col.name).join(","),
    );
    let rows_changed = client.execute(&insert_stmt, &[]).await?;
    tracing::info!(rows_changed, "inserted rows to {qualified_table_name}");

    Ok(())
}

/// For each record in all `update_files`, `UPDATE` all columns that are not the "unmodified
/// string" to their new values provided in the record, based on primary key columns.
async fn update_files(
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    update_files: &[String],
) -> Result<(), anyhow::Error> {
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
        .context("internal error: preparing update statement")?;

    for path in update_files {
        let file = load_file(file_config, path)
            .await
            .with_context(|| format!("loading update file {path}"))?;
        let reader = csv_async::AsyncReaderBuilder::new().create_reader(file);

        update_file(file_config, client, &update_stmt, reader)
            .await
            .with_context(|| format!("handling update file {path}"))?;
    }
    Ok(())
}

async fn update_file(
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    update_stmt: &tokio_postgres::Statement,
    reader: AsyncCsvReader,
) -> Result<(), anyhow::Error> {
    let mut stream = reader.into_byte_records();
    while let Some(record) = stream.try_next().await? {
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
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    delete_files: &[String],
) -> Result<(), anyhow::Error> {
    // TODO(parkmycar): Make sure table exists.
    // TODO(parkmycar): Retry transient errors.

    // Bail early if there is no work to do.
    if delete_files.is_empty() {
        return Ok(());
    }

    // Copy into a temporary table, which we then merge into the destination.
    let (qualified_temp_table_name, columns) = create_temporary_table(table, client).await?;
    let row_count = copy_files(
        file_config,
        delete_files,
        client,
        &qualified_temp_table_name,
    )
    .await
    .context("delete_files")?;

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
        .and_then(|row| row.try_get(0))?;

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
        cols = matching_cols.map(|col| &col.name).join(","),
    );
    let total_count = client.execute(&merge_stmt, &[&synced_time]).await?;
    tracing::info!(?total_count, "altered rows in {qualified_table_name}");

    Ok(())
}

/// Creates a `TEMPORARY` table that will get dropped at the end of the SQL session, with the same
/// format as the provided [`Table`].
async fn create_temporary_table(
    table: &Table,
    client: &tokio_postgres::Client,
) -> Result<(UnresolvedItemName, Vec<ColumnMetadata>), anyhow::Error> {
    // This temporary table names to be a valid identifier within Materialize, and a table with the
    // same name must not already exist in the provided schema.
    let prefix = format!("fivetran_temp_{}_", mz_ore::id_gen::temp_id());
    let mut temp_table_name = Ident::new_lossy(prefix);
    temp_table_name.append_lossy(&table.name);

    // Note: temporary items are all created in the same schema.
    let qualified_temp_table_name = UnresolvedItemName::qualified(&[temp_table_name]);

    let columns = table
        .columns
        .iter()
        .map(|col| {
            let mut ty: Cow<'static, str> = utils::to_materialize_type(col.r#type())?.into();
            if let Some(d) = &col.decimal {
                ty.to_mut()
                    .push_str(&format!("({}, {})", d.precision, d.scale));
            }

            Ok(ColumnMetadata {
                name: col.name.to_string(),
                ty,
                is_primary: col.primary_key || col.name.to_lowercase() == FIVETRAN_SYSTEM_COLUMN_ID,
            })
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;

    // Create the temporary table.
    let defs = columns
        .iter()
        .map(|col| format!("{} {}", col.name, col.ty))
        .join(",");
    let create_table_stmt = format!("CREATE TEMPORARY TABLE {qualified_temp_table_name} ({defs})");
    client.execute(&create_table_stmt, &[]).await?;

    Ok((qualified_temp_table_name, columns))
}

/// Copies a CSV file into a temporary table using the `COPY FROM` protocol.
///
/// It is assumed that the provided CSV files have the same number of columns as the referenced
/// table.
async fn copy_files(
    file_config: &FileConfig,
    files: &[String],
    client: &tokio_postgres::Client,
    temporary_table: &UnresolvedItemName,
) -> Result<u64, anyhow::Error> {
    // Create a Sink which we can stream the CSV files into.
    let copy_in_stmt = format!(
        "COPY {temporary_table} FROM STDIN WITH (FORMAT CSV, HEADER true, NULL {null_value})",
        null_value = escape::escape_literal(&file_config.null_string),
    );
    let sink = client.copy_in(&copy_in_stmt).await?;
    let mut sink = std::pin::pin!(sink);

    // Stream the files into the COPY FROM sink.
    for path in files {
        let file = load_file(file_config, path)
            .await
            .with_context(|| format!("loading delete file {path}"))?;
        let mut file_stream = ReaderStream::new(file).map_err(anyhow::Error::from);

        (&mut sink)
            .sink_map_err(anyhow::Error::from)
            .send_all(&mut file_stream)
            .await
            .context("sinking data")?;
    }
    let row_count = sink.finish().await.context("closing sink")?;
    tracing::info!(row_count, "copied rows into {temporary_table}");

    Ok(row_count)
}

/// Metadata about a column that is relevant to operations peformed by the destination.
#[derive(Debug)]
struct ColumnMetadata {
    /// Name of the column in the destination table.
    name: String,
    /// Type of the column in the destination table.
    ty: Cow<'static, str>,
    /// Is this column a primary key.
    is_primary: bool,
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
