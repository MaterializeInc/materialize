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
use futures::TryStreamExt;
use itertools::Itertools;
use mz_ore::error::ErrorExt;
use postgres_protocol::escape;
use prost::bytes::{BufMut, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio_postgres::types::{to_sql_checked, Format, IsNull, ToSql, Type};

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
    // See: https://materializeinc.slack.com/archives/C060KAR4802/p1706726115147059?thread_ts=1706720574.377339&cid=C060KAR4802

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

type AsyncCsvReader = csv_async::AsyncReader<Pin<Box<dyn AsyncRead + Send>>>;

async fn load_file(file_config: &FileConfig, path: &str) -> Result<AsyncCsvReader, anyhow::Error> {
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

    // Build CSV reader.
    let file = csv_async::AsyncReaderBuilder::new().create_reader(file);

    Ok(file)
}

async fn replace_files(
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    replace_files: &[String],
) -> Result<(), anyhow::Error> {
    // For each record in each replace file, we execute a `DELETE` to remove the
    // old row with that value, matching based on all primary key columns, and
    // then execute an `INSERT` to insert the new row.

    // TODO(benesch): this is hideously inefficient.

    // TODO(benesch): the `DELETE` and `INSERT` are not issued transactionally,
    // so they present as a retraction at one timestamp followed by an insertion
    // at another, rather than presenting as a single update at a single
    // timestamp.

    let mut key = vec![];
    let mut delete_stmt = format!(
        "DELETE FROM {}.{} WHERE ",
        escape::escape_identifier(schema),
        escape::escape_identifier(&table.name),
    );
    let mut p = 1;
    for (i, c) in table.columns.iter().enumerate() {
        if c.primary_key {
            key.push(i);
            if p > 1 {
                delete_stmt += " AND "
            }
            delete_stmt += &format!("{} = ${p}", escape::escape_identifier(&c.name));
            p += 1;
        }
    }

    let insert_stmt = format!(
        "INSERT INTO {}.{} ({}) VALUES ({})",
        escape::escape_identifier(schema),
        escape::escape_identifier(&table.name),
        table
            .columns
            .iter()
            .map(|c| escape::escape_identifier(&c.name))
            .join(","),
        (1..=table.columns.len()).map(|p| format!("${p}")).join(","),
    );

    let delete_stmt = client
        .prepare(&delete_stmt)
        .await
        .context("internal error: preparing delete statement")?;
    let insert_stmt = client
        .prepare(&insert_stmt)
        .await
        .context("internal error: preparing insert statement")?;

    for path in replace_files {
        let reader = load_file(file_config, path)
            .await
            .with_context(|| format!("loading replace file {path}"))?;
        replace_file(
            file_config,
            &key,
            client,
            &delete_stmt,
            &insert_stmt,
            reader,
        )
        .await
        .with_context(|| format!("handling replace file {path}"))?;
    }

    Ok(())
}

async fn replace_file(
    file_config: &FileConfig,
    key: &[usize],
    client: &tokio_postgres::Client,
    delete_stmt: &tokio_postgres::Statement,
    insert_stmt: &tokio_postgres::Statement,
    reader: AsyncCsvReader,
) -> Result<(), anyhow::Error> {
    let mut stream = reader.into_byte_records();
    while let Some(record) = stream.try_next().await? {
        let delete_params = key.iter().map(|i| TextFormatter {
            value: &record[*i],
            null_string: &file_config.null_string,
        });
        client.execute_raw(delete_stmt, delete_params).await?;

        let insert_params = record.iter().map(|value| TextFormatter {
            value,
            null_string: &file_config.null_string,
        });
        client.execute_raw(insert_stmt, insert_params).await?;
    }
    Ok(())
}

async fn update_files(
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    update_files: &[String],
) -> Result<(), anyhow::Error> {
    // For each record in each update file, we execute an `UPDATE` that updates
    // all columns that are not the unmodified string to their new values,
    // matching based on all primary key columns.

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
        let reader = load_file(file_config, path)
            .await
            .with_context(|| format!("loading update file {path}"))?;
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

async fn delete_files(
    schema: &str,
    table: &Table,
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    delete_files: &[String],
) -> Result<(), anyhow::Error> {
    // TODO(parkmycar): Make sure table exists.
    // TODO(parkmycar): Retry transient errors.

    // Copy into a temporary table, which we then merge into the destination.
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .context("time went backwards!")?;
    // TODO(parkmycar): Make sure table name is <= 255 characters.
    let temp_table_name = format!("fivetran_{}_temp_delete_{}", table.name, ts.as_millis());

    let schema_name = escape::escape_identifier(schema);
    let table_name = escape::escape_identifier(&table.name);
    let temp_table_name = escape::escape_identifier(&temp_table_name);

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

    // Create our temporary table that we'll copy the delete files into.
    let defs = columns
        .iter()
        .map(|col| format!("{} {}", col.name, col.ty))
        .join(",");
    let create_table_stmt = format!("CREATE TABLE {schema_name}.{temp_table_name} ({defs})");
    client.batch_execute(&create_table_stmt).await?;

    // TODO(parkmycar): Ideally we'd use the COPY FROM protocol here, but we need to add support
    // for compression and encryption.
    let cols = columns
        .iter()
        .enumerate()
        .map(|(idx, col)| format!("${}::{}", idx + 1, col.ty))
        .join(",");
    let insert_stmt = format!("INSERT INTO {schema_name}.{temp_table_name} VALUES ({cols})",);
    let insert_stmt = client
        .prepare(&insert_stmt)
        .await
        .context("internal error: preparing delete temp table insert statement")?;

    let mut total_count = 0;
    for path in delete_files {
        let reader = load_file(file_config, path)
            .await
            .with_context(|| format!("loading delete file {path}"))?;
        let copied = copy_delete_file(file_config, client, &insert_stmt, reader)
            .await
            .with_context(|| format!("handling delete file {path}"))?;
        total_count += copied;
    }
    tracing::info!(total_count, "inserted rows to {schema}.{temp_table_name}");

    // Skip the update if there are no rows to delete!
    if total_count > 0 {
        // Mark rows as deleted by "merging" our temporary table into the destination table.
        //
        // TODO(parkmycar): Fivetran provides a column name in truncate for soft deletes, should the
        // delete operation be doing the same?

        // HACKY: We want to update the "_fivetran_synced" column for all of the rows we marked as
        // deleted, but don't have a way to read from the temp table that would allow this in an
        // `UPDATE` statement.
        let synced_time_stmt = format!(
            "SELECT MAX({synced_col}) FROM {schema_name}.{temp_table_name}",
            synced_col = escape::escape_identifier(FIVETRAN_SYSTEM_COLUMN_SYNCED)
        );
        let synced_time: SystemTime = client
            .query_one(&synced_time_stmt, &[])
            .await
            .and_then(|row| row.try_get(0))?;

        let matching_cols = columns.iter().filter(|col| col.is_primary);
        let merge_stmt = format!(
            r#"
            UPDATE {schema_name}.{table_name}
            SET {deleted_col} = true, {synced_col} = $1
            WHERE ({cols}) IN (
                SELECT {cols}
                FROM {schema_name}.{temp_table_name}
            )"#,
            deleted_col = escape::escape_identifier(FIVETRAN_SYSTEM_COLUMN_DELETE),
            synced_col = escape::escape_identifier(FIVETRAN_SYSTEM_COLUMN_SYNCED),
            cols = matching_cols.map(|col| &col.name).join(","),
        );
        let total_count = client.execute(&merge_stmt, &[&synced_time]).await?;
        tracing::info!(?total_count, "altered rows in {schema}.{table_name}");
    }

    // Cleanup, drop our temporary table.
    client
        .batch_execute(&format!("DROP TABLE {schema_name}.{temp_table_name}"))
        .await?;

    Ok(())
}

async fn copy_delete_file(
    file_config: &FileConfig,
    client: &tokio_postgres::Client,
    insert_stmt: &tokio_postgres::Statement,
    reader: AsyncCsvReader,
) -> Result<usize, anyhow::Error> {
    let mut count = 0;
    let mut stream = reader.into_byte_records();
    while let Some(record) = stream.try_next().await? {
        let params = record.iter().map(|value| TextFormatter {
            value,
            null_string: &file_config.null_string,
        });
        client.execute_raw(insert_stmt, params).await?;
        count += 1;
    }
    Ok(count)
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
