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
use crate::destination::{config, ddl};
use crate::fivetran_sdk::write_batch_request::FileParams;
use crate::fivetran_sdk::{
    Column, Compression, DataType, Encryption, Table, TruncateRequest, TruncateResponse,
    WriteBatchRequest, WriteBatchResponse,
};

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
    let Some(mut table) = request.table else {
        bail!("internal error: WriteBatchRequest missing \"table\" field");
    };

    if !table.columns.iter().any(|c| c.primary_key) {
        bail!("table has no primary key columns");
    }

    // TODO(benesch): should the SDK be providing these in the request?
    table.columns.push(Column {
        name: "_fivetran_deleted".into(),
        r#type: DataType::Boolean.into(),
        primary_key: false,
        decimal: None,
    });
    table.columns.push(Column {
        name: "_fivetran_synced".into(),
        r#type: DataType::UtcDatetime.into(),
        primary_key: false,
        decimal: None,
    });

    let Some(FileParams::Csv(csv_file_params)) = request.file_params else {
        bail!("internal error: WriteBatchRequest missing \"file_params\" field");
    };

    if !request.delete_files.is_empty() {
        bail!("hard deletions are not supported");
    }

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
                ty = ddl::to_materialize_type(column.r#type())?,
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
