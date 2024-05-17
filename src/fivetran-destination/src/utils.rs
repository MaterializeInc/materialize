// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::{OpError, OpErrorKind};
use crate::fivetran_sdk::{DataType, DecimalParams, Table};

use csv_async::ByteRecord;
use futures::{ready, Sink, Stream, StreamExt};
use mz_pgrepr::Type;
use tokio::io::AsyncRead;

/// According to folks from Fivetran, checking if a column name is prefixed with a specific
/// string is enough to determine if it's a system column.
///
/// See: <https://materializeinc.slack.com/archives/C060KAR4802/p1706557379061899>
///
/// ```
/// use mz_fivetran_destination::utils;
///
/// // Needs to be prefixed with '_fivetran_' to be considered a system column.
/// assert!(!utils::is_system_column("timestamp"));
/// assert!(!utils::is_system_column("_fivetrantimestamp"));
/// assert!(!utils::is_system_column("my_fivetran_timestamp"));
///
/// assert!(utils::is_system_column("_fivetran_timestamp"));
/// ```
pub fn is_system_column(name: &str) -> bool {
    name.starts_with("_fivetran_")
}

/// Converts a type defined in the Fivetran SDK to the name of one that Materialize supports.
///
/// Errors if Materialize doesn't support the data type.
pub fn to_materialize_type(ty: DataType) -> Result<&'static str, OpError> {
    match ty {
        DataType::Boolean => Ok("boolean"),
        DataType::Short => Ok("smallint"),
        DataType::Int => Ok("integer"),
        DataType::Long => Ok("bigint"),
        DataType::Decimal => Ok("numeric"),
        DataType::Float => Ok("real"),
        DataType::Double => Ok("double precision"),
        DataType::NaiveDate => Ok("date"),
        DataType::NaiveDatetime => Ok("timestamp"),
        DataType::UtcDatetime => Ok("timestamptz"),
        DataType::Binary => Ok("bytea"),
        DataType::String => Ok("text"),
        DataType::Json => Ok("jsonb"),
        DataType::Unspecified | DataType::Xml => {
            let msg = format!("{} data type is unsupported", ty.as_str_name());
            Err(OpErrorKind::Unsupported(msg).into())
        }
    }
}

/// Converts a Postgres data type, to one supported by the Fivetran SDK.
pub fn to_fivetran_type(ty: Type) -> Result<(DataType, Option<DecimalParams>), OpError> {
    match ty {
        Type::Bool => Ok((DataType::Boolean, None)),
        Type::Int2 => Ok((DataType::Short, None)),
        Type::Int4 => Ok((DataType::Int, None)),
        Type::Int8 => Ok((DataType::Long, None)),
        Type::Numeric { constraints } => {
            let params = match constraints {
                None => None,
                Some(constraints) => {
                    let precision = u32::try_from(constraints.max_precision()).map_err(|_| {
                        OpErrorKind::InvariantViolated(format!(
                            "negative numeric precision: {}",
                            constraints.max_precision()
                        ))
                    })?;
                    let scale = u32::try_from(constraints.max_scale()).map_err(|_| {
                        OpErrorKind::InvariantViolated(format!(
                            "negative numeric scale: {}",
                            constraints.max_scale()
                        ))
                    })?;
                    Some(DecimalParams { precision, scale })
                }
            };
            Ok((DataType::Decimal, params))
        }
        Type::Float4 => Ok((DataType::Float, None)),
        Type::Float8 => Ok((DataType::Double, None)),
        Type::Date => Ok((DataType::NaiveDate, None)),
        Type::Timestamp { precision: _ } => Ok((DataType::NaiveDatetime, None)),
        Type::TimestampTz { precision: _ } => Ok((DataType::UtcDatetime, None)),
        Type::Bytea => Ok((DataType::Binary, None)),
        Type::Text => Ok((DataType::String, None)),
        Type::Jsonb => Ok((DataType::Json, None)),
        _ => {
            let msg = format!("no mapping to Fivetran data type for OID {}", ty.oid());
            Err(OpErrorKind::Unsupported(msg).into())
        }
    }
}

/// Escapes a string `s` so it can be used in a value in the `options` argument when connecting to
/// Materialize.
///
/// See the [Postgres Docs](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNECT-OPTIONS)
/// for characters that need to be escaped.
///
/// ```
/// use mz_fivetran_destination::utils::escape_options;
///
/// assert_eq!(escape_options("foo"), "foo");
/// assert_eq!(escape_options("name with space"), r#"name\ with\ space"#);
/// assert_eq!(escape_options(r#"\"#), r#"\\"#);
/// assert_eq!(escape_options(r#"\ "#), r#"\\\ "#);
/// ```
pub fn escape_options(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());

    for c in s.chars() {
        match c {
            '\\' => escaped.push_str(r#"\\"#),
            ' ' => {
                escaped.push_str(r#"\ "#);
            }
            other => escaped.push(other),
        }
    }

    escaped
}

/// Maps the column ordering of a CSV file, to the ordering of the provided table.
///
/// The column ordering of a CSV provided by Fivetran is not guaranteed to match the column
/// ordering of the destination table, so based on column name we re-map the CSV.
#[derive(Debug)]
pub struct AsyncCsvReaderTableAdapter<R> {
    /// Async CSV reader.
    csv_reader: csv_async::AsyncReader<R>,
    /// The `i`th column in a table, exists at the `tbl_to_csv[i]` position in a CSV record.
    tbl_to_csv: Vec<usize>,
}

impl<R> AsyncCsvReaderTableAdapter<R>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    /// Creates a new [`AsyncCsvReaderTableAdapter`] from an [`AsyncRead`]-er and a `Table`.
    pub async fn new(reader: R, table: &Table) -> Result<Self, OpError> {
        let mut csv_reader = csv_async::AsyncReaderBuilder::new().create_reader(reader);
        let csv_headers = csv_reader.headers().await?;
        let mut csv_columns: BTreeMap<_, _> = csv_headers
            .iter()
            .enumerate()
            .map(|(idx, name)| (name, idx))
            .collect();

        // TODO(parkmycar): When we support the `mz_extras` column we'll need to handle this case.
        if table.columns.len() != csv_columns.len() {
            let msg = format!(
                "number of columns do not match: table {}, csv {}",
                table.columns.len(),
                csv_columns.len()
            );
            return Err(OpErrorKind::CsvMapping {
                headers: csv_headers.clone(),
                table: table.clone(),
                msg,
            }
            .into());
        }

        // Create our column mapping.
        let tbl_to_csv = table
            .columns
            .iter()
            .map(|col| {
                csv_columns.remove(col.name.as_str()).ok_or_else(|| {
                    let msg = format!("table header '{}' does not exist in csv", col.name);
                    OpErrorKind::CsvMapping {
                        headers: csv_headers.clone(),
                        table: table.clone(),
                        msg,
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        if !csv_columns.is_empty() {
            return Err(OpErrorKind::CsvMapping {
                headers: csv_headers.clone(),
                table: table.clone(),
                msg: format!("Not all CSV columns were used! remaining: {csv_columns:?}"),
            }
            .into());
        }

        Ok(AsyncCsvReaderTableAdapter {
            csv_reader,
            tbl_to_csv,
        })
    }

    /// Consumes `self` returning a [`Stream`] of [`ByteRecord`]s whose columns have been mapped to
    /// match the order of the provided table.
    ///
    /// TODO(parkmycar): Ideally there would be a "Lending Stream" of sorts so we could have a
    /// single `ByteRecord` buffer we map into and then yield references for. This is possible with
    /// GATs but the trait doesn't exist yet.
    pub fn into_stream(self) -> impl Stream<Item = Result<ByteRecord, OpError>> {
        let AsyncCsvReaderTableAdapter {
            csv_reader,
            tbl_to_csv,
        } = self;

        csv_reader.into_byte_records().map(move |record| {
            let record = record?;

            // Create a new properly sized record we can map into.
            let mut mapped_record = record.clone();
            mapped_record.clear();

            for idx in &tbl_to_csv {
                let field = record.get(*idx).ok_or_else(|| {
                    OpErrorKind::InvariantViolated(format!(
                        "invariant violated, {idx} does not exist"
                    ))
                })?;
                mapped_record.push_field(field);
            }

            Ok::<ByteRecord, OpError>(mapped_record)
        })
    }
}

/// An adapter that implements [`tokio::io::AsyncWrite`] for a [`tokio_postgres::CopyInSink`].
///
/// Note: [`tokio_util`] has a similar adapter which we largely derive this implementation from
/// [1]. The reason we don't use it is because of a higher ranked lifetime error which this adapter
/// works around lifetime issues by using an owned [`bytes::Bytes`] instead of a `&[u8]`.
///
/// [1]: <https://github.com/tokio-rs/tokio/blob/59c93646898176961ee914c60932b09ce7f5eb4f/tokio-util/src/io/sink_writer.rs#L92>
pub struct CopyIntoAsyncWrite<'a> {
    inner: Pin<&'a mut tokio_postgres::CopyInSink<bytes::Bytes>>,
}

impl<'a> CopyIntoAsyncWrite<'a> {
    pub fn new(inner: Pin<&'a mut tokio_postgres::CopyInSink<bytes::Bytes>>) -> Self {
        CopyIntoAsyncWrite { inner }
    }
}

impl<'a> tokio::io::AsyncWrite for CopyIntoAsyncWrite<'a> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        ready!(self
            .inner
            .as_mut()
            .poll_ready(cx)
            .map_err(std::io::Error::other))?;

        let len = buf.len();
        let buf = bytes::Bytes::from(buf.to_vec());
        match self.inner.as_mut().start_send(buf) {
            Ok(_) => Poll::Ready(Ok(len)),
            Err(e) => Poll::Ready(Err(std::io::Error::other(e))),
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.inner
            .as_mut()
            .poll_flush(cx)
            .map_err(std::io::Error::other)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.inner
            .as_mut()
            .poll_close(cx)
            .map_err(std::io::Error::other)
    }
}

#[cfg(test)]
mod tests {
    use csv_async::StringRecord;
    use itertools::Itertools;

    use crate::fivetran_sdk::{Column, DataType};

    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn smoketest_csv_table_adapter() {
        // Note: The CSV data has the "country" and "city" columns swapped w.r.t the table
        // definition.
        let data = "country,city,pop\nUnited States,New York,9000000";
        let table = Table {
            name: "stats".to_string(),
            columns: vec![
                Column {
                    name: "city".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: true,
                    decimal: None,
                },
                Column {
                    name: "country".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: false,
                    decimal: None,
                },
                Column {
                    name: "pop".to_string(),
                    r#type: DataType::Int.into(),
                    primary_key: false,
                    decimal: None,
                },
            ],
        };

        let mapped_reader = AsyncCsvReaderTableAdapter::new(data.as_bytes(), &table)
            .await
            .unwrap();
        let mapped_rows = mapped_reader
            .into_stream()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map_ok(|record| StringRecord::from_byte_record(record).unwrap())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Note: We purposefully do not yield the header.
        insta::assert_debug_snapshot!(mapped_rows, @r###"
        [
            StringRecord(["New York", "United States", "9000000"]),
        ]
        "###);
    }

    #[mz_ore::test(tokio::test)]
    async fn test_incorrect_number_of_columns() {
        let data = "country,city,pop\nUnited States,New York,9000000";
        let table = Table {
            name: "stats".to_string(),
            columns: vec![
                Column {
                    name: "city".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: true,
                    decimal: None,
                },
                Column {
                    name: "country".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: false,
                    decimal: None,
                },
            ],
        };

        let error = AsyncCsvReaderTableAdapter::new(data.as_bytes(), &table)
            .await
            .unwrap_err();
        insta::assert_debug_snapshot!(error, @r###"
        OpError {
            kind: CsvMapping {
                headers: StringRecord(["country", "city", "pop"]),
                table: Table {
                    name: "stats",
                    columns: [
                        Column {
                            name: "city",
                            r#type: String,
                            primary_key: true,
                            decimal: None,
                        },
                        Column {
                            name: "country",
                            r#type: String,
                            primary_key: false,
                            decimal: None,
                        },
                    ],
                },
                msg: "number of columns do not match: table 2, csv 3",
            },
            context: [],
        }
        "###);
    }

    #[mz_ore::test(tokio::test)]
    async fn test_missing_column() {
        let data = "non_existant,city\nUnited States,New York";
        let table = Table {
            name: "stats".to_string(),
            columns: vec![
                Column {
                    name: "city".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: true,
                    decimal: None,
                },
                Column {
                    name: "country".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: false,
                    decimal: None,
                },
            ],
        };

        let error = AsyncCsvReaderTableAdapter::new(data.as_bytes(), &table)
            .await
            .unwrap_err();
        insta::assert_debug_snapshot!(error, @r###"
        OpError {
            kind: CsvMapping {
                headers: StringRecord(["non_existant", "city"]),
                table: Table {
                    name: "stats",
                    columns: [
                        Column {
                            name: "city",
                            r#type: String,
                            primary_key: true,
                            decimal: None,
                        },
                        Column {
                            name: "country",
                            r#type: String,
                            primary_key: false,
                            decimal: None,
                        },
                    ],
                },
                msg: "table header 'country' does not exist in csv",
            },
            context: [],
        }
        "###);
    }

    #[mz_ore::test(tokio::test)]
    async fn test_column_names_with_leading_spaces() {
        // Note: The CSV data has the "country" and "city" columns swapped w.r.t the table
        // definition.
        let data = " a,  a,a, \nfoo_bar,100,false,1.0";
        let table = Table {
            name: "stats".to_string(),
            columns: vec![
                Column {
                    name: "a".to_string(),
                    r#type: DataType::Boolean.into(),
                    primary_key: true,
                    decimal: None,
                },
                Column {
                    name: " ".to_string(),
                    r#type: DataType::Float.into(),
                    primary_key: true,
                    decimal: None,
                },
                Column {
                    name: " a".to_string(),
                    r#type: DataType::String.into(),
                    primary_key: true,
                    decimal: None,
                },
                Column {
                    name: "  a".to_string(),
                    r#type: DataType::Int.into(),
                    primary_key: false,
                    decimal: None,
                },
            ],
        };

        let mapped_reader = AsyncCsvReaderTableAdapter::new(data.as_bytes(), &table)
            .await
            .unwrap();
        let mapped_rows = mapped_reader
            .into_stream()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map_ok(|record| StringRecord::from_byte_record(record).unwrap())
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        // Note: We purposefully do not yield the header.
        insta::assert_debug_snapshot!(mapped_rows, @r###"
        [
            StringRecord(["false", "1.0", "foo_bar", "100"]),
        ]
        "###);
    }
}
