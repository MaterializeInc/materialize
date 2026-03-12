// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// The metadata for Arrow `Field` type requires `std::collections::HashMap`, which is disallowed.
#[allow(clippy::disallowed_types)]
use std::collections::HashMap;
use std::pin::Pin;
use std::str;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::Context;
use anyhow::bail;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryBuilder,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, Int64Builder,
    ListBuilder, StringArray, StructArray, Time32SecondArray, TimestampMillisecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::display::ArrayFormatter;
use arrow::util::display::FormatOptions;
use async_compression::tokio::bufread::{BzEncoder, GzipEncoder, XzEncoder, ZstdEncoder};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression as ParquetCompression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use regex::Regex;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::action::file::Compression;
use crate::action::file::build_compression;
use crate::action::file::build_contents;
use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_verify_data(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let mut expected_body = cmd
        .input
        .into_iter()
        // Strip suffix to allow lines with trailing whitespace
        .map(|line| {
            line.trim_end_matches("// allow-trailing-whitespace")
                .to_string()
        })
        .collect::<Vec<String>>();
    let bucket: String = cmd.args.parse("bucket")?;
    let key: String = cmd.args.parse("key")?;
    let sort_rows = cmd.args.opt_bool("sort-rows")?.unwrap_or(false);
    cmd.args.done()?;

    println!("Verifying contents of S3 bucket {bucket} key {key}...");

    let client = mz_aws_util::s3::new_client(&state.aws_config);

    // List the path until the INCOMPLETE sentinel file disappears so we know the
    // data is complete.
    let mut attempts = 0;
    let all_files;
    loop {
        attempts += 1;
        if attempts > 10 {
            bail!("found incomplete sentinel file in path {key} after 10 attempts")
        }

        let files = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&format!("{}/", key))
            .send()
            .await?;
        match files.contents {
            Some(files)
                if files
                    .iter()
                    .any(|obj| obj.key().map_or(false, |key| key.contains("INCOMPLETE"))) =>
            {
                thread::sleep(Duration::from_secs(1))
            }
            None => bail!("no files found in bucket {bucket} key {key}"),
            Some(files) => {
                all_files = files;
                break;
            }
        }
    }

    let mut rows = vec![];
    for obj in all_files.iter() {
        let file = client
            .get_object()
            .bucket(&bucket)
            .key(obj.key().unwrap())
            .send()
            .await?;
        let bytes = file.body.collect().await?.into_bytes();

        let new_rows = match obj.key().unwrap() {
            key if key.ends_with(".csv") => {
                let actual_body = str::from_utf8(bytes.as_ref())?;
                actual_body.lines().map(|l| l.to_string()).collect()
            }
            key if key.ends_with(".parquet") => rows_from_parquet(bytes),
            key => bail!("unexpected file type: {key}"),
        };
        rows.extend(new_rows);
    }
    if sort_rows {
        expected_body.sort();
        rows.sort();
    }
    if rows != expected_body {
        bail!(
            "content did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
            expected_body,
            rows
        );
    }

    Ok(ControlFlow::Continue)
}

pub async fn run_verify_keys(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let bucket: String = cmd.args.parse("bucket")?;
    let prefix_path: String = cmd.args.parse("prefix-path")?;
    let key_pattern: Regex = cmd.args.parse("key-pattern")?;
    let num_attempts = cmd.args.opt_parse("num-attempts")?.unwrap_or(30);
    cmd.args.done()?;

    println!("Verifying {key_pattern} in S3 bucket {bucket} path {prefix_path}...");

    let client = mz_aws_util::s3::new_client(&state.aws_config);

    let mut attempts = 0;
    while attempts <= num_attempts {
        attempts += 1;
        let files = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix(&format!("{}/", prefix_path))
            .send()
            .await?;
        match files.contents {
            Some(files) => {
                let files: Vec<_> = files
                    .iter()
                    .filter(|obj| key_pattern.is_match(obj.key().unwrap()))
                    .map(|obj| obj.key().unwrap())
                    .collect();
                if !files.is_empty() {
                    println!("Found matching files: {files:?}");
                    return Ok(ControlFlow::Continue);
                }
            }
            _ => thread::sleep(Duration::from_secs(1)),
        }
    }

    bail!("Did not find matching files in bucket {bucket} prefix {prefix_path}");
}

fn rows_from_parquet(bytes: bytes::Bytes) -> Vec<String> {
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReader::try_new(bytes, 1_000_000).unwrap();

    let mut ret = vec![];
    let format_options = FormatOptions::default();
    for batch in reader {
        let batch = batch.unwrap();
        let converters = batch
            .columns()
            .iter()
            .map(|a| ArrayFormatter::try_new(a.as_ref(), &format_options).unwrap())
            .collect::<Vec<_>>();

        for row_idx in 0..batch.num_rows() {
            let mut buf = String::new();
            for (col_idx, converter) in converters.iter().enumerate() {
                if col_idx > 0 {
                    buf.push_str(" ");
                }
                converter.value(row_idx).write(&mut buf).unwrap();
            }
            ret.push(buf);
        }
    }
    ret
}

pub async fn run_upload(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let bucket = cmd.args.string("bucket")?;
    let count: Option<usize> = cmd.args.opt_parse("count")?;

    let keys: Vec<String> = if let Some(count) = count {
        // Bulk mode uses `key-prefix` + `i` + optional `key-suffix`,
        let prefix = cmd.args.string("key-prefix")?;
        let suffix = cmd.args.opt_string("key-suffix").unwrap_or_default();
        (0..count).map(|i| format!("{prefix}{i}{suffix}")).collect()
    } else {
        // Single-file mode uses `key`.
        vec![cmd.args.string("key")?]
    };

    let compression = build_compression(&mut cmd)?;
    let content = build_contents(&mut cmd)?;

    let aws_client = mz_aws_util::s3::new_client(&state.aws_config);

    // TODO(parkmycar): Stream data to S3. The ByteStream type from the AWS config is a bit
    // cumbersome to work with, so for now just stick with this.
    let mut body = vec![];
    for line in content {
        body.extend(&line);
        body.push(b'\n');
    }

    let mut reader: Pin<Box<dyn AsyncRead + Send + Sync>> = match compression {
        Compression::None => Box::pin(&body[..]),
        Compression::Gzip => Box::pin(GzipEncoder::new(&body[..])),
        Compression::Bzip2 => Box::pin(BzEncoder::new(&body[..])),
        Compression::Xz => Box::pin(XzEncoder::new(&body[..])),
        Compression::Zstd => Box::pin(ZstdEncoder::new(&body[..])),
    };
    let mut content = vec![];
    reader
        .read_to_end(&mut content)
        .await
        .context("compressing")?;

    // Upload the file(s) to S3.
    println!(
        "Uploading {} files to S3 bucket, starting with '{bucket}/{}'",
        keys.len(),
        keys.first().map(String::as_str).unwrap_or("<none>")
    );
    for key in &keys {
        aws_client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(content.clone().into())
            .send()
            .await
            .context("s3 put")?;
    }

    Ok(ControlFlow::Continue)
}

pub async fn run_set_presigned_url(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let key = cmd.args.string("key")?;
    let bucket = cmd.args.string("bucket")?;
    let var_name = cmd.args.string("var-name")?;

    let aws_client = mz_aws_util::s3::new_client(&state.aws_config);
    let presign_config = mz_aws_util::s3::new_presigned_config();
    let request = aws_client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .presigned(presign_config)
        .await
        .context("s3 presign")?;

    println!("Setting '{var_name}' to presigned URL for {bucket}/{key}");
    state.cmd_vars.insert(var_name, request.uri().to_string());

    Ok(ControlFlow::Continue)
}

/// Generates parquet files covering a wide range of Arrow types and uploads them to S3 with
/// multiple compression variants. This is the Rust equivalent of the Python
/// `generate_parquet_files()` function.
///
/// Uploads:
/// - `{key-prefix}` (uncompressed)
/// - `{key-prefix}.snappy`
/// - `{key-prefix}.gzip`
/// - `{key-prefix}.brotli`
/// - `{key-prefix}.zstd`
/// - `{key-prefix}.lz4`
pub async fn run_upload_parquet_types(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let bucket = cmd.args.string("bucket")?;
    let key_prefix = cmd.args.string("key-prefix")?;
    cmd.args.done()?;

    let batch = build_parquet_types_batch().context("building parquet types batch")?;

    let compressions = vec![
        ("".to_string(), ParquetCompression::UNCOMPRESSED),
        (".snappy".to_string(), ParquetCompression::SNAPPY),
        (
            ".gzip".to_string(),
            ParquetCompression::GZIP(GzipLevel::default()),
        ),
        (
            ".brotli".to_string(),
            ParquetCompression::BROTLI(BrotliLevel::default()),
        ),
        (
            ".zstd".to_string(),
            ParquetCompression::ZSTD(ZstdLevel::default()),
        ),
        (".lz4".to_string(), ParquetCompression::LZ4_RAW),
    ];

    let client = mz_aws_util::s3::new_client(&state.aws_config);

    for (suffix, compression) in compressions {
        let key = format!("{key_prefix}{suffix}");
        println!("Uploading parquet types file to S3 bucket {bucket}/{key}");

        let props = WriterProperties::builder()
            .set_compression(compression)
            .build();
        let mut buf = Vec::new();
        {
            let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
                .context("creating parquet writer")?;
            writer.write(&batch).context("writing parquet batch")?;
            writer.close().context("closing parquet writer")?;
        }

        client
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(buf.into())
            .send()
            .await
            .context("s3 put")?;
    }

    Ok(ControlFlow::Continue)
}

// Using `as ArrayRef` is necessary when creating the struct array because the inner arrays have different types.
// The metadata for Arrow `Field` type requires `std::collections::HashMap`, which is disallowed.
#[allow(clippy::as_conversions, clippy::disallowed_types)]
fn build_parquet_types_batch() -> Result<RecordBatch, anyhow::Error> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

    // date32: days since epoch
    let date_values: Vec<i32> = [
        NaiveDate::from_ymd_opt(2025, 11, 1).unwrap(),
        NaiveDate::from_ymd_opt(2025, 11, 2).unwrap(),
        NaiveDate::from_ymd_opt(2025, 11, 3).unwrap(),
    ]
    .into_iter()
    .map(|d| d.signed_duration_since(epoch).num_days() as i32)
    .collect();

    // timestamp(ms): ms since epoch (no timezone)
    let datetime_values: Vec<i64> = [
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2025, 11, 1).unwrap(),
            NaiveTime::from_hms_opt(10, 0, 0).unwrap(),
        ),
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2025, 11, 1).unwrap(),
            NaiveTime::from_hms_opt(11, 30, 0).unwrap(),
        ),
        NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2025, 11, 1).unwrap(),
            NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
        ),
    ]
    .into_iter()
    .map(|dt| dt.and_utc().timestamp_millis())
    .collect();

    // time32(s): seconds since midnight
    let time_values: Vec<i32> = [
        NaiveTime::from_hms_opt(9, 0, 0).unwrap(),
        NaiveTime::from_hms_opt(10, 30, 15).unwrap(),
        NaiveTime::from_hms_opt(11, 45, 30).unwrap(),
    ]
    .into_iter()
    .map(|t| t.num_seconds_from_midnight() as i32)
    .collect();

    // list<int64>: [-1, 2], [3, 4, 5], []
    let mut list_builder = ListBuilder::new(Int64Builder::new());
    for &val in &[-1i64, 2] {
        list_builder.values().append_value(val);
    }
    list_builder.append(true);
    for &val in &[3i64, 4, 5] {
        list_builder.values().append_value(val);
    }
    list_builder.append(true);
    list_builder.append(true); // empty list
    let list_array = Arc::new(list_builder.finish());

    // decimal128(precision=10, scale=5): -54.321, 123.45, null
    let decimal_array = Arc::new(
        Decimal128Array::from(vec![Some(-5_432_100i128), Some(12_345_000i128), None])
            .with_precision_and_scale(10, 5)
            .context("setting decimal precision/scale")?,
    );

    // struct/record: (name text, age int32, avg float64)
    let struct_array = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("name", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec!["Taco", "Burger", "SlimJim"])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("age", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![3, 2, 1])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("avg", DataType::Float64, true)),
            Arc::new(Float64Array::from(vec![2.2, 4.5, 1.14])) as ArrayRef,
        ),
    ]));

    // uuid: FixedSizeBinary(16) with arrow.uuid extension metadata
    let mut uuid_builder = FixedSizeBinaryBuilder::with_capacity(3, 16);
    for uuid_str in &[
        "badc0deb-adc0-deba-dc0d-ebadc0debadc",
        "deadbeef-dead-4eef-8eef-deaddeadbeef",
        "00000000-0000-0000-0000-000000000000",
    ] {
        let uuid_val = uuid::Uuid::parse_str(uuid_str).context("parsing uuid")?;
        uuid_builder
            .append_value(uuid_val.as_bytes())
            .context("appending uuid bytes")?;
    }
    let uuid_array = Arc::new(uuid_builder.finish());

    // variable-length binary
    let mut binary_builder = BinaryBuilder::new();
    binary_builder.append_value(b"raw1");
    binary_builder.append_value(b"raw2");
    binary_builder.append_value(b"raw3");
    let binary_array = Arc::new(binary_builder.finish());

    let mut uuid_metadata = HashMap::new();
    uuid_metadata.insert("ARROW:extension:name".to_string(), "arrow.uuid".to_string());

    let schema = Arc::new(Schema::new(vec![
        Field::new("int8_col", DataType::Int8, true),
        Field::new("uint8_col", DataType::UInt8, true),
        Field::new("int16_col", DataType::Int16, true),
        Field::new("uint16_col", DataType::UInt16, true),
        Field::new("int32_col", DataType::Int32, true),
        Field::new("uint32_col", DataType::UInt32, true),
        Field::new("int64_col", DataType::Int64, true),
        Field::new("uint64_col", DataType::UInt64, true),
        Field::new("float32_col", DataType::Float32, true),
        Field::new("float64_col", DataType::Float64, true),
        Field::new("bool_col", DataType::Boolean, true),
        Field::new("string_col", DataType::Utf8, true),
        Field::new("binary_col", DataType::Binary, true),
        Field::new("date32_col", DataType::Date32, true),
        Field::new(
            "timestamp_ms_col",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        ),
        Field::new("time32_col", DataType::Time32(TimeUnit::Second), true),
        Field::new(
            "list_col",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        ),
        Field::new("decimal_col", DataType::Decimal128(10, 5), true),
        Field::new("json_col", DataType::Utf8, true),
        Field::new(
            "record_col",
            DataType::Struct(
                vec![
                    Field::new("name", DataType::Utf8, true),
                    Field::new("age", DataType::Int32, true),
                    Field::new("avg", DataType::Float64, true),
                ]
                .into(),
            ),
            true,
        ),
        Field::new("uuid_col", DataType::FixedSizeBinary(16), false).with_metadata(uuid_metadata),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int8Array::from(vec![-1i8, 2, 3])),
            Arc::new(UInt8Array::from(vec![10u8, 20, 30])),
            Arc::new(Int16Array::from(vec![-1000i16, 2000, 3000])),
            Arc::new(UInt16Array::from(vec![10000u16, 20000, 30000])),
            Arc::new(Int32Array::from(vec![-100000i32, 200000, 300000])),
            Arc::new(UInt32Array::from(vec![1000000u32, 2000000, 3000000])),
            Arc::new(Int64Array::from(vec![
                -1_000_000_000i64,
                2_000_000_000,
                3_000_000_000,
            ])),
            Arc::new(UInt64Array::from(vec![
                1_000_000_000_000_000_000u64,
                2_000_000_000_000_000_000,
                3_000_000_000_000_000_000,
            ])),
            Arc::new(Float32Array::from(vec![-1.0f32, 2.5, 3.7])),
            Arc::new(Float64Array::from(vec![-1.0f64, 2.5, 3.7])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(StringArray::from(vec!["apple", "banana", "cherry"])),
            binary_array,
            Arc::new(Date32Array::from(date_values)),
            Arc::new(TimestampMillisecondArray::from(datetime_values)),
            Arc::new(Time32SecondArray::from(time_values)),
            list_array,
            decimal_array,
            Arc::new(StringArray::from(vec![
                r#"{"a": 5, "b": { "c": 1.1 } }"#,
                r#"{ "d": "str", "e" : [1,2,3] }"#,
                "{}",
            ])),
            struct_array,
            uuid_array,
        ],
    )
    .context("building record batch")?;

    Ok(batch)
}
