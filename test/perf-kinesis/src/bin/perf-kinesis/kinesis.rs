// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp;
use std::collections::VecDeque;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, Context};
use aws_sdk_kinesis::model::{PutRecordsRequestEntry, StreamStatus};
use aws_sdk_kinesis::types::{Blob, SdkError};
use tracing::info;

use mz_ore::retry::Retry;
use mz_test_util::generator;

const DUMMY_PARTITION_KEY: &str = "dummy";

/// Creates a Kinesis stream with the given name and shard count.
/// Will fail if the stream is not created and in ACTIVE mode after
/// some amount of retrying.
pub async fn create_stream(
    kinesis_client: &aws_sdk_kinesis::Client,
    stream_name: &str,
    shard_count: i32,
) -> Result<String, anyhow::Error> {
    kinesis_client
        .create_stream()
        .stream_name(stream_name)
        .shard_count(shard_count)
        .send()
        .await
        .context("creating Kinesis stream")?;

    let stream_arn = Retry::default()
        .max_duration(Duration::from_secs(120))
        .retry_async(|_| async {
            let description = &kinesis_client
                .describe_stream()
                .stream_name(stream_name)
                .send()
                .await
                .context("describing Kinesis stream")?
                .stream_description
                .expect("stream description unexpectedly missing");
            match description.stream_status.as_ref() {
                Some(StreamStatus::Active) => Ok(description.stream_arn.clone()),
                other_status => Err(anyhow::Error::msg(format!(
                    "Stream {} is not yet ACTIVE, is {:?}",
                    stream_name, other_status
                ))),
            }
        })
        .await
        .context("Kinesis stream never became ACTIVE")?;

    stream_arn.ok_or_else(|| anyhow!("stream ARN unexpectedly missing"))
}

/// Generate total_records number of records (random strings converted to bytes).
/// Then, put records_per_second records to Kinesis in batches of 500 (the PutRecords API limit).
///
/// This function will log if it's falling behind the expected put rate and once
/// at the end to indicate total amount of time spent generating and putting records.
pub async fn generate_and_put_records(
    kinesis_client: &aws_sdk_kinesis::Client,
    stream_name: &str,
    total_records: u64,
    records_per_second: u64,
) -> Result<(), anyhow::Error> {
    let timer = std::time::Instant::now();
    // For each string, round robin puts across all of the shards.
    let mut shard_starting_hash_keys = mz_kinesis_util::list_shards(&kinesis_client, &stream_name)
        .await?
        .into_iter()
        .map(|shard| {
            shard
                .hash_key_range
                .and_then(|hkr| hkr.starting_hash_key)
                .ok_or_else(|| anyhow!("starting hash key missing"))
        })
        .collect::<Result<VecDeque<_>, _>>()?;

    let mut put_record_count = 0;
    while put_record_count < total_records {
        // Use a basic timer to put records_per_second records/second to Kinesis.
        let put_timer = std::time::Instant::now();

        let target_shard = shard_starting_hash_keys.pop_front().unwrap();
        put_record_count += put_records_one_second(
            kinesis_client,
            stream_name,
            &target_shard,
            records_per_second,
        )
        .await
        .context("putting records to Kinesis")?;
        shard_starting_hash_keys.push_back(target_shard);

        let elapsed = put_timer.elapsed();
        if elapsed < Duration::from_secs(1) {
            thread::sleep(Duration::from_secs(1) - elapsed);
        } else {
            info!(
                "Expected to put {} records in 1s, took {:#?}",
                records_per_second, elapsed
            );
        }
    }
    info!(
        "Generated and put {} records in {} milliseconds.",
        put_record_count,
        timer.elapsed().as_millis()
    );
    Ok(())
}

/// Put records_per_second records to the target Kinesis stream.
/// Use a round-robin strategy to put records: rotate through all of the target
///     stream's shards, putting a second's worth of records to each.
pub async fn put_records_one_second(
    kinesis_client: &aws_sdk_kinesis::Client,
    stream_name: &str,
    shard_starting_hash_key: &str,
    records_per_second: u64,
) -> Result<u64, anyhow::Error> {
    // Generate records.
    let mut records: Vec<PutRecordsRequestEntry> = Vec::new();
    for _ in 0..records_per_second {
        // todo: make the records more realistic json blobs
        records.push(
            PutRecordsRequestEntry::builder()
                .data(Blob::new(generator::bytes::generate_bytes(30)))
                .explicit_hash_key(shard_starting_hash_key.to_owned()) // explicitly push to the current shard
                .partition_key(DUMMY_PARTITION_KEY.to_owned()) // will be overridden by "explicit_hash_key"
                .build(),
        );
    }

    // Put records.
    let mut index = 0;
    let mut put_record_count = 0;
    while put_record_count < records_per_second {
        match kinesis_client
            .put_records()
            .set_records(Some(
                records[index..cmp::min(index + 500, records.len())].to_vec(),
            )) // Can put 500 records at a time.
            .stream_name(stream_name.to_owned())
            .send()
            .await
        {
            Ok(output) => {
                // todo: do something with failed counts
                let records = output
                    .records
                    .ok_or_else(|| anyhow!("records unexpectedly missing"))?;
                let put_records = records.len();
                index += put_records;
                put_record_count += records.len() as u64;
            }
            Err(SdkError::ServiceError { err, .. })
                if err.is_kms_throttling_exception()
                    || err.is_provisioned_throughput_exceeded_exception() =>
            {
                info!("Hit non-fatal error, continuing: {}", err);
            }
            Err(SdkError::ServiceError { err, .. })
                if err
                    .message()
                    .unwrap_or("")
                    .contains("The security token included in the request is expired") =>
            {
                info!(
                    "{:?}. Getting a new aws_sdk_kinesis::Client.",
                    err.message()
                );
            }
            Err(e) => {
                return Err(anyhow::Error::msg(format!(
                    "failed putting records to Kinesis: {}",
                    e
                )));
            }
        }
    }
    Ok(put_record_count)
}

/// Only delete the Kinesis stream generated from this test run.
pub async fn delete_stream(
    kinesis_client: &aws_sdk_kinesis::Client,
    stream_name: &str,
) -> Result<(), anyhow::Error> {
    kinesis_client
        .delete_stream()
        .enforce_consumer_deletion(true)
        .stream_name(stream_name.to_owned())
        .send()
        .await
        .context("deleting Kinesis stream")?;
    info!("Deleted Kinesis stream: {}", &stream_name);
    Ok(())
}
