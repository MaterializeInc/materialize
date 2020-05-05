// Copyright Materialize, Inc. All rights reserved.
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

use anyhow::Context;
use bytes::Bytes;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rusoto_core::RusotoError;
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    PutRecordsError, PutRecordsInput, PutRecordsRequestEntry,
};

use aws_util;
use ore::retry;

const DUMMY_PARTITION_KEY: &str = "dummy";
const ACTIVE: &str = "ACTIVE";

/// Creates a Kinesis stream with the given name and shard count.
/// Will fail if the stream is not created and in ACTIVE mode after
/// some amount of retrying.
pub async fn create_stream(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    shard_count: i64,
) -> Result<String, anyhow::Error> {
    kinesis_client
        .create_stream(CreateStreamInput {
            shard_count,
            stream_name: stream_name.to_string(),
        })
        .await
        .context("creating Kinesis stream")?;

    let stream_arn = retry::retry_for(Duration::from_secs(120), |_| async {
        let description = &kinesis_client
            .describe_stream(DescribeStreamInput {
                exclusive_start_shard_id: None,
                limit: None,
                stream_name: stream_name.to_string(),
            })
            .await
            .context("describing Kinesis stream")?
            .stream_description;
        match description.stream_status.as_ref() {
            ACTIVE => Ok(description.stream_arn.clone()),
            other_status => Err(anyhow::Error::msg(format!(
                "Stream {} is not yet ACTIVE, is {}",
                stream_name.to_string(),
                other_status
            ))),
        }
    })
    .await
    .context("Kinesis stream never became ACTIVE")?;

    Ok(stream_arn)
}

/// Generate total_records number of records (random strings converted to bytes).
/// Then, put records_per_second records to Kinesis in batches of 500 (the PutRecords API limit).
///
/// This function will log if it's falling behind the expected put rate and once
/// at the end to indicate total amount of time spent generating and putting records.
pub async fn generate_and_put_records(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    total_records: i64,
    records_per_second: i64,
) -> Result<(), anyhow::Error> {
    let timer = std::time::Instant::now();
    // For each string, round robin puts across all of the shards.
    let mut shard_starting_hash_keys: VecDeque<String> =
        aws_util::kinesis::list_shards(&kinesis_client, &stream_name)
            .await?
            .iter()
            .map(|shard| shard.hash_key_range.starting_hash_key.clone())
            .collect();

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

        let elapsed = put_timer.elapsed().as_millis();
        if elapsed < 1000 {
            thread::sleep(Duration::from_millis((1000 - elapsed) as u64));
        } else {
            log::info!(
                "Expected to put {} records in 1000 milliseconds, took {}",
                records_per_second,
                elapsed
            );
        }
    }
    log::info!(
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
    kinesis_client: &KinesisClient,
    stream_name: &str,
    shard_starting_hash_key: &str,
    records_per_second: i64,
) -> Result<i64, anyhow::Error> {
    // Generate records.
    let mut records: Vec<PutRecordsRequestEntry> = Vec::new();
    for _i in 0..records_per_second {
        // todo: make the records more realistic json blobs
        records.push(PutRecordsRequestEntry {
            data: Bytes::from(
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ),
            explicit_hash_key: Some(shard_starting_hash_key.to_owned()), // explicitly push to the current shard
            partition_key: DUMMY_PARTITION_KEY.to_owned(), // will be overridden by "explicit_hash_key"
        });
    }

    // Put records.
    let mut index = 0;
    let mut put_record_count = 0;
    while put_record_count < records_per_second {
        match kinesis_client
            .put_records(PutRecordsInput {
                records: records[index..cmp::min(index + 500, records.len())].to_vec(), // Can put 500 records at a time.
                stream_name: stream_name.to_owned(),
            })
            .await
        {
            Ok(output) => {
                // todo: do something with failed counts
                let put_records = output.records.len();
                index += put_records;
                put_record_count += output.records.len() as i64;
            }
            Err(RusotoError::Service(PutRecordsError::KMSThrottling(e)))
            | Err(RusotoError::Service(PutRecordsError::ProvisionedThroughputExceeded(e))) => {
                // todo: do something here to avoid looping forever
                log::info!("Hit non-fatal error, continuing: {}", e);
            }
            Err(RusotoError::Credentials(e)) => {
                if e.message
                    .contains("The security token included in the request is expired")
                {
                    log::info!("{}. Getting a new KinesisClient.", e.message);
                }
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
    kinesis_client: &KinesisClient,
    stream_name: &str,
) -> Result<(), anyhow::Error> {
    kinesis_client
        .delete_stream(DeleteStreamInput {
            enforce_consumer_deletion: Some(true),
            stream_name: stream_name.to_owned(),
        })
        .await
        .context("deleting Kinesis stream")?;
    log::info!("Deleted Kinesis stream: {}", &stream_name);
    Ok(())
}
