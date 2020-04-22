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

use bytes::Bytes;
use chrono::Utc;
use rand::distributions::Alphanumeric;
use rand::Rng;
use rusoto_core::{Region, RusotoError};
use rusoto_credential::AwsCredentials;
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    ListShardsInput, PutRecordsError, PutRecordsInput, PutRecordsRequestEntry, Shard,
};

use testdrive::util;

const DUMMY_PARTITION_KEY: &str = "dummy";
const ACTIVE: &str = "ACTIVE";

#[derive(Debug)]
pub struct KinesisInfo {
    pub aws_region: Region,
    pub aws_account: String,
    pub aws_credentials: AwsCredentials,
}

impl KinesisInfo {
    pub fn new_with(
        aws_region: Region,
        aws_account: String,
        aws_credentials: AwsCredentials,
    ) -> KinesisInfo {
        KinesisInfo {
            aws_region,
            aws_account,
            aws_credentials,
        }
    }
}

pub async fn create_client(aws_region: &str) -> Result<(KinesisClient, KinesisInfo), String> {
    let (aws_account, aws_credentials) =
        util::aws::account_details(Duration::from_secs(5))
            .await
            .map_err(|e| format!("error getting AWS account details: {}", e))?;
    let provider = rusoto_credential::StaticProvider::new(
        aws_credentials.aws_access_key_id().to_owned(),
        aws_credentials.aws_secret_access_key().to_owned(),
        aws_credentials.token().clone(),
        aws_credentials
            .expires_at()
            .map(|expires_at| (expires_at - Utc::now()).num_seconds()),
    );
    let dispatcher = rusoto_core::HttpClient::new().unwrap();

    let region: Region = aws_region.parse().unwrap();
    Ok((
        KinesisClient::new_with(dispatcher, provider, region.clone()),
        KinesisInfo::new_with(region, aws_account, aws_credentials),
    ))
}

pub async fn create_stream(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    shard_count: i64,
) -> Result<(), String> {
    kinesis_client
        .create_stream(CreateStreamInput {
            shard_count,
            stream_name: stream_name.to_string(),
        })
        .await
        .map_err(|e| format!("creating stream: {}", e))?;

    util::retry::retry_for(Duration::from_secs(120), |_| async {
        let description = kinesis_client
            .describe_stream(DescribeStreamInput {
                exclusive_start_shard_id: None,
                limit: None,
                stream_name: stream_name.to_string(),
            })
            .await
            .map_err(|e| format!("describing stream: {}", e))?;
        if description.stream_description.stream_status == ACTIVE {
            Ok(())
        } else {
            Err(format!(
                "Stream {} is not yet active, is {}",
                stream_name.to_string(),
                description.stream_description.stream_status
            ))
        }
    })
    .await
    .map_err(|e| format!("describing stream: {}", e))?;

    Ok(())
}

pub async fn list_shards(
    kinesis_client: &KinesisClient,
    stream_name: &str,
) -> Result<Vec<Shard>, String> {
    match kinesis_client
        .list_shards(ListShardsInput {
            exclusive_start_shard_id: None,
            max_results: None,
            next_token: None,
            stream_creation_timestamp: None,
            stream_name: Some(stream_name.to_string()),
        })
        .await
        .map_err(|e| format!("listing shards: {}", e))?
        .shards
    {
        Some(shards) => Ok(shards),
        None => Err(format!("Stream {} has no shards", stream_name)),
    }
}

pub async fn generate_and_put_records(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    record_count: i64,
    records_per_second: i64,
) -> Result<(), String> {
    let timer = std::time::Instant::now();
    // For each string, round robin puts across all of the shards.
    let mut shard_starting_hash_keys: VecDeque<String> = list_shards(&kinesis_client, &stream_name)
        .await?
        .iter()
        .map(|shard| shard.hash_key_range.starting_hash_key.clone())
        .collect();

    let mut put_record_count = 0;
    while put_record_count < record_count {
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
        .map_err(|e| format!("error putting records to Kinesis: {}", e))?;
        shard_starting_hash_keys.push_back(target_shard);

        let elapsed = put_timer.elapsed().as_millis();
        if elapsed < 1000 {
            thread::sleep(Duration::from_millis((1000 - elapsed) as u64));
        } else {
            println!(
                "expected to put records in 1000 milliseconds, took {}",
                elapsed
            );
        }
    }
    println!(
        "Generated and put {} records in {} milliseconds.",
        put_record_count,
        timer.elapsed().as_millis()
    );
    Ok(())
}

pub async fn put_records_one_second(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    shard_starting_hash_key: &str,
    records_per_second: i64,
) -> Result<i64, String> {
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
                println!("hit non-fatal error, continuing: {}", e);
            }
            Err(e) => {
                println!("{}", e);
                return Err(String::from("failed putting records!"));
            }
        }
    }
    Ok(put_record_count)
}

/// There could be more than one test running at once,
/// only want to delete the specific stream from this test run.
pub async fn delete_stream(
    kinesis_client: &KinesisClient,
    stream_name: &str,
) -> Result<(), String> {
    kinesis_client
        .delete_stream(DeleteStreamInput {
            enforce_consumer_deletion: Some(true),
            stream_name: stream_name.to_owned(),
        })
        .await
        .map_err(|e| format!("deleting Kinesis stream: {}", e))?;
    println!("Deleted Kinesis stream: {}", &stream_name);
    Ok(())
}
