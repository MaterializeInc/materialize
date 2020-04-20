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

pub fn create_fake_client(_aws_region: &str) -> Result<(KinesisClient, KinesisInfo), String> {
    let dummy_aws_account: &str = "000000000000";
    let dummy_aws_access_key: &str = "dummy-access-key";
    let dummy_aws_secret_access_key: &str = "dummy-secret-access-key";

    let aws_credentials = AwsCredentials::new(
        dummy_aws_access_key,
        dummy_aws_secret_access_key,
        Some(String::new()),
        None,
    );
    let provider = rusoto_credential::StaticProvider::new(
        dummy_aws_access_key.to_owned(),
        dummy_aws_secret_access_key.to_owned(),
        Some(String::new()),
        None,
    );
    let dispatcher = rusoto_core::HttpClient::new().unwrap();

    let region = Region::Custom {
        name: "localstack".into(),
        endpoint: "http://localhost:4568".into(),
    };
    Ok((
        KinesisClient::new_with(dispatcher, provider.clone(), region.clone()),
        KinesisInfo::new_with(
            region.clone(),
            dummy_aws_account.to_owned(),
            aws_credentials,
        ),
    ))
}

//pub fn create_client(aws_region: &str) -> Result<(KinesisClient, KinesisInfo), String> {
//    let (aws_account, aws_credentials) =
//        block_on(util::aws::account_details(Duration::from_secs(5)))
//            .map_err(|e| format!("error getting AWS account details: {}", e))?;
//    let provider = rusoto_credential::StaticProvider::new(
//        aws_credentials.aws_access_key_id().to_owned(),
//        aws_credentials.aws_secret_access_key().to_owned(),
//        aws_credentials.token().clone(),
//        aws_credentials
//            .expires_at()
//            .map(|expires_at| (expires_at - Utc::now()).num_seconds()),
//    );
//    let dispatcher = rusoto_core::HttpClient::new().unwrap();
//
//    let region: Region = aws_region.clone().parse().unwrap();
//    Ok((
//        KinesisClient::new_with(dispatcher, provider.clone(), region.clone()),
//        KinesisInfo::new_with(region.clone(), aws_account, aws_credentials),
//    ))
//}

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

    // todo: fix import
    util::retry::retry(|| async {
        let description = kinesis_client
            .describe_stream(DescribeStreamInput {
                exclusive_start_shard_id: None,
                limit: None,
                stream_name: stream_name.to_string(),
            })
            .await
            .map_err(|e| format!("describing stream: {}", e))?;
        if description.stream_description.stream_status == ACTIVE.to_owned() {
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

// todo: fix unwrap.
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

// todo: make the records more realistic json blobs
pub async fn generate_kinesis_records(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    num_records: i64,
) -> Result<Vec<PutRecordsRequestEntry>, String> {
    let timer = std::time::Instant::now();
    let shards = list_shards(kinesis_client, stream_name).await?;
    // For each string, round robin puts across all of the shards.
    let mut shards_queue: VecDeque<Shard> = shards.iter().cloned().collect();
    let mut records: Vec<PutRecordsRequestEntry> = Vec::new();
    for _i in 0..num_records {
        let shard = shards_queue.pop_front().unwrap();
        records.push(PutRecordsRequestEntry {
            data: Bytes::from(
                rand::thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .collect::<String>(),
            ),
            explicit_hash_key: Some(shard.hash_key_range.starting_hash_key.clone()), // explicitly push to the current shard
            partition_key: DUMMY_PARTITION_KEY.to_owned(), // will be overridden by "explicit_hash_key"
        });
        shards_queue.push_back(shard);
    }
    // todo: print size?
    println!(
        "Generated {} records in {} milliseconds.",
        num_records,
        timer.elapsed().as_millis()
    );
    Ok(records)
}

// todo: push json records
pub async fn put_records(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    records: Vec<PutRecordsRequestEntry>,
) -> Result<(), String> {
    let timer = std::time::Instant::now();
    let mut min = 0;
    let mut max = 500;
    let total_records = records.len();
    while max <= total_records {
        match kinesis_client
            .put_records(PutRecordsInput {
                records: (&records[min..cmp::min(max, total_records)]).to_vec(),
                stream_name: stream_name.to_owned(),
            })
            .await
        {
            Ok(_output) => {
                min = max;
                max += 500;
            }
            Err(RusotoError::Service(PutRecordsError::KMSThrottling(e)))
            | Err(RusotoError::Service(PutRecordsError::ProvisionedThroughputExceeded(e))) => {
                println!("hit error, trying again in one second: {}", e);
                thread::sleep(Duration::from_secs(1));
            }
            Err(e) => {
                println!("{}", e);
                return Err(String::from("failed putting records!"));
            }
        }
    }
    println!(
        "Pushed {} records to {} in {} milliseconds",
        total_records,
        stream_name,
        timer.elapsed().as_millis()
    );
    Ok(())
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
    println!("Deleted stale Kinesis stream: {}", &stream_name);
    Ok(())
}
