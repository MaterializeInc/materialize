// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, VecDeque};

use bytes::Bytes;
use chrono::Utc;
use futures::executor::block_on;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::{AwsCredentials, StaticProvider};
use rusoto_kinesis::{
    CreateStreamInput, DeleteStreamInput, DescribeStreamInput, Kinesis, KinesisClient,
    ListShardsInput, ListStreamsInput, PutRecordError, PutRecordInput, Shard,
    UpdateShardCountInput,
};

use std::time::Duration;
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

pub fn create_fake_client(aws_region: &str) -> Result<(KinesisClient, KinesisInfo), String> {
    let DUMMY_AWS_ACCOUNT: &str = "000000000000";
    let DUMMY_AWS_ACCESS_KEY: &str = "dummy-access-key";
    let DUMMY_AWS_SECRET_ACCESS_KEY: &str = "dummy-secret-access-key";

    let aws_credentials = AwsCredentials::new(
        DUMMY_AWS_ACCESS_KEY,
        DUMMY_AWS_SECRET_ACCESS_KEY,
        Some(String::new()),
        None,
    );
    let provider = rusoto_credential::StaticProvider::new(
        DUMMY_AWS_ACCESS_KEY.to_owned(),
        DUMMY_AWS_SECRET_ACCESS_KEY.to_owned(),
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
            DUMMY_AWS_ACCOUNT.to_owned(),
            aws_credentials,
        ),
    ))
}

pub fn create_client(aws_region: &str) -> Result<(KinesisClient, KinesisInfo), String> {
    let (aws_account, aws_credentials) =
        block_on(util::aws::account_details(Duration::from_secs(5)))
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

    let region: Region = aws_region.clone().parse().unwrap();
    Ok((
        KinesisClient::new_with(dispatcher, provider.clone(), region.clone()),
        KinesisInfo::new_with(region.clone(), aws_account, aws_credentials),
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

pub async fn update_shard_count(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    target_shard_count: i64,
) -> Result<(), String> {
    let shards = list_shards(kinesis_client, stream_name).await?.len();
    kinesis_client
        .update_shard_count(UpdateShardCountInput {
            scaling_type: "UNIFORM_SCALING".to_string(),
            stream_name: stream_name.to_string(),
            target_shard_count,
        })
        .await
        .map_err(|e| format!("updating shard count: {}", e))?;
    Ok(())
}

pub async fn put_records(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    data: &HashSet<String>,
) -> Result<(), String> {
    let shards = list_shards(kinesis_client, stream_name).await?;
    // For each string, round robin puts across all of the shards.
    let mut shards_queue: VecDeque<Shard> = shards.iter().cloned().collect();
    for d in data {
        let shard = shards_queue.pop_front().unwrap();
        let result = kinesis_client
            .put_record(PutRecordInput {
                data: Bytes::from(d.clone()),
                explicit_hash_key: Some(shard.hash_key_range.starting_hash_key.clone()), // explicitly push to the current shard
                partition_key: DUMMY_PARTITION_KEY.to_owned(), // will be overridden by "explicit_hash_key"
                sequence_number_for_ordering: None,
                stream_name: stream_name.to_string(),
            })
            .await
            .map_err(|e| format!("putting record: {}", e))?;
        shards_queue.push_back(shard);
    }
    Ok(())
}

///// There could be more than one test running at once,
///// only want to delete the specific stream from this test run.
//fn delete_stream(stream_name: &str) -> Result<(), String> {
//    let list_streams_input = ListStreamsInput {
//        exclusive_start_stream_name: None,
//        limit: None,
//    };
//    let stream_names = state
//        .kinesis_client
//        .list_streams(list_streams_input)
//        .await
//        .map_err(|e| format!("listing Kinesis streams: {}", e))?
//        .stream_names;
//
//    for stream_name in stream_names {
//        let delete_stream_input = DeleteStreamInput {
//            enforce_consumer_deletion: Some(true),
//            stream_name: stream_name.clone(),
//        };
//        state
//            .kinesis_client
//            .delete_stream(delete_stream_input)
//            .await
//            .map_err(|e| format!("deleting Kinesis stream: {}", e))?;
//        println!("Deleted stale Kinesis stream: {}", &stream_name);
//    }
//
//    Ok(())
//}
