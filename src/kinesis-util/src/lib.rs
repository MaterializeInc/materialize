// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS Kinesis utilities.

use aws_sdk_kinesis::error::{GetShardIteratorError, ListShardsError};
use aws_sdk_kinesis::model::{Shard, ShardIteratorType};
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client;

/// Lists the shards of the named Kinesis stream.
///
/// This function wraps the `ListShards` API call. It returns all shards in a
/// given Kinesis stream, automatically handling pagination if required.
///
/// # Errors
///
/// Any errors from the underlying `GetShardIterator` API call are surfaced
/// directly.
pub async fn list_shards(
    client: &aws_sdk_kinesis::Client,
    stream_name: &str,
) -> Result<Vec<Shard>, SdkError<ListShardsError>> {
    let mut next_token = None;
    let mut shards = Vec::new();
    loop {
        let res = client
            .list_shards()
            .set_next_token(next_token)
            .stream_name(stream_name)
            .send()
            .await?;
        shards.extend(res.shards.unwrap_or_else(Vec::new));
        if res.next_token.is_some() {
            next_token = res.next_token;
        } else {
            return Ok(shards);
        }
    }
}

/// Gets the shard IDs of the named Kinesis stream.
///
/// This function is like [`list_shards`], but
///
/// # Errors
///
/// Any errors from the underlying `GetShardIterator` API call are surfaced
/// directly.
pub async fn get_shard_ids(
    client: &Client,
    stream_name: &str,
) -> Result<impl Iterator<Item = String>, SdkError<ListShardsError>> {
    let res = list_shards(client, stream_name).await?;
    Ok(res
        .into_iter()
        .map(|s| s.shard_id.unwrap_or_else(|| "".into())))
}

/// Constructs an iterator over a Kinesis shard.
///
/// This function is a wrapper around around the `GetShardIterator` API. It
/// returns the `TRIM_HORIZON` shard iterator of a given stream and shard,
/// meaning it will return the location in the shard with the oldest data
/// record.
///
/// # Errors
///
/// Any errors from the underlying `GetShardIterator` API call are surfaced
/// directly.
pub async fn get_shard_iterator(
    client: &Client,
    stream_name: &str,
    shard_id: &str,
) -> Result<Option<String>, SdkError<GetShardIteratorError>> {
    let res = client
        .get_shard_iterator()
        .stream_name(stream_name)
        .shard_id(shard_id)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await?;
    Ok(res.shard_iterator)
}
