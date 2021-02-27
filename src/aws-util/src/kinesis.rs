// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility mod for Kinesis.

use std::collections::HashSet;

use anyhow::Context;
use rusoto_kinesis::{GetShardIteratorInput, Kinesis, KinesisClient, ListShardsInput, Shard};

/// Wrapper around AWS Kinesis ListShards API.
///
/// Returns all shards in a given Kinesis stream, potentially paginating through
/// a list of shards (greater than 100) using the next_token request parameter.
///
/// Does not currently handle any ListShards errors, will return all errors
/// directly to the caller.
pub async fn list_shards(
    client: &KinesisClient,
    stream_name: &str,
) -> Result<Vec<Shard>, anyhow::Error> {
    let mut next_token = None;
    let mut all_shards = Vec::new();
    loop {
        let output = client
            .list_shards(ListShardsInput {
                exclusive_start_shard_id: None,
                max_results: None, // this defaults to 100
                next_token,
                stream_creation_timestamp: None,
                stream_name: Some(stream_name.to_owned()),
                shard_filter: None,
            })
            .await
            .context("fetching shard list")?;

        match output.shards {
            Some(shards) => {
                all_shards.extend(shards);
            }
            None => {
                return Err(anyhow::Error::msg(format!(
                    "kinesis stream {} does not contain any shards",
                    stream_name
                )));
            }
        }

        if output.next_token.is_some() {
            // Use the `next_token` field to paginate through Shards.
            next_token = output.next_token;
        } else {
            // When `next_token` is None, we've paginated through all of the Shards.
            break Ok(all_shards);
        }
    }
}

/// Instead of returning Shard objects, only return their ids.
pub async fn get_shard_ids(
    client: &KinesisClient,
    stream_name: &str,
) -> Result<HashSet<String>, anyhow::Error> {
    let shards = list_shards(client, stream_name).await?;
    Ok(shards.into_iter().map(|s| s.shard_id).collect())
}

/// Wrapper around AWS Kinesis GetShardIterator API (and Rusoto).
///
/// This function returns the TRIM_HORIZON shard iterator of a given stream and shard, meaning
/// it will return the location in the shard with the oldest data record. We use this to connect
/// to newly discovered shards.
///
/// Does not currently handle any GetShardIterator errors, will return all errors
/// directly to the caller.
pub async fn get_shard_iterator(
    client: &KinesisClient,
    stream_name: &str,
    shard_id: &str,
) -> Result<Option<String>, anyhow::Error> {
    Ok(client
        .get_shard_iterator(GetShardIteratorInput {
            shard_id: String::from(shard_id),
            shard_iterator_type: String::from("TRIM_HORIZON"),
            starting_sequence_number: None,
            stream_name: String::from(stream_name),
            timestamp: None,
        })
        .await
        .context("fetching shard iterator")?
        .shard_iterator)
}
