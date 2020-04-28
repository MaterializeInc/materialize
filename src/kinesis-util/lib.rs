// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Internal Kinesis utility library for Materialize.
//!
//! Modules in this crate provide wrappers around the AWS Kinesis APIs
//! to be used throughout Materialize code.

#![deny(missing_docs, missing_debug_implementations)]

use std::collections::HashSet;

use failure::{bail, ResultExt};
use rusoto_kinesis::{Kinesis, KinesisClient, ListShardsInput};

/// Wrapper around Kinesis' ListShards API (and Rusoto's).
///
/// Generates a set of all Shard ids in a given stream, potentially paginating through
/// a long list of shards (greater than 100) using the next_token parameter.
///
/// Does not currently handle any ListShards errors, will return all related errors
/// directly to the caller.
pub async fn get_shard_ids(
    client: &KinesisClient,
    stream_name: &str,
) -> Result<HashSet<String>, failure::Error> {
    let mut next_token = None;
    let mut all_shard_ids = HashSet::new();
    loop {
        let output = client
            .list_shards(ListShardsInput {
                exclusive_start_shard_id: None,
                max_results: None, // this defaults to 100
                next_token,
                stream_creation_timestamp: None,
                stream_name: Some(stream_name.to_owned()),
            })
            .await
            .with_context(|e| format!("fetching shard list: {}", e))?;

        match output.shards {
            Some(shards) => {
                all_shard_ids.insert(shards.iter().map(|shard| shard.shard_id.clone()).collect());
            }
            None => bail!("kinesis stream {} does not contain any shards", stream_name),
        }

        if output.next_token.is_some() {
            // Use the `next_token` field to paginate through Shards.
            next_token = output.next_token;
        } else {
            // When `next_token` is None, we've paginated through all of the Shards.
            break Ok(all_shard_ids);
        }
    }
}
