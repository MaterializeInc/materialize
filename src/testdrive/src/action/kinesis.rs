// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use rusoto_kinesis::{Kinesis, KinesisClient, ListShardsInput};

use crate::util::retry;

mod add_shards;
mod create_stream;
mod ingest;
mod remove_shards;
mod verify;

pub use add_shards::build_add_shards;
pub use create_stream::build_create_stream;
use futures::executor::block_on;
pub use ingest::build_ingest;
pub use remove_shards::build_remove_shards;
pub use verify::build_verify;

const DEFAULT_KINESIS_TIMEOUT: Duration = Duration::from_millis(12700);

fn get_current_shard_count(
    kinesis_client: &KinesisClient,
    stream_name: &str,
) -> Result<i64, String> {
    println!("getting current shard count");
    match block_on(kinesis_client.list_shards(ListShardsInput {
        exclusive_start_shard_id: None,
        max_results: None,
        next_token: None,
        stream_creation_timestamp: None,
        stream_name: Some(stream_name.to_owned()),
    }))
    .map_err(|e| format!("listing shards for stream {}: {}", stream_name, e))?
    .shards
    {
        Some(shards) => {
            println!("should be okay...");
            Ok(shards.len() as i64)
        } // todo: is this okay? https://stackoverflow.com/questions/28273169/how-do-i-convert-between-numeric-types-safely-and-idiomatically
        None => Err(format!("no shards found for stream {}", stream_name)),
    }
}

async fn verify_shard_count(
    kinesis_client: &KinesisClient,
    stream_name: &str,
    expected_shard_count: i64,
) -> Result<(), String> {
    let shard_count: i64 = retry::retry(|| async {
        get_current_shard_count(kinesis_client, stream_name)
            .map_err(|e| format!("verifying shard count: {}", e))
    })
    .await?;
    if shard_count != expected_shard_count {
        return Err(format!(
            "Expected {} shards, found {}",
            expected_shard_count, shard_count
        ));
    }
    Ok(())
}
