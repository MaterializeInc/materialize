// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::time::Duration;

use crate::action::State;
use rusoto_kinesis::{DescribeStreamInput, Kinesis};

use ore::retry;

pub async fn wait_for_stream_shards(
    state: &State,
    stream_name: String,
    target_shard_count: i64,
) -> Result<(), String> {
    retry::retry_for(
        Duration::from_secs_f64(if state.default_timeout > 60.0 {
            state.default_timeout
        } else {
            60.0
        }),
        |_| async {
            let description = state
                .kinesis_client
                .describe_stream(DescribeStreamInput {
                    exclusive_start_shard_id: None,
                    limit: None,
                    stream_name: stream_name.clone(),
                })
                .await
                .map_err(|e| format!("getting current shard count: {}", e))?
                .stream_description;
            if description.stream_status != "ACTIVE" {
                return Err(format!(
                    "stream {} is not active, is {}",
                    stream_name, description.stream_status
                ));
            }

            let active_shards_len = i64::try_from(
                description
                    .shards
                    .iter()
                    .filter(|shard| shard.sequence_number_range.ending_sequence_number.is_none())
                    .count(),
            )
            .map_err(|e| format!("converting shard length to i64: {}", e))?;
            if active_shards_len != target_shard_count {
                return Err(format!(
                    "expected {} shards, found {}",
                    target_shard_count, active_shards_len
                ));
            }

            Ok(())
        },
    )
    .await
}
