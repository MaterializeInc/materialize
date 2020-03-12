// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dataflow_types::{Consistency, ExternalSourceConnector, KinesisSourceConnector, Timestamp};
use expr::SourceInstanceId;
use futures::executor::block_on;
use log::{error, warn};
use rusoto_core::{HttpClient, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{
    GetRecordsError, GetRecordsInput, GetRecordsOutput, GetShardIteratorError,
    GetShardIteratorInput, GetShardIteratorOutput, Kinesis, KinesisClient, ListShardsInput,
    ListShardsOutput,
};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};

use super::util::source;
use super::{SourceStatus, SourceToken};
use crate::server::{TimestampChanges, TimestampHistories};

#[allow(clippy::too_many_arguments)]
pub fn kinesis<G>(
    scope: &G,
    name: String,
    connector: KinesisSourceConnector,
    id: SourceInstanceId,
    _advance_timestamp: bool,
    timestamp_histories: TimestampHistories,
    timestamp_tx: TimestampChanges,
    consistency: Consistency,
    read_kinesis: bool,
) -> (Stream<G, (Vec<u8>, Option<i64>)>, Option<SourceToken>)
where
    G: Scope<Timestamp = Timestamp>,
{
    let KinesisSourceConnector {
        stream_name,
        region,
        access_key,
        secret_access_key,
    } = connector.clone();

    // Putting source information on the Timestamp channel lets this
    // Dataflow worker communicate that it has created a source.
    let ts = if read_kinesis {
        let prev = timestamp_histories
            .borrow_mut()
            .insert(id.clone(), HashMap::new());
        assert!(prev.is_none());
        timestamp_tx.as_ref().borrow_mut().push((
            id,
            Some((ExternalSourceConnector::Kinesis(connector), consistency)),
        ));
        Some(timestamp_tx)
    } else {
        None
    };

    let (stream, capability) = source(id, ts, scope, &name.clone(), move |info| {
        let activator = scope.activator_for(&info.address[..]);

        // Create a new AWS Kinesis Client.
        let request_dispatcher = HttpClient::new().unwrap();
        let provider = StaticProvider::new(access_key, secret_access_key, None, None);
        let client = KinesisClient::new_with(request_dispatcher, provider, region);

        // todo@jldlaughlin: Read from multiple shards! #2222
        let shards = block_on(get_shards_list(&client, &stream_name));
        let shard = match shards.shards {
            Some(shards) => match shards.len() {
                1 => Some(shards[0].clone()),
                _ => {
                    error!("Materialize currently only supports reading from a single shard. Found: {}", shards.len());
                    None
                }
            },
            None => {
                error!("Did not find any shards in Kinesis stream: {}", stream_name);
                None
            }
        };
        let mut shard_iterator = match &shard {
            Some(shard) => {
                match block_on(get_shard_iterator(
                    &client,
                    &shard.shard_id,
                    &stream_name,
                    "TRIM_HORIZON",
                )) {
                    Ok(output) => output.shard_iterator,
                    Err(rusoto_err) => {
                        // todo: Better error handling here! Not all errors mean we're done/can't progress.
                        error!("{}", rusoto_err);
                        None
                    }
                }
            }
            None => {
                // Same error as not finding a shard above.
                None
            }
        };

        move |cap, output| {
            // If reading from Kinesis takes more than 10 milliseconds,
            // pause execution and reactivate later.
            let timer = std::time::Instant::now();
            downgrade_capabilities(cap, &name);

            // When the next_shard_iterator is null, the shard has been closed and the
            // requested iterator does not return any more data.
            while let Some(iterator) = &shard_iterator {
                // todo: Better error handling here! Not getting a response != being done.
                let get_records_output = match block_on(get_records(&client, &iterator)) {
                    Ok(output) => {
                        shard_iterator = output.next_shard_iterator.clone();
                        output
                    }
                    Err(rusoto_err) => match rusoto_err {
                        RusotoError::Service(service_err) => match service_err {
                            GetRecordsError::ProvisionedThroughputExceeded(_s) => {
                                activator.activate_after(get_reactivation_duration(timer));
                                return SourceStatus::Alive;
                            }
                            _ => {
                                error!("{}", service_err);
                                return SourceStatus::Done;
                            }
                        },
                        _ => {
                            error!("{}", rusoto_err);
                            return SourceStatus::Done;
                        }
                    },
                };

                for record in get_records_output.records {
                    let data = record.data.as_ref().to_vec();
                    output.session(&cap).give((data, None));

                    downgrade_capabilities(cap, &name);
                }

                if let Some(0) = get_records_output.millis_behind_latest {
                    // This activation does the following:
                    //      1. Ensures we poll Kinesis more often than the eviction timeout (5 minutes)
                    //      2. Proactively and frequently reactivates this source, since we don't have a
                    //         smarter solution atm.
                    // todo@jldlaughlin: Improve Kinesis source activation #2195
                    activator.activate_after(get_reactivation_duration(timer));
                    return SourceStatus::Alive;
                }

                if timer.elapsed().as_millis() > 10 {
                    // We didn't drain the entire queue, so indicate that we
                    // should run again. We suppress the activation when the
                    // queue is drained, as in that case librdkafka is
                    // configured to unpark our thread when a new message
                    // arrives.
                    activator.activate_after(get_reactivation_duration(timer));
                    return SourceStatus::Alive;
                }
                // Each Kinesis shard can support up to 5 read requests per second.
                // This will throttle ourselves.
                activator.activate_after(get_reactivation_duration(timer));
            }
            SourceStatus::Done
        }
    });

    if read_kinesis {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

fn downgrade_capabilities(cap: &mut Capability<u64>, name: &str) {
    // For now, use the system's current timestamp to downgrade
    // capabilities.
    // todo: Implement better offset tracking for Kinesis sources #2219
    let cur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;
    if cur > *cap.time() {
        cap.downgrade(&cur);
    } else {
        warn!(
            "{}: Unexpected - Kinesis source capability ahead of current system time ({} > {})",
            name.to_string(),
            *cap.time(),
            cur
        );
    }
}

// Each Kinesis shard can support up to 5 read requests
// per second. This delay in activation should help
// throttle ourselves.
fn get_reactivation_duration(timer: Instant) -> Duration {
    let elapsed = timer.elapsed();
    if elapsed.as_millis() >= 200 {
        Duration::from_millis(0)
    } else {
        Duration::from_millis(200) - elapsed
    }
}

async fn get_records(
    client: &KinesisClient,
    shard_iterator: &str,
) -> Result<GetRecordsOutput, RusotoError<GetRecordsError>> {
    let get_records_input = GetRecordsInput {
        limit: None,
        shard_iterator: String::from(shard_iterator),
    };
    client.get_records(get_records_input).await
}

async fn get_shard_iterator(
    client: &KinesisClient,
    shard_id: &str,
    stream_name: &str,
    iterator_type: &str,
) -> Result<GetShardIteratorOutput, RusotoError<GetShardIteratorError>> {
    let get_shard_iterator = GetShardIteratorInput {
        shard_id: String::from(shard_id),
        shard_iterator_type: String::from(iterator_type),
        starting_sequence_number: None,
        stream_name: String::from(stream_name),
        timestamp: None,
    };
    client.get_shard_iterator(get_shard_iterator).await
}

async fn get_shards_list(client: &KinesisClient, stream_name: &str) -> ListShardsOutput {
    let list_shards_input = ListShardsInput {
        exclusive_start_shard_id: None,
        max_results: None,
        next_token: None,
        stream_creation_timestamp: None,
        stream_name: Some(String::from(stream_name)),
    };
    client.list_shards(list_shards_input).await.unwrap()
}
