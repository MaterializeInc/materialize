// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::executor::block_on;
use log::warn;
use regex::Regex;
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{
    GetRecordsError, GetRecordsInput, GetRecordsOutput, GetShardIteratorInput,
    GetShardIteratorOutput, Kinesis, KinesisClient, ListShardsInput, ListShardsOutput, Shard,
};

use dataflow_types::{Consistency, ExternalSourceConnector, KinesisSourceConnector, Timestamp};
use expr::SourceInstanceId;
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
        arn,
        access_key,
        secret_access_key,
        region,
    } = connector.clone();

    // Putting source information on the Timestamp channel lets this
    // Dataflow worker communicate that it has created a source.
    let ts = if read_kinesis {
        let prev = timestamp_histories.borrow_mut().insert(id.clone(), vec![]);
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
        let r: Region = region
            .parse()
            .unwrap_or_else(|_| panic!("Failed to parse AWS region: {}", region));
        let client = KinesisClient::new_with(request_dispatcher, provider, r);

        // Get the stream's name from the provided ARN.
        let re = Regex::new(r"/(.*)").unwrap();
        let captured_match = re.captures(&arn).unwrap().get(1); // do something better?
        let stream_name = match captured_match {
            Some(m) => m.as_str(),
            None => panic!(
                "Failed to read a stream name from the provided ARN: {}",
                arn
            ),
        };

        // todo@jldlaughlin: Read from multiple shards! #2222
        let shards = block_on(get_shards_list(&client, stream_name));
        let shard = match shards.shards {
            Some(shards) => {
                if shards.len() != 1 {
                    panic!("Materialize currently only supports reading from a single shard. Found: {}", shards.len());
                }
                shards[0].clone()
            }
            None => panic!("Did not find any shards in Kinesis stream: {}", stream_name),
        };
        let shard_iterator_output = block_on(get_shard_iterator(&client, &shard, stream_name));
        let mut shard_iterator = shard_iterator_output.shard_iterator.unwrap();

        move |cap, output| {
            // If reading from Kinesis takes more than 10 milliseconds,
            // pause execution and reactivate later.
            let timer = std::time::Instant::now();

            let mut get_records_response = get_records_wrapper(&client, &shard_iterator);
            // When the next_shard_iterator is null, the shard has been closed and the
            // requested iterator does not return any more data.
            while let Some(iterator) = get_records_response.next_shard_iterator {
                if let Some(millis) = get_records_response.millis_behind_latest {
                    if millis == 0 {
                        // This activation does the following:
                        //      1. Ensures we poll Kinesis more often than the eviction timeout (5 minutes)
                        //      2. Proactively and frequently reactivates this source, since we don't have a
                        //         smarter solution atm.
                        // todo@jldlaughlin: Improve Kinesis source activation #2195
                        activator.activate_after(Duration::from_secs(5));
                        return SourceStatus::Alive;
                    }
                }

                for record in get_records_response.records {
                    // For now, use the system's current timestamp to downgrade
                    // capabilities.
                    // todo: Implement better offset tracking for Kinesis sources #2219
                    let cur = generate_timestamp();
                    if cur > *cap.time() {
                        cap.downgrade(&cur);
                    } else {
                        warn!(
                            "{}: Unexpected - Kinesis source capability ahead of current system time ({} > {})",
                            name, *cap.time(), cur
                        );
                    }

                    let data = record.data.as_ref().to_vec();
                    output.session(&cap).give((data, None))
                }

                if timer.elapsed().as_millis() > 100 {
                    // We didn't drain the entire queue, so indicate that we
                    // should run again. We suppress the activation when the
                    // queue is drained, as in that case librdkafka is
                    // configured to unpark our thread when a new message
                    // arrives.
                    activator.activate();
                    return SourceStatus::Alive;
                }
                // Each Kinesis shard can support up to 5 read requests per second.
                // This will throttle ourselves.
                thread::sleep(Duration::from_millis(200));
                shard_iterator = iterator;
                get_records_response = get_records_wrapper(&client, &shard_iterator);
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

fn generate_timestamp() -> u64 {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

fn get_records_wrapper(client: &KinesisClient, shard_iterator: &str) -> GetRecordsOutput {
    let result = block_on(get_records(client, shard_iterator));
    match result {
        Ok(get_records_output) => get_records_output,
        Err(e) => match e {
            RusotoError::Service(service_err) => match service_err {
                GetRecordsError::ProvisionedThroughputExceeded(_s) => {
                    thread::sleep(Duration::from_secs(1));
                    get_records_wrapper(client, shard_iterator)
                }
                _ => panic!(service_err),
            },
            _ => panic!(e),
        },
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
    shard: &Shard,
    stream_name: &str,
) -> GetShardIteratorOutput {
    let get_shard_iterator = GetShardIteratorInput {
        shard_id: shard.shard_id.clone(),
        shard_iterator_type: String::from("TRIM_HORIZON"),
        starting_sequence_number: None,
        stream_name: String::from(stream_name),
        timestamp: None,
    };
    client.get_shard_iterator(get_shard_iterator).await.unwrap()
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
