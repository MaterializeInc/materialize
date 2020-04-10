// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use failure::{bail, ResultExt};
use futures::executor::block_on;
use log::{error, warn};
use rusoto_core::{HttpClient, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{
    GetRecordsError, GetRecordsInput, GetRecordsOutput, GetShardIteratorError,
    GetShardIteratorInput, GetShardIteratorOutput, Kinesis, KinesisClient, ListShardsError,
    ListShardsInput, ListShardsOutput,
};

use dataflow_types::{Consistency, ExternalSourceConnector, KinesisSourceConnector, Timestamp};
use expr::SourceInstanceId;
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
    // Putting source information on the Timestamp channel lets this
    // Dataflow worker communicate that it has created a source.
    let ts = if read_kinesis {
        let prev = timestamp_histories
            .borrow_mut()
            .insert(id.clone(), HashMap::new());
        assert!(prev.is_none());
        timestamp_tx.as_ref().borrow_mut().push((
            id,
            Some((
                ExternalSourceConnector::Kinesis(connector.clone()),
                consistency,
            )),
        ));
        Some(timestamp_tx)
    } else {
        None
    };

    let mut state = create_state(connector);

    let (stream, capability) = source(id, ts, scope, &name.clone(), move |info| {
        let activator = scope.activator_for(&info.address[..]);

        move |cap, output| {
            // todo@jldlaughlin: We need to be able to update our list of shards dynamically. #2607
            let (client, shard_queue) = match &mut state {
                Ok(state) => state,
                Err(e) => {
                    error!("failed to create Kinesis state: {}", e);
                    return SourceStatus::Done;
                }
            };

            // If reading from Kinesis takes more than 10 milliseconds,
            // pause execution and reactivate later.
            let timer = std::time::Instant::now();

            // Rotate through the queue of shards. Start with a new
            // shard on each activation.
            while let Some(shard_information) = shard_queue.pop_front() {
                // While the next_shard_iterator is Some(iterator), the shard is open
                // and could return more data.
                let (shard_id, mut shard_iterator) = shard_information;
                while let Some(iterator) = &shard_iterator {
                    // Pushing back to the shard_queue will allow us to read from the
                    // shard again.
                    let get_records_output = match block_on(get_records(&client, &iterator)) {
                        Ok(output) => {
                            shard_iterator = output.next_shard_iterator.clone();
                            shard_queue.push_back((shard_id.clone(), shard_iterator.clone()));
                            output
                        }
                        Err(RusotoError::HttpDispatch(e)) => {
                            // todo@jldlaughlin: Parse this to determine fatal/retriable?
                            // Retry reading from the same iterator.
                            shard_queue.push_back((shard_id, shard_iterator.clone()));
                            error!("{}", e);
                            activator.activate_after(get_reactivation_duration(timer));
                            return SourceStatus::Alive;
                        }
                        Err(RusotoError::Service(GetRecordsError::ExpiredIterator(e))) => {
                            // todo@jldlaughlin: Will need track source offsets to grab a new iterator.
                            error!("{}", e);
                            return SourceStatus::Done;
                        }
                        Err(RusotoError::Service(
                            GetRecordsError::ProvisionedThroughputExceeded(_),
                        )) => {
                            // Retry reading from the same iterator.
                            shard_queue.push_back((shard_id, shard_iterator.clone()));
                            activator.activate();
                            return SourceStatus::Alive;
                        }
                        Err(e) => {
                            // Fatal service errors:
                            //  - InvalidArgument
                            //  - KMSAccessDenied, KMSDisabled, KMSInvalidState, KMSNotFound,
                            //    KMSOptInRequired, KMSThrottling
                            //  - ResourceNotFound
                            //
                            // Other fatal Rusoto errors:
                            // - Credentials
                            // - Validation
                            // - ParseError
                            // - Unknown (raw HTTP provided)
                            // - Blocking
                            error!("{}", e);
                            return SourceStatus::Done;
                        }
                    };

                    for record in get_records_output.records {
                        let data = record.data.as_ref().to_vec();
                        output.session(&cap).give((data, None));
                    }
                    downgrade_capability(cap, &name);

                    if let Some(0) = get_records_output.millis_behind_latest {
                        // This activation does the following:
                        //      1. Ensures we poll Kinesis more often than the eviction timeout (5 minutes)
                        //      2. Proactively and frequently reactivates this source, since we don't have a
                        //         smarter solution atm.
                        activator.activate();
                        return SourceStatus::Alive;
                    }

                    if timer.elapsed().as_millis() > 10 {
                        // todo: fix this comment -- copied from kafka
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
                    activator.activate();
                }
            }
            // When there are no more shards in the queue,
            // all shards for the stream have been closed and the
            // source is finished.
            SourceStatus::Done
        }
    });

    if read_kinesis {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

// todo: Better error handling here! Not all errors mean we're done/can't progress.
fn create_state(
    c: KinesisSourceConnector,
) -> Result<(KinesisClient, VecDeque<(String, Option<String>)>), failure::Error> {
    let http_client = HttpClient::new()?;
    let provider = StaticProvider::new(c.access_key, c.secret_access_key, c.token, None);
    let client = KinesisClient::new_with(http_client, provider, c.region);

    let shard_queue = match block_on(get_shards_list(&client, &c.stream_name))
        .with_context(|e| format!("fetching shard list: {}", e))?
        .shards
    {
        Some(shards) => {
            let mut shard_queue: VecDeque<(String, Option<String>)> = VecDeque::new();
            for shard in shards {
                shard_queue.push_back((
                    shard.shard_id.clone(),
                    block_on(get_shard_iterator(
                        &client,
                        &shard.shard_id,
                        &c.stream_name,
                        "TRIM_HORIZON",
                    ))
                    .with_context(|e| format!("fetching shard iterator: {}", e))?
                    .shard_iterator,
                ));
            }
            shard_queue
        }
        None => bail!("kinesis stream does not contain any shards"),
    };
    Ok((client, shard_queue))
}

fn downgrade_capability(cap: &mut Capability<u64>, name: &str) {
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

async fn get_shards_list(
    client: &KinesisClient,
    stream_name: &str,
) -> Result<ListShardsOutput, RusotoError<ListShardsError>> {
    let list_shards_input = ListShardsInput {
        exclusive_start_shard_id: None,
        max_results: None,
        next_token: None,
        stream_creation_timestamp: None,
        stream_name: Some(String::from(stream_name)),
    };
    client.list_shards(list_shards_input).await
}
