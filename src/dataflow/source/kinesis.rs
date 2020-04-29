// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use failure::ResultExt;
use futures::executor::block_on;
use lazy_static::lazy_static;
use log::{error, warn};
use prometheus::{register_int_counter, register_int_gauge_vec, IntCounter, IntGauge, IntGaugeVec};
use rusoto_core::{HttpClient, RusotoError};
use rusoto_credential::StaticProvider;
use rusoto_kinesis::{
    GetRecordsError, GetRecordsInput, GetRecordsOutput, GetShardIteratorInput, Kinesis,
    KinesisClient,
};

use dataflow_types::{ExternalSourceConnector, KinesisSourceConnector, Timestamp};
use kinesis_util::get_shard_ids;
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::scheduling::Activator;

use super::util::source;
use super::{SourceConfig, SourceStatus, SourceToken};
use crate::metrics::EVENTS_COUNTER;

lazy_static! {
    static ref MILLIS_BEHIND_LATEST: IntGaugeVec = register_int_gauge_vec!(
        "mz_kinesis_shard_millis_behind_latest",
        "How far the shard is behind the tip of the stream",
        &["stream_name", "shard_id"]
    )
    .expect("Can construct an intgauge for millis_behind_latest");
}

lazy_static! {
    static ref BYTES_READ_COUNTER: IntCounter = register_int_counter!(
        "mz_kinesis_bytes_read_total",
        "Count of kinesis bytes we have read from the wire"
    )
    .unwrap();
}

/// To read all data from a Kinesis stream, we need to continually update
/// our knowledge of the stream's shards by calling the ListShards API.
///
/// We will call ListShards at most this often to stay under the API rate limit
/// (100x/sec per stream) and to improve source performance overall.
const KINESIS_SHARD_REFRESH_RATE: Duration = Duration::from_secs(60);

pub fn kinesis<G>(
    config: SourceConfig<G>,
    connector: KinesisSourceConnector,
) -> (Stream<G, (Vec<u8>, Option<i64>)>, Option<SourceToken>)
where
    G: Scope<Timestamp = Timestamp>,
{
    // Putting source information on the Timestamp channel lets this
    // Dataflow worker communicate that it has created a source.
    let ts = if config.active {
        let prev = config
            .timestamp_histories
            .borrow_mut()
            .insert(config.id.clone(), HashMap::new());
        assert!(prev.is_none());
        config.timestamp_tx.as_ref().borrow_mut().push((
            config.id,
            Some((
                ExternalSourceConnector::Kinesis(connector.clone()),
                config.consistency,
            )),
        ));
        Some(config.timestamp_tx)
    } else {
        None
    };

    let mut state = block_on(create_state(connector));
    let mut last_checked_shards = std::time::Instant::now();

    let SourceConfig { name, scope, .. } = config;

    let (stream, capability) = source(config.id, ts, scope, &name.clone(), move |info| {
        let activator = scope.activator_for(&info.address[..]);

        move |cap, output| {
            let (client, stream_name, shard_set, shard_queue) = match &mut state {
                Ok(state) => state,
                Err(e) => {
                    error!("failed to create Kinesis state: {}", e);
                    return SourceStatus::Done;
                }
            };

            if last_checked_shards.elapsed() >= KINESIS_SHARD_REFRESH_RATE {
                if let Err(e) = block_on(update_shard_information(
                    &client,
                    &stream_name,
                    shard_set,
                    shard_queue,
                )) {
                    error!("{}", e);
                    return SourceStatus::Done;
                }
                last_checked_shards = std::time::Instant::now();
            }

            let timer = std::time::Instant::now();
            // Rotate through all of a stream's shards, start with a new shard on each activation.
            while let Some((shard_id, mut shard_iterator)) = shard_queue.pop_front() {
                // While the next_shard_iterator is Some(iterator), the shard is open
                // and could return more data.
                while let Some(iterator) = &shard_iterator {
                    // Pushing back to the shard_queue will allow us to read from the
                    // shard again.
                    let get_records_output = match block_on(get_records(&client, &iterator)) {
                        Ok(output) => {
                            shard_iterator = output.next_shard_iterator.clone();
                            if let Some(millis) = output.millis_behind_latest {
                                let shard_metrics: IntGauge = MILLIS_BEHIND_LATEST
                                    .with_label_values(&[&stream_name, &shard_id]);
                                shard_metrics.set(millis);
                            }
                            output
                        }
                        Err(RusotoError::HttpDispatch(e)) => {
                            // todo@jldlaughlin: Parse this to determine fatal/retriable?
                            error!("{}", e);
                            return reactivate_kinesis_source(
                                &activator,
                                shard_queue,
                                &shard_id,
                                shard_iterator,
                            );
                        }
                        Err(RusotoError::Service(GetRecordsError::ExpiredIterator(e))) => {
                            // todo@jldlaughlin: Will need track source offsets to grab a new iterator.
                            error!("{}", e);
                            return SourceStatus::Done;
                        }
                        Err(RusotoError::Service(
                            GetRecordsError::ProvisionedThroughputExceeded(_),
                        )) => {
                            return reactivate_kinesis_source(
                                &activator,
                                shard_queue,
                                &shard_id,
                                shard_iterator,
                            );
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

                    let mut events_success = 0;
                    let mut bytes_read = 0;
                    for record in get_records_output.records {
                        let data = record.data.as_ref().to_vec();
                        bytes_read += data.len() as i64;
                        output.session(&cap).give((data, None));
                        events_success += 1;
                    }
                    downgrade_capability(cap, &name);
                    EVENTS_COUNTER.raw.success.inc_by(events_success);
                    BYTES_READ_COUNTER.inc_by(bytes_read);

                    if get_records_output.millis_behind_latest == Some(0)
                        || timer.elapsed().as_millis() > 10
                    {
                        return reactivate_kinesis_source(
                            &activator,
                            shard_queue,
                            &shard_id,
                            shard_iterator,
                        );
                    }

                    // Each Kinesis shard can support up to 5 read requests per second.
                    // This will throttle ourselves.
                    activator.activate();
                }
            }
            // todo@jdlaughlin: Revisit when Kinesis sources should be marked as done.
            // Should switch to when we fail to get a stream description (the stream is
            // closed)?
            SourceStatus::Done
        }
    });

    if config.active {
        (stream, Some(capability))
    } else {
        (stream, None)
    }
}

fn reactivate_kinesis_source(
    activator: &Activator,
    shard_queue: &mut VecDeque<(String, Option<String>)>,
    shard_id: &str,
    shard_iterator: Option<String>,
) -> SourceStatus {
    shard_queue.push_back((shard_id.to_owned(), shard_iterator));
    activator.activate();
    SourceStatus::Alive
}

// todo: Better error handling here! Not all errors mean we're done/can't progress.
async fn create_state(
    c: KinesisSourceConnector,
) -> Result<
    (
        KinesisClient,
        String,
        HashSet<String>,
        VecDeque<(String, Option<String>)>,
    ),
    failure::Error,
> {
    let http_client = HttpClient::new()?;
    let provider = StaticProvider::new(c.access_key, c.secret_access_key, c.token, None);
    let client = KinesisClient::new_with(http_client, provider, c.region);

    let shard_set: HashSet<String> = get_shard_ids(&client, &c.stream_name).await?;
    let mut shard_queue: VecDeque<(String, Option<String>)> = VecDeque::new();
    for shard_id in &shard_set {
        shard_queue.push_back((
            shard_id.clone(),
            get_shard_iterator(&client, shard_id, &c.stream_name, "TRIM_HORIZON").await?,
        ))
    }
    Ok((client, c.stream_name.clone(), shard_set, shard_queue))
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

async fn get_records(
    client: &KinesisClient,
    shard_iterator: &str,
) -> Result<GetRecordsOutput, RusotoError<GetRecordsError>> {
    client
        .get_records(GetRecordsInput {
            limit: None,
            shard_iterator: String::from(shard_iterator),
        })
        .await
}

async fn get_shard_iterator(
    client: &KinesisClient,
    shard_id: &str,
    stream_name: &str,
    iterator_type: &str,
) -> Result<Option<String>, failure::Error> {
    Ok(client
        .get_shard_iterator(GetShardIteratorInput {
            shard_id: String::from(shard_id),
            shard_iterator_type: String::from(iterator_type),
            starting_sequence_number: None,
            stream_name: String::from(stream_name),
            timestamp: None,
        })
        .await
        .with_context(|e| format!("fetching shard iterator: {}", e))?
        .shard_iterator)
}

async fn update_shard_information(
    client: &KinesisClient,
    stream_name: &str,
    shard_set: &mut HashSet<String>,
    shard_queue: &mut VecDeque<(String, Option<String>)>,
) -> Result<(), failure::Error> {
    let new_shards: HashSet<String> = get_shard_ids(&client, stream_name)
        .await?
        .difference(shard_set)
        .map(|shard_id| shard_id.to_owned())
        .collect();
    for shard_id in new_shards {
        shard_set.insert(shard_id.clone());
        shard_queue.push_back((
            shard_id.clone(),
            get_shard_iterator(&client, &shard_id, stream_name, "TRIM_HORIZON").await?,
        ));
    }
    Ok(())
}
