// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashSet, VecDeque};
use std::convert::TryInto;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use aws_util::kinesis::{get_shard_ids, get_shard_iterator};
use futures::executor::block_on;
use lazy_static::lazy_static;
use log::error;
use prometheus::{register_int_gauge_vec, IntGauge, IntGaugeVec};
use rusoto_core::RusotoError;
use rusoto_kinesis::{GetRecordsError, GetRecordsInput, GetRecordsOutput, Kinesis, KinesisClient};
use timely::scheduling::{Activator, SyncActivator};

use dataflow_types::{DataEncoding, ExternalSourceConnector, KinesisSourceConnector, MzOffset};
use expr::{PartitionId, SourceInstanceId};

use crate::logging::materialized::Logger;
use crate::source::{
    ConsistencyInfo, NextMessage, PartitionMetrics, SourceConstructor, SourceInfo, SourceMessage,
};

lazy_static! {
    static ref MILLIS_BEHIND_LATEST: IntGaugeVec = register_int_gauge_vec!(
        "mz_kinesis_shard_millis_behind_latest",
        "How far the shard is behind the tip of the stream",
        &["stream_name", "shard_id"]
    )
    .expect("Can construct an intgauge for millis_behind_latest");
}

/// To read all data from a Kinesis stream, we need to continually update
/// our knowledge of the stream's shards by calling the ListShards API.
///
/// We will call ListShards at most this often to stay under the API rate limit
/// (100x/sec per stream) and to improve source performance overall.
const KINESIS_SHARD_REFRESH_RATE: Duration = Duration::from_secs(60);

/// Contains all information necessary to ingest data from Kinesis
pub struct KinesisSourceInfo {
    /// Source Name
    name: String,
    /// Unique Source Id
    id: SourceInstanceId,
    /// Field is set if this operator is responsible for ingesting data
    is_activated_reader: bool,
    /// Kinesis client used to obtain records
    kinesis_client: KinesisClient,
    /// Timely worker logger for source events
    logger: Option<Logger>,
    /// The name of the stream
    stream_name: String,
    /// The set of active shards
    shard_set: HashSet<String>,
    /// A queue representing the next shard to read from. This is necessary
    /// to ensure that all shards are read from uniformly
    shard_queue: VecDeque<(String, Option<String>)>,
    /// The time at which we last refreshed metadata
    /// TODO(natacha): this should be moved to timestamper
    last_checked_shards: Instant,
    /// Storage for messages that have not yet been timestamped
    buffered_messages: VecDeque<SourceMessage<Vec<u8>>>,
    /// Count of processed message
    processed_message_count: i64,
}

impl SourceConstructor<Vec<u8>> for KinesisSourceInfo {
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        active: bool,
        _worker_id: usize,
        _worker_count: usize,
        logger: Option<Logger>,
        _consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        consistency_info: &mut ConsistencyInfo,
        _encoding: DataEncoding,
    ) -> Result<Self, anyhow::Error> {
        let kc = match connector {
            ExternalSourceConnector::Kinesis(kc) => kc,
            _ => unreachable!(),
        };

        let state = block_on(create_state(
            kc,
            &source_name,
            source_id,
            consistency_info,
            logger.clone(),
        ));
        match state {
            Ok((kinesis_client, stream_name, shard_set, shard_queue)) => Ok(KinesisSourceInfo {
                name: source_name,
                id: source_id,
                is_activated_reader: active,
                kinesis_client,
                logger,
                shard_queue,
                last_checked_shards: Instant::now(),
                buffered_messages: VecDeque::new(),
                shard_set,
                stream_name,
                processed_message_count: 0,
            }),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
}

impl KinesisSourceInfo {
    async fn update_shard_information(
        &mut self,
        consistency_info: &mut ConsistencyInfo,
    ) -> Result<(), anyhow::Error> {
        let new_shards: HashSet<String> = get_shard_ids(&self.kinesis_client, &self.stream_name)
            .await?
            .difference(&self.shard_set)
            .map(|shard_id| shard_id.to_owned())
            .collect();
        for shard_id in new_shards {
            self.shard_set.insert(shard_id.clone());
            self.shard_queue.push_back((
                shard_id.clone(),
                get_shard_iterator(&self.kinesis_client, &self.stream_name, &shard_id).await?,
            ));
            let kinesis_id = PartitionId::Kinesis(shard_id);
            consistency_info.update_partition_metadata(kinesis_id.clone());
            consistency_info.partition_metrics.insert(
                kinesis_id.clone(),
                PartitionMetrics::new(
                    &self.name,
                    self.id,
                    &kinesis_id.to_string(),
                    self.logger.clone(),
                ),
            );
        }
        Ok(())
    }

    /// Obtains the next record for this shard given a shard iterator
    async fn get_records(
        &self,
        shard_iterator: &str,
    ) -> Result<GetRecordsOutput, RusotoError<GetRecordsError>> {
        self.kinesis_client
            .get_records(GetRecordsInput {
                limit: None,
                shard_iterator: String::from(shard_iterator),
            })
            .await
    }
}

impl SourceInfo<Vec<u8>> for KinesisSourceInfo {
    fn can_close_timestamp(
        &self,
        consistency_info: &ConsistencyInfo,
        pid: &PartitionId,
        offset: MzOffset,
    ) -> bool {
        if !self.is_activated_reader {
            true
        } else {
            // Guaranteed to exist if we receive a message from this partition
            let last_offset = consistency_info
                .partition_metadata
                .get(&pid)
                .unwrap()
                .offset;
            last_offset >= offset
        }
    }

    fn get_worker_partition_count(&self) -> i32 {
        if self.is_activated_reader {
            // Kinesis does not support more than i32 partitions
            self.shard_set.len().try_into().unwrap()
        } else {
            0
        }
    }

    fn has_partition(&self, _partition_id: PartitionId) -> bool {
        self.is_activated_reader
    }

    fn ensure_has_partition(&mut self, _consistency_info: &mut ConsistencyInfo, _pid: PartitionId) {
        //TODO(natacha): do nothing for now, as do not currently use timestamper
    }

    fn update_partition_count(
        &mut self,
        _consistency_info: &mut ConsistencyInfo,
        _partition_count: i32,
    ) {
        //TODO(natacha): do nothing for now as do not currently use timestamper
    }

    fn get_next_message(
        &mut self,
        consistency_info: &mut ConsistencyInfo,
        activator: &Activator,
    ) -> Result<NextMessage<Vec<u8>>, anyhow::Error> {
        assert_eq!(self.shard_queue.len(), self.shard_set.len());

        //TODO move to timestamper
        if self.last_checked_shards.elapsed() >= KINESIS_SHARD_REFRESH_RATE {
            if let Err(e) = block_on(self.update_shard_information(consistency_info)) {
                error!("{:#?}", e);
                return Err(anyhow::Error::msg(e.to_string()));
            }
            self.last_checked_shards = std::time::Instant::now();
        }

        if let Some(message) = self.buffered_messages.pop_front() {
            Ok(NextMessage::Ready(message))
        } else {
            // Rotate through all of a stream's shards, start with a new shard on each activation.
            if let Some((shard_id, mut shard_iterator)) = self.shard_queue.pop_front() {
                if let Some(iterator) = &shard_iterator {
                    let get_records_output = match block_on(self.get_records(&iterator)) {
                        Ok(output) => {
                            shard_iterator = output.next_shard_iterator.clone();
                            if let Some(millis) = output.millis_behind_latest {
                                let shard_metrics: IntGauge = MILLIS_BEHIND_LATEST
                                    .with_label_values(&[&self.stream_name, &shard_id]);
                                shard_metrics.set(millis);
                            }
                            output
                        }
                        Err(RusotoError::HttpDispatch(e)) => {
                            // todo@jldlaughlin: Parse this to determine fatal/retriable?
                            error!("{}", e);
                            self.shard_queue.push_back((shard_id, shard_iterator));
                            activator.activate();
                            // Do not send error message as this would cause source to terminate
                            return Ok(NextMessage::Pending);
                        }
                        Err(RusotoError::Service(GetRecordsError::ExpiredIterator(e))) => {
                            // todo@jldlaughlin: Will need track source offsets to grab a new iterator.
                            error!("{}", e);
                            return Err(anyhow::Error::msg(e));
                        }
                        Err(RusotoError::Service(
                            GetRecordsError::ProvisionedThroughputExceeded(_),
                        )) => {
                            self.shard_queue.push_back((shard_id, shard_iterator));
                            activator.activate();
                            // Do not send error message as this would cause source to terminate
                            return Ok(NextMessage::Pending);
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
                            return Err(anyhow!("{}", e));
                        }
                    };

                    for record in get_records_output.records {
                        let data = record.data.as_ref().to_vec();
                        self.processed_message_count += 1;
                        let source_message = SourceMessage {
                            partition: PartitionId::Kinesis(shard_id.clone()),
                            offset: MzOffset {
                                //TODO: should MzOffset be modified to be a string?
                                offset: self.processed_message_count,
                            },
                            upstream_time_millis: None,
                            key: None,
                            payload: Some(data),
                        };
                        self.buffered_messages.push_back(source_message);
                    }
                    self.shard_queue.push_back((shard_id, shard_iterator));
                }
            }
            Ok(match self.buffered_messages.pop_front() {
                Some(message) => NextMessage::Ready(message),
                None => NextMessage::Pending,
            })
        }
    }
}

/// Creates the necessary data-structures for shard management
// todo: Better error handling here! Not all errors mean we're done/can't progress.
async fn create_state(
    c: KinesisSourceConnector,
    name: &str,
    id: SourceInstanceId,
    consistency_info: &mut ConsistencyInfo,
    logger: Option<Logger>,
) -> Result<
    (
        KinesisClient,
        String,
        HashSet<String>,
        VecDeque<(String, Option<String>)>,
    ),
    anyhow::Error,
> {
    let kinesis_client = aws_util::client::kinesis(c.aws_info).await?;

    let shard_set: HashSet<String> = get_shard_ids(&kinesis_client, &c.stream_name).await?;
    let mut shard_queue: VecDeque<(String, Option<String>)> = VecDeque::new();
    for shard_id in &shard_set {
        let kinesis_id = PartitionId::Kinesis(shard_id.clone());
        shard_queue.push_back((
            shard_id.clone(),
            get_shard_iterator(&kinesis_client, &c.stream_name, shard_id).await?,
        ));
        consistency_info.update_partition_metadata(kinesis_id.clone());
        consistency_info.partition_metrics.insert(
            kinesis_id.clone(),
            PartitionMetrics::new(name, id, &kinesis_id.to_string(), logger.clone()),
        );
    }

    Ok((
        kinesis_client,
        c.stream_name.clone(),
        shard_set,
        shard_queue,
    ))
}
