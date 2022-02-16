// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::{Client as KinesisClient, SdkError};
use futures::executor::block_on;
use mz_dataflow_types::sources::AwsExternalId;
use prometheus::core::AtomicI64;
use timely::scheduling::SyncActivator;
use tracing::error;

use mz_aws_util::kinesis;
use mz_dataflow_types::sources::{
    encoding::SourceDataEncoding, ExternalSourceConnector, KinesisSourceConnector, MzOffset,
};
use mz_expr::{PartitionId, SourceInstanceId};
use mz_ore::metrics::{DeleteOnDropGauge, GaugeVecExt};

use crate::logging::materialized::Logger;
use crate::source::metrics::{KinesisMetrics, SourceBaseMetrics};
use crate::source::{NextMessage, SourceMessage, SourceReader};

/// To read all data from a Kinesis stream, we need to continually update
/// our knowledge of the stream's shards by calling the ListShards API.
///
/// We will call ListShards at most this often to stay under the API rate limit
/// (100x/sec per stream) and to improve source performance overall.
const KINESIS_SHARD_REFRESH_RATE: Duration = Duration::from_secs(60);

/// Contains all information necessary to ingest data from Kinesis
pub struct KinesisSourceReader {
    /// Kinesis client used to obtain records
    kinesis_client: KinesisClient,
    /// The name of the stream
    stream_name: String,
    /// The set of active shards
    shard_set: HashMap<String, ShardMetrics>,
    /// A queue representing the next shard to read from. This is necessary
    /// to ensure that all shards are read from uniformly
    shard_queue: VecDeque<(String, Option<String>)>,
    /// The time at which we last refreshed metadata
    /// TODO(natacha): this should be moved to timestamper
    last_checked_shards: Instant,
    /// Storage for messages that have not yet been timestamped
    buffered_messages: VecDeque<SourceMessage<Option<Vec<u8>>, Option<Vec<u8>>>>,
    /// Count of processed message
    processed_message_count: i64,
    /// Metrics from which per-shard metrics get created.
    base_metrics: KinesisMetrics,
}

struct ShardMetrics {
    millis_behind_latest: DeleteOnDropGauge<'static, AtomicI64, Vec<String>>,
}

impl ShardMetrics {
    fn new(kinesis_metrics: &KinesisMetrics, stream_name: &str, shard_id: &str) -> Self {
        Self {
            millis_behind_latest: kinesis_metrics
                .millis_behind_latest
                .get_delete_on_drop_gauge(vec![stream_name.to_string(), shard_id.to_string()]),
        }
    }
}

impl KinesisSourceReader {
    async fn update_shard_information(&mut self) -> Result<(), anyhow::Error> {
        let current_shards: HashSet<_> =
            kinesis::get_shard_ids(&self.kinesis_client, &self.stream_name)
                .await?
                .collect();
        let known_shards: HashSet<_> = self.shard_set.keys().cloned().collect();
        let new_shards = current_shards
            .difference(&known_shards)
            .map(|shard_id| shard_id.to_owned());
        for shard_id in new_shards {
            self.shard_set.insert(
                shard_id.to_string(),
                ShardMetrics::new(&self.base_metrics, &self.stream_name, &shard_id),
            );
            self.shard_queue.push_back((
                shard_id.to_string(),
                kinesis::get_shard_iterator(&self.kinesis_client, &self.stream_name, &shard_id)
                    .await?,
            ));
        }
        Ok(())
    }

    /// Obtains the next record for this shard given a shard iterator
    async fn get_records(
        &self,
        shard_iterator: &str,
    ) -> Result<GetRecordsOutput, SdkError<GetRecordsError>> {
        self.kinesis_client
            .get_records()
            .shard_iterator(shard_iterator)
            .send()
            .await
    }
}

impl SourceReader for KinesisSourceReader {
    type Key = Option<Vec<u8>>;
    type Value = Option<Vec<u8>>;

    fn new(
        _source_name: String,
        _source_id: SourceInstanceId,
        _worker_id: usize,
        _worker_count: usize,
        _consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        aws_external_id: AwsExternalId,
        _restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _: Option<Logger>,
        base_metrics: SourceBaseMetrics,
    ) -> Result<Self, anyhow::Error> {
        let kc = match connector {
            ExternalSourceConnector::Kinesis(kc) => kc,
            _ => unreachable!(),
        };

        let state = block_on(create_state(&base_metrics.kinesis, kc, aws_external_id));
        match state {
            Ok((kinesis_client, stream_name, shard_set, shard_queue)) => Ok(KinesisSourceReader {
                kinesis_client,
                shard_queue,
                last_checked_shards: Instant::now(),
                buffered_messages: VecDeque::new(),
                shard_set,
                stream_name,
                processed_message_count: 0,
                base_metrics: base_metrics.kinesis,
            }),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
    fn get_next_message(&mut self) -> Result<NextMessage<Self::Key, Self::Value>, anyhow::Error> {
        assert_eq!(self.shard_queue.len(), self.shard_set.len());

        //TODO move to timestamper
        if self.last_checked_shards.elapsed() >= KINESIS_SHARD_REFRESH_RATE {
            if let Err(e) = block_on(self.update_shard_information()) {
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
                                self.shard_set
                                    .get(&shard_id)
                                    .unwrap()
                                    .millis_behind_latest
                                    .set(millis);
                            }
                            output
                        }
                        Err(SdkError::DispatchFailure(e)) => {
                            // todo@jldlaughlin: Parse this to determine fatal/retriable?
                            error!("{}", e);
                            self.shard_queue.push_back((shard_id, shard_iterator));
                            // Do not send error message as this would cause source to terminate
                            return Ok(NextMessage::TransientDelay);
                        }
                        Err(SdkError::ServiceError { err, .. })
                            if err.is_expired_iterator_exception() =>
                        {
                            // todo@jldlaughlin: Will need track source offsets to grab a new iterator.
                            error!("{}", err);
                            return Err(anyhow::Error::msg(err));
                        }
                        Err(SdkError::ServiceError { err, .. })
                            if err.is_provisioned_throughput_exceeded_exception() =>
                        {
                            self.shard_queue.push_back((shard_id, shard_iterator));
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

                    for record in get_records_output.records.unwrap_or_default() {
                        let data = record
                            .data
                            .map(|blob| blob.into_inner())
                            .unwrap_or_else(Vec::new);
                        self.processed_message_count += 1;
                        let source_message = SourceMessage {
                            partition: PartitionId::None,
                            offset: MzOffset {
                                //TODO: should MzOffset be modified to be a string?
                                offset: self.processed_message_count,
                            },
                            upstream_time_millis: None,
                            key: None,
                            value: Some(data),
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
    base_metrics: &KinesisMetrics,
    c: KinesisSourceConnector,
    aws_external_id: AwsExternalId,
) -> Result<
    (
        KinesisClient,
        String,
        HashMap<String, ShardMetrics>,
        VecDeque<(String, Option<String>)>,
    ),
    anyhow::Error,
> {
    let config = c.aws.load(aws_external_id).await;
    let kinesis_client = kinesis::client(&config);

    let shard_set = kinesis::get_shard_ids(&kinesis_client, &c.stream_name).await?;
    let mut shard_queue: VecDeque<(String, Option<String>)> = VecDeque::new();
    let mut shard_map = HashMap::new();
    for shard_id in shard_set {
        shard_queue.push_back((
            shard_id.clone(),
            kinesis::get_shard_iterator(&kinesis_client, &c.stream_name, &shard_id).await?,
        ));
        shard_map.insert(
            shard_id.clone(),
            ShardMetrics::new(base_metrics, &c.stream_name, &shard_id),
        );
    }

    Ok((
        kinesis_client,
        c.stream_name.clone(),
        shard_map,
        shard_queue,
    ))
}
