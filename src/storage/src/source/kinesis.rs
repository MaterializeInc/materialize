// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use aws_sdk_kinesis::error::GetRecordsError;
use aws_sdk_kinesis::output::GetRecordsOutput;
use aws_sdk_kinesis::types::SdkError;
use aws_sdk_kinesis::Client as KinesisClient;
use prometheus::core::AtomicI64;
use timely::dataflow::operators::Capability;
use timely::progress::Antichain;
use timely::scheduling::SyncActivator;
use tokio::runtime::Handle as TokioHandle;
use tracing::error;

use mz_cloud_resources::AwsExternalIdPrefix;
use mz_ore::metrics::{DeleteOnDropGauge, GaugeVecExt};
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use mz_storage_client::types::connections::ConnectionContext;
use mz_storage_client::types::errors::SourceErrorDetails;
use mz_storage_client::types::sources::encoding::SourceDataEncoding;
use mz_storage_client::types::sources::{KinesisSourceConnection, MzOffset};

use crate::source::commit::LogCommitter;
use crate::source::metrics::KinesisMetrics;
use crate::source::types::SourceConnectionBuilder;
use crate::source::{
    NextMessage, SourceMessage, SourceMessageType, SourceReader, SourceReaderError,
};

/// To read all data from a Kinesis stream, we need to continually update
/// our knowledge of the stream's shards by calling the ListShards API.
///
/// We will call ListShards at most this often to stay under the API rate limit
/// (100x/sec per stream) and to improve source performance overall.
const KINESIS_SHARD_REFRESH_RATE: Duration = Duration::from_secs(60);

/// Contains all information necessary to ingest data from Kinesis
pub struct KinesisSourceReader {
    tokio_handle: TokioHandle,
    /// Kinesis client used to obtain records
    kinesis_client: KinesisClient,
    /// The name of the stream
    stream_name: String,
    /// The set of active shards
    shard_set: BTreeMap<String, ShardMetrics>,
    /// A queue representing the next shard to read from. This is necessary
    /// to ensure that all shards are read from uniformly
    shard_queue: VecDeque<(String, Option<String>)>,
    /// The time at which we last refreshed metadata
    /// TODO(natacha): this should be moved to timestamper
    last_checked_shards: Instant,
    /// Storage for messages that have not yet been timestamped
    buffered_messages: VecDeque<(SourceMessage<(), Option<Vec<u8>>>, MzOffset)>,
    /// Count of processed message
    processed_message_count: u64,
    /// Metrics from which per-shard metrics get created.
    base_metrics: KinesisMetrics,
    // Kinesis sources support single-threaded ingestion only, so only one of
    // the `KinesisSourceReader`s will actually produce data.
    active_read_worker: bool,
    /// Capabilities used to produce messages
    data_capability: Capability<MzOffset>,
    upper_capability: Capability<MzOffset>,
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
        let current_shards: BTreeSet<_> =
            mz_kinesis_util::get_shard_ids(&self.kinesis_client, &self.stream_name)
                .await?
                .collect();
        let known_shards: BTreeSet<_> = self.shard_set.keys().cloned().collect();
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
                mz_kinesis_util::get_shard_iterator(
                    &self.kinesis_client,
                    &self.stream_name,
                    &shard_id,
                )
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

impl SourceConnectionBuilder for KinesisSourceConnection {
    type Reader = KinesisSourceReader;
    type OffsetCommitter = LogCommitter;

    fn into_reader(
        self,
        _source_name: String,
        source_id: GlobalId,
        worker_id: usize,
        worker_count: usize,
        _consumer_activator: SyncActivator,
        data_capability: Capability<<Self::Reader as SourceReader>::Time>,
        upper_capability: Capability<<Self::Reader as SourceReader>::Time>,
        _resume_upper: Antichain<<Self::Reader as SourceReader>::Time>,
        _encoding: SourceDataEncoding,
        metrics: crate::source::metrics::SourceBaseMetrics,
        connection_context: ConnectionContext,
    ) -> Result<(Self::Reader, Self::OffsetCommitter), anyhow::Error> {
        let active_read_worker =
            crate::source::responsible_for(&source_id, worker_id, worker_count, ());

        // TODO: This creates all the machinery, even for the non-active workers.
        // We could change that to only spin up Kinesis when needed.
        let state = TokioHandle::current().block_on(create_state(
            &metrics.kinesis,
            self,
            connection_context.aws_external_id_prefix.as_ref(),
            source_id,
            &*connection_context.secrets_reader,
        ));
        match state {
            Ok((kinesis_client, stream_name, shard_set, shard_queue)) => Ok((
                KinesisSourceReader {
                    tokio_handle: TokioHandle::current(),
                    kinesis_client,
                    shard_queue,
                    last_checked_shards: Instant::now(),
                    buffered_messages: VecDeque::new(),
                    shard_set,
                    stream_name,
                    processed_message_count: 0,
                    base_metrics: metrics.kinesis,
                    active_read_worker,
                    data_capability,
                    upper_capability,
                },
                LogCommitter {
                    source_id,
                    worker_id,
                    worker_count,
                },
            )),
            Err(e) => Err(anyhow!("{}", e)),
        }
    }
}

impl SourceReader for KinesisSourceReader {
    type Key = ();
    type Value = Option<Vec<u8>>;
    type Time = MzOffset;
    type Diff = u32;

    fn get_next_message(&mut self) -> NextMessage<Self::Key, Self::Value, Self::Time, Self::Diff> {
        if !self.active_read_worker {
            return NextMessage::Finished;
        }

        assert_eq!(self.shard_queue.len(), self.shard_set.len());

        //TODO move to timestamper
        if self.last_checked_shards.elapsed() >= KINESIS_SHARD_REFRESH_RATE {
            if let Err(e) = self
                .tokio_handle
                .clone()
                .block_on(self.update_shard_information())
            {
                error!("{:#?}", e);
                // XXX(petrosagg): We are fabricating a timestamp here. Is the error truly
                // definite?
                let ts = MzOffset::from(self.processed_message_count);
                let cap = self.data_capability.delayed(&ts);
                self.upper_capability.downgrade(&(ts + 1));
                let msg = Err(SourceReaderError::other_definite(e));
                return NextMessage::Ready(SourceMessageType::Message(msg, cap, 1));
            }
            self.last_checked_shards = std::time::Instant::now();
        }

        if let Some((message, ts)) = self.buffered_messages.pop_front() {
            let cap = self.data_capability.delayed(&ts);
            let next_ts = ts + 1;
            self.data_capability.downgrade(&next_ts);
            self.upper_capability.downgrade(&next_ts);
            NextMessage::Ready(SourceMessageType::Message(Ok(message), cap, 1))
        } else {
            // Rotate through all of a stream's shards, start with a new shard on each activation.
            if let Some((shard_id, mut shard_iterator)) = self.shard_queue.pop_front() {
                if let Some(iterator) = &shard_iterator {
                    let get_records_output =
                        match self.tokio_handle.block_on(self.get_records(iterator)) {
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
                            Err(e @ SdkError::DispatchFailure(_)) => {
                                // todo@jldlaughlin: Parse this to determine fatal/retriable?
                                error!("{}", e);
                                self.shard_queue.push_back((shard_id, shard_iterator));
                                // Do not send error message as this would cause source to terminate
                                return NextMessage::TransientDelay;
                            }
                            Err(SdkError::ServiceError(err))
                                if err.err().is_expired_iterator_exception() =>
                            {
                                // todo@jldlaughlin: Will need track source offsets to grab a new iterator.
                                error!("{}", err.err());
                                // XXX(petrosagg): We are fabricating a timestamp here. Is the
                                // error truly definite?
                                let ts = MzOffset::from(self.processed_message_count);
                                let cap = self.data_capability.delayed(&ts);
                                self.upper_capability.downgrade(&(ts + 1));
                                let msg = Err(SourceReaderError {
                                    inner: SourceErrorDetails::Other(err.err().to_string()),
                                });
                                return NextMessage::Ready(SourceMessageType::Message(msg, cap, 1));
                            }
                            Err(SdkError::ServiceError(err))
                                if err.err().is_provisioned_throughput_exceeded_exception() =>
                            {
                                self.shard_queue.push_back((shard_id, shard_iterator));
                                // Do not send error message as this would cause source to terminate
                                return NextMessage::Pending;
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
                                // XXX(petrosagg): We are fabricating a timestamp here. Is the
                                // error truly definite?
                                let ts = MzOffset::from(self.processed_message_count);
                                let cap = self.data_capability.delayed(&ts);
                                self.upper_capability.downgrade(&(ts + 1));
                                let msg = Err(SourceReaderError {
                                    inner: SourceErrorDetails::Other(e.to_string()),
                                });
                                return NextMessage::Ready(SourceMessageType::Message(msg, cap, 1));
                            }
                        };

                    for record in get_records_output.records.unwrap_or_default() {
                        let data = record
                            .data
                            .map(|blob| blob.into_inner())
                            .unwrap_or_else(Vec::new);
                        self.processed_message_count += 1;

                        //TODO: should MzOffset be modified to be a string?
                        let ts = MzOffset::from(self.processed_message_count);
                        let source_message = SourceMessage {
                            output: 0,
                            upstream_time_millis: None,
                            key: (),
                            value: Some(data),
                            headers: None,
                        };
                        self.buffered_messages.push_back((source_message, ts));
                    }
                    self.shard_queue.push_back((shard_id, shard_iterator));
                }
            }
            match self.buffered_messages.pop_front() {
                Some((msg, ts)) => {
                    let cap = self.data_capability.delayed(&ts);
                    let next_ts = ts + 1;
                    self.data_capability.downgrade(&next_ts);
                    self.upper_capability.downgrade(&next_ts);
                    NextMessage::Ready(SourceMessageType::Message(Ok(msg), cap, 1))
                }
                None => NextMessage::Pending,
            }
        }
    }
}

/// Creates the necessary data-structures for shard management
// todo: Better error handling here! Not all errors mean we're done/can't progress.
async fn create_state(
    base_metrics: &KinesisMetrics,
    c: KinesisSourceConnection,
    aws_external_id_prefix: Option<&AwsExternalIdPrefix>,
    source_id: GlobalId,
    secrets_reader: &dyn SecretsReader,
) -> Result<
    (
        KinesisClient,
        String,
        BTreeMap<String, ShardMetrics>,
        VecDeque<(String, Option<String>)>,
    ),
    anyhow::Error,
> {
    let config = c
        .aws
        .load(aws_external_id_prefix, Some(&source_id), secrets_reader)
        .await;

    let kinesis_client = aws_sdk_kinesis::Client::new(&config);

    let shard_set = mz_kinesis_util::get_shard_ids(&kinesis_client, &c.stream_name).await?;
    let mut shard_queue: VecDeque<(String, Option<String>)> = VecDeque::new();
    let mut shard_map = BTreeMap::new();
    for shard_id in shard_set {
        shard_queue.push_back((
            shard_id.clone(),
            mz_kinesis_util::get_shard_iterator(&kinesis_client, &c.stream_name, &shard_id).await?,
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
