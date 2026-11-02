// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Serving the Kafka protocol on a single client connection.
//!
//! Kafka clients pipeline requests and match responses to requests by order, so responses must
//! be written in the order the requests arrived. Requests are decoded and dispatched in order
//! on the connection task, which also enqueues produced batches into topic channels, so batches
//! from one connection reach a topic in request order. Each request yields a future for its
//! response. The futures complete out of order (a produce response waits for its batch to become
//! durable) and are written back in order by a writer that runs alongside the reader, so that a
//! request that takes long to dispatch, such as a batch being validated, does not hold back the
//! responses to earlier requests.

use std::io::Read;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use bytes::{Bytes, BytesMut};
use futures::channel::mpsc;
use futures::future::{BoxFuture, FutureExt};
use futures::{SinkExt, StreamExt};
use kafka_protocol::compression::{Decompressor, Gzip, Snappy};
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{
    BatchIndexAndErrorMessage, PartitionProduceResponse, TopicProduceResponse,
};
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, ApiVersionsRequest,
    ApiVersionsResponse, BrokerId, CreateTopicsRequest, CreateTopicsResponse, EndTxnRequest,
    EndTxnResponse, FetchRequest, FetchResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse,
    MetadataRequest, MetadataResponse, ProduceRequest, ProduceResponse, ProducerId, RequestHeader,
    ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse, TopicName,
};
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, StrBytes, decode_request_header_from_buffer,
};
use kafka_protocol::records::{
    Compression, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record, RecordBatchDecoder, RecordBatchEncoder,
    RecordEncodeOptions, TimestampType,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::oneshot;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::debug;

use crate::{
    KafkaServer, NODE_ID, ProduceAck, ProduceBatch, ProducedRecord, ProducerBatchId, Rejection,
    SaslPlainCredentials,
};

/// The largest request the server accepts, matching Kafka's default `socket.request.max.bytes`.
const MAX_FRAME_LENGTH: usize = 100 * 1024 * 1024;

/// The number of requests a connection may have awaiting a response before the server stops
/// reading from it.
const MAX_IN_FLIGHT_REQUESTS: usize = 128;

/// Kafka's sentinel for "authorized operations were not requested".
const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

/// The longest a `Fetch` that finds no records waits before it responds.
const MAX_FETCH_WAIT: Duration = Duration::from_secs(1);

/// The APIs and version ranges the server supports.
///
/// The ranges are capped below what `kafka-protocol` can decode where higher versions change
/// semantics the server does not implement: `Produce` v13 and `Fetch` v13 address topics by ID,
/// `EndTxn` v5 expects the server to bump the producer epoch with every transaction,
/// `AddPartitionsToTxn` v4 is for brokers only, and `SaslHandshake` v0 sends the SASL exchange
/// outside of `SaslAuthenticate` requests.
///
/// NOTE: librdkafka only produces record batch format v2, which carries headers and producer
/// IDs, to brokers that advertise both `Produce` v3 and `Fetch` v4. Without it, librdkafka falls
/// back to message format v0, which the server cannot decode.
const SUPPORTED_APIS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 11),
    (ApiKey::Fetch, 4, 12),
    (ApiKey::ListOffsets, 1, 7),
    (ApiKey::Metadata, 0, 12),
    (ApiKey::FindCoordinator, 0, 4),
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::CreateTopics, 2, 7),
    (ApiKey::InitProducerId, 0, 4),
    (ApiKey::AddPartitionsToTxn, 0, 3),
    (ApiKey::EndTxn, 0, 4),
    (ApiKey::SaslHandshake, 1, 1),
    (ApiKey::SaslAuthenticate, 0, 2),
];

fn supported_versions(api_key: ApiKey) -> Option<(i16, i16)> {
    SUPPORTED_APIS
        .iter()
        .find(|(key, _, _)| *key == api_key)
        .map(|(_, min, max)| (*min, *max))
}

type PendingResponse = BoxFuture<'static, Result<Option<Bytes>, anyhow::Error>>;

#[derive(Default)]
struct ConnectionState {
    /// Whether the client completed a `SaslHandshake` for the `PLAIN` mechanism.
    sasl_handshake_done: bool,
    /// The credentials the client authenticated with, if any.
    credentials: Option<SaslPlainCredentials>,
}

pub(crate) async fn serve<S>(server: &KafkaServer, stream: S) -> Result<(), anyhow::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let codec = LengthDelimitedCodec::builder()
        .length_field_length(4)
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec();
    let (mut sink, mut frames) = Framed::new(stream, codec).split();
    let mut state = ConnectionState::default();
    // The channel and the writer's buffer together bound the requests awaiting a response.
    let (tx, rx) = mpsc::channel::<PendingResponse>(MAX_IN_FLIGHT_REQUESTS / 2);

    let read = async move {
        let mut tx = tx;
        while let Some(frame) = frames.next().await {
            let frame = frame.context("reading request")?;
            let response = handle_request(server, &mut state, frame.freeze()).await?;
            if tx.send(response).await.is_err() {
                // The writer failed, and reports why.
                break;
            }
        }
        // Dropping `tx` lets the writer finish the responses to the requests read so far.
        Ok::<_, anyhow::Error>(())
    };
    let write = async move {
        let mut responses = rx.buffered(MAX_IN_FLIGHT_REQUESTS / 2);
        while let Some(response) = responses.next().await {
            if let Some(response) = response? {
                sink.send(response).await.context("writing response")?;
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    futures::try_join!(read, write)?;
    Ok(())
}

async fn handle_request(
    server: &KafkaServer,
    state: &mut ConnectionState,
    mut frame: Bytes,
) -> Result<PendingResponse, anyhow::Error> {
    let header = decode_request_header_from_buffer(&mut frame)?;
    let api_key = ApiKey::try_from(header.request_api_key)
        .map_err(|()| anyhow!("unknown API key {}", header.request_api_key))?;
    let version = header.request_api_version;
    debug!(
        "Kafka request {api_key:?} v{version} correlation_id={} client_id={:?}",
        header.correlation_id, header.client_id
    );

    let supported =
        supported_versions(api_key).is_some_and(|(min, max)| (min..=max).contains(&version));
    if !supported {
        if api_key == ApiKey::ApiVersions {
            // A client that sends an `ApiVersions` request newer than the server supports
            // expects a v0 response that lists the supported versions, and then retries.
            let mut response = api_versions_response();
            response.error_code = ResponseError::UnsupportedVersion.code();
            return ready(encode_response(&header, 0, &response));
        }
        bail!("unsupported request {api_key:?} v{version}");
    }

    match api_key {
        ApiKey::ApiVersions => {
            ApiVersionsRequest::decode(&mut frame, version)?;
            ready(encode_response(&header, version, &api_versions_response()))
        }
        ApiKey::Metadata => {
            let request = MetadataRequest::decode(&mut frame, version)?;
            let response = metadata(server, state, &request, version);
            ready(encode_response(&header, version, &response))
        }
        ApiKey::InitProducerId => {
            let request = InitProducerIdRequest::decode(&mut frame, version)?;
            let mut response = InitProducerIdResponse::default();
            let result = match &request.transactional_id {
                Some(transactional_id) => {
                    let producer = (request.producer_id.0 >= 0)
                        .then_some((request.producer_id.0, request.producer_epoch));
                    server.init_transactional_producer(transactional_id.as_str(), producer)
                }
                None => Ok((server.allocate_producer_id(), 0)),
            };
            match result {
                Ok((producer_id, epoch)) => {
                    response.producer_id = ProducerId(producer_id);
                    response.producer_epoch = epoch;
                }
                Err(e) => {
                    response.error_code = e.code();
                    response.producer_id = ProducerId(-1);
                    response.producer_epoch = -1;
                }
            }
            ready(encode_response(&header, version, &response))
        }
        ApiKey::FindCoordinator => {
            let request = FindCoordinatorRequest::decode(&mut frame, version)?;
            // The server coordinates every group and transaction itself.
            let config = server.config();
            let host = StrBytes::from_string(config.advertised_host.clone());
            let port = i32::from(config.advertised_port);
            let mut response = FindCoordinatorResponse::default();
            if version < 4 {
                response.node_id = BrokerId(NODE_ID);
                response.host = host;
                response.port = port;
            } else {
                response.coordinators = request
                    .coordinator_keys
                    .into_iter()
                    .map(|key| {
                        let mut coordinator = Coordinator::default();
                        coordinator.key = key;
                        coordinator.node_id = BrokerId(NODE_ID);
                        coordinator.host = host.clone();
                        coordinator.port = port;
                        coordinator
                    })
                    .collect();
            }
            ready(encode_response(&header, version, &response))
        }
        ApiKey::AddPartitionsToTxn => {
            let request = AddPartitionsToTxnRequest::decode(&mut frame, version)?;
            let response = add_partitions_to_txn(server, state, request);
            ready(encode_response(&header, version, &response))
        }
        ApiKey::EndTxn => {
            let request = EndTxnRequest::decode(&mut frame, version)?;
            let end = server.end_transaction(
                request.transactional_id.0.to_string(),
                request.producer_id.0,
                request.producer_epoch,
                request.committed,
                state.credentials.as_ref(),
            );
            Ok(async move {
                let mut response = EndTxnResponse::default();
                if let Err(e) = end.await {
                    response.error_code = e.code();
                }
                encode_response(&header, version, &response).map(Some)
            }
            .boxed())
        }
        ApiKey::CreateTopics => {
            let request = CreateTopicsRequest::decode(&mut frame, version)?;
            let response = create_topics(server, request);
            ready(encode_response(&header, version, &response))
        }
        ApiKey::ListOffsets => {
            let request = ListOffsetsRequest::decode(&mut frame, version)?;
            let response = list_offsets(server, request);
            ready(encode_response(&header, version, &response))
        }
        ApiKey::Fetch => {
            let request = FetchRequest::decode(&mut frame, version)?;
            fetch(server, state, header, version, request)
        }
        ApiKey::SaslHandshake => {
            let request = SaslHandshakeRequest::decode(&mut frame, version)?;
            let mut response = SaslHandshakeResponse::default();
            response.mechanisms = vec![StrBytes::from_static_str("PLAIN")];
            if request.mechanism.as_str() == "PLAIN" {
                state.sasl_handshake_done = true;
            } else {
                response.error_code = ResponseError::UnsupportedSaslMechanism.code();
            }
            ready(encode_response(&header, version, &response))
        }
        ApiKey::SaslAuthenticate => {
            let request = SaslAuthenticateRequest::decode(&mut frame, version)?;
            let response = sasl_authenticate(server, state, &request);
            ready(encode_response(&header, version, &response))
        }
        ApiKey::Produce => {
            let request = ProduceRequest::decode(&mut frame, version)?;
            produce(server, state, header, version, request).await
        }
        _ => unreachable!("checked against SUPPORTED_APIS"),
    }
}

fn ready(response: Result<Bytes, anyhow::Error>) -> Result<PendingResponse, anyhow::Error> {
    let response = response?;
    Ok(futures::future::ready(Ok(Some(response))).boxed())
}

fn encode_response<R>(
    request_header: &RequestHeader,
    version: i16,
    body: &R,
) -> Result<Bytes, anyhow::Error>
where
    R: Encodable + HeaderVersion,
{
    let mut buf = BytesMut::new();
    let header = ResponseHeader::default().with_correlation_id(request_header.correlation_id);
    header.encode(&mut buf, R::header_version(version))?;
    body.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

fn api_versions_response() -> ApiVersionsResponse {
    let mut response = ApiVersionsResponse::default();
    response.api_keys = SUPPORTED_APIS
        .iter()
        .map(|(key, min, max)| {
            let mut v = ApiVersion::default();
            // `ApiKey`'s discriminants are the protocol's API keys, and it offers no other
            // conversion.
            #[allow(clippy::as_conversions)]
            let api_key = *key as i16;
            v.api_key = api_key;
            v.min_version = *min;
            v.max_version = *max;
            v
        })
        .collect();
    response
}

fn metadata(
    server: &KafkaServer,
    state: &ConnectionState,
    request: &MetadataRequest,
    version: i16,
) -> MetadataResponse {
    let config = server.config();
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = BrokerId(NODE_ID);
    broker.host = StrBytes::from_string(config.advertised_host.clone());
    broker.port = i32::from(config.advertised_port);

    let topics = server.topics(state.credentials.as_ref());
    // In v0 an empty list requests all topics. From v1 on, that is expressed by a null list.
    let requested: Vec<String> = match &request.topics {
        Some(requested) if !(version == 0 && requested.is_empty()) => requested
            .iter()
            .filter_map(|t| t.name.as_ref().map(|n| n.0.to_string()))
            .collect(),
        _ => topics
            .iter()
            .filter(|(_, authorized)| **authorized)
            .map(|(name, _)| name.clone())
            .collect(),
    };

    let mut response = MetadataResponse::default();
    response.brokers = vec![broker];
    response.cluster_id = Some(StrBytes::from_string(config.cluster_id.clone()));
    response.controller_id = BrokerId(NODE_ID);
    response.cluster_authorized_operations = AUTHORIZED_OPERATIONS_OMITTED;
    response.topics = requested
        .into_iter()
        .map(|name| {
            let mut topic = MetadataResponseTopic::default();
            topic.name = Some(TopicName(StrBytes::from_string(name.clone())));
            topic.topic_authorized_operations = AUTHORIZED_OPERATIONS_OMITTED;
            match topics.get(&name) {
                None => topic.error_code = ResponseError::UnknownTopicOrPartition.code(),
                Some(false) => topic.error_code = ResponseError::TopicAuthorizationFailed.code(),
                Some(true) => {
                    let mut partition = MetadataResponsePartition::default();
                    partition.partition_index = 0;
                    partition.leader_id = BrokerId(NODE_ID);
                    partition.leader_epoch = 0;
                    partition.replica_nodes = vec![BrokerId(NODE_ID)];
                    partition.isr_nodes = vec![BrokerId(NODE_ID)];
                    topic.partitions = vec![partition];
                }
            }
            topic
        })
        .collect();
    response
}

fn sasl_authenticate(
    server: &KafkaServer,
    state: &mut ConnectionState,
    request: &SaslAuthenticateRequest,
) -> SaslAuthenticateResponse {
    let mut response = SaslAuthenticateResponse::default();
    if !state.sasl_handshake_done || state.credentials.is_some() {
        response.error_code = ResponseError::IllegalSaslState.code();
        return response;
    }
    // RFC 4616: `[authzid] NUL authcid NUL passwd`.
    let credentials = std::str::from_utf8(&request.auth_bytes)
        .ok()
        .and_then(|token| {
            let mut parts = token.split('\0');
            let _authzid = parts.next()?;
            let username = parts.next()?;
            let password = parts.next()?;
            parts.next().is_none().then(|| SaslPlainCredentials {
                username: username.into(),
                password: password.into(),
            })
        });
    match credentials {
        Some(credentials) if server.authenticate(&credentials) => {
            debug!("Kafka client authenticated as {}", credentials.username);
            state.credentials = Some(credentials);
        }
        _ => {
            response.error_code = ResponseError::SaslAuthenticationFailed.code();
            response.error_message = Some(StrBytes::from_static_str(
                "Authentication failed: invalid username or password",
            ));
        }
    }
    response
}

/// Hands the batches of `request` to their topics and returns a future for the response.
///
/// Enqueuing waits for capacity in each topic's channel, which applies backpressure to the
/// connection.
async fn produce(
    server: &KafkaServer,
    state: &ConnectionState,
    header: RequestHeader,
    version: i16,
    request: ProduceRequest,
) -> Result<PendingResponse, anyhow::Error> {
    let acks = request.acks;
    let timeout = Duration::from_millis(u64::try_from(request.timeout_ms).unwrap_or(0));
    let transactional_id = request.transactional_id.as_ref().map(|id| id.0.to_string());

    let mut topic_results = Vec::new();
    for topic_data in request.topic_data {
        let name = topic_data.name.0.to_string();
        let mut partition_results = Vec::new();
        for partition_data in topic_data.partition_data {
            let index = partition_data.index;
            let result = enqueue_partition(
                server,
                state,
                &name,
                index,
                partition_data.records,
                acks,
                transactional_id.as_deref(),
            )
            .await;
            partition_results.push((index, result));
        }
        topic_results.push((topic_data.name, partition_results));
    }

    Ok(async move {
        let mut response = ProduceResponse::default();
        for (name, partitions) in topic_results {
            let mut topic = TopicProduceResponse::default();
            topic.name = name;
            for (index, result) in partitions {
                let result = match result {
                    Ok(Some(rx)) => match tokio::time::timeout(timeout, rx).await {
                        Ok(Ok(result)) => result.map_err(Rejection::from),
                        // The source dropped the batch, for example because it restarted.
                        Ok(Err(_)) => Err(ResponseError::NotLeaderOrFollower.into()),
                        Err(_) => Err(ResponseError::RequestTimedOut.into()),
                    },
                    Ok(None) => Ok(-1),
                    Err(e) => Err(e),
                };
                let mut partition = PartitionProduceResponse::default();
                partition.index = index;
                partition.log_append_time_ms = -1;
                partition.log_start_offset = -1;
                match result {
                    Ok(base_offset) => partition.base_offset = base_offset,
                    Err(rejection) => {
                        partition.error_code = rejection.error.code();
                        partition.base_offset = -1;
                        partition.error_message = rejection.message.map(StrBytes::from_string);
                        partition.record_errors = rejection
                            .record_errors
                            .into_iter()
                            .map(|(index, message)| {
                                let mut error = BatchIndexAndErrorMessage::default();
                                error.batch_index = i32::try_from(index).unwrap_or(i32::MAX);
                                error.batch_index_error_message =
                                    Some(StrBytes::from_string(message));
                                error
                            })
                            .collect();
                    }
                }
                topic.partition_responses.push(partition);
            }
            response.responses.push(topic);
        }
        if acks == 0 {
            return Ok(None);
        }
        encode_response(&header, version, &response).map(Some)
    }
    .boxed())
}

type AckReceiver = oneshot::Receiver<Result<i64, ResponseError>>;

/// Decodes and validates the records for one partition and hands them to the topic, or to the
/// producer's transaction.
///
/// Returns the receiver for the batch's acknowledgement, or `None` if there is none to wait
/// for: the producer does not want one, or the batch joined a transaction.
async fn enqueue_partition(
    server: &KafkaServer,
    state: &ConnectionState,
    topic: &str,
    partition: i32,
    records: Option<Bytes>,
    acks: i16,
    transactional_id: Option<&str>,
) -> Result<Option<AckReceiver>, Rejection> {
    if partition != 0 {
        return Err(ResponseError::UnknownTopicOrPartition.into());
    }
    let (records, producer, transactional) = decode_records(records.unwrap_or_default())?;
    if records.is_empty() {
        return Err(ResponseError::InvalidRecord.into());
    }
    // Summed here, on the connection's task, so that the consumer of the batch need not walk
    // the records again.
    let bytes = records.iter().map(ProducedRecord::size).sum();
    match (transactional_id, transactional, &producer) {
        (Some(transactional_id), true, Some(id)) => {
            // Topics joined the transaction in `AddPartitionsToTxn`, which checked access.
            if let Some(validator) = server.validator(topic) {
                validator.validate(&records).await?;
            }
            server.produce_transactional(transactional_id, topic, records, bytes, id)?;
            return Ok(None);
        }
        (None, false, _) => {}
        _ => return Err(ResponseError::InvalidRecord.into()),
    }
    let (sender, validator) = server.topic_sender(topic, state.credentials.as_ref())?;
    // Requests of a connection are handled one at a time, so validating here keeps the
    // connection's batches in request order.
    if let Some(validator) = validator {
        validator.validate(&records).await?;
    }
    let (tx, rx) = if acks == 0 {
        (None, None)
    } else {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    };
    sender
        .send(ProduceBatch {
            records,
            bytes,
            producer,
            compacted: Vec::new(),
            ack: ProduceAck { tx },
        })
        .await?;
    Ok(rx)
}

/// Decodes the record batches of one partition into records, the batch's idempotence fields
/// if the producer is idempotent, and whether the batches are transactional.
fn decode_records(
    mut buf: Bytes,
) -> Result<(Vec<ProducedRecord>, Option<ProducerBatchId>, bool), ResponseError> {
    let batches = RecordBatchDecoder::decode_batch_info(&mut buf.clone()).map_err(|e| {
        debug!("invalid record batch: {e:#}");
        ResponseError::CorruptMessage
    })?;
    for batch in &batches {
        if !matches!(
            batch.compression,
            Compression::None | Compression::Gzip | Compression::Snappy | Compression::Lz4
        ) {
            return Err(ResponseError::UnsupportedCompressionType);
        }
    }
    let transactional = batches.first().is_some_and(|b| b.transactional);
    if batches.iter().any(|b| b.transactional != transactional) {
        return Err(ResponseError::InvalidRecord);
    }
    let mut record_sets = Vec::with_capacity(batches.len());
    while !buf.is_empty() {
        let set = RecordBatchDecoder::decode_with_custom_compression(&mut buf, Some(decompress))
            .map_err(|e| {
                debug!("invalid record batch: {e:#}");
                ResponseError::CorruptMessage
            })?;
        record_sets.push(set);
    }
    let records: Vec<_> = record_sets
        .into_iter()
        .flat_map(|set| set.records)
        .filter(|record| !record.control)
        .collect();

    // An idempotent producer sends exactly one batch per partition and request, which Kafka
    // brokers enforce. A batch's records then carry consecutive sequence numbers.
    let producer = match records.first() {
        Some(first) if first.producer_id >= 0 => {
            let consistent = batches.len() == 1
                && records.iter().all(|r| {
                    r.producer_id == first.producer_id && r.producer_epoch == first.producer_epoch
                });
            if !consistent {
                return Err(ResponseError::InvalidRecord);
            }
            Some(ProducerBatchId {
                producer_id: first.producer_id,
                epoch: first.producer_epoch,
                first_sequence: first.sequence,
            })
        }
        _ => None,
    };

    let now = i64::try_from((mz_ore::now::SYSTEM_TIME.clone())()).unwrap_or(i64::MAX);
    let records = records
        .into_iter()
        .map(|record| ProducedRecord {
            key: record.key,
            value: record.value,
            headers: record
                .headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            timestamp: match record.timestamp_type {
                TimestampType::Creation => record.timestamp,
                TimestampType::LogAppend => now,
            },
        })
        .collect();
    Ok((records, producer, transactional))
}

/// Decompresses the records of a record batch. lz4 is decoded with `lz4_flex`, which is pure
/// Rust, where `kafka-protocol` would link the C library. Materialize Kafka sinks compress with
/// lz4 by default.
fn decompress(buf: &mut Bytes, compression: Compression) -> anyhow::Result<Bytes> {
    let take = |b: &mut Bytes| Ok(std::mem::take(b));
    match compression {
        Compression::None => take(buf),
        Compression::Gzip => Gzip::decompress(buf, take),
        Compression::Snappy => Snappy::decompress(buf, take),
        Compression::Lz4 => {
            let mut decompressed = Vec::new();
            lz4_flex::frame::FrameDecoder::new(&buf[..]).read_to_end(&mut decompressed)?;
            Ok(decompressed.into())
        }
        Compression::Zstd => bail!("unsupported compression {compression:?}"),
    }
}

fn add_partitions_to_txn(
    server: &KafkaServer,
    state: &ConnectionState,
    request: AddPartitionsToTxnRequest,
) -> AddPartitionsToTxnResponse {
    let topics: Vec<String> = request
        .v3_and_below_topics
        .iter()
        .map(|t| t.name.0.to_string())
        .collect();
    let results = server.add_to_transaction(
        request.v3_and_below_transactional_id.as_str(),
        request.v3_and_below_producer_id.0,
        request.v3_and_below_producer_epoch,
        &topics,
        state.credentials.as_ref(),
    );
    let mut response = AddPartitionsToTxnResponse::default();
    for (i, topic) in request.v3_and_below_topics.into_iter().enumerate() {
        let topic_result = match &results {
            Ok(results) => results[i],
            Err(e) => Err(*e),
        };
        let mut result = AddPartitionsToTxnTopicResult::default();
        result.name = topic.name;
        result.results_by_partition = topic
            .partitions
            .into_iter()
            .map(|partition_index| {
                let mut partition = AddPartitionsToTxnPartitionResult::default();
                partition.partition_index = partition_index;
                let error = match topic_result {
                    Ok(()) if partition_index != 0 => Some(ResponseError::UnknownTopicOrPartition),
                    Ok(()) => None,
                    Err(e) => Some(e),
                };
                partition.partition_error_code = error.map_or(0, |e| e.code());
                partition
            })
            .collect();
        response.results_by_topic_v3_and_below.push(result);
    }
    response
}

fn create_topics(server: &KafkaServer, request: CreateTopicsRequest) -> CreateTopicsResponse {
    let mut response = CreateTopicsResponse::default();
    for topic in request.topics {
        let name = topic.name.0.to_string();
        // -1 asks for the broker's default.
        let result = if !matches!(topic.num_partitions, 1 | -1) {
            Err(ResponseError::InvalidPartitions)
        } else if !matches!(topic.replication_factor, 1 | -1) {
            Err(ResponseError::InvalidReplicationFactor)
        } else if request.validate_only {
            Ok(())
        } else {
            server.create_compacted_topic(&name)
        };
        let mut result_topic = CreatableTopicResult::default();
        result_topic.name = topic.name;
        result_topic.num_partitions = 1;
        result_topic.replication_factor = 1;
        if let Err(e) = result {
            result_topic.error_code = e.code();
        }
        response.topics.push(result_topic);
    }
    response
}

/// Kafka's `ListOffsets` timestamp that asks for the earliest offset.
const EARLIEST_TIMESTAMP: i64 = -2;
/// Kafka's `ListOffsets` timestamp that asks for the latest offset.
const LATEST_TIMESTAMP: i64 = -1;

fn list_offsets(server: &KafkaServer, request: ListOffsetsRequest) -> ListOffsetsResponse {
    let mut response = ListOffsetsResponse::default();
    for topic in request.topics {
        let high_watermark = server.compacted_high_watermark(topic.name.as_str());
        let mut topic_response = ListOffsetsTopicResponse::default();
        topic_response.name = topic.name;
        for partition in topic.partitions {
            let mut partition_response = ListOffsetsPartitionResponse::default();
            partition_response.partition_index = partition.partition_index;
            partition_response.timestamp = -1;
            let offset = match high_watermark {
                Some(high_watermark) if partition.partition_index == 0 => {
                    match partition.timestamp {
                        // Compacted topics keep no records below the first one.
                        EARLIEST_TIMESTAMP => Ok(0),
                        LATEST_TIMESTAMP => Ok(high_watermark),
                        _ => Err(ResponseError::InvalidRequest),
                    }
                }
                _ => Err(ResponseError::UnknownTopicOrPartition),
            };
            match offset {
                Ok(offset) => partition_response.offset = offset,
                Err(e) => {
                    partition_response.error_code = e.code();
                    partition_response.offset = -1;
                }
            }
            topic_response.partitions.push(partition_response);
        }
        response.topics.push(topic_response);
    }
    response
}

/// Serves compacted topics. Every partition is fetched in full on every request, so fetch
/// sessions are not needed.
fn fetch(
    server: &KafkaServer,
    state: &ConnectionState,
    header: RequestHeader,
    version: i16,
    request: FetchRequest,
) -> Result<PendingResponse, anyhow::Error> {
    let mut response = FetchResponse::default();
    let mut found_records = false;
    for topic in request.topics {
        let mut topic_response = FetchableTopicResponse::default();
        for partition in topic.partitions {
            let mut data = PartitionData::default();
            data.partition_index = partition.partition;
            data.preferred_read_replica = BrokerId(-1);
            let result = if partition.partition == 0 {
                server.compacted_records(
                    topic.topic.as_str(),
                    partition.fetch_offset,
                    state.credentials.as_ref(),
                )
            } else {
                Err(ResponseError::UnknownTopicOrPartition)
            };
            match result {
                Ok((_, high_watermark)) if partition.fetch_offset > high_watermark => {
                    data.error_code = ResponseError::OffsetOutOfRange.code();
                    data.high_watermark = high_watermark;
                }
                Ok((records, high_watermark)) => {
                    data.high_watermark = high_watermark;
                    // Only committed records are served.
                    data.last_stable_offset = high_watermark;
                    data.log_start_offset = 0;
                    data.aborted_transactions = Some(Vec::new());
                    if !records.is_empty() {
                        found_records = true;
                        data.records = Some(encode_compacted(records)?);
                    }
                }
                Err(e) => data.error_code = e.code(),
            }
            topic_response.partitions.push(data);
        }
        topic_response.topic = topic.topic;
        response.responses.push(topic_response);
    }

    // A consumer fetches again as soon as it gets a response, so an empty one is delayed as
    // the consumer asks, instead of being answered in a busy loop.
    let wait = if found_records {
        Duration::ZERO
    } else {
        Duration::from_millis(u64::try_from(request.max_wait_ms).unwrap_or(0)).min(MAX_FETCH_WAIT)
    };
    Ok(async move {
        tokio::time::sleep(wait).await;
        encode_response(&header, version, &response).map(Some)
    }
    .boxed())
}

fn encode_compacted(records: Vec<(i64, Bytes, Option<Bytes>)>) -> Result<Bytes, anyhow::Error> {
    let records: Vec<Record> = records
        .into_iter()
        .map(|(offset, key, value)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: -1,
            timestamp: -1,
            key: Some(key),
            value,
            headers: Default::default(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        &records,
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )?;
    Ok(buf.freeze())
}
