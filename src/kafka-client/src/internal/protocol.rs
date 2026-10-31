// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Kafka wire protocol: the API versions this client speaks, version
//! negotiation against a broker's ApiVersions answer, request and response
//! framing, and broker error codes.
//!
//! Message structs come from `kafka-protocol`, which is generated from Apache
//! Kafka's message schemas. Record batches are encoded and decoded by
//! [`records`](crate::internal::records), not here.
//!
//! The version ranges in [`SUPPORTED`] deliberately stop below
//! versions that identify topics by ID instead of name (Fetch 13,
//! OffsetCommit/OffsetFetch 10, Produce 13), below the batched coordinator and
//! offset-fetch formats (FindCoordinator 4, OffsetFetch 8), and below the
//! transaction protocol version 2 requests (Produce 12, AddPartitionsToTxn 4,
//! AddOffsetsToTxn 4, TxnOffsetCommit 5, EndTxn 5, InitProducerId 5), whose
//! semantics this client does not implement.
//!
//! NOTE: kafka-protocol 0.18 preallocates arrays (`Vec::with_capacity`) and
//! byte fields (`vec![0; n]`) from the counts and lengths it reads, before
//! checking them against the input. A response of a few bytes from a broker,
//! or anything answering at a broker address (ApiVersions is decoded before
//! authentication), can declare a count of `i32::MAX` and so request an
//! allocation of hundreds of gigabytes; if the allocator refuses it, the
//! process aborts. Frames are bounded by `receive.message.max.bytes`, but the
//! declared count, not the frame size, drives the allocation, and there is no
//! hook to validate counts short of re-implementing the decoders. The fix
//! belongs in kafka-protocol (cap preallocation by the remaining input); until
//! it is released this is a residual risk of connecting to untrusted
//! endpoints.

use std::collections::VecDeque;
use std::io::IoSlice;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, ProduceRequest, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, Request, StrBytes, VersionRange};

use crate::types::RDKafkaErrorCode;

/// The APIs this client uses: key, wire value of the key (kafka-protocol
/// offers no conversion from [`ApiKey`]), and the supported version range.
const SUPPORTED: &[(ApiKey, i16, i16, i16)] = &[
    (ApiKey::Produce, 0, 3, 11),
    (ApiKey::Fetch, 1, 4, 12),
    (ApiKey::ListOffsets, 2, 1, 9),
    (ApiKey::Metadata, 3, 0, 12),
    (ApiKey::OffsetCommit, 8, 2, 9),
    (ApiKey::OffsetFetch, 9, 1, 7),
    (ApiKey::FindCoordinator, 10, 0, 3),
    (ApiKey::JoinGroup, 11, 0, 9),
    (ApiKey::Heartbeat, 12, 0, 4),
    (ApiKey::LeaveGroup, 13, 0, 5),
    (ApiKey::SyncGroup, 14, 0, 5),
    (ApiKey::SaslHandshake, 17, 0, 1),
    (ApiKey::ApiVersions, 18, 0, 3),
    (ApiKey::CreateTopics, 19, 2, 7),
    (ApiKey::DeleteTopics, 20, 1, 5),
    (ApiKey::DeleteRecords, 21, 0, 2),
    (ApiKey::InitProducerId, 22, 0, 4),
    (ApiKey::AddPartitionsToTxn, 24, 0, 3),
    (ApiKey::AddOffsetsToTxn, 25, 0, 3),
    (ApiKey::EndTxn, 26, 0, 4),
    (ApiKey::TxnOffsetCommit, 28, 0, 4),
    (ApiKey::DescribeConfigs, 32, 1, 4),
    (ApiKey::AlterConfigs, 33, 0, 2),
    (ApiKey::SaslAuthenticate, 36, 0, 2),
    (ApiKey::CreatePartitions, 37, 0, 3),
];

fn supported_index(key: ApiKey) -> Option<usize> {
    SUPPORTED.iter().position(|(k, ..)| *k == key)
}

/// librdkafka's name of an API (`rd_kafka_ApiKey2str`), as used in log lines
/// (`Timed out MetadataRequest in flight`) and statistics.
pub(crate) fn api_name(key: ApiKey) -> String {
    match key {
        ApiKey::ApiVersions => "ApiVersion".to_string(),
        key => format!("{key:?}"),
    }
}

/// The wire value of `key`, if this client uses the API.
pub(crate) fn api_key_value(key: ApiKey) -> Option<i16> {
    supported_index(key).map(|i| SUPPORTED[i].1)
}

/// The ApiVersions version sent first. v3 carries the client software name
/// and version. A broker that does not support it answers with a v0 response
/// carrying `UNSUPPORTED_VERSION`, after which the client retries with v0.
pub(crate) const API_VERSIONS_FIRST_VERSION: i16 = 3;

/// The software name sent in ApiVersions v3, as librdkafka does.
pub(crate) const CLIENT_SOFTWARE_NAME: &str = "librdkafka";

/// The software version sent in ApiVersions v3: the librdkafka version whose
/// behavior this client follows.
pub(crate) const CLIENT_SOFTWARE_VERSION: &str = "2.12.1";

/// The versions negotiated with one broker connection: for each API this
/// client uses, the highest version both sides support.
#[derive(Clone, Debug)]
pub(crate) struct ApiVersions {
    /// Indexed like `SUPPORTED`; `None` if the broker does not support any
    /// version this client supports, or does not support the API at all.
    versions: [Option<i16>; SUPPORTED.len()],
}

impl ApiVersions {
    /// Negotiates with the ranges a broker advertised in ApiVersions:
    /// `(api key, min version, max version)`.
    pub(crate) fn negotiate(broker_ranges: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        let mut versions = ApiVersions {
            versions: [None; SUPPORTED.len()],
        };
        for (key, min, max) in broker_ranges {
            let Some(idx) = SUPPORTED.iter().position(|(_, raw, ..)| *raw == key) else {
                continue;
            };
            let broker = VersionRange { min, max };
            let (_, _, our_min, our_max) = SUPPORTED[idx];
            let both = VersionRange {
                min: our_min,
                max: our_max,
            }
            .intersect(&broker);
            if !both.is_empty() {
                versions.versions[idx] = Some(both.max);
            }
        }
        versions
    }

    /// The negotiated version for `key`, or `None` if the broker supports no
    /// version this client supports.
    pub(crate) fn version(&self, key: ApiKey) -> Option<i16> {
        self.versions[supported_index(key)?]
    }
}

/// Size of the length prefix of every frame.
pub(crate) const FRAME_LEN_SIZE: usize = 4;

/// Encodes a complete request frame: length prefix, request header (v1 or v2
/// per the API and version), and body.
pub(crate) fn encode_request<R: Request>(
    request: &R,
    version: i16,
    correlation_id: i32,
    client_id: &str,
) -> Result<Bytes, String> {
    let api_key = ApiKey::try_from(R::KEY).map_err(|_| format!("unknown API key {}", R::KEY))?;
    let header_version = api_key.request_header_version(version);
    let header = RequestHeader::default()
        .with_request_api_key(R::KEY)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_string())));
    let size = header
        .compute_size(header_version)
        .and_then(|h| Ok(h + request.compute_size(version)?))
        .map_err(|e| format!("failed to encode {api_key:?} v{version} request: {e:#}"))?;
    let mut buf = BytesMut::with_capacity(FRAME_LEN_SIZE + size);
    let len = i32::try_from(size).map_err(|_| format!("{api_key:?} request too large"))?;
    buf.put_i32(len);
    header
        .encode(&mut buf, header_version)
        .and_then(|()| request.encode(&mut buf, version))
        .map_err(|e| format!("failed to encode {api_key:?} v{version} request: {e:#}"))?;
    Ok(buf.freeze())
}

/// A request frame as segments written back to back, consumed as a [`Buf`]
/// while it is written.
#[derive(Debug)]
pub(crate) struct Frame {
    /// Never contains empty segments, so that `chunk` is not empty while
    /// bytes remain.
    segments: VecDeque<Bytes>,
    remaining: usize,
}

impl Frame {
    fn push(&mut self, segment: Bytes) {
        if !segment.is_empty() {
            self.remaining += segment.len();
            self.segments.push_back(segment);
        }
    }
}

impl From<Bytes> for Frame {
    fn from(frame: Bytes) -> Frame {
        let mut f = Frame {
            segments: VecDeque::with_capacity(1),
            remaining: 0,
        };
        f.push(frame);
        f
    }
}

impl Buf for Frame {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.segments.front().map_or(&[], |s| s.as_ref())
    }

    fn advance(&mut self, mut cnt: usize) {
        assert!(cnt <= self.remaining, "advance past the end of the frame");
        self.remaining -= cnt;
        while cnt > 0 {
            let front = self.segments.front_mut().expect("checked remaining");
            if cnt < front.len() {
                front.advance(cnt);
                return;
            }
            cnt -= front.len();
            self.segments.pop_front();
        }
    }

    fn chunks_vectored<'a>(&'a self, dst: &mut [IoSlice<'a>]) -> usize {
        let n = dst.len().min(self.segments.len());
        for (i, segment) in self.segments.iter().take(n).enumerate() {
            dst[i] = IoSlice::new(segment);
        }
        n
    }
}

/// Like [`encode_request`] for a ProduceRequest, but each partition's record
/// batch stays in its own buffer, a segment of the frame, instead of being
/// copied into the frame.
pub(crate) fn encode_produce_request(
    request: &ProduceRequest,
    version: i16,
    correlation_id: i32,
    client_id: &str,
) -> Result<Frame, String> {
    let tagged = !request.unknown_tagged_fields.is_empty()
        || request.topic_data.iter().any(|t| {
            !t.unknown_tagged_fields.is_empty()
                || t.partition_data
                    .iter()
                    .any(|p| !p.unknown_tagged_fields.is_empty())
        });
    // The encoding below covers v3 (which added the transactional id) to
    // v12 (the last that names topics), without tagged fields.
    if !(3..=12).contains(&version) || tagged {
        return encode_request(request, version, correlation_id, client_id).map(Frame::from);
    }
    let flexible = version >= 9;
    let header_version = ApiKey::Produce.request_header_version(version);
    let header = RequestHeader::default()
        .with_request_api_key(ProduceRequest::KEY)
        .with_request_api_version(version)
        .with_correlation_id(correlation_id)
        .with_client_id(Some(StrBytes::from_string(client_id.to_string())));
    let size = header
        .compute_size(header_version)
        .and_then(|h| Ok(h + request.compute_size(version)?))
        .map_err(|e| format!("failed to encode Produce v{version} request: {e:#}"))?;
    let len = i32::try_from(size).map_err(|_| "Produce request too large".to_string())?;
    let partitions: usize = request
        .topic_data
        .iter()
        .map(|t| t.partition_data.len())
        .sum();
    let names: usize = request.topic_data.iter().map(|t| t.name.0.len()).sum();
    let mut buf = BytesMut::with_capacity(64 + client_id.len() + names + 16 * partitions);
    let mut frame = Frame {
        segments: VecDeque::with_capacity(2 * partitions + 1),
        remaining: 0,
    };
    buf.put_i32(len);
    header
        .encode(&mut buf, header_version)
        .map_err(|e| format!("failed to encode Produce v{version} request: {e:#}"))?;
    put_string(
        &mut buf,
        flexible,
        request.transactional_id.as_ref().map(|id| id.0.as_str()),
    )?;
    buf.put_i16(request.acks);
    buf.put_i32(request.timeout_ms);
    put_array_len(&mut buf, flexible, request.topic_data.len())?;
    for topic in &request.topic_data {
        put_string(&mut buf, flexible, Some(topic.name.0.as_str()))?;
        put_array_len(&mut buf, flexible, topic.partition_data.len())?;
        for partition in &topic.partition_data {
            buf.put_i32(partition.index);
            match &partition.records {
                None if flexible => buf.put_u8(0),
                None => buf.put_i32(-1),
                Some(records) => {
                    let n = i32::try_from(records.len())
                        .map_err(|_| "record batch too large".to_string())?;
                    if flexible {
                        put_uvarint(&mut buf, n.unsigned_abs() + 1);
                    } else {
                        buf.put_i32(n);
                    }
                    frame.push(buf.split().freeze());
                    frame.push(records.clone());
                }
            }
            if flexible {
                // No tagged fields.
                buf.put_u8(0);
            }
        }
        if flexible {
            buf.put_u8(0);
        }
    }
    if flexible {
        buf.put_u8(0);
    }
    frame.push(buf.freeze());
    Ok(frame)
}

/// A nullable string: an `INT16` length (-1 for null), or for flexible
/// versions an unsigned varint of the length plus one (0 for null).
fn put_string(buf: &mut BytesMut, flexible: bool, s: Option<&str>) -> Result<(), String> {
    match s {
        None if flexible => buf.put_u8(0),
        None => buf.put_i16(-1),
        Some(s) => {
            if flexible {
                let len = u32::try_from(s.len()).map_err(|_| "string too long".to_string())?;
                put_uvarint(buf, len + 1);
            } else {
                buf.put_i16(i16::try_from(s.len()).map_err(|_| "string too long".to_string())?);
            }
            buf.put_slice(s.as_bytes());
        }
    }
    Ok(())
}

/// An array length: an `INT32`, or for flexible versions an unsigned varint
/// of the length plus one.
fn put_array_len(buf: &mut BytesMut, flexible: bool, len: usize) -> Result<(), String> {
    let len = i32::try_from(len).map_err(|_| "array too long".to_string())?;
    if flexible {
        put_uvarint(buf, len.unsigned_abs() + 1);
    } else {
        buf.put_i32(len);
    }
    Ok(())
}

fn put_uvarint(buf: &mut BytesMut, mut v: u32) {
    while v >= 0x80 {
        buf.put_u8(u8::try_from(v & 0x7f).expect("masked") | 0x80);
        v >>= 7;
    }
    buf.put_u8(u8::try_from(v).expect("below 0x80"));
}

/// Reads the correlation id at the start of a response frame (after the
/// length prefix). The correlation id leads both response header versions.
pub(crate) fn peek_correlation_id(frame: &[u8]) -> Option<i32> {
    let bytes: [u8; 4] = frame.get(..4)?.try_into().ok()?;
    Some(i32::from_be_bytes(bytes))
}

/// Decodes a response frame (without its length prefix) for request `R`
/// sent with `version`.
pub(crate) fn decode_response<R: Request>(
    mut frame: Bytes,
    version: i16,
) -> Result<R::Response, String> {
    let api_key = ApiKey::try_from(R::KEY).map_err(|_| format!("unknown API key {}", R::KEY))?;
    let header_version = api_key.response_header_version(version);
    ResponseHeader::decode(&mut frame, header_version)
        .and_then(|_| R::Response::decode(&mut frame, version))
        .map_err(|e| format!("failed to decode {api_key:?} v{version} response: {e:#}"))
}

/// Reads the error code of an ApiVersions response body (it leads every
/// version), without decoding the rest.
pub(crate) fn api_versions_error_code(frame: &[u8]) -> Option<i16> {
    // Response header v0: correlation id only.
    let mut body = frame.get(4..)?;
    (body.remaining() >= 2).then(|| body.get_i16())
}

/// Maps a broker error code. `None` for `NONE` (0).
///
/// Codes librdkafka does not know map to [`RDKafkaErrorCode::Unknown`].
pub(crate) fn broker_error(code: i16) -> Option<RDKafkaErrorCode> {
    (code != 0).then(|| RDKafkaErrorCode::from(code))
}

/// Reports whether a broker error code is retriable in the Kafka protocol's
/// sense (`RetriableException` subclasses in the Java client).
pub(crate) fn is_retriable(code: RDKafkaErrorCode) -> bool {
    use RDKafkaErrorCode::*;
    matches!(
        code,
        // Local transport-level conditions.
        BrokerTransportFailure
            | OperationTimedOut
            | TimedOutQueue
            | Resolve
            | AllBrokersDown
            | WaitingForCoordinator
            | DestroyBroker
            // Broker errors.
            | InvalidMessage
            | UnknownTopicOrPartition
            | LeaderNotAvailable
            | NotLeaderForPartition
            | RequestTimedOut
            | NetworkException
            | CoordinatorLoadInProgress
            | CoordinatorNotAvailable
            | NotCoordinator
            | NotEnoughReplicas
            | NotEnoughReplicasAfterAppend
            | NotController
            | KafkaStorageError
            | FetchSessionIdNotFound
            | InvalidFetchSessionEpoch
            | ListenerNotFound
            | FencedLeaderEpoch
            | UnknownLeaderEpoch
            | OffsetNotAvailable
            | PreferredLeaderNotAvailable
            | EligibleLeadersNotAvailable
            | ElectionNotNeeded
            | UnstableOffsetCommit
            | ThrottlingQuotaExceeded
            | ConcurrentTransactions
            | ReassignmentInProgress
            | UnknownTopicId
    )
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::{ApiVersionsRequest, MetadataRequest};

    use super::*;

    #[mz_ore::test]
    fn supported_ranges_are_encodable() {
        for (key, raw, min, max) in SUPPORTED {
            assert_eq!(ApiKey::try_from(*raw), Ok(*key));
            let valid = key.valid_versions();
            assert!(
                valid.min <= *min && *max <= valid.max,
                "{key:?}: {min}..{max} not within kafka-protocol's {valid}"
            );
            assert!(min <= max);
        }
        assert!(api_key_value(ApiKey::DescribeGroups).is_none());
        assert_eq!(api_key_value(ApiKey::Metadata), Some(3));
    }

    #[mz_ore::test]
    fn negotiation_picks_max_of_intersection() {
        let v = ApiVersions::negotiate([
            (1, 0, 17),
            (0, 0, 8),
            (3, 0, 4),
            // Disjoint: broker only supports versions we don't.
            (8, 10, 11),
            // Unknown API key.
            (500, 0, 3),
        ]);
        assert_eq!(v.version(ApiKey::Fetch), Some(12));
        assert_eq!(v.version(ApiKey::Produce), Some(8));
        assert_eq!(v.version(ApiKey::Metadata), Some(4));
        assert_eq!(v.version(ApiKey::OffsetCommit), None);
        assert_eq!(v.version(ApiKey::ListOffsets), None);
    }

    #[mz_ore::test]
    fn request_framing() {
        let frame = encode_request(&ApiVersionsRequest::default(), 0, 7, "cid").unwrap();
        // len, api key 18, version 0, correlation id 7, client id "cid".
        assert_eq!(
            &frame[..],
            &[0, 0, 0, 13, 0, 18, 0, 0, 0, 0, 0, 7, 0, 3, b'c', b'i', b'd']
        );
        // v3 uses the flexible header v2: a trailing empty tagged field set
        // after the client id.
        let req = ApiVersionsRequest::default()
            .with_client_software_name(StrBytes::from_static_str("a"))
            .with_client_software_version(StrBytes::from_static_str("b"));
        let frame = encode_request(&req, 3, 1, "c").unwrap();
        let len = usize::try_from(i32::from_be_bytes(frame[..4].try_into().unwrap())).unwrap();
        assert_eq!(len, frame.len() - 4);
        assert_eq!(&frame[4..15], &[0, 18, 0, 3, 0, 0, 0, 1, 0, 1, b'c']);
        assert_eq!(frame[15], 0, "header tagged fields");

        let frame = encode_request(&MetadataRequest::default(), 12, -5, "").unwrap();
        assert_eq!(peek_correlation_id(&frame[8..]), Some(-5));
    }

    #[mz_ore::test]
    fn response_decoding() {
        use kafka_protocol::messages::ApiVersionsResponse;
        use kafka_protocol::messages::api_versions_response::ApiVersion;
        let resp = ApiVersionsResponse::default()
            .with_error_code(0)
            .with_api_keys(vec![
                ApiVersion::default()
                    .with_api_key(3)
                    .with_min_version(0)
                    .with_max_version(12),
            ]);
        for version in [0, 3] {
            let mut buf = BytesMut::new();
            // Response header v0 for ApiVersions regardless of version.
            buf.put_i32(42);
            resp.encode(&mut buf, version).unwrap();
            let frame = buf.freeze();
            assert_eq!(peek_correlation_id(&frame), Some(42));
            assert_eq!(api_versions_error_code(&frame), Some(0));
            let decoded = decode_response::<ApiVersionsRequest>(frame, version).unwrap();
            assert_eq!(decoded.api_keys.len(), 1);
            assert_eq!(decoded.api_keys[0].max_version, 12);
        }
        let err = decode_response::<ApiVersionsRequest>(Bytes::from_static(&[0, 0, 0, 1]), 3)
            .unwrap_err();
        assert!(
            err.starts_with("failed to decode ApiVersions v3 response"),
            "{err}"
        );
    }

    #[mz_ore::test]
    fn error_codes() {
        assert_eq!(broker_error(0), None);
        assert_eq!(
            broker_error(3),
            Some(RDKafkaErrorCode::UnknownTopicOrPartition)
        );
        assert_eq!(
            broker_error(58),
            Some(RDKafkaErrorCode::SaslAuthenticationFailed)
        );
        assert_eq!(broker_error(90), Some(RDKafkaErrorCode::ProducerFenced));
        assert_eq!(broker_error(-1), Some(RDKafkaErrorCode::Unknown));
        assert_eq!(broker_error(1000), Some(RDKafkaErrorCode::Unknown));
        assert!(is_retriable(RDKafkaErrorCode::NotCoordinator));
        assert!(is_retriable(RDKafkaErrorCode::BrokerTransportFailure));
        assert!(!is_retriable(RDKafkaErrorCode::TopicAuthorizationFailed));
        assert!(!is_retriable(RDKafkaErrorCode::ProducerFenced));
    }

    #[mz_ore::test]
    fn segmented_produce_request_matches_kafka_protocol() {
        use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
        use kafka_protocol::messages::{TopicName, TransactionalId};

        let records = |n: usize| Some(Bytes::from(vec![u8::try_from(n % 251).unwrap(); n]));
        // 300 bytes need a two-byte varint length.
        let topic = |name: &str, sizes: &[usize]| {
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_string(name.to_string())))
                .with_partition_data(
                    sizes
                        .iter()
                        .enumerate()
                        .map(|(i, n)| {
                            PartitionProduceData::default()
                                .with_index(i32::try_from(i).unwrap())
                                .with_records(records(*n))
                        })
                        .collect(),
                )
        };
        let requests = [
            ProduceRequest::default()
                .with_acks(-1)
                .with_timeout_ms(30_000)
                .with_topic_data(vec![topic("a", &[300, 1]), topic("bb", &[70_000])]),
            ProduceRequest::default()
                .with_transactional_id(Some(TransactionalId(StrBytes::from_static_str("txn"))))
                .with_acks(1)
                .with_timeout_ms(5)
                .with_topic_data(vec![
                    topic("t", &[5]),
                    TopicProduceData::default()
                        .with_name(TopicName(StrBytes::from_static_str("nulls")))
                        .with_partition_data(vec![PartitionProduceData::default().with_index(3)]),
                ]),
            ProduceRequest::default(),
        ];
        for request in &requests {
            for version in 3..=12 {
                let expected = encode_request(request, version, 42, "client").unwrap();
                let mut frame = encode_produce_request(request, version, 42, "client").unwrap();
                assert_eq!(frame.remaining(), expected.len(), "v{version}");
                let mut actual = Vec::new();
                while frame.has_remaining() {
                    let chunk = frame.chunk();
                    assert!(!chunk.is_empty());
                    actual.extend_from_slice(chunk);
                    let n = chunk.len();
                    frame.advance(n);
                }
                assert_eq!(actual, expected, "v{version}");
            }
        }
    }

    #[mz_ore::test]
    fn segmented_produce_request_keeps_batches_out_of_line() {
        use kafka_protocol::messages::TopicName;
        use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};

        let batch = Bytes::from(vec![7; 1000]);
        let request = ProduceRequest::default().with_topic_data(vec![
            TopicProduceData::default()
                .with_name(TopicName(StrBytes::from_static_str("t")))
                .with_partition_data(vec![
                    PartitionProduceData::default().with_records(Some(batch.clone())),
                ]),
        ]);
        let frame = encode_produce_request(&request, 9, 1, "c").unwrap();
        let mut slices = [IoSlice::new(&[]); 8];
        let n = frame.chunks_vectored(&mut slices);
        assert_eq!(n, 3);
        assert_eq!(slices[1].as_ptr(), batch.as_ptr());
        assert_eq!(
            slices.iter().map(|s| s.len()).sum::<usize>(),
            frame.remaining()
        );
    }
}
