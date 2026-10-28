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
//! The version ranges in [`supported_versions`] deliberately stop below
//! versions that identify topics by ID instead of name (Fetch 13,
//! OffsetCommit/OffsetFetch 10, Produce 13), below the batched coordinator and
//! offset-fetch formats (FindCoordinator 4, OffsetFetch 8), and below the
//! transaction protocol version 2 requests (Produce 12, AddPartitionsToTxn 4,
//! AddOffsetsToTxn 4, TxnOffsetCommit 5, EndTxn 5, InitProducerId 5), whose
//! semantics this client does not implement.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
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

/// The version range this client supports for `key`, or `None` if the client
/// does not use the API.
pub(crate) fn supported_versions(key: ApiKey) -> Option<VersionRange> {
    supported_index(key).map(|i| VersionRange {
        min: SUPPORTED[i].2,
        max: SUPPORTED[i].3,
    })
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
    /// The broker's advertised ranges, indexed like `SUPPORTED`.
    broker: [Option<VersionRange>; SUPPORTED.len()],
}

impl ApiVersions {
    /// Negotiates with the ranges a broker advertised in ApiVersions:
    /// `(api key, min version, max version)`.
    pub(crate) fn negotiate(broker_ranges: impl IntoIterator<Item = (i16, i16, i16)>) -> Self {
        let mut versions = ApiVersions {
            versions: [None; SUPPORTED.len()],
            broker: [None; SUPPORTED.len()],
        };
        for (key, min, max) in broker_ranges {
            let Some(idx) = SUPPORTED.iter().position(|(_, raw, ..)| *raw == key) else {
                continue;
            };
            let broker = VersionRange { min, max };
            versions.broker[idx] = Some(broker);
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

    /// The broker's advertised range for `key`, if any.
    pub(crate) fn broker_range(&self, key: ApiKey) -> Option<VersionRange> {
        self.broker[supported_index(key)?]
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
        assert!(supported_versions(ApiKey::DescribeGroups).is_none());
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
        assert_eq!(
            v.broker_range(ApiKey::OffsetCommit),
            Some(VersionRange { min: 10, max: 11 })
        );
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
}
