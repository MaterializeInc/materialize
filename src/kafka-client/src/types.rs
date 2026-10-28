// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error code and configuration result enums.
//!
//! Mirrors `rdkafka::types` (`rdkafka_sys::types`). The numeric values,
//! variant names and descriptions are librdkafka 2.12.1's
//! (`rd_kafka_resp_err_t` and `rd_kafka_err_descs`), because both reach users
//! through error messages. Broker error codes equal the Kafka protocol error
//! codes; local errors are negative and never appear on the wire.

use std::error::Error;
use std::fmt;

macro_rules! error_codes {
    ($($(#[$attr:meta])* $variant:ident = $value:literal, $name:literal, $desc:expr;)*) => {
        /// Native librdkafka error code.
        ///
        /// `Display` is `"{variant name} ({librdkafka description})"`, for example
        /// `BrokerTransportFailure (Local: Broker transport failure)`.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum RDKafkaErrorCode {
            $($(#[$attr])* $variant = $value,)*
        }

        impl RDKafkaErrorCode {
            /// The librdkafka numeric value. For broker errors this is the
            /// Kafka protocol error code.
            pub(crate) const fn to_i32(self) -> i32 {
                match self {
                    $(Self::$variant => $value,)*
                }
            }

            /// Maps a numeric value to its code, or `None` if librdkafka does
            /// not define it.
            pub(crate) const fn from_i32_opt(code: i32) -> Option<Self> {
                match code {
                    $($value => Some(Self::$variant),)*
                    _ => None,
                }
            }

            /// librdkafka's `rd_kafka_err2str` text, if it defines one.
            const fn description_text(self) -> Option<&'static str> {
                match self {
                    $(Self::$variant => $desc,)*
                }
            }

            /// librdkafka's `rd_kafka_err2name` text, e.g. `_TIMED_OUT`.
            pub(crate) const fn name(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)*
                }
            }
        }
    };
}

// Generated from librdkafka 2.12.1's `rdkafka.h` (values), `rdkafka.c`
// (`rd_kafka_err_descs`) and rdkafka-sys's variant names. `DestroyBroker`
// (-137) and `RebootstrapRequired` (129) are absent from rdkafka-sys's
// bindings but librdkafka 2.12.1 defines them.
error_codes! {
    #[doc(hidden)]
    Begin = -200, "_BEGIN", None;
    /// Received message is incorrect.
    BadMessage = -199, "_BAD_MSG", Some("Local: Bad message format");
    /// Bad/unknown compression.
    BadCompression = -198, "_BAD_COMPRESSION", Some("Local: Invalid compressed data");
    /// Broker is going away.
    BrokerDestroy = -197, "_DESTROY", Some("Local: Broker handle destroyed for termination");
    /// Generic failure.
    Fail = -196, "_FAIL", Some("Local: Communication failure with broker");
    /// Broker transport failure.
    BrokerTransportFailure = -195, "_TRANSPORT", Some("Local: Broker transport failure");
    /// Critical system resource.
    CriticalSystemResource = -194, "_CRIT_SYS_RESOURCE", Some("Local: Critical system resource failure");
    /// Failed to resolve broker.
    Resolve = -193, "_RESOLVE", Some("Local: Host resolution failure");
    /// Produced message timed out.
    MessageTimedOut = -192, "_MSG_TIMED_OUT", Some("Local: Message timed out");
    /// Reached the end of the topic+partition queue on the broker. Not really an error.
    PartitionEOF = -191, "_PARTITION_EOF", Some("Broker: No more messages");
    /// Permanent: Partition does not exist in cluster.
    UnknownPartition = -190, "_UNKNOWN_PARTITION", Some("Local: Unknown partition");
    /// File or filesystem error.
    FileSystem = -189, "_FS", Some("Local: File or filesystem error");
    /// Permanent: Topic does not exist in cluster.
    UnknownTopic = -188, "_UNKNOWN_TOPIC", Some("Local: Unknown topic");
    /// All broker connections are down.
    AllBrokersDown = -187, "_ALL_BROKERS_DOWN", Some("Local: All broker connections are down");
    /// Invalid argument, or invalid configuration.
    InvalidArgument = -186, "_INVALID_ARG", Some("Local: Invalid argument or configuration");
    /// Operation timed out.
    OperationTimedOut = -185, "_TIMED_OUT", Some("Local: Timed out");
    /// Queue is full.
    QueueFull = -184, "_QUEUE_FULL", Some("Local: Queue full");
    /// ISR count < required.acks.
    ISRInsufficient = -183, "_ISR_INSUFF", Some("Local: ISR count insufficient");
    /// Broker node update.
    NodeUpdate = -182, "_NODE_UPDATE", Some("Local: Broker node update");
    /// SSL error.
    SSL = -181, "_SSL", Some("Local: SSL error");
    /// Waiting for coordinator to become available.
    WaitingForCoordinator = -180, "_WAIT_COORD", Some("Local: Waiting for coordinator");
    /// Unknown client group.
    UnknownGroup = -179, "_UNKNOWN_GROUP", Some("Local: Unknown group");
    /// Operation in progress.
    InProgress = -178, "_IN_PROGRESS", Some("Local: Operation in progress");
    /// Previous operation in progress, wait for it to finish.
    PreviousInProgress = -177, "_PREV_IN_PROGRESS", Some("Local: Previous operation in progress");
    /// This operation would interfere with an existing subscription.
    ExistingSubscription = -176, "_EXISTING_SUBSCRIPTION", Some("Local: Existing subscription");
    /// Assigned partitions (rebalance_cb).
    AssignPartitions = -175, "_ASSIGN_PARTITIONS", Some("Local: Assign partitions");
    /// Revoked partitions (rebalance_cb).
    RevokePartitions = -174, "_REVOKE_PARTITIONS", Some("Local: Revoke partitions");
    /// Conflicting use.
    Conflict = -173, "_CONFLICT", Some("Local: Conflicting use");
    /// Wrong state.
    State = -172, "_STATE", Some("Local: Erroneous state");
    /// Unknown protocol.
    UnknownProtocol = -171, "_UNKNOWN_PROTOCOL", Some("Local: Unknown protocol");
    /// Not implemented.
    NotImplemented = -170, "_NOT_IMPLEMENTED", Some("Local: Not implemented");
    /// Authentication failure.
    Authentication = -169, "_AUTHENTICATION", Some("Local: Authentication failure");
    /// No stored offset.
    NoOffset = -168, "_NO_OFFSET", Some("Local: No offset stored");
    /// Outdated.
    Outdated = -167, "_OUTDATED", Some("Local: Outdated");
    /// Timed out in queue.
    TimedOutQueue = -166, "_TIMED_OUT_QUEUE", Some("Local: Timed out in queue");
    /// Feature not supported by broker.
    UnsupportedFeature = -165, "_UNSUPPORTED_FEATURE", Some("Local: Required feature not supported by broker");
    /// Awaiting cache update.
    WaitCache = -164, "_WAIT_CACHE", Some("Local: Awaiting cache update");
    /// Operation interrupted (e.g., due to yield).
    Interrupted = -163, "_INTR", Some("Local: Operation interrupted");
    /// Key serialization error.
    KeySerialization = -162, "_KEY_SERIALIZATION", Some("Local: Key serialization error");
    /// Value serialization error.
    ValueSerialization = -161, "_VALUE_SERIALIZATION", Some("Local: Value serialization error");
    /// Key deserialization error.
    KeyDeserialization = -160, "_KEY_DESERIALIZATION", Some("Local: Key deserialization error");
    /// Value deserialization error.
    ValueDeserialization = -159, "_VALUE_DESERIALIZATION", Some("Local: Value deserialization error");
    /// Partial response.
    Partial = -158, "_PARTIAL", Some("Local: Partial response");
    /// Modification attempted on read-only object.
    ReadOnly = -157, "_READ_ONLY", Some("Local: Read-only object");
    /// No such entry or item not found.
    NoEnt = -156, "_NOENT", Some("Local: No such entry");
    /// Read underflow.
    Underflow = -155, "_UNDERFLOW", Some("Local: Read underflow");
    /// Invalid type.
    InvalidType = -154, "_INVALID_TYPE", Some("Local: Invalid type");
    /// Retry operation.
    Retry = -153, "_RETRY", Some("Local: Retry operation");
    /// Purged in queue.
    PurgeQueue = -152, "_PURGE_QUEUE", Some("Local: Purged in queue");
    /// Purged in flight.
    PurgeInflight = -151, "_PURGE_INFLIGHT", Some("Local: Purged in flight");
    /// Fatal error: see rd_kafka_fatal_error().
    Fatal = -150, "_FATAL", Some("Local: Fatal error");
    /// Inconsistent state.
    Inconsistent = -149, "_INCONSISTENT", Some("Local: Inconsistent state");
    /// Gap-less ordering would not be guaranteed if proceeding.
    GaplessGuarantee = -148, "_GAPLESS_GUARANTEE", Some("Local: Gap-less ordering would not be guaranteed if proceeding");
    /// Maximum poll interval exceeded.
    PollExceeded = -147, "_MAX_POLL_EXCEEDED", Some("Local: Maximum application poll interval (max.poll.interval.ms) exceeded");
    /// Unknown broker.
    UnknownBroker = -146, "_UNKNOWN_BROKER", Some("Local: Unknown broker");
    /// Functionality not configured.
    NotConfigured = -145, "_NOT_CONFIGURED", Some("Local: Functionality not configured");
    /// Instance has been fenced.
    Fenced = -144, "_FENCED", Some("Local: This instance has been fenced by a newer instance");
    /// Application generated error.
    Application = -143, "_APPLICATION", Some("Local: Application generated error");
    /// Assignment lost.
    AssignmentLost = -142, "_ASSIGNMENT_LOST", Some("Local: Group partition assignment lost");
    /// No operation performed.
    Noop = -141, "_NOOP", Some("Local: No operation performed");
    /// No offset to automatically reset to.
    AutoOffsetReset = -140, "_AUTO_OFFSET_RESET", Some("Local: No offset to automatically reset to");
    /// Partition log truncation detected
    LogTruncation = -139, "_LOG_TRUNCATION", Some("Local: Partition log truncation detected");
    /// A different record in the batch was invalid and this message failed persisting.
    InvalidDifferentRecord = -138, "_INVALID_DIFFERENT_RECORD", Some("Local: an invalid record in the same batch caused the failure of this message too");
    /// Broker handle destroyed without termination.
    DestroyBroker = -137, "_DESTROY_BROKER", Some("Local: Broker handle destroyed without termination");
    #[doc(hidden)]
    End = -100, "_END", None;
    /// Unknown broker error.
    Unknown = -1, "UNKNOWN", Some("Unknown broker error");
    /// Success.
    NoError = 0, "NO_ERROR", Some("Success");
    /// Offset out of range.
    OffsetOutOfRange = 1, "OFFSET_OUT_OF_RANGE", Some("Broker: Offset out of range");
    /// Invalid message.
    InvalidMessage = 2, "INVALID_MSG", Some("Broker: Invalid message");
    /// Unknown topic or partition.
    UnknownTopicOrPartition = 3, "UNKNOWN_TOPIC_OR_PART", Some("Broker: Unknown topic or partition");
    /// Invalid message size.
    InvalidMessageSize = 4, "INVALID_MSG_SIZE", Some("Broker: Invalid message size");
    /// Leader not available.
    LeaderNotAvailable = 5, "LEADER_NOT_AVAILABLE", Some("Broker: Leader not available");
    /// Not leader for partition.
    NotLeaderForPartition = 6, "NOT_LEADER_FOR_PARTITION", Some("Broker: Not leader for partition");
    /// Request timed out.
    RequestTimedOut = 7, "REQUEST_TIMED_OUT", Some("Broker: Request timed out");
    /// Broker not available.
    BrokerNotAvailable = 8, "BROKER_NOT_AVAILABLE", Some("Broker: Broker not available");
    /// Replica not available.
    ReplicaNotAvailable = 9, "REPLICA_NOT_AVAILABLE", Some("Broker: Replica not available");
    /// Message size too large.
    MessageSizeTooLarge = 10, "MSG_SIZE_TOO_LARGE", Some("Broker: Message size too large");
    /// Stale controller epoch code.
    StaleControllerEpoch = 11, "STALE_CTRL_EPOCH", Some("Broker: StaleControllerEpochCode");
    /// Offset metadata string too large.
    OffsetMetadataTooLarge = 12, "OFFSET_METADATA_TOO_LARGE", Some("Broker: Offset metadata string too large");
    /// Broker disconnected before response received.
    NetworkException = 13, "NETWORK_EXCEPTION", Some("Broker: Broker disconnected before response received");
    /// Coordinator load in progress.
    CoordinatorLoadInProgress = 14, "COORDINATOR_LOAD_IN_PROGRESS", Some("Broker: Coordinator load in progress");
    /// Coordinator not available.
    CoordinatorNotAvailable = 15, "COORDINATOR_NOT_AVAILABLE", Some("Broker: Coordinator not available");
    /// Not coordinator.
    NotCoordinator = 16, "NOT_COORDINATOR", Some("Broker: Not coordinator");
    /// Invalid topic.
    InvalidTopic = 17, "TOPIC_EXCEPTION", Some("Broker: Invalid topic");
    /// Message batch larger than configured server segment size.
    MessageBatchTooLarge = 18, "RECORD_LIST_TOO_LARGE", Some("Broker: Message batch larger than configured server segment size");
    /// Not enough in-sync replicas.
    NotEnoughReplicas = 19, "NOT_ENOUGH_REPLICAS", Some("Broker: Not enough in-sync replicas");
    /// Message(s) written to insufficient number of in-sync replicas.
    NotEnoughReplicasAfterAppend = 20, "NOT_ENOUGH_REPLICAS_AFTER_APPEND", Some("Broker: Message(s) written to insufficient number of in-sync replicas");
    /// Invalid required acks value.
    InvalidRequiredAcks = 21, "INVALID_REQUIRED_ACKS", Some("Broker: Invalid required acks value");
    /// Specified group generation id is not valid.
    IllegalGeneration = 22, "ILLEGAL_GENERATION", Some("Broker: Specified group generation id is not valid");
    /// Inconsistent group protocol.
    InconsistentGroupProtocol = 23, "INCONSISTENT_GROUP_PROTOCOL", Some("Broker: Inconsistent group protocol");
    /// Invalid group.id.
    InvalidGroupId = 24, "INVALID_GROUP_ID", Some("Broker: Invalid group.id");
    /// Unknown member.
    UnknownMemberId = 25, "UNKNOWN_MEMBER_ID", Some("Broker: Unknown member");
    /// Invalid session timeout.
    InvalidSessionTimeout = 26, "INVALID_SESSION_TIMEOUT", Some("Broker: Invalid session timeout");
    /// Group rebalance in progress.
    RebalanceInProgress = 27, "REBALANCE_IN_PROGRESS", Some("Broker: Group rebalance in progress");
    /// Commit offset data size is not valid.
    InvalidCommitOffsetSize = 28, "INVALID_COMMIT_OFFSET_SIZE", Some("Broker: Commit offset data size is not valid");
    /// Topic authorization failed.
    TopicAuthorizationFailed = 29, "TOPIC_AUTHORIZATION_FAILED", Some("Broker: Topic authorization failed");
    /// Group authorization failed.
    GroupAuthorizationFailed = 30, "GROUP_AUTHORIZATION_FAILED", Some("Broker: Group authorization failed");
    /// Cluster authorization failed.
    ClusterAuthorizationFailed = 31, "CLUSTER_AUTHORIZATION_FAILED", Some("Broker: Cluster authorization failed");
    /// Invalid timestamp.
    InvalidTimestamp = 32, "INVALID_TIMESTAMP", Some("Broker: Invalid timestamp");
    /// Unsupported SASL mechanism.
    UnsupportedSASLMechanism = 33, "UNSUPPORTED_SASL_MECHANISM", Some("Broker: Unsupported SASL mechanism");
    /// Illegal SASL state.
    IllegalSASLState = 34, "ILLEGAL_SASL_STATE", Some("Broker: Request not valid in current SASL state");
    /// Unsupported version.
    UnsupportedVersion = 35, "UNSUPPORTED_VERSION", Some("Broker: API version not supported");
    /// Topic already exists.
    TopicAlreadyExists = 36, "TOPIC_ALREADY_EXISTS", Some("Broker: Topic already exists");
    /// Invalid number of partitions.
    InvalidPartitions = 37, "INVALID_PARTITIONS", Some("Broker: Invalid number of partitions");
    /// Invalid replication factor.
    InvalidReplicationFactor = 38, "INVALID_REPLICATION_FACTOR", Some("Broker: Invalid replication factor");
    /// Invalid replica assignment.
    InvalidReplicaAssignment = 39, "INVALID_REPLICA_ASSIGNMENT", Some("Broker: Invalid replica assignment");
    /// Invalid config.
    InvalidConfig = 40, "INVALID_CONFIG", Some("Broker: Configuration is invalid");
    /// Not controller for cluster.
    NotController = 41, "NOT_CONTROLLER", Some("Broker: Not controller for cluster");
    /// Invalid request.
    InvalidRequest = 42, "INVALID_REQUEST", Some("Broker: Invalid request");
    /// Message format on broker does not support request.
    UnsupportedForMessageFormat = 43, "UNSUPPORTED_FOR_MESSAGE_FORMAT", Some("Broker: Message format on broker does not support request");
    /// Policy violation.
    PolicyViolation = 44, "POLICY_VIOLATION", Some("Broker: Policy violation");
    /// Broker received an out of order sequence number.
    OutOfOrderSequenceNumber = 45, "OUT_OF_ORDER_SEQUENCE_NUMBER", Some("Broker: Broker received an out of order sequence number");
    /// Broker received a duplicate sequence number.
    DuplicateSequenceNumber = 46, "DUPLICATE_SEQUENCE_NUMBER", Some("Broker: Broker received a duplicate sequence number");
    /// Producer attempted an operation with an old epoch.
    InvalidProducerEpoch = 47, "INVALID_PRODUCER_EPOCH", Some("Broker: Producer attempted an operation with an old epoch");
    /// Producer attempted a transactional operation in an invalid state.
    InvalidTransactionalState = 48, "INVALID_TXN_STATE", Some("Broker: Producer attempted a transactional operation in an invalid state");
    /// Producer attempted to use a producer id which is currently assigned to its transactional id.
    InvalidProducerIdMapping = 49, "INVALID_PRODUCER_ID_MAPPING", Some("Broker: Producer attempted to use a producer id which is not currently assigned to its transactional id");
    /// Transaction timeout is larger than the maxi value allowed by the broker's max.transaction.timeout.ms.
    InvalidTransactionTimeout = 50, "INVALID_TRANSACTION_TIMEOUT", Some("Broker: Transaction timeout is larger than the maximum value allowed by the broker's max.transaction.timeout.ms");
    /// Producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.
    ConcurrentTransactions = 51, "CONCURRENT_TRANSACTIONS", Some("Broker: Producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing");
    /// Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.
    TransactionCoordinatorFenced = 52, "TRANSACTION_COORDINATOR_FENCED", Some("Broker: Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer");
    /// Transactional Id authorization failed.
    TransactionalIdAuthorizationFailed = 53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED", Some("Broker: Transactional Id authorization failed");
    /// Security features are disabled.
    SecurityDisabled = 54, "SECURITY_DISABLED", Some("Broker: Security features are disabled");
    /// Operation not attempted.
    OperationNotAttempted = 55, "OPERATION_NOT_ATTEMPTED", Some("Broker: Operation not attempted");
    /// Disk error when trying to access log file on the disk.
    KafkaStorageError = 56, "KAFKA_STORAGE_ERROR", Some("Broker: Disk error when trying to access log file on disk");
    /// The user-specified log directory is not found in the broker config.
    LogDirNotFound = 57, "LOG_DIR_NOT_FOUND", Some("Broker: The user-specified log directory is not found in the broker config");
    /// SASL Authentication failed.
    SaslAuthenticationFailed = 58, "SASL_AUTHENTICATION_FAILED", Some("Broker: SASL Authentication failed");
    /// Unknown Producer Id.
    UnknownProducerId = 59, "UNKNOWN_PRODUCER_ID", Some("Broker: Unknown Producer Id");
    /// Partition reassignment is in progress.
    ReassignmentInProgress = 60, "REASSIGNMENT_IN_PROGRESS", Some("Broker: Partition reassignment is in progress");
    /// Delegation Token feature is not enabled.
    DelegationTokenAuthDisabled = 61, "DELEGATION_TOKEN_AUTH_DISABLED", Some("Broker: Delegation Token feature is not enabled");
    /// Delegation Token is not found on server.
    DelegationTokenNotFound = 62, "DELEGATION_TOKEN_NOT_FOUND", Some("Broker: Delegation Token is not found on server");
    /// Specified Principal is not valid Owner/Renewer.
    DelegationTokenOwnerMismatch = 63, "DELEGATION_TOKEN_OWNER_MISMATCH", Some("Broker: Specified Principal is not valid Owner/Renewer");
    /// Delegation Token requests are not allowed on this connection.
    DelegationTokenRequestNotAllowed = 64, "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", Some("Broker: Delegation Token requests are not allowed on this connection");
    /// Delegation Token authorization failed.
    DelegationTokenAuthorizationFailed = 65, "DELEGATION_TOKEN_AUTHORIZATION_FAILED", Some("Broker: Delegation Token authorization failed");
    /// Delegation Token is expired.
    DelegationTokenExpired = 66, "DELEGATION_TOKEN_EXPIRED", Some("Broker: Delegation Token is expired");
    /// Supplied principalType is not supported.
    InvalidPrincipalType = 67, "INVALID_PRINCIPAL_TYPE", Some("Broker: Supplied principalType is not supported");
    /// The group is not empty.
    NonEmptyGroup = 68, "NON_EMPTY_GROUP", Some("Broker: The group is not empty");
    /// The group id does not exist.
    GroupIdNotFound = 69, "GROUP_ID_NOT_FOUND", Some("Broker: The group id does not exist");
    /// The fetch session ID was not found.
    FetchSessionIdNotFound = 70, "FETCH_SESSION_ID_NOT_FOUND", Some("Broker: The fetch session ID was not found");
    /// The fetch session epoch is invalid.
    InvalidFetchSessionEpoch = 71, "INVALID_FETCH_SESSION_EPOCH", Some("Broker: The fetch session epoch is invalid");
    /// No matching listener.
    ListenerNotFound = 72, "LISTENER_NOT_FOUND", Some("Broker: No matching listener");
    /// Topic deletion is disabled.
    TopicDeletionDisabled = 73, "TOPIC_DELETION_DISABLED", Some("Broker: Topic deletion is disabled");
    /// Leader epoch is older than broker epoch.
    FencedLeaderEpoch = 74, "FENCED_LEADER_EPOCH", Some("Broker: Leader epoch is older than broker epoch");
    /// Leader epoch is newer than broker epoch.
    UnknownLeaderEpoch = 75, "UNKNOWN_LEADER_EPOCH", Some("Broker: Leader epoch is newer than broker epoch");
    /// Unsupported compression type.
    UnsupportedCompressionType = 76, "UNSUPPORTED_COMPRESSION_TYPE", Some("Broker: Unsupported compression type");
    /// Broker epoch has changed.
    StaleBrokerEpoch = 77, "STALE_BROKER_EPOCH", Some("Broker: Broker epoch has changed");
    /// Leader high watermark is not caught up.
    OffsetNotAvailable = 78, "OFFSET_NOT_AVAILABLE", Some("Broker: Leader high watermark is not caught up");
    /// Group member needs a valid member ID.
    MemberIdRequired = 79, "MEMBER_ID_REQUIRED", Some("Broker: Group member needs a valid member ID");
    /// Preferred leader was not available.
    PreferredLeaderNotAvailable = 80, "PREFERRED_LEADER_NOT_AVAILABLE", Some("Broker: Preferred leader was not available");
    /// Consumer group has reached maximum size.
    GroupMaxSizeReached = 81, "GROUP_MAX_SIZE_REACHED", Some("Broker: Consumer group has reached maximum size");
    /// Static consumer fenced by other consumer with same group.instance.id.
    FencedInstanceId = 82, "FENCED_INSTANCE_ID", Some("Broker: Static consumer fenced by other consumer with same group.instance.id");
    /// Eligible partition leaders are not available.
    EligibleLeadersNotAvailable = 83, "ELIGIBLE_LEADERS_NOT_AVAILABLE", Some("Broker: Eligible partition leaders are not available");
    /// Leader election not needed for topic partition.
    ElectionNotNeeded = 84, "ELECTION_NOT_NEEDED", Some("Broker: Leader election not needed for topic partition");
    /// No partition reassignment is in progress.
    NoReassignmentInProgress = 85, "NO_REASSIGNMENT_IN_PROGRESS", Some("Broker: No partition reassignment is in progress");
    /// Deleting offsets of a topic while the consumer group is subscribed to it.
    GroupSubscribedToTopic = 86, "GROUP_SUBSCRIBED_TO_TOPIC", Some("Broker: Deleting offsets of a topic while the consumer group is subscribed to it");
    /// Broker failed to validate record.
    InvalidRecord = 87, "INVALID_RECORD", Some("Broker: Broker failed to validate record");
    /// There are unstable offsets that need to be cleared.
    UnstableOffsetCommit = 88, "UNSTABLE_OFFSET_COMMIT", Some("Broker: There are unstable offsets that need to be cleared");
    /// Throttling quota has been exceeded.
    ThrottlingQuotaExceeded = 89, "THROTTLING_QUOTA_EXCEEDED", Some("Broker: Throttling quota has been exceeded");
    /// There is a newer producer with the same transactional ID which fences the current one.
    ProducerFenced = 90, "PRODUCER_FENCED", Some("Broker: There is a newer producer with the same transactionalId which fences the current one");
    /// Request illegally referred to resource that does not exist.
    ResourceNotFound = 91, "RESOURCE_NOT_FOUND", Some("Broker: Request illegally referred to resource that does not exist");
    /// Request illegally referred to the same resource twice.
    DuplicateResource = 92, "DUPLICATE_RESOURCE", Some("Broker: Request illegally referred to the same resource twice");
    /// Requested credential would not meet criteria for acceptability.
    UnacceptableCredential = 93, "UNACCEPTABLE_CREDENTIAL", Some("Broker: Requested credential would not meet criteria for acceptability");
    /// Either the sender or recipient of a voter-only request is not one of the expected voters.
    InconsistentVoterSet = 94, "INCONSISTENT_VOTER_SET", Some("Broker: Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters");
    /// Invalid update version.
    InvalidUpdateVersion = 95, "INVALID_UPDATE_VERSION", Some("Broker: Invalid update version");
    /// Unable to update finalized features due to server error.
    FeatureUpdateFailed = 96, "FEATURE_UPDATE_FAILED", Some("Broker: Unable to update finalized features due to server error");
    /// Request principal deserialization failed during forwarding.
    PrincipalDeserializationFailure = 97, "PRINCIPAL_DESERIALIZATION_FAILURE", Some("Broker: Request principal deserialization failed during forwarding");
    /// Unknown topic ID.
    UnknownTopicId = 100, "UNKNOWN_TOPIC_ID", Some("Broker: Unknown topic id");
    /// The member epoch is fenced by the group coordinator.
    FencedMemberEpoch = 110, "FENCED_MEMBER_EPOCH", Some("Broker: The member epoch is fenced by the group coordinator");
    /// The instance ID is still used by another member in the consumer group.
    UnreleasedInstanceId = 111, "UNRELEASED_INSTANCE_ID", Some("Broker: The instance ID is still used by another member in the consumer group");
    /// The assignor or its version range is not supported by the consumer group.
    UnsupportedAssignor = 112, "UNSUPPORTED_ASSIGNOR", Some("Broker: The assignor or its version range is not supported by the consumer group");
    /// The member epoch is stale.
    StaleMemberEpoch = 113, "STALE_MEMBER_EPOCH", Some("Broker: The member epoch is stale");
    /// The client sent a push telemetry request with an invalid or outdated subscription ID.
    UnknownSubscriptionId = 117, "UNKNOWN_SUBSCRIPTION_ID", Some("Broker: Client sent a push telemetry request with an invalid or outdated subscription ID");
    /// The client sent a push telemetry request larger than the maximum size that the broker will accept.
    TelemetryTooLarge = 118, "TELEMETRY_TOO_LARGE", Some("Broker: Client sent a push telemetry request larger than the maximum size the broker will accept");
    /// Client metadata is stale, client should rebootstrap.
    RebootstrapRequired = 129, "REBOOTSTRAP_REQUIRED", Some("Broker: Client metadata is stale, client should rebootstrap to obtain new metadata");
    #[doc(hidden)]
    EndAll = 130, "END_ALL", None;
}

impl RDKafkaErrorCode {
    /// librdkafka's `rd_kafka_err2str` text: the description, or `Err-<n>?`
    /// for codes without one.
    pub(crate) fn err2str(self) -> String {
        match self.description_text() {
            Some(desc) => desc.to_string(),
            None => format!("Err-{}?", self.to_i32()),
        }
    }
}

impl fmt::Display for RDKafkaErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.description_text() {
            Some(desc) => write!(f, "{:?} ({})", self, desc),
            None => write!(f, "{:?} (Err-{}?)", self, self.to_i32()),
        }
    }
}

impl Error for RDKafkaErrorCode {}

/// Maps a librdkafka or Kafka protocol error code.
///
/// Codes librdkafka does not define map to [`RDKafkaErrorCode::Unknown`]. Code
/// that needs the raw value of an unrecognized broker error must log it
/// before converting.
impl From<i32> for RDKafkaErrorCode {
    fn from(code: i32) -> RDKafkaErrorCode {
        RDKafkaErrorCode::from_i32_opt(code).unwrap_or(RDKafkaErrorCode::Unknown)
    }
}

/// Maps a Kafka protocol error code (the wire type is `INT16`). See the
/// `From<i32>` impl for unknown codes.
impl From<i16> for RDKafkaErrorCode {
    fn from(code: i16) -> RDKafkaErrorCode {
        RDKafkaErrorCode::from(i32::from(code))
    }
}

/// Response error.
///
/// NOTE: In rust-rdkafka this is the bindgen enum `rd_kafka_resp_err_t`. Here it
/// is the same type as [`RDKafkaErrorCode`], so `RDKafkaErrorCode::from(err)` is
/// the identity conversion.
pub type RDKafkaRespErr = RDKafkaErrorCode;

/// Result of setting a configuration property.
#[allow(non_camel_case_types)]
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum RDKafkaConfRes {
    /// Unknown configuration name.
    RD_KAFKA_CONF_UNKNOWN = -2,
    /// Invalid configuration value or property or value not supported in this
    /// build.
    RD_KAFKA_CONF_INVALID = -1,
    /// Configuration okay.
    RD_KAFKA_CONF_OK = 0,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn display_matches_rdkafka() {
        let cases = [
            (
                RDKafkaErrorCode::BrokerTransportFailure,
                "BrokerTransportFailure (Local: Broker transport failure)",
            ),
            (
                RDKafkaErrorCode::UnknownTopicOrPartition,
                "UnknownTopicOrPartition (Broker: Unknown topic or partition)",
            ),
            (
                RDKafkaErrorCode::OperationTimedOut,
                "OperationTimedOut (Local: Timed out)",
            ),
            (
                RDKafkaErrorCode::InvalidConfig,
                "InvalidConfig (Broker: Configuration is invalid)",
            ),
            (
                RDKafkaErrorCode::PartitionEOF,
                "PartitionEOF (Broker: No more messages)",
            ),
            (RDKafkaErrorCode::Unknown, "Unknown (Unknown broker error)"),
            (RDKafkaErrorCode::Begin, "Begin (Err--200?)"),
            (RDKafkaErrorCode::End, "End (Err--100?)"),
            (
                RDKafkaErrorCode::RebootstrapRequired,
                "RebootstrapRequired (Broker: Client metadata is stale, client should \
                 rebootstrap to obtain new metadata)",
            ),
        ];
        for (code, expected) in cases {
            assert_eq!(code.to_string(), expected);
        }
    }

    #[mz_ore::test]
    fn numeric_round_trip() {
        for value in -250..200 {
            if let Some(code) = RDKafkaErrorCode::from_i32_opt(value) {
                assert_eq!(code.to_i32(), value);
                assert_eq!(RDKafkaErrorCode::from(value), code);
            } else {
                assert_eq!(RDKafkaErrorCode::from(value), RDKafkaErrorCode::Unknown);
            }
        }
        assert_eq!(
            RDKafkaErrorCode::from(3_i16),
            RDKafkaErrorCode::UnknownTopicOrPartition
        );
        assert_eq!(
            RDKafkaErrorCode::from(36_i16),
            RDKafkaErrorCode::TopicAlreadyExists
        );
        assert_eq!(
            RDKafkaErrorCode::from(-137),
            RDKafkaErrorCode::DestroyBroker
        );
        assert_eq!(RDKafkaErrorCode::from(98_i16), RDKafkaErrorCode::Unknown);
        assert_eq!(RDKafkaErrorCode::OperationTimedOut.name(), "_TIMED_OUT");
        assert_eq!(RDKafkaErrorCode::ProducerFenced.name(), "PRODUCER_FENCED");
    }
}
