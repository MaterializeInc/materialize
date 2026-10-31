// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Admin client.
//!
//! The main object is the [`AdminClient`] struct. Its operations return
//! futures that do not borrow the client or their arguments, are `Send`, and
//! work with any executor.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::future::Future;
use std::sync::Arc;

use crate::client::{Client, ClientContext, DefaultClientContext};
use crate::config::{
    ClientConfig, ClientKind, ClientSettings, FromClientConfig, FromClientConfigAndContext,
};
use crate::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use crate::internal::admin::{
    AdminInner, AlterSpec, NewPartitionsSpec, NewTopicSpec, OpOptions, RESOURCE_BROKER,
    RESOURCE_GROUP, RESOURCE_TOPIC, ResourceKey,
};
use crate::topic_partition_list::TopicPartitionList;
use crate::util::Timeout;

/// A client for the Kafka admin API.
///
/// `AdminClient` provides programmatic access to managing a Kafka cluster,
/// notably manipulating topics, partitions, and configuration parameters.
pub struct AdminClient<C: ClientContext + 'static> {
    client: Client<C>,
    inner: Arc<AdminInner>,
}

impl<C: ClientContext> AdminClient<C> {
    /// Creates the admin engine on top of a client.
    pub(crate) fn from_client(client: Client<C>) -> KafkaResult<AdminClient<C>> {
        let inner = Arc::new(AdminInner::new(Arc::clone(client.cluster())));
        Ok(AdminClient { client, inner })
    }

    /// Creates new topics according to the provided `NewTopic` specifications.
    ///
    /// Note that while the API supports creating multiple topics at once, it
    /// is not transactional. Creation of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub fn create_topics<'a, I>(
        &self,
        topics: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<TopicResult>>> + Send + use<'a, C, I>
    where
        I: IntoIterator<Item = &'a NewTopic<'a>>,
    {
        let started = topics
            .into_iter()
            .map(NewTopic::to_spec)
            .collect::<KafkaResult<Vec<_>>>()
            .and_then(|topics| Ok(self.inner.create_topics(self.op_options(opts)?, topics)));
        async move { started?.await }
    }

    /// Deletes the named topics.
    ///
    /// Note that while the API supports deleting multiple topics at once, it
    /// is not transactional. Deletion of some topics may succeed while others
    /// fail. Be sure to check the result of each individual operation.
    pub fn delete_topics(
        &self,
        topic_names: &[&str],
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<TopicResult>>> + Send + use<C> {
        let started = topic_names
            .iter()
            .map(|name| {
                check_nul(name)?;
                Ok(name.to_string())
            })
            .collect::<KafkaResult<Vec<_>>>()
            .and_then(|names| Ok(self.inner.delete_topics(self.op_options(opts)?, names)));
        async move { started?.await }
    }

    /// Adds additional partitions to existing topics according to the provided
    /// `NewPartitions` specifications.
    ///
    /// Note that while the API supports creating partitions for multiple topics
    /// at once, it is not transactional. Creation of partitions for some
    /// topics may succeed while others fail. Be sure to check the result of
    /// each individual operation.
    pub fn create_partitions<'a, I>(
        &self,
        partitions: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<TopicResult>>> + Send + use<'a, C, I>
    where
        I: IntoIterator<Item = &'a NewPartitions<'a>>,
    {
        let started = partitions
            .into_iter()
            .map(NewPartitions::to_spec)
            .collect::<KafkaResult<Vec<_>>>()
            .and_then(|partitions| {
                Ok(self
                    .inner
                    .create_partitions(self.op_options(opts)?, partitions))
            });
        async move { started?.await }
    }

    /// Deletes records from a topic.
    ///
    /// The provided `offsets` is a topic partition list specifying which
    /// records to delete from a list of topic partitions. For each entry in the
    /// list, the messages at offsets before the specified offsets (exclusive)
    /// in the specified partition will be deleted. Use offset
    /// [`Offset::End`](crate::Offset::End) to delete all records in the
    /// partition.
    ///
    /// Returns a topic partition list describing the result of the deletion.
    /// If the operation succeeded for a partition, the offset for that
    /// partition will be set to the post-deletion low-water mark for that
    /// partition. If the operation failed for a partition, there will be an
    /// error for that partition's entry in the list.
    pub fn delete_records(
        &self,
        offsets: &TopicPartitionList,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<TopicPartitionList>> + Send + use<C> {
        let started = self
            .op_options(opts)
            .map(|opts| self.inner.delete_records(opts, offsets.clone()));
        async move { started?.await }
    }

    /// Retrieves the configuration parameters for the specified resources.
    ///
    /// Note that while the API supports describing multiple configurations at
    /// once, it is not transactional. There is no guarantee that you will see
    /// a consistent snapshot of the configuration across different resources.
    pub fn describe_configs<'a, I>(
        &self,
        configs: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<ConfigResourceResult>>> + Send + use<'a, C, I>
    where
        I: IntoIterator<Item = &'a ResourceSpecifier<'a>>,
    {
        let started = configs
            .into_iter()
            .map(ResourceSpecifier::to_key)
            .collect::<KafkaResult<Vec<_>>>()
            .and_then(|resources| {
                Ok(self
                    .inner
                    .describe_configs(self.op_options(opts)?, resources))
            });
        async move { started?.await }
    }

    /// Sets configuration parameters for the specified resources.
    ///
    /// Note that while the API supports altering multiple resources at once, it
    /// is not transactional. Alteration of some resources may succeed while
    /// others fail. Be sure to check the result of each individual operation.
    ///
    /// The alteration is not incremental: configuration parameters of a
    /// resource that are not included revert to their defaults.
    pub fn alter_configs<'a, I>(
        &self,
        configs: I,
        opts: &AdminOptions,
    ) -> impl Future<Output = KafkaResult<Vec<AlterConfigsResult>>> + Send + use<'a, C, I>
    where
        I: IntoIterator<Item = &'a AlterConfig<'a>>,
    {
        let started = configs
            .into_iter()
            .map(AlterConfig::to_spec)
            .collect::<KafkaResult<Vec<_>>>()
            .and_then(|resources| Ok(self.inner.alter_configs(self.op_options(opts)?, resources)));
        async move { started?.await }
    }

    /// Returns the client underlying this admin client.
    pub fn inner(&self) -> &Client<C> {
        &self.client
    }

    fn op_options(&self, opts: &AdminOptions) -> KafkaResult<OpOptions> {
        OpOptions::new(opts, self.inner.settings())
    }
}

/// Fails like rust-rdkafka's `CString::new` for strings passed to librdkafka.
fn check_nul(s: &str) -> KafkaResult<()> {
    CString::new(s).map(drop).map_err(KafkaError::Nul)
}

/// `RD_KAFKAP_PARTITIONS_MAX`.
const PARTITIONS_MAX: i32 = 100_000;
/// `RD_KAFKAP_BROKERS_MAX`.
const BROKERS_MAX: i32 = 10_000;

/// Validates a replica assignment like `rd_kafka_*_set_replica_assignment`.
fn assignment_spec(assignment: PartitionAssignment<'_>) -> KafkaResult<Vec<Vec<i32>>> {
    assignment
        .iter()
        .map(|brokers| {
            if brokers.len() > usize::try_from(BROKERS_MAX).expect("positive") {
                return Err(KafkaError::AdminOpCreation(format!(
                    "Too many brokers specified (RD_KAFKAP_BROKERS_MAX={BROKERS_MAX})"
                )));
            }
            Ok(brokers.to_vec())
        })
        .collect()
}

impl FromClientConfig for AdminClient<DefaultClientContext> {
    fn from_config(config: &ClientConfig) -> KafkaResult<AdminClient<DefaultClientContext>> {
        AdminClient::from_config_and_context(config, DefaultClientContext)
    }
}

impl<C: ClientContext> FromClientConfigAndContext<C> for AdminClient<C> {
    fn from_config_and_context(config: &ClientConfig, context: C) -> KafkaResult<AdminClient<C>> {
        let settings = ClientSettings::from_config(config, ClientKind::Admin)?;
        let client = Client::new(settings, context)?;
        AdminClient::from_client(client)
    }
}

/// Options for an admin API request.
#[derive(Default)]
pub struct AdminOptions {
    request_timeout: Option<Timeout>,
    operation_timeout: Option<Timeout>,
    validate_only: bool,
    broker_id: Option<i32>,
}

impl AdminOptions {
    /// Creates a new `AdminOptions`.
    pub fn new() -> AdminOptions {
        AdminOptions::default()
    }

    /// Sets the overall request timeout, including broker lookup, request
    /// transmission, operation time on broker, and response.
    ///
    /// Defaults to the `socket.timeout.ms` configuration parameter.
    pub fn request_timeout<T: Into<Timeout>>(mut self, timeout: Option<T>) -> Self {
        self.request_timeout = timeout.map(Into::into);
        self
    }

    /// Sets the broker's operation timeout, such as the timeout for
    /// CreateTopics to complete the creation of topics on the controller before
    /// returning a result to the application.
    ///
    /// Defaults to the `socket.timeout.ms` configuration parameter, as in
    /// librdkafka. rust-rdkafka documents that unset means not waiting, which
    /// is not what librdkafka does.
    ///
    /// NOTE: Zero, and [`Timeout::Never`] which librdkafka sends as `-1`, ask
    /// the broker not to wait. As in librdkafka, a `REQUEST_TIMED_OUT` answer
    /// then counts as success. KRaft-based Kafka answers that without
    /// creating the topic.
    ///
    /// Only the CreateTopics, DeleteTopics, CreatePartitions and DeleteRecords
    /// API calls respect this option.
    pub fn operation_timeout<T: Into<Timeout>>(mut self, timeout: Option<T>) -> Self {
        self.operation_timeout = timeout.map(Into::into);
        self
    }

    /// Tells the broker to only validate the request, without performing the
    /// requested operation.
    ///
    /// Defaults to false.
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Override what broker the admin request will be sent to.
    ///
    /// By default, a reasonable broker will be selected automatically.
    pub fn broker_id<T: Into<Option<i32>>>(mut self, broker_id: T) -> Self {
        self.broker_id = broker_id.into();
        self
    }

    pub(crate) fn get_request_timeout(&self) -> Option<Timeout> {
        self.request_timeout
    }

    pub(crate) fn get_operation_timeout(&self) -> Option<Timeout> {
        self.operation_timeout
    }

    pub(crate) fn get_validate_only(&self) -> bool {
        self.validate_only
    }

    pub(crate) fn get_broker_id(&self) -> Option<i32> {
        self.broker_id
    }
}

/// An individual result of a [`AdminClient::create_topics`],
/// [`AdminClient::delete_topics`], or [`AdminClient::create_partitions`]
/// operation.
pub type TopicResult = Result<String, (String, RDKafkaErrorCode)>;

/// Configuration for a CreateTopic operation.
#[derive(Debug)]
pub struct NewTopic<'a> {
    /// The name of the new topic.
    pub name: &'a str,
    /// The initial number of partitions, or -1 for the broker default.
    pub num_partitions: i32,
    /// The initial replication configuration.
    pub replication: TopicReplication<'a>,
    /// The initial configuration parameters for the topic.
    pub config: Vec<(&'a str, &'a str)>,
}

impl<'a> NewTopic<'a> {
    /// Creates a new `NewTopic`.
    pub fn new(
        name: &'a str,
        num_partitions: i32,
        replication: TopicReplication<'a>,
    ) -> NewTopic<'a> {
        NewTopic {
            name,
            num_partitions,
            replication,
            config: Vec::new(),
        }
    }

    /// Sets a new parameter in the initial topic configuration.
    pub fn set(mut self, key: &'a str, value: &'a str) -> NewTopic<'a> {
        self.config.push((key, value));
        self
    }

    /// Validates the topic like rust-rdkafka and `rd_kafka_NewTopic_new`.
    fn to_spec(&self) -> KafkaResult<NewTopicSpec> {
        check_nul(self.name)?;
        let (replication_factor, assignments) = match self.replication {
            TopicReplication::Fixed(n) => (n, vec![]),
            TopicReplication::Variable(assignment) => {
                if i32::try_from(assignment.len()).ok() != Some(self.num_partitions) {
                    return Err(KafkaError::AdminOpCreation(format!(
                        "replication configuration for topic '{}' assigns {} partition(s), \
                         which does not match the specified number of partitions ({})",
                        self.name,
                        assignment.len(),
                        self.num_partitions,
                    )));
                }
                (-1, assignment_spec(assignment)?)
            }
        };
        if self.num_partitions < -1 || self.num_partitions > PARTITIONS_MAX {
            return Err(KafkaError::AdminOpCreation(format!(
                "num_partitions out of expected range 1..{PARTITIONS_MAX} or -1 for broker default"
            )));
        }
        if replication_factor < -1 || replication_factor > BROKERS_MAX {
            return Err(KafkaError::AdminOpCreation(format!(
                "replication_factor out of expected range -1..{BROKERS_MAX}"
            )));
        }
        let configs = self
            .config
            .iter()
            .map(|(k, v)| {
                check_nul(k)?;
                check_nul(v)?;
                Ok((k.to_string(), v.to_string()))
            })
            .collect::<KafkaResult<_>>()?;
        Ok(NewTopicSpec {
            name: self.name.to_string(),
            num_partitions: self.num_partitions,
            replication_factor,
            assignments,
            configs,
        })
    }
}

/// An assignment of partitions to replicas.
///
/// Each element in the outer slice corresponds to the partition with that
/// index. The inner slice specifies the broker IDs to which replicas of that
/// partition should be assigned.
pub type PartitionAssignment<'a> = &'a [&'a [i32]];

/// Replication configuration for a new topic.
#[derive(Debug)]
pub enum TopicReplication<'a> {
    /// All partitions should use the same fixed replication factor, or -1 for
    /// the broker default.
    Fixed(i32),
    /// Each partition should use the replica assignment from
    /// `PartitionAssignment`.
    Variable(PartitionAssignment<'a>),
}

/// Configuration for a CreatePartitions operation.
pub struct NewPartitions<'a> {
    /// The name of the topic to which partitions should be added.
    pub topic_name: &'a str,
    /// The total number of partitions after the operation.
    pub new_partition_count: usize,
    /// The replica assignments for the new partitions.
    pub assignment: Option<PartitionAssignment<'a>>,
}

impl<'a> NewPartitions<'a> {
    /// Creates a new `NewPartitions`.
    pub fn new(topic_name: &'a str, new_partition_count: usize) -> NewPartitions<'a> {
        NewPartitions {
            topic_name,
            new_partition_count,
            assignment: None,
        }
    }

    /// Sets the partition replica assignment for the new partitions. Only
    /// assignments for newly created replicas should be included.
    pub fn assign(mut self, assignment: PartitionAssignment<'a>) -> NewPartitions<'a> {
        self.assignment = Some(assignment);
        self
    }

    /// Validates like rust-rdkafka and `rd_kafka_NewPartitions_new`.
    fn to_spec(&self) -> KafkaResult<NewPartitionsSpec> {
        check_nul(self.topic_name)?;
        if let Some(assignment) = self.assignment {
            if assignment.len() > self.new_partition_count {
                return Err(KafkaError::AdminOpCreation(format!(
                    "partition assignment for topic '{}' assigns {} partition(s), \
                     which is more than the requested total number of partitions ({})",
                    self.topic_name,
                    assignment.len(),
                    self.new_partition_count,
                )));
            }
        }
        let total = match i32::try_from(self.new_partition_count) {
            Ok(n) if (1..=PARTITIONS_MAX).contains(&n) => n,
            _ => {
                return Err(KafkaError::AdminOpCreation(format!(
                    "new_total_cnt out of expected range 1..{PARTITIONS_MAX}"
                )));
            }
        };
        Ok(NewPartitionsSpec {
            topic: self.topic_name.to_string(),
            total,
            assignments: self.assignment.map(assignment_spec).transpose()?,
        })
    }
}

/// The result of an individual DescribeConfig operation.
pub type ConfigResourceResult = Result<ConfigResource, RDKafkaErrorCode>;

/// Specification of a configurable resource.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ResourceSpecifier<'a> {
    /// A topic resource, identified by its name.
    Topic(&'a str),
    /// A group resource, identified by its ID.
    Group(&'a str),
    /// A broker resource, identified by its ID.
    Broker(i32),
}

impl ResourceSpecifier<'_> {
    fn to_key(&self) -> KafkaResult<ResourceKey> {
        let (resource_type, name) = match *self {
            ResourceSpecifier::Topic(name) => (RESOURCE_TOPIC, name.to_string()),
            ResourceSpecifier::Group(name) => (RESOURCE_GROUP, name.to_string()),
            ResourceSpecifier::Broker(id) => (RESOURCE_BROKER, id.to_string()),
        };
        check_nul(&name)?;
        Ok(ResourceKey {
            resource_type,
            name,
        })
    }
}

/// A `ResourceSpecifier` that owns its data.
#[derive(Debug, Eq, PartialEq)]
pub enum OwnedResourceSpecifier {
    /// A topic resource, identified by its name.
    Topic(String),
    /// A group resource, identified by its ID.
    Group(String),
    /// A broker resource, identified by its ID.
    Broker(i32),
}

/// The source of a configuration entry.
#[derive(Debug, Eq, PartialEq)]
pub enum ConfigSource {
    /// Unknown. Note that Kafka brokers before v1.1.0 do not reliably provide
    /// configuration source information.
    Unknown,
    /// A dynamic topic configuration.
    DynamicTopic,
    /// A dynamic broker configuration.
    DynamicBroker,
    /// The default dynamic broker configuration.
    DynamicDefaultBroker,
    /// The static broker configuration.
    StaticBroker,
    /// The hardcoded default configuration.
    Default,
}

/// An individual configuration parameter for a `ConfigResource`.
#[derive(Debug, Eq, PartialEq)]
pub struct ConfigEntry {
    /// The name of the configuration parameter.
    pub name: String,
    /// The value of the configuration parameter.
    pub value: Option<String>,
    /// The source of the configuration parameter.
    pub source: ConfigSource,
    /// Whether the configuration parameter is read only.
    pub is_read_only: bool,
    /// Whether the configuration parameter currently has the default value.
    pub is_default: bool,
    /// Whether the configuration parameter contains sensitive data.
    pub is_sensitive: bool,
}

/// A configurable resource and its current configuration values.
#[derive(Debug)]
pub struct ConfigResource {
    /// Identifies the resource.
    pub specifier: OwnedResourceSpecifier,
    /// The current configuration parameters.
    pub entries: Vec<ConfigEntry>,
}

impl ConfigResource {
    /// Builds a map of configuration entries, keyed by configuration entry
    /// name.
    ///
    /// NOTE: rust-rdkafka returns a `HashMap`.
    pub fn entry_map(&self) -> BTreeMap<&str, &ConfigEntry> {
        self.entries.iter().map(|e| (&*e.name, e)).collect()
    }

    /// Searches the configuration entries to find the named parameter.
    pub fn get(&self, name: &str) -> Option<&ConfigEntry> {
        self.entries.iter().find(|e| e.name == name)
    }
}

/// The result of an individual AlterConfig operation.
pub type AlterConfigsResult =
    Result<OwnedResourceSpecifier, (OwnedResourceSpecifier, RDKafkaErrorCode)>;

/// Configuration for an AlterConfig operation.
pub struct AlterConfig<'a> {
    /// Identifies the resource to be altered.
    pub specifier: ResourceSpecifier<'a>,
    /// The configuration parameters to be updated.
    ///
    /// NOTE: rust-rdkafka uses a `HashMap`.
    pub entries: BTreeMap<&'a str, &'a str>,
}

impl<'a> AlterConfig<'a> {
    /// Creates a new `AlterConfig`.
    pub fn new(specifier: ResourceSpecifier<'_>) -> AlterConfig<'_> {
        AlterConfig {
            specifier,
            entries: BTreeMap::new(),
        }
    }

    /// Sets the configuration parameter named `key` to the specified `value`.
    pub fn set(mut self, key: &'a str, value: &'a str) -> AlterConfig<'a> {
        self.entries.insert(key, value);
        self
    }

    fn to_spec(&self) -> KafkaResult<AlterSpec> {
        let resource = self.specifier.to_key()?;
        let entries = self
            .entries
            .iter()
            .map(|(k, v)| {
                check_nul(k)?;
                check_nul(v)?;
                Ok((k.to_string(), v.to_string()))
            })
            .collect::<KafkaResult<_>>()?;
        Ok(AlterSpec { resource, entries })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[mz_ore::test]
    fn builders() {
        let opts = AdminOptions::new()
            .request_timeout(Some(Duration::from_secs(5)))
            .operation_timeout(None::<Duration>)
            .validate_only(true)
            .broker_id(3);
        assert_eq!(
            opts.get_request_timeout(),
            Some(Timeout::After(Duration::from_secs(5)))
        );
        assert_eq!(opts.get_operation_timeout(), None);
        assert!(opts.get_validate_only());
        assert_eq!(opts.get_broker_id(), Some(3));

        let topic = NewTopic::new("t", -1, TopicReplication::Fixed(-1))
            .set("cleanup.policy", "compact")
            .set("retention.ms", "-1");
        assert_eq!(
            topic.config,
            [("cleanup.policy", "compact"), ("retention.ms", "-1")]
        );

        let alter = AlterConfig::new(ResourceSpecifier::Topic("t"))
            .set("a", "1")
            .set("a", "2");
        assert_eq!(alter.entries.get("a"), Some(&"2"));

        let p = NewPartitions::new("t", 4).assign(&[&[1], &[2]]);
        assert_eq!(p.new_partition_count, 4);
        assert!(p.assignment.is_some());

        let resource = ConfigResource {
            specifier: OwnedResourceSpecifier::Broker(1),
            entries: vec![ConfigEntry {
                name: "num.partitions".into(),
                value: Some("3".into()),
                source: ConfigSource::StaticBroker,
                is_read_only: false,
                is_default: false,
                is_sensitive: false,
            }],
        };
        assert_eq!(
            resource.get("num.partitions").unwrap().value.as_deref(),
            Some("3")
        );
        assert_eq!(resource.entry_map().len(), 1);
    }
}
