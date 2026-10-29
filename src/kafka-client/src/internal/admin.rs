// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The admin engine: topic, partition, record and config administration.
//!
//! Each operation runs as one task on the `kafka-io` runtime. It starts when
//! the facade method is called, as librdkafka enqueues an admin op at call
//! time, and is aborted when its future is dropped.
//!
//! # Timeouts
//!
//! As in librdkafka, the request timeout bounds the whole operation: waiting
//! for the target broker's connection, sending, and the response. Waiting for
//! a broker retries lookups and connection failures until then, and the
//! operation fails with `OperationTimedOut` when it passes. The operation
//! timeout is sent to the broker (CreateTopics, DeleteTopics,
//! CreatePartitions, DeleteRecords), which waits that long for the operation
//! to complete. Each request also has librdkafka's network timeout:
//! `socket.timeout.ms`, or the operation timeout plus one second if that is
//! longer. Both default to `socket.timeout.ms`.
//!
//! # Target brokers
//!
//! CreateTopics, DeleteTopics and CreatePartitions go to the controller.
//! DescribeConfigs and AlterConfigs go to the broker of a broker resource, of
//! which there may be at most one, else to the controller. DeleteRecords goes
//! to each partition's leader. An explicit `AdminOptions::broker_id` overrides
//! all of these, as in librdkafka.
//!
//! # Errors
//!
//! Failures of the request as a whole are `KafkaError::AdminOp(code)`, with
//! librdkafka's codes. Per-resource errors are returned per resource. Unlike
//! librdkafka:
//!
//! * Topics that fail with `NOT_CONTROLLER` are retried at the new controller
//!   until the request timeout, like the Java admin client.
//! * Config sources this client does not know map to `ConfigSource::Unknown`
//!   instead of failing the whole result.
//!
//! The crate docs list the deviations from rust-rdkafka: per-resource errors
//! of DescribeConfigs and AlterConfigs, and [`KafkaError::Canceled`] for
//! operations pending at shutdown.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::alter_configs_request::{AlterConfigsResource, AlterableConfig};
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsTopic,
};
use kafka_protocol::messages::create_topics_request::{
    CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
};
use kafka_protocol::messages::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsTopic,
};
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::messages::{
    AlterConfigsRequest, ApiKey, BrokerId, CreatePartitionsRequest, CreateTopicsRequest,
    DeleteRecordsRequest, DeleteTopicsRequest, DescribeConfigsRequest, MetadataRequest,
    MetadataResponse, TopicName,
};
use kafka_protocol::protocol::{Request, StrBytes};
use mz_ore::task::AbortOnDropHandle;

use crate::admin::{
    AdminOptions, AlterConfigsResult, ConfigEntry, ConfigResource, ConfigResourceResult,
    ConfigSource, OwnedResourceSpecifier, TopicResult,
};
use crate::config::ClientSettings;
use crate::config::settings::DebugContext;
use crate::error::{KafkaError, KafkaResult};
use crate::internal::cluster::{Broker, Cluster, MetadataTopics};
use crate::internal::net::{NetError, RequestOptions};
use crate::internal::{protocol, runtime};
use crate::topic_partition_list::TopicPartitionList;
use crate::types::RDKafkaErrorCode;
use crate::util::{Deadline, Timeout};

#[cfg(test)]
mod tests;

/// The future of an admin operation running on the `kafka-io` runtime.
pub(crate) type AdminFuture<T> = AbortOnDropHandle<KafkaResult<T>>;

/// The admin engine of one admin client.
pub(crate) struct AdminInner {
    cluster: Arc<Cluster>,
}

/// The validated options of one operation, see the module documentation.
#[derive(Clone, Debug)]
pub(crate) struct OpOptions {
    request_timeout: Duration,
    /// Sent to the broker; `-1` and `0` mean not to wait.
    operation_timeout_ms: i32,
    validate_only: bool,
    broker_id: Option<i32>,
}

impl OpOptions {
    /// Validates `opts` like `rd_kafka_AdminOptions_set_*`, failing with
    /// [`KafkaError::AdminOpCreation`] and librdkafka's text for values out of
    /// range.
    pub(crate) fn new(opts: &AdminOptions, settings: &ClientSettings) -> KafkaResult<OpOptions> {
        let default_ms = duration_millis(settings.socket_timeout);
        let request_ms = opts
            .get_request_timeout()
            .map_or(default_ms, timeout_millis);
        check_range("request_timeout", request_ms, 0, 3_600_000)?;
        let operation_timeout_ms = opts
            .get_operation_timeout()
            .map_or(default_ms, timeout_millis);
        check_range("operation_timeout", operation_timeout_ms, -1, 3_600_000)?;
        if let Some(id) = opts.get_broker_id() {
            check_range("broker", id, 0, i32::MAX)?;
        }
        Ok(OpOptions {
            request_timeout: Duration::from_millis(u64::try_from(request_ms).expect("checked")),
            operation_timeout_ms,
            validate_only: opts.get_validate_only(),
            broker_id: opts.get_broker_id(),
        })
    }
}

/// librdkafka's representation of a timeout in milliseconds: `-1` for
/// [`Timeout::Never`].
fn timeout_millis(timeout: Timeout) -> i32 {
    match timeout {
        Timeout::After(d) => duration_millis(d),
        Timeout::Never => -1,
    }
}

fn duration_millis(d: Duration) -> i32 {
    i32::try_from(d.as_millis()).unwrap_or(i32::MAX)
}

/// `rd_kafka_confval_set_type`'s range check.
fn check_range(name: &str, value: i32, min: i32, max: i32) -> KafkaResult<()> {
    if value < min || value > max {
        return Err(KafkaError::AdminOpCreation(format!(
            "Invalid value type for \"{name}\": expecting integer in range {min}..{max}"
        )));
    }
    Ok(())
}

/// A topic to create.
#[derive(Clone, Debug)]
pub(crate) struct NewTopicSpec {
    pub name: String,
    /// `-1` for the broker default.
    pub num_partitions: i32,
    /// `-1` for the broker default, and when `assignments` is not empty.
    pub replication_factor: i32,
    /// The replicas of each partition, starting at partition 0.
    pub assignments: Vec<Vec<i32>>,
    pub configs: Vec<(String, String)>,
}

/// Partitions to add to a topic.
#[derive(Clone, Debug)]
pub(crate) struct NewPartitionsSpec {
    pub topic: String,
    /// The total number of partitions after the operation.
    pub total: i32,
    /// The replicas of each new partition.
    pub assignments: Option<Vec<Vec<i32>>>,
}

/// A config resource as sent on the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResourceKey {
    /// The `ConfigResource` type: 2 topic, 4 broker, 32 group.
    pub resource_type: i8,
    pub name: String,
}

pub(crate) const RESOURCE_TOPIC: i8 = 2;
pub(crate) const RESOURCE_BROKER: i8 = 4;
pub(crate) const RESOURCE_GROUP: i8 = 32;

impl ResourceKey {
    fn specifier(&self) -> OwnedResourceSpecifier {
        match self.resource_type {
            RESOURCE_BROKER => OwnedResourceSpecifier::Broker(
                self.name
                    .parse()
                    .expect("broker resources are built from an i32"),
            ),
            RESOURCE_GROUP => OwnedResourceSpecifier::Group(self.name.clone()),
            _ => OwnedResourceSpecifier::Topic(self.name.clone()),
        }
    }
}

/// The configs to set on a resource with AlterConfigs.
#[derive(Clone, Debug)]
pub(crate) struct AlterSpec {
    pub resource: ResourceKey,
    pub entries: Vec<(String, String)>,
}

/// Where a request goes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Controller,
    Broker(i32),
}

impl AdminInner {
    pub(crate) fn new(cluster: Arc<Cluster>) -> AdminInner {
        AdminInner { cluster }
    }

    pub(crate) fn settings(&self) -> &ClientSettings {
        self.cluster.settings()
    }

    /// Starts `run` on the `kafka-io` runtime with a fresh operation context.
    fn start<T, F, Fut>(&self, name: &'static str, opts: OpOptions, run: F) -> AdminFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Op) -> Fut,
        Fut: Future<Output = KafkaResult<T>> + Send + 'static,
    {
        let op = Op::new(Arc::clone(&self.cluster), opts);
        let shutdown = self.cluster.shutdown_token();
        let fut = run(op);
        runtime::spawn(name, async move {
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => Err(KafkaError::Canceled),
                result = fut => result,
            }
        })
        .abort_on_drop()
    }

    pub(crate) fn create_topics(
        &self,
        opts: OpOptions,
        topics: Vec<NewTopicSpec>,
    ) -> AdminFuture<Vec<TopicResult>> {
        self.start("kafka-admin-create-topics", opts, |op| async move {
            op.create_topics(topics).await
        })
    }

    pub(crate) fn delete_topics(
        &self,
        opts: OpOptions,
        topics: Vec<String>,
    ) -> AdminFuture<Vec<TopicResult>> {
        self.start("kafka-admin-delete-topics", opts, |op| async move {
            op.delete_topics(topics).await
        })
    }

    pub(crate) fn create_partitions(
        &self,
        opts: OpOptions,
        partitions: Vec<NewPartitionsSpec>,
    ) -> AdminFuture<Vec<TopicResult>> {
        self.start("kafka-admin-create-partitions", opts, |op| async move {
            op.create_partitions(partitions).await
        })
    }

    pub(crate) fn delete_records(
        &self,
        opts: OpOptions,
        offsets: TopicPartitionList,
    ) -> AdminFuture<TopicPartitionList> {
        self.start("kafka-admin-delete-records", opts, |op| async move {
            op.delete_records(offsets).await
        })
    }

    pub(crate) fn describe_configs(
        &self,
        opts: OpOptions,
        resources: Vec<ResourceKey>,
    ) -> AdminFuture<Vec<ConfigResourceResult>> {
        self.start("kafka-admin-describe-configs", opts, |op| async move {
            op.describe_configs(resources).await
        })
    }

    pub(crate) fn alter_configs(
        &self,
        opts: OpOptions,
        resources: Vec<AlterSpec>,
    ) -> AdminFuture<Vec<AlterConfigsResult>> {
        self.start("kafka-admin-alter-configs", opts, |op| async move {
            op.alter_configs(resources).await
        })
    }
}

/// One running admin operation.
struct Op {
    cluster: Arc<Cluster>,
    opts: OpOptions,
    /// The request timeout, started when the operation was created.
    deadline: Deadline,
}

fn admin_err(code: RDKafkaErrorCode) -> KafkaError {
    KafkaError::AdminOp(code)
}

fn str_bytes(s: &str) -> StrBytes {
    StrBytes::from_string(s.to_string())
}

fn topic_name(s: &str) -> TopicName {
    TopicName(str_bytes(s))
}

impl Op {
    fn new(cluster: Arc<Cluster>, opts: OpOptions) -> Op {
        let deadline = Deadline::new(opts.request_timeout);
        Op {
            cluster,
            opts,
            deadline,
        }
    }

    fn debug(&self, args: fmt::Arguments<'_>) {
        self.cluster
            .logger()
            .debug(DebugContext::Admin, "ADMIN", args);
    }

    /// The network timeout of one request: `socket.timeout.ms`, or the
    /// operation timeout plus one second if that is longer, as librdkafka's
    /// admin request builders set it.
    fn network_timeout(&self) -> Duration {
        let socket_timeout = self.cluster.settings().socket_timeout;
        let op_timeout =
            Duration::from_millis(u64::try_from(self.opts.operation_timeout_ms).unwrap_or(0));
        if op_timeout > socket_timeout {
            op_timeout + Duration::from_secs(1)
        } else {
            socket_timeout
        }
    }

    /// The error that ends a wait for a broker: `Canceled` once the client
    /// shuts down, `OperationTimedOut` once the request timeout passed.
    /// `None` to keep waiting.
    fn wait_failed(&self, e: &NetError) -> Option<KafkaError> {
        if e.code() == RDKafkaErrorCode::BrokerDestroy || self.cluster.is_shut_down() {
            Some(KafkaError::Canceled)
        } else if self.deadline.is_elapsed() {
            Some(admin_err(RDKafkaErrorCode::OperationTimedOut))
        } else {
            None
        }
    }

    /// Sleeps for `backoff`, at most until the deadline, then doubles it up
    /// to `retry.backoff.max.ms`.
    async fn backoff(&self, backoff: &mut Duration) {
        let mut sleep = *backoff;
        if let Some(remaining) = self.deadline.remaining_duration() {
            sleep = sleep.min(remaining);
        }
        tokio::time::sleep(sleep).await;
        *backoff = (*backoff * 2).min(self.cluster.settings().retry_backoff_max);
    }

    /// Waits until `target` has a usable connection, like librdkafka's admin
    /// worker in its `WAIT_BROKER` and `WAIT_CONTROLLER` states: lookups and
    /// connection failures are retried until the deadline. Returns the broker
    /// and the version of `api` negotiated with it.
    async fn ready(&self, target: Target, api: ApiKey) -> KafkaResult<(Arc<Broker>, i16)> {
        let mut backoff = self.cluster.settings().retry_backoff;
        loop {
            let broker = match self.lookup(target).await {
                Ok(b) => b,
                Err(e) => {
                    if let Some(err) = self.wait_failed(&e) {
                        return Err(err);
                    }
                    self.debug(format_args!("{target:?} not available: {e}"));
                    self.backoff(&mut backoff).await;
                    continue;
                }
            };
            match broker.connection(self.deadline).await {
                Ok(conn) => {
                    let version = conn
                        .api_versions()
                        .version(api)
                        .ok_or_else(|| admin_err(RDKafkaErrorCode::UnsupportedFeature))?;
                    return Ok((broker, version));
                }
                Err(e) => {
                    if let Some(err) = self.wait_failed(&e) {
                        return Err(err);
                    }
                    self.debug(format_args!("{} not connected: {e}", broker.name()));
                    self.backoff(&mut backoff).await;
                }
            }
        }
    }

    async fn lookup(&self, target: Target) -> Result<Arc<Broker>, NetError> {
        match target {
            Target::Controller => self.cluster.controller(self.deadline).await,
            Target::Broker(id) => {
                if let Some(b) = self.cluster.broker(id) {
                    return Ok(b);
                }
                // Not (yet) in the metadata. librdkafka waits for the broker
                // to appear; ask for the broker list instead of waiting for
                // the next periodic refresh.
                self.cluster
                    .fetch_metadata(MetadataTopics::None, self.deadline)
                    .await?;
                self.cluster.broker(id).ok_or_else(|| {
                    NetError::new(
                        RDKafkaErrorCode::BrokerNotAvailable,
                        format!("broker {id} not in metadata"),
                    )
                })
            }
        }
    }

    /// Sends a request to `broker`, waiting at most until the deadline.
    async fn send<R, F>(&self, broker: &Arc<Broker>, build: F) -> KafkaResult<R::Response>
    where
        R: Request + Any,
        R::Response: Any,
        F: FnOnce(i16) -> R,
    {
        let request =
            broker.request::<R, F>(build, RequestOptions::with_timeout(self.network_timeout()));
        let result = match self.deadline.instant() {
            Some(at) => tokio::time::timeout_at(at.into(), request)
                .await
                .map_err(|_| admin_err(RDKafkaErrorCode::OperationTimedOut))?,
            None => request.await,
        };
        result.map_err(|e| match e.code() {
            RDKafkaErrorCode::BrokerDestroy => KafkaError::Canceled,
            code => admin_err(code),
        })
    }

    /// Sends a request to `target` once it is ready. `check` may reject the
    /// request for the negotiated version before it is sent.
    async fn call<R, C, F>(&self, target: Target, check: C, build: F) -> KafkaResult<R::Response>
    where
        R: Request + Any,
        R::Response: Any,
        C: Fn(i16) -> KafkaResult<()>,
        F: Fn(i16) -> R,
    {
        let api = ApiKey::try_from(R::KEY).expect("known API key");
        let target = match self.opts.broker_id {
            Some(id) => Target::Broker(id),
            None => target,
        };
        loop {
            let (broker, version) = self.ready(target, api).await?;
            check(version)?;
            self.debug(format_args!(
                "sending {api:?} v{version} to {}",
                broker.name()
            ));
            match self.send::<R, _>(&broker, &build).await {
                // The broker left the cluster before the request was sent.
                Err(KafkaError::AdminOp(RDKafkaErrorCode::DestroyBroker))
                    if !self.deadline.is_elapsed() =>
                {
                    continue;
                }
                result => return result,
            }
        }
    }

    /// Runs a controller request with a result per topic, retrying topics
    /// that fail with `NOT_CONTROLLER` at the new controller until the
    /// deadline. `build` and `check` receive the version and the indexes of
    /// the topics to send; `parse` extracts `(topic, error code)` pairs.
    /// Results are in the order of `names`.
    async fn per_topic<R, C, B, P>(
        &self,
        names: &[String],
        check: C,
        build: B,
        parse: P,
    ) -> KafkaResult<Vec<TopicResult>>
    where
        R: Request + Any,
        R::Response: Any,
        C: Fn(i16, &[usize]) -> KafkaResult<()>,
        B: Fn(i16, &[usize]) -> R,
        P: Fn(R::Response) -> Vec<(String, i16)>,
    {
        let mut results: Vec<Option<Option<RDKafkaErrorCode>>> = vec![None; names.len()];
        let mut pending: Vec<usize> = (0..names.len()).collect();
        let mut backoff = self.cluster.settings().retry_backoff;
        loop {
            let response = self
                .call::<R, _, _>(
                    Target::Controller,
                    |v| check(v, &pending),
                    |v| build(v, &pending),
                )
                .await?;
            let mut codes = BTreeMap::new();
            for (name, code) in parse(response) {
                codes.entry(name).or_insert(code);
            }
            let mut retry = Vec::new();
            for &i in &pending {
                let Some(&code) = codes.get(&names[i]) else {
                    self.debug(format_args!("no result for topic {}", names[i]));
                    return Err(admin_err(RDKafkaErrorCode::BadMessage));
                };
                let code = match protocol::broker_error(code) {
                    // librdkafka hides this from non-blocking requests: the
                    // operation was started and is in progress.
                    Some(RDKafkaErrorCode::RequestTimedOut)
                        if self.opts.operation_timeout_ms <= 0 =>
                    {
                        None
                    }
                    code => code,
                };
                if code == Some(RDKafkaErrorCode::NotController) {
                    retry.push(i);
                }
                results[i] = Some(code);
            }
            if retry.is_empty() || self.deadline.is_elapsed() {
                break;
            }
            self.debug(format_args!(
                "{} topic(s) failed with NOT_CONTROLLER: refreshing the controller",
                retry.len()
            ));
            if let Err(e) = self
                .cluster
                .fetch_metadata(MetadataTopics::None, self.deadline)
                .await
            {
                if let Some(err) = self.wait_failed(&e) {
                    return Err(err);
                }
            }
            self.backoff(&mut backoff).await;
            pending = retry;
        }
        Ok(results
            .into_iter()
            .enumerate()
            .map(|(i, code)| {
                let name = names[i].clone();
                match code.expect("every topic has a result") {
                    None => Ok(name),
                    Some(code) => Err((name, code)),
                }
            })
            .collect())
    }

    async fn create_topics(&self, topics: Vec<NewTopicSpec>) -> KafkaResult<Vec<TopicResult>> {
        if topics.is_empty() {
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        let names: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
        let timeout_ms = self.opts.operation_timeout_ms;
        let validate_only = self.opts.validate_only;
        self.per_topic::<CreateTopicsRequest, _, _, _>(
            &names,
            |version, pending| {
                // KIP-464 broker defaults need CreateTopics v4 (Kafka 2.4).
                // librdkafka fails the request, and so do we.
                let needs_defaults = pending.iter().map(|&i| &topics[i]).any(|t| {
                    t.num_partitions == -1
                        || (t.replication_factor == -1 && t.assignments.is_empty())
                });
                if version < 4 && needs_defaults {
                    return Err(admin_err(RDKafkaErrorCode::UnsupportedFeature));
                }
                Ok(())
            },
            |_, pending| {
                CreateTopicsRequest::default()
                    .with_topics(
                        pending
                            .iter()
                            .map(|&i| creatable_topic(&topics[i]))
                            .collect(),
                    )
                    .with_timeout_ms(timeout_ms)
                    .with_validate_only(validate_only)
            },
            |response| {
                response
                    .topics
                    .into_iter()
                    .map(|t| (t.name.0.to_string(), t.error_code))
                    .collect()
            },
        )
        .await
    }

    async fn delete_topics(&self, names: Vec<String>) -> KafkaResult<Vec<TopicResult>> {
        if names.is_empty() {
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        let timeout_ms = self.opts.operation_timeout_ms;
        self.per_topic::<DeleteTopicsRequest, _, _, _>(
            &names,
            |_, _| Ok(()),
            |_, pending| {
                DeleteTopicsRequest::default()
                    .with_topic_names(pending.iter().map(|&i| topic_name(&names[i])).collect())
                    .with_timeout_ms(timeout_ms)
            },
            |response| {
                response
                    .responses
                    .into_iter()
                    .filter_map(|t| Some((t.name?.0.to_string(), t.error_code)))
                    .collect()
            },
        )
        .await
    }

    async fn create_partitions(
        &self,
        partitions: Vec<NewPartitionsSpec>,
    ) -> KafkaResult<Vec<TopicResult>> {
        if partitions.is_empty() {
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        let names: Vec<String> = partitions.iter().map(|p| p.topic.clone()).collect();
        let timeout_ms = self.opts.operation_timeout_ms;
        let validate_only = self.opts.validate_only;
        self.per_topic::<CreatePartitionsRequest, _, _, _>(
            &names,
            |_, _| Ok(()),
            |_, pending| {
                let topics = pending
                    .iter()
                    .map(|&i| {
                        let p = &partitions[i];
                        CreatePartitionsTopic::default()
                            .with_name(topic_name(&p.topic))
                            .with_count(p.total)
                            .with_assignments(p.assignments.as_ref().map(|a| {
                                a.iter()
                                    .map(|brokers| {
                                        CreatePartitionsAssignment::default()
                                            .with_broker_ids(broker_ids(brokers))
                                    })
                                    .collect()
                            }))
                    })
                    .collect();
                CreatePartitionsRequest::default()
                    .with_topics(topics)
                    .with_timeout_ms(timeout_ms)
                    .with_validate_only(validate_only)
            },
            |response| {
                response
                    .results
                    .into_iter()
                    .map(|t| (t.name.0.to_string(), t.error_code))
                    .collect()
            },
        )
        .await
    }

    /// The target of a config request: the broker of the broker resource if
    /// there is one, else the controller. Fails like librdkafka's
    /// `rd_kafka_ConfigResource_get_single_broker_id`.
    fn config_target<'a>(
        &self,
        resources: impl Iterator<Item = &'a ResourceKey>,
    ) -> KafkaResult<Target> {
        let mut target = Target::Controller;
        for r in resources.filter(|r| r.resource_type == RESOURCE_BROKER) {
            if target != Target::Controller {
                // "Only one ConfigResource of type BROKER is allowed per call"
                return Err(admin_err(RDKafkaErrorCode::Conflict));
            }
            match r.name.parse::<i32>() {
                Ok(id) if id >= 0 => target = Target::Broker(id),
                // "Expected an int32 broker_id for ConfigResource(type=BROKER, ..)"
                _ => return Err(admin_err(RDKafkaErrorCode::InvalidArgument)),
            }
        }
        Ok(target)
    }

    async fn describe_configs(
        &self,
        resources: Vec<ResourceKey>,
    ) -> KafkaResult<Vec<ConfigResourceResult>> {
        if resources.is_empty() {
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        let target = self.config_target(resources.iter())?;
        let response = self
            .call::<DescribeConfigsRequest, _, _>(
                target,
                |_| Ok(()),
                |_| {
                    DescribeConfigsRequest::default().with_resources(
                        resources
                            .iter()
                            .map(|r| {
                                DescribeConfigsResource::default()
                                    .with_resource_type(r.resource_type)
                                    .with_resource_name(str_bytes(&r.name))
                                    .with_configuration_keys(None)
                            })
                            .collect(),
                    )
                },
            )
            .await?;
        let mut by_key = BTreeMap::new();
        for result in response.results {
            by_key
                .entry((result.resource_type, result.resource_name.to_string()))
                .or_insert(result);
        }
        resources
            .iter()
            .map(|r| {
                // Cloned, not removed: a resource may be requested twice.
                let Some(result) = by_key.get(&(r.resource_type, r.name.clone())).cloned() else {
                    self.debug(format_args!("no result for config resource {r:?}"));
                    return Err(admin_err(RDKafkaErrorCode::BadMessage));
                };
                if let Some(code) = protocol::broker_error(result.error_code) {
                    return Ok(Err(code));
                }
                let entries = result
                    .configs
                    .into_iter()
                    .map(|c| {
                        let source = config_source(c.config_source);
                        ConfigEntry {
                            name: c.name.to_string(),
                            value: c.value.map(|v| v.to_string()),
                            is_default: source == ConfigSource::Default,
                            source,
                            is_read_only: c.read_only,
                            is_sensitive: c.is_sensitive,
                        }
                    })
                    .collect();
                Ok(Ok(ConfigResource {
                    specifier: r.specifier(),
                    entries,
                }))
            })
            .collect()
    }

    async fn alter_configs(
        &self,
        resources: Vec<AlterSpec>,
    ) -> KafkaResult<Vec<AlterConfigsResult>> {
        if resources.is_empty() {
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        let target = self.config_target(resources.iter().map(|r| &r.resource))?;
        let validate_only = self.opts.validate_only;
        let response = self
            .call::<AlterConfigsRequest, _, _>(
                target,
                |_| Ok(()),
                |_| {
                    AlterConfigsRequest::default()
                        .with_resources(
                            resources
                                .iter()
                                .map(|r| {
                                    AlterConfigsResource::default()
                                        .with_resource_type(r.resource.resource_type)
                                        .with_resource_name(str_bytes(&r.resource.name))
                                        .with_configs(
                                            r.entries
                                                .iter()
                                                .map(|(k, v)| {
                                                    AlterableConfig::default()
                                                        .with_name(str_bytes(k))
                                                        .with_value(Some(str_bytes(v)))
                                                })
                                                .collect(),
                                        )
                                })
                                .collect(),
                        )
                        .with_validate_only(validate_only)
                },
            )
            .await?;
        let mut by_key = BTreeMap::new();
        for result in response.responses {
            by_key
                .entry((result.resource_type, result.resource_name.to_string()))
                .or_insert(result.error_code);
        }
        resources
            .iter()
            .map(|r| {
                let key = &r.resource;
                let Some(&code) = by_key.get(&(key.resource_type, key.name.clone())) else {
                    self.debug(format_args!("no result for config resource {key:?}"));
                    return Err(admin_err(RDKafkaErrorCode::BadMessage));
                };
                Ok(match protocol::broker_error(code) {
                    None => Ok(key.specifier()),
                    Some(code) => Err((key.specifier(), code)),
                })
            })
            .collect()
    }

    /// DeleteRecords, as librdkafka's fanout: look up the partition leaders,
    /// then send one request per leader. The result is a copy of `offsets`
    /// sorted by topic and partition, whose elements carry the new low
    /// watermark as offset, or an error. Elements that were never sent keep
    /// their offset and fail with `Noop`; a failed leader request fails all
    /// of its elements.
    async fn delete_records(&self, offsets: TopicPartitionList) -> KafkaResult<TopicPartitionList> {
        if offsets.count() == 0 {
            // "No records to delete"
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        // librdkafka sorts the list while checking it for duplicates, and
        // returns it in that order.
        let mut sorted: Vec<(String, i32, i64, String)> = offsets
            .elements()
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let raw_offset = offsets.raw_offset_at(i);
                (
                    e.topic().to_string(),
                    e.partition(),
                    raw_offset,
                    e.metadata().to_string(),
                )
            })
            .collect();
        sorted.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
        if sorted
            .windows(2)
            .any(|w| (&w[0].0, w[0].1) == (&w[1].0, w[1].1))
        {
            // "Duplicate partitions not allowed"
            return Err(admin_err(RDKafkaErrorCode::InvalidArgument));
        }
        let mut result = TopicPartitionList::with_capacity(sorted.len());
        let mut elements = Vec::with_capacity(sorted.len());
        for (topic, partition, raw_offset, metadata) in sorted {
            result.push_entry(
                &topic,
                partition,
                raw_offset,
                metadata,
                RDKafkaErrorCode::Noop,
            );
            elements.push((topic, partition));
        }

        // Leader lookup. Indexes of the elements per leader.
        let mut by_leader: BTreeMap<i32, Vec<usize>> = BTreeMap::new();
        for (i, leader) in self
            .partition_leaders(&elements)
            .await?
            .into_iter()
            .enumerate()
        {
            match leader {
                Ok(id) => by_leader.entry(id).or_default().push(i),
                Err(code) => result.set_error_at(i, code),
            }
        }
        if by_leader.is_empty() {
            // "Failed to query partition leaders: No leaders found"
            return Err(admin_err(RDKafkaErrorCode::NoEnt));
        }

        // librdkafka's per-leader requests are new admin ops, each with the
        // full request timeout.
        let requests = by_leader.into_iter().map(|(leader, indexes)| {
            let op = Op::new(Arc::clone(&self.cluster), self.opts.clone());
            let elements = &elements;
            let offsets: Vec<(usize, i64)> = indexes
                .iter()
                .map(|&i| (i, result.raw_offset_at(i)))
                .collect();
            async move {
                let response = op
                    .call::<DeleteRecordsRequest, _, _>(
                        Target::Broker(leader),
                        |_| Ok(()),
                        |_| {
                            let mut topics: BTreeMap<&str, Vec<DeleteRecordsPartition>> =
                                BTreeMap::new();
                            for &(i, offset) in &offsets {
                                let (topic, partition) = &elements[i];
                                topics.entry(topic).or_default().push(
                                    DeleteRecordsPartition::default()
                                        .with_partition_index(*partition)
                                        .with_offset(offset),
                                );
                            }
                            DeleteRecordsRequest::default()
                                .with_topics(
                                    topics
                                        .into_iter()
                                        .map(|(topic, partitions)| {
                                            DeleteRecordsTopic::default()
                                                .with_name(topic_name(topic))
                                                .with_partitions(partitions)
                                        })
                                        .collect(),
                                )
                                .with_timeout_ms(op.opts.operation_timeout_ms)
                        },
                    )
                    .await;
                (indexes, response)
            }
        });
        for (indexes, response) in futures::future::join_all(requests).await {
            let response = match response {
                Ok(response) => response,
                Err(KafkaError::AdminOp(code)) => {
                    for i in indexes {
                        result.set_error_at(i, code);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            for topic in response.topics {
                for p in topic.partitions {
                    let key = (topic.name.0.to_string(), p.partition_index);
                    match elements.iter().position(|e| *e == key) {
                        Some(i) => {
                            result.set_raw_offset_at(i, p.low_watermark);
                            result.set_error_at(
                                i,
                                protocol::broker_error(p.error_code)
                                    .unwrap_or(RDKafkaErrorCode::NoError),
                            );
                        }
                        None => self.cluster.logger().log_fmt(
                            crate::config::RDKafkaLogLevel::Warning,
                            "DELETERECORDS",
                            format_args!(
                                "DeleteRecords response contains unexpected {} [{}] which was \
                                 not in the request list: ignored",
                                key.0, key.1
                            ),
                        ),
                    }
                }
            }
        }
        Ok(result)
    }

    /// The leader of each `(topic, partition)`, or the error that keeps the
    /// partition from being sent, following librdkafka's
    /// `rd_kafka_topic_partition_list_get_leaders`: a topic error other than
    /// `LEADER_NOT_AVAILABLE`, `UnknownPartition` for a partition the topic
    /// does not have, and `LEADER_NOT_AVAILABLE` (or the topic error) for a
    /// partition without a leader are final. Topics that the broker does not
    /// report or that have no partitions yet are asked for again until the
    /// deadline, which fails the operation with `OperationTimedOut`.
    async fn partition_leaders(
        &self,
        elements: &[(String, i32)],
    ) -> KafkaResult<Vec<Result<i32, RDKafkaErrorCode>>> {
        let mut leaders: Vec<Option<Result<i32, RDKafkaErrorCode>>> = vec![None; elements.len()];
        let mut backoff = self.cluster.settings().retry_backoff;
        loop {
            let topics: Vec<String> = elements
                .iter()
                .enumerate()
                .filter(|(i, _)| leaders[*i].is_none())
                .map(|(_, (t, _))| t.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let response = self.topic_metadata(&topics).await?;
            for (i, (topic, partition)) in elements.iter().enumerate() {
                if leaders[i].is_none() {
                    leaders[i] = self.classify_partition(&response, topic, *partition);
                }
            }
            if leaders.iter().all(Option::is_some) {
                return Ok(leaders.into_iter().map(|l| l.expect("checked")).collect());
            }
            if self.deadline.is_elapsed() {
                return Err(admin_err(RDKafkaErrorCode::OperationTimedOut));
            }
            self.backoff(&mut backoff).await;
        }
    }

    fn classify_partition(
        &self,
        response: &MetadataResponse,
        topic: &str,
        partition: i32,
    ) -> Option<Result<i32, RDKafkaErrorCode>> {
        let t = response
            .topics
            .iter()
            .find(|t| t.name.as_ref().is_some_and(|n| n.0.as_str() == topic))?;
        let topic_error = protocol::broker_error(t.error_code);
        if let Some(code) = topic_error {
            if code != RDKafkaErrorCode::LeaderNotAvailable {
                return Some(Err(code));
            }
        }
        let Some(p) = t.partitions.iter().find(|p| p.partition_index == partition) else {
            return (!t.partitions.is_empty()).then_some(Err(RDKafkaErrorCode::UnknownPartition));
        };
        let leader = p.leader_id.0;
        if leader < 0 || self.cluster.broker(leader).is_none() {
            return Some(Err(
                topic_error.unwrap_or(RDKafkaErrorCode::LeaderNotAvailable)
            ));
        }
        Some(Ok(leader))
    }

    /// Requests the metadata of `topics` without auto-creating them, as
    /// librdkafka's leader queries do whatever `allow.auto.create.topics`
    /// says. Retries failures until the deadline.
    async fn topic_metadata(&self, topics: &[String]) -> KafkaResult<MetadataResponse> {
        let mut backoff = self.cluster.settings().retry_backoff;
        loop {
            let broker = match self.cluster.any_broker(self.deadline).await {
                Ok(b) => b,
                Err(e) => match self.wait_failed(&e) {
                    Some(err) => return Err(err),
                    None => {
                        self.backoff(&mut backoff).await;
                        continue;
                    }
                },
            };
            let response = self
                .send::<MetadataRequest, _>(&broker, |version| {
                    let mut request = MetadataRequest::default().with_topics(Some(
                        topics
                            .iter()
                            .map(|t| MetadataRequestTopic::default().with_name(Some(topic_name(t))))
                            .collect(),
                    ));
                    if version >= 4 {
                        request.allow_auto_topic_creation = false;
                    }
                    request
                })
                .await;
            match response {
                Ok(response) => return Ok(response),
                Err(KafkaError::AdminOp(code)) if !self.deadline.is_elapsed() => {
                    self.debug(format_args!("metadata request failed: {code:?}"));
                    self.backoff(&mut backoff).await;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn broker_ids(ids: &[i32]) -> Vec<BrokerId> {
    ids.iter().copied().map(BrokerId).collect()
}

fn creatable_topic(t: &NewTopicSpec) -> CreatableTopic {
    let mut topic = CreatableTopic::default()
        .with_name(topic_name(&t.name))
        .with_configs(
            t.configs
                .iter()
                .map(|(k, v)| {
                    CreatableTopicConfig::default()
                        .with_name(str_bytes(k))
                        .with_value(Some(str_bytes(v)))
                })
                .collect(),
        );
    if t.assignments.is_empty() {
        topic.num_partitions = t.num_partitions;
        // Checked to be in -1..=10000 by the facade.
        topic.replication_factor = i16::try_from(t.replication_factor).expect("checked");
    } else {
        // Both must be -1 when a replica assignment is sent.
        topic.num_partitions = -1;
        topic.replication_factor = -1;
        topic.assignments = t
            .assignments
            .iter()
            .enumerate()
            .map(|(partition, brokers)| {
                CreatableReplicaAssignment::default()
                    .with_partition_index(i32::try_from(partition).expect("checked"))
                    .with_broker_ids(broker_ids(brokers))
            })
            .collect();
    }
    topic
}

/// Maps a DescribeConfigs `ConfigSource`. Sources rust-rdkafka does not know
/// (broker logger, client metrics and group configs) map to `Unknown`.
fn config_source(source: i8) -> ConfigSource {
    match source {
        1 => ConfigSource::DynamicTopic,
        2 => ConfigSource::DynamicBroker,
        3 => ConfigSource::DynamicDefaultBroker,
        4 => ConfigSource::StaticBroker,
        5 => ConfigSource::Default,
        _ => ConfigSource::Unknown,
    }
}
