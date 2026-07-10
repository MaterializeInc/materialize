// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use mz_aws_glue_schema_registry::{
    Client as GlueClient, Compatibility as GlueCompatibility, CreateSchemaError, DataFormat,
    GetSchemaByDefinitionError, GetSchemaVersionError, RegisterSchemaVersionError,
    RegisteredSchemaVersion, SchemaVersionLifecycleStatus,
};
use mz_ccsr::GetSubjectConfigError;
use mz_kafka_util::admin::EnsureTopicConfig;
use mz_kafka_util::client::MzClientContext;
use mz_ore::collections::CollectionExt;
use mz_ore::future::{InTask, OreFutureExt};
use mz_ore::retry::{Retry, RetryResult};
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::connections::KafkaTopicOptions;
use mz_storage_types::errors::ContextCreationErrorExt;
use mz_storage_types::sinks::KafkaSinkConnection;
use rdkafka::ClientContext;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication};
use tracing::warn;
use uuid::Uuid;

pub mod progress_key {
    use std::fmt;

    use mz_repr::GlobalId;
    use rdkafka::message::ToBytes;

    /// A key identifying a given sink within a progress topic.
    #[derive(Debug, Clone)]
    pub struct ProgressKey(String);

    impl ProgressKey {
        /// Constructs a progress key for the sink with the specified ID.
        pub fn new(sink_id: GlobalId) -> ProgressKey {
            ProgressKey(format!("mz-sink-{sink_id}"))
        }
    }

    impl ToBytes for ProgressKey {
        fn to_bytes(&self) -> &[u8] {
            self.0.as_bytes()
        }
    }

    impl fmt::Display for ProgressKey {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            self.0.fmt(f)
        }
    }
}

struct TopicConfigs {
    partition_count: i32,
    replication_factor: i32,
}

async fn discover_topic_configs<C: ClientContext>(
    client: &AdminClient<C>,
    topic: &str,
    fetch_timeout: Duration,
) -> Result<TopicConfigs, anyhow::Error> {
    let mut partition_count = -1;
    let mut replication_factor = -1;

    let metadata = client
        .inner()
        .fetch_metadata(None, fetch_timeout)
        .with_context(|| {
            format!(
                "error fetching metadata when creating new topic {} for sink",
                topic
            )
        })?;

    if metadata.brokers().len() == 0 {
        Err(anyhow!("zero brokers discovered in metadata request"))?;
    }

    let broker = metadata.brokers()[0].id();
    let configs = client
        .describe_configs(
            &[ResourceSpecifier::Broker(broker)],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        )
        .await
        .with_context(|| {
            format!(
                "error fetching configuration from broker {} when creating new topic {} for sink",
                broker, topic
            )
        })?;

    if configs.len() != 1 {
        Err(anyhow!(
            "error creating topic {} for sink: broker {} returned {} config results, but one was expected",
            topic,
            broker,
            configs.len()
        ))?;
    }

    let config = configs.into_element().map_err(|e| {
        anyhow!(
            "error reading broker configuration when creating topic {} for sink: {}",
            topic,
            e
        )
    })?;

    if config.entries.is_empty() {
        bail!("read empty cluster configuration; do we have DescribeConfigs permissions?")
    }

    for entry in config.entries {
        if entry.name == "num.partitions" && partition_count == -1 {
            if let Some(s) = entry.value {
                partition_count = s.parse::<i32>().with_context(|| {
                    format!(
                        "default partition count {} cannot be parsed into an integer",
                        s
                    )
                })?;
            }
        } else if entry.name == "default.replication.factor" && replication_factor == -1 {
            if let Some(s) = entry.value {
                replication_factor = s.parse::<i32>().with_context(|| {
                    format!(
                        "default replication factor {} cannot be parsed into an integer",
                        s
                    )
                })?;
            }
        }
    }

    Ok(TopicConfigs {
        partition_count,
        replication_factor,
    })
}

/// Ensures that the named Kafka topic exists.
///
/// If the topic does not exist, the function creates the topic with the
/// provided `config`. Note that if the topic already exists, the function does
/// *not* verify that the topic's configuration matches `config`.
///
/// Returns a boolean indicating whether the topic already existed.
pub async fn ensure_kafka_topic(
    connection: &KafkaSinkConnection,
    storage_configuration: &StorageConfiguration,
    topic: &str,
    KafkaTopicOptions {
        partition_count,
        replication_factor,
        topic_config,
    }: &KafkaTopicOptions,
    ensure_topic_config: EnsureTopicConfig,
) -> Result<bool, anyhow::Error> {
    let client: AdminClient<_> = connection
        .connection
        .create_with_context(
            storage_configuration,
            MzClientContext::default(),
            &BTreeMap::new(),
            // Only called from `mz_storage`.
            InTask::Yes,
        )
        .await
        .add_context("creating admin client failed")?;
    let mut partition_count = partition_count.map(|f| *f);
    let mut replication_factor = replication_factor.map(|f| *f);
    // If either partition count or replication factor should be defaulted to the broker's config
    // (signaled by a value of None), explicitly poll the broker to discover the defaults.
    // Newer versions of Kafka can instead send create topic requests with -1 and have this happen
    // behind the scenes, but this is unsupported and will result in errors on pre-2.4 Kafka.
    if partition_count.is_none() || replication_factor.is_none() {
        let fetch_timeout = storage_configuration
            .parameters
            .kafka_timeout_config
            .fetch_metadata_timeout;
        match discover_topic_configs(&client, topic, fetch_timeout).await {
            Ok(configs) => {
                if partition_count.is_none() {
                    partition_count = Some(configs.partition_count);
                }
                if replication_factor.is_none() {
                    replication_factor = Some(configs.replication_factor);
                }
            }
            Err(e) => {
                // Recent versions of Kafka can handle an explicit -1 config, so use this instead
                // and the request will probably still succeed. Logging anyways for visibility.
                warn!("Failed to discover default values for topic configs: {e}");
                if partition_count.is_none() {
                    partition_count = Some(-1);
                }
                if replication_factor.is_none() {
                    replication_factor = Some(-1);
                }
            }
        };
    }

    let mut kafka_topic = NewTopic::new(
        topic,
        partition_count.expect("always set above"),
        TopicReplication::Fixed(replication_factor.expect("always set above")),
    );

    for (key, value) in topic_config {
        kafka_topic = kafka_topic.set(key, value);
    }

    mz_kafka_util::admin::ensure_topic(
        &client,
        &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        &kafka_topic,
        ensure_topic_config,
    )
    .await
    .with_context(|| format!("Error creating topic {} for sink", topic))
}

/// Publish a schema for a given subject, and set
/// compatibility levels for the schema if applicable.
///
/// TODO(benesch): do we need to delete the Kafka topic if publishing the
/// schema fails?
pub async fn publish_kafka_schema(
    ccsr: mz_ccsr::Client,
    subject: String,
    schema: String,
    schema_type: mz_ccsr::SchemaType,
    compatibility_level: Option<mz_ccsr::CompatibilityLevel>,
) -> Result<i32, anyhow::Error> {
    if let Some(compatibility_level) = compatibility_level {
        let ccsr = ccsr.clone();
        let subject = subject.clone();
        async move {
            // Only update the compatibility level if it's not already set to something.
            match ccsr.get_subject_config(&subject).await {
                Ok(config) => {
                    if config.compatibility_level != compatibility_level {
                        tracing::debug!(
                            "compatibility level '{}' does not match intended '{}'",
                            config.compatibility_level,
                            compatibility_level
                        );
                    }
                    Ok(())
                }
                Err(GetSubjectConfigError::SubjectCompatibilityLevelNotSet)
                | Err(GetSubjectConfigError::SubjectNotFound) => ccsr
                    .set_subject_compatibility_level(&subject, compatibility_level)
                    .await
                    .map_err(anyhow::Error::from),
                Err(e) => Err(e.into()),
            }
        }
        .run_in_task(|| "set_compatibility_level".to_string())
        .await
        .context("unable to update schema compatibility level in kafka sink")?;
    }

    let schema_id = async move {
        ccsr.publish_schema(&subject, &schema, schema_type, &[])
            .await
    }
    .run_in_task(|| "publish_kafka_schema".to_string())
    .await
    .context("unable to publish schema to registry in kafka sink")?;

    Ok(schema_id)
}

/// Register `schema` for a sink in an AWS Glue Schema Registry, returning the
/// schema-version UUID to frame records with.
///
/// Reuses an already-registered definition so a sink restart does not create a
/// duplicate version. On first publish the schema is created with
/// `compatibility` as its evolution policy, defaulting to Glue's `BACKWARD` when
/// unset. For an existing schema the compatibility is only read and warned on
/// when it differs, never overwritten: Glue fixes compatibility at creation
/// time, and the sink must not silently change a policy it may share with other
/// producers.
pub async fn publish_glue_schema(
    client: GlueClient,
    registry_name: String,
    schema_name: String,
    schema: String,
    compatibility: Option<GlueCompatibility>,
) -> Result<Uuid, anyhow::Error> {
    async move {
        // Reuse: if this exact definition is already registered, we're done.
        match client
            .get_schema_by_definition(&registry_name, &schema_name, &schema)
            .await
        {
            // Glue matches by definition only, so this can return a version
            // that failed its compatibility check or is mid-deletion. Such a
            // version is unusable: fall through and register, which surfaces
            // a clear error if Glue still resolves the definition to it.
            Ok(RegisteredSchemaVersion {
                lifecycle_status:
                    Some(
                        SchemaVersionLifecycleStatus::Failure
                        | SchemaVersionLifecycleStatus::Deleting,
                    ),
                ..
            }) => {}
            Ok(registered) => {
                warn_on_glue_compatibility_mismatch(
                    &client,
                    &registry_name,
                    &schema_name,
                    compatibility.as_ref(),
                )
                .await;
                return await_glue_schema_version_available(&client, registered).await;
            }
            Err(GetSchemaByDefinitionError::NotFound) => {}
            Err(e) => return Err(e).context("looking up Glue schema by definition"),
        }

        // Not yet registered. Add a version if the schema exists, else create it.
        let registered = match client
            .register_schema_version(&registry_name, &schema_name, &schema)
            .await
        {
            Ok(registered) => {
                warn_on_glue_compatibility_mismatch(
                    &client,
                    &registry_name,
                    &schema_name,
                    compatibility.as_ref(),
                )
                .await;
                registered
            }
            Err(RegisterSchemaVersionError::SchemaNotFound) => {
                // First publish: create the schema and set its compatibility.
                let compatibility = compatibility.unwrap_or(GlueCompatibility::Backward);
                match client
                    .create_schema(
                        &registry_name,
                        &schema_name,
                        DataFormat::Avro,
                        compatibility,
                        &schema,
                    )
                    .await
                {
                    Ok(registered) => registered,
                    // Lost a race with another writer that created the schema
                    // between our register and create. Retry the register.
                    Err(CreateSchemaError::AlreadyExists) => client
                        .register_schema_version(&registry_name, &schema_name, &schema)
                        .await
                        .context("registering Glue schema version after create race")?,
                    Err(e) => return Err(e).context("creating Glue schema"),
                }
            }
            Err(e) => return Err(e).context("registering Glue schema version"),
        };
        await_glue_schema_version_available(&client, registered).await
    }
    .run_in_task(|| "publish_glue_schema".to_string())
    .await
    .context("unable to publish schema to registry in kafka sink")
}

/// Wait until Glue reports the schema version `registered` as `Available`,
/// returning its id.
///
/// Glue runs compatibility checks asynchronously: a freshly registered version
/// is `Pending` and only later transitions to `Available` or `Failure`.
/// Records must not be framed with a version id until it is `Available`,
/// otherwise a version that fails its check leaves the topic holding ids that
/// consumers cannot resolve. Errors if the version resolves to `Failure` or
/// `Deleting`, or does not become `Available` within the polling deadline.
async fn await_glue_schema_version_available(
    client: &GlueClient,
    registered: RegisteredSchemaVersion,
) -> Result<Uuid, anyhow::Error> {
    let id = registered.id;
    // The status from the write response answers the first poll without
    // another API call. Later polls re-fetch.
    let mut write_status = registered.lifecycle_status;

    // The timeouts are arbitrary, but are roughly similar to default AWS SDK behavior.
    Retry::default()
        .initial_backoff(Duration::from_millis(200))
        .clamp_backoff(Duration::from_secs(2))
        .max_duration(Duration::from_secs(30))
        .retry_async(|_| {
            let known = write_status.take();
            async move {
                let status = match known {
                    Some(status) => Some(status),
                    None => match client.get_schema_version_by_id(id).await {
                        Ok(version) => version.lifecycle_status,
                        Err(GetSchemaVersionError::NotFound) => {
                            return RetryResult::FatalErr(anyhow!(
                                "Glue schema version {id} disappeared while waiting for it \
                                 to become available"
                            ));
                        }
                        Err(e) => {
                            return RetryResult::RetryableErr(
                                anyhow::Error::new(e)
                                    .context(format!("polling status of Glue schema version {id}")),
                            );
                        }
                    },
                };
                match status {
                    Some(SchemaVersionLifecycleStatus::Available) => RetryResult::Ok(id),
                    Some(SchemaVersionLifecycleStatus::Failure) => RetryResult::FatalErr(anyhow!(
                        "Glue schema version {id} failed its compatibility check"
                    )),
                    Some(SchemaVersionLifecycleStatus::Deleting) => {
                        RetryResult::FatalErr(anyhow!("Glue schema version {id} is being deleted"))
                    }
                    status @ (Some(
                        SchemaVersionLifecycleStatus::Pending
                        | SchemaVersionLifecycleStatus::Unknown(_),
                    )
                    | None) => RetryResult::RetryableErr(anyhow!(
                        "Glue schema version {id} is not yet available (status: {status:?})"
                    )),
                }
            }
        })
        .await
}

/// Warn if the schema's existing compatibility differs from `desired`.
///
/// Best-effort and advisory only: a failed read is logged and ignored. The sink
/// never changes an existing schema's compatibility (see [`publish_glue_schema`]),
/// so a mismatch is surfaced for the operator, not acted on.
async fn warn_on_glue_compatibility_mismatch(
    client: &GlueClient,
    registry_name: &str,
    schema_name: &str,
    desired: Option<&GlueCompatibility>,
) {
    let Some(desired) = desired else { return };
    match client.get_schema(registry_name, schema_name).await {
        Ok(schema) => {
            if schema.compatibility.as_ref() != Some(desired) {
                warn!(
                    "Glue schema {schema_name:?} has compatibility {:?}, which does not match \
                     the intended {desired:?}; leaving it unchanged",
                    schema.compatibility
                );
            }
        }
        Err(e) => warn!(
            "unable to read Glue schema {schema_name:?} compatibility to check for a mismatch: {e}"
        ),
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_glue::operation::get_schema_by_definition::{
        GetSchemaByDefinitionError as SdkGetSchemaByDefinitionError, GetSchemaByDefinitionOutput,
    };
    use aws_sdk_glue::operation::get_schema_version::GetSchemaVersionOutput;
    use aws_sdk_glue::operation::register_schema_version::RegisterSchemaVersionOutput;
    use aws_sdk_glue::types::SchemaVersionStatus;
    use aws_sdk_glue::types::error::EntityNotFoundException;
    use aws_smithy_mocks::{Rule, RuleMode, mock, mock_client};

    use super::*;

    const PUBLISHED_ID: &str = "12345678-1234-5678-1234-567812345678";
    const REUSED_ID: &str = "87654321-4321-8765-4321-876543218765";

    /// A mocked client that panics on any request no rule covers, so each test
    /// also asserts which Glue APIs the publish path may touch.
    fn glue_client(rules: &[&Rule]) -> GlueClient {
        GlueClient::from_sdk_client(mock_client!(aws_sdk_glue, RuleMode::MatchAny, rules))
    }

    async fn publish(client: GlueClient) -> Result<Uuid, anyhow::Error> {
        publish_glue_schema(
            client,
            "registry".to_string(),
            "schema".to_string(),
            "{}".to_string(),
            None,
        )
        .await
    }

    fn definition_not_found() -> Rule {
        mock!(aws_sdk_glue::Client::get_schema_by_definition).then_error(|| {
            SdkGetSchemaByDefinitionError::EntityNotFoundException(
                EntityNotFoundException::builder().build(),
            )
        })
    }

    fn register_returns(status: SchemaVersionStatus) -> Rule {
        mock!(aws_sdk_glue::Client::register_schema_version).then_output(move || {
            RegisterSchemaVersionOutput::builder()
                .schema_version_id(PUBLISHED_ID)
                .status(status.clone())
                .build()
        })
    }

    /// A version that registers as `Pending` is polled until Glue reports it
    /// `Available`.
    #[mz_ore::test(tokio::test)]
    async fn publish_glue_schema_waits_for_pending_version() {
        let lookup = definition_not_found();
        let register = register_returns(SchemaVersionStatus::Pending);
        let poll = mock!(aws_sdk_glue::Client::get_schema_version)
            .sequence()
            .output(|| {
                GetSchemaVersionOutput::builder()
                    .schema_version_id(PUBLISHED_ID)
                    .status(SchemaVersionStatus::Pending)
                    .build()
            })
            .output(|| {
                GetSchemaVersionOutput::builder()
                    .schema_version_id(PUBLISHED_ID)
                    .status(SchemaVersionStatus::Available)
                    .build()
            })
            .build();

        let id = publish(glue_client(&[&lookup, &register, &poll]))
            .await
            .expect("pending version becomes available");
        assert_eq!(id.to_string(), PUBLISHED_ID);
        assert_eq!(poll.num_calls(), 2);
    }

    /// A version whose asynchronous compatibility check fails surfaces an
    /// error instead of an id that consumers could never resolve.
    #[mz_ore::test(tokio::test)]
    async fn publish_glue_schema_errors_on_failed_compatibility_check() {
        let lookup = definition_not_found();
        let register = register_returns(SchemaVersionStatus::Pending);
        let poll = mock!(aws_sdk_glue::Client::get_schema_version).then_output(|| {
            GetSchemaVersionOutput::builder()
                .schema_version_id(PUBLISHED_ID)
                .status(SchemaVersionStatus::Failure)
                .build()
        });

        let err = publish(glue_client(&[&lookup, &register, &poll]))
            .await
            .expect_err("failed version must not publish");
        assert!(
            format!("{err:#}").contains("failed its compatibility check"),
            "unexpected error: {err:#}"
        );
    }

    /// A definition match in `Available` state is reused as-is, with no
    /// registration and no status polling.
    #[mz_ore::test(tokio::test)]
    async fn publish_glue_schema_reuses_available_definition_match() {
        let lookup = mock!(aws_sdk_glue::Client::get_schema_by_definition).then_output(|| {
            GetSchemaByDefinitionOutput::builder()
                .schema_version_id(REUSED_ID)
                .status(SchemaVersionStatus::Available)
                .build()
        });

        let id = publish(glue_client(&[&lookup]))
            .await
            .expect("available version is reused");
        assert_eq!(id.to_string(), REUSED_ID);
    }

    /// A definition match whose version failed its compatibility check is not
    /// reused: the definition is registered anew.
    #[mz_ore::test(tokio::test)]
    async fn publish_glue_schema_skips_failed_definition_match() {
        let lookup = mock!(aws_sdk_glue::Client::get_schema_by_definition).then_output(|| {
            GetSchemaByDefinitionOutput::builder()
                .schema_version_id(REUSED_ID)
                .status(SchemaVersionStatus::Failure)
                .build()
        });
        let register = register_returns(SchemaVersionStatus::Available);

        let id = publish(glue_client(&[&lookup, &register]))
            .await
            .expect("failed match falls through to registration");
        assert_eq!(id.to_string(), PUBLISHED_ID);
        assert_eq!(register.num_calls(), 1);
    }
}
