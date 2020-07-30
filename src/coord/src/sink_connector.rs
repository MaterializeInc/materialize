// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::OpenOptions;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

use dataflow_types::{
    AvroOcfSinkConnector, AvroOcfSinkConnectorBuilder, KafkaSinkConnector,
    KafkaSinkConnectorBuilder, KafkaSinkConsistencyConnector, SinkConnector, SinkConnectorBuilder,
    Timestamp,
};
use expr::GlobalId;
use ore::collections::CollectionExt;
use timely::progress::Antichain;

pub async fn build(
    builder: SinkConnectorBuilder,
    with_snapshot: bool,
    frontier: Antichain<Timestamp>,
    id: GlobalId,
) -> Result<SinkConnector, anyhow::Error> {
    match builder {
        SinkConnectorBuilder::Kafka(k) => build_kafka(k, with_snapshot, frontier, id).await,
        SinkConnectorBuilder::AvroOcf(a) => build_avro_ocf(a, with_snapshot, frontier, id),
    }
}

async fn register_kafka_topic(
    client: &AdminClient<DefaultClientContext>,
    topic: &str,
    replication_factor: i32,
    ccsr: &ccsr::Client,
    schema: &str,
) -> Result<i32, anyhow::Error> {
    let res = client
        .create_topics(
            &[NewTopic::new(
                &topic,
                1,
                TopicReplication::Fixed(replication_factor),
            )],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        )
        .await
        .with_context(|| format!("error creating new topic {} for sink", topic))?;
    if res.len() != 1 {
        bail!(
            "error creating topic {} for sink: \
             kafka topic creation returned {} results, but exactly one result was expected",
            topic,
            res.len()
        );
    }
    res.into_element()
        .map_err(|(_, e)| anyhow!("error creating topic {} for sink: {}", topic, e))?;

    // Publish value schema for the topic.
    //
    // TODO(benesch): do we need to delete the Kafka topic if publishing the
    // schema fails?
    // TODO(sploiselle): support SSL auth'ed sinks
    let schema_id = ccsr
        .publish_schema(&format!("{}-value", topic), schema)
        .await
        .context("unable to publish schema to registry in kafka sink")?;

    Ok(schema_id)
}

async fn build_kafka(
    builder: KafkaSinkConnectorBuilder,
    with_snapshot: bool,
    frontier: Antichain<Timestamp>,
    id: GlobalId,
) -> Result<SinkConnector, anyhow::Error> {
    let topic = format!("{}-{}-{}", builder.topic_prefix, id, builder.topic_suffix);

    // Create Kafka topic with single partition.
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &builder.broker_addr.to_string());
    let client = config
        .create::<AdminClient<_>>()
        .expect("creating admin client failed");
    let ccsr = ccsr::ClientConfig::new(builder.schema_registry_url).build();

    let schema_id = register_kafka_topic(
        &client,
        &topic,
        builder.replication_factor as i32,
        &ccsr,
        &builder.value_schema,
    )
    .await
    .context("error registering kafka topic for sink")?;

    let consistency = if let Some(consistency_value_schema) = builder.consistency_value_schema {
        let consistency_topic = format!("{}-consistency", topic);
        let consistency_schema_id = register_kafka_topic(
            &client,
            &consistency_topic,
            builder.replication_factor as i32,
            &ccsr,
            &consistency_value_schema,
        )
        .await
        .context("error registering kafka consistency topic for sink")?;

        Some(KafkaSinkConsistencyConnector {
            topic: consistency_topic,
            schema_id: consistency_schema_id,
        })
    } else {
        None
    };

    Ok(SinkConnector::Kafka(KafkaSinkConnector {
        schema_id,
        topic,
        addr: builder.broker_addr,
        consistency,
        fuel: builder.fuel,
        frontier,
        strict: !with_snapshot,
    }))
}

fn build_avro_ocf(
    builder: AvroOcfSinkConnectorBuilder,
    with_snapshot: bool,
    frontier: Antichain<Timestamp>,
    id: GlobalId,
) -> Result<SinkConnector, anyhow::Error> {
    let mut name = match builder.path.file_stem() {
        None => bail!(
            "unable to read file name from path {}",
            builder.path.display()
        ),
        Some(stem) => stem.to_owned(),
    };
    name.push("-");
    name.push(id.to_string());
    name.push("-");
    name.push(builder.file_name_suffix);
    if let Some(extension) = builder.path.extension() {
        name.push(".");
        name.push(extension);
    }

    let path = builder.path.with_file_name(name);

    // Try to create a new sink file
    let _ = OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| {
            anyhow!(
                "unable to create avro ocf sink file {} : {}",
                path.display(),
                e
            )
        })?;
    Ok(SinkConnector::AvroOcf(AvroOcfSinkConnector {
        path,
        frontier,
        strict: !with_snapshot,
    }))
}
