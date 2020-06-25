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

use failure::{bail, format_err, ResultExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;

use dataflow_types::{
    AvroOcfSinkConnector, AvroOcfSinkConnectorBuilder, KafkaSinkConnector,
    KafkaSinkConnectorBuilder, SinkConnector, SinkConnectorBuilder,
};
use expr::GlobalId;
use ore::collections::CollectionExt;
use timely::progress::Antichain;

pub async fn build(
    builder: SinkConnectorBuilder,
    with_snapshot: bool,
    frontier: Antichain<u64>,
    id: GlobalId,
) -> Result<SinkConnector, failure::Error> {
    match builder {
        SinkConnectorBuilder::Kafka(k) => build_kafka(k, with_snapshot, frontier, id).await,
        SinkConnectorBuilder::AvroOcf(a) => build_avro_ocf(a, with_snapshot, frontier, id),
    }
}

async fn build_kafka(
    builder: KafkaSinkConnectorBuilder,
    with_snapshot: bool,
    frontier: Antichain<u64>,
    id: GlobalId,
) -> Result<SinkConnector, failure::Error> {
    let topic = format!("{}-{}-{}", builder.topic_prefix, id, builder.topic_suffix);

    // Create Kafka topic with single partition.
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", &builder.broker_url.to_string());
    let res = config
        .create::<AdminClient<_>>()
        .expect("creating admin kafka client failed")
        .create_topics(
            &[NewTopic::new(
                &topic,
                1,
                TopicReplication::Fixed(builder.replication_factor as i32),
            )],
            &AdminOptions::new().request_timeout(Some(Duration::from_secs(5))),
        )
        .await
        .with_context(|e| format!("error creating new topic {} for sink: {}", topic, e))?;
    if res.len() != 1 {
        bail!(
            "error creating topic {} for sink: \
             kafka topic creation returned {} results, but exactly one result was expected",
            topic,
            res.len()
        );
    }
    res.into_element()
        .map_err(|(_, e)| format_err!("error creating topic {} for sink: {}", topic, e))?;

    // Publish value schema for the topic.
    //
    // TODO(benesch): do we need to delete the Kafka topic if publishing the
    // schema fails?
    // TODO(sploiselle): support SSL auth'ed sinks
    let schema_id = ccsr::ClientConfig::new(builder.schema_registry_url)
        .build()
        .publish_schema(&format!("{}-value", topic), &builder.value_schema)
        .await
        .with_context(|e| format!("unable to publish schema to registry in kafka sink: {}", e))?;

    Ok(SinkConnector::Kafka(KafkaSinkConnector {
        schema_id,
        topic,
        url: builder.broker_url,
        fuel: builder.fuel,
        frontier,
        strict: !with_snapshot,
    }))
}

fn build_avro_ocf(
    builder: AvroOcfSinkConnectorBuilder,
    with_snapshot: bool,
    frontier: Antichain<u64>,
    id: GlobalId,
) -> Result<SinkConnector, failure::Error> {
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
            format_err!(
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
