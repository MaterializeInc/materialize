// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use failure::{bail, format_err, ResultExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::config::ClientConfig;

use dataflow_types::{
    KafkaSinkConnector, KafkaSinkConnectorBuilder, SinkConnector, SinkConnectorBuilder,
};
use expr::GlobalId;
use ore::collections::CollectionExt;

pub async fn build(
    builder: SinkConnectorBuilder,
    id: GlobalId,
) -> Result<SinkConnector, failure::Error> {
    match builder {
        SinkConnectorBuilder::Kafka(k) => build_kafka(k, id).await,
    }
}

async fn build_kafka(
    builder: KafkaSinkConnectorBuilder,
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
            &[NewTopic::new(&topic, 1, TopicReplication::Fixed(1))],
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
    let schema_id = ccsr::AsyncClient::new(builder.schema_registry_url)
        .publish_schema(&format!("{}-value", topic), &builder.value_schema)
        .await
        .with_context(|e| format!("unable to publish schema to registry in kafka sink: {}", e))?;

    Ok(SinkConnector::Kafka(KafkaSinkConnector {
        schema_id,
        topic,
        url: builder.broker_url,
    }))
}
