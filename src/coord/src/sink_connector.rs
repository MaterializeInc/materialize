// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fs::OpenOptions;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;

use dataflow_types::{
    AvroOcfSinkConnector, AvroOcfSinkConnectorBuilder, ExternalSourceConnector, KafkaSinkConnector,
    KafkaSinkConnectorBuilder, KafkaSinkConsistencyConnector, Persistence, SinkConnector,
    SinkConnectorBuilder, SourceConnector, Timestamp,
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
pub async fn handle_persistence(
    connector: SourceConnector,
    id: GlobalId,
) -> Result<Option<SourceConnector>, anyhow::Error> {
    match connector {
        SourceConnector::External {
            connector,
            encoding,
            envelope,
            consistency,
            max_ts_batch,
            ts_frequency,
            persistence,
        } => {
            if persistence.is_none() {
                // There's nothing to handle
                Ok(None)
            } else {
                let persistence = persistence.expect("persistence known to exist");
                let connector = handle_persistence_inner(connector, &persistence, id).await?;

                if let Some(connector) = connector {
                    Ok(Some(SourceConnector::External {
                        connector,
                        encoding,
                        envelope,
                        consistency,
                        max_ts_batch,
                        ts_frequency,
                        persistence: Some(persistence),
                    }))
                } else {
                    Ok(None)
                }
            }
        }
        SourceConnector::Local => Ok(None),
    }
}

async fn handle_persistence_inner(
    mut connector: ExternalSourceConnector,
    persistence: &Persistence,
    id: GlobalId,
) -> Result<Option<ExternalSourceConnector>, anyhow::Error> {
    match &mut connector {
        ExternalSourceConnector::Kafka(k) => {
            // First let's read the finalized files directory to figure out what persistence
            // files we have, if any
            let files =
                std::fs::read_dir(&persistence.path).expect("reading directory cannot fail");
            let file_prefix = format!("materialized-source-{}", id);
            let mut read_offsets: HashMap<i32, i64> = HashMap::new();
            let mut paths = Vec::new();

            for f in files {
                // TODO there has to be a better way
                let path = f.expect("file known to exist").path();
                let filename = path.file_name().unwrap().to_str().unwrap().to_owned();
                if filename.starts_with(&file_prefix) {
                    let parts: Vec<_> = filename.split('-').collect();

                    if parts.len() != 6 {
                        eprintln!("Found unexpected file {}", filename);
                        continue;
                    }

                    let partition_id = parts[3].parse::<i32>().unwrap();
                    let end_offset = parts[4].parse::<i64>().unwrap();
                    paths.push(path);
                    // TODO this is lazy and incomplete
                    match read_offsets.get(&partition_id) {
                        None => {
                            read_offsets.insert(partition_id, end_offset);
                        }
                        Some(o) => {
                            if end_offset > *o {
                                read_offsets.insert(partition_id, end_offset);
                            }
                        }
                    };
                }
            }

            if paths.len() > 0 {
                k.start_offsets = read_offsets;
                k.persisted_files = Some(paths);
            } else {
                return Ok(None);
            }
        }
        _ => bail!("persistence only enabled for Kafka sources at this time"),
    }

    Ok(Some(connector))
}
