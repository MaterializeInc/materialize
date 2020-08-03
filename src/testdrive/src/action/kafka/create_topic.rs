// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use async_trait::async_trait;
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::error::RDKafkaError;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct CreateTopicAction {
    topic_prefix: String,
    partitions: i32,
}

pub fn build_create_topic(mut cmd: BuiltinCommand) -> Result<CreateTopicAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partitions = cmd.args.opt_parse("partitions")?.unwrap_or(1);
    cmd.args.done()?;

    Ok(CreateTopicAction {
        topic_prefix,
        partitions,
    })
}

#[async_trait]
impl Action for CreateTopicAction {
    async fn undo(&self, state: &mut State) -> Result<(), String> {
        let metadata = state
            .kafka_producer
            .client()
            .fetch_metadata(None, Some(Duration::from_secs(1)))
            .map_err(|e| e.to_string())?;

        let stale_kafka_topics: Vec<_> = metadata
            .topics()
            .iter()
            .filter_map(|t| {
                if t.name().starts_with(&self.topic_prefix) {
                    Some(t.name())
                } else {
                    None
                }
            })
            .collect();

        if !stale_kafka_topics.is_empty() {
            println!(
                "Deleting stale Kafka topics {}",
                stale_kafka_topics.join(", ")
            );
            let res = state
                .kafka_admin
                .delete_topics(&stale_kafka_topics, &state.kafka_admin_opts)
                .await;
            let res = match res {
                Err(err) => return Err(err.to_string()),
                Ok(res) => res,
            };
            if res.len() != stale_kafka_topics.len() {
                return Err(format!(
                    "kafka topic deletion returned {} results, but exactly {} expected",
                    res.len(),
                    stale_kafka_topics.len()
                ));
            }
            for (res, topic) in res.iter().zip(stale_kafka_topics.iter()) {
                match res {
                    Ok(_) | Err((_, RDKafkaError::UnknownTopicOrPartition)) => (),
                    Err((_, err)) => {
                        eprintln!("warning: unable to delete {}: {}", topic, err.to_string())
                    }
                }
            }
        }
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        // NOTE(benesch): it is critical that we invent a new topic name on
        // every testdrive run. We previously tried to delete and recreate the
        // topic with a fixed name, but ran into serious race conditions in
        // Kafka that would regularly cause CI to hang. Details follow.
        //
        // Kafka topic creation and deletion is documented to be asynchronous.
        // That seems fine at first, as the Kafka admin API exposes an
        // `operation_timeout` option that would appear to allow you to opt into
        // a synchronous request by setting a massive timeout. As it turns out,
        // this parameter doesn't actually do anything [0].
        //
        // So, fine, we can implement our own polling for topic creation and
        // deletion, since the Kafka API exposes the list of topics currently
        // known to Kafka. This polling works well enough for topic creation.
        // After issuing a CreateTopics request, we poll the metadata list until
        // the topic appears with the requested number of partitions. (Yes,
        // sometimes the topic will appear with the wrong number of partitions
        // at first, and later sort itself out.)
        //
        // For deletion, though, there's another problem. Not only is deletion
        // of the topic metadata asynchronous, but deletion of the
        // topic data is *also* asynchronous, and independently so. As best as
        // I can tell, the following sequence of events is not only plausible,
        // but likely:
        //
        //     1. Client issues DeleteTopics(FOO).
        //     2. Kafka launches garbage collection of topic FOO.
        //     3. Kafka deletes metadata for topic FOO.
        //     4. Client polls and discovers topic FOO's metadata is gone.
        //     5. Client issues CreateTopics(FOO).
        //     6. Client writes some data to topic FOO.
        //     7. Kafka deletes data for topic FOO, including the data that was
        //        written to the second incarnation of topic FOO.
        //     8. Client attempts to read data written to topic FOO and waits
        //        forever, since there is no longer any data in the topic.
        //        Client becomes very confused and sad.
        //
        // There doesn't appear to be any sane way to poll to determine whether
        // the data has been deleted, since Kafka doesn't expose how many
        // messages are in a topic, and it's therefore impossible to distinguish
        // an empty topic from a deleted topic. And that's not even accounting
        // for the behavior when auto.create.topics.enable is true, which it
        // is by default, where asking about a topic that doesn't exist will
        // automatically create it.
        //
        // All this to say: please think twice before changing the topic naming
        // strategy.
        //
        // [0]: https://github.com/confluentinc/confluent-kafka-python/issues/524#issuecomment-456783176
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Creating Kafka topic {} with partition count of {}",
            topic_name, self.partitions
        );
        let new_topic = NewTopic::new(&topic_name, self.partitions, TopicReplication::Fixed(1))
            // Disabling retention is very important! Our testdrive tests
            // use hardcoded timestamps that are immediately eligible for
            // deletion by Kafka's garbage collector. E.g., the timestamp
            // "1" is interpreted as January 1, 1970 00:00:01, which is
            // breaches the default 7-day retention policy.
            .set("retention.ms", "-1");
        kafka_util::admin::create_topic(&state.kafka_admin, &state.kafka_admin_opts, &new_topic)
            .await
            .map_err(|e| e.to_string())?;
        state.kafka_topics.insert(topic_name, self.partitions);
        Ok(())
    }
}
