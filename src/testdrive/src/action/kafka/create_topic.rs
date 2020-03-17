// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::thread;
use std::time::Duration;

use futures::executor::block_on;
use rdkafka::admin::{NewTopic, TopicReplication};
use rdkafka::error::RDKafkaError;

use ore::collections::CollectionExt;

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

impl Action for CreateTopicAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
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
            let res = block_on(
                state
                    .kafka_admin
                    .delete_topics(&stale_kafka_topics, &state.kafka_admin_opts),
            );
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

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Creating Kafka topic {} with partition count of {}",
            topic_name, self.partitions
        );
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
        let new_topic = NewTopic::new(&topic_name, self.partitions, TopicReplication::Fixed(1))
            // Disabling retention is very important! Our testdrive tests
            // use hardcoded timestamps that are immediately eligible for
            // deletion by Kafka's garbage collector. E.g., the timestamp
            // "1" is interpreted as January 1, 1970 00:00:01, which is
            // breaches the default 7-day retention policy.
            .set("retention.ms", "-1");
        let res = block_on(
            state
                .kafka_admin
                .create_topics(&[new_topic], &state.kafka_admin_opts),
        );
        let res = match res {
            Err(err) => return Err(err.to_string()),
            Ok(res) => res,
        };
        if res.len() != 1 {
            return Err(format!(
                "kafka topic creation returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        match res.into_element() {
            Ok(_) | Err((_, RDKafkaError::TopicAlreadyExists)) => Ok(()),
            Err((_, err)) => Err(err.to_string()),
        }?;

        // Topic creation is asynchronous, and if we don't wait for it to
        // complete, we might produce a message (below) that causes it to
        // get automatically created with multiple partitions. (Since
        // multiple partitions have no ordering guarantees, this violates
        // many assumptions that our tests make.)
        let mut i = 0;
        loop {
            let res = (|| {
                let metadata = state
                    .kafka_producer
                    .client()
                    // N.B. It is extremely important not to ask specifically
                    // about the topic here, even though the API supports it!
                    // Asking about the topic will create it automatically...
                    // with the wrong number of partitions. Yes, this is
                    // unbelievably horrible.
                    .fetch_metadata(None, Some(Duration::from_secs(1)))
                    .map_err(|e| e.to_string())?;
                if metadata.topics().is_empty() {
                    return Err("metadata fetch returned no topics".to_string());
                }
                let topic = match metadata.topics().iter().find(|t| t.name() == topic_name) {
                    Some(topic) => topic,
                    None => {
                        return Err(format!(
                            "metadata fetch did not return topic {}",
                            topic_name,
                        ))
                    }
                };
                if topic.partitions().is_empty() {
                    return Err("metadata fetch returned a topic with no partitions".to_string());
                } else if topic.partitions().len() as i32 != self.partitions {
                    return Err(format!(
                        "topic {} was created with {} partitions when exactly {} was expected",
                        topic_name,
                        topic.partitions().len(),
                        self.partitions
                    ));
                }
                Ok(())
            })();
            match res {
                Ok(()) => break,
                Err(e) if i == 6 => return Err(e),
                _ => {
                    thread::sleep(Duration::from_millis(100 * 2_u64.pow(i)));
                    i += 1;
                }
            }
        }
        state.kafka_topics.insert(topic_name, self.partitions);
        Ok(())
    }
}
