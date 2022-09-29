// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::admin::{NewTopic, TopicReplication};

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct CreateTopicAction {
    topic_prefix: String,
    partitions: Option<usize>,
    replication_factor: i32,
    compression: String,
    compaction: bool,
}

pub fn build_create_topic(mut cmd: BuiltinCommand) -> Result<CreateTopicAction, anyhow::Error> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partitions = cmd.args.opt_parse("partitions")?;

    let replication_factor = cmd.args.opt_parse("replication-factor")?.unwrap_or(1);
    let compression = cmd
        .args
        .opt_string("compression")
        .unwrap_or_else(|| "producer".into());
    let compaction = cmd.args.opt_parse("compaction")?.unwrap_or(false);
    cmd.args.done()?;

    Ok(CreateTopicAction {
        topic_prefix,
        partitions,
        replication_factor,
        compression,
        compaction,
    })
}

#[async_trait]
impl Action for CreateTopicAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
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
        let partitions = self.partitions.unwrap_or(state.kafka_default_partitions);

        println!(
            "Creating Kafka topic {} with partition count of {}",
            topic_name, partitions
        );

        let new_topic = NewTopic::new(
            &topic_name,
            i32::try_from(partitions)
                .map_err(|_| anyhow!("partition count must fit in an i32: {}", partitions))?,
            TopicReplication::Fixed(self.replication_factor),
        )
        // Disabling retention is very important! Our testdrive tests
        // use hardcoded timestamps that are immediately eligible for
        // deletion by Kafka's garbage collector. E.g., the timestamp
        // "1" is interpreted as January 1, 1970 00:00:01, which is
        // breaches the default 7-day retention policy.
        .set("retention.ms", "-1")
        .set("compression.type", &self.compression);

        // aggressive compaction, when it is enabled
        let new_topic = if self.compaction {
            new_topic
                .set("cleanup.policy", "compact")
                // eagerly roll over segments
                .set("segment.ms", "100")
                // make sure we get compaction even with low throughput
                .set("min.cleanable.dirty.ratio", "0.01")
                .set("min.compaction.lag.ms", "100")
                .set("delete.retention.ms", "100")
        } else {
            new_topic
        };

        mz_kafka_util::admin::ensure_topic(&state.kafka_admin, &state.kafka_admin_opts, &new_topic)
            .await?;
        state.kafka_topics.insert(topic_name, partitions);
        Ok(ControlFlow::Continue)
    }
}
