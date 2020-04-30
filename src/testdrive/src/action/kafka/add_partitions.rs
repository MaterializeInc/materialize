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
use rdkafka::admin::NewPartitions;

use ore::cast::CastFrom;
use ore::collections::CollectionExt;
use ore::retry;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct AddPartitionsAction {
    topic_prefix: String,
    partitions: i32,
}

pub fn build_add_partitions(mut cmd: BuiltinCommand) -> Result<AddPartitionsAction, String> {
    let topic_prefix = format!("testdrive-{}", cmd.args.string("topic")?);
    let partitions = cmd.args.opt_parse("total-partitions")?.unwrap_or(1);
    cmd.args.done()?;

    Ok(AddPartitionsAction {
        topic_prefix,
        partitions,
    })
}

#[async_trait]
impl Action for AddPartitionsAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let topic_name = format!("{}-{}", self.topic_prefix, state.seed);
        println!(
            "Raising partition count of Kafka topic {} to {}",
            topic_name, self.partitions
        );

        match state.kafka_topics.get(&topic_name) {
            Some(partitions) => {
                if self.partitions <= *partitions {
                    return Err(format!(
                        "new partition count {} is not greater than current partition count {}",
                        self.partitions, partitions
                    ));
                }
            }
            None => {
                return Err(format!(
                    "topic {} not created by kafka-create-topic",
                    topic_name
                ))
            }
        }

        let partitions = NewPartitions::new(&topic_name, usize::cast_from(self.partitions));
        let res = state
            .kafka_admin
            .create_partitions(&[partitions], &state.kafka_admin_opts)
            .await
            .map_err(|e| e.to_string())?;
        if res.len() != 1 {
            return Err(format!(
                "kafka partition addition returned {} results, but exactly one result was expected",
                res.len()
            ));
        }
        if let Err((_topic_name, e)) = res.into_element() {
            return Err(e.to_string());
        }

        retry::retry_for(Duration::from_secs(8), |_| async {
            let metadata = state
                .kafka_producer
                .client()
                .fetch_metadata(Some(&topic_name), Some(Duration::from_secs(1)))
                .map_err(|e| e.to_string())?;
            if metadata.topics().len() != 1 {
                return Err("metadata fetch returned no topics".to_string());
            }
            let topic = metadata.topics().into_element();
            if topic.partitions().len() as i32 != self.partitions {
                return Err(format!(
                    "topic {} has {} partitions when exactly {} was expected",
                    topic_name,
                    topic.partitions().len(),
                    self.partitions,
                ));
            }
            Ok(())
        })
        .await?;

        state.kafka_topics.insert(topic_name, self.partitions);
        Ok(())
    }
}
