// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str;

use async_trait::async_trait;
use lazy_static::lazy_static;
use regex::Regex;

use crate::action::{Action, State};
use crate::parser::BuiltinCommand;

pub struct VerifyTopicAction {
    sink: String,
    topic: String,
    consistency: bool,
    nonced: bool,
}

pub fn build_verify_topic(mut cmd: BuiltinCommand) -> Result<VerifyTopicAction, String> {
    let sink = cmd.args.string("sink")?;
    let topic = cmd.args.string("topic")?;
    let consistency = cmd.args.opt_bool("consistency")?.unwrap_or(false);
    let nonced = cmd.args.opt_bool("nonced")?.unwrap_or(false);
    cmd.args.done()?;
    Ok(VerifyTopicAction {
        sink,
        topic,
        consistency,
        nonced,
    })
}

#[async_trait]
impl Action for VerifyTopicAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        lazy_static! {
            static ref NONCED_TOPIC: Regex =
                Regex::new(r#".*-[u,s]\d{0,20}-\d{0,19}-\d{0,20}"#).expect("known valid");
        }
        async fn get_topic(
            sink: &str,
            topic_field: &str,
            state: &mut State,
        ) -> Result<String, String> {
            let query = format!("SELECT {} FROM mz_catalog_names JOIN mz_kafka_sinks ON global_id = sink_id WHERE name = $1", topic_field);
            let result = state
                .pgclient
                .query_one(query.as_str(), &[&sink])
                .await
                .map_err(|e| format!("retrieving topic name: {}", e))?
                .get(topic_field);
            Ok(result)
        }

        println!("Verifying topic for sink {}", &self.sink);

        let topic: String = if self.consistency {
            get_topic(&self.sink, "consistency_topic", state).await?
        } else {
            get_topic(&self.sink, "topic", state).await?
        };
        if self.nonced {
            if !NONCED_TOPIC.is_match(&topic) {
                return Err(format!(
                    "fetched topic doesn't match expected nonce pattern: {}",
                    &topic
                ));
            }
        } else {
            if self.topic != topic {
                return Err(format!(
                    "fetched topic {} doesn't match expected topic {}",
                    &topic, &self.topic
                ));
            }
        }
        Ok(())
    }
}
