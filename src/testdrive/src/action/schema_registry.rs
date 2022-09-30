// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use anyhow::{bail, Context};
use async_trait::async_trait;

use mz_ccsr::{SchemaReference, SchemaType};
use mz_ore::retry::Retry;

use crate::action::{Action, ControlFlow, State};
use crate::parser::BuiltinCommand;

pub struct PublishAction {
    subject: String,
    schema: String,
    schema_type: SchemaType,
    references: Vec<String>,
}

pub fn build_publish(mut cmd: BuiltinCommand) -> Result<PublishAction, anyhow::Error> {
    let subject = cmd.args.string("subject")?;
    let schema_type = match cmd.args.string("schema-type")?.as_str() {
        "avro" => SchemaType::Avro,
        "json" => SchemaType::Json,
        "protobuf" => SchemaType::Protobuf,
        s => bail!("unknown schema type: {}", s),
    };
    let references = match cmd.args.opt_string("references") {
        None => vec![],
        Some(s) => s.split(',').map(|s| s.into()).collect(),
    };
    Ok(PublishAction {
        subject,
        schema: cmd.input.join("\n"),
        schema_type,
        references,
    })
}

#[async_trait]
impl Action for PublishAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        let mut references = vec![];
        for reference in &self.references {
            let subject = state
                .ccsr_client
                .get_subject(reference)
                .await
                .with_context(|| format!("fetching reference {}", reference))?;
            references.push(SchemaReference {
                name: subject.name,
                subject: reference.clone(),
                version: subject.version,
            })
        }
        state
            .ccsr_client
            .publish_schema(&self.subject, &self.schema, self.schema_type, &references)
            .await
            .context("publishing schema")?;
        Ok(ControlFlow::Continue)
    }
}

pub struct WaitSchemaAction {
    schema: String,
}

pub fn build_wait(mut cmd: BuiltinCommand) -> Result<WaitSchemaAction, anyhow::Error> {
    let schema = cmd.args.string("schema")?;
    cmd.args.done()?;
    Ok(WaitSchemaAction { schema })
}

#[async_trait]
impl Action for WaitSchemaAction {
    async fn run(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        Retry::default()
            .initial_backoff(Duration::from_millis(50))
            .factor(1.5)
            .max_duration(state.timeout)
            .retry_async_canceling(|_| async {
                state
                    .ccsr_client
                    .get_schema_by_subject(&self.schema)
                    .await
                    .context("fetching schema")
                    .and(Ok(()))
            })
            .await?;
        Ok(ControlFlow::Continue)
    }
}
