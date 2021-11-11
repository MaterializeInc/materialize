// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use async_trait::async_trait;

use ccsr::{SchemaReference, SchemaType};
use ore::retry::Retry;

use crate::action::{Action, Context, State};
use crate::parser::BuiltinCommand;

pub struct PublishAction {
    subject: String,
    schema: String,
    schema_type: SchemaType,
    references: Vec<String>,
}

pub fn build_publish(mut cmd: BuiltinCommand) -> Result<PublishAction, String> {
    let subject = cmd.args.string("subject")?;
    let schema_type = match cmd.args.string("schema-type")?.as_str() {
        "avro" => SchemaType::Avro,
        "json" => SchemaType::Json,
        "protobuf" => SchemaType::Protobuf,
        s => return Err(format!("unknown schema type: {}", s)),
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
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let mut references = vec![];
        for reference in &self.references {
            let subject = state
                .ccsr_client
                .get_subject(reference)
                .await
                .map_err(|e| format!("fetching reference {}: {}", reference, e))?;
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
            .map_err(|e| format!("publishing schema: {}", e))?;
        Ok(())
    }
}

pub struct WaitSchemaAction {
    schema: String,
    context: Context,
}

pub fn build_wait(mut cmd: BuiltinCommand, context: Context) -> Result<WaitSchemaAction, String> {
    let schema = cmd.args.string("schema")?;
    cmd.args.done()?;
    Ok(WaitSchemaAction { schema, context })
}

#[async_trait]
impl Action for WaitSchemaAction {
    async fn undo(&self, _: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        Retry::default()
            .initial_backoff(Duration::from_millis(50))
            .factor(1.5)
            .max_duration(self.context.timeout)
            .retry(|_| async {
                state
                    .ccsr_client
                    .get_schema_by_subject(&self.schema)
                    .await
                    .map_err(|e| format!("fetching schema: {}", e))
                    .and(Ok(()))
            })
            .await
    }
}
