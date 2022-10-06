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

use mz_ccsr::{SchemaReference, SchemaType};
use mz_ore::retry::Retry;

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

pub async fn run_publish(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let subject = cmd.args.string("subject")?;
    let schema_type = match cmd.args.string("schema-type")?.as_str() {
        "avro" => SchemaType::Avro,
        "json" => SchemaType::Json,
        "protobuf" => SchemaType::Protobuf,
        s => bail!("unknown schema type: {}", s),
    };
    let references_outer = match cmd.args.opt_string("references") {
        None => vec![],
        Some(s) => s.split(',').map(|s| s.to_string()).collect(),
    };

    let schema = cmd.input.join("\n");

    let mut references = vec![];
    for reference in references_outer {
        let subject = state
            .ccsr_client
            .get_subject(&reference)
            .await
            .with_context(|| format!("fetching reference {}", reference))?;
        references.push(SchemaReference {
            name: subject.name,
            subject: reference.to_string(),
            version: subject.version,
        })
    }
    state
        .ccsr_client
        .publish_schema(&subject, &schema, schema_type, &references)
        .await
        .context("publishing schema")?;
    Ok(ControlFlow::Continue)
}

pub async fn run_wait(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let schema = cmd.args.string("schema")?;
    cmd.args.done()?;

    Retry::default()
        .initial_backoff(Duration::from_millis(50))
        .factor(1.5)
        .max_duration(state.timeout)
        .retry_async_canceling(|_| async {
            state
                .ccsr_client
                .get_schema_by_subject(&schema)
                .await
                .context("fetching schema")
                .and(Ok(()))
        })
        .await?;
    Ok(ControlFlow::Continue)
}
