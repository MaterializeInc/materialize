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
use mz_ore::str::StrExt;

use crate::action::{ControlFlow, State};
use crate::format::avro;
use crate::parser::BuiltinCommand;

pub async fn run_publish(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    // Parse arguments.
    let subject = cmd.args.string("subject")?;
    let schema_type = match cmd.args.string("schema-type")?.as_str() {
        "avro" => SchemaType::Avro,
        "json" => SchemaType::Json,
        "protobuf" => SchemaType::Protobuf,
        s => bail!("unknown schema type: {}", s),
    };
    let references_in = match cmd.args.opt_string("references") {
        None => vec![],
        Some(s) => s.split(',').map(|s| s.to_string()).collect(),
    };
    cmd.args.done()?;
    let schema = cmd.input.join("\n");

    // Run action.
    println!(
        "Publishing schema for subject {} to the schema registry...",
        subject.quoted(),
    );
    let mut references = vec![];
    for reference in references_in {
        let subject = state
            .ccsr_client
            .get_subject_latest(&reference)
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

pub async fn run_verify(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    // Parse arguments.
    let subject = cmd.args.string("subject")?;
    match cmd.args.string("schema-type")?.as_str() {
        "avro" => (),
        f => bail!("unknown format: {}", f),
    };
    let compatibility_level = cmd.args.opt_string("compatibility-level");
    cmd.args.done()?;
    let expected_schema = match &cmd.input[..] {
        [expected_schema] => {
            avro::parse_schema(expected_schema).context("parsing expected avro schema")?
        }
        _ => bail!("unable to read expected schema input"),
    };

    // Run action.
    println!(
        "Verifying contents of latest schema for subject {} in the schema registry...",
        subject.quoted(),
    );

    // Finding the published schema is retryable because it's published
    // asynchronously and only after the source/sink is created.
    let actual_schema = mz_ore::retry::Retry::default()
        .max_duration(state.default_timeout)
        .retry_async(|_| async {
            match state.ccsr_client.get_schema_by_subject(&subject).await {
                Ok(s) => mz_ore::retry::RetryResult::Ok(s.raw),
                Err(
                    e @ mz_ccsr::GetBySubjectError::SubjectNotFound
                    | e @ mz_ccsr::GetBySubjectError::VersionNotFound(_),
                ) => mz_ore::retry::RetryResult::RetryableErr(e),
                Err(e) => mz_ore::retry::RetryResult::FatalErr(e),
            }
        })
        .await
        .context("fetching schema")?;

    let actual_schema = avro::parse_schema(&actual_schema).context("parsing actual avro schema")?;
    if expected_schema != actual_schema {
        bail!(
            "schema did not match\nexpected:\n{:?}\n\nactual:\n{:?}",
            expected_schema,
            actual_schema,
        );
    }

    if let Some(compatibility_level) = compatibility_level {
        println!(
            "Verifying compatibility level of subject {} in the schema registry...",
            subject.quoted(),
        );
        let res = state.ccsr_client.get_subject_config(&subject).await?;
        if compatibility_level != res.compatibility_level.to_string() {
            bail!(
                "compatibility level did not match\nexpected: {}\nactual: {}",
                compatibility_level,
                res.compatibility_level,
            );
        }
    }
    Ok(ControlFlow::Continue)
}

pub async fn run_wait(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    // Parse arguments.
    let topic = cmd.args.string("topic")?;
    let subjects = [format!("{}-value", topic), format!("{}-key", topic)];

    cmd.args.done()?;
    cmd.assert_no_input()?;

    // Run action.

    let mut waiting_for_kafka = false;

    println!(
        "Waiting for schema for subjects {:?} to become available in the schema registry...",
        subjects
    );

    let topic = &topic;
    let subjects = &subjects;
    Retry::default()
        .initial_backoff(Duration::from_millis(50))
        .factor(1.5)
        .max_duration(state.timeout)
        .retry_async_canceling(|_| async move {
            if !waiting_for_kafka {
                futures::future::try_join_all(subjects.iter().map(|subject| async move {
                    state
                        .ccsr_client
                        // This doesn't take `ccsr_client` by `&mut self`, so it should be safe to cancel
                        // by try-joining.
                        .get_schema_by_subject(subject)
                        .await
                        .context("fetching schema")
                        .and(Ok(()))
                }))
                .await?;

                waiting_for_kafka = true;
                println!("Waiting for Kafka topic {} to exist", topic);
            }

            if waiting_for_kafka {
                super::kafka::check_topic_exists(topic, state).await?
            }

            Ok::<(), anyhow::Error>(())
        })
        .await?;

    Ok(ControlFlow::Continue)
}
