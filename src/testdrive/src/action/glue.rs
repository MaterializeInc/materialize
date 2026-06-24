// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::{Context, anyhow, bail};
use aws_sdk_glue::types::{Compatibility, DataFormat, RegistryId, SchemaId};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

/// Register a schema in AWS Glue Schema Registry and stash its version UUID in a
/// testdrive variable.
///
/// This keeps a single source of truth in the `.td` file: define the schema
/// once as a pretty-printed `$ set` variable, then reference it both here (to
/// register it) and in `kafka-ingest` (to encode records) — so the body is
/// never written twice.
///
/// ```text
/// $ set my-schema={
///     "type": "record", "name": "row",
///     "fields": [{"name": "a", "type": "long"}]
///   }
///
/// $ glue-create-schema registry=my-registry name=my-schema set-version-id-var=my-version-id schema=${my-schema}
/// ```
///
/// Arguments:
///   * `name` (required): the schema name.
///   * `schema` (required unless given as the command body): the schema
///     definition. Typically a `${...}` reference to a `$ set` variable.
///   * `set-version-id-var` (optional): testdrive variable to receive the
///     returned `SchemaVersionId`. Omit when the schema is referenced only by
///     name (e.g. a negative test that registers a schema just to be rejected).
///   * `registry` (optional): registry name. Omit to target Glue's implicit
///     default registry.
///   * `data-format` (optional, default `avro`): one of `avro`, `json`,
///     `protobuf`.
///   * `compatibility` (optional, default `backward`).
///
/// If a schema with this name already exists, a new version is registered
/// instead — which is how schema-evolution tests register v2 atop v1.
pub async fn run_create_schema(
    mut cmd: BuiltinCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    let name = cmd.args.string("name")?;
    let version_id_var = cmd.args.opt_string("set-version-id-var");
    let registry = cmd.args.opt_string("registry");
    let schema_arg = cmd.args.opt_string("schema");
    let data_format = match cmd
        .args
        .opt_string("data-format")
        .unwrap_or_else(|| "avro".into())
        .to_lowercase()
        .as_str()
    {
        "avro" => DataFormat::Avro,
        "json" => DataFormat::Json,
        "protobuf" => DataFormat::Protobuf,
        other => bail!("unknown data-format: {}", other),
    };
    let compatibility = match cmd
        .args
        .opt_string("compatibility")
        .unwrap_or_else(|| "backward".into())
        .to_lowercase()
        .as_str()
    {
        "backward" => Compatibility::Backward,
        "backward_all" => Compatibility::BackwardAll,
        "forward" => Compatibility::Forward,
        "forward_all" => Compatibility::ForwardAll,
        "full" => Compatibility::Full,
        "full_all" => Compatibility::FullAll,
        "none" => Compatibility::None,
        "disabled" => Compatibility::Disabled,
        other => bail!("unknown compatibility: {}", other),
    };
    cmd.args.done()?;

    // The schema definition comes from the `schema=` argument (typically a
    // `${...}` reference to a `$ set` variable) or, failing that, the command
    // body.
    let definition = match schema_arg {
        Some(schema) => schema,
        None => cmd.input.join("\n"),
    };
    if definition.trim().is_empty() {
        bail!("glue-create-schema requires a `schema=` argument or a schema definition body");
    }

    println!(
        "Registering Glue schema {:?} (registry {:?})...",
        name,
        registry.as_deref().unwrap_or("<default>"),
    );

    let glue = aws_sdk_glue::Client::new(&state.aws_config);

    let mut create = glue
        .create_schema()
        .schema_name(&name)
        .data_format(data_format)
        .compatibility(compatibility)
        .schema_definition(&definition);
    if let Some(registry) = &registry {
        create = create.registry_id(RegistryId::builder().registry_name(registry).build());
    }

    let version_id = match create.send().await {
        Ok(resp) => resp.schema_version_id().map(|s| s.to_string()),
        Err(err) => {
            // The schema already exists — register a new version of it. This is
            // the schema-evolution path (v2 atop v1).
            let svc = err.into_service_error();
            if svc.is_already_exists_exception() {
                let mut schema_id = SchemaId::builder().schema_name(&name);
                if let Some(registry) = &registry {
                    schema_id = schema_id.registry_name(registry);
                }
                let resp = glue
                    .register_schema_version()
                    .schema_id(schema_id.build())
                    .schema_definition(&definition)
                    .send()
                    .await
                    .context("registering new Glue schema version")?;
                resp.schema_version_id().map(|s| s.to_string())
            } else {
                return Err(anyhow::Error::new(svc).context("creating Glue schema"));
            }
        }
    };
    if let Some(var) = version_id_var {
        let version_id =
            version_id.ok_or_else(|| anyhow!("Glue did not return a schema version id"))?;
        state.cmd_vars.insert(var, version_id);
    }
    Ok(ControlFlow::Continue)
}
