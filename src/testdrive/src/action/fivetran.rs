// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use anyhow::{Context, bail};

use crate::action::{ControlFlow, State};
use crate::parser::BuiltinCommand;

// Note(parkmycar): We wrap this in a `mod` block soley for the purpose of allowing lints for the
// generated protobuf code.
#[allow(dead_code, clippy::as_conversions, clippy::clone_on_ref_ptr)]
mod proto {
    pub mod fivetran {
        include!(concat!(env!("OUT_DIR"), "/fivetran_sdk.v2.rs"));
    }
}

// We explicitly generate and then include the "well known types" for Protobuf so we can derive
// `serde` traits.
#[allow(clippy::as_conversions, clippy::clone_on_ref_ptr)]
mod google {
    pub mod protobuf {
        include!(concat!(env!("OUT_DIR"), "/google.protobuf.rs"));
    }
}

/// Sends a gRPC request to the currently running Fivetran Destination.
pub async fn run_destination_command(
    mut cmd: BuiltinCommand,
    state: &State,
) -> Result<ControlFlow, anyhow::Error> {
    let action = cmd.args.string("action")?;

    // Interpret the remaining arguments are part of the connection config.

    let sql_host = url::Url::parse(&state.materialize.sql_addr)
        .expect("failed to parse Materialize SQL addr")
        .scheme()
        .to_string();
    let default_config = [
        ("host", sql_host),
        ("user", "materialize".into()),
        ("app_password", "ignored".into()),
        ("dbname", "materialize".into()),
    ];
    let mut config: BTreeMap<_, _> = cmd.args.into_iter().collect();
    for (key, value) in default_config {
        config.entry(key.into()).or_insert(value);
    }

    let config: serde_json::Map<String, serde_json::Value> = config
        .into_iter()
        .map(|(key, value)| (key, serde_json::Value::String(value)))
        .collect();

    let body = cmd.input.join("\n");
    let objects = serde_json::Deserializer::from_str(&body)
        .into_iter::<serde_json::Value>()
        .collect::<Result<Vec<_>, _>>()
        .context("reading body input")?;
    let (mut request, response) = match &objects[..] {
        [req, resp] => (req.clone(), resp.clone()),
        x => bail!("Expected 2 JSON objects, found {}", x.len()),
    };

    // Splice the configuration into the request.
    let Some(request_map) = request.as_object_mut() else {
        bail!("Invalid type found for request");
    };
    if request_map.contains_key("configuration") {
        bail!("Request object should not contain key 'configuration'");
    }
    request_map.insert("configuration".into(), serde_json::Value::Object(config));

    // Connect to the destination.
    println!("{action} @ {}", state.fivetran_destination_url);
    let mut fivetran_client =
        proto::fivetran::destination_connector_client::DestinationConnectorClient::connect(
            state.fivetran_destination_url.clone(),
        )
        .await
        .context("connecting to fivetran destination")?;

    match action.as_str() {
        "describe" => {
            let request: proto::fivetran::DescribeTableRequest =
                serde_json::from_value(request).context("describe request")?;
            let expected_response: proto::fivetran::DescribeTableResponse =
                serde_json::from_value(response).context("describe response")?;

            let response = fivetran_client
                .describe_table(request)
                .await
                .context("describe")?;
            let response = response.into_inner();

            if response != expected_response {
                bail!(
                    "Describe Table Response did not match expected\n
                    response: {response:#?}\n
                    expected: {expected_response:#?}"
                );
            }
        }
        "write_batch" => {
            let request: proto::fivetran::WriteBatchRequest =
                serde_json::from_value(request).context("write batch request")?;
            let expected_response: proto::fivetran::WriteBatchResponse =
                serde_json::from_value(response).context("write batch response")?;

            let response = fivetran_client
                .write_batch(request)
                .await
                .context("write batch")?;
            let response = response.into_inner();

            if response != expected_response {
                bail!(
                    "Write Batch Response did not match expected\n
                    response: {response:#?}\n
                    expected: {expected_response:#?}"
                );
            }
        }
        other => bail!("Unsupported command {other}"),
    }

    Ok(ControlFlow::Continue)
}
