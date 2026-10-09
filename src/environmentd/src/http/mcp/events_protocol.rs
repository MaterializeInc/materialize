// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wire types shared by every MCP event.

#![allow(dead_code, reason = "no event is registered")]

use schemars::Schema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{JSONRPC_VERSION, McpError};

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
pub(super) enum RequestId {
    String(String),
    Signed(i64),
    Unsigned(u64),
}

impl RequestId {
    pub fn parse(value: Value) -> Result<Self, String> {
        if let Some(value) = value.as_i64() {
            Ok(Self::Signed(value))
        } else if let Some(value) = value.as_u64() {
            Ok(Self::Unsigned(value))
        } else if let Value::String(value) = value {
            Ok(Self::String(value))
        } else {
            Err("Request id must be a string or integer".into())
        }
    }

    pub fn into_value(self) -> Value {
        match self {
            Self::String(value) => Value::String(value),
            Self::Signed(value) => Value::from(value),
            Self::Unsigned(value) => Value::from(value),
        }
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct ListParams {
    pub cursor: Option<String>,
}

impl ListParams {
    pub fn parse(params: Option<Value>) -> Result<Self, String> {
        let params = params.unwrap_or_else(|| serde_json::json!({}));
        if !params.is_object() {
            return Err("params must be an object".into());
        }
        serde_json::from_value(params).map_err(|error| error.to_string())
    }
}

/// `events/stream` params. Each event validates its own `arguments` and `cursor`.
#[derive(Debug, Deserialize)]
pub(super) struct StreamParams {
    pub name: String,
    #[serde(default)]
    pub arguments: Value,
    #[serde(default)]
    pub cursor: Value,
}

impl StreamParams {
    pub fn parse(params: Value) -> Result<Self, McpError> {
        serde_json::from_value(params).map_err(|error| McpError {
            code: -32602,
            message: error.to_string(),
            data: None,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EventParams<P> {
    pub name: &'static str,
    pub timestamp: String,
    pub event_id: String,
    pub data: P,
}

/// Notifications that carry no event payload.
#[derive(Serialize)]
#[serde(untagged)]
pub(super) enum Control {
    Active { truncated: bool },
    Heartbeat {},
    Terminated { error: McpError },
}

impl Control {
    fn method(&self) -> &'static str {
        match self {
            Self::Active { .. } => "notifications/events/active",
            Self::Heartbeat {} => "notifications/events/heartbeat",
            Self::Terminated { .. } => "notifications/events/terminated",
        }
    }
}

#[derive(Serialize)]
struct Metadata<'a> {
    #[serde(rename = "io.modelcontextprotocol/subscriptionId")]
    request_id: &'a RequestId,
}

#[derive(Serialize)]
pub(super) struct Notification<'a, C, B> {
    jsonrpc: &'static str,
    method: &'static str,
    params: NotificationParams<'a, C, B>,
}

#[derive(Serialize)]
struct NotificationParams<'a, C, B> {
    cursor: Option<&'a C>,
    #[serde(rename = "_meta")]
    meta: Metadata<'a>,
    #[serde(flatten)]
    body: B,
}

impl<'a, C, B> Notification<'a, C, B> {
    fn new(
        method: &'static str,
        body: B,
        cursor: Option<&'a C>,
        request_id: &'a RequestId,
    ) -> Self {
        Self {
            jsonrpc: JSONRPC_VERSION,
            method,
            params: NotificationParams {
                cursor,
                meta: Metadata { request_id },
                body,
            },
        }
    }
}

impl<'a, C> Notification<'a, C, Control> {
    pub fn control(body: Control, cursor: Option<&'a C>, request_id: &'a RequestId) -> Self {
        Self::new(body.method(), body, cursor, request_id)
    }
}

impl<'a, C, P> Notification<'a, C, EventParams<P>> {
    pub fn event(body: EventParams<P>, cursor: Option<&'a C>, request_id: &'a RequestId) -> Self {
        Self::new("notifications/events/event", body, cursor, request_id)
    }
}

#[derive(Debug, Serialize)]
pub(super) struct ListResult {
    pub events: Vec<EventDefinition>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EventDefinition {
    pub name: &'static str,
    pub delivery: [&'static str; 1],
    pub description: &'static str,
    pub input_schema: Schema,
    pub payload_schema: Schema,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[mz_ore::test]
    fn request_ids_are_lossless_strings_or_integers() {
        for value in [json!("watch"), json!(i64::MIN), json!(0), json!(u64::MAX)] {
            let id = RequestId::parse(value.clone()).unwrap();
            let notification = serde_json::to_value(Notification::<(), _>::control(
                Control::Heartbeat {},
                None,
                &id,
            ))
            .unwrap();
            assert_eq!(
                notification["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
                value
            );
            assert_eq!(id.into_value(), value);
        }
        for value in [Value::Null, json!(true), json!(1.5), json!([]), json!({})] {
            assert!(RequestId::parse(value).is_err());
        }
    }
}
