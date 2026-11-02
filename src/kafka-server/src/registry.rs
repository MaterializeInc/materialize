// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A read-only schema registry for the claimed topics, serving the subset of the Confluent
//! Schema Registry API that serializers use.
//!
//! A topic registered with [`TopicSchemas`] has the subjects `<topic>-key` and `<topic>-value`
//! (Confluent's `TopicNameStrategy`), each with a single version. A subject is readable with the
//! access its topic requires to produce, presented as HTTP basic auth.
//!
//! Registering a schema under a subject succeeds only if it has the same [Parsing Canonical
//! Form] as the subject's schema, and returns the subject's ID. Records encoded with such a
//! schema encode the same way as with the subject's schema, which is what lets a serializer that
//! registers its schema before producing, as Confluent serializers do by default, write to the
//! topic.
//!
//! An ID is derived from the schema's canonical form, so it is the same on every replica and
//! after restarts, and the same schema has the same ID under every subject.
//!
//! [Parsing Canonical Form]:
//!     https://avro.apache.org/docs/++version++/specification#parsing-canonical-form-for-schemas

use std::str::FromStr;

use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use headers::HeaderMapExt;
use headers::authorization::{Authorization, Basic};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::{KafkaServer, SaslPlainCredentials};

/// The Avro schemas of a claimed topic's keys and values.
#[derive(Debug, Clone, Default)]
pub struct TopicSchemas {
    pub key: Option<String>,
    pub value: Option<String>,
}

/// A schema the registry serves.
#[derive(Debug, Clone)]
pub(crate) struct ServedSchema {
    /// The schema as the topic's consumer gave it.
    raw: String,
    canonical: String,
    id: i32,
}

impl ServedSchema {
    pub(crate) fn new(raw: String) -> Result<ServedSchema, anyhow::Error> {
        let canonical = canonical_form(&raw)?;
        let id = schema_id(&canonical);
        Ok(ServedSchema { raw, canonical, id })
    }
}

fn canonical_form(schema: &str) -> Result<String, anyhow::Error> {
    Ok(mz_avro::Schema::from_str(schema)?.canonical_form())
}

/// Derives a schema ID from the schema's canonical form.
///
/// IDs are positive, as clients expect. Two schemas with different canonical forms get the same
/// ID with a probability of 2^-31 per pair, in which case lookups by ID return either.
fn schema_id(canonical: &str) -> i32 {
    let digest = Sha256::digest(canonical.as_bytes());
    let bits = u32::from_be_bytes(digest[..4].try_into().expect("digest has 32 bytes"));
    i32::try_from(bits >> 1).expect("31 bits fit").max(1)
}

/// The subject of the key or value schema of `topic`.
fn subject(topic: &str, key: bool) -> String {
    format!("{topic}-{}", if key { "key" } else { "value" })
}

/// Returns the router that serves the registry.
pub(crate) fn router(server: KafkaServer) -> Router {
    Router::new()
        .route("/", get(async || Json(json!({}))))
        .route("/subjects", get(list_subjects))
        .route("/subjects/{subject}", post(lookup))
        .route(
            "/subjects/{subject}/versions",
            get(list_versions).post(register),
        )
        .route("/subjects/{subject}/versions/{version}", get(get_version))
        .route("/schemas/ids/{id}", get(get_by_id))
        .route("/schemas/ids/{id}/versions", get(get_versions_by_id))
        .with_state(server)
}

/// A response with the registry's content type.
struct Json(serde_json::Value);

impl IntoResponse for Json {
    fn into_response(self) -> Response {
        (
            [(
                header::CONTENT_TYPE,
                "application/vnd.schemaregistry.v1+json",
            )],
            self.0.to_string(),
        )
            .into_response()
    }
}

/// An error in the registry's format. Clients act on `error_code`, for example a serializer
/// treats 40401 as "subject not found".
struct Error {
    status: StatusCode,
    code: u32,
    message: String,
}

impl Error {
    fn subject_not_found(subject: &str) -> Error {
        Error {
            status: StatusCode::NOT_FOUND,
            code: 40401,
            message: format!("Subject '{subject}' not found."),
        }
    }

    fn version_not_found(version: &str) -> Error {
        Error {
            status: StatusCode::NOT_FOUND,
            code: 40402,
            message: format!("Version {version} not found."),
        }
    }

    fn schema_not_found() -> Error {
        Error {
            status: StatusCode::NOT_FOUND,
            code: 40403,
            message: "Schema not found.".into(),
        }
    }

    fn invalid_schema(e: anyhow::Error) -> Error {
        Error {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            code: 42201,
            message: format!("Invalid schema: {e:#}"),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let body = Json(json!({"error_code": self.code, "message": self.message}));
        (self.status, body).into_response()
    }
}

/// The schemas readable with the request's credentials, by subject.
///
/// Fails if the request presents credentials that no topic accepts, like a failed SASL
/// authentication.
fn readable(
    server: &KafkaServer,
    headers: &HeaderMap,
) -> Result<Vec<(String, ServedSchema)>, Error> {
    let credentials =
        headers
            .typed_get::<Authorization<Basic>>()
            .map(|auth| SaslPlainCredentials {
                username: auth.username().into(),
                password: auth.password().into(),
            });
    if let Some(credentials) = &credentials {
        if !server.authenticate(credentials) {
            return Err(Error {
                status: StatusCode::UNAUTHORIZED,
                code: 40101,
                message: "Unauthorized.".into(),
            });
        }
    }
    Ok(server.served_schemas(credentials.as_ref()))
}

fn find(schemas: Vec<(String, ServedSchema)>, subject: &str) -> Result<ServedSchema, Error> {
    schemas
        .into_iter()
        .find_map(|(s, schema)| (s == subject).then_some(schema))
        .ok_or_else(|| Error::subject_not_found(subject))
}

fn version_response(subject: &str, schema: &ServedSchema) -> Json {
    Json(json!({
        "subject": subject,
        "version": 1,
        "id": schema.id,
        "schema": schema.raw,
    }))
}

async fn list_subjects(State(server): State<KafkaServer>, headers: HeaderMap) -> Response {
    match readable(&server, &headers) {
        Ok(schemas) => {
            let subjects: Vec<_> = schemas.into_iter().map(|(subject, _)| subject).collect();
            Json(json!(subjects)).into_response()
        }
        Err(e) => e.into_response(),
    }
}

async fn list_versions(
    State(server): State<KafkaServer>,
    Path(subject): Path<String>,
    headers: HeaderMap,
) -> Response {
    match readable(&server, &headers).and_then(|s| find(s, &subject)) {
        Ok(_) => Json(json!([1])).into_response(),
        Err(e) => e.into_response(),
    }
}

async fn get_version(
    State(server): State<KafkaServer>,
    Path((subject, version)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let result = readable(&server, &headers)
        .and_then(|s| find(s, &subject))
        .and_then(|schema| match version.as_str() {
            "1" | "latest" | "-1" => Ok(version_response(&subject, &schema)),
            _ => Err(Error::version_not_found(&version)),
        });
    match result {
        Ok(response) => response.into_response(),
        Err(e) => e.into_response(),
    }
}

async fn get_by_id(
    State(server): State<KafkaServer>,
    Path(id): Path<i32>,
    headers: HeaderMap,
) -> Response {
    let result = readable(&server, &headers).and_then(|schemas| {
        schemas
            .into_iter()
            .find(|(_, schema)| schema.id == id)
            .map(|(_, schema)| Json(json!({"schema": schema.raw})))
            .ok_or_else(Error::schema_not_found)
    });
    match result {
        Ok(response) => response.into_response(),
        Err(e) => e.into_response(),
    }
}

async fn get_versions_by_id(
    State(server): State<KafkaServer>,
    Path(id): Path<i32>,
    headers: HeaderMap,
) -> Response {
    let result = readable(&server, &headers).and_then(|schemas| {
        let versions: Vec<_> = schemas
            .into_iter()
            .filter(|(_, schema)| schema.id == id)
            .map(|(subject, _)| json!({"subject": subject, "version": 1}))
            .collect();
        if versions.is_empty() {
            Err(Error::schema_not_found())
        } else {
            Ok(Json(json!(versions)))
        }
    });
    match result {
        Ok(response) => response.into_response(),
        Err(e) => e.into_response(),
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SchemaRequest {
    schema: String,
    /// `AVRO` if absent. References are not supported, and a schema that uses them fails to
    /// parse.
    schema_type: Option<String>,
}

/// Returns the subject's schema if `request` has the same canonical form.
fn matching(
    server: &KafkaServer,
    headers: &HeaderMap,
    subject: &str,
    request: &SchemaRequest,
) -> Result<Option<ServedSchema>, Error> {
    let schema = readable(server, headers).and_then(|s| find(s, subject))?;
    if request.schema_type.as_deref().is_some_and(|t| t != "AVRO") {
        return Ok(None);
    }
    let canonical = canonical_form(&request.schema).map_err(Error::invalid_schema)?;
    Ok((canonical == schema.canonical).then_some(schema))
}

async fn lookup(
    State(server): State<KafkaServer>,
    Path(subject): Path<String>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<SchemaRequest>,
) -> Response {
    match matching(&server, &headers, &subject, &request) {
        Ok(Some(schema)) => version_response(&subject, &schema).into_response(),
        Ok(None) => Error::schema_not_found().into_response(),
        Err(e) => e.into_response(),
    }
}

async fn register(
    State(server): State<KafkaServer>,
    Path(subject): Path<String>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<SchemaRequest>,
) -> Response {
    match matching(&server, &headers, &subject, &request) {
        Ok(Some(schema)) => Json(json!({"id": schema.id})).into_response(),
        Ok(None) => Error {
            status: StatusCode::CONFLICT,
            code: 409,
            message: format!(
                "Schema being registered is incompatible with the schema of subject \
                 '{subject}'. This registry serves the schemas of Materialize tables and \
                 accepts only those."
            ),
        }
        .into_response(),
        Err(e) => e.into_response(),
    }
}

impl KafkaServer {
    /// Returns the router that serves the schemas of the claimed topics, see [`self`].
    pub fn schema_registry(&self) -> Router {
        router(self.clone())
    }

    /// The schemas of the claimed topics that a connection authenticated with `credentials` may
    /// produce to, by subject, in subject order.
    fn served_schemas(
        &self,
        credentials: Option<&SaslPlainCredentials>,
    ) -> Vec<(String, ServedSchema)> {
        let state = self.inner.state.lock().expect("lock poisoned");
        let mut schemas = Vec::new();
        for (topic, entry) in &state.topics {
            if !crate::authorized(entry, credentials) {
                continue;
            }
            for (key, schema) in [(true, &entry.schemas.0), (false, &entry.schemas.1)] {
                if let Some(schema) = schema {
                    schemas.push((subject(topic, key), schema.clone()));
                }
            }
        }
        schemas.sort_by(|(a, _), (b, _)| a.cmp(b));
        schemas
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn schema_ids_depend_only_on_the_canonical_form() {
        let a = ServedSchema::new(
            r#"{"type": "record", "name": "r", "doc": "x", "fields": [{"name": "f", "type": "long"}]}"#
                .into(),
        )
        .unwrap();
        let b = ServedSchema::new(
            r#"{"name":"r","type":"record","fields":[{"type":"long","name":"f"}]}"#.into(),
        )
        .unwrap();
        let c = ServedSchema::new(
            r#"{"type": "record", "name": "r", "fields": [{"name": "f", "type": "int"}]}"#.into(),
        )
        .unwrap();
        assert_eq!(a.id, b.id);
        assert_ne!(a.id, c.id);
        assert!(a.id > 0);
    }
}
