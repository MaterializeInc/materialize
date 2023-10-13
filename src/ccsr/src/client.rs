// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use anyhow::bail;
use reqwest::{Method, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config::Auth;

/// An API client for a Confluent-compatible schema registry.
pub struct Client {
    inner: reqwest::Client,
    url: Arc<dyn Fn() -> Url + Send + Sync + 'static>,
    auth: Option<Auth>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("inner", &self.inner)
            .field("url", &"...")
            .field("auth", &self.auth)
            .finish()
    }
}

impl Client {
    pub(crate) fn new(
        inner: reqwest::Client,
        url: Arc<dyn Fn() -> Url + Send + Sync + 'static>,
        auth: Option<Auth>,
    ) -> Result<Self, anyhow::Error> {
        if url().cannot_be_a_base() {
            bail!("cannot construct a CCSR client with a cannot-be-a-base URL");
        }
        Ok(Client { inner, url, auth })
    }

    fn make_request<P>(&self, method: Method, path: P) -> reqwest::RequestBuilder
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        let mut url = (self.url)();
        url.path_segments_mut()
            .expect("constructor validated URL can be a base")
            .clear()
            .extend(path);

        let mut request = self.inner.request(method, url);
        if let Some(auth) = &self.auth {
            request = request.basic_auth(&auth.username, auth.password.as_ref());
        }
        request
    }

    /// Gets the schema with the associated ID.
    pub async fn get_schema_by_id(&self, id: i32) -> Result<Schema, GetByIdError> {
        let req = self.make_request(Method::GET, &["schemas", "ids", &id.to_string()]);
        let res: GetByIdResponse = send_request(req).await?;
        Ok(Schema {
            id,
            raw: res.schema,
        })
    }

    /// Gets the latest schema for the specified subject.
    pub async fn get_schema_by_subject(&self, subject: &str) -> Result<Schema, GetBySubjectError> {
        self.get_subject(subject).await.map(|s| s.schema)
    }

    /// Gets the latest version of the specified subject.
    pub async fn get_subject(&self, subject: &str) -> Result<Subject, GetBySubjectError> {
        let req = self.make_request(Method::GET, &["subjects", subject, "versions", "latest"]);
        let res: GetBySubjectResponse = send_request(req).await?;
        Ok(Subject {
            schema: Schema {
                id: res.id,
                raw: res.schema,
            },
            version: res.version,
            name: res.subject,
        })
    }

    /// Gets the latest version of the specified subject as well as all other
    /// subjects referenced by that subject (recursively).
    ///
    /// The dependencies are returned in alphabetical order by subject name.
    pub async fn get_subject_and_references(
        &self,
        subject: &str,
    ) -> Result<(Subject, Vec<Subject>), GetBySubjectError> {
        self.get_subject_and_references_by_version(subject, "latest".to_owned())
            .await
    }

    /// Gets a subject and all other subjects referenced by that subject (recursively)
    ///
    /// The dependencies are returned in alphabetical order by subject name.
    async fn get_subject_and_references_by_version(
        &self,
        subject: &str,
        version: String,
    ) -> Result<(Subject, Vec<Subject>), GetBySubjectError> {
        let mut subjects = vec![];
        let mut seen = BTreeSet::new();
        let mut subjects_queue = vec![(subject.to_owned(), version)];
        while let Some((subject, version)) = subjects_queue.pop() {
            let req = self.make_request(Method::GET, &["subjects", &subject, "versions", &version]);
            let res: GetBySubjectResponse = send_request(req).await?;
            subjects.push(Subject {
                schema: Schema {
                    id: res.id,
                    raw: res.schema,
                },
                version: res.version,
                name: res.subject.clone(),
            });
            seen.insert(res.subject);
            subjects_queue.extend(
                res.references
                    .into_iter()
                    .filter(|r| !seen.contains(&r.subject))
                    .map(|r| (r.subject, r.version.to_string())),
            );
        }
        assert!(subjects.len() > 0, "Request should error if no subjects");

        let primary = subjects.remove(0);
        subjects.sort_by(|a, b| a.name.cmp(&b.name));
        Ok((primary, subjects))
    }

    /// Publishes a new schema for the specified subject. The ID of the new
    /// schema is returned.
    ///
    /// Note that if a schema that is identical to an existing schema for the
    /// same subject is published, the ID of the existing schema will be
    /// returned.
    pub async fn publish_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<i32, PublishError> {
        let req = self.make_request(Method::POST, &["subjects", subject, "versions"]);
        let req = req.json(&PublishRequest {
            schema,
            schema_type,
            references,
        });
        let res: PublishResponse = send_request(req).await?;
        Ok(res.id)
    }

    /// Lists the names of all subjects that the schema registry is aware of.
    pub async fn list_subjects(&self) -> Result<Vec<String>, ListError> {
        let req = self.make_request(Method::GET, &["subjects"]);
        Ok(send_request(req).await?)
    }

    /// Deletes all schema versions associated with the specified subject.
    ///
    /// This API is only intended to be used in development environments.
    /// Deleting schemas only allows new, potentially incompatible schemas to
    /// be registered under the same subject. It does not allow the schema ID
    /// to be reused.
    pub async fn delete_subject(&self, subject: &str) -> Result<(), DeleteError> {
        let req = self.make_request(Method::DELETE, &["subjects", subject]);
        let _res: Vec<i32> = send_request(req).await?;
        Ok(())
    }

    /// Gets the latest version of the first subject found associated with the scheme with
    /// the given id, as well as all other subjects referenced by that subject (recursively).
    ///
    /// The dependencies are returned in alphabetical order by subject name.
    pub async fn get_subject_and_references_by_id(
        &self,
        id: i32,
    ) -> Result<(Subject, Vec<Subject>), GetBySubjectError> {
        let req = self.make_request(
            Method::GET,
            &["schemas", "ids", &id.to_string(), "versions"],
        );
        let res: Vec<SubjectVersion> = send_request(req).await?;

        // NOTE NOTE NOTE
        // We take the FIRST subject that matches this schema id. This could be DIFFERENT
        // than the actual subject we are interested in (it could even be from a different test
        // run), but we are trusting the schema registry to only output the same schema id for
        // identical subjects.
        // This was validated by publishing 2 empty schemas (i.e., identical), with different
        // references (one empty, one with a random reference), and they were not linked to the
        // same schema id.
        //
        // See https://docs.confluent.io/platform/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions
        // for more info.
        match res.as_slice() {
            [first, ..] => {
                self.get_subject_and_references_by_version(
                    &first.subject,
                    first.version.to_string(),
                )
                .await
            }
            _ => Err(GetBySubjectError::SubjectNotFound),
        }
    }
}

async fn send_request<T>(req: reqwest::RequestBuilder) -> Result<T, UnhandledError>
where
    T: DeserializeOwned,
{
    let res = req.send().await?;
    let status = res.status();
    if status.is_success() {
        Ok(res.json().await?)
    } else {
        match res.json::<ErrorResponse>().await {
            Ok(err_res) => Err(UnhandledError::Api {
                code: err_res.error_code,
                message: err_res.message,
            }),
            Err(_) => Err(UnhandledError::Api {
                code: i32::from(status.as_u16()),
                message: "unable to decode error details".into(),
            }),
        }
    }
}

/// The type of a schema stored by a schema registry.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum SchemaType {
    /// An Avro schema.
    Avro,
    /// A Protobuf schema.
    Protobuf,
    /// A JSON schema.
    Json,
}

impl SchemaType {
    fn is_default(&self) -> bool {
        matches!(self, SchemaType::Avro)
    }
}

/// A schema stored by a schema registry.
#[derive(Debug, Eq, PartialEq)]
pub struct Schema {
    /// The ID of the schema.
    pub id: i32,
    /// The raw text representing the schema.
    pub raw: String,
}

/// A subject stored by a schema registry.
#[derive(Debug, Eq, PartialEq)]
pub struct Subject {
    /// The version of the schema.
    pub version: i32,
    /// The name of the schema.
    pub name: String,
    /// The schema of the `version` of the `Subject`.
    pub schema: Schema,
}

/// A reference from one schema in a schema registry to another.
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaReference {
    /// The name of the reference.
    pub name: String,
    /// The subject under which the referenced schema is registered.
    pub subject: String,
    /// The version of the referenced schema.
    pub version: i32,
}

#[derive(Debug, Deserialize)]
struct GetByIdResponse {
    schema: String,
}

/// Errors for schema lookups by ID.
#[derive(Debug)]
pub enum GetByIdError {
    /// No schema with the requested ID exists.
    SchemaNotFound,
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

#[derive(Debug, Deserialize)]
struct SubjectVersion {
    /// The name of the subject
    pub subject: String,
    /// The version of the schema
    pub version: i32,
}

impl From<UnhandledError> for GetByIdError {
    fn from(err: UnhandledError) -> GetByIdError {
        match err {
            UnhandledError::Transport(err) => GetByIdError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40403 => GetByIdError::SchemaNotFound,
                _ => GetByIdError::Server { code, message },
            },
        }
    }
}

impl Error for GetByIdError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetByIdError::SchemaNotFound | GetByIdError::Server { .. } => None,
            GetByIdError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetByIdError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GetByIdError::SchemaNotFound => write!(f, "schema not found"),
            GetByIdError::Transport(err) => write!(f, "transport: {}", err),
            GetByIdError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetBySubjectResponse {
    id: i32,
    schema: String,
    version: i32,
    subject: String,
    #[serde(default)]
    references: Vec<SchemaReference>,
}

/// Errors for schema lookups by subject.
#[derive(Debug)]
pub enum GetBySubjectError {
    /// The requested subject does not exist.
    SubjectNotFound,
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

impl From<UnhandledError> for GetBySubjectError {
    fn from(err: UnhandledError) -> GetBySubjectError {
        match err {
            UnhandledError::Transport(err) => GetBySubjectError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40401 => GetBySubjectError::SubjectNotFound,
                _ => GetBySubjectError::Server { code, message },
            },
        }
    }
}

impl Error for GetBySubjectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetBySubjectError::SubjectNotFound | GetBySubjectError::Server { .. } => None,
            GetBySubjectError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetBySubjectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GetBySubjectError::SubjectNotFound => write!(f, "subject not found"),
            GetBySubjectError::Transport(err) => write!(f, "transport: {}", err),
            GetBySubjectError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PublishRequest<'a> {
    schema: &'a str,
    // Omitting the following fields when they're set to their defaults provides
    // compatibility with old versions of the schema registry that don't
    // understand these fields.
    #[serde(skip_serializing_if = "SchemaType::is_default")]
    schema_type: SchemaType,
    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    references: &'a [SchemaReference],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishResponse {
    id: i32,
}

/// Errors for publish operations.
#[derive(Debug)]
pub enum PublishError {
    /// The provided schema was not compatible with existing schemas for that
    /// subject, according to the subject's forwards- or backwards-compatibility
    /// requirements.
    IncompatibleSchema,
    /// The provided schema was invalid.
    InvalidSchema { message: String },
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

impl From<UnhandledError> for PublishError {
    fn from(err: UnhandledError) -> PublishError {
        match err {
            UnhandledError::Transport(err) => PublishError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                409 => PublishError::IncompatibleSchema,
                42201 => PublishError::InvalidSchema { message },
                _ => PublishError::Server { code, message },
            },
        }
    }
}

impl Error for PublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PublishError::IncompatibleSchema
            | PublishError::InvalidSchema { .. }
            | PublishError::Server { .. } => None,
            PublishError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // The error descriptions for IncompatibleSchema and InvalidSchema
            // are copied from the schema registry itself.
            PublishError::IncompatibleSchema => write!(
                f,
                "schema being registered is incompatible with an earlier schema"
            ),
            PublishError::InvalidSchema { message } => write!(f, "{}", message),
            PublishError::Transport(err) => write!(f, "transport: {}", err),
            PublishError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

/// Errors for list operations.
#[derive(Debug)]
pub enum ListError {
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

impl From<UnhandledError> for ListError {
    fn from(err: UnhandledError) -> ListError {
        match err {
            UnhandledError::Transport(err) => ListError::Transport(err),
            UnhandledError::Api { code, message } => ListError::Server { code, message },
        }
    }
}

impl Error for ListError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ListError::Server { .. } => None,
            ListError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for ListError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ListError::Transport(err) => write!(f, "transport: {}", err),
            ListError::Server { code, message } => write!(f, "server error {}: {}", code, message),
        }
    }
}

/// Errors for delete operations.
#[derive(Debug)]
pub enum DeleteError {
    /// The specified subject does not exist.
    SubjectNotFound,
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

impl From<UnhandledError> for DeleteError {
    fn from(err: UnhandledError) -> DeleteError {
        match err {
            UnhandledError::Transport(err) => DeleteError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40401 => DeleteError::SubjectNotFound,
                _ => DeleteError::Server { code, message },
            },
        }
    }
}

impl Error for DeleteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            DeleteError::SubjectNotFound | DeleteError::Server { .. } => None,
            DeleteError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for DeleteError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            DeleteError::SubjectNotFound => write!(f, "subject not found"),
            DeleteError::Transport(err) => write!(f, "transport: {}", err),
            DeleteError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error_code: i32,
    message: String,
}

#[derive(Debug)]
enum UnhandledError {
    Transport(reqwest::Error),
    Api { code: i32, message: String },
}

impl From<reqwest::Error> for UnhandledError {
    fn from(err: reqwest::Error) -> UnhandledError {
        UnhandledError::Transport(err)
    }
}
