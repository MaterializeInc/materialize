// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use proptest_derive::Arbitrary;
use reqwest::{Method, Response, Url};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::config::Auth;

/// An API client for a Confluent-compatible schema registry.
#[derive(Clone)]
pub struct Client {
    inner: reqwest::Client,
    url: Arc<dyn Fn() -> Url + Send + Sync + 'static>,
    auth: Option<Auth>,
    timeout: Duration,
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
        timeout: Duration,
    ) -> Result<Self, anyhow::Error> {
        if url().cannot_be_a_base() {
            bail!("cannot construct a CCSR client with a cannot-be-a-base URL");
        }
        Ok(Client {
            inner,
            url,
            auth,
            timeout,
        })
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

    pub fn timeout(&self) -> Duration {
        self.timeout
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
        self.get_subject_latest(subject).await.map(|s| s.schema)
    }

    /// Gets the latest version of the specified subject.
    pub async fn get_subject_latest(&self, subject: &str) -> Result<Subject, GetBySubjectError> {
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

    /// Gets the latest version of the specified subject along with its direct references.
    /// Returns the subject and a list of subject names that this subject directly references.
    pub async fn get_subject_with_references(
        &self,
        subject: &str,
    ) -> Result<(Subject, Vec<SubjectVersion>), GetBySubjectError> {
        let req = self.make_request(Method::GET, &["subjects", subject, "versions", "latest"]);
        let res: GetBySubjectResponse = send_request(req).await?;
        let referenced_subjects: Vec<_> = res
            .references
            .into_iter()
            .map(|r| SubjectVersion {
                subject: r.subject,
                version: r.version,
            })
            .collect();
        Ok((
            Subject {
                schema: Schema {
                    id: res.id,
                    raw: res.schema,
                },
                version: res.version,
                name: res.subject,
            },
            referenced_subjects,
        ))
    }

    /// Gets the config set for the specified subject
    pub async fn get_subject_config(
        &self,
        subject: &str,
    ) -> Result<SubjectConfig, GetSubjectConfigError> {
        let req = self.make_request(Method::GET, &["config", subject]);
        let res: SubjectConfig = send_request(req).await?;
        Ok(res)
    }

    /// Gets the latest version of the specified subject as well as all other
    /// subjects referenced by that subject (recursively).
    ///
    /// The dependencies are returned in dependency order, with dependencies first.
    pub async fn get_subject_and_references(
        &self,
        subject: &str,
    ) -> Result<(Subject, Vec<Subject>), GetBySubjectError> {
        self.get_subject_and_references_by_version(subject, "latest".to_owned())
            .await
    }

    /// Gets a subject and all other subjects referenced by that subject (recursively)
    ///
    /// The dependencies are returned in dependency order, with dependencies first.
    #[allow(clippy::disallowed_types)]
    async fn get_subject_and_references_by_version(
        &self,
        subject: &str,
        version: String,
    ) -> Result<(Subject, Vec<Subject>), GetBySubjectError> {
        let mut subjects = vec![];
        // HashMap are used as we strictly need lookup, not ordering.
        let mut graph = std::collections::HashMap::new();
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
            let subject_key = SubjectVersion {
                subject: res.subject,
                version: res.version,
            };

            let dependents: Vec<_> = res
                .references
                .into_iter()
                .map(|reference| SubjectVersion {
                    subject: reference.subject,
                    version: reference.version,
                })
                .collect();

            graph
                .entry(subject_key)
                .or_insert_with(Vec::new)
                .extend(dependents.iter().cloned());
            subjects_queue.extend(
                dependents
                    .into_iter()
                    // Add dependents into the graph before adding to the queue as the same
                    // named type may be encountered multiple times, but we only want to look it up
                    // once.
                    .filter(|dep| match graph.entry(dep.clone()) {
                        std::collections::hash_map::Entry::Occupied(_) => false,
                        std::collections::hash_map::Entry::Vacant(vacant) => {
                            vacant.insert(vec![]);
                            true
                        }
                    })
                    .map(|dep| (dep.subject, dep.version.to_string())),
            );
        }
        assert!(subjects.len() > 0, "Request should error if no subjects");

        let primary = subjects.remove(0);

        let ordered =
            topological_sort(&graph).map_err(|_| GetBySubjectError::SchemaReferenceCycle)?;

        subjects.sort_by(|a, b| {
            let a = SubjectVersion {
                subject: a.name.clone(),
                version: a.version,
            };
            let b = SubjectVersion {
                subject: b.name.clone(),
                version: b.version,
            };
            ordered
                .get(&b)
                .unwrap_or_else(|| panic!("b {b:?}"))
                .cmp(ordered.get(&a).unwrap_or_else(|| panic!("a {a:?}")))
        });

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

    /// Sets the compatibility level for the specified subject.
    pub async fn set_subject_compatibility_level(
        &self,
        subject: &str,
        compatibility_level: CompatibilityLevel,
    ) -> Result<(), SetCompatibilityLevelError> {
        let req = self.make_request(Method::PUT, &["config", subject]);
        let req = req.json(&CompatibilityLevelRequest {
            compatibility: compatibility_level,
        });
        send_request_raw(req).await?;
        Ok(())
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
        send_request_raw(req).await?;
        Ok(())
    }

    /// Gets the latest version of the first subject found associated with the scheme with
    /// the given id, as well as all other subjects referenced by that subject (recursively).
    ///
    /// The dependencies are returned in dependency order, with dependencies first.
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

/// Generates a topological ordering for a DAG.  If a cycle is detected in any returns an error.
/// This can operator on a disconnected graph containing multiple DAGs.
#[allow(clippy::disallowed_types)]
pub fn topological_sort<T: Hash + Eq>(
    graph: &std::collections::HashMap<T, Vec<T>>,
) -> Result<std::collections::HashMap<&T, i32>, anyhow::Error> {
    let mut referenced_by: std::collections::HashMap<&T, std::collections::HashSet<&T>> =
        std::collections::HashMap::new();
    for (subject, references) in graph.iter() {
        for reference in references {
            referenced_by.entry(reference).or_default().insert(subject);
        }
    }

    // Start with nodes that have no incoming edges (empty referenced_by sets).
    // Also include nodes in graph that aren't in referenced_by at all (roots).
    let mut queue: Vec<_> = graph
        .keys()
        .filter(|key| {
            referenced_by
                .get(*key)
                .map_or(true, |subjects| subjects.is_empty())
        })
        .collect();

    let mut ordered = std::collections::HashMap::new();
    let mut n = 0;
    while let Some(subj_ver) = queue.pop() {
        if let Some(refs) = graph.get(subj_ver) {
            for ref_ver in refs {
                let Some(subjects) = referenced_by.get_mut(ref_ver) else {
                    continue;
                };
                subjects.remove(&subj_ver);
                if subjects.is_empty() {
                    referenced_by.remove_entry(ref_ver);
                    queue.push(ref_ver);
                }
            }
        }
        ordered.insert(subj_ver, n);
        n += 1;
    }

    if referenced_by.is_empty() {
        Ok(ordered)
    } else {
        Err(anyhow!("Cycled detected during topoligical sort"))
    }
}

async fn send_request<T>(req: reqwest::RequestBuilder) -> Result<T, UnhandledError>
where
    T: DeserializeOwned,
{
    let res = send_request_raw(req).await?;
    Ok(res.json().await?)
}

async fn send_request_raw(req: reqwest::RequestBuilder) -> Result<Response, UnhandledError> {
    let res = req.send().await?;
    let status = res.status();
    if status.is_success() {
        Ok(res)
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

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SubjectVersion {
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
pub struct SubjectConfig {
    pub compatibility_level: CompatibilityLevel,
    // There are other fields to include if we need them.
}

/// Errors for schema lookups by subject.
#[derive(Debug)]
pub enum GetSubjectConfigError {
    /// The requested subject does not exist.
    SubjectNotFound,
    /// The compatibility level for the subject has not been set.
    SubjectCompatibilityLevelNotSet,
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

impl From<UnhandledError> for GetSubjectConfigError {
    fn from(err: UnhandledError) -> GetSubjectConfigError {
        match err {
            UnhandledError::Transport(err) => GetSubjectConfigError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                404 => GetSubjectConfigError::SubjectNotFound,
                40408 => GetSubjectConfigError::SubjectCompatibilityLevelNotSet,
                _ => GetSubjectConfigError::Server { code, message },
            },
        }
    }
}

impl Error for GetSubjectConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetSubjectConfigError::SubjectNotFound
            | GetSubjectConfigError::SubjectCompatibilityLevelNotSet
            | GetSubjectConfigError::Server { .. } => None,
            GetSubjectConfigError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetSubjectConfigError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GetSubjectConfigError::SubjectNotFound => write!(f, "subject not found"),
            GetSubjectConfigError::SubjectCompatibilityLevelNotSet => {
                write!(f, "subject level compatibility not set")
            }
            GetSubjectConfigError::Transport(err) => write!(f, "transport: {}", err),
            GetSubjectConfigError::Server { code, message } => {
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
    /// The requested version does not exist.
    VersionNotFound(String),
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
    /// Cycle detected in schemas
    SchemaReferenceCycle,
}

impl From<UnhandledError> for GetBySubjectError {
    fn from(err: UnhandledError) -> GetBySubjectError {
        match err {
            UnhandledError::Transport(err) => GetBySubjectError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40401 => GetBySubjectError::SubjectNotFound,
                40402 => GetBySubjectError::VersionNotFound(message),
                _ => GetBySubjectError::Server { code, message },
            },
        }
    }
}

impl Error for GetBySubjectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetBySubjectError::SubjectNotFound
            | GetBySubjectError::VersionNotFound(_)
            | GetBySubjectError::Server { .. }
            | GetBySubjectError::SchemaReferenceCycle => None,
            GetBySubjectError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetBySubjectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GetBySubjectError::SubjectNotFound => write!(f, "subject not found"),
            GetBySubjectError::VersionNotFound(message) => {
                write!(f, "version not found: {}", message)
            }
            GetBySubjectError::Transport(err) => write!(f, "transport: {}", err),
            GetBySubjectError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
            GetBySubjectError::SchemaReferenceCycle => {
                write!(f, "cycle detected in schema references")
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

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CompatibilityLevelRequest {
    compatibility: CompatibilityLevel,
}

#[derive(Arbitrary, Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CompatibilityLevel {
    Backward,
    BackwardTransitive,
    Forward,
    ForwardTransitive,
    Full,
    FullTransitive,
    None,
}

impl TryFrom<&str> for CompatibilityLevel {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "BACKWARD" => Ok(CompatibilityLevel::Backward),
            "BACKWARD_TRANSITIVE" => Ok(CompatibilityLevel::BackwardTransitive),
            "FORWARD" => Ok(CompatibilityLevel::Forward),
            "FORWARD_TRANSITIVE" => Ok(CompatibilityLevel::ForwardTransitive),
            "FULL" => Ok(CompatibilityLevel::Full),
            "FULL_TRANSITIVE" => Ok(CompatibilityLevel::FullTransitive),
            "NONE" => Ok(CompatibilityLevel::None),
            _ => Err(format!("invalid compatibility level: {}", value)),
        }
    }
}

impl fmt::Display for CompatibilityLevel {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CompatibilityLevel::Backward => write!(f, "BACKWARD"),
            CompatibilityLevel::BackwardTransitive => write!(f, "BACKWARD_TRANSITIVE"),
            CompatibilityLevel::Forward => write!(f, "FORWARD"),
            CompatibilityLevel::ForwardTransitive => write!(f, "FORWARD_TRANSITIVE"),
            CompatibilityLevel::Full => write!(f, "FULL"),
            CompatibilityLevel::FullTransitive => write!(f, "FULL_TRANSITIVE"),
            CompatibilityLevel::None => write!(f, "NONE"),
        }
    }
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

/// Errors for setting compatibility level operations.
#[derive(Debug)]
pub enum SetCompatibilityLevelError {
    /// The compatibility level is invalid.
    InvalidCompatibilityLevel,
    /// The underlying HTTP transport failed.
    Transport(reqwest::Error),
    /// An internal server error occurred.
    Server { code: i32, message: String },
}

impl From<UnhandledError> for SetCompatibilityLevelError {
    fn from(err: UnhandledError) -> SetCompatibilityLevelError {
        match err {
            UnhandledError::Transport(err) => SetCompatibilityLevelError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                42203 => SetCompatibilityLevelError::InvalidCompatibilityLevel,
                _ => SetCompatibilityLevelError::Server { code, message },
            },
        }
    }
}

impl Error for SetCompatibilityLevelError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            SetCompatibilityLevelError::InvalidCompatibilityLevel
            | SetCompatibilityLevelError::Server { .. } => None,
            SetCompatibilityLevelError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for SetCompatibilityLevelError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            SetCompatibilityLevelError::InvalidCompatibilityLevel => {
                write!(f, "invalid compatibility level")
            }
            SetCompatibilityLevelError::Transport(err) => write!(f, "transport: {}", err),
            SetCompatibilityLevelError::Server { code, message } => {
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

#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_types)]
    use super::*;
    use std::collections::HashMap;

    /// Helper to create a SubjectVersion
    fn sv(subject: &str, version: i32) -> SubjectVersion {
        SubjectVersion {
            subject: subject.to_string(),
            version,
        }
    }

    /// Helper to build a graph from a list of edges. Each edge is (from, to) meaning "from"
    /// depends on "to".
    ///
    /// We take the root separately to build a graph without edges.
    fn build_graph(
        edges: &[(SubjectVersion, Option<SubjectVersion>)],
    ) -> HashMap<SubjectVersion, Vec<SubjectVersion>> {
        let mut graph: HashMap<SubjectVersion, Vec<SubjectVersion>> = HashMap::new();

        // Add edges: from depends on to
        for (from, to) in edges {
            let deps = graph.entry(from.clone()).or_default();
            if let Some(to) = to {
                deps.push(to.clone());
            }
        }

        graph
    }

    /// Verify that all edges are respected in the ordering.
    /// For edge (from, to) where 'from' depends on 'to':
    /// - 'from' should be processed before 'to' (lower order number)
    /// - This is because the algorithm processes from roots (nothing depends on them)
    ///   towards leaves (they don't depend on anything)
    fn verify_edge_ordering(
        ordered: &HashMap<&SubjectVersion, i32>,
        edges: &[(SubjectVersion, Option<SubjectVersion>)],
    ) {
        for (from, to) in edges {
            if let Some(to) = to {
                let from_order = ordered.get(from).expect("from node should be in ordering");
                let to_order = ordered.get(to).expect("to node should be in ordering");

                // 'from' depends on 'to', so 'from' is processed first (lower order number)
                // The algorithm starts at roots (nodes nothing depends on) and works toward leaves
                assert!(
                    from_order < to_order,
                    "{:?} (order {}) depends on {:?} (order {}), so should be processed first",
                    from,
                    from_order,
                    to,
                    to_order
                );
            }
        }
    }

    #[mz_ore::test]
    fn test_topological_sort_empty_graph() {
        let graph: HashMap<SubjectVersion, Vec<SubjectVersion>> = HashMap::new();

        let ordered = topological_sort(&graph).unwrap();

        assert!(ordered.is_empty());
    }

    #[mz_ore::test]
    fn test_topological_sort_single_node() {
        // Single node with no dependencies
        let a = sv("a", 1);
        let graph = build_graph(&[(a.clone(), None)]);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 1);
        assert!(ordered.contains_key(&a));
    }

    #[mz_ore::test]
    fn test_topological_sort_linear_chain() {
        // A -> B -> C -> D
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (b.clone(), Some(c.clone())),
            (c.clone(), Some(d.clone())),
            (d.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 4);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_diamond() {
        // Classic diamond pattern:
        //     A
        //    / \
        //   B   C
        //    \ /
        //     D
        // A depends on B and C, both B and C depend on D
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (b.clone(), Some(d.clone())),
            (c.clone(), Some(d.clone())),
            (d.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 4);
        verify_edge_ordering(&ordered, &edges);

        // A must come before B and C, and B and C must come before D
        assert!(ordered[&a] < ordered[&d]);
    }

    #[mz_ore::test]
    fn test_topological_sort_wide_graph() {
        // Wide graph: A depends on B, C, D, E, F (many dependencies)
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);
        let e = sv("e", 1);
        let f = sv("f", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (a.clone(), Some(d.clone())),
            (a.clone(), Some(e.clone())),
            (a.clone(), Some(f.clone())),
            (b.clone(), None),
            (c.clone(), None),
            (d.clone(), None),
            (e.clone(), None),
            (f.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 6);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_complex_dag() {
        // Complex DAG:
        //       A
        //      /|\
        //     B C D
        //     |/| |
        //     E F G
        //      \|/
        //       H
        // A -> B, C, D
        // B -> E
        // C -> E, F
        // D -> G
        // E -> H
        // F -> H
        // G -> H
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);
        let e = sv("e", 1);
        let f = sv("f", 1);
        let g = sv("g", 1);
        let h = sv("h", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (a.clone(), Some(d.clone())),
            (b.clone(), Some(e.clone())),
            (c.clone(), Some(e.clone())),
            (c.clone(), Some(f.clone())),
            (d.clone(), Some(g.clone())),
            (e.clone(), Some(h.clone())),
            (f.clone(), Some(h.clone())),
            (g.clone(), Some(h.clone())),
            (h.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 8);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_with_versions() {
        // Same subject, different versions
        //        A2
        //       / |
        //      A1 |
        //       \ |
        //        B1
        let a_v1 = sv("a", 1);
        let a_v2 = sv("a", 2);
        let b_v1 = sv("b", 1);

        // a_v2 depends on a_v1, and both depend on b_v1
        let edges = vec![
            (a_v2.clone(), Some(a_v1.clone())),
            (a_v2.clone(), Some(b_v1.clone())),
            (a_v1.clone(), Some(b_v1.clone())),
            (b_v1.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 3);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_multi_level_diamond() {
        // Double diamond:
        //       A
        //      / \
        //     B   C
        //      \ /
        //       D
        //      / \
        //     E   F
        //      \ /
        //       G
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);
        let e = sv("e", 1);
        let f = sv("f", 1);
        let g = sv("g", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (b.clone(), Some(d.clone())),
            (c.clone(), Some(d.clone())),
            (d.clone(), Some(e.clone())),
            (d.clone(), Some(f.clone())),
            (e.clone(), Some(g.clone())),
            (f.clone(), Some(g.clone())),
            (g.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 7);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_shared_dependency_at_multiple_levels() {
        // Shared dependency accessed at multiple levels:
        //     A
        //    /|\
        //   B C |
        //   |/  |
        //   D   |
        //    \ /
        //     E
        //     |
        //     F
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);
        let e = sv("e", 1);
        let f = sv("f", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (a.clone(), Some(e.clone())),
            (b.clone(), Some(d.clone())),
            (c.clone(), Some(d.clone())),
            (d.clone(), Some(e.clone())),
            (e.clone(), Some(f.clone())),
            (f.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).expect("no cycle");

        assert_eq!(ordered.len(), 6);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_lattice_structure() {
        // Can you tell I had Claude help make up tests?
        // Lattice structure (more complex than diamond):
        //       A
        //      /|\
        //     B C D
        //     |\|/|
        //     | X |
        //     |/|\|
        //     E F G
        //      \|/
        //       H
        // A -> B, C, D
        // B -> E, F
        // C -> E, F, G
        // D -> F, G
        // E -> H
        // F -> H
        // G -> H
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);
        let e = sv("e", 1);
        let f = sv("f", 1);
        let g = sv("g", 1);
        let h = sv("h", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (a.clone(), Some(d.clone())),
            (b.clone(), Some(e.clone())),
            (b.clone(), Some(f.clone())),
            (c.clone(), Some(e.clone())),
            (c.clone(), Some(f.clone())),
            (c.clone(), Some(g.clone())),
            (d.clone(), Some(f.clone())),
            (d.clone(), Some(g.clone())),
            (e.clone(), Some(h.clone())),
            (f.clone(), Some(h.clone())),
            (g.clone(), Some(h.clone())),
            (h.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 8);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_binary_tree() {
        // Full binary tree structure (inverted, dependencies flow down):
        //        A
        //       / \
        //      B   C
        //     /|   |\
        //    D E   F G
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);
        let d = sv("d", 1);
        let e = sv("e", 1);
        let f = sv("f", 1);
        let g = sv("g", 1);

        let edges = vec![
            (a.clone(), Some(b.clone())),
            (a.clone(), Some(c.clone())),
            (b.clone(), Some(d.clone())),
            (b.clone(), Some(e.clone())),
            (c.clone(), Some(f.clone())),
            (c.clone(), Some(g.clone())),
            (d.clone(), None),
            (e.clone(), None),
            (f.clone(), None),
            (g.clone(), None),
        ];
        let graph = build_graph(&edges);

        let ordered = topological_sort(&graph).unwrap();

        assert_eq!(ordered.len(), 7);
        verify_edge_ordering(&ordered, &edges);
    }

    #[mz_ore::test]
    fn test_topological_sort_simple_cycle() {
        // Simple cycle: A -> B -> A
        let a = sv("a", 1);
        let b = sv("b", 1);

        let mut graph: HashMap<SubjectVersion, Vec<SubjectVersion>> = HashMap::new();
        graph.insert(a.clone(), vec![b.clone()]);
        graph.insert(b.clone(), vec![a.clone()]);

        let sort_result = topological_sort(&graph);

        assert!(sort_result.is_err(), "Expected sort to detect cycle");
    }

    #[mz_ore::test]
    fn test_topological_sort_cycle_with_entry_point() {
        // Cycle with entry point: A -> B -> C -> B (B and C form a cycle)
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);

        let mut graph: HashMap<SubjectVersion, Vec<SubjectVersion>> = HashMap::new();
        graph.insert(a.clone(), vec![b.clone()]);
        graph.insert(b.clone(), vec![c.clone()]);
        graph.insert(c.clone(), vec![b.clone()]); // C points back to B

        let sort_result = topological_sort(&graph);
        assert!(sort_result.is_err(), "Expected sort to detect cycle");
    }

    #[mz_ore::test]
    fn test_topological_sort_self_reference() {
        // Self-referencing node: A -> A
        let a = sv("a", 1);

        let mut graph: HashMap<SubjectVersion, Vec<SubjectVersion>> = HashMap::new();
        graph.insert(a.clone(), vec![a.clone()]);

        let sort_result = topological_sort(&graph);
        assert!(sort_result.is_err(), "Expected sort to detect cycle");
    }

    #[mz_ore::test]
    fn test_topological_sort_three_node_cycle() {
        // Three node cycle: A -> B -> C -> A
        let a = sv("a", 1);
        let b = sv("b", 1);
        let c = sv("c", 1);

        let mut graph: HashMap<SubjectVersion, Vec<SubjectVersion>> = HashMap::new();
        graph.insert(a.clone(), vec![b.clone()]);
        graph.insert(b.clone(), vec![c.clone()]);
        graph.insert(c.clone(), vec![a.clone()]);

        let sort_result = topological_sort(&graph);
        assert!(sort_result.is_err(), "Expected sort to detect cycle");
    }
}
