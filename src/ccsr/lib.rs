// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Confluent-compatible schema registry API client.

use std::error::Error;
use std::fmt;

use reqwest::{RedirectPolicy, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;

/// An API client for a Confluent-compatible schema registry.
pub struct Client {
    inner: reqwest::Client,
    url: Url,
}

impl Client {
    /// Creates a new API client that will send requests to the schema registry
    /// at the provided URL.
    pub fn new(url: Url) -> Client {
        let inner = reqwest::Client::builder()
            .redirect(RedirectPolicy::none())
            .build()
            .unwrap();
        Client { inner, url }
    }

    /// Gets the schema with the associated ID.
    pub fn get_schema(&self, id: i32) -> Result<String, GetError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/schemas/ids/{}", id));
        let res: GetResponse = send_request(self.inner.get(url))?;
        Ok(res.schema)
    }

    /// Publishes a new schema for the specified subject. The ID of the new
    /// schema is returned.
    ///
    /// Note that if a schema that is identical to an existing schema for the
    /// same subject is published, the ID of the existing schema will be
    /// returned.
    pub fn publish_schema(&self, subject: &str, schema: &str) -> Result<i32, PublishError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/subjects/{}/versions", subject));
        let json = json!({ "schema": schema }).to_string();
        let res: PublishResponse = send_request(self.inner.post(url).body(json))?;
        Ok(res.id)
    }

    /// Lists the names of all subjects that the schema registry is aware of.
    pub fn list_subjects(&self) -> Result<Vec<String>, ListError> {
        let mut url = self.url.clone();
        url.set_path("/subjects");
        Ok(send_request(self.inner.get(url))?)
    }

    /// Deletes all schema versions associated with the specified subject.
    ///
    /// This API is only intended to be used in development environments.
    /// Deleting schemas only allows new, potentially incompatible schemas to
    /// be registered under the same subject. It does not allow the schema ID
    /// to be reused.
    pub fn delete_subject(&self, subject: &str) -> Result<(), DeleteError> {
        let mut url = self.url.clone();
        url.set_path(&format!("/subjects/{}", subject));
        let _res: Vec<i32> = send_request(self.inner.delete(url))?;
        Ok(())
    }
}

fn send_request<T>(req: reqwest::RequestBuilder) -> Result<T, UnhandledError>
where
    T: DeserializeOwned,
{
    let mut res = req.send()?;
    if res.status().is_success() {
        Ok(res.json()?)
    } else {
        match res.json::<ErrorResponse>() {
            Ok(err_res) => Err(UnhandledError::Api {
                code: err_res.error_code,
                message: err_res.message,
            }),
            Err(_) => Err(UnhandledError::Api {
                code: i32::from(res.status().as_u16()),
                message: "unable to decode error details".into(),
            }),
        }
    }
}

#[derive(Debug, Deserialize)]
struct GetResponse {
    schema: String,
}

#[derive(Debug)]
pub enum GetError {
    SchemaNotFound,
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for GetError {
    fn from(err: UnhandledError) -> GetError {
        match err {
            UnhandledError::Transport(err) => GetError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                40403 => GetError::SchemaNotFound,
                _ => GetError::Server { code, message },
            },
        }
    }
}

impl Error for GetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            GetError::SchemaNotFound | GetError::Server { .. } => None,
            GetError::Transport(err) => Some(err),
        }
    }
}

impl fmt::Display for GetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // The error description for SchemaNotFound is copied from the
            // schema registry itself.
            GetError::SchemaNotFound => write!(f, "schema not found"),
            GetError::Transport(err) => write!(f, "transport: {}", err),
            GetError::Server { code, message } => write!(f, "server error {}: {}", code, message),
        }
    }
}

#[derive(Debug, Deserialize)]
struct PublishResponse {
    id: i32,
}

#[derive(Debug)]
pub enum PublishError {
    IncompatibleSchema,
    InvalidSchema,
    Transport(reqwest::Error),
    Server { code: i32, message: String },
}

impl From<UnhandledError> for PublishError {
    fn from(err: UnhandledError) -> PublishError {
        match err {
            UnhandledError::Transport(err) => PublishError::Transport(err),
            UnhandledError::Api { code, message } => match code {
                409 => PublishError::IncompatibleSchema,
                42201 => PublishError::InvalidSchema,
                _ => PublishError::Server { code, message },
            },
        }
    }
}

impl Error for PublishError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            PublishError::IncompatibleSchema
            | PublishError::InvalidSchema
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
            PublishError::InvalidSchema => write!(f, "input schema is an invalid avro schema"),
            PublishError::Transport(err) => write!(f, "transport: {}", err),
            PublishError::Server { code, message } => {
                write!(f, "server error {}: {}", code, message)
            }
        }
    }
}

#[derive(Debug)]
pub enum ListError {
    Transport(reqwest::Error),
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

#[derive(Debug)]
pub enum DeleteError {
    SubjectNotFound,
    Transport(reqwest::Error),
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
            // The error description for SubjectNotFound is copied from the
            // schema registry itself.
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
pub enum UnhandledError {
    Transport(reqwest::Error),
    Api { code: i32, message: String },
}

impl From<reqwest::Error> for UnhandledError {
    fn from(err: reqwest::Error) -> UnhandledError {
        UnhandledError::Transport(err)
    }
}
